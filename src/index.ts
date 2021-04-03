import crypto from "crypto";
import Queue from "./lib/queue";
import Semaphore from "./lib/semaphore";
import EventEmitter from "eventemitter3";
import debug from "debug";

export interface Task<T> {
  id: string;
  name: string; // must be a unique string
  run: (worker: Worker) => Promise<T>;
}

export interface TaskResult<T> {
  error?: Error;
  value?: T;
}

export const newLambdaTask = <T>(name: string, func: (worker: Worker) => Promise<T>) => {
  return {
    id: crypto.randomBytes(8).toString("hex"),
    name: name,
    run: func,
  } as Task<T>;
};

interface WorkerJob {
  task: Task<any>;
  children: Task<any>[];
  status: "pending" | "executing" | "blocked" | "done";
  callbacks: ((result: TaskResult<any>) => void)[];
  blocked: boolean;
  priority: number;
}

/**
 * TODO: implement garbage collection of completed worker jobs
 */
let nextPoolId = 0;
export class WorkerPool {
  private _id: number = nextPoolId++;
  private workers: Worker[] = [];
  private workerJobs: { [id: string]: WorkerJob } = {};

  private availableJobs: Semaphore = new Semaphore(0);
  private queues: Queue<WorkerJob>[] = [];
  private debug = debug("workqueue:pool");

  constructor(numWorkers: number) {
    for (let i = 0; i < numWorkers; ++i) {
      this.workers.push(new Worker(this));
    }
    // push the initial queue, there is always at least one
    this.queues.push(new Queue());
  }

  get id() {
    return this._id;
  }

  async execute<T>(rootTask: Task<T>) {
    const newRootJob: WorkerJob = {
      task: rootTask,
      priority: 0,
      children: [],
      status: "pending",
      callbacks: [],
      blocked: false,
    };
    this.workerJobs[newRootJob.task.id] = newRootJob;
    this.queues[0].enqueue(newRootJob);
    this.availableJobs.V(); // increment to indicate a job is available

    const workersCompleted = [];
    for (const worker of this.workers) {
      workersCompleted.push(worker.run());
    }

    this.debug("pool.execute awaiting result of root task");
    const result: TaskResult<T> = await new Promise((accept) => {
      newRootJob.callbacks.push(accept);
    });
    this.debug("pool has detected that the root task exited.");

    // signal to kill the workers when the root task completes
    this.debug("availableJobs is " + this.availableJobs.value() + ", signaling until all workers are unblocked.");
    while (this.availableJobs.value() <= 0) {
      // wake up the workers but no tasks will be available triggering them to exit
      this.availableJobs.V(); 
    }

    this.debug("waiting for all workers to exit.");
    await Promise.all(workersCompleted);

    return result;
  }

  // TODO: implemnent mechanism to seed the root task...
  enqueueTask(parentTask: Task<any>, task: Task<any>) {
    const parentJob = this.workerJobs[parentTask.id];
    if (!parentJob) {
      throw new Error("no such parent task: " + parentTask.id);
    }
    const newJob: WorkerJob = {
      task,
      priority: this.workerJobs[parentTask.id].priority + 1,
      children: [],
      status: "pending",
      callbacks: [],
      blocked: false,
    };
    parentJob.children.push(task);

    // add new queues until there is a slot for it
    while (this.queues.length <= newJob.priority) {
      this.queues.push(new Queue());
    }

    this.queues[newJob.priority].enqueue(newJob);
    this.workerJobs[task.id] = newJob;

    // increment the number of available jobs
    this.debug(`enqueued new job: (id: ${task.id}) ${task.name}`);
    this.availableJobs.V();

    return newJob;
  }

  getJobForTask(parentTask: Task<any>, task: Task<any>): WorkerJob {
    let job = this.workerJobs[task.id];
    if (job) {
      return job;
    }
    return this.enqueueTask(parentTask, task);
  }

  async getNextJob() {
    await this.availableJobs.P();
    
    let queue = this.queues[this.queues.length - 1];
    while (queue.size() === 0 && this.queues.length > 1) {
      this.queues.pop();
      queue = this.queues[this.queues.length - 1];
    }

    return queue.dequeue();
  }

  getNumWorkers() {
    return this.workers.length;
  }
}

let nextWorkerId = 0;

export class Worker {
  /*
    worker represents one thread of execution
    worker maintains a stack of jobs, only one job in the stack should ever be unblocked
  */

  private _id: number;
  private pool: WorkerPool;
  private workSemaphore: Semaphore = new Semaphore(1);
  private curJob: WorkerJob | null = null;
  private debug: debug.Debugger;

  constructor(pool: WorkerPool) {
    this.pool = pool;
    this._id = this.pool.getNumWorkers();
    this.debug = debug("workqueue:" + this.pool.id + this.id);
  }

  get id() {
    return this._id;
  }

  async runJob(job: WorkerJob) {
    // run the task
    let result;
    try {
      // acquire the work semaphore / right to execute
      await this.workSemaphore.P();
      this.curJob = job;

      job.status = "executing";
      result = {
        value: await job.task.run(this)
      }
    } catch (e) {
      result = {
        error: e
      };
    } finally {
      job.status = "done";
      this.curJob = null;
      this.workSemaphore.V();
    }

    // return the results via the callbacks
    for (const callback of job.callbacks) {
      callback(result);
    }
  }

  async awaitResults<T>(tasks: Task<T>[]): Promise<TaskResult<T>[]> {
    const curJob = this.curJob;
    curJob.status = "blocked";
    this.curJob = null; 

    // release the work semaphore 
    setImmediate(() => {
      this.workSemaphore.V();
    });

    // wait on the results via callbacks
    const promises = tasks.map((task) => {
      const job = this.pool.getJobForTask(curJob.task, task);
      return new Promise((accept, reject) => {
        job.callbacks.push(accept);
      });
    });

    console.log("running awaitResults on " + tasks.length + " tasks.");
    const results = (await Promise.all(promises)) as TaskResult<T>[];
    console.log("unblocked awaitTasks, job completed.");

    // reacquire the work semaphore 
    await this.workSemaphore.P();
    return results;
  }

  async run() {
    while (true) {
      this.debug("worker " + this.id + " running, waiting for the next job.");
      const job = await this.pool.getNextJob();
      this.debug("worker " + this.id + " running, got inside getNextJob critical section.");
      if (!job) {
        this.debug("worker " + this.id + " exiting -- queue provided null job indicating end.");
        break;
      }
      this.debug("worker " + this.id + " pulled job from queue... task name: " + job.task.name);
      await this.runJob(job); 
    }
  }
}
