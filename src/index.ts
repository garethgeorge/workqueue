import crypto from "crypto";
import Queue from "./lib/queue";
import Semaphore from "./lib/semaphore";
import EventEmitter from "eventemitter3";
import debug from "debug";

/**
 * An individual unit of work
 */
export interface Task<T> {
  id: string;
  name: string; // must be a unique string
  run: (worker: Worker) => Promise<T>;
}

/**
 * The result of executing a task
 */
export interface TaskResult<T> {
  error?: Error;
  value?: T;
}

/**
 * Utility function to create a task with a given name from an anonymous asynchronous function.
 * @param name name for the generated task
 * @param func asynchronous function to execute
 * @returns a newly constructed task with the specified name and function
 */
export const newLambdaTask = <T>(
  name: string,
  func: (worker: Worker) => Promise<T>
) => {
  return {
    id: crypto.randomBytes(8).toString("hex"),
    name: name,
    run: func,
  } as Task<T>;
};

enum JobStatus {
  PENDING = "pending",
  RUNNING = "running",
  DONE = "done",
}

/**
 * Wrapper of a task that includes its metadata and execution status
 */
interface WorkerJob {
  task: Task<any>;
  children: Task<any>[];
  callbacks: ((result: TaskResult<any>) => void)[];
  priority: number;
  status: JobStatus;
  blocked: boolean;
}

/**
 * !TODO: implement garbage collection of completed worker jobs that enter 'done' state.
 *  NOTE: job lifecycle management should be extensible.
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

  spawnWorker() {
    const newWorker = new Worker(this);
    this.workers.push(newWorker);
    return newWorker;
  }

  killWorker(workerToKill: Worker) {
    workerToKill.kill();
    this.workers = this.workers.filter((worker) => {
      return worker != workerToKill;
    });
  }

  async execute<T>(rootTask: Task<T>) {
    const newRootJob: WorkerJob = {
      task: rootTask,
      priority: 0,
      children: [],
      status: JobStatus.PENDING,
      blocked: false,
      callbacks: [],
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
    for (const worker of this.workers) {
      this.debug("killing worker: " + worker.id);
      this.killWorker(worker); // NOTE: O(n) making this loop O(n^2)
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

    // setup a job for the task which tracks its execution status and priority etc.
    const newJob: WorkerJob = {
      task,
      priority: this.workerJobs[parentTask.id].priority + 1,
      children: [],
      status: JobStatus.PENDING,
      blocked: false,
      callbacks: [],
    };
    parentJob.children.push(task);
    this.workerJobs[task.id] = newJob;

    // enqueue the newly created job
    this.enqueueJob(newJob);
    if (this.debug.enabled)
      this.debug(`enqueued new job: (id: ${task.id}) ${task.name}`);
    return newJob;
  }

  enqueueJob(job: WorkerJob) {
    // add new queues until there is a slot for it
    while (this.queues.length <= job.priority) {
      this.queues.push(new Queue());
    }
    // add to the queue corresponding to the jobs priority (which must now exist)
    this.queues[job.priority].enqueue(job);
    this.availableJobs.V();
  }

  /**
   * Either returns metadata for a given task or enqueues it as a child of 'parentTask' if it does not exist.
   * @param parentTask the task to make the parent task if no job is found for 'task'
   * @param task the task we want to find job metadata for or enqueue if it does not exist
   * @returns job metadata associated with the provided task
   */
  getOrMakeJobForTask(parentTask: Task<any>, task: Task<any>): WorkerJob {
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

  get numWorkers() {
    return this.workers.length;
  }
}

let nextWorkerId = 0;

export class Worker {
  /*
    worker represents one thread of execution
    worker maintains a stack of jobs, only one job in the stack should ever be unblocked
  */

  // worker info
  private _id: number;
  private pool: WorkerPool;
  private debug: debug.Debugger;

  // execution status variables
  private curJob: WorkerJob | null = null; // should only ever be set by runJob
  private started: boolean = false; // has this worker been run yet, once set true should never be set back to false
  private killed: boolean = false; // has the worker been killed, once set to true should never be set back to false
  private onKilled: (() => void) | null; // used to cancel getNextJobOrDeath

  constructor(pool: WorkerPool) {
    this.pool = pool;
    this._id = this.pool.numWorkers;
    this.debug = debug("workqueue:" + this.pool.id + "-" + this.id);
  }

  get id() {
    return this._id;
  }

  /**
   * Runs a specific job on this worker.
   * @param job the job to execute
   */
  private async runJob(job: WorkerJob) {
    if (this.debug.enabled) this.debug("started job: " + job.task.name);
    let result;
    try {
      this.curJob = job;
      job.status = JobStatus.RUNNING;
      result = {
        value: await job.task.run(this),
      };
    } catch (e) {
      result = {
        error: e,
      };
      console.error(e);
    } finally {
      this.curJob = null;
      job.status = JobStatus.DONE;
      // return the results via the callbacks
      setImmediate(() => {
        for (const callback of job.callbacks) {
          callback(result);
        }
      });
      if (this.debug.enabled) this.debug("finished job: " + job.task.name);
    }
  }

  /**
   * Wait for the provided tasks to finish and returns their results.
   * @param tasks list of tasks to block the worker on
   * @returns list of taskresults in the order of the tasks waited on.
   */
  async awaitResults<T>(tasks: Task<T>[]): Promise<TaskResult<T>[]> {
    this.curJob.blocked = true;

    // spawn an additional worker to this one's place while it is blocked.
    this.debug(
      "awaitResults spawning an extra worker to maintain currency. This worker will be blocked shortly."
    );
    const tmpWorker = this.pool.spawnWorker();
    const tmpWorkerRunPromise = tmpWorker.run();

    // wait on the results via callbacks
    const promises = tasks.map((task) => {
      const job = this.pool.getOrMakeJobForTask(this.curJob.task, task);
      return new Promise((accept, reject) => {
        job.callbacks.push((value) => {
          this.debug("task " + task.name + " reported results.");
          accept(value);
        });
      });
    });

    this.debug("running awaitResults on " + tasks.length + " tasks.");
    const results = (await Promise.all(promises)) as TaskResult<T>[];
    this.debug(
      "unblocked awaitTasks, killing temp worker and awaiting tmp worker run loop exit."
    );
    this.pool.killWorker(tmpWorker);
    await tmpWorkerRunPromise;
    this.debug(
      "awaitResults returning. All results are available and tmp worker has exited."
    );

    return results;
  }

  /**
   * Gets next job or returns null if killed before a job becomes avialable
   */
  private getNextJobOrDeath(): Promise<WorkerJob | null> {
    return new Promise((accept, reject) => {
      let diedFirst = false;

      this.pool
        .getNextJob()
        .then((job) => {
          this.onKilled = null;
          if (diedFirst) {
            this.debug.log("died first, reenqueueing job");
            this.pool.enqueueJob(job);
            return;
          }
          accept(job);
        })
        .catch(reject);

      this.onKilled = () => {
        diedFirst = true;
        accept(null);
      };
    });
  }

  async run() {
    if (this.started) {
      throw new Error("worker has already been started elsewhere");
    }
    this.started = true;

    while (!this.killed) {
      this.debug("worker " + this.id + " waiting for the next job.");
      const job = await this.getNextJobOrDeath();
      if (!job) {
        if (this.debug.enabled)
          this.debug(
            "worker " +
              this.id +
              " exiting -- queue provided null job indicating worker killed or queue exhausted"
          );
        break;
      }
      if (this.debug.enabled)
        this.debug(
          "worker " +
            this.id +
            " pulled job from queue... task name: " +
            job.task.name
        );
      await this.runJob(job);
      if (this.debug.enabled)
        this.debug("worker " + this.id + " finished job: " + job.task.name);
    }
  }

  /**
   * @internal
   * signals to the worker that it should exit. NEVER CALL DIRECTLY, USE pool.killWorker(worker)
   */
  kill() {
    this.debug("received kill signal.");
    this.killed = true;
    if (this.onKilled) {
      setImmediate(this.onKilled.bind(this));
    }
  }

  /**
   * False if the worker has been killed
   * @returns true if the worker has not yet been killed
   */
  isAlive() {
    return !this.killed;
  }

  isBlocked() {
    return this.curJob && this.curJob.blocked;
  }

  isRunningJob() {
    return this.curJob != null;
  }
}
