import { Queue, Semaphore } from "synchronization-js";
import debug from "debug";
import { randomBytes } from "crypto";

import { Logger, LoggerFactory } from "./logger";
import EventEmitter from "./lib/eventemitter";

/**
 * An individual unit of work
 */
export interface Task<T> {
  id: string; // must be a unique string
  name: string;
  run: (worker: Worker, logger: Logger) => Promise<T>;
}

/**
 * Utility function to create a task with a given name from an anonymous asynchronous function.
 * @param name name for the generated task
 * @param func asynchronous function to execute
 * @returns a newly constructed task with the specified name and function
 */
export const newLambdaTask = <T>(
  name: string,
  func: (worker: Worker, logger: Logger) => Promise<T>
) => {
  return {
    id: randomBytes(8).toString("hex"),
    name: name,
    run: func,
  } as Task<T>;
};

export enum JobStatus {
  PENDING = "pending",
  RUNNING = "running",
  DONE = "done",
}

/**
 * Wrapper of a task that includes its metadata and execution status
 */
export class WorkerJob {
  task: Task<any>;
  children: Task<any>[];
  onResult: EventEmitter<any>;
  onError: EventEmitter<Error>;
  onUpdate: EventEmitter<void>; // status change or blocked
  priority: number;
  status: JobStatus;
  blocked: boolean;
  logger: Logger | null;
}

/**
 * NOTE: need to provide a mechanism for hooking into this process.
 */
let nextPoolId = 0;
export class WorkerPool {
  private _id: number = nextPoolId++;
  private loggerFactory: LoggerFactory;
  private workers: Worker[] = [];
  private workerJobs: { [id: string]: WorkerJob } = {};
  private rootJob: WorkerJob | null = null;

  // only one root task can be executed on a workerpool at a time.
  private executeExclusion: Semaphore = new Semaphore(1);
  private availableJobs: Semaphore = new Semaphore(0);
  private queues: Queue<WorkerJob>[] = [];
  private debug = debug("workqueue:pool");
  private killWorkerCallbacks: ((worker: Worker) => void)[] = [];

  public onDequeueJob = new EventEmitter<WorkerJob>(); // job fetched from queue by worker
  public onEnqueueJob = new EventEmitter<WorkerJob>(); // job added to the queue
  public onRemoveJob = new EventEmitter<WorkerJob>(); // job completed
  public onSpawnWorker = new EventEmitter<Worker>(); // worker started
  public onKillWorker = new EventEmitter<Worker>(); // worker killed but not necessarily dead yet.
  public onDone = new EventEmitter<void>(); // worker done.

  constructor(numWorkers: number, loggerFactory: LoggerFactory) {
    this.loggerFactory = loggerFactory;

    for (let i = 0; i < numWorkers; ++i) {
      this.spawnWorker();
    }
    // push the initial queue, there is always at least one
    this.queues.push(new Queue());
  }

  get id() {
    return this._id;
  }

  get numWorkers() {
    return this.workers.length;
  }

  spawnWorker() {
    const newWorker = new Worker(this, this.loggerFactory);
    this.workers.push(newWorker);
    this.onSpawnWorker.emit(newWorker);
    return newWorker;
  }

  async killWorker() {
    if (this.workers.length - this.killWorkerCallbacks.length < 0) {
      throw new Error("No workers left to kill.");
    }

    this.availableJobs.V();
    const killedWorker = (await new Promise((accept) => {
      this.killWorkerCallbacks.push(accept);
    })) as Worker;

    this.workers = this.workers.filter((worker) => {
      return worker !== killedWorker;
    });

    this.onKillWorker.emit(killedWorker);

    return killedWorker;
  }

  // TODO: should we remove the return value from this function?
  async execute<T>(rootTask: Task<T>): Promise<T> {
    await this.executeExclusion.P();
    const newRootJob: WorkerJob = {
      task: rootTask,
      priority: 0,
      children: [],
      status: JobStatus.PENDING,
      blocked: false,
      onResult: new EventEmitter(),
      onError: new EventEmitter(),
      onUpdate: new EventEmitter(),
      logger: null,
    };
    this.rootJob = newRootJob;
    this.workerJobs[newRootJob.task.id] = newRootJob;
    this.queues[0].enqueue(newRootJob);
    this.availableJobs.V(); // increment to indicate a job is available

    const workersCompleted = [];
    for (const worker of this.workers) {
      workersCompleted.push(worker.run());
    }

    try {
      this.debug("pool.execute awaiting result of root task");
      return await new Promise((accept, reject) => {
        newRootJob.onResult.listen(accept);
        newRootJob.onError.listen(reject);
      });
    } finally {
      this.debug("pool has detected that the root task exited.");

      // signal to kill the workers when the root task completes
      for (const worker of this.workers) {
        this.debug("killing a worker");
        await this.killWorker();
      }

      this.availableJobs = new Semaphore(0);

      this.debug("waiting for all workers to exit.");
      await Promise.all(workersCompleted);

      if (
        this.getQueuedJobs().length !== 0 ||
        Object.values(this.workerJobs).length !== 0
      ) {
        console.log(this.queues[0].size());
        console.log(this.workerJobs);
        throw new Error("expected queues to be empty at the end of the run");
      }

      this.rootJob = null;
      this.executeExclusion.V();
      this.onDone.emit();
    }
  }

  getRootJob(): WorkerJob | null {
    return this.rootJob;
  }

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
      onError: new EventEmitter(),
      onResult: new EventEmitter(),
      onUpdate: new EventEmitter(),
      logger: null,
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
    this.onEnqueueJob.emit(job);
  }

  /**
   * Unregisters a given task, should only be called when the task is done as this function will not
   * remove the task from the queue.
   * @param task the task to remove the task -> job mapping for
   */
  removeTask(task: Task<any>) {
    delete this.workerJobs[task.id];
  }

  /**
   * Returns the job for the task or null if none exists.
   * @param task the task we want to find job metadata for or enqueue if it does not exist
   * @returns job metadata associated with the provided task
   */
  getJobForTask(task: Task<any>): WorkerJob | null {
    return this.workerJobs[task.id] || null;
  }

  async getNextJob(worker: Worker) {
    await this.availableJobs.P();

    if (this.killWorkerCallbacks.length > 0) {
      // we awoke to kill a worker.
      this.killWorkerCallbacks.pop()(worker);
      return null;
    }

    let queue = this.queues[this.queues.length - 1];
    while (queue.size() === 0 && this.queues.length > 1) {
      this.queues.pop();
      queue = this.queues[this.queues.length - 1];
    }

    return queue.dequeue();
  }

  /**
   * Get the list of queued jobs in order from highest priority to lowest priority
   * @returns array of queued jobs.
   */
  getQueuedJobs() {
    const jobs: WorkerJob[] = [];
    for (let i = this.queues.length - 1; i >= 0; --i) {
      jobs.push.apply(jobs, this.queues[i].toArray());
    }
    return jobs;
  }

  /**
   * Get the list of workers
   */
  getWorkers() {
    return this.workers;
  }
}

export class Worker {
  /*
    worker represents one thread of execution
    worker maintains a stack of jobs, only one job in the stack should ever be unblocked
  */

  // worker info
  private _id: number;
  private pool: WorkerPool;
  private loggerFactory: LoggerFactory;
  private debug: debug.Debugger;

  // execution status variables
  private curJob: WorkerJob | null = null; // should only ever be set by runJob
  private started: boolean = false; // has this worker been run yet, once set true should never be set back to false
  private killed: boolean = false; // has the worker been killed, once set to true should never be set back to false

  public onRunJob = new EventEmitter<WorkerJob>();

  constructor(pool: WorkerPool, loggerFactory: LoggerFactory) {
    this.pool = pool;
    this._id = this.pool.numWorkers;
    this.debug = debug("workqueue:" + this.pool.id + "-" + this.id);
    this.loggerFactory = loggerFactory;
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
    await new Promise((accept) => {
      setImmediate(accept);
    });

    try {
      this.curJob = job;
      job.logger = this.loggerFactory.createLogger(job.task);
      job.status = JobStatus.RUNNING;
      job.onUpdate.emit();
      this.onRunJob.emit(job);
      let result = await job.task.run(this, job.logger);
      // return the results via the callbacks
      setImmediate(() => {
        job.onResult.emit(result);
      });
      return result;
    } catch (e) {
      setImmediate(() => {
        job.logger.writableStream().write("" + e);
        job.onError.emit(e);
      });
    } finally {
      this.pool.removeTask(job.task);
      this.curJob = null;
      job.status = JobStatus.DONE;
      job.onUpdate.emit();

      if (this.debug.enabled) this.debug("finished job: " + job.task.name);
    }
  }

  /**
   * Wait for the provided tasks to finish and returns their results.
   * @param tasks list of tasks to block the worker on
   * @returns list of taskresults in the order of the tasks waited on.
   */
  public async awaitResults<T>(
    tasks: Task<T>[],
    logger: Logger | null = null
  ): Promise<T[]> {
    this.curJob.blocked = true;
    this.curJob.onUpdate.emit();
    if (logger) {
      logger.setProgress(0);
    }

    // spawn an additional worker to this one's place while it is blocked.
    this.debug(
      "awaitResults spawning an extra worker to maintain currency. This worker will be blocked shortly."
    );
    this.pool.spawnWorker().run();

    try {
      // wait on the results via callbacks
      const promises = tasks.map((task) => {
        // find the task or create one if none exists.
        let job = this.pool.getJobForTask(task);
        if (!job) {
          job = this.pool.enqueueTask(this.curJob.task, task);
        }

        // create a callback awaiting the completion of the job.
        return new Promise((accept, reject) => {
          if (this.debug) {
            let oldAccept = accept;
            let oldReject = reject;
            accept = (value) => {
              this.debug("task " + task.name + " provided results.");
              oldAccept(value);
            };
            reject = (value) => {
              this.debug("task " + task.name + " errored.");
              oldReject(value);
            };
          }
          if (logger) {
            job.onResult.listen((value) => {
              logger.setProgress(
                Math.min(100, logger.getProgress() + 100 / tasks.length)
              );
              accept(value);
            });
          } else job.onResult.listen(accept);
          job.onError.listen(reject);
        });
      });

      this.debug("running awaitResults on " + tasks.length + " tasks.");
      return (await Promise.all(promises)) as T[];
    } catch (e) {
      console.log("CAUGHT AN EXCEPTION: " + e);
      throw e;
    } finally {
      this.debug(
        "unblocked awaitTasks, killing temp worker and awaiting tmp worker run loop exit."
      );
      await this.pool.killWorker();
      this.curJob.blocked = false;
      this.curJob.onUpdate.emit();
      this.debug(
        "awaitResults returning. All results are available and tmp worker has exited."
      );
    }
  }

  async run() {
    if (this.started) {
      throw new Error("worker has already been started elsewhere");
    }
    this.started = true;

    while (true) {
      this.debug("worker " + this.id + " waiting for the next job.");
      const job = await this.pool.getNextJob(this);
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
   * False if the worker has been killed
   * @returns true if the worker has not yet been killed
   */
  isAlive() {
    return !this.killed;
  }

  getRunningJob() {
    return this.curJob;
  }
}
