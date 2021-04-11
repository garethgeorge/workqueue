import {
  JobStatus,
  newLambdaTask,
  Task,
  Worker,
  WorkerJob,
  WorkerPool,
} from "./worker";
import blessed from "blessed";
import { MemoryLoggerFactory } from "./logger";

const debounce = function (
  debounceMs: number,
  callback: (...args: any) => void
) {
  const myself = this;
  let timeout: NodeJS.Timeout | null = null;
  return function () {
    const args = arguments;
    if (timeout) {
      clearTimeout(timeout);
    }
    timeout = setTimeout(() => {
      timeout = null;
      callback.apply(myself, args);
    }, debounceMs);
  };
};

class WorkQueueVisualizer {
  private pool: WorkerPool;

  private screen = blessed.screen({
    smartCSR: true,
    title: "WorkQueue progress",
    cursor: {
      artificial: true,
      shape: "line",
      blink: true,
      color: null, // null for default
    },
  });

  private queueList = blessed.list({
    parent: this.screen,
    label: "Queue",
    top: 0,
    left: 0,
    width: "30%",
    height: "100%",
    border: "line",
    tags: true,
    scrollable: true,
    scrollbar: {
      style: {
        bg: "white",
      },
    },
    keys: true,
    vi: true,
    alwaysScroll: true,
  });

  private workerVisualizers: { [id: string]: WorkerVisualizer } = {};

  constructor(pool: WorkerPool, refreshInterval: number) {
    this.pool = pool;
    this.screen.key(["escape", "q", "C-c"], (ch, key) => {
      this.screen.destroy();
      return process.exit();
    });

    let refreshTimer = setInterval(this.refresh.bind(this), refreshInterval);
    this.pool.onDone.listen(() => {
      this.screen.destroy();
      clearInterval(refreshTimer);
    });

    let debouncedRefresh = debounce(10, this.refresh.bind(this));

    this.pool.onDequeueJob.listen(debouncedRefresh);
    this.pool.onEnqueueJob.listen(debouncedRefresh);
    this.pool.onRemoveJob.listen(debouncedRefresh);
    this.pool.onSpawnWorker.listen(debouncedRefresh);
    this.pool.onKillWorker.listen(debouncedRefresh);
  }

  refresh() {
    const jobToListItem = (job: WorkerJob): string => {
      let jobInfo = `[${job.task.id}] ${job.task.name}`;
      if (job.logger && job.logger.getProgress()) {
        jobInfo += " - %" + Math.round(job.logger.getProgress() * 10) / 10;
      }

      switch (job.status) {
        case JobStatus.RUNNING:
          if (job.blocked) {
            return `{yellow-fg}${jobInfo}{/yellow-fg}`;
          } else {
            return `{cyan-fg}${jobInfo}{/cyan-fg}`;
          }
        case JobStatus.PENDING:
          return `{grey-fg}${jobInfo}{/grey-fg}`;
        case JobStatus.DONE:
          return `{green-fg}${jobInfo}{/green-fg}`;
      }
      return "ERROR INVALID JOB STATUS";
    };
    const listItems: string[] = [];

    this.pool.getWorkers().forEach((worker) => {
      const job = worker.getRunningJob();
      if (job) {
        listItems.push(jobToListItem(job));
      }
    });

    this.pool.getQueuedJobs().forEach((job) => {
      listItems.push(jobToListItem(job));
    });

    if (this.queueList.getScroll() >= listItems.length) {
      this.queueList.setScroll(listItems.length - 1);
    }

    this.queueList.setItems(listItems as any[]);
    this.screen.render();
  }
}

class WorkerVisualizer {
  private worker: Worker;
  private box: blessed.Widgets.BoxElement;
  constructor(worker: Worker, parent: blessed.Widgets.Node) {
    this.worker = worker;
    this.box = blessed.box({
      parent: parent,
      content: "Hello world",
      border: "line",
    });
  }

  getWorker() {
    return this.worker;
  }

  render(xoffset: string, yoffset: string, width: string, height: string) {
    this.box.left = xoffset;
    this.box.top = yoffset;
    this.box.width = width;
    this.box.height = height;
  }

  destroy() {
    this.box.destroy();
  }
}

const awaitTreeHelper = async (
  workerPool: WorkerPool,
  treeDepth: number,
  branchingFactor: number
) => {
  let tasksRun = 0;

  const taskGen = (treeLevel: number, taskNo: number) => {
    return newLambdaTask(
      "taskDepth: " + treeLevel + " taskNo: " + taskNo,
      async (worker, logger) => {
        console.log(logger);
        logger.setProgress(0);
        while (logger.getProgress() < 100) {
          logger.setProgress(logger.getProgress() + 1);
        }
        if (treeLevel === treeDepth) {
          tasksRun++;
          return 1;
        } else {
          let tasks: Task<number>[] = [];
          for (let i = 0; i < branchingFactor; ++i) {
            tasks.push(taskGen(treeLevel + 1, i));
          }
          const results = await worker.awaitResults(tasks);
          return results.reduce((prev, next) => {
            return prev + next;
          }, 0);
        }
      }
    );
  };

  const result = await workerPool.execute(taskGen(0, 0));
};

const workerPool = new WorkerPool(4, new MemoryLoggerFactory());
new WorkQueueVisualizer(workerPool, 50);
awaitTreeHelper(workerPool, 8, 2);
