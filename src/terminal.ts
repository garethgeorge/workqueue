import { JobStatus, newLambdaTask, Task, Worker, WorkerPool } from "./worker";
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
  });

  private workersContainer = blessed.box({
    parent: this.screen,
    top: 0,
    left: "30%",
    width: "30%",
    keys: true,
    vi: true,
    alwaysScroll: true,
    scrollable: true,
    scrollbar: {
      style: {
        bg: "yellow",
      },
    },
  });

  private workerVisualizers: { [id: string]: WorkerVisualizer } = {};

  constructor(pool: WorkerPool, refreshInterval: number) {
    this.pool = pool;

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
    this.screen.render();
  }

  refresh() {
    this.queueList.setItems(
      this.pool.getQueuedJobs().map((workerJob, index) => {
        return (
          index + 1 + ") " + workerJob.task.id + ": " + workerJob.task.name
        );
      }) as any[]
    );
    this.queueList.render();
    // setup the worker visualizers
    const workers = this.pool.getWorkers();
    let workersSet: { [id: string]: boolean } = {};
    for (const worker of workers) {
      workersSet[worker.id] = true;
      if (!this.workerVisualizers[worker.id]) {
        this.workerVisualizers[worker.id] = new WorkerVisualizer(
          worker,
          this.workersContainer
        );
      }
    }
    for (const workerVisualizer of Object.values(this.workerVisualizers)) {
      const worker = workerVisualizer.getWorker();
      if (!workersSet[worker.id]) {
        this.workerVisualizers[worker.id].destroy();
        delete this.workerVisualizers[worker.id];
      }
    }

    const visualizers = Object.values(this.workerVisualizers);
    let height = 10;
    let yoffset = 0;
    for (const visualizer of visualizers) {
      visualizer.render("0", "" + yoffset, "90%", "" + height);
      yoffset += height;
    }
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
      "taskDepth: " + treeDepth + " taskNo: " + taskNo,
      async (worker) => {
        if (treeLevel === treeDepth) {
          await new Promise((accept) => {
            setTimeout(accept, 1000000);
          });
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

const workerPool = new WorkerPool(2, new MemoryLoggerFactory());
new WorkQueueVisualizer(workerPool, 1000);
awaitTreeHelper(workerPool, 2, 2);
