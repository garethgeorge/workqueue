import { JobStatus, Worker, WorkerJob, WorkerPool } from "./worker";
import blessed from "blessed";

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

const jobToListItem = (job: WorkerJob): string => {
  let jobInfo = `[${job.task.id}] ${job.task.name}`;
  if (job.logger && job.logger.getProgress() != null) {
    jobInfo += " - %" + Math.round(job.logger.getProgress() * 10) / 10;
  }
  jobInfo = blessed.escape(jobInfo);

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

export default class WorkQueueVisualizer {
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

  private queueList = blessed.box({
    parent: this.screen,
    label: "Task Queue",
    top: 0,
    left: 0,
    width: "50%",
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

  private logBox = blessed.layout({
    parent: this.screen,
    label: "Worker Logs",
    top: 0,
    right: 0,
    width: "50%",
    height: "100%",
    layout: "inline-block",
  });

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
    this.pool.onSpawnWorker.listen((worker) => {
      debouncedRefresh();
      worker.onRunJob.listen((workerJob) => {
        if (workerJob.logger) {
          new JobLogVisualizer(this.logBox, workerJob);
        }
      });
    });
    this.pool.onKillWorker.listen(debouncedRefresh);
  }

  refresh() {
    try {
      this.pool.getRootJob();

      const listItems: string[] = [];

      // RENDER JOBS FROM THE TREE
      if (this.pool.getRootJob()) {
        const walk = (job: WorkerJob) => {
          if (!job) {
            return;
          }
          let prefix = "";
          for (let i = 0; i < job.priority; ++i) {
            prefix += " ";
          }

          listItems.push(prefix + jobToListItem(job));
          for (const child of job.children) {
            walk(this.pool.getJobForTask(child));
          }
        };

        walk(this.pool.getRootJob());
      }

      /*
      // RENDER JOBS FROM THE QUEUE
      this.pool.getWorkers().forEach((worker) => {
        const job = worker.getRunningJob();
        if (job) {
          listItems.push(jobToListItem(job));
        }
      });

      this.pool.getQueuedJobs().forEach((job) => {
        listItems.push(jobToListItem(job));
      });
      */

      // if (this.queueList.getScroll() >= listItems.length) {
      //   this.queueList.setScroll(listItems.length - 1);
      // }

      this.queueList.setContent(listItems.join("\n"));
      this.screen.render();
    } catch (e) {
      this.screen.destroy();
      console.error(e);
    }
  }
}

class JobLogVisualizer {
  private job: WorkerJob;
  private box: blessed.Widgets.BoxElement;
  private onUpdateListener: () => void;

  constructor(parent: blessed.Widgets.Node, job: WorkerJob) {
    let heightLines = 10;
    this.job = job;
    this.box = blessed.box({
      label: jobToListItem(job),
      tags: true,
      top: 0,
      right: 0,
      width: "100%",
      height: "10%",
      border: "line",
    });
    parent.prepend(this.box);

    this.onUpdateListener = job.onUpdate.listen(() => {
      this.box.setLabel(jobToListItem(job));
      this.box.render();
    });

    const onComplete = () => {
      setTimeout(() => {
        this.destroy();
      }, 500);
    };

    job.onResult.listen(onComplete);
    job.onError.listen(onComplete);

    job.logger.readableStream().on("data", (data) => {
      let content = this.box.getText().split("\n");
      if (content.length > heightLines * 2) {
        content = content.slice(0, heightLines * 2);
      }
      this.box.setText(data + "\n" + content.join("\n"));
    });
  }

  destroy() {
    this.box.destroy();
    this.job.onUpdate.removeListener(this.onUpdateListener);
  }
}
