import { MemoryLoggerFactory } from "./logger";
import { newLambdaTask, Task, WorkerPool } from "./worker";
import WorkQueueVisualizer from "./terminal";

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
        if (treeLevel === treeDepth) {
          logger.setProgress(0);
          while (logger.getProgress() < 100) {
            logger.setProgress(logger.getProgress() + 10);
            await new Promise((accept, reject) => {
              setTimeout(accept, Math.random() * 100);
            });
          }
          logger.writableStream().write("Done " + taskNo);
          tasksRun++;
          return 1;
        } else {
          await new Promise((accept) => {
            setTimeout(accept, 200);
          });
          let tasks: Task<number>[] = [];
          for (let i = 0; i < branchingFactor; ++i) {
            tasks.push(taskGen(treeLevel + 1, i));
          }
          const results = await worker.awaitResults(tasks, logger);
          return results.reduce((prev, next) => {
            return prev + next;
          }, 0);
        }
      }
    );
  };

  const result = await workerPool.execute(taskGen(0, 0));
  console.log(result);
};

const workerPool = new WorkerPool(2, new MemoryLoggerFactory());
new WorkQueueVisualizer(workerPool, 100);
awaitTreeHelper(workerPool, 16, 5);
