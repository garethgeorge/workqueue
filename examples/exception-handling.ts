import { MemoryLoggerFactory } from "../src/logger";
import { newLambdaTask, Task, WorkerPool } from "../src/worker";
import { TerminalVisualizer } from "../src/";

const awaitTreeHelper = async (
  workerPool: WorkerPool,
  treeDepth: number,
  branchingFactor: number
) => {
  const taskGen = (treeLevel: number, taskNo: number) => {
    return newLambdaTask(
      "taskDepth: " + treeLevel + " taskNo: " + taskNo,
      async (worker, logger) => {
        await new Promise((accept) => {
          setImmediate(accept);
        });

        if (treeLevel === treeDepth) {
          logger.setProgress(0);
          throw new Error("FAILURE");
        } else {
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

  workerPool.execute(taskGen(0, 0)).catch((e) => {
    console.log("CAUGHT AN ERROR HERE: " + e.toString());
  });
};

(async () => {
  const workerPool = new WorkerPool(4, new MemoryLoggerFactory());
  // let termViz = new TerminalVisualizer(workerPool, 100);
  try {
    await awaitTreeHelper(workerPool, 4, 2);
  } catch (e) {
    console.log("FOOBAR: " + e);
    process.exit(2);
  }
})();
