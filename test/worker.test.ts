import {
  Task,
  TaskResult,
  Worker,
  WorkerPool,
  newLambdaTask,
} from "../src/index";
import Debug from "debug";
import expect from "expect";
const debug = Debug("worker.test");

describe("worker", () => {
  const poolTestHelper = async (numWorkers: number) => {
    // !ERROR this is currently set to 2, should be set to 1
    const workerPool = new WorkerPool(numWorkers);

    let numTasksRun = 0;
    let runningTasks = 0;

    const rootTask = newLambdaTask("rootTask", async (worker) => {
      const tasks: Task<boolean>[] = [];
      for (let i = 0; i < 10; ++i) {
        ((i) => {
          tasks.push(
            newLambdaTask("testTask" + i, async (worker) => {
              debug("RUNNING TASK! " + i);
              runningTasks++;
              numTasksRun++;
              expect(runningTasks).toBeLessThan(numWorkers + 1);
              await new Promise((accept, reject) => {
                setTimeout(() => {
                  expect(runningTasks).toBeLessThan(numWorkers + 1);
                  runningTasks--;
                  accept(true);
                }, Math.random() * 10 + 10);
              });
              return true;
            })
          );
        })(i);
      }

      debug("ROOT TASK IS WAITING FOR RESULTS");
      await worker.awaitResults(tasks);
      debug("ROOT TASK IS DONE WOOT WOOT!");
    });

    await workerPool.execute(rootTask);

    expect(numTasksRun).toEqual(10);
  };

  const awaitTreeHelper = async (
    numWorkers: number,
    treeDepth: number,
    branchingFactor: number
  ) => {
    const workerPool = new WorkerPool(numWorkers);

    const taskGen = (treeLevel: number, taskNo: number) => {
      return newLambdaTask(
        "taskDepth: " + treeDepth + " taskNo: " + taskNo,
        async (worker) => {
          if (treeLevel === treeDepth) {
            await new Promise((accept) => {
              setTimeout(accept, Math.random() * 10);
            });
            return true;
          } else {
            let tasks: Task<boolean>[] = [];
            for (let i = 0; i < branchingFactor; ++i) {
              tasks.push(taskGen(treeLevel + 1, i));
            }
            await worker.awaitResults(tasks);
          }
        }
      );
    };

    await workerPool.execute(taskGen(0, 0));
  };

  it("should be able to queue up jobs with one worker", async () => {
    await poolTestHelper(1);
  });
  it("should be able to queue up jobs with two workers", async () => {
    await poolTestHelper(2);
  });
  it("should be able to queue up jobs with four workers", async () => {
    await poolTestHelper(4);
  });

  it("should be able to create a tree of awaited tasks with one worker", async () => {
    await awaitTreeHelper(1, 4, 2);
  });

  it("should be able to create a bigger tree of awaited tasks with multiple workers", async () => {
    await awaitTreeHelper(2, 4, 2);
  });

  it("should be able to create a really big and complicated tree with many workers", async () => {
    await awaitTreeHelper(10, 8, 2);
  });
});
