import { Task, MemoryLoggerFactory, WorkerPool, newLambdaTask } from "../src/";
import Debug from "debug";
import expect from "expect";
const debug = Debug("worker.test");

describe("work pool", () => {
  const poolTestHelper = async (workerPool: WorkerPool) => {
    let numWorkers = workerPool.numWorkers;
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
              setTimeout(accept, Math.random() * 10);
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
    expect(result).toBe(Math.pow(branchingFactor, treeDepth));
  };

  it("should be able to queue up jobs with one worker", async () => {
    const workerPool = new WorkerPool(1, new MemoryLoggerFactory());
    await poolTestHelper(workerPool);
  });
  it("should be able to queue up jobs with two workers", async () => {
    const workerPool = new WorkerPool(2, new MemoryLoggerFactory());
    await poolTestHelper(workerPool);
  });

  it("should be able to queue up jobs with four workers", async () => {
    const workerPool = new WorkerPool(4, new MemoryLoggerFactory());
    await poolTestHelper(workerPool);
  });

  it("should be able to create a tree of awaited tasks with one worker", async () => {
    const workerPool = new WorkerPool(1, new MemoryLoggerFactory());
    await awaitTreeHelper(workerPool, 4, 2);
  });

  it("should be able to create a bigger tree of awaited tasks with multiple workers", async () => {
    const workerPool = new WorkerPool(2, new MemoryLoggerFactory());
    await awaitTreeHelper(workerPool, 4, 2);
  });

  it.only("should be able to create a really big and complicated tree with many workers", async () => {
    const workerPool = new WorkerPool(10, new MemoryLoggerFactory());
    await awaitTreeHelper(workerPool, 8, 2);
  });

  describe("a job that throws an error", () => {
    it("should propogate the error to the root", (done) => {
      const workerPool = new WorkerPool(1, new MemoryLoggerFactory());
      workerPool
        .execute(
          newLambdaTask("rootTask", async (worker, logger) => {
            throw new Error("test test test");
          })
        )
        .catch((error) => {
          done();
        });
    });
  });

  // TODO: test error propagation

  it.skip("should be able to run twice", async () => {
    const workerPool = new WorkerPool(10, new MemoryLoggerFactory());
    await awaitTreeHelper(workerPool, 4, 2);
    await awaitTreeHelper(workerPool, 4, 2);
  });
});
