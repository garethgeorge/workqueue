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
  }

  it("should be able to queue up jobs with one worker", async () => {
    await poolTestHelper(1);
  });
  it("should be able to queue up jobs with two workers", async () => {
    await poolTestHelper(2);
  });
  it("should be able to queue up jobs with four workers", async () => {
    await poolTestHelper(4);
  });
});
