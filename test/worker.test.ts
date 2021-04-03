import {
  Task,
  TaskResult,
  Worker,
  WorkerPool,
  newLambdaTask,
} from "../src/index";
import expect from "expect";

describe("worker", () => {

  const poolTestHelper = async (numWorkers: number) => {
    // !ERROR this is currently set to 2, should be set to 1
    const workerPool = new WorkerPool(numWorkers);

    let numTasksRun = 0;
    let runningTasks = 0;
    
    const rootTask = newLambdaTask("rootTask", async (worker) => {
      const tasks: Task<boolean>[] = [];
      for (let i = 0; i < 10; ++i) {
        tasks.push(
          newLambdaTask("testTask", async (worker) => {
            console.log("RUNNING TASK!");
            runningTasks++;
            numTasksRun++;
            expect(runningTasks).toBe(1);
            await new Promise((accept) => {
              setTimeout(() => {
                expect(runningTasks).toBe(1);
                runningTasks--;
                accept(true);
              }, 10);
            });
            return true;
          })
        );
      }

      console.log("ROOT TASK IS WAITING FOR RESULTS");
      await worker.awaitResults(tasks);
      console.log("ROOT TASK IS DONE WOOT WOOT!");
    });

    await workerPool.execute(rootTask);

    expect(numTasksRun).toEqual(10);
  }

  it("should be able to queue up jobs with two workers", async () => {
    await poolTestHelper(2);
  });
  it("should be able to queue up jobs with one worker", async () => {
    await poolTestHelper(1);
  });
});
