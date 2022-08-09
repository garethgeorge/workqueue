# WorkQueue
A lightweight library for executing simple asynchronous workqueues with tree structured dependencies.

NOTE: more complex DAG like dependency structures can be constructed programatically and are supported but are not the primary goal of this library.

```js
const pool = new WorkQueue(newLambdaTask(async (worker) => {
  // step 1: construct some set of child tasks
  // step 2: get results of those tasks with worker.getResults()
  // step 3: evaluate the results and return
}));
```
