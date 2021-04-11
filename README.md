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

# TODO
 - split out lib/queue and lib/semaphore into a separate package for synchronization
 - note: there is a bug. rather than signaling for a specific worker to be killed when a task wishes to reawaken, it needs to just signal that some worker that be killed in order to unblock. This should be whatever worker next tries to block to get a task. 
 - I can think of a few ways to do this but i'll leave the implementation to tomorrow.