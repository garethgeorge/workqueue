import expect from "expect";
import Queue, { AsyncQueue } from "../src/lib/queue";

describe("queue", () => {
  let q: Queue<number> | undefined;
  beforeEach(() => {
    q = new Queue();
  });

  it("should be empty when initialized", () => {
    expect(q.isEmpty()).toBeTruthy();
    expect(q.size()).toEqual(0);
  });

  it("should enqueue and dequeue in order", () => {
    const array = [];
    for (let i = 0; i < 100; ++i) {
      array.push(i);
      q.enqueue(i);
    }

    const values = [];
    for (let i = 0; i < 100; ++i) {
      values.push(q.dequeue());
    }
    expect(values).toEqual(array);
  });
});

describe("async queue", () => {
  let q: AsyncQueue<number> | undefined;

  beforeEach(() => {
    q = new AsyncQueue();
  });

  it("should enqueue and dequeue in order", () => {
    const array = [];
    for (let i = 0; i < 100; ++i) {
      array.push(i);
      q.enqueue(i);
    }

    const values = [];
    for (let i = 0; i < 100; ++i) {
      values.push(q.dequeue());
    }
    expect(values).toEqual(array);
  });

  it("should unblock queue when a value is added", async () => {
    setImmediate(async () => {
      for (let i = 0; i < 10; ++i) {
        q.enqueue(i);
        await new Promise((accept) => setImmediate(accept));
      }

      for (let i = 10; i < 20; ++i) {
        q.enqueue(i);
      }
    });

    expect(q.dequeue()).toBe(undefined);
    for (let i = 0; i < 20; ++i) {
      expect(await q.dequeueOrWait()).toBe(i);
    }
  });
})