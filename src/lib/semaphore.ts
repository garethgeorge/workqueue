import Queue from "./queue";

export default class Semaphore {
  private count: number;
  private awaiters: Queue<() => void> = new Queue;

  constructor(initialValue: number) {
    this.count = initialValue;
  }

  /**
   * Decrement the semaphore
   */
  async P() {
    this.count--;
    if (this.count < 0) {
      await new Promise((accept) => {
        this.awaiters.enqueue(accept as () => void);
      });
    }
  }

  /**
   * Increment the semaphore
   */
  V() {
    this.count++;
    if (this.count <= 0) {
      setImmediate(this.awaiters.dequeue());
    }
  }

  async use(thunk: () => Promise<void>) {
    try {
      await this.P();
      await thunk();
    } finally {
      this.V();
    }
  }

  value() {
    return this.count;
  }
}