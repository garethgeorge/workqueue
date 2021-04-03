export default class Queue<T> {
  private items: T[] = [];
  private offset: number = 0;

  enqueue(item: T): T {
    this.items.push(item);
    return item;
  }

  dequeue(): T | undefined {
    if (this.items.length === 0) {
      return undefined;
    }

    const item = this.items[this.offset];
    this.offset++;
    if (this.offset * 2 >= this.items.length) {
      this.items = this.items.slice(this.offset);
      this.offset = 0;
    }
    return item;
  }

  peek(): T | undefined {
    if (this.items.length === 0) return undefined;
    return this.items[this.offset];
  }

  size(): number {
    return this.items.length - this.offset;
  }

  isEmpty() {
    return this.size() === 0;
  }

  clear() {
    this.items = [];
  }
}

export class AsyncQueue<T> extends Queue<T> {
  private awaiters: Queue<(item: T) => void> = new Queue();

  enqueue(item: T): T {
    const awaiter = this.awaiters.dequeue();
    if (awaiter !== undefined) {
      setImmediate(() => {
        awaiter(item);
      });
    } else
      super.enqueue(item);
    return item;
  }

  dequeueOrWait(): Promise<T> {
    const item = this.dequeue();
    if (!item) {
      return new Promise((accept) => {
        this.awaiters.enqueue(accept);
      });
    } else {
      return new Promise((accept) => {
        accept(item);
      });
    }
  }
}
