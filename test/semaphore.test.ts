import expect from "expect";
import Semaphore from "../src/lib/semaphore";

/** 
 * TODO: move datastructures and tests into their own libraries
 */
describe("lock", () => {
  it("should only ever allow one entry into the critical section", async () => {
    const lock = new Semaphore(1);
    const entryOrder = [];
    let count = 0;

    const worker = async (index) => {
      await lock.use(async () => {
        entryOrder.push(index);
        count++;
        expect(count).toEqual(1);
        await new Promise((accept) => {
          setTimeout(accept, 1);
        });
        expect(count).toEqual(1);
        count--;
      });
    }

    await Promise.all([worker(1), worker(2), worker(3)]);
    expect(entryOrder).toEqual([1, 2, 3]);
  });
});