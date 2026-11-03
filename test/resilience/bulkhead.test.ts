import { describe, expect, it } from "vitest";
import { Bulkhead } from "../../src/resilience/bulkhead.js";
import { BulkheadRejectedError } from "../../src/resilience/errors.js";

describe("Bulkhead", () => {
  it("caps concurrency, queues overflow and rejects beyond the queue", async () => {
    const bulkhead = new Bulkhead({ name: "assignment", maxConcurrent: 2, maxQueue: 1 });
    let running = 0;
    let peak = 0;
    const gates: (() => void)[] = [];
    const task = () =>
      new Promise<void>((resolve) => {
        running++;
        peak = Math.max(peak, running);
        gates.push(() => {
          running--;
          resolve();
        });
      });

    const a = bulkhead.run(task);
    const b = bulkhead.run(task);
    const c = bulkhead.run(task);
    await expect(bulkhead.run(task)).rejects.toBeInstanceOf(BulkheadRejectedError);
    expect(bulkhead.stats).toEqual({ active: 2, queued: 1 });

    gates.shift()?.();
    await a;
    await new Promise((r) => setImmediate(r));
    expect(bulkhead.stats).toEqual({ active: 2, queued: 0 });
    while (gates.length > 0) {
      gates.shift()?.();
      await new Promise((r) => setImmediate(r));
    }
    await Promise.all([b, c]);
    expect(peak).toBe(2);
    expect(bulkhead.stats).toEqual({ active: 0, queued: 0 });
  });

  it("releases the slot when the task throws", async () => {
    const bulkhead = new Bulkhead({ name: "x", maxConcurrent: 1, maxQueue: 0 });
    await expect(bulkhead.run(() => Promise.reject(new Error("nope")))).rejects.toThrow("nope");
    await expect(bulkhead.run(() => Promise.resolve(1))).resolves.toBe(1);
  });
});
