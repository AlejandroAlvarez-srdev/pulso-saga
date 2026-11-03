import { describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import {
  DuplicateInFlightError,
  IdempotentConsumer,
  InMemoryProcessedMessageStore,
} from "../../src/messaging/idempotency.js";
import { InMemoryBus } from "../../src/messaging/in-memory-bus.js";
import { createMessage } from "../../src/messaging/message.js";

const command: AnyMessage = createMessage("payment.release", "order-1", { orderId: "order-1" });

describe("IdempotentConsumer", () => {
  it("runs the side effect once and replays the cached replies for duplicates", async () => {
    const consumer = new IdempotentConsumer("payments", new InMemoryProcessedMessageStore());
    let effects = 0;
    const handle = () => {
      effects++;
      return Promise.resolve([
        createMessage("payment.released", "order-1", { orderId: "order-1", noop: false }, command.id),
      ]);
    };
    const first = await consumer.process(command, handle);
    const second = await consumer.process(command, handle);
    const third = await consumer.process({ ...command }, handle);
    expect(effects).toBe(1);
    expect(first.duplicate).toBe(false);
    expect(second.duplicate).toBe(true);
    expect(third.duplicate).toBe(true);
    expect(second.replies.map((r) => r.id)).toEqual(first.replies.map((r) => r.id));
  });

  it("rejects a concurrent duplicate while the first delivery is still running", async () => {
    const consumer = new IdempotentConsumer("payments", new InMemoryProcessedMessageStore());
    let finish: () => void = () => undefined;
    const slow = consumer.process(
      command,
      () =>
        new Promise<AnyMessage[]>((resolve) => {
          finish = () => {
            resolve([]);
          };
        }),
    );
    await expect(consumer.process(command, () => Promise.resolve([]))).rejects.toBeInstanceOf(DuplicateInFlightError);
    finish();
    await slow;
  });

  it("releases the claim on failure so a redelivery can retry", async () => {
    const consumer = new IdempotentConsumer("payments", new InMemoryProcessedMessageStore());
    await expect(consumer.process(command, () => Promise.reject(new Error("503")))).rejects.toThrow("503");
    const retried = await consumer.process(command, () => Promise.resolve([]));
    expect(retried.duplicate).toBe(false);
  });

  it("keeps effects exactly-once under a bus that duplicates every delivery", async () => {
    const bus = new InMemoryBus({ duplicateRate: 1 });
    const consumer = new IdempotentConsumer("svc", new InMemoryProcessedMessageStore());
    const effects = new Map<string, number>();
    await bus.subscribe(
      "commands",
      { group: "svc", consumer: "c", maxDeliveries: 5, redeliveryDelayMs: () => 1, concurrency: 4 },
      async (message) => {
        await consumer.process(message, () => {
          effects.set(message.id, (effects.get(message.id) ?? 0) + 1);
          return Promise.resolve([]);
        });
      },
    );
    const messages = Array.from(
      { length: 25 },
      (_, i) => createMessage("payment.capture", `o-${String(i)}`, { orderId: `o-${String(i)}` }) as AnyMessage,
    );
    for (const m of messages) await bus.publish("commands", m);
    await bus.drain();
    expect(effects.size).toBe(25);
    expect([...effects.values()].every((n) => n === 1)).toBe(true);
    expect(bus.deadLettered).toHaveLength(0);
  });
});
