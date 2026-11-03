import { describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import type { MessageBus } from "../../src/messaging/bus.js";
import { IdempotentConsumer, InMemoryProcessedMessageStore } from "../../src/messaging/idempotency.js";
import { InMemoryBus } from "../../src/messaging/in-memory-bus.js";
import { createMessage } from "../../src/messaging/message.js";
import { InMemoryOutbox, type OutboxEntry } from "../../src/outbox/outbox.js";
import { OutboxRelay } from "../../src/outbox/relay.js";

const entry = (n: number): OutboxEntry => {
  const message = createMessage("payment.capture", `o-${String(n)}`, { orderId: `o-${String(n)}` }) as AnyMessage;
  return { id: `e-${String(n)}`, stream: "commands", message, createdAt: n };
};

describe("OutboxRelay", () => {
  it("publishes pending entries in order and removes them", async () => {
    const outbox = new InMemoryOutbox();
    const bus = new InMemoryBus();
    outbox.append([entry(1), entry(2), entry(3)]);
    const relay = new OutboxRelay(outbox, bus, { batchSize: 2, intervalMs: 10 });
    expect(await relay.flush()).toBe(3);
    expect(bus.messages("commands").map((m) => m.sagaId)).toEqual(["o-1", "o-2", "o-3"]);
    expect(outbox.size).toBe(0);
  });

  it("keeps unpublished entries when the broker fails and retries them later", async () => {
    const outbox = new InMemoryOutbox();
    const inner = new InMemoryBus();
    let failNext = false;
    const flaky: MessageBus = {
      publish: (stream, message) => {
        if (failNext) {
          failNext = false;
          return Promise.reject(new Error("broker unavailable"));
        }
        return inner.publish(stream, message);
      },
      subscribe: (...args) => inner.subscribe(...args),
      subscribeDeadLetters: (...args) => inner.subscribeDeadLetters(...args),
      close: () => inner.close(),
    };
    outbox.append([entry(1), entry(2), entry(3)]);
    const relay = new OutboxRelay(outbox, flaky, { batchSize: 10, intervalMs: 10 });
    expect(await relay.runOnce()).toBe(3);

    outbox.append([entry(4), entry(5)]);
    failNext = true;
    expect(await relay.runOnce()).toBe(0);
    expect(outbox.size).toBe(2);
    expect(await relay.runOnce()).toBe(2);
    expect(outbox.size).toBe(0);
    expect(inner.messages("commands").map((m) => m.sagaId)).toEqual(["o-1", "o-2", "o-3", "o-4", "o-5"]);
  });

  it("re-publishes after a crash between publish and mark; idempotent consumers absorb it", async () => {
    const outbox = new InMemoryOutbox();
    const bus = new InMemoryBus();
    let crashOnMark = true;
    const crashingOutbox = {
      pending: (limit: number) => outbox.pending(limit),
      markPublished: (ids: string[]) => {
        if (crashOnMark) {
          crashOnMark = false;
          return Promise.reject(new Error("process killed"));
        }
        return outbox.markPublished(ids);
      },
    };
    const effects: string[] = [];
    const consumer = new IdempotentConsumer("svc", new InMemoryProcessedMessageStore());
    await bus.subscribe(
      "commands",
      { group: "svc", consumer: "c", maxDeliveries: 3, redeliveryDelayMs: () => 0 },
      async (message) => {
        await consumer.process(message, () => {
          effects.push(message.sagaId);
          return Promise.resolve([]);
        });
      },
    );
    outbox.append([entry(1), entry(2)]);
    const relay = new OutboxRelay(crashingOutbox, bus, { batchSize: 10, intervalMs: 10 });
    await expect(relay.runOnce()).rejects.toThrow("process killed");
    await relay.runOnce();
    await bus.drain();
    expect(bus.messages("commands")).toHaveLength(4);
    expect(effects).toEqual(["o-1", "o-2"]);
  });

  it("runs on an interval until stopped", async () => {
    const outbox = new InMemoryOutbox();
    const bus = new InMemoryBus();
    const relay = new OutboxRelay(outbox, bus, { batchSize: 10, intervalMs: 5 });
    relay.start();
    outbox.append([entry(1)]);
    await new Promise((r) => setTimeout(r, 40));
    await relay.stop();
    expect(bus.messages("commands")).toHaveLength(1);
  });
});
