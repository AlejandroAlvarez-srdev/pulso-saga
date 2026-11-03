import { describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import { InMemoryBus } from "../../src/messaging/in-memory-bus.js";
import { createMessage } from "../../src/messaging/message.js";

const msg = (orderId: string): AnyMessage => createMessage("payment.capture", orderId, { orderId });

const options = { consumer: "c1", maxDeliveries: 3, redeliveryDelayMs: () => 0 };

describe("InMemoryBus", () => {
  it("delivers every message to each consumer group", async () => {
    const bus = new InMemoryBus();
    const a: string[] = [];
    const b: string[] = [];
    await bus.subscribe("s", { ...options, group: "a" }, (m) => {
      a.push(m.sagaId);
      return Promise.resolve();
    });
    await bus.subscribe("s", { ...options, group: "b" }, (m) => {
      b.push(m.sagaId);
      return Promise.resolve();
    });
    await bus.publish("s", msg("1"));
    await bus.publish("s", msg("2"));
    await bus.drain();
    expect(a).toEqual(["1", "2"]);
    expect(b).toEqual(["1", "2"]);
  });

  it("delivers the backlog to a group created after messages were published", async () => {
    const bus = new InMemoryBus();
    await bus.publish("s", msg("early"));
    const seen: string[] = [];
    await bus.subscribe("s", { ...options, group: "late" }, (m) => {
      seen.push(m.sagaId);
      return Promise.resolve();
    });
    await bus.drain();
    expect(seen).toEqual(["early"]);
  });

  it("redelivers failed messages and dead-letters them after maxDeliveries", async () => {
    const bus = new InMemoryBus();
    const deliveries: number[] = [];
    const letters: string[] = [];
    await bus.subscribeDeadLetters("ops", (letter) => {
      letters.push(`${letter.message.sagaId}:${letter.deliveryCount}:${letter.error}`);
      return Promise.resolve();
    });
    await bus.subscribe("s", { ...options, group: "g" }, (_m, ctx) => {
      deliveries.push(ctx.deliveryCount);
      return Promise.reject(new Error("downstream down"));
    });
    await bus.publish("s", msg("poison"));
    await bus.drain();
    expect(deliveries).toEqual([1, 2, 3]);
    expect(letters).toEqual(["poison:3:downstream down"]);
    expect(bus.deadLettered).toHaveLength(1);
  });

  it("succeeds on redelivery without dead-lettering", async () => {
    const bus = new InMemoryBus();
    let calls = 0;
    await bus.subscribe("s", { ...options, group: "g" }, () => {
      calls++;
      return calls === 1 ? Promise.reject(new Error("blip")) : Promise.resolve();
    });
    await bus.publish("s", msg("x"));
    await bus.drain();
    expect(calls).toBe(2);
    expect(bus.deadLettered).toHaveLength(0);
  });

  it("can inject duplicate deliveries", async () => {
    const bus = new InMemoryBus({ duplicateRate: 1 });
    let calls = 0;
    await bus.subscribe("s", { ...options, group: "g" }, () => {
      calls++;
      return Promise.resolve();
    });
    await bus.publish("s", msg("dup"));
    await bus.drain();
    expect(calls).toBe(2);
  });
});
