/**
 * Runs only when REDIS_URL is set (CI starts a Redis service; locally: docker compose up redis).
 * It FLUSHDBs the target database, so point it at a throwaway instance.
 */
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import { RedisStreamsBus } from "../../src/infra/redis/redis-bus.js";
import { RedisProcessedMessageStore } from "../../src/infra/redis/redis-processed-store.js";
import { RedisSagaRepository } from "../../src/infra/redis/redis-saga-repository.js";
import type { DeadLetter } from "../../src/messaging/bus.js";
import { createMessage } from "../../src/messaging/message.js";
import { SagaOrchestrator } from "../../src/saga/orchestrator.js";
import { ConcurrencyError } from "../../src/saga/repository.js";
import { OrchestratorRuntime } from "../../src/runtime/orchestrator-runtime.js";
import { createParticipants } from "../../src/runtime/participants.js";
import { makeOrder } from "../../src/services/fixtures.js";
import { seededRandom } from "../../src/shared/random.js";
import { reliableConfig } from "../support/harness.js";

const url = process.env.REDIS_URL;
const random = seededRandom(5);

async function eventually(check: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("condition not met in time");
}

describe.skipIf(!url)("Redis adapters (integration)", () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis(url ?? "");
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.quit();
  });

  it("consumer groups ack, redeliver from the PEL and dead-letter", async () => {
    const bus = new RedisStreamsBus(redis.duplicate(), { blockMs: 100, reclaimIntervalMs: 50 });
    const stream = `test-${randomUUID()}`;
    const deliveries: number[] = [];
    const ok: string[] = [];
    const letters: DeadLetter[] = [];
    await bus.subscribeDeadLetters("test-dlq", (letter) => {
      letters.push(letter);
      return Promise.resolve();
    });
    await bus.subscribe(
      stream,
      { group: "g", consumer: "c1", maxDeliveries: 3, redeliveryDelayMs: () => 20 },
      (message, ctx) => {
        if (message.sagaId === "poison") {
          deliveries.push(ctx.deliveryCount);
          return Promise.reject(new Error("boom"));
        }
        ok.push(message.sagaId);
        return Promise.resolve();
      },
    );
    await bus.publish(stream, createMessage("payment.capture", "good", { orderId: "good" }));
    await bus.publish(stream, createMessage("payment.capture", "poison", { orderId: "poison" }));

    await eventually(() => Promise.resolve(letters.some((l) => l.message.sagaId === "poison")));
    expect(ok).toEqual(["good"]);
    expect(deliveries).toEqual([1, 2, 3]);
    // The poison message is published to the DLQ first and only then XACKed (so it can never be
    // lost), which means the dead letter can be observed a moment before the PEL drains.
    const pendingCount = async () => ((await redis.call("XPENDING", stream, "g")) as [number, ...unknown[]])[0];
    await eventually(async () => (await pendingCount()) === 0);
    expect(await pendingCount()).toBe(0);
    await bus.close();
  });

  it("saga repository commits atomically with optimistic concurrency and an outbox", async () => {
    const repo = new RedisSagaRepository(redis, { prefix: `t-${randomUUID()}` });
    const orchestrator = new SagaOrchestrator({
      repository: repo,
      stepTimeoutMs: 1_000,
      maxCompensationAttempts: 2,
      compensationBackoff: { baseMs: 10, maxMs: 10 },
    });
    const order = makeOrder(random);
    const state = await orchestrator.start(order);
    expect((await repo.get(order.id))?.version).toBe(1);
    expect((await repo.outbox.pending(10)).map((e) => e.message.type)).toEqual(["order.create"]);
    await expect(
      repo.commit({ state: { ...state, version: 2 }, expectedVersion: 0, outbox: [], processedMessageId: null }),
    ).rejects.toBeInstanceOf(ConcurrencyError);
    expect(await repo.dueForTimeout(Date.now() + 2_000, 10)).toEqual([order.id]);

    const reply = createMessage("order.created", order.id, { orderId: order.id, status: "PAYMENT_PENDING" }, state.pending?.messageId ?? null) as AnyMessage;
    await orchestrator.handle(reply);
    expect(await repo.hasProcessed(reply.id)).toBe(true);
    await orchestrator.handle(reply);
    expect((await repo.get(order.id))?.version).toBe(2);

    const pending = await repo.outbox.pending(10);
    expect(pending.map((e) => e.message.type)).toEqual(["order.create", "payment.authorize"]);
    await repo.outbox.markPublished(pending.map((e) => e.id));
    expect(await repo.outbox.pending(10)).toEqual([]);
  });

  it("processed-message store claims atomically", async () => {
    const store = new RedisProcessedMessageStore(redis, { prefix: `d-${randomUUID()}` });
    expect(await store.claim("k", 1)).toBe(true);
    expect(await store.claim("k", 2)).toEqual({ status: "in_progress", claimedAt: 1 });
    await store.complete("k", []);
    expect(await store.claim("k", 3)).toEqual({ status: "done", replies: [] });
  });

  it("runs the whole saga over Redis Streams", async () => {
    const bus = new RedisStreamsBus(redis.duplicate(), { blockMs: 100, reclaimIntervalMs: 50 });
    const repo = new RedisSagaRepository(redis, { prefix: `e2e-${randomUUID()}` });
    const orchestrator = new SagaOrchestrator({
      repository: repo,
      stepTimeoutMs: 5_000,
      maxCompensationAttempts: 3,
      compensationBackoff: { baseMs: 50, maxMs: 200 },
    });
    const runtime = new OrchestratorRuntime(bus, repo, orchestrator, {
      consumer: "it",
      relayIntervalMs: 10,
      timeoutCheckIntervalMs: 100,
    });
    const participants = createParticipants(reliableConfig, random);
    const subscriptions = await participants.start(bus, new RedisProcessedMessageStore(redis, { prefix: `e2e-d-${randomUUID()}` }), "it");
    await runtime.start();

    const order = makeOrder(random);
    await orchestrator.start(order);
    await eventually(async () => (await orchestrator.get(order.id))?.status === "COMPLETED");
    expect(participants.payments.status(order.id)).toBe("CAPTURED");

    await runtime.stop();
    await Promise.all(subscriptions.map((s) => s.stop()));
    await bus.close();
  });
});
