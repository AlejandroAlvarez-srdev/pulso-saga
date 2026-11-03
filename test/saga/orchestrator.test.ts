import { describe, expect, it } from "vitest";
import type { AnyMessage, EventPayloads, EventType } from "../../src/domain/messages.js";
import { createMessage } from "../../src/messaging/message.js";
import { SagaOrchestrator } from "../../src/saga/orchestrator.js";
import { ConcurrencyError, InMemorySagaRepository } from "../../src/saga/repository.js";
import type { SagaState } from "../../src/saga/state.js";
import { makeOrder } from "../../src/services/fixtures.js";
import { ManualClock } from "../../src/shared/clock.js";
import { seededRandom } from "../../src/shared/random.js";

/**
 * Drives the orchestrator by hand (no participants) to exercise the edge cases that are hard
 * to time with real services: late replies, stale replies, duplicates and stuck compensations.
 */
function setup() {
  const clock = new ManualClock(Date.parse("2026-11-03T12:00:00Z"));
  const repository = new InMemorySagaRepository();
  const orchestrator = new SagaOrchestrator({
    repository,
    clock,
    stepTimeoutMs: 1_000,
    maxCompensationAttempts: 3,
    compensationBackoff: { baseMs: 100, maxMs: 1_000, jitter: "none" },
  });
  const order = makeOrder(seededRandom(3));

  const state = async (): Promise<SagaState> => {
    const s = await orchestrator.get(order.id);
    if (!s) throw new Error("saga not found");
    return s;
  };
  const pendingId = async () => (await state()).pending?.messageId ?? "none";
  const replyTo = <T extends EventType>(causationId: string, type: T, payload: EventPayloads[T]): AnyMessage =>
    createMessage(type, order.id, payload as never, causationId) as AnyMessage;
  const sentTypes = async () => (await repository.outbox.pending(100)).map((e) => e.message.type);

  return { clock, repository, orchestrator, order, state, pendingId, replyTo, sentTypes };
}

describe("SagaOrchestrator edge cases", () => {
  it("ignores duplicate replies (same message id) without changing state", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    const created = t.replyTo(await t.pendingId(), "order.created", { orderId: t.order.id, status: "PAYMENT_PENDING" });
    await t.orchestrator.handle(created);
    const after = await t.state();
    await t.orchestrator.handle(created);
    expect((await t.state()).version).toBe(after.version);
    expect(await t.sentTypes()).toEqual(["order.create", "payment.authorize"]);
  });

  it("logs and ignores stale replies whose causation does not match the pending command", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    await t.orchestrator.handle(t.replyTo("some-old-command", "order.created", { orderId: t.order.id, status: "PAYMENT_PENDING" }));
    const s = await t.state();
    expect(s.completedSteps).toEqual([]);
    expect(s.log.at(-1)?.event).toBe("MESSAGE_IGNORED");
  });

  it("compensates a step whose success arrives after it timed out (late success)", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "order.created", { orderId: t.order.id, status: "PAYMENT_PENDING" }));
    const authorizeId = await t.pendingId();

    t.clock.advance(1_000);
    await t.orchestrator.checkTimeouts();
    let s = await t.state();
    expect(s.status).toBe("COMPENSATING");
    expect(s.pending?.type).toBe("payment.release");

    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "payment.released", { orderId: t.order.id, noop: true }));
    expect((await t.state()).pending?.type).toBe("order.cancel");

    // The provider finally answers the authorize we gave up on.
    const late = t.replyTo(authorizeId, "payment.authorized", { orderId: t.order.id, authorizationId: "auth_late" });
    await t.orchestrator.handle(late);
    s = await t.state();
    expect(s.log.at(-1)?.event).toBe("LATE_SUCCESS");
    expect(s.compensationQueue).toEqual(["authorizePayment"]);

    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "order.cancelled", { orderId: t.order.id, reason: "saga_compensation" }));
    expect((await t.state()).pending?.type).toBe("payment.release");
    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "payment.released", { orderId: t.order.id, noop: false }));

    s = await t.state();
    expect(s.status).toBe("COMPENSATED");
    expect(s.compensatedSteps).toEqual(["authorizePayment", "createOrder", "authorizePayment"]);

    // The same late success re-emitted under a new id is not compensated twice.
    await t.orchestrator.handle(t.replyTo(authorizeId, "payment.authorized", { orderId: t.order.id, authorizationId: "auth_late" }));
    expect((await t.state()).status).toBe("COMPENSATED");
  });

  it("reopens a COMPENSATED saga when a late success arrives after compensation finished", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    const createId = await t.pendingId();
    t.clock.advance(1_000);
    await t.orchestrator.checkTimeouts();
    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "order.cancelled", { orderId: t.order.id, reason: "saga_compensation" }));
    expect((await t.state()).status).toBe("COMPENSATED");

    await t.orchestrator.handle(t.replyTo(createId, "order.created", { orderId: t.order.id, status: "PAYMENT_PENDING" }));
    const s = await t.state();
    expect(s.status).toBe("COMPENSATING");
    expect(s.pending?.type).toBe("order.cancel");
  });

  it("re-sends an unanswered compensation with the same id and fails after max attempts", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "order.created", { orderId: t.order.id, status: "PAYMENT_PENDING" }));
    await t.orchestrator.handle(t.replyTo(await t.pendingId(), "payment.failed", { orderId: t.order.id, reason: "card_declined" }));
    const cancelId = await t.pendingId();

    t.clock.advance(1_000);
    await t.orchestrator.checkTimeouts();
    let s = await t.state();
    expect(s.pending?.messageId).toBe(cancelId);
    expect(s.pending?.attempt).toBe(2);
    expect(s.log.at(-1)?.event).toBe("COMMAND_RESENT");

    t.clock.advance(1_100);
    await t.orchestrator.checkTimeouts();
    expect((await t.state()).pending?.attempt).toBe(3);

    t.clock.advance(1_200);
    await t.orchestrator.checkTimeouts();
    s = await t.state();
    expect(s.status).toBe("FAILED");
    expect(s.log.at(-1)?.event).toBe("SAGA_FAILED");
    const cancels = (await t.repository.outbox.pending(100)).filter((e) => e.message.type === "order.cancel");
    expect(cancels.map((e) => e.message.id)).toEqual([cancelId, cancelId, cancelId]);
  });

  it("does not time out before the deadline", async () => {
    const t = setup();
    await t.orchestrator.start(t.order);
    t.clock.advance(999);
    expect(await t.orchestrator.checkTimeouts()).toBe(0);
    expect((await t.state()).status).toBe("RUNNING");
  });

  it("uses optimistic concurrency so two writers cannot both advance the saga", async () => {
    const t = setup();
    const s = await t.orchestrator.start(t.order);
    await t.repository.commit({ state: { ...s, version: 2 }, expectedVersion: 1, outbox: [], processedMessageId: null });
    await expect(
      t.repository.commit({ state: { ...s, version: 2 }, expectedVersion: 1, outbox: [], processedMessageId: null }),
    ).rejects.toBeInstanceOf(ConcurrencyError);
  });
});
