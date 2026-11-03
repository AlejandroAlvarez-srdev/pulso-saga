import { describe, expect, it } from "vitest";
import { makeOrder } from "../../src/services/fixtures.js";
import { seededRandom } from "../../src/shared/random.js";
import { createHarness } from "../support/harness.js";

const orderRandom = seededRandom(99);

describe("order saga (end to end, in-memory adapters)", () => {
  it("happy path: create → authorize → assign → capture → confirm", async () => {
    const h = await createHarness();
    const order = makeOrder(orderRandom);
    await h.orchestrator.start(order);
    await h.settle();

    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPLETED");
    expect(saga?.completedSteps).toEqual(["createOrder", "authorizePayment", "assignPulser", "capturePayment", "confirmOrder"]);
    expect(saga?.data.authorizationId).toMatch(/^auth_/);
    expect(h.participants.orders.get(order.id)?.status).toBe("ASSIGNED");
    expect(h.participants.payments.status(order.id)).toBe("CAPTURED");
    const assignment = h.participants.assignments.assignmentFor(order.id);
    expect(assignment?.pulserId).toBe(saga?.data.assignment?.pulserId);
    expect(h.participants.assignments.pulser(assignment?.pulserId ?? "")?.activeTasks).toBe(1);
    expect(saga?.log.map((e) => e.event).filter((e) => e !== "COMMAND_SENT")).toEqual([
      "SAGA_STARTED",
      "STEP_COMPLETED",
      "STEP_COMPLETED",
      "STEP_COMPLETED",
      "STEP_COMPLETED",
      "STEP_COMPLETED",
      "SAGA_COMPLETED",
    ]);
  });

  it("payment declined: cancels the order and never assigns a pulser", async () => {
    const h = await createHarness({ provider: { failureRate: 0, timeoutRate: 0, declineRate: 1, latencyMs: { min: 0, max: 1 } } });
    const order = makeOrder(orderRandom);
    await h.orchestrator.start(order);
    await h.settle();

    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPENSATED");
    expect(saga?.failure).toEqual({ step: "authorizePayment", reason: "card_declined" });
    expect(saga?.compensatedSteps).toEqual(["createOrder"]);
    expect(h.participants.orders.get(order.id)?.status).toBe("CANCELLED");
    expect(h.participants.assignments.assignmentFor(order.id)).toBeUndefined();
  });

  it("no pulser available: releases the payment authorization, then cancels the order", async () => {
    const h = await createHarness({ pulsers: 0 });
    const order = makeOrder(orderRandom);
    await h.orchestrator.start(order);
    await h.settle();

    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPENSATED");
    expect(saga?.failure).toEqual({ step: "assignPulser", reason: "no_pulser_available" });
    expect(saga?.compensatedSteps).toEqual(["authorizePayment", "createOrder"]);
    expect(h.participants.payments.status(order.id)).toBe("RELEASED");
    expect(h.participants.orders.get(order.id)?.status).toBe("CANCELLED");
  });

  it("confirmation fails after capture: refunds, frees the pulser, skips the void, cancels", async () => {
    const h = await createHarness({ merchantWithdrawRate: 1 });
    const order = makeOrder(orderRandom);
    await h.orchestrator.start(order);
    await h.settle();

    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPENSATED");
    expect(saga?.failure).toEqual({ step: "confirmOrder", reason: "merchant_withdrew" });
    expect(saga?.compensatedSteps).toEqual(["capturePayment", "assignPulser", "createOrder"]);
    expect(saga?.log.find((e) => e.event === "COMPENSATION_SKIPPED")?.step).toBe("authorizePayment");
    expect(h.participants.payments.status(order.id)).toBe("REFUNDED");
    const pulserId = saga?.data.assignment?.pulserId ?? "";
    expect(h.participants.assignments.pulser(pulserId)?.activeTasks).toBe(0);
    expect(h.participants.orders.get(order.id)?.status).toBe("CANCELLED");
  });

  it("provider outage: command is dead-lettered, the step times out and the saga compensates", async () => {
    const h = await createHarness({
      provider: { failureRate: 1, timeoutRate: 0, declineRate: 0, latencyMs: { min: 0, max: 0 } },
      maxDeliveries: 4,
    });
    const order = makeOrder(orderRandom);
    await h.orchestrator.start(order);
    await h.settle();

    expect((await h.orchestrator.get(order.id))?.status).toBe("RUNNING");
    expect(h.bus.deadLettered.map((d) => d.message.type)).toEqual(["payment.authorize"]);
    // 3 deliveries x 3 in-process attempts, then the breaker opens on the 10th failure and the
    // 4th delivery fails fast instead of hammering the provider.
    expect(h.participants.provider.stats.calls).toBe(10);
    expect(h.participants.metrics.breakerTransitions[0]).toBe("CLOSED->OPEN");

    h.clock.advance(5_000);
    expect(await h.orchestrator.checkTimeouts()).toBe(1);
    await h.settle();

    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPENSATED");
    expect(saga?.failure).toEqual({ step: "authorizePayment", reason: "timeout" });
    // The timed-out step is compensated too: its outcome is unknown.
    expect(saga?.compensatedSteps).toEqual(["authorizePayment", "createOrder"]);
    expect(h.participants.payments.status(order.id)).toBe("RELEASED");
  });

  it("stays correct when every message is delivered twice", async () => {
    const h = await createHarness({}, { duplicateRate: 1 });
    const orders = [makeOrder(orderRandom), makeOrder(orderRandom), makeOrder(orderRandom)];
    for (const order of orders) await h.orchestrator.start(order);
    await h.settle();

    for (const order of orders) {
      const saga = await h.orchestrator.get(order.id);
      expect(saga?.status).toBe("COMPLETED");
      expect(saga?.log.filter((e) => e.event === "STEP_COMPLETED")).toHaveLength(5);
    }
    // authorize + capture per order: duplicates never reach the provider twice.
    expect(h.participants.provider.stats.calls).toBe(orders.length * 2);
    const busy = orders.map((o) => h.participants.assignments.assignmentFor(o.id)?.pulserId ?? "");
    const totalTasks = [...new Set(busy)].reduce(
      (sum, id) => sum + (h.participants.assignments.pulser(id)?.activeTasks ?? 0),
      0,
    );
    expect(totalTasks).toBe(orders.length);
  });

  it("stays correct when deliveries are reordered by random latency", async () => {
    const h = await createHarness({}, { deliveryJitterMs: 5, duplicateRate: 0.3 }, 7);
    const orders = Array.from({ length: 10 }, () => makeOrder(orderRandom));
    for (const order of orders) await h.orchestrator.start(order);
    await h.settle();
    for (const order of orders) expect((await h.orchestrator.get(order.id))?.status).toBe("COMPLETED");
  });

  it("start is idempotent per order id", async () => {
    const h = await createHarness();
    const order = makeOrder(orderRandom);
    const first = await h.orchestrator.start(order);
    const second = await h.orchestrator.start(order);
    expect(second.sagaId).toBe(first.sagaId);
    expect(h.repository.outbox.size).toBe(1);
  });

  it("rejects invalid orders without touching payments", async () => {
    const h = await createHarness();
    const order = makeOrder(orderRandom, { items: [] });
    await h.orchestrator.start(order);
    await h.settle();
    const saga = await h.orchestrator.get(order.id);
    expect(saga?.status).toBe("COMPENSATED");
    expect(saga?.failure?.step).toBe("createOrder");
    expect(saga?.compensatedSteps).toEqual([]);
    expect(h.participants.provider.stats.calls).toBe(0);
  });
});
