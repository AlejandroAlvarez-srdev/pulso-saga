import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { Streams } from "../../src/messaging/bus.js";
import { checkConsistency } from "../../src/saga/invariants.js";
import { TERMINAL_STATUSES } from "../../src/saga/state.js";
import { makeOrder } from "../../src/services/fixtures.js";
import { seededRandom } from "../../src/shared/random.js";
import { createHarness } from "../support/harness.js";

/**
 * Property-based tests: fast-check generates whole fault scenarios (provider error / hang /
 * decline rates, pulser supply, merchant withdrawals, duplicate deliveries, reordering, invalid
 * orders, several sagas sharing one pulser pool) and the consistency invariants from
 * `src/saga/invariants.ts` must hold for every one of them once the system is quiet. On a
 * failure fast-check shrinks the scenario to a minimal one and prints its seed and path.
 */

const scenario = fc.record({
  seed: fc.integer({ min: 1, max: 2 ** 31 - 1 }),
  orders: fc.integer({ min: 1, max: 5 }),
  invalidOrders: fc.integer({ min: 0, max: 1 }),
  failureRate: fc.constantFrom(0, 0.1, 0.3),
  /** Provider answers 100 % 5xx for the whole run: breaker opens, commands are dead-lettered. */
  outage: fc.boolean(),
  timeoutRate: fc.constantFrom(0, 0.05),
  declineRate: fc.constantFrom(0, 0.2, 1),
  pulsers: fc.integer({ min: 0, max: 4 }),
  merchantWithdrawRate: fc.constantFrom(0, 0.3, 1),
  duplicateRate: fc.constantFrom(0, 0.5, 1),
  deliveryJitterMs: fc.constantFrom(0, 3, 10),
  /** Fire every saga deadline this many ms into the run, while replies are still in flight. */
  earlyDeadlineAfterMs: fc.option(fc.integer({ min: 0, max: 20 }), { nil: undefined }),
});

type Scenario = typeof scenario extends fc.Arbitrary<infer T> ? T : never;

// Deeper local runs: SAGA_PROPERTY_RUNS=500 SAGA_PROPERTY_SEED=$RANDOM npx vitest run test/saga/saga-properties.test.ts
const RUNS = Number(process.env.SAGA_PROPERTY_RUNS) || undefined;
const SEED = Number(process.env.SAGA_PROPERTY_SEED) || undefined;
const runOptions = (defaultRuns: number, defaultSeed: number) => ({ numRuns: RUNS ?? defaultRuns, seed: SEED ?? defaultSeed });

const TIMEOUT_MS = Math.max(120_000, (RUNS ?? 0) * 2_000);

const STEP_TIMEOUT_MS = 5_000;
const MAX_COMPENSATION_BACKOFF_MS = 1_000;

async function runScenario(s: Scenario) {
  const h = await createHarness(
    {
      provider: {
        failureRate: s.outage ? 1 : s.failureRate,
        timeoutRate: s.timeoutRate,
        declineRate: s.declineRate,
        latencyMs: { min: 0, max: 1 },
      },
      pulsers: s.pulsers,
      merchantWithdrawRate: s.merchantWithdrawRate,
    },
    { duplicateRate: s.duplicateRate, deliveryJitterMs: s.deliveryJitterMs },
    s.seed,
  );
  const orderRandom = seededRandom(s.seed ^ 0x5bd1e995);
  for (let i = 0; i < s.orders; i++) {
    const invalid = i < s.invalidOrders;
    await h.orchestrator.start(makeOrder(orderRandom, invalid ? { items: [] } : {}));
  }

  // Optionally expire deadlines mid-flight so replies race the timeout (late successes, stale
  // replies, compensations crossing actions). Then drive the system to quiescence: deliver
  // everything, fire the saga deadlines on the manual clock (forward-step timeouts and
  // compensation re-sends) and repeat until every saga is terminal.
  if (s.earlyDeadlineAfterMs === undefined) {
    await h.settle();
  } else {
    const settling = h.settle();
    await new Promise((resolve) => setTimeout(resolve, s.earlyDeadlineAfterMs));
    h.clock.advance(STEP_TIMEOUT_MS);
    await h.orchestrator.checkTimeouts();
    await settling;
    await h.settle();
  }
  for (let round = 0; round < 12; round++) {
    if (h.repository.all().every((saga) => TERMINAL_STATUSES.includes(saga.status))) break;
    h.clock.advance(STEP_TIMEOUT_MS + MAX_COMPENSATION_BACKOFF_MS);
    await h.orchestrator.checkTimeouts();
    await h.settle();
  }
  return h;
}

function snapshot(h: Awaited<ReturnType<typeof runScenario>>) {
  return h.repository.all().map((saga) => ({
    id: saga.sagaId,
    status: saga.status,
    order: h.participants.orders.get(saga.sagaId)?.status,
    payment: h.participants.payments.status(saga.sagaId),
    pulser: h.participants.assignments.assignmentFor(saga.sagaId)?.pulserId,
  }));
}

describe("saga properties (generated fault scenarios)", () => {
  it("every saga ends terminal and consistent, whatever faults are injected", async () => {
    await fc.assert(
      fc.asyncProperty(scenario, async (s) => {
        const h = await runScenario(s);
        try {
          expect(h.repository.all()).toHaveLength(s.orders);
          expect(checkConsistency(h.repository.all(), h.participants)).toEqual([]);
        } finally {
          await h.bus.close();
        }
      }),
      runOptions(40, 2026),
    );
  }, TIMEOUT_MS);

  it("replaying every message ever published changes nothing (effectively-once)", async () => {
    await fc.assert(
      fc.asyncProperty(scenario, async (s) => {
        const h = await runScenario(s);
        try {
          // The provider recovers before the replay, so dead-lettered commands now succeed if
          // they are allowed to run: participants must refuse them via their tombstones.
          h.participants.provider.configure({ failureRate: 0 });
          const before = snapshot(h);
          const providerCalls = h.participants.provider.stats.calls;

          // Simulates a relay that re-publishes its whole history (or a consumer group reset):
          // commands, including dead-lettered ones, and replies all arrive a second time.
          const streams = [Streams.orderCommands, Streams.paymentCommands, Streams.assignmentCommands, Streams.sagaEvents];
          const history = streams.flatMap((stream) => h.bus.messages(stream).map((message) => ({ stream, message })));
          for (const { stream, message } of history) await h.bus.publish(stream, message);
          await h.settle();

          expect(snapshot(h)).toEqual(before);
          expect(checkConsistency(h.repository.all(), h.participants)).toEqual([]);
          // Only commands that never completed (dead-lettered) may reach the provider again.
          const deadLetteredPayments = h.bus.deadLettered.filter((d) => d.stream === Streams.paymentCommands).length;
          expect(h.participants.provider.stats.calls - providerCalls).toBeLessThanOrEqual(deadLetteredPayments * 3);
        } finally {
          await h.bus.close();
        }
      }),
      runOptions(30, 2027),
    );
  }, TIMEOUT_MS);
});
