/**
 * Fault-injection simulator: runs N order sagas through the full in-memory system (outbox,
 * at-least-once bus with duplicates and reordering, idempotent consumers, retries, circuit
 * breaker, DLQ, saga timeouts) against a payment provider that fails and hangs on purpose,
 * then checks that every saga ended in a consistent state.
 *
 *   npm run simulate -- --sagas 300 --fail 0.3 --timeout 0.1
 */
import { parseArgs } from "node:util";
import { InMemoryProcessedMessageStore } from "../src/messaging/idempotency.js";
import { InMemoryBus } from "../src/messaging/in-memory-bus.js";
import { SagaOrchestrator } from "../src/saga/orchestrator.js";
import { InMemorySagaRepository } from "../src/saga/repository.js";
import { TERMINAL_STATUSES, type SagaState } from "../src/saga/state.js";
import { OrchestratorRuntime } from "../src/runtime/orchestrator-runtime.js";
import { createParticipants } from "../src/runtime/participants.js";
import { makeOrder } from "../src/services/fixtures.js";
import { seededRandom } from "../src/shared/random.js";

const { values } = parseArgs({
  options: {
    sagas: { type: "string", default: "200" },
    fail: { type: "string", default: "0.2" },
    timeout: { type: "string", default: "0.05" },
    decline: { type: "string", default: "0.05" },
    withdraw: { type: "string", default: "0.03" },
    pulsers: { type: "string", default: "120" },
    duplicates: { type: "string", default: "0.1" },
    jitter: { type: "string", default: "3" },
    outage: { type: "string", default: "1000-2500" },
    seed: { type: "string", default: "2026" },
  },
});

const config = {
  sagas: Number(values.sagas),
  failureRate: Number(values.fail),
  timeoutRate: Number(values.timeout),
  declineRate: Number(values.decline),
  withdrawRate: Number(values.withdraw),
  pulsers: Number(values.pulsers),
  duplicateRate: Number(values.duplicates),
  jitterMs: Number(values.jitter),
  outage: parseWindow(values.outage),
  seed: Number(values.seed),
};

/** "from-to" in ms since start, during which every provider call fails; "none" disables it. */
function parseWindow(raw: string): { from: number; to: number } | null {
  if (raw === "none") return null;
  const [from, to] = raw.split("-").map(Number);
  if (from === undefined || to === undefined || Number.isNaN(from) || Number.isNaN(to) || to <= from) {
    throw new Error(`invalid --outage "${raw}", expected e.g. 1000-2500 or none`);
  }
  return { from, to };
}

const pct = (n: number, total: number) => (total === 0 ? "0.0%" : `${((n / total) * 100).toFixed(1)}%`);
const row = (label: string, value: string | number, extra = "") =>
  `  ${label.padEnd(36)} ${String(value).padStart(6)}  ${extra}`;

async function main(): Promise<void> {
  const random = seededRandom(config.seed);
  const bus = new InMemoryBus({ duplicateRate: config.duplicateRate, deliveryJitterMs: config.jitterMs, random });
  const repository = new InMemorySagaRepository();
  const orchestrator = new SagaOrchestrator({
    repository,
    stepTimeoutMs: 10_000,
    maxCompensationAttempts: 5,
    compensationBackoff: { baseMs: 200, maxMs: 2_000, jitter: "full" },
  });
  const runtime = new OrchestratorRuntime(bus, repository, orchestrator, {
    consumer: "sim",
    relayIntervalMs: 2,
    timeoutCheckIntervalMs: 50,
  });
  const participants = createParticipants(
    {
      provider: {
        failureRate: config.failureRate,
        timeoutRate: config.timeoutRate,
        declineRate: config.declineRate,
        latencyMs: { min: 2, max: 15 },
      },
      pulsers: config.pulsers,
      merchantWithdrawRate: config.withdrawRate,
      dispatchLatencyMs: { min: 5, max: 25 },
      maxDeliveries: 6,
    },
    random,
  );
  await participants.start(bus, new InMemoryProcessedMessageStore(), "sim");
  await runtime.start();

  const started = Date.now();
  const outageTimers: NodeJS.Timeout[] = [];
  if (config.outage) {
    outageTimers.push(
      setTimeout(() => {
        participants.provider.configure({ failureRate: 1 });
      }, config.outage.from),
      setTimeout(() => {
        participants.provider.configure({ failureRate: config.failureRate });
      }, config.outage.to),
    );
  }
  const orderIds: string[] = [];
  // Arrivals spread over ~4s so the outage window hits sagas at different steps.
  for (let i = 0; i < config.sagas; i++) {
    const order = makeOrder(random);
    orderIds.push(order.id);
    await orchestrator.start(order);
    await new Promise((r) => setTimeout(r, 4_000 / config.sagas));
  }

  const allDone = () => repository.all().every((s) => TERMINAL_STATUSES.includes(s.status));
  while (!allDone() && Date.now() - started < 120_000) await new Promise((r) => setTimeout(r, 50));
  // Let stragglers (late replies, duplicate deliveries) land before checking invariants.
  await new Promise((r) => setTimeout(r, 500));
  await bus.drain();
  outageTimers.forEach(clearTimeout);
  await runtime.stop();
  await bus.close();
  const elapsed = Date.now() - started;

  const sagas = repository.all();
  const by = (status: SagaState["status"]) => sagas.filter((s) => s.status === status);
  const completed = by("COMPLETED");
  const compensated = by("COMPENSATED");
  const failed = by("FAILED");
  const stuck = sagas.filter((s) => !TERMINAL_STATUSES.includes(s.status));

  const reasons = new Map<string, number>();
  for (const s of compensated) {
    const key = s.failure ? `${s.failure.step}: ${s.failure.reason}` : "unknown";
    reasons.set(key, (reasons.get(key) ?? 0) + 1);
  }
  const dlqByType = new Map<string, number>();
  for (const letter of bus.deadLettered) dlqByType.set(letter.message.type, (dlqByType.get(letter.message.type) ?? 0) + 1);
  const logCount = (event: string) => sagas.reduce((n, s) => n + s.log.filter((e) => e.event === event).length, 0);

  // Consistency invariants: what must hold once the system is quiet.
  const violations: string[] = [];
  const { orders, payments, assignments } = participants;
  for (const s of completed) {
    if (orders.get(s.sagaId)?.status !== "ASSIGNED") violations.push(`${s.sagaId}: completed but order not ASSIGNED`);
    if (payments.status(s.sagaId) !== "CAPTURED") violations.push(`${s.sagaId}: completed but payment not CAPTURED`);
    if (!assignments.assignmentFor(s.sagaId)) violations.push(`${s.sagaId}: completed without assignment`);
  }
  for (const s of compensated) {
    if (orders.get(s.sagaId)?.status === "ASSIGNED") violations.push(`${s.sagaId}: compensated but order ASSIGNED`);
    const p = payments.status(s.sagaId);
    if (p === "AUTHORIZED" || p === "CAPTURED") violations.push(`${s.sagaId}: compensated but money held (${p})`);
    if (assignments.assignmentFor(s.sagaId)) violations.push(`${s.sagaId}: compensated but pulser still assigned`);
  }
  const busyTasks = orderIds.reduce((n, id) => n + (assignments.assignmentFor(id) ? 1 : 0), 0);
  if (busyTasks !== completed.length) violations.push(`pulser tasks ${String(busyTasks)} != completed ${String(completed.length)}`);

  const total = sagas.length;
  const lines = [
    "",
    "Pulso saga — fault-injection run",
    `  ${String(config.sagas)} sagas | provider: ${pct(config.failureRate, 1)} 5xx, ${pct(config.timeoutRate, 1)} hang, ${pct(config.declineRate, 1)} decline | merchant withdraw ${pct(config.withdrawRate, 1)}`,
    `  outage: ${config.outage ? `100% 5xx from ${String(config.outage.from)}ms to ${String(config.outage.to)}ms` : "none"}`,
    `  bus: ${pct(config.duplicateRate, 1)} duplicate deliveries, up to ${String(config.jitterMs)}ms reorder jitter | seed ${String(config.seed)} | ${String(elapsed)}ms`,
    "",
    "Outcomes",
    row("completed", completed.length, pct(completed.length, total)),
    row("compensated", compensated.length, pct(compensated.length, total)),
    ...[...reasons.entries()].sort((a, b) => b[1] - a[1]).map(([reason, n]) => row(`  ${reason}`, n)),
    row("failed (manual action)", failed.length, pct(failed.length, total)),
    row("still running", stuck.length),
    row(
      "dead-lettered deliveries",
      bus.deadLettered.length,
      `${String(new Set(bus.deadLettered.map((d) => d.message.id)).size)} distinct commands: ${[...dlqByType.entries()].map(([t, n]) => `${t}=${String(n)}`).join(" ")}`,
    ),
    "",
    "Resilience",
    row("provider calls", participants.provider.stats.calls),
    row("  5xx injected", participants.provider.stats.failures),
    row("  hangs injected (timed out)", participants.provider.stats.hangs),
    row("in-process retries", participants.metrics.providerRetries),
    row("circuit breaker transitions", participants.metrics.breakerTransitions.length,
      summarizeTransitions(participants.metrics.breakerTransitions)),
    row("saga steps timed out", logCount("STEP_TIMED_OUT")),
    row("late successes compensated", logCount("LATE_SUCCESS")),
    row("stale replies ignored", logCount("MESSAGE_IGNORED")),
    row("compensations re-sent", logCount("COMMAND_RESENT")),
    "",
    "Consistency",
    violations.length === 0
      ? "  OK: every completed saga is ASSIGNED + CAPTURED; no compensated saga holds money or a pulser"
      : violations.slice(0, 10).map((v) => `  VIOLATION ${v}`).join("\n"),
    "",
  ];
  process.stdout.write(lines.join("\n"));
  process.exitCode = violations.length === 0 && stuck.length === 0 ? 0 : 1;
}

function summarizeTransitions(transitions: string[]): string {
  const counts = new Map<string, number>();
  for (const t of transitions) counts.set(t, (counts.get(t) ?? 0) + 1);
  return [...counts.entries()].map(([t, n]) => `${t}=${String(n)}`).join(" ");
}

await main();
