import { InMemoryProcessedMessageStore } from "../../src/messaging/idempotency.js";
import { InMemoryBus, type InMemoryBusOptions } from "../../src/messaging/in-memory-bus.js";
import { Streams } from "../../src/messaging/bus.js";
import { OutboxRelay } from "../../src/outbox/relay.js";
import { SagaOrchestrator } from "../../src/saga/orchestrator.js";
import { InMemorySagaRepository } from "../../src/saga/repository.js";
import { createParticipants, type ParticipantsConfig } from "../../src/runtime/participants.js";
import { ManualClock } from "../../src/shared/clock.js";
import { seededRandom } from "../../src/shared/random.js";

export const reliableConfig: ParticipantsConfig = {
  provider: { failureRate: 0, timeoutRate: 0, declineRate: 0, latencyMs: { min: 0, max: 1 } },
  pulsers: 20,
  merchantWithdrawRate: 0,
  dispatchLatencyMs: { min: 0, max: 1 },
  maxDeliveries: 3,
};

/**
 * Full in-memory system with a manual clock for saga deadlines. `settle()` relays the outbox
 * and drains the bus until nothing moves, so tests are deterministic and need no sleeps.
 */
export async function createHarness(
  config: Partial<ParticipantsConfig> = {},
  busOptions: InMemoryBusOptions = {},
  seed = 1,
) {
  const random = seededRandom(seed);
  const clock = new ManualClock(Date.parse("2026-11-03T12:00:00Z"));
  const bus = new InMemoryBus({ random, ...busOptions });
  const repository = new InMemorySagaRepository();
  const orchestrator = new SagaOrchestrator({
    repository,
    clock,
    stepTimeoutMs: 5_000,
    maxCompensationAttempts: 3,
    compensationBackoff: { baseMs: 100, maxMs: 1_000, jitter: "none" },
  });
  const relay = new OutboxRelay(repository.outbox, bus, { batchSize: 50, intervalMs: 10 });
  const participants = createParticipants({ ...reliableConfig, ...config }, random);
  const store = new InMemoryProcessedMessageStore();
  await participants.start(bus, store, "test");
  await bus.subscribe(
    Streams.sagaEvents,
    { group: "saga-orchestrator", consumer: "test", maxDeliveries: 5, redeliveryDelayMs: () => 0 },
    (message) => orchestrator.handle(message),
  );

  const settle = async () => {
    for (let i = 0; i < 50; i++) {
      const published = await relay.flush();
      await bus.drain();
      if (published === 0 && repository.outbox.size === 0) return;
    }
    throw new Error("system did not settle");
  };

  return { bus, repository, orchestrator, relay, participants, store, clock, random, settle };
}
