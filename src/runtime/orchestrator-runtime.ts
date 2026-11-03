import { Streams, type MessageBus, type Subscription } from "../messaging/bus.js";
import { OutboxRelay } from "../outbox/relay.js";
import { backoffDelay } from "../resilience/backoff.js";
import type { SagaOrchestrator } from "../saga/orchestrator.js";
import type { SagaRepository } from "../saga/repository.js";
import { errorMessage, silentLogger, type Logger } from "../shared/logger.js";

export interface OrchestratorRuntimeOptions {
  consumer: string;
  relayIntervalMs: number;
  timeoutCheckIntervalMs: number;
  logger?: Logger;
}

/** Runs the orchestrator's three loops: reply consumer, outbox relay and deadline checker. */
export class OrchestratorRuntime {
  readonly relay: OutboxRelay;
  private subscription: Subscription | undefined;
  private timeoutTimer: NodeJS.Timeout | undefined;
  private readonly logger: Logger;

  constructor(
    private readonly bus: MessageBus,
    repository: SagaRepository,
    private readonly orchestrator: SagaOrchestrator,
    private readonly options: OrchestratorRuntimeOptions,
  ) {
    this.logger = options.logger ?? silentLogger;
    this.relay = new OutboxRelay(repository.outbox, bus, {
      batchSize: 100,
      intervalMs: options.relayIntervalMs,
      logger: this.logger,
    });
  }

  async start(): Promise<void> {
    this.subscription = await this.bus.subscribe(
      Streams.sagaEvents,
      {
        group: "saga-orchestrator",
        consumer: this.options.consumer,
        maxDeliveries: 10,
        concurrency: 16,
        redeliveryDelayMs: (count) => backoffDelay(count - 1, { baseMs: 5, maxMs: 200, jitter: "full" }),
      },
      (message) => this.orchestrator.handle(message),
    );
    this.relay.start();
    const tick = () => {
      void this.orchestrator
        .checkTimeouts()
        .catch((error: unknown) => {
          this.logger.error("timeout check failed", { error: errorMessage(error) });
        })
        .finally(() => {
          if (this.timeoutTimer) this.timeoutTimer = setTimeout(tick, this.options.timeoutCheckIntervalMs);
        });
    };
    this.timeoutTimer = setTimeout(tick, this.options.timeoutCheckIntervalMs);
  }

  async stop(): Promise<void> {
    clearTimeout(this.timeoutTimer);
    this.timeoutTimer = undefined;
    await this.relay.stop();
    await this.subscription?.stop();
  }
}
