import type { MessageBus } from "../messaging/bus.js";
import { errorMessage, silentLogger, type Logger } from "../shared/logger.js";
import type { OutboxReader } from "./outbox.js";

export interface OutboxRelayOptions {
  batchSize: number;
  intervalMs: number;
  logger?: Logger;
}

/**
 * Polls the outbox and publishes entries in order. Delivery is at-least-once: if the process
 * dies after publishing but before marking an entry, it is published again on restart, which
 * is why every consumer dedupes by message id.
 */
export class OutboxRelay {
  private timer: NodeJS.Timeout | undefined;
  private running: Promise<number> | undefined;
  private readonly logger: Logger;

  constructor(
    private readonly outbox: OutboxReader,
    private readonly bus: MessageBus,
    private readonly options: OutboxRelayOptions,
  ) {
    this.logger = options.logger ?? silentLogger;
  }

  /** Publishes one batch. Stops at the first failure to preserve per-outbox ordering. */
  async runOnce(): Promise<number> {
    const entries = await this.outbox.pending(this.options.batchSize);
    const published: string[] = [];
    try {
      for (const entry of entries) {
        await this.bus.publish(entry.stream, entry.message);
        published.push(entry.id);
      }
    } catch (error) {
      this.logger.warn("outbox publish failed; will retry", { error: errorMessage(error) });
    }
    if (published.length > 0) await this.outbox.markPublished(published);
    return published.length;
  }

  /** Drains everything currently in the outbox. */
  async flush(): Promise<number> {
    let total = 0;
    for (;;) {
      const count = await this.runOnce();
      total += count;
      if (count < this.options.batchSize) return total;
    }
  }

  start(): void {
    if (this.timer) return;
    const tick = () => {
      this.running = this.flush().catch((error: unknown) => {
        this.logger.error("outbox relay tick failed", { error: errorMessage(error) });
        return 0;
      });
      void this.running.then(() => {
        if (this.timer) this.timer = setTimeout(tick, this.options.intervalMs);
      });
    };
    this.timer = setTimeout(tick, this.options.intervalMs);
  }

  async stop(): Promise<void> {
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.running;
  }
}
