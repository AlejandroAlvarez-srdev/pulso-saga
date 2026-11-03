import type { Redis } from "ioredis";
import type { AnyMessage } from "../../domain/messages.js";
import {
  Streams,
  type DeadLetter,
  type MessageBus,
  type MessageHandler,
  type SubscribeOptions,
  type Subscription,
} from "../../messaging/bus.js";
import { errorMessage, silentLogger, type Logger } from "../../shared/logger.js";
import { parseEntries, parsePending, parseReadGroup, type StreamEntry } from "./stream-reply.js";

export interface RedisBusOptions {
  /** XREADGROUP BLOCK timeout; bounds how long stop() waits for a reader. */
  blockMs?: number;
  batchSize?: number;
  /** How often each subscription scans its group's pending entries list (PEL). */
  reclaimIntervalMs?: number;
  /** Approximate cap per stream (XADD MAXLEN ~). */
  maxLen?: number;
  logger?: Logger;
}

const FIELD = "message";

/**
 * Redis Streams adapter.
 *
 * - publish        XADD stream MAXLEN ~ n * message <json>
 * - consume        XREADGROUP GROUP g c COUNT n BLOCK ms STREAMS stream >
 * - ack            XACK after the handler resolves
 * - redelivery     a failed message stays in the PEL; a reclaim loop scans XPENDING and
 *                  XCLAIMs entries idle for longer than the backoff for their delivery count
 *                  (this also rescues messages owned by a consumer that crashed)
 * - dead letters   once deliveries reach maxDeliveries: XADD to the DLQ stream, then XACK
 */
export class RedisStreamsBus implements MessageBus {
  private readonly readers = new Set<Redis>();
  private readonly options: Required<Omit<RedisBusOptions, "logger">>;
  private readonly logger: Logger;

  constructor(
    private readonly redis: Redis,
    options: RedisBusOptions = {},
  ) {
    this.options = {
      blockMs: options.blockMs ?? 1_000,
      batchSize: options.batchSize ?? 32,
      reclaimIntervalMs: options.reclaimIntervalMs ?? 500,
      maxLen: options.maxLen ?? 100_000,
    };
    this.logger = options.logger ?? silentLogger;
  }

  async publish(stream: string, message: AnyMessage): Promise<void> {
    await this.redis.call("XADD", stream, "MAXLEN", "~", this.options.maxLen, "*", FIELD, JSON.stringify(message));
  }

  subscribe(stream: string, options: SubscribeOptions, handler: MessageHandler): Promise<Subscription> {
    return this.consume(stream, options, async (entry, deliveryCount) => {
      const message = JSON.parse(entry.fields[FIELD] ?? "null") as AnyMessage | null;
      if (!message) return; // malformed entry: ack and drop
      await handler(message, { deliveryCount });
    });
  }

  subscribeDeadLetters(group: string, handler: (letter: DeadLetter) => Promise<void>): Promise<Subscription> {
    return this.consume(
      Streams.deadLetter,
      { group, consumer: `${group}-1`, maxDeliveries: Number.MAX_SAFE_INTEGER, redeliveryDelayMs: () => 1_000 },
      async (entry) => {
        const letter = JSON.parse(entry.fields[FIELD] ?? "null") as DeadLetter | null;
        if (letter) await handler(letter);
      },
    );
  }

  async close(): Promise<void> {
    for (const reader of this.readers) reader.disconnect();
    this.readers.clear();
    await this.redis.quit();
  }

  private async consume(
    stream: string,
    options: SubscribeOptions,
    process: (entry: StreamEntry, deliveryCount: number) => Promise<void>,
  ): Promise<Subscription> {
    await this.ensureGroup(stream, options.group);
    // XREADGROUP BLOCK holds the connection, so each subscription gets its own.
    const reader = this.redis.duplicate();
    this.readers.add(reader);
    const state = { running: true };
    // A function call defeats control-flow narrowing: stop() flips the flag while a read is awaited.
    const isRunning = () => state.running;
    const inFlight = new Set<Promise<void>>();
    const concurrency = options.concurrency ?? 1;

    const runOne = async (entry: StreamEntry, deliveryCount: number) => {
      try {
        await process(entry, deliveryCount);
        await this.redis.call("XACK", stream, options.group, entry.id);
      } catch (error) {
        // Not acked: stays in the PEL and the reclaim loop redelivers it after the backoff.
        this.logger.warn("handler failed; left pending", { stream, id: entry.id, deliveryCount, error: errorMessage(error) });
      }
    };

    const schedule = async (entry: StreamEntry, deliveryCount: number) => {
      while (inFlight.size >= concurrency) await Promise.race(inFlight);
      const task = runOne(entry, deliveryCount).finally(() => inFlight.delete(task));
      inFlight.add(task);
    };

    const readLoop = async () => {
      while (isRunning()) {
        try {
          const raw = await reader.call(
            "XREADGROUP", "GROUP", options.group, options.consumer,
            "COUNT", this.options.batchSize, "BLOCK", this.options.blockMs,
            "STREAMS", stream, ">",
          );
          for (const entry of parseReadGroup(raw)) await schedule(entry, 1);
        } catch (error) {
          if (!isRunning()) return;
          this.logger.error("XREADGROUP failed", { stream, error: errorMessage(error) });
          await new Promise((r) => setTimeout(r, 500));
        }
      }
    };

    const reclaim = async () => {
      const pending = parsePending(
        await this.redis.call("XPENDING", stream, options.group, "-", "+", this.options.batchSize),
      );
      for (const p of pending) {
        if (p.deliveries >= options.maxDeliveries) {
          await this.deadLetter(stream, options.group, p.id, p.deliveries);
          continue;
        }
        if (p.idleMs < options.redeliveryDelayMs(p.deliveries)) continue;
        // XCLAIM with min-idle makes the claim atomic: if another consumer just took it, we get nothing.
        const claimed = parseEntries(
          await this.redis.call("XCLAIM", stream, options.group, options.consumer, options.redeliveryDelayMs(p.deliveries), p.id),
        );
        for (const entry of claimed) await schedule(entry, p.deliveries + 1);
      }
    };

    let reclaimTimer: NodeJS.Timeout | undefined;
    const reclaimTick = () => {
      void reclaim()
        .catch((error: unknown) => {
          this.logger.error("reclaim failed", { stream, error: errorMessage(error) });
        })
        .finally(() => {
          if (isRunning()) reclaimTimer = setTimeout(reclaimTick, this.options.reclaimIntervalMs);
        });
    };
    reclaimTimer = setTimeout(reclaimTick, this.options.reclaimIntervalMs);
    const loop = readLoop();

    return {
      stop: async () => {
        state.running = false;
        clearTimeout(reclaimTimer);
        reader.disconnect();
        this.readers.delete(reader);
        await loop;
        await Promise.all(inFlight);
      },
    };
  }

  private async deadLetter(stream: string, group: string, id: string, deliveries: number): Promise<void> {
    const [entry] = parseEntries(await this.redis.call("XRANGE", stream, id, id));
    const message = entry ? (JSON.parse(entry.fields[FIELD] ?? "null") as AnyMessage | null) : null;
    if (message) {
      const letter: DeadLetter = {
        stream,
        group,
        deliveryCount: deliveries,
        error: `exceeded ${String(deliveries)} deliveries`,
        deadLetteredAt: new Date().toISOString(),
        message,
      };
      await this.redis.call("XADD", Streams.deadLetter, "*", FIELD, JSON.stringify(letter));
      this.logger.warn("message dead-lettered", { stream, group, id, type: message.type, sagaId: message.sagaId });
    }
    await this.redis.call("XACK", stream, group, id);
  }

  private async ensureGroup(stream: string, group: string): Promise<void> {
    try {
      await this.redis.call("XGROUP", "CREATE", stream, group, "0", "MKSTREAM");
    } catch (error) {
      if (!errorMessage(error).includes("BUSYGROUP")) throw error;
    }
  }
}
