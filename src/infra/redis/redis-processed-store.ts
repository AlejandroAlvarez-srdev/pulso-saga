import type { Redis } from "ioredis";
import type { AnyMessage } from "../../domain/messages.js";
import type { ProcessedMessageStore, ProcessedRecord } from "../../messaging/idempotency.js";

export interface RedisProcessedStoreOptions {
  prefix?: string;
  /** How long a completed record (and its cached replies) is kept. */
  doneTtlSeconds?: number;
  /** An in-progress claim expires so a crashed consumer does not block the message forever. */
  claimTtlSeconds?: number;
}

/** Dedupe store on Redis: SET NX for the claim, then overwrite with the cached replies. */
export class RedisProcessedMessageStore implements ProcessedMessageStore {
  private readonly prefix: string;
  private readonly doneTtl: number;
  private readonly claimTtl: number;

  constructor(
    private readonly redis: Redis,
    options: RedisProcessedStoreOptions = {},
  ) {
    this.prefix = options.prefix ?? "pulso:dedupe";
    this.doneTtl = options.doneTtlSeconds ?? 7 * 24 * 3600;
    this.claimTtl = options.claimTtlSeconds ?? 60;
  }

  async claim(key: string, now: number): Promise<true | ProcessedRecord> {
    const record: ProcessedRecord = { status: "in_progress", claimedAt: now };
    const ok = await this.redis.set(this.key(key), JSON.stringify(record), "EX", this.claimTtl, "NX");
    if (ok === "OK") return true;
    const existing = await this.redis.get(this.key(key));
    // Expired between SET NX and GET: report in-progress, the redelivery will claim it.
    return existing ? (JSON.parse(existing) as ProcessedRecord) : record;
  }

  async complete(key: string, replies: AnyMessage[]): Promise<void> {
    const record: ProcessedRecord = { status: "done", replies };
    await this.redis.set(this.key(key), JSON.stringify(record), "EX", this.doneTtl);
  }

  async release(key: string): Promise<void> {
    await this.redis.del(this.key(key));
  }

  private key(key: string): string {
    return `${this.prefix}:${key}`;
  }
}
