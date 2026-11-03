import type { AnyMessage } from "../domain/messages.js";

export type ProcessedRecord = { status: "in_progress"; claimedAt: number } | { status: "done"; replies: AnyMessage[] };

/**
 * Processed-message store keyed by a dedupe key (consumer name + message id).
 * `claim` must be atomic (SET NX in Redis) so two concurrent deliveries of the same
 * message cannot both run the side effect.
 */
export interface ProcessedMessageStore {
  /** Returns true if the caller now owns the key; otherwise the existing record. */
  claim(key: string, now: number): Promise<true | ProcessedRecord>;
  complete(key: string, replies: AnyMessage[]): Promise<void>;
  /** Drops a claim after a failure so a redelivery can try again. */
  release(key: string): Promise<void>;
}

export class InMemoryProcessedMessageStore implements ProcessedMessageStore {
  private readonly records = new Map<string, ProcessedRecord>();

  claim(key: string, now: number): Promise<true | ProcessedRecord> {
    const existing = this.records.get(key);
    if (existing) return Promise.resolve(existing);
    this.records.set(key, { status: "in_progress", claimedAt: now });
    return Promise.resolve(true);
  }

  complete(key: string, replies: AnyMessage[]): Promise<void> {
    this.records.set(key, { status: "done", replies });
    return Promise.resolve();
  }

  release(key: string): Promise<void> {
    this.records.delete(key);
    return Promise.resolve();
  }

  get size(): number {
    return this.records.size;
  }
}

export class DuplicateInFlightError extends Error {
  override readonly name = "DuplicateInFlightError";
  constructor(readonly key: string) {
    super(`message ${key} is already being processed`);
  }
}

export interface IdempotentResult {
  replies: AnyMessage[];
  duplicate: boolean;
}

/**
 * Idempotent consumer with response caching. The first delivery runs `handle` and stores
 * the replies it produced; any later delivery of the same message returns those exact
 * replies (same message ids) instead of repeating the side effect, so downstream
 * consumers can dedupe them too.
 */
export class IdempotentConsumer {
  constructor(
    private readonly consumer: string,
    private readonly store: ProcessedMessageStore,
    private readonly now: () => number = Date.now,
  ) {}

  async process(message: AnyMessage, handle: () => Promise<AnyMessage[]>): Promise<IdempotentResult> {
    const key = `${this.consumer}:${message.id}`;
    const claim = await this.store.claim(key, this.now());
    if (claim !== true) {
      if (claim.status === "done") return { replies: claim.replies, duplicate: true };
      throw new DuplicateInFlightError(key);
    }
    try {
      const replies = await handle();
      await this.store.complete(key, replies);
      return { replies, duplicate: false };
    } catch (error) {
      await this.store.release(key);
      throw error;
    }
  }
}
