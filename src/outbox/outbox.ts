import type { AnyMessage } from "../domain/messages.js";

/**
 * A message waiting to be published. It is written in the same atomic commit as the state
 * change that produced it, so "state changed but message lost" (dual write) cannot happen.
 */
export interface OutboxEntry {
  id: string;
  stream: string;
  message: AnyMessage;
  createdAt: number;
}

export interface OutboxReader {
  /** Oldest unpublished entries first. */
  pending(limit: number): Promise<OutboxEntry[]>;
  markPublished(ids: string[]): Promise<void>;
}

/** In-memory outbox table; written by a repository commit, drained by the relay. */
export class InMemoryOutbox implements OutboxReader {
  private readonly entries = new Map<string, OutboxEntry>();

  /** Only call from inside a repository commit. */
  append(entries: OutboxEntry[]): void {
    for (const entry of entries) this.entries.set(entry.id, entry);
  }

  pending(limit: number): Promise<OutboxEntry[]> {
    return Promise.resolve([...this.entries.values()].slice(0, limit));
  }

  markPublished(ids: string[]): Promise<void> {
    for (const id of ids) this.entries.delete(id);
    return Promise.resolve();
  }

  get size(): number {
    return this.entries.size;
  }
}
