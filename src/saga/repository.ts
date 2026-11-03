import { InMemoryOutbox, type OutboxEntry, type OutboxReader } from "../outbox/outbox.js";
import type { SagaState } from "./state.js";

export class ConcurrencyError extends Error {
  override readonly name = "ConcurrencyError";
  constructor(sagaId: string, expected: number) {
    super(`saga ${sagaId} was modified concurrently (expected version ${expected})`);
  }
}

/**
 * Everything one orchestrator step writes, committed atomically:
 * new saga state, outgoing commands (outbox) and the id of the message just consumed.
 */
export interface SagaCommit {
  state: SagaState;
  expectedVersion: number;
  outbox: OutboxEntry[];
  processedMessageId: string | null;
}

export interface SagaRepository {
  get(sagaId: string): Promise<SagaState | undefined>;
  hasProcessed(messageId: string): Promise<boolean>;
  /** Throws ConcurrencyError if the stored version is not `expectedVersion`. */
  commit(commit: SagaCommit): Promise<void>;
  /** Saga ids whose pending command deadline is <= now. */
  dueForTimeout(now: number, limit: number): Promise<string[]>;
  readonly outbox: OutboxReader;
}

export class InMemorySagaRepository implements SagaRepository {
  readonly outbox = new InMemoryOutbox();
  private readonly sagas = new Map<string, SagaState>();
  private readonly processed = new Set<string>();

  get(sagaId: string): Promise<SagaState | undefined> {
    const state = this.sagas.get(sagaId);
    return Promise.resolve(state ? structuredClone(state) : undefined);
  }

  hasProcessed(messageId: string): Promise<boolean> {
    return Promise.resolve(this.processed.has(messageId));
  }

  commit({ state, expectedVersion, outbox, processedMessageId }: SagaCommit): Promise<void> {
    const current = this.sagas.get(state.sagaId);
    if ((current?.version ?? 0) !== expectedVersion) {
      return Promise.reject(new ConcurrencyError(state.sagaId, expectedVersion));
    }
    if (processedMessageId && this.processed.has(processedMessageId)) {
      return Promise.reject(new ConcurrencyError(state.sagaId, expectedVersion));
    }
    // Single synchronous block == one transaction in this adapter.
    this.sagas.set(state.sagaId, structuredClone(state));
    this.outbox.append(outbox);
    if (processedMessageId) this.processed.add(processedMessageId);
    return Promise.resolve();
  }

  dueForTimeout(now: number, limit: number): Promise<string[]> {
    const due = [...this.sagas.values()]
      .filter((s) => s.pending !== null && s.pending.deadline <= now)
      .slice(0, limit)
      .map((s) => s.sagaId);
    return Promise.resolve(due);
  }

  all(): SagaState[] {
    return [...this.sagas.values()].map((s) => structuredClone(s));
  }
}
