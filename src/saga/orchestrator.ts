import { randomUUID } from "node:crypto";
import type { AnyMessage } from "../domain/messages.js";
import type { Order } from "../domain/order.js";
import type { OutboxEntry } from "../outbox/outbox.js";
import { backoffDelay, type BackoffOptions } from "../resilience/backoff.js";
import { systemClock, type Clock } from "../shared/clock.js";
import { silentLogger, type Logger } from "../shared/logger.js";
import { orderSaga, stepByName, type CommandSpec, type StepDefinition } from "./definition.js";
import { ConcurrencyError, type SagaRepository } from "./repository.js";
import { TERMINAL_STATUSES, type SagaLogEntry, type SagaState, type StepName } from "./state.js";

export interface OrchestratorOptions {
  repository: SagaRepository;
  /** How long a forward step may stay unanswered before the saga compensates. */
  stepTimeoutMs: number;
  /** Compensations must eventually succeed, so they are re-sent instead of abandoned. */
  maxCompensationAttempts: number;
  compensationBackoff: BackoffOptions;
  steps?: readonly StepDefinition[];
  clock?: Clock;
  logger?: Logger;
  onTerminal?: (state: SagaState) => void;
}

/**
 * Orchestrated saga. Every reaction to a message is computed on a copy of the saga state
 * and committed atomically together with the commands to send (outbox) and the id of the
 * consumed message (dedupe). Commands carry no in-memory callbacks, so the orchestrator can
 * crash and resume from the repository at any point.
 */
export class SagaOrchestrator {
  private readonly repo: SagaRepository;
  private readonly steps: readonly StepDefinition[];
  private readonly clock: Clock;
  private readonly logger: Logger;

  constructor(private readonly options: OrchestratorOptions) {
    this.repo = options.repository;
    this.steps = options.steps ?? orderSaga;
    this.clock = options.clock ?? systemClock;
    this.logger = options.logger ?? silentLogger;
  }

  /** Starts the saga for an order. Idempotent: the order id is the saga id. */
  async start(order: Order): Promise<SagaState> {
    const existing = await this.repo.get(order.id);
    if (existing) return existing;
    const nowIso = new Date(this.clock.now()).toISOString();
    const state: SagaState = {
      sagaId: order.id,
      status: "RUNNING",
      version: 1,
      data: { order, authorizationId: null, assignment: null },
      stepIndex: 0,
      completedSteps: [],
      compensatedSteps: [],
      compensationQueue: [],
      pending: null,
      failure: null,
      lateSuccesses: [],
      log: [],
      createdAt: nowIso,
      updatedAt: nowIso,
    };
    const outbox: OutboxEntry[] = [];
    this.log(state, { event: "SAGA_STARTED" });
    this.advance(state, outbox);
    try {
      await this.repo.commit({ state, expectedVersion: 0, outbox, processedMessageId: null });
      return state;
    } catch (error) {
      if (!(error instanceof ConcurrencyError)) throw error;
      const winner = await this.repo.get(order.id);
      if (!winner) throw error;
      return winner;
    }
  }

  /**
   * Handles a participant reply. Throws ConcurrencyError on a lost race so the bus redelivers;
   * the retry then sees the newer state (or the processed marker) and does the right thing.
   */
  async handle(message: AnyMessage): Promise<void> {
    if (await this.repo.hasProcessed(message.id)) return;
    const current = await this.repo.get(message.sagaId);
    if (!current) {
      this.logger.warn("reply for unknown saga ignored", { sagaId: message.sagaId, type: message.type });
      return;
    }
    const state = structuredClone(current);
    const outbox: OutboxEntry[] = [];
    this.react(state, message, outbox);
    state.version = current.version + 1;
    await this.repo.commit({ state, expectedVersion: current.version, outbox, processedMessageId: message.id });
    this.notifyIfTerminal(current, state);
  }

  /** Fails forward steps whose deadline passed and re-sends stuck compensations. */
  async checkTimeouts(limit = 100): Promise<number> {
    const now = this.clock.now();
    let handled = 0;
    for (const sagaId of await this.repo.dueForTimeout(now, limit)) {
      const current = await this.repo.get(sagaId);
      if (!current?.pending || current.pending.deadline > now) continue;
      const state = structuredClone(current);
      const outbox: OutboxEntry[] = [];
      this.onDeadline(state, outbox);
      state.version = current.version + 1;
      try {
        await this.repo.commit({ state, expectedVersion: current.version, outbox, processedMessageId: null });
        handled++;
        this.notifyIfTerminal(current, state);
      } catch (error) {
        if (!(error instanceof ConcurrencyError)) throw error;
        // A reply won the race; the next tick re-evaluates.
      }
    }
    return handled;
  }

  get(sagaId: string): Promise<SagaState | undefined> {
    return this.repo.get(sagaId);
  }

  private react(state: SagaState, message: AnyMessage, outbox: OutboxEntry[]): void {
    const pending = state.pending;
    const meta = { messageId: message.id, messageType: message.type };

    if (pending && message.causationId === pending.messageId) {
      const step = stepByName(this.steps, pending.step);
      if (pending.kind === "action" && message.type === step.successEvent) {
        state.data = step.onSuccess ? step.onSuccess(state.data, message) : state.data;
        state.completedSteps.push(step.name);
        state.pending = null;
        state.stepIndex++;
        this.log(state, { event: "STEP_COMPLETED", step: step.name, ...meta });
        this.advance(state, outbox, message.id);
        return;
      }
      if (pending.kind === "action" && (step.failureEvents as readonly string[]).includes(message.type)) {
        const reason = "reason" in message.payload ? message.payload.reason : message.type;
        state.failure = { step: step.name, reason };
        this.log(state, { event: "STEP_FAILED", step: step.name, detail: reason, ...meta });
        this.beginCompensation(state, outbox, false, message.id);
        return;
      }
      if (pending.kind === "compensation" && message.type === step.compensation?.completedBy) {
        state.compensatedSteps.push(step.name);
        state.pending = null;
        this.log(state, { event: "COMPENSATION_COMPLETED", step: step.name, ...meta });
        this.advanceCompensation(state, outbox, message.id);
        return;
      }
    }

    // Out-of-order or stale reply. The only one that needs action is a success that arrives
    // after we gave up on that step: the side effect happened, so it must be undone.
    const lateStep = this.steps.find((s) => s.successEvent === message.type);
    const givenUp = state.status === "COMPENSATING" || state.status === "COMPENSATED" || state.status === "FAILED";
    if (
      lateStep?.compensation &&
      givenUp &&
      !state.completedSteps.includes(lateStep.name) &&
      message.causationId !== null &&
      !state.lateSuccesses.includes(message.causationId)
    ) {
      state.lateSuccesses.push(message.causationId);
      this.log(state, { event: "LATE_SUCCESS", step: lateStep.name, detail: "re-running compensation", ...meta });
      if (state.status === "COMPENSATING") {
        state.compensationQueue.push(lateStep.name);
      } else {
        state.status = "COMPENSATING";
        state.compensationQueue = [lateStep.name];
        this.advanceCompensation(state, outbox, message.id);
      }
      return;
    }
    this.log(state, { event: "MESSAGE_IGNORED", detail: "stale or duplicate reply", ...meta });
  }

  private onDeadline(state: SagaState, outbox: OutboxEntry[]): void {
    const pending = state.pending;
    if (!pending) return;
    if (pending.kind === "action") {
      state.failure = { step: pending.step, reason: "timeout" };
      this.log(state, { event: "STEP_TIMED_OUT", step: pending.step, messageId: pending.messageId });
      // Unknown outcome: the step may have happened, so compensate it too (compensations are idempotent).
      this.beginCompensation(state, outbox, true, null);
      return;
    }
    if (pending.attempt >= this.options.maxCompensationAttempts) {
      state.pending = null;
      state.status = "FAILED";
      this.log(state, {
        event: "SAGA_FAILED",
        step: pending.step,
        detail: `compensation unanswered after ${pending.attempt} attempts; manual intervention required`,
      });
      return;
    }
    // Same message id on purpose: if the participant already did it, its idempotent consumer
    // replays the cached reply; if the command was dead-lettered, it runs again.
    const step = stepByName(this.steps, pending.step);
    const spec = step.compensation?.command(state.data);
    if (!spec) return;
    const now = this.clock.now();
    state.pending = {
      ...pending,
      attempt: pending.attempt + 1,
      sentAt: now,
      deadline: now + this.options.stepTimeoutMs + backoffDelay(pending.attempt - 1, this.options.compensationBackoff),
    };
    outbox.push(this.outboxEntry(pending.stream, this.toMessage(spec, state.sagaId, pending.causationOf, pending.messageId)));
    this.log(state, { event: "COMMAND_RESENT", step: pending.step, messageId: pending.messageId, detail: `attempt ${state.pending.attempt}` });
  }

  private advance(state: SagaState, outbox: OutboxEntry[], causationId: string | null = null): void {
    const step = this.steps[state.stepIndex];
    if (!step) {
      state.status = "COMPLETED";
      state.pending = null;
      this.log(state, { event: "SAGA_COMPLETED" });
      return;
    }
    this.send(state, outbox, step, "action", step.stream, step.action(state.data), causationId);
  }

  private beginCompensation(
    state: SagaState,
    outbox: OutboxEntry[],
    includeCurrent: boolean,
    causationId: string | null,
  ): void {
    const current = state.pending?.step;
    const toUndo: StepName[] = [...state.completedSteps].reverse();
    if (includeCurrent && current) toUndo.unshift(current);
    state.status = "COMPENSATING";
    state.pending = null;
    state.compensationQueue = toUndo.filter((name) => stepByName(this.steps, name).compensation);
    this.advanceCompensation(state, outbox, causationId);
  }

  private advanceCompensation(state: SagaState, outbox: OutboxEntry[], causationId: string | null): void {
    for (let name = state.compensationQueue.shift(); name; name = state.compensationQueue.shift()) {
      const step = stepByName(this.steps, name);
      const compensation = step.compensation;
      if (!compensation) continue;
      const skipReason = compensation.skip?.(state) ?? null;
      if (skipReason) {
        this.log(state, { event: "COMPENSATION_SKIPPED", step: name, detail: skipReason });
        continue;
      }
      this.send(state, outbox, step, "compensation", compensation.stream, compensation.command(state.data), causationId);
      return;
    }
    state.status = "COMPENSATED";
    this.log(state, { event: "SAGA_COMPENSATED", detail: state.failure ? `${state.failure.step}: ${state.failure.reason}` : "" });
  }

  private send(
    state: SagaState,
    outbox: OutboxEntry[],
    step: StepDefinition,
    kind: "action" | "compensation",
    stream: string,
    spec: CommandSpec,
    causationId: string | null,
  ): void {
    const message = this.toMessage(spec, state.sagaId, causationId);
    const now = this.clock.now();
    state.pending = {
      messageId: message.id,
      causationOf: causationId,
      type: spec.type,
      stream,
      step: step.name,
      kind,
      sentAt: now,
      deadline: now + this.options.stepTimeoutMs,
      attempt: 1,
    };
    outbox.push(this.outboxEntry(stream, message));
    this.log(state, { event: "COMMAND_SENT", step: step.name, messageId: message.id, messageType: spec.type });
  }

  private toMessage(spec: CommandSpec, sagaId: string, causationId: string | null, id: string = randomUUID()): AnyMessage {
    return {
      id,
      type: spec.type,
      sagaId,
      causationId,
      payload: spec.payload,
      occurredAt: new Date(this.clock.now()).toISOString(),
    } as AnyMessage;
  }

  private outboxEntry(stream: string, message: AnyMessage): OutboxEntry {
    return { id: `${message.id}:${randomUUID()}`, stream, message, createdAt: this.clock.now() };
  }

  private log(state: SagaState, entry: Omit<SagaLogEntry, "at">): void {
    const at = new Date(this.clock.now()).toISOString();
    state.log.push({ at, ...entry });
    state.updatedAt = at;
  }

  private notifyIfTerminal(before: SagaState, after: SagaState): void {
    if (before.status !== after.status && TERMINAL_STATUSES.includes(after.status)) {
      this.logger.info("saga finished", { sagaId: after.sagaId, status: after.status, failure: after.failure });
      this.options.onTerminal?.(after);
    }
  }
}
