import type { CommandType } from "../domain/messages.js";
import type { Order } from "../domain/order.js";
import type { Assignment } from "../domain/pulser.js";

export type StepName = "createOrder" | "authorizePayment" | "assignPulser" | "capturePayment" | "confirmOrder";

/**
 * RUNNING       executing forward steps
 * COMPENSATING  a step failed or timed out; undoing completed steps in reverse order
 * COMPLETED     every step succeeded (terminal)
 * COMPENSATED   every needed compensation succeeded (terminal, consistent)
 * FAILED        a compensation kept failing; needs a human (terminal, inconsistent)
 */
export type SagaStatus = "RUNNING" | "COMPENSATING" | "COMPLETED" | "COMPENSATED" | "FAILED";

export interface SagaData {
  order: Order;
  authorizationId: string | null;
  assignment: Assignment | null;
}

export interface PendingCommand {
  messageId: string;
  /** Id of the message that triggered this command (kept so a re-send is byte-identical). */
  causationOf: string | null;
  type: CommandType;
  stream: string;
  step: StepName;
  kind: "action" | "compensation";
  sentAt: number;
  deadline: number;
  attempt: number;
}

export type SagaLogEvent =
  | "SAGA_STARTED"
  | "COMMAND_SENT"
  | "COMMAND_RESENT"
  | "STEP_COMPLETED"
  | "STEP_FAILED"
  | "STEP_TIMED_OUT"
  | "COMPENSATION_SKIPPED"
  | "COMPENSATION_COMPLETED"
  | "LATE_SUCCESS"
  | "MESSAGE_IGNORED"
  | "SAGA_COMPLETED"
  | "SAGA_COMPENSATED"
  | "SAGA_FAILED";

export interface SagaLogEntry {
  at: string;
  event: SagaLogEvent;
  step?: StepName;
  messageId?: string;
  messageType?: string;
  detail?: string;
}

/** Persisted per order. `version` drives optimistic concurrency on every commit. */
export interface SagaState {
  sagaId: string;
  status: SagaStatus;
  version: number;
  data: SagaData;
  stepIndex: number;
  completedSteps: StepName[];
  compensatedSteps: StepName[];
  compensationQueue: StepName[];
  pending: PendingCommand | null;
  failure: { step: StepName; reason: string } | null;
  /** Causation ids of late successes already compensated (dedupe across re-emitted replies). */
  lateSuccesses: string[];
  log: SagaLogEntry[];
  createdAt: string;
  updatedAt: string;
}

export const TERMINAL_STATUSES: readonly SagaStatus[] = ["COMPLETED", "COMPENSATED", "FAILED"];
