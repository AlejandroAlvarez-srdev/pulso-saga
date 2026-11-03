import type { AnyMessage, CommandPayloads, CommandType, EventType } from "../domain/messages.js";
import { Streams } from "../messaging/bus.js";
import type { SagaData, SagaState, StepName } from "./state.js";

export type CommandSpec = { [K in CommandType]: { type: K; payload: CommandPayloads[K] } }[CommandType];

export interface CompensationDefinition {
  stream: string;
  command: (data: SagaData) => CommandSpec;
  completedBy: EventType;
  /** Return a reason to skip (e.g. no need to release an authorization that was captured). */
  skip?: (state: SagaState) => string | null;
}

export interface StepDefinition {
  name: StepName;
  stream: string;
  action: (data: SagaData) => CommandSpec;
  successEvent: EventType;
  failureEvents: EventType[];
  /** Folds data from the success reply into the saga (authorization id, assignment...). */
  onSuccess?: (data: SagaData, reply: AnyMessage) => SagaData;
  /** Absent for the pivot/last step: nothing after it can fail. */
  compensation?: CompensationDefinition;
}

/**
 * The Pulso order saga.
 *
 *   createOrder → authorizePayment → assignPulser → capturePayment → confirmOrder
 *   cancel order ← release auth    ← release pulser ← refund
 *
 * Compensations run in reverse order of the steps that completed.
 */
export const orderSaga: readonly StepDefinition[] = [
  {
    name: "createOrder",
    stream: Streams.orderCommands,
    action: (d) => ({ type: "order.create", payload: { order: d.order } }),
    successEvent: "order.created",
    failureEvents: ["order.rejected"],
    compensation: {
      stream: Streams.orderCommands,
      command: (d) => ({ type: "order.cancel", payload: { orderId: d.order.id, reason: "saga_compensation" } }),
      completedBy: "order.cancelled",
    },
  },
  {
    name: "authorizePayment",
    stream: Streams.paymentCommands,
    action: (d) => ({
      type: "payment.authorize",
      payload: {
        orderId: d.order.id,
        merchantId: d.order.merchantId,
        amountCents: d.order.amountCents,
        currency: d.order.currency,
      },
    }),
    successEvent: "payment.authorized",
    failureEvents: ["payment.failed"],
    onSuccess: (d, reply) =>
      reply.type === "payment.authorized" ? { ...d, authorizationId: reply.payload.authorizationId } : d,
    compensation: {
      stream: Streams.paymentCommands,
      command: (d) => ({ type: "payment.release", payload: { orderId: d.order.id } }),
      completedBy: "payment.released",
      skip: (s) =>
        s.completedSteps.includes("capturePayment") ? "authorization was captured; refund covers it" : null,
    },
  },
  {
    name: "assignPulser",
    stream: Streams.assignmentCommands,
    action: (d) => ({ type: "pulser.assign", payload: { order: d.order } }),
    successEvent: "pulser.assigned",
    failureEvents: ["assignment.failed"],
    onSuccess: (d, reply) => (reply.type === "pulser.assigned" ? { ...d, assignment: reply.payload } : d),
    compensation: {
      stream: Streams.assignmentCommands,
      command: (d) => ({ type: "pulser.release", payload: { orderId: d.order.id } }),
      completedBy: "pulser.released",
    },
  },
  {
    name: "capturePayment",
    stream: Streams.paymentCommands,
    action: (d) => ({ type: "payment.capture", payload: { orderId: d.order.id } }),
    successEvent: "payment.captured",
    failureEvents: ["payment.capture_failed"],
    compensation: {
      stream: Streams.paymentCommands,
      command: (d) => ({ type: "payment.refund", payload: { orderId: d.order.id } }),
      completedBy: "refund.issued",
    },
  },
  {
    name: "confirmOrder",
    stream: Streams.orderCommands,
    action: (d) => ({
      type: "order.confirm",
      payload: { orderId: d.order.id, pulserId: d.assignment?.pulserId ?? "" },
    }),
    successEvent: "order.confirmed",
    failureEvents: ["order.confirm_failed"],
  },
];

export function stepByName(steps: readonly StepDefinition[], name: StepName): StepDefinition {
  const step = steps.find((s) => s.name === name);
  if (!step) throw new Error(`unknown saga step ${name}`);
  return step;
}
