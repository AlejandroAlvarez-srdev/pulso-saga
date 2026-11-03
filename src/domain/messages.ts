import type { Order, OrderStatus } from "./order.js";
import type { Assignment } from "./pulser.js";

/** Commands sent by the orchestrator to the participant services. */
export interface CommandPayloads {
  "order.create": { order: Order };
  "order.confirm": { orderId: string; pulserId: string };
  "order.cancel": { orderId: string; reason: string };
  "payment.authorize": { orderId: string; merchantId: string; amountCents: number; currency: "MXN" };
  "payment.capture": { orderId: string };
  "payment.release": { orderId: string };
  "payment.refund": { orderId: string };
  "pulser.assign": { order: Order };
  "pulser.release": { orderId: string };
}

/** Events (replies) emitted by the participant services. */
export interface EventPayloads {
  "order.created": { orderId: string; status: OrderStatus };
  "order.rejected": { orderId: string; reason: string };
  "order.confirmed": { orderId: string; pulserId: string; status: OrderStatus };
  "order.confirm_failed": { orderId: string; reason: string };
  "order.cancelled": { orderId: string; reason: string };
  "payment.authorized": { orderId: string; authorizationId: string };
  "payment.failed": { orderId: string; reason: string };
  "payment.captured": { orderId: string; authorizationId: string };
  "payment.capture_failed": { orderId: string; reason: string };
  "payment.released": { orderId: string; noop: boolean };
  "refund.issued": { orderId: string; amountCents: number; noop: boolean };
  "pulser.assigned": Assignment;
  "assignment.failed": { orderId: string; reason: string };
  "pulser.released": { orderId: string; pulserId: string | null };
}

export type CommandType = keyof CommandPayloads;
export type EventType = keyof EventPayloads;
export type MessageType = CommandType | EventType;
export type PayloadOf<T extends MessageType> = T extends CommandType
  ? CommandPayloads[T]
  : T extends EventType
    ? EventPayloads[T]
    : never;

/**
 * Envelope shared by every message on the bus.
 * `id` is the dedupe key used by idempotent consumers; `causationId` links a reply to the
 * command that produced it, which is how the orchestrator detects stale/out-of-order replies.
 */
export interface Message<T extends MessageType = MessageType> {
  id: string;
  type: T;
  sagaId: string;
  causationId: string | null;
  payload: PayloadOf<T>;
  occurredAt: string;
}

export type AnyMessage = { [K in MessageType]: Message<K> }[MessageType];
