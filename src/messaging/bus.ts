import type { AnyMessage } from "../domain/messages.js";

export const Streams = {
  orderCommands: "pulso.commands.order",
  paymentCommands: "pulso.commands.payment",
  assignmentCommands: "pulso.commands.assignment",
  /** All participant replies flow back to the orchestrator on one stream. */
  sagaEvents: "pulso.events.saga",
  deadLetter: "pulso.dlq",
} as const;

export type StreamName = (typeof Streams)[keyof typeof Streams];

export interface DeliveryContext {
  /** 1 on first delivery; grows with every redelivery of the same message. */
  deliveryCount: number;
}

export type MessageHandler = (message: AnyMessage, ctx: DeliveryContext) => Promise<void>;

export interface SubscribeOptions {
  /** Consumer group: each message is delivered to one consumer per group. */
  group: string;
  /** Consumer name inside the group (used by Redis to track pending entries). */
  consumer: string;
  /** After this many failed deliveries the message is moved to the dead-letter stream. */
  maxDeliveries: number;
  /** Delay before a failed message becomes visible again. */
  redeliveryDelayMs: (deliveryCount: number) => number;
  /** Messages handled concurrently by this subscription. */
  concurrency?: number;
}

/** What lands on the dead-letter stream: the original message plus why it gave up. */
export interface DeadLetter {
  stream: string;
  group: string;
  deliveryCount: number;
  error: string;
  deadLetteredAt: string;
  message: AnyMessage;
}

export interface Subscription {
  stop(): Promise<void>;
}

/**
 * At-least-once message bus with consumer groups, acknowledgements, redelivery and a
 * dead-letter stream. Handlers must be idempotent: a message may be delivered more than once.
 * A handler that resolves acknowledges the message; one that throws asks for redelivery.
 */
export interface MessageBus {
  publish(stream: string, message: AnyMessage): Promise<void>;
  subscribe(stream: string, options: SubscribeOptions, handler: MessageHandler): Promise<Subscription>;
  subscribeDeadLetters(group: string, handler: (letter: DeadLetter) => Promise<void>): Promise<Subscription>;
  close(): Promise<void>;
}
