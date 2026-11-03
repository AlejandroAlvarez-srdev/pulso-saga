import { randomUUID } from "node:crypto";
import type { Message, MessageType, PayloadOf } from "../domain/messages.js";

export function createMessage<T extends MessageType>(
  type: T,
  sagaId: string,
  payload: PayloadOf<T>,
  causationId: string | null = null,
  occurredAt: Date = new Date(),
): Message<T> {
  return { id: randomUUID(), type, sagaId, causationId, payload, occurredAt: occurredAt.toISOString() };
}
