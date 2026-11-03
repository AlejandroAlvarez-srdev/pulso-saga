import type { AnyMessage, EventPayloads, EventType, PayloadOf } from "../domain/messages.js";
import { Streams, type MessageBus, type Subscription } from "../messaging/bus.js";
import { IdempotentConsumer, type ProcessedMessageStore } from "../messaging/idempotency.js";
import { createMessage } from "../messaging/message.js";
import { backoffDelay, type BackoffOptions } from "../resilience/backoff.js";
import { errorMessage, silentLogger, type Logger } from "../shared/logger.js";

/** A saga participant: consumes commands from one stream and answers with events. */
export interface Participant {
  readonly name: string;
  readonly stream: string;
  /**
   * Returns the replies for a command. Business outcomes (declined, no pulser...) are
   * replies; infrastructure failures are thrown so the bus redelivers and finally dead-letters.
   */
  handle(command: AnyMessage): Promise<AnyMessage[]>;
}

export class UnsupportedCommandError extends Error {
  override readonly name = "UnsupportedCommandError";
  constructor(participant: string, type: string) {
    super(`${participant} cannot handle ${type}`);
  }
}

export function reply<T extends EventType>(command: AnyMessage, type: T, payload: EventPayloads[T]): AnyMessage {
  return createMessage<T>(type, command.sagaId, payload as PayloadOf<T>, command.id) as AnyMessage;
}

export interface ParticipantRuntimeOptions {
  store: ProcessedMessageStore;
  consumer: string;
  maxDeliveries: number;
  redeliveryBackoff: BackoffOptions;
  concurrency: number;
  logger?: Logger;
}

/** Wires a participant to the bus behind an idempotent consumer. */
export function runParticipant(
  bus: MessageBus,
  participant: Participant,
  options: ParticipantRuntimeOptions,
): Promise<Subscription> {
  const logger = options.logger ?? silentLogger;
  const idempotent = new IdempotentConsumer(participant.name, options.store);
  return bus.subscribe(
    participant.stream,
    {
      group: participant.name,
      consumer: options.consumer,
      maxDeliveries: options.maxDeliveries,
      concurrency: options.concurrency,
      redeliveryDelayMs: (count) => backoffDelay(count - 1, options.redeliveryBackoff),
    },
    async (command, ctx) => {
      try {
        const { replies } = await idempotent.process(command, () => participant.handle(command));
        // Re-publishing cached replies on a duplicate is deliberate: the first publish may be the
        // one that got lost. Replies keep their ids, so the orchestrator dedupes them.
        for (const message of replies) await bus.publish(Streams.sagaEvents, message);
      } catch (error) {
        logger.warn("command failed; will be redelivered", {
          participant: participant.name,
          type: command.type,
          sagaId: command.sagaId,
          delivery: ctx.deliveryCount,
          error: errorMessage(error),
        });
        throw error;
      }
    },
  );
}
