import type { AnyMessage } from "../domain/messages.js";
import { errorMessage } from "../shared/logger.js";
import type { Random } from "../shared/random.js";
import type {
  DeadLetter,
  MessageBus,
  MessageHandler,
  SubscribeOptions,
  Subscription,
} from "./bus.js";

export interface InMemoryBusOptions {
  /** Probability that a delivery is duplicated (simulates at-least-once redelivery). */
  duplicateRate?: number;
  /** Max random delay per delivery; > 0 lets messages overtake each other. */
  deliveryJitterMs?: number;
  random?: Random;
}

interface Entry {
  message: AnyMessage;
  deliveryCount: number;
}

interface Group {
  stream: string;
  options: SubscribeOptions;
  handler: MessageHandler;
  ready: Entry[];
  inFlight: number;
  stopped: boolean;
}

/**
 * In-process implementation of the bus contract for tests and the simulator.
 * Mirrors the Redis Streams semantics the code relies on: groups read the stream from the
 * beginning, every group gets every message, failures are redelivered and eventually
 * dead-lettered. It can also inject duplicates and reordering.
 */
export class InMemoryBus implements MessageBus {
  private readonly streams = new Map<string, AnyMessage[]>();
  private readonly groups = new Map<string, Group[]>();
  private readonly deadLetters: DeadLetter[] = [];
  private readonly deadLetterHandlers: ((letter: DeadLetter) => Promise<void>)[] = [];
  private timers = new Set<NodeJS.Timeout>();
  private readonly random: Random;

  constructor(private readonly options: InMemoryBusOptions = {}) {
    this.random = options.random ?? Math.random;
  }

  publish(stream: string, message: AnyMessage): Promise<void> {
    const log = this.streams.get(stream) ?? [];
    log.push(message);
    this.streams.set(stream, log);
    for (const group of this.groups.get(stream) ?? []) {
      this.enqueue(group, { message, deliveryCount: 0 });
      if (this.random() < (this.options.duplicateRate ?? 0)) {
        this.enqueue(group, { message, deliveryCount: 0 });
      }
    }
    return Promise.resolve();
  }

  subscribe(stream: string, options: SubscribeOptions, handler: MessageHandler): Promise<Subscription> {
    const group: Group = { stream, options, handler, ready: [], inFlight: 0, stopped: false };
    const groups = this.groups.get(stream) ?? [];
    if (groups.some((g) => g.options.group === options.group)) {
      throw new Error(`group ${options.group} already subscribed to ${stream}`);
    }
    groups.push(group);
    this.groups.set(stream, groups);
    for (const message of this.streams.get(stream) ?? []) this.enqueue(group, { message, deliveryCount: 0 });
    return Promise.resolve({
      stop: () => {
        group.stopped = true;
        return Promise.resolve();
      },
    });
  }

  subscribeDeadLetters(_group: string, handler: (letter: DeadLetter) => Promise<void>): Promise<Subscription> {
    this.deadLetterHandlers.push(handler);
    return Promise.resolve({ stop: () => Promise.resolve() });
  }

  /** Every message published to a stream, in publish order. */
  messages(stream: string): AnyMessage[] {
    return [...(this.streams.get(stream) ?? [])];
  }

  get deadLettered(): readonly DeadLetter[] {
    return this.deadLetters;
  }

  /** Resolves once nothing is queued, in flight or scheduled for redelivery. */
  async drain(maxIterations = 100_000): Promise<void> {
    for (let i = 0; i < maxIterations; i++) {
      if (this.isIdle()) {
        // Give handlers that just finished a chance to publish follow-up messages.
        await new Promise((resolve) => setImmediate(resolve));
        if (this.isIdle()) return;
      }
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    throw new Error("bus did not drain");
  }

  close(): Promise<void> {
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    for (const groups of this.groups.values()) for (const g of groups) g.stopped = true;
    return Promise.resolve();
  }

  private isIdle(): boolean {
    if (this.timers.size > 0) return false;
    for (const groups of this.groups.values()) {
      for (const g of groups) if (!g.stopped && (g.ready.length > 0 || g.inFlight > 0)) return false;
    }
    return true;
  }

  private enqueue(group: Group, entry: Entry, delayMs = 0): void {
    const jitter = this.options.deliveryJitterMs ?? 0;
    const delay = delayMs + (jitter > 0 ? Math.floor(this.random() * jitter) : 0);
    if (delay <= 0) {
      group.ready.push(entry);
      queueMicrotask(() => {
        this.pump(group);
      });
      return;
    }
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      group.ready.push(entry);
      this.pump(group);
    }, delay);
    this.timers.add(timer);
  }

  private pump(group: Group): void {
    const concurrency = group.options.concurrency ?? 1;
    while (!group.stopped && group.inFlight < concurrency && group.ready.length > 0) {
      const entry = group.ready.shift();
      if (!entry) return;
      group.inFlight++;
      void this.deliver(group, entry);
    }
  }

  private async deliver(group: Group, entry: Entry): Promise<void> {
    const deliveryCount = entry.deliveryCount + 1;
    try {
      await group.handler(entry.message, { deliveryCount });
    } catch (error) {
      if (deliveryCount >= group.options.maxDeliveries) {
        await this.deadLetter({
          stream: group.stream,
          group: group.options.group,
          deliveryCount,
          error: errorMessage(error),
          deadLetteredAt: new Date().toISOString(),
          message: entry.message,
        });
      } else {
        this.enqueue(group, { message: entry.message, deliveryCount }, group.options.redeliveryDelayMs(deliveryCount));
      }
    } finally {
      group.inFlight--;
      this.pump(group);
    }
  }

  private async deadLetter(letter: DeadLetter): Promise<void> {
    this.deadLetters.push(letter);
    for (const handler of this.deadLetterHandlers) await handler(letter);
  }
}
