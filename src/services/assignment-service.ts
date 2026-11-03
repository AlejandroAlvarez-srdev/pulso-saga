import type { AnyMessage } from "../domain/messages.js";
import type { Order } from "../domain/order.js";
import { etaMinutes, isAvailable, scorePulser, type Assignment, type Pulser } from "../domain/pulser.js";
import { Streams } from "../messaging/bus.js";
import type { Bulkhead } from "../resilience/bulkhead.js";
import { withTimeout } from "../resilience/timeout.js";
import type { Random } from "../shared/random.js";
import { reply, UnsupportedCommandError, type Participant } from "./participant.js";

export interface Candidate {
  pulserId: string;
  score: number;
  etaMinutes: number;
}

/** The (slow, remote) matching engine the assignment service calls. */
export interface DispatchEngine {
  rank(order: Order, pulsers: readonly Pulser[], signal: AbortSignal): Promise<Candidate[]>;
}

/** Scores every available pulser after a simulated network/compute delay. */
export class FakeDispatchEngine implements DispatchEngine {
  constructor(
    private readonly latencyMs: { min: number; max: number },
    private readonly random: Random = Math.random,
  ) {}

  rank(order: Order, pulsers: readonly Pulser[], signal: AbortSignal): Promise<Candidate[]> {
    const delay = this.latencyMs.min + Math.floor(this.random() * (this.latencyMs.max - this.latencyMs.min + 1));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        resolve(
          pulsers
            .filter(isAvailable)
            .map((p) => ({ pulserId: p.id, score: scorePulser(p, order.pickup), etaMinutes: etaMinutes(p, order.pickup) }))
            .sort((a, b) => b.score - a.score),
        );
      }, delay);
      signal.addEventListener("abort", () => {
        clearTimeout(timer);
        reject(new Error("aborted by caller"));
      });
    });
  }
}

export interface AssignmentServiceOptions {
  pulsers: Pulser[];
  engine: DispatchEngine;
  /** Caps concurrent calls into the dispatch engine. */
  bulkhead: Bulkhead;
  callTimeoutMs: number;
  now?: () => Date;
}

export class AssignmentService implements Participant {
  readonly name = "assignment-service";
  readonly stream = Streams.assignmentCommands;
  private readonly pulsers: Map<string, Pulser>;
  private readonly assignments = new Map<string, Assignment>();
  private readonly released = new Set<string>();
  private readonly now: () => Date;

  constructor(private readonly options: AssignmentServiceOptions) {
    this.pulsers = new Map(options.pulsers.map((p) => [p.id, { ...p }]));
    this.now = options.now ?? (() => new Date());
  }

  pulser(id: string): Pulser | undefined {
    return this.pulsers.get(id);
  }

  assignmentFor(orderId: string): Assignment | undefined {
    return this.assignments.get(orderId);
  }

  async handle(command: AnyMessage): Promise<AnyMessage[]> {
    switch (command.type) {
      case "pulser.assign": {
        const { order } = command.payload;
        const existing = this.assignments.get(order.id);
        if (existing) return [reply(command, "pulser.assigned", existing)];
        if (this.released.has(order.id)) {
          return [reply(command, "assignment.failed", { orderId: order.id, reason: "order_released" })];
        }
        // Bulkhead rejections and timeouts propagate: the bus retries them later.
        const candidates = await this.options.bulkhead.run(() =>
          withTimeout((signal) => this.options.engine.rank(order, [...this.pulsers.values()], signal), this.options.callTimeoutMs),
        );
        // Capacity may have changed while the engine was thinking; take the best still free.
        const chosen = candidates.find((c) => {
          const p = this.pulsers.get(c.pulserId);
          return p !== undefined && isAvailable(p);
        });
        if (this.released.has(order.id)) {
          return [reply(command, "assignment.failed", { orderId: order.id, reason: "order_released" })];
        }
        if (!chosen) {
          return [reply(command, "assignment.failed", { orderId: order.id, reason: "no_pulser_available" })];
        }
        const pulser = this.pulsers.get(chosen.pulserId);
        if (pulser) pulser.activeTasks++;
        const assignment: Assignment = {
          orderId: order.id,
          pulserId: chosen.pulserId,
          score: chosen.score,
          etaMinutes: chosen.etaMinutes,
          assignedAt: this.now().toISOString(),
        };
        this.assignments.set(order.id, assignment);
        return [reply(command, "pulser.assigned", assignment)];
      }
      case "pulser.release": {
        const { orderId } = command.payload;
        this.released.add(orderId);
        const assignment = this.assignments.get(orderId);
        if (!assignment) return [reply(command, "pulser.released", { orderId, pulserId: null })];
        const pulser = this.pulsers.get(assignment.pulserId);
        if (pulser) pulser.activeTasks = Math.max(0, pulser.activeTasks - 1);
        this.assignments.delete(orderId);
        return [reply(command, "pulser.released", { orderId, pulserId: assignment.pulserId })];
      }
      default:
        throw new UnsupportedCommandError(this.name, command.type);
    }
  }
}
