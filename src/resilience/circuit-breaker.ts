import { systemClock, type Clock } from "../shared/clock.js";
import { CircuitOpenError } from "./errors.js";

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface CircuitBreakerOptions {
  name: string;
  /** Number of most recent calls considered when computing the failure rate. */
  windowSize: number;
  /** Do not trip before this many calls have been observed in the window. */
  minimumCalls: number;
  /** 0..1 — trip when failures / calls in window >= this. */
  failureRateThreshold: number;
  /** How long to stay OPEN before letting trial calls through. */
  openDurationMs: number;
  /** Concurrent trial calls allowed while HALF_OPEN; all must succeed to close. */
  halfOpenMaxCalls: number;
  /** Errors for which this returns false (e.g. a card decline) count as successes. */
  isFailure?: (error: unknown) => boolean;
  clock?: Clock;
  onStateChange?: (from: CircuitState, to: CircuitState) => void;
}

/**
 * Count-based sliding-window circuit breaker.
 *
 * CLOSED   → calls pass; outcomes recorded. Failure rate over the window ≥ threshold → OPEN.
 * OPEN     → calls fail fast with CircuitOpenError until openDurationMs elapses → HALF_OPEN.
 * HALF_OPEN→ up to halfOpenMaxCalls trial calls. Any failure → OPEN; all succeed → CLOSED.
 */
export class CircuitBreaker {
  private state: CircuitState = "CLOSED";
  private window: boolean[] = [];
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private halfOpenSuccesses = 0;
  private readonly clock: Clock;
  private readonly isFailure: (error: unknown) => boolean;

  constructor(private readonly options: CircuitBreakerOptions) {
    if (options.failureRateThreshold <= 0 || options.failureRateThreshold > 1) {
      throw new RangeError("failureRateThreshold must be in (0, 1]");
    }
    this.clock = options.clock ?? systemClock;
    this.isFailure = options.isFailure ?? (() => true);
  }

  get currentState(): CircuitState {
    this.maybeHalfOpen();
    return this.state;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.maybeHalfOpen();
    if (this.state === "OPEN") {
      throw new CircuitOpenError(this.options.name, this.openedAt + this.options.openDurationMs - this.clock.now());
    }
    if (this.state === "HALF_OPEN") {
      if (this.halfOpenInFlight >= this.options.halfOpenMaxCalls) {
        throw new CircuitOpenError(this.options.name, 0);
      }
      this.halfOpenInFlight++;
    }
    const trial = this.state === "HALF_OPEN";
    try {
      const result = await fn();
      this.onSuccess(trial);
      return result;
    } catch (error) {
      if (this.isFailure(error)) this.onFailure(trial);
      else this.onSuccess(trial);
      throw error;
    }
  }

  private onSuccess(trial: boolean): void {
    if (trial) {
      this.halfOpenInFlight--;
      if (this.state !== "HALF_OPEN") return;
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.options.halfOpenMaxCalls) this.transition("CLOSED");
      return;
    }
    this.record(true);
  }

  private onFailure(trial: boolean): void {
    if (trial) {
      this.halfOpenInFlight--;
      if (this.state === "HALF_OPEN") this.transition("OPEN");
      return;
    }
    this.record(false);
    if (this.state !== "CLOSED" || this.window.length < this.options.minimumCalls) return;
    const failures = this.window.filter((ok) => !ok).length;
    if (failures / this.window.length >= this.options.failureRateThreshold) this.transition("OPEN");
  }

  private record(ok: boolean): void {
    this.window.push(ok);
    if (this.window.length > this.options.windowSize) this.window.shift();
  }

  private maybeHalfOpen(): void {
    if (this.state === "OPEN" && this.clock.now() - this.openedAt >= this.options.openDurationMs) {
      this.transition("HALF_OPEN");
    }
  }

  private transition(to: CircuitState): void {
    const from = this.state;
    if (from === to) return;
    this.state = to;
    if (to === "OPEN") this.openedAt = this.clock.now();
    if (to === "HALF_OPEN") {
      this.halfOpenInFlight = 0;
      this.halfOpenSuccesses = 0;
    }
    if (to === "CLOSED") this.window = [];
    this.options.onStateChange?.(from, to);
  }
}
