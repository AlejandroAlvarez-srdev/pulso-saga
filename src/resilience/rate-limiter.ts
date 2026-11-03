import { realSleep, systemClock, type Clock, type Sleep } from "../shared/clock.js";
import { RateLimitExceededError } from "./errors.js";

export interface TokenBucketOptions {
  /** Maximum burst size. */
  capacity: number;
  refillPerSecond: number;
  clock?: Clock;
  sleep?: Sleep;
}

/**
 * Token bucket: allows bursts up to `capacity`, then a sustained `refillPerSecond`.
 * Used to respect the payment provider's request quota on our side, so we degrade by
 * queueing briefly instead of getting 429s that would also trip the circuit breaker.
 */
export class TokenBucket {
  private tokens: number;
  private lastRefill: number;
  private readonly clock: Clock;
  private readonly sleep: Sleep;

  constructor(private readonly options: TokenBucketOptions) {
    if (options.capacity <= 0 || options.refillPerSecond <= 0) {
      throw new RangeError("capacity and refillPerSecond must be positive");
    }
    this.clock = options.clock ?? systemClock;
    this.sleep = options.sleep ?? realSleep;
    this.tokens = options.capacity;
    this.lastRefill = this.clock.now();
  }

  get available(): number {
    this.refill();
    return this.tokens;
  }

  tryAcquire(count = 1): boolean {
    this.refill();
    if (this.tokens < count) return false;
    this.tokens -= count;
    return true;
  }

  /** Milliseconds until `count` tokens will be available (0 if available now). */
  waitTimeMs(count = 1): number {
    this.refill();
    if (this.tokens >= count) return 0;
    return Math.ceil(((count - this.tokens) / this.options.refillPerSecond) * 1000);
  }

  /** Waits for a token, but never longer than `maxWaitMs`. */
  async acquire(maxWaitMs: number, count = 1): Promise<void> {
    if (count > this.options.capacity) throw new RangeError("count exceeds bucket capacity");
    const deadline = this.clock.now() + maxWaitMs;
    while (!this.tryAcquire(count)) {
      const wait = this.waitTimeMs(count);
      if (this.clock.now() + wait > deadline) throw new RateLimitExceededError(wait);
      await this.sleep(wait);
    }
  }

  private refill(): void {
    const now = this.clock.now();
    const elapsed = now - this.lastRefill;
    if (elapsed <= 0) return;
    this.tokens = Math.min(this.options.capacity, this.tokens + (elapsed / 1000) * this.options.refillPerSecond);
    this.lastRefill = now;
  }
}
