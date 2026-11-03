import { describe, expect, it } from "vitest";
import { RateLimitExceededError } from "../../src/resilience/errors.js";
import { TokenBucket } from "../../src/resilience/rate-limiter.js";
import { ManualClock } from "../../src/shared/clock.js";

describe("TokenBucket", () => {
  it("allows a burst up to capacity, then refills over time", () => {
    const clock = new ManualClock();
    const bucket = new TokenBucket({ capacity: 3, refillPerSecond: 2, clock });
    expect([bucket.tryAcquire(), bucket.tryAcquire(), bucket.tryAcquire(), bucket.tryAcquire()]).toEqual([
      true,
      true,
      true,
      false,
    ]);
    expect(bucket.waitTimeMs()).toBe(500);
    clock.advance(500);
    expect(bucket.tryAcquire()).toBe(true);
    clock.advance(10_000);
    expect(bucket.available).toBe(3);
  });

  it("acquire waits for the next token using the injected sleep", async () => {
    const clock = new ManualClock();
    const slept: number[] = [];
    const bucket = new TokenBucket({
      capacity: 1,
      refillPerSecond: 4,
      clock,
      sleep: (ms) => {
        slept.push(ms);
        clock.advance(ms);
        return Promise.resolve();
      },
    });
    await bucket.acquire(1_000);
    await bucket.acquire(1_000);
    expect(slept).toEqual([250]);
  });

  it("acquire gives up when the wait would exceed maxWaitMs", async () => {
    const clock = new ManualClock();
    const bucket = new TokenBucket({ capacity: 1, refillPerSecond: 1, clock });
    bucket.tryAcquire();
    await expect(bucket.acquire(100)).rejects.toBeInstanceOf(RateLimitExceededError);
  });
});
