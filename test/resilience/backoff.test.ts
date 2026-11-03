import { describe, expect, it, vi } from "vitest";
import { backoffDelay } from "../../src/resilience/backoff.js";
import { retry } from "../../src/resilience/retry.js";
import { seededRandom } from "../../src/shared/random.js";

describe("backoffDelay", () => {
  const opts = { baseMs: 100, maxMs: 2_000, factor: 2 };

  it("grows exponentially and is capped without jitter", () => {
    const delays = [0, 1, 2, 3, 4, 5, 6].map((a) => backoffDelay(a, { ...opts, jitter: "none" }));
    expect(delays).toEqual([100, 200, 400, 800, 1600, 2000, 2000]);
  });

  it("full jitter stays within [0, cap)", () => {
    const random = seededRandom(42);
    for (let attempt = 0; attempt < 8; attempt++) {
      const cap = Math.min(2_000, 100 * 2 ** attempt);
      for (let i = 0; i < 200; i++) {
        const d = backoffDelay(attempt, { ...opts, jitter: "full" }, random);
        expect(d).toBeGreaterThanOrEqual(0);
        expect(d).toBeLessThan(cap);
      }
    }
  });

  it("equal jitter keeps at least half of the capped delay", () => {
    expect(backoffDelay(3, { ...opts, jitter: "equal" }, () => 0)).toBe(400);
    expect(backoffDelay(3, { ...opts, jitter: "equal" }, () => 0.999)).toBe(799);
  });

  it("spreads retries of many clients (no thundering herd)", () => {
    const random = seededRandom(7);
    const delays = new Set(Array.from({ length: 100 }, () => backoffDelay(4, { ...opts, jitter: "full" }, random)));
    expect(delays.size).toBeGreaterThan(90);
  });

  it("rejects invalid attempts", () => {
    expect(() => backoffDelay(-1, opts)).toThrow(RangeError);
  });
});

describe("retry", () => {
  it("retries retryable errors with backoff and returns the eventual result", async () => {
    const sleep = vi.fn(() => Promise.resolve());
    let calls = 0;
    const result = await retry(
      () => {
        calls++;
        return calls < 3 ? Promise.reject(new Error("flaky")) : Promise.resolve("done");
      },
      { maxAttempts: 5, backoff: { baseMs: 10, maxMs: 1_000, jitter: "none" }, sleep },
    );
    expect(result).toBe("done");
    expect(calls).toBe(3);
    expect(sleep.mock.calls).toEqual([[10], [20]]);
  });

  it("stops at maxAttempts and rethrows the last error", async () => {
    let calls = 0;
    await expect(
      retry(
        () => {
          calls++;
          return Promise.reject(new Error(`fail ${calls}`));
        },
        { maxAttempts: 3, backoff: { baseMs: 1, maxMs: 1 }, sleep: () => Promise.resolve() },
      ),
    ).rejects.toThrow("fail 3");
    expect(calls).toBe(3);
  });

  it("does not retry non-retryable errors", async () => {
    let calls = 0;
    await expect(
      retry(
        () => {
          calls++;
          return Promise.reject(new TypeError("bad request"));
        },
        {
          maxAttempts: 5,
          backoff: { baseMs: 1, maxMs: 1 },
          isRetryable: (e) => !(e instanceof TypeError),
          sleep: () => Promise.resolve(),
        },
      ),
    ).rejects.toThrow("bad request");
    expect(calls).toBe(1);
  });
});
