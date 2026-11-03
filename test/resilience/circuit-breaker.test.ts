import { describe, expect, it } from "vitest";
import { CircuitBreaker, type CircuitState } from "../../src/resilience/circuit-breaker.js";
import { CircuitOpenError } from "../../src/resilience/errors.js";
import { ManualClock } from "../../src/shared/clock.js";

class Declined extends Error {}

function setup() {
  const clock = new ManualClock(1_000);
  const transitions: string[] = [];
  const breaker = new CircuitBreaker({
    name: "payments",
    windowSize: 4,
    minimumCalls: 4,
    failureRateThreshold: 0.5,
    openDurationMs: 5_000,
    halfOpenMaxCalls: 2,
    isFailure: (error) => !(error instanceof Declined),
    clock,
    onStateChange: (from: CircuitState, to: CircuitState) => transitions.push(`${from}->${to}`),
  });
  return { breaker, clock, transitions };
}

const ok = () => Promise.resolve("ok");
const boom = () => Promise.reject(new Error("provider 503"));

describe("CircuitBreaker", () => {
  it("stays CLOSED below the minimum number of calls", async () => {
    const { breaker } = setup();
    for (let i = 0; i < 3; i++) await expect(breaker.execute(boom)).rejects.toThrow("503");
    expect(breaker.currentState).toBe("CLOSED");
  });

  it("opens when the failure rate over the window reaches the threshold", async () => {
    const { breaker, transitions } = setup();
    await breaker.execute(ok);
    await breaker.execute(ok);
    await expect(breaker.execute(boom)).rejects.toThrow();
    expect(breaker.currentState).toBe("CLOSED");
    await expect(breaker.execute(boom)).rejects.toThrow();
    expect(breaker.currentState).toBe("OPEN");
    expect(transitions).toEqual(["CLOSED->OPEN"]);
  });

  it("fails fast while OPEN without calling the dependency", async () => {
    const { breaker } = setup();
    for (let i = 0; i < 4; i++) await expect(breaker.execute(boom)).rejects.toThrow();
    let called = false;
    const error = await breaker
      .execute(() => {
        called = true;
        return ok();
      })
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(CircuitOpenError);
    expect((error as CircuitOpenError).retryAfterMs).toBe(5_000);
    expect(called).toBe(false);
  });

  it("moves to HALF_OPEN after the open duration and closes after successful trials", async () => {
    const { breaker, clock, transitions } = setup();
    for (let i = 0; i < 4; i++) await expect(breaker.execute(boom)).rejects.toThrow();
    clock.advance(4_999);
    expect(breaker.currentState).toBe("OPEN");
    clock.advance(1);
    expect(breaker.currentState).toBe("HALF_OPEN");
    await breaker.execute(ok);
    expect(breaker.currentState).toBe("HALF_OPEN");
    await breaker.execute(ok);
    expect(breaker.currentState).toBe("CLOSED");
    expect(transitions).toEqual(["CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"]);
  });

  it("re-opens on a failed trial call", async () => {
    const { breaker, clock, transitions } = setup();
    for (let i = 0; i < 4; i++) await expect(breaker.execute(boom)).rejects.toThrow();
    clock.advance(5_000);
    await expect(breaker.execute(boom)).rejects.toThrow("503");
    expect(breaker.currentState).toBe("OPEN");
    expect(transitions).toEqual(["CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->OPEN"]);
  });

  it("limits concurrent trial calls while HALF_OPEN", async () => {
    const { breaker, clock } = setup();
    for (let i = 0; i < 4; i++) await expect(breaker.execute(boom)).rejects.toThrow();
    clock.advance(5_000);
    const release: (() => void)[] = [];
    const slow = () =>
      new Promise<string>((resolve) => {
        release.push(() => {
          resolve("ok");
        });
      });
    const first = breaker.execute(slow);
    const second = breaker.execute(slow);
    await expect(breaker.execute(ok)).rejects.toBeInstanceOf(CircuitOpenError);
    release.forEach((fn) => {
      fn();
    });
    await Promise.all([first, second]);
    expect(breaker.currentState).toBe("CLOSED");
  });

  it("does not count business errors (e.g. card declined) as failures", async () => {
    const { breaker } = setup();
    for (let i = 0; i < 10; i++) {
      await expect(breaker.execute(() => Promise.reject(new Declined("declined")))).rejects.toThrow();
    }
    expect(breaker.currentState).toBe("CLOSED");
  });
});
