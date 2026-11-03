import { describe, expect, it } from "vitest";
import { TimeoutError } from "../../src/resilience/errors.js";
import { withTimeout } from "../../src/resilience/timeout.js";

describe("withTimeout", () => {
  it("rejects with TimeoutError and aborts the callee", async () => {
    let aborted = false;
    const error = await withTimeout(
      (signal) =>
        new Promise((_, reject) => {
          signal.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("aborted"));
          });
        }),
      20,
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(TimeoutError);
    expect(aborted).toBe(true);
  });

  it("returns the value when the call finishes in time", async () => {
    await expect(withTimeout(() => Promise.resolve(42), 50)).resolves.toBe(42);
  });
});
