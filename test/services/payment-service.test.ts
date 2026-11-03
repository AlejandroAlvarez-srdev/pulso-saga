import { describe, expect, it } from "vitest";
import type { AnyMessage } from "../../src/domain/messages.js";
import { createMessage } from "../../src/messaging/message.js";
import { CircuitBreaker } from "../../src/resilience/circuit-breaker.js";
import { TokenBucket } from "../../src/resilience/rate-limiter.js";
import {
  PaymentDeclinedError,
  ProviderUnavailableError,
  type PaymentProvider,
} from "../../src/services/payment-provider.js";
import { PaymentService } from "../../src/services/payment-service.js";

type Script = ("ok" | "503" | "decline" | "hang")[];

/** Provider that follows a script of outcomes, one per call. */
function scriptedProvider(script: Script) {
  const calls: string[] = [];
  const next = async (op: string, signal: AbortSignal) => {
    calls.push(op);
    const outcome = script.shift() ?? "ok";
    if (outcome === "503") throw new ProviderUnavailableError("503");
    if (outcome === "decline") throw new PaymentDeclinedError("declined");
    if (outcome === "hang") {
      await new Promise((_, reject) => {
        signal.addEventListener("abort", () => {
          reject(new Error("aborted"));
        });
      });
    }
  };
  const provider: PaymentProvider = {
    authorize: async (_req, signal) => {
      await next("authorize", signal);
      return { authorizationId: "auth_1" };
    },
    capture: (_id, signal) => next("capture", signal),
    void: (_id, signal) => next("void", signal),
    refund: (_id, _amount, signal) => next("refund", signal),
  };
  return { provider, calls };
}

function service(script: Script) {
  const { provider, calls } = scriptedProvider(script);
  let retries = 0;
  const payments = new PaymentService({
    provider,
    breaker: new CircuitBreaker({
      name: "p",
      windowSize: 10,
      minimumCalls: 10,
      failureRateThreshold: 0.5,
      openDurationMs: 1_000,
      halfOpenMaxCalls: 1,
      isFailure: (e) => !(e instanceof PaymentDeclinedError),
    }),
    limiter: new TokenBucket({ capacity: 10, refillPerSecond: 100 }),
    rateLimitMaxWaitMs: 100,
    callTimeoutMs: 20,
    retry: { maxAttempts: 3, backoff: { baseMs: 1, maxMs: 1, jitter: "none" } },
    onRetry: () => {
      retries++;
    },
  });
  return { payments, calls, retries: () => retries };
}

const authorize = (orderId = "o-1"): AnyMessage =>
  createMessage("payment.authorize", orderId, { orderId, merchantId: "m-1", amountCents: 12_000, currency: "MXN" });
const release = (orderId = "o-1"): AnyMessage => createMessage("payment.release", orderId, { orderId });

describe("PaymentService", () => {
  it("retries transient provider errors and timeouts, then authorizes", async () => {
    const s = service(["503", "hang", "ok"]);
    const [reply] = await s.payments.handle(authorize());
    expect(reply?.type).toBe("payment.authorized");
    expect(s.calls).toEqual(["authorize", "authorize", "authorize"]);
    expect(s.retries()).toBe(2);
  });

  it("throws after retries are exhausted so the bus can redeliver / dead-letter", async () => {
    const s = service(["503", "503", "503"]);
    await expect(s.payments.handle(authorize())).rejects.toBeInstanceOf(ProviderUnavailableError);
    expect(s.payments.status("o-1")).toBeUndefined();
  });

  it("turns a decline into payment.failed without retrying", async () => {
    const s = service(["decline"]);
    const [reply] = await s.payments.handle(authorize());
    expect(reply?.type).toBe("payment.failed");
    expect(s.calls).toHaveLength(1);
  });

  it("a release that arrives before the authorize leaves a tombstone", async () => {
    const s = service([]);
    const [released] = await s.payments.handle(release());
    expect(released?.payload).toEqual({ orderId: "o-1", noop: true });
    const [failed] = await s.payments.handle(authorize());
    expect(failed?.type).toBe("payment.failed");
    expect(s.calls).toEqual([]);
  });

  it("voids an authorization when a release raced an in-flight authorize", async () => {
    const s = service(["ok", "ok"]);
    const inFlight = s.payments.handle(authorize());
    await s.payments.handle(release());
    const [reply] = await inFlight;
    expect(reply?.type).toBe("payment.failed");
    expect(s.calls).toEqual(["authorize", "void"]);
  });

  it("refund is a no-op unless the payment was captured", async () => {
    const s = service([]);
    const [reply] = await s.payments.handle(createMessage("payment.refund", "o-9", { orderId: "o-9" }));
    expect(reply?.payload).toEqual({ orderId: "o-9", amountCents: 0, noop: true });
    expect(s.calls).toEqual([]);
  });
});
