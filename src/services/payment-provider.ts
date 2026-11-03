import { randomUUID } from "node:crypto";
import type { Random } from "../shared/random.js";

/** 5xx / connection reset: transient, retryable, counts against the circuit breaker. */
export class ProviderUnavailableError extends Error {
  override readonly name = "ProviderUnavailableError";
}

/** Card declined: a business answer. Never retried, never trips the breaker. */
export class PaymentDeclinedError extends Error {
  override readonly name = "PaymentDeclinedError";
}

export interface AuthorizeRequest {
  /** Providers dedupe on this, so retrying an authorize after a timeout cannot double-charge. */
  idempotencyKey: string;
  amountCents: number;
  currency: "MXN";
}

export interface PaymentProvider {
  authorize(request: AuthorizeRequest, signal: AbortSignal): Promise<{ authorizationId: string }>;
  capture(authorizationId: string, signal: AbortSignal): Promise<void>;
  void(authorizationId: string, signal: AbortSignal): Promise<void>;
  refund(authorizationId: string, amountCents: number, signal: AbortSignal): Promise<void>;
}

export interface FakeProviderOptions {
  /** Probability a call fails with a 5xx. */
  failureRate: number;
  /** Probability a call hangs until the caller aborts it. */
  timeoutRate: number;
  /** Probability an authorize is declined. */
  declineRate: number;
  latencyMs: { min: number; max: number };
  random?: Random;
}

export interface ProviderStats {
  calls: number;
  failures: number;
  hangs: number;
  declines: number;
}

/** A deliberately unreliable payment gateway for tests and fault-injection runs. */
export class FakePaymentProvider implements PaymentProvider {
  readonly stats: ProviderStats = { calls: 0, failures: 0, hangs: 0, declines: 0 };
  private readonly authorizations = new Map<string, string>();
  private readonly random: Random;

  constructor(private options: FakeProviderOptions) {
    this.random = options.random ?? Math.random;
  }

  /** Change fault rates at runtime (e.g. simulate an outage window). */
  configure(patch: Partial<Omit<FakeProviderOptions, "random">>): void {
    this.options = { ...this.options, ...patch };
  }

  async authorize(request: AuthorizeRequest, signal: AbortSignal): Promise<{ authorizationId: string }> {
    await this.simulateNetwork(signal);
    if (this.random() < this.options.declineRate) {
      this.stats.declines++;
      throw new PaymentDeclinedError("card_declined");
    }
    const existing = this.authorizations.get(request.idempotencyKey);
    if (existing) return { authorizationId: existing };
    const authorizationId = `auth_${randomUUID()}`;
    this.authorizations.set(request.idempotencyKey, authorizationId);
    return { authorizationId };
  }

  async capture(_authorizationId: string, signal: AbortSignal): Promise<void> {
    await this.simulateNetwork(signal);
  }

  async void(_authorizationId: string, signal: AbortSignal): Promise<void> {
    await this.simulateNetwork(signal);
  }

  async refund(_authorizationId: string, _amountCents: number, signal: AbortSignal): Promise<void> {
    await this.simulateNetwork(signal);
  }

  private async simulateNetwork(signal: AbortSignal): Promise<void> {
    this.stats.calls++;
    const roll = this.random();
    if (roll < this.options.timeoutRate) {
      this.stats.hangs++;
      await new Promise<never>((_, reject) => {
        signal.addEventListener("abort", () => {
          reject(new Error("aborted by caller"));
        });
      });
    }
    const { min, max } = this.options.latencyMs;
    await abortableSleep(min + Math.floor(this.random() * (max - min + 1)), signal);
    if (roll < this.options.timeoutRate + this.options.failureRate) {
      this.stats.failures++;
      throw new ProviderUnavailableError("payment provider returned 503");
    }
  }
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new Error("aborted by caller"));
    });
  });
}
