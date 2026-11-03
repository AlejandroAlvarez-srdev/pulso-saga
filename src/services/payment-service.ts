import type { AnyMessage } from "../domain/messages.js";
import { Streams } from "../messaging/bus.js";
import type { BackoffOptions } from "../resilience/backoff.js";
import type { CircuitBreaker } from "../resilience/circuit-breaker.js";
import { TimeoutError } from "../resilience/errors.js";
import type { TokenBucket } from "../resilience/rate-limiter.js";
import { retry } from "../resilience/retry.js";
import { withTimeout } from "../resilience/timeout.js";
import type { Sleep } from "../shared/clock.js";
import type { Random } from "../shared/random.js";
import { reply, UnsupportedCommandError, type Participant } from "./participant.js";
import { PaymentDeclinedError, ProviderUnavailableError, type PaymentProvider } from "./payment-provider.js";

type PaymentStatus = "AUTHORIZED" | "CAPTURED" | "RELEASED" | "REFUNDED" | "DECLINED";

interface PaymentRecord {
  status: PaymentStatus;
  authorizationId: string | null;
  amountCents: number;
}

export interface PaymentServiceOptions {
  provider: PaymentProvider;
  breaker: CircuitBreaker;
  limiter: TokenBucket;
  /** Upper bound on how long a call waits for a rate-limit token. */
  rateLimitMaxWaitMs: number;
  callTimeoutMs: number;
  retry: { maxAttempts: number; backoff: BackoffOptions };
  sleep?: Sleep;
  random?: Random;
  onRetry?: () => void;
}

export function isTransientProviderError(error: unknown): boolean {
  return error instanceof ProviderUnavailableError || error instanceof TimeoutError;
}

/**
 * Payment participant. Every provider call goes through:
 *   token bucket → retry(exponential backoff + jitter) → circuit breaker → timeout → provider
 * Declines become `payment.failed`; exhausted retries or an open circuit are thrown so the
 * bus redelivers the command later and finally dead-letters it.
 */
export class PaymentService implements Participant {
  readonly name = "payment-service";
  readonly stream = Streams.paymentCommands;
  private readonly payments = new Map<string, PaymentRecord>();

  constructor(private readonly options: PaymentServiceOptions) {}

  status(orderId: string): PaymentStatus | undefined {
    return this.payments.get(orderId)?.status;
  }

  async handle(command: AnyMessage): Promise<AnyMessage[]> {
    switch (command.type) {
      case "payment.authorize": {
        const { orderId, amountCents, currency } = command.payload;
        const existing = this.payments.get(orderId);
        if (existing?.status === "RELEASED") {
          return [reply(command, "payment.failed", { orderId, reason: "authorization_released" })];
        }
        if (existing?.authorizationId && (existing.status === "AUTHORIZED" || existing.status === "CAPTURED")) {
          return [reply(command, "payment.authorized", { orderId, authorizationId: existing.authorizationId })];
        }
        let authorizationId: string;
        try {
          ({ authorizationId } = await this.call((signal) =>
            this.options.provider.authorize({ idempotencyKey: orderId, amountCents, currency }, signal),
          ));
        } catch (error) {
          if (!(error instanceof PaymentDeclinedError)) throw error;
          this.payments.set(orderId, { status: "DECLINED", authorizationId: null, amountCents });
          return [reply(command, "payment.failed", { orderId, reason: "card_declined" })];
        }
        // A release may have arrived while we were waiting on the provider.
        if (this.payments.get(orderId)?.status === "RELEASED") {
          await this.call((signal) => this.options.provider.void(authorizationId, signal));
          return [reply(command, "payment.failed", { orderId, reason: "authorization_released" })];
        }
        this.payments.set(orderId, { status: "AUTHORIZED", authorizationId, amountCents });
        return [reply(command, "payment.authorized", { orderId, authorizationId })];
      }
      case "payment.capture": {
        const { orderId } = command.payload;
        const record = this.payments.get(orderId);
        if (record?.status === "CAPTURED" && record.authorizationId) {
          return [reply(command, "payment.captured", { orderId, authorizationId: record.authorizationId })];
        }
        if (record?.status !== "AUTHORIZED" || !record.authorizationId) {
          return [reply(command, "payment.capture_failed", { orderId, reason: "no_active_authorization" })];
        }
        const authorizationId = record.authorizationId;
        await this.call((signal) => this.options.provider.capture(authorizationId, signal));
        record.status = "CAPTURED";
        return [reply(command, "payment.captured", { orderId, authorizationId })];
      }
      case "payment.release": {
        const { orderId } = command.payload;
        const record = this.payments.get(orderId);
        if (!record) {
          // Tombstone: an authorize that is still in flight must not succeed afterwards.
          this.payments.set(orderId, { status: "RELEASED", authorizationId: null, amountCents: 0 });
          return [reply(command, "payment.released", { orderId, noop: true })];
        }
        if (record.status !== "AUTHORIZED" || !record.authorizationId) {
          return [reply(command, "payment.released", { orderId, noop: true })];
        }
        const authorizationId = record.authorizationId;
        await this.call((signal) => this.options.provider.void(authorizationId, signal));
        record.status = "RELEASED";
        return [reply(command, "payment.released", { orderId, noop: false })];
      }
      case "payment.refund": {
        const { orderId } = command.payload;
        const record = this.payments.get(orderId);
        if (record?.status !== "CAPTURED" || !record.authorizationId) {
          return [reply(command, "refund.issued", { orderId, amountCents: 0, noop: true })];
        }
        const { authorizationId, amountCents } = record;
        await this.call((signal) => this.options.provider.refund(authorizationId, amountCents, signal));
        record.status = "REFUNDED";
        return [reply(command, "refund.issued", { orderId, amountCents, noop: false })];
      }
      default:
        throw new UnsupportedCommandError(this.name, command.type);
    }
  }

  private call<T>(fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const { breaker, limiter } = this.options;
    return retry(
      async () => {
        await limiter.acquire(this.options.rateLimitMaxWaitMs);
        return breaker.execute(() => withTimeout(fn, this.options.callTimeoutMs));
      },
      {
        maxAttempts: this.options.retry.maxAttempts,
        backoff: this.options.retry.backoff,
        isRetryable: isTransientProviderError,
        onRetry: () => this.options.onRetry?.(),
        ...(this.options.sleep ? { sleep: this.options.sleep } : {}),
        ...(this.options.random ? { random: this.options.random } : {}),
      },
    );
  }
}
