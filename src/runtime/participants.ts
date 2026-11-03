import type { MessageBus, Subscription } from "../messaging/bus.js";
import type { ProcessedMessageStore } from "../messaging/idempotency.js";
import { Bulkhead } from "../resilience/bulkhead.js";
import { CircuitBreaker, type CircuitState } from "../resilience/circuit-breaker.js";
import { TokenBucket } from "../resilience/rate-limiter.js";
import type { Logger } from "../shared/logger.js";
import type { Random } from "../shared/random.js";
import { AssignmentService, FakeDispatchEngine } from "../services/assignment-service.js";
import { makePulsers } from "../services/fixtures.js";
import { OrderService } from "../services/order-service.js";
import { runParticipant } from "../services/participant.js";
import { PaymentDeclinedError, FakePaymentProvider, type FakeProviderOptions } from "../services/payment-provider.js";
import { PaymentService } from "../services/payment-service.js";

export interface ParticipantsConfig {
  provider: Omit<FakeProviderOptions, "random">;
  pulsers: number;
  merchantWithdrawRate: number;
  dispatchLatencyMs: { min: number; max: number };
  maxDeliveries: number;
}

export interface ParticipantMetrics {
  breakerTransitions: string[];
  providerRetries: number;
}

export interface Participants {
  orders: OrderService;
  payments: PaymentService;
  assignments: AssignmentService;
  provider: FakePaymentProvider;
  breaker: CircuitBreaker;
  metrics: ParticipantMetrics;
  start(bus: MessageBus, store: ProcessedMessageStore, consumer: string, logger?: Logger): Promise<Subscription[]>;
}

/** Builds the three fake participant services with their resilience policies. */
export function createParticipants(config: ParticipantsConfig, random: Random): Participants {
  const metrics: ParticipantMetrics = { breakerTransitions: [], providerRetries: 0 };
  const provider = new FakePaymentProvider({ ...config.provider, random });
  const breaker = new CircuitBreaker({
    name: "payment-provider",
    windowSize: 20,
    minimumCalls: 10,
    failureRateThreshold: 0.5,
    openDurationMs: 500,
    halfOpenMaxCalls: 2,
    // A decline is the provider working correctly; only infrastructure errors count.
    isFailure: (error) => !(error instanceof PaymentDeclinedError),
    onStateChange: (from: CircuitState, to: CircuitState) => metrics.breakerTransitions.push(`${from}->${to}`),
  });
  const payments = new PaymentService({
    provider,
    breaker,
    limiter: new TokenBucket({ capacity: 50, refillPerSecond: 500 }),
    rateLimitMaxWaitMs: 1_000,
    callTimeoutMs: 60,
    retry: { maxAttempts: 3, backoff: { baseMs: 10, maxMs: 200, jitter: "full" } },
    random,
    onRetry: () => {
      metrics.providerRetries++;
    },
  });
  const assignments = new AssignmentService({
    pulsers: makePulsers(config.pulsers, random),
    engine: new FakeDispatchEngine(config.dispatchLatencyMs, random),
    bulkhead: new Bulkhead({ name: "dispatch-engine", maxConcurrent: 4, maxQueue: 16 }),
    callTimeoutMs: 200,
  });
  const orders = new OrderService({ merchantWithdrawRate: config.merchantWithdrawRate, random });

  return {
    orders,
    payments,
    assignments,
    provider,
    breaker,
    metrics,
    start: (bus, store, consumer, logger) =>
      Promise.all(
        [orders, payments, assignments].map((participant) =>
          runParticipant(bus, participant, {
            store,
            consumer,
            maxDeliveries: config.maxDeliveries,
            // Longer than the breaker's open window, so a redelivery usually finds it half-open.
            redeliveryBackoff: { baseMs: 250, maxMs: 4_000, jitter: "equal" },
            concurrency: 8,
            ...(logger ? { logger } : {}),
          }),
        ),
      ),
  };
}
