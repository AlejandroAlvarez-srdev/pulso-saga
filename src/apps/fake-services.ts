import { hostname } from "node:os";
import { Redis } from "ioredis";
import { RedisStreamsBus } from "../infra/redis/redis-bus.js";
import { RedisProcessedMessageStore } from "../infra/redis/redis-processed-store.js";
import { createParticipants } from "../runtime/participants.js";
import { jsonLogger } from "../shared/logger.js";
import { envNumber, envString, onShutdown } from "./env.js";

/**
 * Order, payment and assignment services in one process. Their business state is in memory
 * (they are fakes); messaging and dedupe go through Redis like a real participant would.
 */
const logger = jsonLogger("fake-services");
const redis = new Redis(envString("REDIS_URL", "redis://localhost:6379"));
const bus = new RedisStreamsBus(redis, { logger });

const participants = createParticipants(
  {
    provider: {
      failureRate: envNumber("PAYMENT_FAILURE_RATE", 0.2),
      timeoutRate: envNumber("PAYMENT_TIMEOUT_RATE", 0.05),
      declineRate: envNumber("PAYMENT_DECLINE_RATE", 0.05),
      latencyMs: { min: 5, max: 40 },
    },
    pulsers: envNumber("PULSERS", 50),
    merchantWithdrawRate: envNumber("MERCHANT_WITHDRAW_RATE", 0.02),
    dispatchLatencyMs: { min: 10, max: 80 },
    maxDeliveries: envNumber("MAX_DELIVERIES", 5),
  },
  Math.random,
);

const subscriptions = await participants.start(
  bus,
  new RedisProcessedMessageStore(redis),
  `fake-services-${hostname()}`,
  logger,
);
logger.info("fake services consuming", { streams: ["order", "payment", "assignment"] });

const breakerLog = setInterval(() => {
  logger.info("payment provider", {
    breaker: participants.breaker.currentState,
    ...participants.provider.stats,
    retries: participants.metrics.providerRetries,
  });
}, 10_000);

onShutdown(async () => {
  clearInterval(breakerLog);
  await Promise.all(subscriptions.map((s) => s.stop()));
  await bus.close();
});
