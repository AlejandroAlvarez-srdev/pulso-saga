import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { hostname } from "node:os";
import { Redis } from "ioredis";
import type { Order } from "../domain/order.js";
import { RedisStreamsBus } from "../infra/redis/redis-bus.js";
import { RedisSagaRepository } from "../infra/redis/redis-saga-repository.js";
import { SagaOrchestrator } from "../saga/orchestrator.js";
import { OrchestratorRuntime } from "../runtime/orchestrator-runtime.js";
import { makeOrder } from "../services/fixtures.js";
import { errorMessage, jsonLogger } from "../shared/logger.js";
import { envNumber, envString, onShutdown } from "./env.js";

const logger = jsonLogger("orchestrator");
const redis = new Redis(envString("REDIS_URL", "redis://localhost:6379"));
const bus = new RedisStreamsBus(redis, { logger });
const repository = new RedisSagaRepository(redis);
const orchestrator = new SagaOrchestrator({
  repository,
  stepTimeoutMs: envNumber("STEP_TIMEOUT_MS", 15_000),
  maxCompensationAttempts: envNumber("MAX_COMPENSATION_ATTEMPTS", 5),
  compensationBackoff: { baseMs: 1_000, maxMs: 30_000, jitter: "full" },
  logger,
});
const runtime = new OrchestratorRuntime(bus, repository, orchestrator, {
  consumer: `orchestrator-${hostname()}`,
  relayIntervalMs: 50,
  timeoutCheckIntervalMs: 500,
  logger,
});

function send(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString("utf8").trim();
  return text ? (JSON.parse(text) as unknown) : {};
}

async function route(req: IncomingMessage): Promise<[number, unknown]> {
  const url = new URL(req.url ?? "/", "http://localhost");
  if (req.method === "GET" && url.pathname === "/health") return [200, { ok: true }];
  if (req.method === "POST" && url.pathname === "/orders") {
    // Any field omitted from the body is filled with demo data, so `curl -X POST` just works.
    const body = (await readJson(req)) as Partial<Order>;
    const order = makeOrder(Math.random, { id: randomUUID(), ...body, status: "CREATED" });
    const saga = await orchestrator.start(order);
    return [202, { sagaId: saga.sagaId, status: saga.status, links: { saga: `/sagas/${saga.sagaId}` } }];
  }
  const match = /^\/sagas\/([\w-]+)$/.exec(url.pathname);
  if (req.method === "GET" && match?.[1]) {
    const saga = await orchestrator.get(match[1]);
    return saga ? [200, saga] : [404, { error: "saga not found" }];
  }
  return [404, { error: "not found" }];
}

const server = createServer((req, res) => {
  route(req)
    .then(([status, body]) => {
      send(res, status, body);
    })
    .catch((error: unknown) => {
      logger.error("request failed", { error: errorMessage(error) });
      send(res, 500, { error: "internal error" });
    });
});

await runtime.start();
await bus.subscribeDeadLetters("ops-alerts", (letter) => {
  logger.warn("dead letter", { type: letter.message.type, sagaId: letter.message.sagaId, error: letter.error });
  return Promise.resolve();
});
const port = envNumber("PORT", 3000);
server.listen(port, () => {
  logger.info("orchestrator listening", { port });
});

onShutdown(async () => {
  logger.info("shutting down");
  server.close();
  await runtime.stop();
  await bus.close();
});
