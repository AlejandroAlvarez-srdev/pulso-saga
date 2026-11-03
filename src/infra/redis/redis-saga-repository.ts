import type { Redis } from "ioredis";
import type { OutboxEntry, OutboxReader } from "../../outbox/outbox.js";
import { ConcurrencyError, type SagaCommit, type SagaRepository } from "../../saga/repository.js";
import type { SagaState } from "../../saga/state.js";

/**
 * Atomic commit as a Lua script: Redis runs it without interleaving other commands, which
 * gives us the "state + outbox + processed marker in one transaction" the outbox pattern needs.
 *
 * KEYS: 1 saga hash, 2 processed marker, 3 deadlines zset, 4 outbox hash, 5 outbox queue zset, 6 outbox seq
 * ARGV: 1 expected version, 2 new version, 3 state json, 4 saga id, 5 deadline|"" ,
 *       6 has processed "1"|"0", 7 processed ttl seconds, 8.. outbox (id, json) pairs
 */
const COMMIT_SCRIPT = `
local current = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
if current ~= tonumber(ARGV[1]) then return 0 end
if ARGV[6] == '1' and redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'state', ARGV[3])
if ARGV[6] == '1' then redis.call('SET', KEYS[2], '1', 'EX', ARGV[7]) end
if ARGV[5] == '' then
  redis.call('ZREM', KEYS[3], ARGV[4])
else
  redis.call('ZADD', KEYS[3], ARGV[5], ARGV[4])
end
for i = 8, #ARGV, 2 do
  local seq = redis.call('INCR', KEYS[6])
  redis.call('HSET', KEYS[4], ARGV[i], ARGV[i + 1])
  redis.call('ZADD', KEYS[5], seq, ARGV[i])
end
return 1
`;

export interface RedisSagaRepositoryOptions {
  prefix?: string;
  processedTtlSeconds?: number;
}

/**
 * Saga store on Redis. All keys of one deployment share a prefix; for Redis Cluster they would
 * need a common hash tag ({pulso}) so the Lua script touches a single slot.
 */
export class RedisSagaRepository implements SagaRepository {
  readonly outbox: OutboxReader;
  private readonly prefix: string;
  private readonly processedTtl: number;

  constructor(
    private readonly redis: Redis,
    options: RedisSagaRepositoryOptions = {},
  ) {
    this.prefix = options.prefix ?? "pulso";
    this.processedTtl = options.processedTtlSeconds ?? 7 * 24 * 3600;
    const entries = this.key("outbox:entries");
    const queue = this.key("outbox:queue");
    this.outbox = {
      pending: async (limit: number): Promise<OutboxEntry[]> => {
        const ids = await redis.zrange(queue, 0, limit - 1);
        if (ids.length === 0) return [];
        const values = await redis.hmget(entries, ...ids);
        return values.flatMap((json) => (json ? [JSON.parse(json) as OutboxEntry] : []));
      },
      markPublished: async (ids: string[]): Promise<void> => {
        if (ids.length === 0) return;
        await redis.multi().zrem(queue, ...ids).hdel(entries, ...ids).exec();
      },
    };
  }

  async get(sagaId: string): Promise<SagaState | undefined> {
    const json = await this.redis.hget(this.key(`saga:${sagaId}`), "state");
    return json ? (JSON.parse(json) as SagaState) : undefined;
  }

  async hasProcessed(messageId: string): Promise<boolean> {
    return (await this.redis.exists(this.key(`processed:${messageId}`))) === 1;
  }

  async commit({ state, expectedVersion, outbox, processedMessageId }: SagaCommit): Promise<void> {
    const outboxArgs = outbox.flatMap((entry) => [entry.id, JSON.stringify(entry)]);
    const result = await this.redis.eval(
      COMMIT_SCRIPT,
      6,
      this.key(`saga:${state.sagaId}`),
      this.key(`processed:${processedMessageId ?? "none"}`),
      this.key("saga:deadlines"),
      this.key("outbox:entries"),
      this.key("outbox:queue"),
      this.key("outbox:seq"),
      expectedVersion,
      state.version,
      JSON.stringify(state),
      state.sagaId,
      state.pending ? String(state.pending.deadline) : "",
      processedMessageId ? "1" : "0",
      this.processedTtl,
      ...outboxArgs,
    );
    if (result !== 1) throw new ConcurrencyError(state.sagaId, expectedVersion);
  }

  async dueForTimeout(now: number, limit: number): Promise<string[]> {
    return this.redis.zrangebyscore(this.key("saga:deadlines"), "-inf", now, "LIMIT", 0, limit);
  }

  private key(suffix: string): string {
    return `${this.prefix}:${suffix}`;
  }
}
