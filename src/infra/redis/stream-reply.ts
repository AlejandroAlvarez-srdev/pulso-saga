/** Helpers to turn raw RESP replies (`redis.call`) into typed values without `any`. */

export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

export interface PendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveries: number;
}

const isArray = (value: unknown): value is unknown[] => Array.isArray(value);

function toStr(value: unknown): string {
  if (typeof value === "string") return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  if (typeof value === "number") return String(value);
  throw new TypeError(`unexpected redis reply element: ${typeof value}`);
}

/** [[id, [k1, v1, k2, v2...]], ...] → StreamEntry[]. Deleted entries come back with null fields. */
export function parseEntries(raw: unknown): StreamEntry[] {
  if (!isArray(raw)) return [];
  const entries: StreamEntry[] = [];
  for (const item of raw) {
    if (!isArray(item) || item.length < 2) continue;
    const [id, flat] = item;
    if (!isArray(flat)) continue;
    const fields: Record<string, string> = {};
    for (let i = 0; i + 1 < flat.length; i += 2) fields[toStr(flat[i])] = toStr(flat[i + 1]);
    entries.push({ id: toStr(id), fields });
  }
  return entries;
}

/** XREADGROUP reply: [[stream, entries], ...] or null on BLOCK timeout. */
export function parseReadGroup(raw: unknown): StreamEntry[] {
  if (!isArray(raw)) return [];
  return raw.flatMap((streamReply) => (isArray(streamReply) ? parseEntries(streamReply[1]) : []));
}

/** Extended XPENDING reply: [[id, consumer, idle, deliveries], ...]. */
export function parsePending(raw: unknown): PendingEntry[] {
  if (!isArray(raw)) return [];
  return raw.filter(isArray).map(([id, consumer, idle, deliveries]) => ({
    id: toStr(id),
    consumer: toStr(consumer),
    idleMs: Number(toStr(idle)),
    deliveries: Number(toStr(deliveries)),
  }));
}
