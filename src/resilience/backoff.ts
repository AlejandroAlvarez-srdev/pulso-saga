import type { Random } from "../shared/random.js";

export type Jitter = "none" | "full" | "equal";

export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
  factor?: number;
  jitter?: Jitter;
}

/**
 * Delay before retry number `attempt` (0-based).
 *
 * - none:  base * factor^attempt, capped at max
 * - full:  uniform in [0, capped]            (AWS "full jitter": best at spreading herds)
 * - equal: capped/2 + uniform in [0, capped/2] (keeps a guaranteed minimum wait)
 */
export function backoffDelay(attempt: number, options: BackoffOptions, random: Random = Math.random): number {
  if (attempt < 0 || !Number.isInteger(attempt)) throw new RangeError("attempt must be a non-negative integer");
  const factor = options.factor ?? 2;
  const capped = Math.min(options.maxMs, options.baseMs * factor ** attempt);
  switch (options.jitter ?? "full") {
    case "none":
      return capped;
    case "full":
      return Math.floor(random() * capped);
    case "equal":
      return Math.floor(capped / 2 + random() * (capped / 2));
  }
}
