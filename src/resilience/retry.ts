import { realSleep, type Sleep } from "../shared/clock.js";
import type { Random } from "../shared/random.js";
import { backoffDelay, type BackoffOptions } from "./backoff.js";

export interface RetryOptions {
  /** Total attempts including the first one. */
  maxAttempts: number;
  backoff: BackoffOptions;
  isRetryable?: (error: unknown) => boolean;
  onRetry?: (info: { attempt: number; delayMs: number; error: unknown }) => void;
  sleep?: Sleep;
  random?: Random;
}

/**
 * Runs `fn` until it succeeds, a non-retryable error is thrown or attempts are exhausted.
 * The last error is rethrown unchanged so callers can still classify it.
 */
export async function retry<T>(fn: (attempt: number) => Promise<T>, options: RetryOptions): Promise<T> {
  const sleep = options.sleep ?? realSleep;
  const isRetryable = options.isRetryable ?? (() => true);
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn(attempt);
    } catch (error) {
      const isLast = attempt + 1 >= options.maxAttempts;
      if (isLast || !isRetryable(error)) throw error;
      const delayMs = backoffDelay(attempt, options.backoff, options.random);
      options.onRetry?.({ attempt: attempt + 1, delayMs, error });
      await sleep(delayMs);
    }
  }
}
