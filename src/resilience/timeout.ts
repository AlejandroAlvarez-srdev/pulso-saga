import { TimeoutError } from "./errors.js";

/**
 * Races `fn` against a timer. The AbortSignal lets the callee stop work (e.g. cancel an
 * HTTP request) instead of leaking it after the caller gave up.
 */
export async function withTimeout<T>(fn: (signal: AbortSignal) => Promise<T>, timeoutMs: number): Promise<T> {
  const controller = new AbortController();
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      // Reject first so the race settles with TimeoutError, then tell the callee to stop.
      reject(new TimeoutError(timeoutMs));
      controller.abort();
    }, timeoutMs);
  });
  try {
    return await Promise.race([fn(controller.signal), timeout]);
  } finally {
    clearTimeout(timer);
  }
}
