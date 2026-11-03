export class TimeoutError extends Error {
  override readonly name = "TimeoutError";
  constructor(readonly timeoutMs: number) {
    super(`operation timed out after ${timeoutMs}ms`);
  }
}

export class CircuitOpenError extends Error {
  override readonly name = "CircuitOpenError";
  constructor(
    readonly breaker: string,
    readonly retryAfterMs: number,
  ) {
    super(`circuit "${breaker}" is open; retry in ${retryAfterMs}ms`);
  }
}

export class RateLimitExceededError extends Error {
  override readonly name = "RateLimitExceededError";
  constructor(readonly waitMs: number) {
    super(`rate limit exceeded; next token in ${waitMs}ms`);
  }
}

export class BulkheadRejectedError extends Error {
  override readonly name = "BulkheadRejectedError";
  constructor(readonly bulkhead: string) {
    super(`bulkhead "${bulkhead}" is full`);
  }
}
