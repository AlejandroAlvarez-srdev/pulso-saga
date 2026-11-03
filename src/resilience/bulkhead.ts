import { BulkheadRejectedError } from "./errors.js";

export interface BulkheadOptions {
  name: string;
  maxConcurrent: number;
  maxQueue: number;
}

/**
 * Semaphore-style bulkhead: caps concurrent calls to a dependency so a slow assignment
 * engine cannot exhaust the process. Overflow waits in a bounded queue, then is rejected.
 */
export class Bulkhead {
  private active = 0;
  private readonly queue: (() => void)[] = [];

  constructor(private readonly options: BulkheadOptions) {
    if (options.maxConcurrent <= 0 || options.maxQueue < 0) throw new RangeError("invalid bulkhead limits");
  }

  get stats(): { active: number; queued: number } {
    return { active: this.active, queued: this.queue.length };
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active >= this.options.maxConcurrent) {
      if (this.queue.length >= this.options.maxQueue) throw new BulkheadRejectedError(this.options.name);
      await new Promise<void>((resolve) => this.queue.push(resolve));
    } else {
      this.active++;
    }
    try {
      return await fn();
    } finally {
      const next = this.queue.shift();
      if (next) next();
      else this.active--;
    }
  }
}
