export interface Clock {
  now(): number;
}

export const systemClock: Clock = { now: () => Date.now() };

export type Sleep = (ms: number) => Promise<void>;

export const realSleep: Sleep = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/** Manually advanced clock for deterministic tests. */
export class ManualClock implements Clock {
  constructor(private current = 0) {}

  now(): number {
    return this.current;
  }

  advance(ms: number): void {
    this.current += ms;
  }
}
