export function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (Number.isNaN(value)) throw new Error(`${name} must be a number, got "${raw}"`);
  return value;
}

export function envString(name: string, fallback: string): string {
  return process.env[name] ?? fallback;
}

export function onShutdown(stop: () => Promise<void>): void {
  let stopping = false;
  const handler = () => {
    if (stopping) return;
    stopping = true;
    stop()
      .then(() => process.exit(0))
      .catch(() => process.exit(1));
  };
  process.on("SIGTERM", handler);
  process.on("SIGINT", handler);
}
