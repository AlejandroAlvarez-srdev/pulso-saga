export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

export const silentLogger: Logger = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

export function jsonLogger(service: string): Logger {
  const write = (level: string, msg: string, fields?: Record<string, unknown>) => {
    process.stdout.write(
      `${JSON.stringify({ ts: new Date().toISOString(), level, service, msg, ...fields })}\n`,
    );
  };
  return {
    info: (msg, fields) => {
      write("info", msg, fields);
    },
    warn: (msg, fields) => {
      write("warn", msg, fields);
    },
    error: (msg, fields) => {
      write("error", msg, fields);
    },
  };
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
