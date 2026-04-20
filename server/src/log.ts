/**
 * Structured logger with level filtering.
 * Format: [ISO_TIMESTAMP] [LEVEL] message {json_data}
 */

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface Logger {
  debug(msg: string, data?: Record<string, unknown>): void;
  info(msg: string, data?: Record<string, unknown>): void;
  warn(msg: string, data?: Record<string, unknown>): void;
  error(msg: string, data?: Record<string, unknown>): void;
}

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

export function createLogger(level: LogLevel): Logger {
  const threshold = LEVEL_ORDER[level];

  function emit(lvl: LogLevel, msg: string, data?: Record<string, unknown>): void {
    if (LEVEL_ORDER[lvl] < threshold) return;

    const ts = new Date().toISOString();
    const tag = lvl.toUpperCase().padEnd(5);
    const suffix = data && Object.keys(data).length > 0 ? ` ${JSON.stringify(data)}` : "";

    const line = `[${ts}] [${tag}] ${msg}${suffix}`;

    if (lvl === "error") {
      console.error(line);
    } else if (lvl === "warn") {
      console.warn(line);
    } else {
      console.log(line);
    }
  }

  return {
    debug: (msg, data) => emit("debug", msg, data),
    info: (msg, data) => emit("info", msg, data),
    warn: (msg, data) => emit("warn", msg, data),
    error: (msg, data) => emit("error", msg, data),
  };
}
