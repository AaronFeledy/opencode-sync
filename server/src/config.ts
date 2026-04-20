/**
 * Server configuration loaded from environment variables.
 */

import type { LogLevel } from "./log.js";

export interface ServerConfig {
  /** Bearer token for auth (required) */
  token: string;
  /** Listen address host:port */
  listen: string;
  /** Data directory for SQLite + blobs */
  dataDir: string;
  /** Minimum log level */
  logLevel: LogLevel;
  /** Server version */
  version: string;
}

const VALID_LOG_LEVELS = new Set(["debug", "info", "warn", "error"]);

export function loadConfig(): ServerConfig {
  const token = process.env["OPENCODE_SYNC_TOKEN"];
  if (!token) {
    throw new Error(
      "OPENCODE_SYNC_TOKEN is required. Set it to a shared secret for authenticating clients.",
    );
  }

  const listen = process.env["OPENCODE_SYNC_LISTEN"] ?? "0.0.0.0:4455";

  const dataDir = process.env["OPENCODE_SYNC_DATA_DIR"] ?? "./data";

  const rawLevel = process.env["OPENCODE_SYNC_LOG_LEVEL"] ?? "info";
  if (!VALID_LOG_LEVELS.has(rawLevel)) {
    throw new Error(
      `Invalid OPENCODE_SYNC_LOG_LEVEL="${rawLevel}". Must be one of: debug, info, warn, error`,
    );
  }
  const logLevel = rawLevel as LogLevel;

  let version = "0.0.1";
  try {
    const pkg = require("../package.json");
    if (pkg.version) version = pkg.version;
  } catch {
    // package.json not resolvable — use default
  }

  return Object.freeze({
    token,
    listen,
    dataDir,
    logLevel,
    version,
  });
}
