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
  const rawToken = process.env["OPENCODE_SYNC_TOKEN"];
  if (!rawToken) {
    throw new Error(
      "OPENCODE_SYNC_TOKEN is required. Set it to a shared secret for authenticating clients.",
    );
  }
  // Trim — a trailing newline from `echo $TOKEN > env.sh` or similar
  // shell habits silently fails the bearer-token comparison against a
  // trimming client. Plugin already trims (plugin/src/config.ts); the
  // asymmetry would produce silent 401s. Warn if trimming changed the
  // value so the operator can clean up their env. See FINDINGS.md L4.
  const token = rawToken.trim();
  if (token.length === 0) {
    throw new Error(
      "OPENCODE_SYNC_TOKEN is whitespace-only. Set it to a non-empty shared secret.",
    );
  }
  if (token !== rawToken) {
    console.warn(
      "[WARN] OPENCODE_SYNC_TOKEN contained leading/trailing whitespace; " +
        "it has been trimmed. Check your shell export for stray newlines.",
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
