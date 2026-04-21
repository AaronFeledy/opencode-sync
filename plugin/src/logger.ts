/**
 * File-based logger for the opencode-sync plugin.
 *
 * Writes to `~/.local/share/opencode/opencode-sync/plugin.log` instead of
 * stdout/stderr. Plugin code runs inside the opencode process; anything
 * written to stdout overlays the TUI's alternate-screen rendering and
 * corrupts the display (the original symptom: `pushAll complete` lines
 * appearing on top of the TUI on every sync). stderr has the same
 * problem on most terminals.
 *
 * Writes are synchronous (`appendFileSync`) — the plugin emits at most a
 * handful of lines per sync cycle (default 15s), so the per-call
 * open/write/close cost is negligible, and we get crash-safety for free
 * (no buffered output lost on SIGTERM). Logging failures are swallowed
 * so a broken disk never crashes the plugin.
 *
 * Rotation: when the active log exceeds `MAX_BYTES`, it's renamed to
 * `plugin.log.old` (overwriting any prior rotation) and a fresh file is
 * started. Single-generation rotation is enough for ad-hoc debugging;
 * users who need long-term history should ship logs elsewhere.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

// ── Paths ──────────────────────────────────────────────────────────

const LOG_DIR = path.join(os.homedir(), ".local", "share", "opencode", "opencode-sync");
const LOG_PATH = path.join(LOG_DIR, "plugin.log");
const ROTATED_PATH = LOG_PATH + ".old";

/** Rotate after the active log grows past this many bytes. */
const MAX_BYTES = 10 * 1024 * 1024; // 10 MB

/** Re-stat for size only every Nth write to avoid a syscall on the hot path. */
const ROTATION_CHECK_INTERVAL = 100;

// ── Internal state ─────────────────────────────────────────────────

let dirEnsured = false;
let writeCount = 0;

function ensureDir(): void {
  if (dirEnsured) return;
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    dirEnsured = true;
  } catch {
    // If we can't create the directory, every subsequent appendFileSync
    // will throw and be swallowed. Don't latch dirEnsured — retry next call.
  }
}

function maybeRotate(): void {
  if (writeCount++ % ROTATION_CHECK_INTERVAL !== 0) return;
  try {
    const st = fs.statSync(LOG_PATH);
    if (st.size > MAX_BYTES) {
      try {
        fs.renameSync(LOG_PATH, ROTATED_PATH);
      } catch {
        // Another process may have rotated already — ignore.
      }
    }
  } catch {
    // File doesn't exist yet — nothing to rotate.
  }
}

function writeLine(line: string): void {
  try {
    ensureDir();
    maybeRotate();
    fs.appendFileSync(LOG_PATH, line + "\n");
  } catch {
    // Never throw out of the logger — losing a log line is preferable to
    // crashing the plugin (and therefore the host opencode process).
  }
}

function formatExtras(args: readonly unknown[]): string {
  if (args.length === 0) return "";
  const parts = args.map((arg) => {
    if (arg instanceof Error) return arg.stack ?? arg.message;
    if (typeof arg === "string") return arg;
    try {
      return JSON.stringify(arg);
    } catch {
      return String(arg);
    }
  });
  return " " + parts.join(" ");
}

// ── Public API ─────────────────────────────────────────────────────

export interface Logger {
  log: (msg: string, data?: Record<string, unknown>) => void;
  error: (msg: string, ...args: unknown[]) => void;
}

export const logger: Logger = {
  log(msg, data) {
    const ts = new Date().toISOString();
    const suffix = data ? " " + JSON.stringify(data) : "";
    writeLine(`${ts} INFO  opencode-sync: ${msg}${suffix}`);
  },
  error(msg, ...args) {
    const ts = new Date().toISOString();
    writeLine(`${ts} ERROR opencode-sync: ${msg}${formatExtras(args)}`);
  },
};

/** Path to the active log file — exposed so callers (and the README) can point users at it. */
export const LOG_FILE_PATH = LOG_PATH;
