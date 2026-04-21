/**
 * opencode-sync plugin entry point.
 *
 * Syncs opencode sessions and config files across machines
 * through a self-hosted server.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { Plugin } from "@opencode-ai/plugin";
import { loadPluginConfig } from "./config.js";
import { SyncClient } from "./client.js";
import { StateManager } from "./state.js";
import { DbReader } from "./db-read.js";
import { DbWriter } from "./db-write.js";
import { SessionSync } from "./sessions.js";
import { FileSync } from "./files.js";
import { createEventHandler } from "./hooks.js";
import { applyLoadedOverrides } from "./overrides.js";
import { logger, LOG_FILE_PATH } from "./logger.js";
import { isSyncHalted, readHaltDetails, writeHaltMarker, HALT_REASONS, HALT_MARKER_PATH } from "./halt.js";
import { captureDbFingerprint, compareFingerprints } from "./db-fingerprint.js";

type BootStatus = {
  step: (message: string) => void;
  finish: (message: string) => void;
};

function createBootStatus(name: string): BootStatus {
  const stream = process.stderr;
  const prefix = `${name}: `;
  const frames = ["|", "/", "-", "\\"];
  const clearWidth = 100;
  let current = "";
  let frame = 0;
  let timer: ReturnType<typeof setInterval> | null = null;

  const writeLine = (line: string): void => {
    try {
      if (stream.isTTY) {
        stream.write(`\r${line.padEnd(clearWidth)}`);
        return;
      }
      stream.write(line + "\n");
    } catch {
      // Boot status is best-effort only — never fail plugin startup on I/O.
    }
  };

  const render = (): void => {
    if (!current) return;
    writeLine(`[${frames[frame]}] ${prefix}${current}`);
    frame = (frame + 1) % frames.length;
  };

  const stopTimer = (): void => {
    if (timer === null) return;
    clearInterval(timer);
    timer = null;
  };

  return {
    step(message) {
      current = message;

      if (!stream.isTTY) {
        writeLine(`${prefix}${message}`);
        return;
      }

      if (timer === null) {
        render();
        timer = setInterval(render, 120);
        return;
      }

      render();
    },
    finish(message) {
      stopTimer();
      writeLine(`${prefix}${message}`);
      if (stream.isTTY) {
        try {
          stream.write("\n");
        } catch {
          // Ignore terminal write failures during shutdown.
        }
      }
    },
  };
}

// ── Plugin module exports ──────────────────────────────────────────
//
// opencode loads plugin files from ~/.config/opencode/plugins/ and iterates
// every named export, requiring each to be a function. We MUST NOT export a
// non-function named binding (e.g. a string `id`) at module top level —
// that would trip opencode's plugin loader with "Plugin export is not a
// function". The plugin's identity for our own logging is just the string
// `opencode-sync` baked into every line written by `logger.log` below.

export const server: Plugin = async (_input, _options) => {
  // All log output goes to a file — see `logger.ts` for the rationale
  // (stdout/stderr both bleed through the TUI's alternate-screen render
  // and overlay sync messages on top of the rendered UI).
  const log = logger.log;
  // Best-effort terminal status is limited to startup, before the plugin
  // returns and the host TUI is fully interactive.
  const boot = createBootStatus("opencode-sync");
  boot.step("loading plugin config");

  // 1. Load config
  let config: ReturnType<typeof loadPluginConfig>;
  try {
    config = loadPluginConfig();
  } catch (err) {
    boot.finish(`startup failed: ${String(err)}`);
    log("failed to load config, plugin disabled", { error: String(err) });
    return {};
  }

  log("initializing", {
    server: config.serverUrl,
    machine: config.machineId,
    syncInterval: config.syncIntervalSec,
    logFile: LOG_FILE_PATH,
  });

  // 2. Create HTTP client
  const client = new SyncClient(config.serverUrl, config.token);

  // 3. Verify server connectivity
  boot.step("checking sync server");
  try {
    const health = await client.health();
    log("connected to server", {
      version: health.version,
      serverTime: health.time,
    });
  } catch (err) {
    log("warning: server unreachable at startup, will retry", {
      error: String(err),
    });
  }

  // 4. Create state manager
  boot.step("loading saved sync state");
  const stateManager = new StateManager(config.machineId);
  stateManager.load();
  log("state loaded", {
    lastPulledSeq: stateManager.state.lastPulledSeq,
    trackedRows: stateManager.state.lastPushedRowIds.size,
  });

  // 5. Find opencode.db — defer if it doesn't exist yet
  const dbPath = path.join(os.homedir(), ".local", "share", "opencode", "opencode.db");

  // Mutable state for lazy DB initialization — stored in an object
  // so TypeScript doesn't over-narrow the types.
  const dbState: {
    reader: DbReader | null;
    writer: DbWriter | null;
    sessionSync: SessionSync | null;
  } = { reader: null, writer: null, sessionSync: null };

  function ensureDb(): SessionSync | null {
    if (dbState.sessionSync) return dbState.sessionSync;

    if (!fs.existsSync(dbPath)) {
      return null;
    }

    try {
      dbState.reader = new DbReader(dbPath);
      dbState.writer = new DbWriter(dbPath);
      dbState.sessionSync = new SessionSync(
        dbState.reader,
        dbState.writer,
        client,
        stateManager,
        config.machineId,
        log,
      );
      log("opened opencode database", { path: dbPath });
      return dbState.sessionSync;
    } catch (err) {
      log("failed to open opencode database, will retry", {
        path: dbPath,
        error: String(err),
      });
      return null;
    }
  }

  // Try opening immediately
  boot.step("checking opencode database");
  ensureDb();

  // 6. Create file sync (independent of DB)
  const fileSync = new FileSync(
    client,
    config.machineId,
    config.fileSync,
    stateManager,
    log,
  );

  // 7. Define the sync routine (used for both initial and periodic runs).
  //    Single shared `syncInProgress` guard prevents the periodic timer
  //    from re-entering while a long-running initial sync is still in
  //    flight — both invocations would otherwise race on `state.json`
  //    writes and the in-memory `lastPushedRowIds` dedup set.
  const intervalMs = config.syncIntervalSec * 1000;
  let syncInProgress = false;
  // Latch so we log the halt reason once per process rather than on every
  // 15s tick. The user has to clear the marker file to reset.
  let haltLogged = false;

  /**
   * Run row sync if and only if the deletion-safety guard hasn't halted
   * us. The guard owns two pieces of state:
   *
   *   1. A persistent halt marker on disk (~/.local/share/.../HALTED_SYNC).
   *      Survives plugin restart. Block ALL row sync (push and pull both)
   *      while it exists — we don't even pull, because pulling extends
   *      knownRows with rows that the (still-broken) push path would
   *      then try to tombstone again on a future cycle.
   *
   *   2. A DB fingerprint stored in state.json. On every sync we compare
   *      the live opencode.db's (inode, mtime, size) against what we
   *      recorded after the last successful push. Mismatches trigger a
   *      halt — the local DB has been wiped/restored/replaced and we
   *      can't trust knownRows anymore.
   *
   * File sync runs regardless of the row-sync halt — different scope,
   * different blast radius, has its own less-catastrophic safety
   * properties (per-file SHA cross-check in files.ts).
   */
  const runRowSync = async (label: string): Promise<void> => {
    const sync = ensureDb();
    if (!sync) return;

    if (isSyncHalted()) {
      if (!haltLogged) {
        const details = readHaltDetails();
        log("ROW SYNC HALTED — deletion-safety marker present", {
          marker_path: HALT_MARKER_PATH,
          details: details ?? "<unparseable>",
          recovery: "Inspect the marker file's instructions and remove it manually to resume.",
        });
        haltLogged = true;
      }
      return;
    }

    // Marker is gone — reset the latch so a future re-trip (the user
    // restored data with a different inode → fingerprint mismatch, or
    // some other condition trips the threshold guard again) emits a
    // fresh "ROW SYNC HALTED" log on the next tick instead of being
    // silently suppressed by a stale `haltLogged = true` from before
    // the previous marker was cleared.
    haltLogged = false;

    // Fingerprint check: compare what opencode.db looks like RIGHT NOW
    // against what we recorded after the last successful push. A
    // mismatch usually means the file under us has been wiped, restored,
    // or replaced — emitting tombstones for rows it no longer contains
    // would propagate that emptiness to every peer.
    //
    // First sync after install / state.json upgrade has previousFp=null,
    // which `compareFingerprints` treats as "same" so this guard is a
    // no-op — capture happens at the end of pushAll once we've confirmed
    // the round-trip works.
    const previousFp = stateManager.state.dbFingerprint;
    const currentFp = captureDbFingerprint(dbPath);
    const fpCmp = compareFingerprints(previousFp, currentFp);
    if (!fpCmp.same) {
      log("DELETION-SAFETY HALT — opencode.db fingerprint changed", {
        reason: fpCmp.reason,
        details: fpCmp.details,
      });
      writeHaltMarker({
        triggeredAt: Date.now(),
        reason: HALT_REASONS.DB_FINGERPRINT_MISMATCH,
        message:
          `The opencode.db file fingerprint differs from the state we last ` +
          `synced (${fpCmp.reason}). This usually means the database was ` +
          `wiped, restored from backup, or replaced. Sync push is halted ` +
          `to prevent propagating row deletions to every other peer.`,
        extra: { ...fpCmp.details, previous_fingerprint: previousFp, current_fingerprint: currentFp },
      });
      // Intentionally do NOT set `haltLogged = true` here. The next tick
      // will see `isSyncHalted()` and emit the standard "ROW SYNC HALTED
      // — deletion-safety marker present" message with marker_path and
      // recovery instructions, which is useful diagnostic context that
      // the trip log above doesn't include. After that, `haltLogged`
      // latches normally and stays quiet until the marker is cleared.
      return;
    }

    await sync.sync();

    // Capture fingerprint AFTER a successful sync. Doing this on every
    // sync (instead of only on first observation) keeps us tracking
    // legitimate DB growth — opencode constantly writes to the DB during
    // active sessions, so the mtime and size move forward all the time
    // and that's fine. We only halt on the unusual changes (inode flip,
    // size shrink, mtime regress).
    if (currentFp) {
      stateManager.setDbFingerprint(currentFp);
    }
  };

  const runSync = async (label: string): Promise<boolean> => {
    if (syncInProgress) return false;
    syncInProgress = true;

    try {
      if (label === "initial sync") boot.step("syncing session rows");
      await runRowSync(label);

      if (label === "initial sync") boot.step("syncing config files");
      await fileSync.sync();
      return true;
    } catch (err) {
      log(`${label} error`, { error: String(err) });
      return false;
    } finally {
      syncInProgress = false;
    }
  };

  // Forward-declare `timer` so cleanup can reach it without depending on
  // the order of timer creation vs. cleanup registration.
  let timer: ReturnType<typeof setInterval> | null = null;

  // Clean up on process exit — use process.once to avoid listener accumulation
  const cleanup = () => {
    if (timer !== null) clearInterval(timer);
    try {
      dbState.reader?.close();
      dbState.writer?.close();
    } catch {
      // ignore close errors during shutdown
    }
    log("shutdown");
  };

  process.once("exit", cleanup);
  process.once("SIGINT", cleanup);
  process.once("SIGTERM", cleanup);

  // 8. Run initial sync to completion BEFORE arming the periodic timer.
  //    On a fresh peer the initial sync can take longer than one tick of
  //    the interval (default 15s), so arming the timer first would let
  //    `periodicSync` fire concurrently with the initial sync.
  boot.step("running initial sync");
  if (await runSync("initial sync")) {
    log("initial sync complete");
    boot.finish("startup complete");
  } else {
    log("initial sync failed, will retry on next interval");
    boot.finish("startup incomplete; retrying in background");
  }

  timer = setInterval(() => {
    // setInterval doesn't await — wrap in a void IIFE so an unhandled
    // rejection from runSync doesn't crash the process.
    void runSync("periodic sync");
  }, intervalMs);

  // 9. Return hooks
  return {
    config: async (input) => {
      if (applyLoadedOverrides(input as Record<string, unknown>)) {
        log("applied local config overrides");
      }
    },
    event: createEventHandler({
      get sessionSync(): SessionSync | null {
        return ensureDb();
      },
      fileSync,
      machineId: config.machineId,
      log,
    }),
  };
};
