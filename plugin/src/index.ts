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

// ── Plugin module exports ──────────────────────────────────────────
//
// opencode loads plugin files from ~/.config/opencode/plugins/ and iterates
// every named export, requiring each to be a function. We MUST NOT export a
// non-function named binding (e.g. a string `id`) at module top level —
// that would trip opencode's plugin loader with "Plugin export is not a
// function". The plugin's identity for our own logging is just the string
// embedded inside the `log()` helper below.

export const server: Plugin = async (_input, _options) => {
  const log = (msg: string, data?: Record<string, unknown>) => {
    const suffix = data ? " " + JSON.stringify(data) : "";
    console.log(`opencode-sync: ${msg}${suffix}`);
  };

  // 1. Load config
  let config: ReturnType<typeof loadPluginConfig>;
  try {
    config = loadPluginConfig();
  } catch (err) {
    log("failed to load config, plugin disabled", { error: String(err) });
    return {};
  }

  log("initializing", {
    server: config.serverUrl,
    machine: config.machineId,
    syncInterval: config.syncIntervalSec,
  });

  // 2. Create HTTP client
  const client = new SyncClient(config.serverUrl, config.token);

  // 3. Verify server connectivity
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

  const runSync = async (label: string): Promise<boolean> => {
    if (syncInProgress) return false;
    syncInProgress = true;

    try {
      const sync = ensureDb();
      if (sync) {
        await sync.sync();
      }
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
  if (await runSync("initial sync")) {
    log("initial sync complete");
  } else {
    log("initial sync failed, will retry on next interval");
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
