/**
 * Persistent sync state — tracks what we've pushed/pulled so far.
 * Heavy maps live in state.sqlite. Legacy state.json is imported once
 * (immediately if small, after plugin return if huge).
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { Database } from "bun:sqlite";
import type { FileManifestEntry, SyncKind } from "@opencode-sync/shared";
import { logger } from "./logger.js";

/**
 * Threshold beyond which a single `(kind, id, server_seq)` envelope is
 * considered "poison" — malformed, unknown kind on an older client, or
 * persistently SQL-incompatible — and skipped past so subsequent pulls
 * can proceed. Before H3, a single bad envelope blocked ALL subsequent
 * pulls forever.
 *
 * At the default 15s sync interval, 10 retries ≈ 2.5 min of transient
 * error tolerance — long enough for FK-ordering inversions and
 * SQLITE_BUSY squirms to resolve, short enough that genuine poison
 * stops blocking progress within a few minutes.
 */
export const PULL_POISON_THRESHOLD = 10;

/** Cap on `poisonedEnvelopes` to bound growth under attack. */
const POISONED_ENVELOPES_MAX = 500;

const JSON_MIGRATE_SYNC_MAX_BYTES = 5 * 1024 * 1024;
const DELETION_RECONCILE_INTERVAL_MS = 5 * 60_000;

// ── Types ──────────────────────────────────────────────────────────

export interface SyncState {
  machineId: string;
  /** Server-assigned monotonic cursor — pull rows with seq > this */
  lastPulledSeq: number;
  /**
   * Cursor for the launch-blocking recent-session pull (`min_time_updated`).
   * Independent of `lastPulledSeq` so background catch-up can keep walking
   * the full ledger without skipping rows the recent path already applied.
   */
  lastRecentPulledSeq: number;
  /** Track what we've pushed so we don't re-push unchanged rows */
  lastPushedRowIds: Set<string>;
  /**
   * Highest local `time_updated` we have ever included in a push batch.
   * Used as a `since` filter when scanning local tables in `pushAll` so
   * periodic syncs cost O(delta) instead of O(table size). Not the same as
   * `lastPulledSeq` — that is server-assigned; this is wall-clock from the
   * local opencode DB.
   */
  lastPushedRowTime: number;
  /** Last known server-visible version of local DB rows */
  knownRows: Record<string, number>;
  /**
   * Last known synced local file manifest. `size` is stored alongside
   * `mtime` so `computeLocalManifest` can short-circuit re-hashing
   * unchanged files using the standard (mtime, size) heuristic.
   */
  knownFiles: Record<string, { sha256: string; mtime: number; size: number }>;
  /** Last time we ran file sync (ms epoch) */
  lastFileSyncTime: number;
  /**
   * Fingerprint of the opencode.db file as observed at the end of the last
   * successful push. Used by the deletion-safety guard to detect when the
   * DB was wiped, restored from backup, or replaced from under us — in
   * which case `buildDeletionEnvelopes` would otherwise interpret every
   * `knownRows` entry as an intentional deletion and tombstone the entire
   * fleet's data.
   *
   * `null` until the first successful capture (e.g. fresh install before
   * the first sync, or older state.json upgraded in place).
   *
   * `mtime` and `size` come from `fs.statSync`; `inode` distinguishes
   * "same path, different file" (atomic rename / restore-from-backup),
   * which `mtime` alone can miss when the replacement happens to share a
   * timestamp with the original.
   */
  dbFingerprint: { inode: number; mtime: number; size: number } | null;
  /**
   * Two-cycle deletion confirmation buffer. Keys are the same `${kind}:${id}`
   * shape as `knownRows`; values track when we first noticed the row was
   * missing and what `time_updated` we last knew it had.
   *
   * On detection, a candidate moves into this buffer instead of being
   * tombstoned immediately. On the NEXT sync cycle, if it's still missing
   * AND has been pending for at least `TOMBSTONE_CONFIRMATION_DELAY_MS`,
   * we emit the tombstone. If the row reappears in the live DB, the
   * pending entry is dropped — protects against transient DB-locked /
   * mid-migration / mid-restore false positives.
   *
   * Persisted across plugin restarts so a crash mid-confirmation doesn't
   * reset the timer (and doesn't open a window where a freshly-restarted
   * plugin tombstones things on the very first cycle).
   */
  pendingTombstones: Record<string, { firstSeenAt: number; knownTimeUpdated: number }>;
  /**
   * Pull-apply attempt counters keyed by `${kind}:${id}:${server_seq}`.
   * Incremented each time `applyEnvelope` returns `"error"` (or throws)
   * for an envelope with this exact triple. When a counter exceeds
   * `PULL_POISON_THRESHOLD`, the envelope is skipped permanently —
   * its server_seq is crossed, a warning is logged, and the key is
   * moved to `poisonedEnvelopes` for operator audit. A successful
   * apply (or skipped/conflict) removes the counter. See FINDINGS.md H3.
   */
  pullErrorCounts: Record<string, number>;
  /**
   * Envelopes that exceeded `PULL_POISON_THRESHOLD` and were skipped.
   * Kept as a diagnostic breadcrumb — operator can inspect the server
   * ledger for these `server_seq`s. FIFO-capped at
   * `POISONED_ENVELOPES_MAX` to bound state.json growth under attack
   * scenarios.
   */
  poisonedEnvelopes: Array<{
    kind: SyncKind;
    id: string;
    server_seq: number;
    skippedAt: number;
    lastError?: string;
  }>;
  /**
   * Secondary index: child rowKey -> parent session id. Used by
   * `markExpectedDeletion` to cascade-expand a deleted session's
   * rowKey into all of its children (messages, parts, todos,
   * session_share), so the deletion-safety threshold doesn't halt on
   * a routine "delete my biggest session" action.
   *
   * Without this index, only todos (prefix-scannable via
   * `todo:<sessionId>:*`) and `session_share` (keyed directly by
   * session id) could be cascade-expanded. Messages and parts are
   * keyed by their own ids (`message:<msgid>`, `part:<partid>`), so
   * they couldn't be discovered from a session delete alone — leaving
   * them in `unexpectedCandidates` where they'd trip the 95%
   * threshold on users whose session tree dominated knownRows.
   *
   * Populated in `rememberRows` by parsing `session_id` out of
   * envelope data for `message`, `part`, `todo`, and `session_share`.
   * Cleared in `forgetRows`. Persists across restarts. Entries for
   * kinds with no parent (project, session, permission) are not
   * stored. See FINDINGS.md M1.
   */
  rowParents: Record<string, string>;
}

/** JSON-serialisable representation of SyncState */
interface SyncStateJson {
  machineId: string;
  lastPulledSeq: number;
  lastRecentPulledSeq?: number;
  lastPushedRowIds: string[];
  lastPushedRowTime?: number;
  knownRows?: Record<string, number>;
  knownFiles?: Record<string, { sha256: string; mtime: number; size?: number }>;
  lastFileSyncTime: number;
  dbFingerprint?: { inode: number; mtime: number; size: number } | null;
  pendingTombstones?: Record<string, { firstSeenAt: number; knownTimeUpdated: number }>;
  pullErrorCounts?: Record<string, number>;
  poisonedEnvelopes?: Array<{
    kind: string;
    id: string;
    server_seq: number;
    skippedAt: number;
    lastError?: string;
  }>;
  rowParents?: Record<string, string>;
}

/**
 * Safety margin (ms) for the `pushAll` delta-read cursor. We read rows with
 * `time_updated > (lastPushedRowTime - PUSH_CURSOR_MARGIN_MS)` to absorb any
 * minor clock skew or out-of-order writes. The dedup set
 * (`lastPushedRowIds`) filters away anything we've already pushed, so the
 * margin only costs a slightly larger read window — never correctness.
 */
const PUSH_CURSOR_MARGIN_MS = 60_000;

// ── State manager ──────────────────────────────────────────────────

const STATE_DIR = path.join(os.homedir(), ".local", "share", "opencode", "opencode-sync");
const STATE_FILE = path.join(STATE_DIR, "state.json");
const STATE_DB_FILE = path.join(STATE_DIR, "state.sqlite");

const STATE_SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS known_rows (row_key TEXT PRIMARY KEY, time_updated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS row_parents (row_key TEXT PRIMARY KEY, parent TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pending_tombstones (
  row_key TEXT PRIMARY KEY,
  first_seen_at INTEGER NOT NULL,
  known_time_updated INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pull_error_counts (envelope_key TEXT PRIMARY KEY, count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS poisoned (
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  server_seq INTEGER NOT NULL,
  skipped_at INTEGER NOT NULL,
  last_error TEXT,
  PRIMARY KEY (kind, id, server_seq)
);
CREATE TABLE IF NOT EXISTS last_pushed (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS known_files (
  relpath TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL,
  mtime REAL NOT NULL,
  size INTEGER NOT NULL
);
`;

export class StateManager {
  private _state: SyncState;
  /** Re-entrant batch depth. While > 0, mutating methods skip `save()`. */
  private _batchDepth = 0;
  /** Set by mutating methods when they would have saved during a batch. */
  private _batchDirty = false;
  private _db: Database | null = null;
  private _heavyLoaded = false;
  private _pendingJsonMigration: string | null = null;
  private _poisonKeys = new Set<string>();
  private _lastDeletionReconcileAt = 0;

  constructor(machineId: string) {
    this._state = {
      machineId,
      lastPulledSeq: 0,
      lastRecentPulledSeq: 0,
      lastPushedRowIds: new Set(),
      lastPushedRowTime: 0,
      knownRows: {},
      knownFiles: {},
      lastFileSyncTime: 0,
      dbFingerprint: null,
      pendingTombstones: {},
      pullErrorCounts: {},
      poisonedEnvelopes: [],
      rowParents: {},
    };
  }

  /**
   * Load state from disk, creating directory if needed.
   *
   * On corruption (malformed JSON), the corrupt file is backed up to
   * `${STATE_FILE}.corrupt-${timestamp}` and the in-memory state is
   * left at constructor defaults. On filesystem errors (EACCES, EIO),
   * the error is rethrown so the caller sees a loud failure rather
   * than silently resetting to defaults. See FINDINGS.md M3.
   */
  load(): void {
    this.openDb();
    if (this.sqliteHasMeta()) {
      this.loadFromSqlite();
      return;
    }
    if (!fs.existsSync(STATE_FILE)) return;

    let size = 0;
    try {
      size = fs.statSync(STATE_FILE).size;
    } catch (err) {
      logger.error("state.load: failed to stat state file", {
        path: STATE_FILE,
        error: String(err),
      });
      throw err;
    }

    if (size > JSON_MIGRATE_SYNC_MAX_BYTES) {
      this._pendingJsonMigration = STATE_FILE;
      logger.log("deferring large state.json import until after startup", {
        path: STATE_FILE,
        bytes: size,
      });
      return;
    }

    const json = this.readJsonFile(STATE_FILE);
    if (!json) return;
    this.applyJson(json);
    this._heavyLoaded = true;
    this.save();
  }

  async migrateDeferredJson(): Promise<void> {
    const pending = this._pendingJsonMigration;
    if (!pending) return;
    this._pendingJsonMigration = null;
    const json = this.readJsonFile(pending);
    if (!json) return;
    this.applyJson(json);
    this._heavyLoaded = true;
    this.save();
    try {
      fs.renameSync(pending, `${pending}.migrated`);
    } catch (err) {
      logger.error("state: failed to rename migrated state.json", { error: String(err) });
    }
  }

  save(): void {
    this.openDb();
    const db = this._db!;
    db.transaction(() => {
      this.writeMeta();
      db.exec("DELETE FROM last_pushed");
      const insPushed = db.prepare("INSERT INTO last_pushed (id) VALUES (?)");
      for (const id of this._state.lastPushedRowIds) insPushed.run(id);

      db.exec("DELETE FROM known_files");
      const insFile = db.prepare("INSERT INTO known_files (relpath, sha256, mtime, size) VALUES (?, ?, ?, ?)");
      for (const [relpath, file] of Object.entries(this._state.knownFiles)) {
        insFile.run(relpath, file.sha256, file.mtime, file.size);
      }

      db.exec("DELETE FROM pending_tombstones");
      const insPending = db.prepare(
        "INSERT INTO pending_tombstones (row_key, first_seen_at, known_time_updated) VALUES (?, ?, ?)",
      );
      for (const [rowKey, entry] of Object.entries(this._state.pendingTombstones)) {
        insPending.run(rowKey, entry.firstSeenAt, entry.knownTimeUpdated);
      }

      db.exec("DELETE FROM pull_error_counts");
      const insErr = db.prepare("INSERT INTO pull_error_counts (envelope_key, count) VALUES (?, ?)");
      for (const [key, count] of Object.entries(this._state.pullErrorCounts)) {
        insErr.run(key, count);
      }

      db.exec("DELETE FROM poisoned");
      const insPoison = db.prepare(
        "INSERT INTO poisoned (kind, id, server_seq, skipped_at, last_error) VALUES (?, ?, ?, ?, ?)",
      );
      for (const p of this._state.poisonedEnvelopes) {
        insPoison.run(p.kind, p.id, p.server_seq, p.skippedAt, p.lastError ?? null);
      }

      if (this._heavyLoaded) {
        db.exec("DELETE FROM known_rows");
        const insRow = db.prepare("INSERT INTO known_rows (row_key, time_updated) VALUES (?, ?)");
        for (const [rowKey, timeUpdated] of Object.entries(this._state.knownRows)) {
          insRow.run(rowKey, timeUpdated);
        }
        db.exec("DELETE FROM row_parents");
        const insParent = db.prepare("INSERT INTO row_parents (row_key, parent) VALUES (?, ?)");
        for (const [rowKey, parent] of Object.entries(this._state.rowParents)) {
          insParent.run(rowKey, parent);
        }
      }
    })();
  }

  /**
   * Compute the `since` filter to use when scanning local tables for a
   * `pushAll`. Normally returns `lastPushedRowTime - PUSH_CURSOR_MARGIN_MS`.
   *
   * M6: detect wall-clock backjumps and reset the cursor. If
   * `Date.now()` has moved backward past our saved cursor by more than
   * the margin (NTP step after suspend/resume, VM migration, BIOS
   * battery failure, container clock drift), fresh local rows written
   * with the post-jump `Date.now()` would have `time_updated <
   * lastPushedRowTime - margin` and be permanently invisible to
   * `iterateAllEnvelopes` until wall clock caught up — potentially
   * hours. Detecting this here and resetting the cursor to the current
   * wall clock ensures post-jump rows are picked up on the next push.
   */
  pushReadSince(): number {
    const now = Date.now();
    // If the wall clock is much earlier than our saved cursor, a
    // backjump has happened. Reset so newly-written rows (which now
    // carry smaller time_updated values) aren't filtered out.
    if (now + PUSH_CURSOR_MARGIN_MS < this._state.lastPushedRowTime) {
      logger.log("push cursor rewinding: wall clock moved backward past saved cursor", {
        now,
        lastPushedRowTime: this._state.lastPushedRowTime,
      });
      this._state.lastPushedRowTime = now;
      this.maybeSave();
    }
    return Math.max(0, this._state.lastPushedRowTime - PUSH_CURSOR_MARGIN_MS);
  }

  /**
   * Advance the push cursor. Caller passes the max `time_updated` it just
   * pushed; we keep the strictly-greater value (cursor is monotonic).
   *
   * Clamped to `Date.now()` so the cursor can never run ahead of wall
   * clock. Without this, a row written by a peer with a forward-skewed
   * clock (or a tombstone stamped via `Math.max(Date.now(), prev + 1)`
   * where `prev` came from such a peer) would park `lastPushedRowTime`
   * minutes/hours into the future. Combined with `pushReadSince`'s 60s
   * margin, that would silently filter newly-written local rows out of
   * the delta read until wall clock caught up.
   */
  advancePushedRowTime(timeUpdated: number): void {
    const clamped = Math.min(timeUpdated, Date.now());
    if (clamped <= this._state.lastPushedRowTime) return;
    this._state.lastPushedRowTime = clamped;
    this.maybeSave();
  }

  get state(): SyncState {
    const self = this;
    return new Proxy(this._state, {
      get(target, prop, receiver) {
        if (prop === "knownRows" || prop === "rowParents") self.ensureHeavy();
        return Reflect.get(target, prop, receiver);
      },
    });
  }

  /**
   * Run `fn` with auto-save suppressed; persist once at the end if any
   * mutating method was called. Re-entrant — nested batches are merged into
   * the outermost one and only that outer scope writes to disk.
   *
   * Use to avoid the `save()`-per-mutation overhead in tight loops (e.g.
   * paginated pull/push, where each iteration calls `markPushed`,
   * `rememberRows`, `forgetRows`, and `updateSeq`).
   */
  async withBatch<T>(fn: () => Promise<T>, options?: { persist?: boolean }): Promise<T> {
    this._batchDepth++;
    try {
      return await fn();
    } finally {
      this._batchDepth--;
      if (this._batchDepth === 0 && this._batchDirty) {
        this._batchDirty = false;
        if (options?.persist !== false) this.save();
      }
    }
  }

  /**
   * Persist immediately if not in a batch; otherwise mark the batch dirty
   * so `withBatch` writes once on exit.
   */
  private maybeSave(): void {
    if (this._batchDepth > 0) {
      this._batchDirty = true;
      return;
    }
    this.save();
  }

  updateSeq(seq: number): void {
    this._state.lastPulledSeq = seq;
    this.maybeSave();
  }

  updateRecentSeq(seq: number): void {
    this._state.lastRecentPulledSeq = seq;
    this.maybeSave();
  }

  markPushed(ids: string[]): void {
    for (const id of ids) {
      this._state.lastPushedRowIds.add(id);
    }
    // Cap the set to avoid unbounded growth — keep last 50 000 entries
    if (this._state.lastPushedRowIds.size > 50_000) {
      const arr = [...this._state.lastPushedRowIds];
      this._state.lastPushedRowIds = new Set(arr.slice(arr.length - 40_000));
    }
    this.maybeSave();
  }

  /**
   * Record that a set of rows are known to the server. The `rows` map
   * values can be either a plain `time_updated` number (legacy shape)
   * or an object carrying the row's `time_updated` plus an optional
   * `parent` session id for cascade-expansion via M1's rowParents
   * index. Mixing shapes in one call is allowed.
   */
  rememberRows(rows: Record<string, number | { time_updated: number; parent?: string }>): void {
    let changed = false;
    const db = this._db;

    for (const [rowKey, entry] of Object.entries(rows)) {
      const timeUpdated = typeof entry === "number" ? entry : entry.time_updated;
      const parent = typeof entry === "number" ? undefined : entry.parent;

      if (this._state.knownRows[rowKey] !== timeUpdated) {
        this._state.knownRows[rowKey] = timeUpdated;
        this._heavyLoaded = true;
        changed = true;
        db?.prepare("INSERT INTO known_rows (row_key, time_updated) VALUES (?, ?) ON CONFLICT(row_key) DO UPDATE SET time_updated = excluded.time_updated").run(rowKey, timeUpdated);
      }

      if (parent !== undefined && this._state.rowParents[rowKey] !== parent) {
        this._state.rowParents[rowKey] = parent;
        changed = true;
        db?.prepare("INSERT INTO row_parents (row_key, parent) VALUES (?, ?) ON CONFLICT(row_key) DO UPDATE SET parent = excluded.parent").run(rowKey, parent);
      }
    }

    if (changed && !db) this.maybeSave();
  }

  forgetRows(rowKeys: string[]): void {
    let changed = false;
    const db = this._db;

    for (const rowKey of rowKeys) {
      if (rowKey in this._state.knownRows) {
        delete this._state.knownRows[rowKey];
        changed = true;
      }
      if (rowKey in this._state.rowParents) {
        delete this._state.rowParents[rowKey];
        changed = true;
      }
      db?.prepare("DELETE FROM known_rows WHERE row_key = ?").run(rowKey);
      db?.prepare("DELETE FROM row_parents WHERE row_key = ?").run(rowKey);
    }

    if (changed && !db) this.maybeSave();
  }

  /**
   * Capture the current opencode.db fingerprint. Called after a successful
   * pushAll so the next sync cycle can detect a wipe/restore/replacement.
   *
   * Idempotent: same fingerprint passed in twice is a no-op (no save).
   */
  setDbFingerprint(
    fingerprint: { inode: number; mtime: number; size: number } | null,
  ): void {
    const current = this._state.dbFingerprint;
    if (
      current === fingerprint ||
      (current &&
        fingerprint &&
        current.inode === fingerprint.inode &&
        current.mtime === fingerprint.mtime &&
        current.size === fingerprint.size)
    ) {
      return;
    }
    this._state.dbFingerprint = fingerprint;
    this.maybeSave();
  }

  /**
   * Add a candidate to the two-cycle confirmation buffer. If the key is
   * already present, the existing `firstSeenAt` is preserved (so the
   * confirmation timer keeps counting from the original detection).
   */
  addPendingTombstone(rowKey: string, knownTimeUpdated: number): void {
    if (rowKey in this._state.pendingTombstones) return;
    this._state.pendingTombstones[rowKey] = {
      firstSeenAt: Date.now(),
      knownTimeUpdated,
    };
    this.maybeSave();
  }

  /**
   * Drop entries from the confirmation buffer — called both when a row
   * reappears (false positive) and after we successfully tombstone it.
   */
  removePendingTombstones(rowKeys: Iterable<string>): void {
    let changed = false;
    for (const key of rowKeys) {
      if (key in this._state.pendingTombstones) {
        delete this._state.pendingTombstones[key];
        changed = true;
      }
    }
    if (changed) this.maybeSave();
  }

  /**
   * Wipe ALL pending tombstones in one shot — used by the deletion-safety
   * guard when it defers the entire cycle (DB fingerprint mismatch, halt
   * marker present, etc.) so a transient corruption doesn't accumulate
   * pending entries that fire in concert later.
   */
  clearPendingTombstones(): void {
    if (Object.keys(this._state.pendingTombstones).length === 0) return;
    this._state.pendingTombstones = {};
    this.maybeSave();
  }

  /**
   * Increment the per-envelope error counter and return the new value.
   * Keyed by `${kind}:${id}:${server_seq}`. See FINDINGS.md H3.
   */
  incrementPullErrorCount(envelopeKey: string): number {
    const next = (this._state.pullErrorCounts[envelopeKey] ?? 0) + 1;
    this._state.pullErrorCounts[envelopeKey] = next;
    this.maybeSave();
    return next;
  }

  /**
   * Drop the counter for an envelope — called on successful apply OR
   * when we decide to poison-skip (the durable record moves to
   * `poisonedEnvelopes`).
   */
  clearPullErrorCount(envelopeKey: string): void {
    if (!(envelopeKey in this._state.pullErrorCounts)) return;
    delete this._state.pullErrorCounts[envelopeKey];
    this.maybeSave();
  }

  /**
   * Record an envelope as permanently skipped. FIFO-capped at
   * `POISONED_ENVELOPES_MAX` to bound state.json growth under attack.
   */
  recordPoisonedEnvelope(entry: {
    kind: SyncKind;
    id: string;
    server_seq: number;
    lastError?: string;
  }): void {
    this._state.poisonedEnvelopes.push({
      kind: entry.kind,
      id: entry.id,
      server_seq: entry.server_seq,
      skippedAt: Date.now(),
      lastError: entry.lastError,
    });
    this._poisonKeys.add(`${entry.kind}:${entry.id}:${entry.server_seq}`);
    if (this._state.poisonedEnvelopes.length > POISONED_ENVELOPES_MAX) {
      this._state.poisonedEnvelopes = this._state.poisonedEnvelopes.slice(
        this._state.poisonedEnvelopes.length - POISONED_ENVELOPES_MAX,
      );
      this.rebuildPoisonKeys();
    }
    this.maybeSave();
  }

  isPoisoned(kind: string, id: string, serverSeq: number): boolean {
    return this._poisonKeys.has(`${kind}:${id}:${serverSeq}`);
  }

  getKnownTime(rowKey: string): number | undefined {
    if (this._heavyLoaded || !this._db) return this._state.knownRows[rowKey];
    const row = this._db.prepare<{ time_updated: number }, [string]>(
      "SELECT time_updated FROM known_rows WHERE row_key = ?",
    ).get(rowKey);
    return row?.time_updated;
  }

  shouldReconcileDeletions(): boolean {
    if (Object.keys(this._state.pendingTombstones).length > 0) return true;
    const known = this.knownRowCount();
    if (known < 10_000) return true;
    if (this._lastDeletionReconcileAt === 0) return true;
    return Date.now() - this._lastDeletionReconcileAt >= DELETION_RECONCILE_INTERVAL_MS;
  }

  knownRowCount(): number {
    if (this._heavyLoaded || !this._db) return Object.keys(this._state.knownRows).length;
    const row = this._db.prepare<{ n: number }, []>("SELECT COUNT(*) AS n FROM known_rows").get();
    return row?.n ?? 0;
  }

  markDeletionReconciled(): void {
    this._lastDeletionReconcileAt = Date.now();
    this.maybeSave();
  }

  replaceKnownFiles(entries: FileManifestEntry[]): void {
    this._state.knownFiles = Object.fromEntries(
      entries.map((entry) => [
        entry.relpath,
        { sha256: entry.sha256, mtime: entry.mtime, size: entry.size },
      ]),
    );
    this._state.lastFileSyncTime = Date.now();
    this.maybeSave();
  }

  updateFileSyncTime(): void {
    this._state.lastFileSyncTime = Date.now();
    this.maybeSave();
  }

  private openDb(): void {
    if (this._db) return;
    fs.mkdirSync(STATE_DIR, { recursive: true });
    this._db = new Database(STATE_DB_FILE);
    this._db.exec("PRAGMA journal_mode = WAL");
    this._db.exec("PRAGMA synchronous = NORMAL");
    this._db.exec(STATE_SCHEMA);
  }

  private sqliteHasMeta(): boolean {
    if (!this._db) return false;
    const row = this._db.prepare<{ v: string }, [string]>("SELECT v FROM meta WHERE k = ?").get("lastPulledSeq");
    return row !== null && row !== undefined;
  }

  private loadFromSqlite(): void {
    const db = this._db!;
    const meta = (k: string): string | undefined =>
      db.prepare<{ v: string }, [string]>("SELECT v FROM meta WHERE k = ?").get(k)?.v;

    this._state.lastPulledSeq = Number(meta("lastPulledSeq") ?? 0) || 0;
    this._state.lastRecentPulledSeq = Number(meta("lastRecentPulledSeq") ?? 0) || 0;
    this._state.lastPushedRowTime = Number(meta("lastPushedRowTime") ?? 0) || 0;
    this._state.lastFileSyncTime = Number(meta("lastFileSyncTime") ?? 0) || 0;
    this._lastDeletionReconcileAt = Number(meta("lastDeletionReconcileAt") ?? 0) || 0;
    const fp = meta("dbFingerprint");
    this._state.dbFingerprint = fp ? this.parseDbFingerprint(JSON.parse(fp)) : null;

    this._state.lastPushedRowIds = new Set(
      db.prepare<{ id: string }, []>("SELECT id FROM last_pushed").all().map((row) => row.id),
    );

    this._state.knownFiles = {};
    for (const row of db.prepare<{ relpath: string; sha256: string; mtime: number; size: number }, []>(
      "SELECT relpath, sha256, mtime, size FROM known_files",
    ).all()) {
      this._state.knownFiles[row.relpath] = { sha256: row.sha256, mtime: row.mtime, size: row.size };
    }

    this._state.pendingTombstones = {};
    for (const row of db.prepare<{ row_key: string; first_seen_at: number; known_time_updated: number }, []>(
      "SELECT row_key, first_seen_at, known_time_updated FROM pending_tombstones",
    ).all()) {
      this._state.pendingTombstones[row.row_key] = {
        firstSeenAt: row.first_seen_at,
        knownTimeUpdated: row.known_time_updated,
      };
    }

    this._state.pullErrorCounts = {};
    for (const row of db.prepare<{ envelope_key: string; count: number }, []>(
      "SELECT envelope_key, count FROM pull_error_counts",
    ).all()) {
      this._state.pullErrorCounts[row.envelope_key] = row.count;
    }

    this._state.poisonedEnvelopes = db.prepare<{
      kind: string;
      id: string;
      server_seq: number;
      skipped_at: number;
      last_error: string | null;
    }, []>("SELECT kind, id, server_seq, skipped_at, last_error FROM poisoned").all().map((row) => ({
      kind: row.kind as SyncKind,
      id: row.id,
      server_seq: row.server_seq,
      skippedAt: row.skipped_at,
      ...(row.last_error ? { lastError: row.last_error } : {}),
    }));
    this.rebuildPoisonKeys();
    this._heavyLoaded = false;
  }

  private writeMeta(): void {
    const db = this._db!;
    const upsert = db.prepare("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v");
    upsert.run("lastPulledSeq", String(this._state.lastPulledSeq));
    upsert.run("lastRecentPulledSeq", String(this._state.lastRecentPulledSeq));
    upsert.run("lastPushedRowTime", String(this._state.lastPushedRowTime));
    upsert.run("lastFileSyncTime", String(this._state.lastFileSyncTime));
    upsert.run("lastDeletionReconcileAt", String(this._lastDeletionReconcileAt));
    upsert.run("dbFingerprint", JSON.stringify(this._state.dbFingerprint));
  }

  private ensureHeavy(): void {
    if (this._heavyLoaded) return;
    this._heavyLoaded = true;
    if (!this._db) return;
    for (const row of this._db.prepare<{ row_key: string; time_updated: number }, []>(
      "SELECT row_key, time_updated FROM known_rows",
    ).all()) {
      this._state.knownRows[row.row_key] = row.time_updated;
    }
    for (const row of this._db.prepare<{ row_key: string; parent: string }, []>(
      "SELECT row_key, parent FROM row_parents",
    ).all()) {
      this._state.rowParents[row.row_key] = row.parent;
    }
  }

  private rebuildPoisonKeys(): void {
    this._poisonKeys = new Set(
      this._state.poisonedEnvelopes.map((p) => `${p.kind}:${p.id}:${p.server_seq}`),
    );
  }

  private readJsonFile(filePath: string): Partial<SyncStateJson> | null {
    let raw: string;
    try {
      raw = fs.readFileSync(filePath, "utf-8");
    } catch (err) {
      logger.error("state.load: failed to read state file", {
        path: filePath,
        error: String(err),
      });
      throw err;
    }

    try {
      return JSON.parse(raw) as Partial<SyncStateJson>;
    } catch (err) {
      const backup = `${filePath}.corrupt-${Date.now()}`;
      try {
        fs.renameSync(filePath, backup);
      } catch (backupErr) {
        logger.error(
          "state.load: corrupt state.json AND backup failed — resetting to defaults",
          {
            path: filePath,
            parseError: String(err),
            backupError: String(backupErr),
          },
        );
        return null;
      }
      logger.error(
        "state.load: corrupt state.json — backed up and reset to defaults",
        {
          path: filePath,
          backup,
          bytes: raw.length,
          error: String(err),
        },
      );
      return null;
    }
  }

  private applyJson(json: Partial<SyncStateJson>): void {
    this._state.lastPulledSeq = json.lastPulledSeq ?? 0;
    this._state.lastRecentPulledSeq = json.lastRecentPulledSeq ?? 0;
    this._state.lastPushedRowIds = new Set(json.lastPushedRowIds ?? []);
    this._state.lastPushedRowTime = json.lastPushedRowTime ?? 0;
    this._state.knownRows = this.parseKnownRows(json.knownRows);
    this._state.knownFiles = this.parseKnownFiles(json.knownFiles);
    this._state.lastFileSyncTime = json.lastFileSyncTime ?? 0;
    this._state.dbFingerprint = this.parseDbFingerprint(json.dbFingerprint);
    this._state.pendingTombstones = this.parsePendingTombstones(json.pendingTombstones);
    this._state.pullErrorCounts = this.parsePullErrorCounts(json.pullErrorCounts);
    this._state.poisonedEnvelopes = this.parsePoisonedEnvelopes(json.poisonedEnvelopes);
    this._state.rowParents = this.parseRowParents(json.rowParents);
    this.rebuildPoisonKeys();
  }

  private parseKnownRows(value: unknown): Record<string, number> {
    if (!value || typeof value !== "object") return {};

    const entries = Object.entries(value as Record<string, unknown>).filter(
      ([, timeUpdated]) => typeof timeUpdated === "number" && Number.isFinite(timeUpdated),
    );

    return Object.fromEntries(entries) as Record<string, number>;
  }

  private parseDbFingerprint(
    value: unknown,
  ): { inode: number; mtime: number; size: number } | null {
    if (!value || typeof value !== "object") return null;
    const v = value as Record<string, unknown>;
    const inode = v["inode"];
    const mtime = v["mtime"];
    const size = v["size"];
    if (typeof inode !== "number" || !Number.isFinite(inode)) return null;
    if (typeof mtime !== "number" || !Number.isFinite(mtime)) return null;
    if (typeof size !== "number" || !Number.isFinite(size)) return null;
    return { inode, mtime, size };
  }

  private parsePendingTombstones(
    value: unknown,
  ): Record<string, { firstSeenAt: number; knownTimeUpdated: number }> {
    if (!value || typeof value !== "object") return {};
    const out: Record<string, { firstSeenAt: number; knownTimeUpdated: number }> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (!raw || typeof raw !== "object") continue;
      const e = raw as Record<string, unknown>;
      const firstSeenAt = e["firstSeenAt"];
      const knownTimeUpdated = e["knownTimeUpdated"];
      if (typeof firstSeenAt !== "number" || !Number.isFinite(firstSeenAt)) continue;
      if (typeof knownTimeUpdated !== "number" || !Number.isFinite(knownTimeUpdated)) continue;
      out[key] = { firstSeenAt, knownTimeUpdated };
    }
    return out;
  }

  private parsePullErrorCounts(value: unknown): Record<string, number> {
    if (!value || typeof value !== "object") return {};
    const out: Record<string, number> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (typeof raw === "number" && Number.isFinite(raw) && raw > 0) {
        out[key] = raw;
      }
    }
    return out;
  }

  private parsePoisonedEnvelopes(
    value: unknown,
  ): Array<{ kind: SyncKind; id: string; server_seq: number; skippedAt: number; lastError?: string }> {
    if (!Array.isArray(value)) return [];
    const out: Array<{
      kind: SyncKind;
      id: string;
      server_seq: number;
      skippedAt: number;
      lastError?: string;
    }> = [];
    for (const raw of value) {
      if (!raw || typeof raw !== "object") continue;
      const v = raw as Record<string, unknown>;
      const kind = v["kind"];
      const id = v["id"];
      const server_seq = v["server_seq"];
      const skippedAt = v["skippedAt"];
      const lastError = v["lastError"];
      if (typeof kind !== "string") continue;
      if (typeof id !== "string") continue;
      if (typeof server_seq !== "number" || !Number.isFinite(server_seq)) continue;
      if (typeof skippedAt !== "number" || !Number.isFinite(skippedAt)) continue;
      out.push({
        kind: kind as SyncKind,
        id,
        server_seq,
        skippedAt,
        ...(typeof lastError === "string" ? { lastError } : {}),
      });
    }
    return out;
  }

  private parseRowParents(value: unknown): Record<string, string> {
    if (!value || typeof value !== "object") return {};
    const out: Record<string, string> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (typeof raw === "string" && raw.length > 0) {
        out[key] = raw;
      }
    }
    return out;
  }

  private parseKnownFiles(
    value: unknown,
  ): Record<string, { sha256: string; mtime: number; size: number }> {
    if (!value || typeof value !== "object") return {};

    const entries = Object.entries(value as Record<string, unknown>).flatMap(([relpath, entry]) => {
      if (!entry || typeof entry !== "object") return [];

      const sha256 = (entry as Record<string, unknown>)["sha256"];
      const mtime = (entry as Record<string, unknown>)["mtime"];
      const size = (entry as Record<string, unknown>)["size"];

      if (typeof sha256 !== "string") return [];
      if (typeof mtime !== "number" || !Number.isFinite(mtime)) return [];

      // `size` was added later — older state files won't have it. Default
      // to -1 so the (mtime, size) cache check in computeLocalManifest
      // misses on first read after upgrade and re-hashes the file (which
      // re-populates size correctly). -1 is safer than 0 because a 0-byte
      // file with stable mtime would otherwise spuriously hit the cache.
      const sizeValue =
        typeof size === "number" && Number.isFinite(size) ? size : -1;

      return [[relpath, { sha256, mtime, size: sizeValue }] as const];
    });

    return Object.fromEntries(entries);
  }
}
