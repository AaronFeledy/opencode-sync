/**
 * Persistent sync state — tracks what we've pushed/pulled so far.
 * Stored at ~/.local/share/opencode/opencode-sync/state.json
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { FileManifestEntry } from "@opencode-sync/shared";
import { atomicWriteFileSync } from "./util.js";

// ── Types ──────────────────────────────────────────────────────────

export interface SyncState {
  machineId: string;
  /** Server-assigned monotonic cursor — pull rows with seq > this */
  lastPulledSeq: number;
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
}

/** JSON-serialisable representation of SyncState */
interface SyncStateJson {
  machineId: string;
  lastPulledSeq: number;
  lastPushedRowIds: string[];
  lastPushedRowTime?: number;
  knownRows?: Record<string, number>;
  knownFiles?: Record<string, { sha256: string; mtime: number; size?: number }>;
  lastFileSyncTime: number;
  dbFingerprint?: { inode: number; mtime: number; size: number } | null;
  pendingTombstones?: Record<string, { firstSeenAt: number; knownTimeUpdated: number }>;
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

export class StateManager {
  private _state: SyncState;
  /** Re-entrant batch depth. While > 0, mutating methods skip `save()`. */
  private _batchDepth = 0;
  /** Set by mutating methods when they would have saved during a batch. */
  private _batchDirty = false;

  constructor(machineId: string) {
    this._state = {
      machineId,
      lastPulledSeq: 0,
      lastPushedRowIds: new Set(),
      lastPushedRowTime: 0,
      knownRows: {},
      knownFiles: {},
      lastFileSyncTime: 0,
      dbFingerprint: null,
      pendingTombstones: {},
    };
  }

  /** Load state from disk, creating directory if needed. */
  load(): void {
    if (!fs.existsSync(STATE_FILE)) return;

    try {
      const raw = fs.readFileSync(STATE_FILE, "utf-8");
      const json = JSON.parse(raw) as Partial<SyncStateJson>;

      this._state.lastPulledSeq = json.lastPulledSeq ?? 0;
      this._state.lastPushedRowIds = new Set(json.lastPushedRowIds ?? []);
      this._state.lastPushedRowTime = json.lastPushedRowTime ?? 0;
      this._state.knownRows = this.parseKnownRows(json.knownRows);
      this._state.knownFiles = this.parseKnownFiles(json.knownFiles);
      this._state.lastFileSyncTime = json.lastFileSyncTime ?? 0;
      this._state.dbFingerprint = this.parseDbFingerprint(json.dbFingerprint);
      this._state.pendingTombstones = this.parsePendingTombstones(json.pendingTombstones);

      // Preserve machineId from constructor — don't override with stale file
    } catch {
      // Corrupted state file — start fresh
    }
  }

  /** Persist state to disk atomically. */
  save(): void {
    fs.mkdirSync(STATE_DIR, { recursive: true });

    const json: SyncStateJson = {
      machineId: this._state.machineId,
      lastPulledSeq: this._state.lastPulledSeq,
      lastPushedRowIds: [...this._state.lastPushedRowIds],
      lastPushedRowTime: this._state.lastPushedRowTime,
      knownRows: this._state.knownRows,
      knownFiles: this._state.knownFiles,
      lastFileSyncTime: this._state.lastFileSyncTime,
      dbFingerprint: this._state.dbFingerprint,
      pendingTombstones: this._state.pendingTombstones,
    };

    atomicWriteFileSync(STATE_FILE, JSON.stringify(json, null, 2));
  }

  /**
   * Compute the `since` filter to use when scanning local tables for a
   * `pushAll`. Returns `lastPushedRowTime - PUSH_CURSOR_MARGIN_MS`, clamped
   * to >= 0. On a fresh state (cursor=0) returns 0, so the first push reads
   * everything.
   */
  pushReadSince(): number {
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
    return this._state;
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
  async withBatch<T>(fn: () => Promise<T>): Promise<T> {
    this._batchDepth++;
    try {
      return await fn();
    } finally {
      this._batchDepth--;
      if (this._batchDepth === 0 && this._batchDirty) {
        this._batchDirty = false;
        this.save();
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

  rememberRows(rows: Record<string, number>): void {
    let changed = false;

    for (const [rowKey, timeUpdated] of Object.entries(rows)) {
      if (this._state.knownRows[rowKey] === timeUpdated) continue;
      this._state.knownRows[rowKey] = timeUpdated;
      changed = true;
    }

    if (changed) this.maybeSave();
  }

  forgetRows(rowKeys: string[]): void {
    let changed = false;

    for (const rowKey of rowKeys) {
      if (!(rowKey in this._state.knownRows)) continue;
      delete this._state.knownRows[rowKey];
      changed = true;
    }

    if (changed) this.maybeSave();
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
