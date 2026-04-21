/**
 * Fingerprint helpers for opencode.db — the deletion-safety guard uses
 * these to detect when the database file under our feet has been wiped,
 * restored from backup, or replaced atomically (rename-over).
 *
 * Why three components:
 *
 * - `inode` catches atomic replacements. `mv backup.db opencode.db` (or
 *   `cp --reflink`, or `rsync --inplace=false`) gives the new file a new
 *   inode. SQLite holds the file open by FD, so opencode would still be
 *   reading the OLD inode — but the next time the plugin reopens the DB
 *   it'll see the NEW one. Inode mismatch is the strongest signal that
 *   "this is not the file we last synced from."
 *
 * - `mtime` catches in-place modification. SQLite WAL writes update the
 *   main DB file's mtime when the WAL is checkpointed. Sustained
 *   monotonic mtime growth is normal; mtime jumping BACKWARDS means a
 *   restore-from-backup happened in place (rare but possible).
 *
 * - `size` catches truncation/rebuild. `sqlite3 opencode.db .clone` into
 *   the same path, or any `VACUUM` followed by data loss, drops the
 *   size dramatically. A size drop of >50% is a near-certain signal of
 *   a destructive operation.
 *
 * None of these are perfect alone — together they cover the realistic
 * "user did something to opencode.db" scenarios without flagging
 * normal SQLite-internal mtime tics during checkpoint.
 */
import * as fs from "node:fs";

export interface DbFingerprint {
  inode: number;
  mtime: number;
  size: number;
}

/**
 * Stat the DB file and return its fingerprint, or null if the file
 * doesn't exist (which is itself a signal — caller decides what to do).
 *
 * Uses `bigint: false` for the inode; on Linux/macOS inodes fit
 * comfortably in JS Number range and round-tripping as bigint would
 * complicate JSON serialisation in state.json.
 */
export function captureDbFingerprint(dbPath: string): DbFingerprint | null {
  let stat: fs.Stats;
  try {
    stat = fs.statSync(dbPath);
  } catch {
    return null;
  }
  return {
    inode: stat.ino,
    mtime: stat.mtimeMs,
    size: stat.size,
  };
}

export interface FingerprintComparison {
  /** True iff the new fingerprint should be considered "the same DB". */
  same: boolean;
  /** Short machine-readable reason if different — populated even when same=true is borderline. */
  reason: string;
  /** Free-form details for logging. */
  details: Record<string, unknown>;
}

/**
 * Decide whether `next` plausibly represents the same opencode.db that
 * `previous` was captured from.
 *
 * Rules (in order — first match wins):
 *
 *   1. No previous fingerprint at all → same=true (first observation;
 *      caller will capture and move on).
 *   2. No current file (next is null) → same=false, reason="db_missing".
 *   3. Inode changed → same=false, reason="inode_changed". Strongest
 *      replacement signal.
 *   4. Size dropped by >50% → same=false, reason="size_shrunk". Catches
 *      truncation / rebuild even when inode survived (e.g. in-place
 *      `VACUUM INTO` followed by partial data load).
 *   5. mtime moved backward by more than 5 minutes → same=false,
 *      reason="mtime_regressed". Catches in-place restore-from-backup.
 *      The 5-minute fudge avoids false positives from clock skew /
 *      filesystem mtime resolution differences.
 *   6. Otherwise → same=true.
 */
export function compareFingerprints(
  previous: DbFingerprint | null,
  next: DbFingerprint | null,
): FingerprintComparison {
  if (previous === null) {
    return { same: true, reason: "no_previous", details: {} };
  }
  if (next === null) {
    return { same: false, reason: "db_missing", details: { previous } };
  }
  if (previous.inode !== next.inode) {
    return {
      same: false,
      reason: "inode_changed",
      details: {
        previous_inode: previous.inode,
        current_inode: next.inode,
      },
    };
  }
  if (previous.size > 0 && next.size < previous.size * 0.5) {
    return {
      same: false,
      reason: "size_shrunk",
      details: {
        previous_size: previous.size,
        current_size: next.size,
      },
    };
  }
  // 5-minute backwards-tolerance for clock / FS resolution noise
  const mtimeRegressionToleranceMs = 5 * 60 * 1000;
  if (next.mtime + mtimeRegressionToleranceMs < previous.mtime) {
    return {
      same: false,
      reason: "mtime_regressed",
      details: {
        previous_mtime: previous.mtime,
        current_mtime: next.mtime,
        regressed_by_ms: previous.mtime - next.mtime,
      },
    };
  }
  return { same: true, reason: "ok", details: {} };
}
