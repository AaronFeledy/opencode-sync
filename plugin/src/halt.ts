/**
 * Sync-halt marker — a sticky filesystem flag that aborts row sync when
 * the deletion-safety guard detects a probable mass-tombstoning event.
 *
 * Purpose: prevent a corrupted/wiped/restored opencode.db from
 * propagating its own emptiness to every other peer via tombstones.
 * `buildDeletionEnvelopes` happily emits a tombstone for every
 * `knownRows` entry that's missing from the live DB — that's the
 * intended path for legitimate user deletions, but it's also the path
 * that turns a single local DB wipe into a fleet-wide data loss event.
 *
 * When the guard trips it writes this marker and *aborts the push*.
 * The marker file is intentionally not auto-cleared:
 *
 *   1. Auto-recovery would re-run the same risky logic on the next
 *      cycle. If the underlying issue (DB still wiped, fingerprint still
 *      mismatched) hasn't been resolved, we'd just trip again — or
 *      worse, the resolution criterion would race against a transient
 *      condition and let the deletions through.
 *
 *   2. A halted sync is a loud failure mode that the user should see
 *      and investigate. Silent recovery is exactly the wrong default
 *      for "we were about to delete potentially everything."
 *
 * Recovery is manual: user reads the marker contents, confirms the
 * local state is what they expect, and deletes the marker file.
 *
 * Located at `~/.local/share/opencode/opencode-sync/HALTED_SYNC` —
 * sibling to `state.json` so anyone debugging sync issues finds both.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { atomicWriteFileSync } from "./util.js";

const HALT_DIR = path.join(os.homedir(), ".local", "share", "opencode", "opencode-sync");
export const HALT_MARKER_PATH = path.join(HALT_DIR, "HALTED_SYNC");

export interface HaltDetails {
  /** When the halt was triggered (ms since epoch) */
  triggeredAt: number;
  /** Short machine-readable reason; see HALT_REASONS for known values */
  reason: string;
  /** Human-readable explanation of what was detected */
  message: string;
  /** Number of tombstones the guard would have emitted (when applicable) */
  candidateCount?: number;
  /** Total knownRows entries at detection time (when applicable) */
  knownRowsSize?: number;
  /** Up to 20 example rowKeys that would have been tombstoned, for forensics */
  sampleCandidates?: string[];
  /** Free-form extra diagnostic context */
  extra?: Record<string, unknown>;
}

export const HALT_REASONS = {
  /** Tombstone count exceeded threshold — likely DB wipe/restore. */
  TOMBSTONE_THRESHOLD: "tombstone_threshold",
  /** opencode.db file fingerprint changed unexpectedly between syncs. */
  DB_FINGERPRINT_MISMATCH: "db_fingerprint_mismatch",
} as const;

/**
 * True iff the halt marker exists. Cheap; safe to call on every sync.
 */
export function isSyncHalted(): boolean {
  try {
    return fs.existsSync(HALT_MARKER_PATH);
  } catch {
    // If we can't even stat the file, fail open rather than blocking
    // sync forever — fs errors here usually mean the directory doesn't
    // exist yet, which means no halt was ever set.
    return false;
  }
}

/**
 * Read the marker file's contents, or null if missing/unreadable.
 * Used by the boot path to log WHY sync is halted.
 */
export function readHaltDetails(): HaltDetails | null {
  try {
    if (!fs.existsSync(HALT_MARKER_PATH)) return null;
    const raw = fs.readFileSync(HALT_MARKER_PATH, "utf-8");
    // The marker is a JSON object wrapped in a human-readable header.
    // Find the JSON section by looking for the first `{` to the end.
    const jsonStart = raw.indexOf("{");
    if (jsonStart < 0) return null;
    return JSON.parse(raw.slice(jsonStart)) as HaltDetails;
  } catch {
    return null;
  }
}

/**
 * Write the halt marker. Idempotent: re-calling overwrites the file with
 * fresh details. We don't try to detect "already halted" here — the
 * caller (the deletion-safety guard) has already decided this cycle
 * should halt, and the freshest diagnostic context is more useful than
 * the original.
 */
export function writeHaltMarker(details: HaltDetails): void {
  try {
    fs.mkdirSync(HALT_DIR, { recursive: true });
  } catch {
    // If the directory can't be made, the atomic write below will throw
    // and propagate — that's fine; halting hard is the whole point.
  }

  const header =
    "# opencode-sync HALTED\n" +
    "#\n" +
    "# Row sync has been stopped because the deletion-safety guard\n" +
    "# detected a condition that would otherwise have propagated mass\n" +
    "# row deletions to every peer connected to your sync server.\n" +
    "#\n" +
    `# Triggered: ${new Date(details.triggeredAt).toISOString()}\n` +
    `# Reason:    ${details.reason}\n` +
    "#\n" +
    "# To resume sync:\n" +
    "#   1. Read the JSON below and the plugin log at\n" +
    "#      ~/.local/share/opencode/opencode-sync/plugin.log\n" +
    "#   2. Confirm the LOCAL opencode.db is in the state you expect.\n" +
    "#      (If a project's data is missing here, do NOT clear this file —\n" +
    "#      restore the data first, otherwise resuming sync will\n" +
    "#      propagate the deletion to all your other machines.)\n" +
    "#   3. Delete this file:  rm " + HALT_MARKER_PATH + "\n" +
    "#\n" +
    "# Note: file sync (config/auth) is unaffected and continues to run.\n" +
    "#\n";

  atomicWriteFileSync(
    HALT_MARKER_PATH,
    header + JSON.stringify(details, null, 2) + "\n",
  );
}

/**
 * Remove the marker file. NOT called automatically — exposed only so
 * tests can reset between cases. End users clear via `rm`.
 */
export function clearHaltMarker(): void {
  try {
    fs.unlinkSync(HALT_MARKER_PATH);
  } catch {
    // Already gone — fine.
  }
}
