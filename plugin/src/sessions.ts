/**
 * Session sync orchestration — push/pull/reconcile session data
 * between the local opencode DB and the remote sync server.
 */
import { SYNC_KINDS, type SyncEnvelope, type SyncKind } from "@opencode-sync/shared";
import type { DbReader } from "./db-read.js";
import type { ApplyResult, DbWriter } from "./db-write.js";
import type { SyncClient } from "./client.js";
import { EndpointMissingError } from "./client.js";
import { PULL_POISON_THRESHOLD, type StateManager } from "./state.js";
import { writeHaltMarker, isSyncHalted, HALT_REASONS } from "./halt.js";

// ── Constants ──────────────────────────────────────────────────────

const PUSH_BATCH_SIZE = 100;

type SessionSyncProgress = (message: string) => void;

type SessionSyncOptions = {
  progress?: SessionSyncProgress;
};

/**
 * Deletion-safety thresholds. All tombstone emission passes through
 * `buildDeletionEnvelopes`, which consults these before allowing any
 * row to be marked as deleted on the server.
 */

/**
 * Below this many `knownRows` entries we never trigger the threshold
 * halt — small states aren't a catastrophic-data-loss risk, and the
 * threshold's false-positive rate gets unusably high at tiny sizes
 * (losing 2-of-4 rows is 50%, but "50% of 4 rows" isn't a fleet
 * disaster).
 */
const TOMBSTONE_THRESHOLD_MIN_KNOWN = 50;

/**
 * Threshold for unexpected deletions when NO `session.deleted` events
 * have fired this cycle. With nothing supposedly being deleted by the
 * user, more than half the rows going missing is almost certainly a
 * DB wipe / restore / replacement.
 */
const TOMBSTONE_THRESHOLD_FRACTION_STRICT = 0.5;

/**
 * Threshold for unexpected deletions when at least one explicit
 * `markExpectedDeletion` call landed this cycle. The user is actively
 * deleting *something*, so we trust them more — only halt on
 * near-total disappearance.
 *
 * Why this matters: opencode's cascade FK delete means a single user
 * "delete this session" action can wipe 1 session row + many message
 * rows + many part rows + a handful of todos. We auto-expand the
 * `markExpectedDeletion('session:X')` hint to the session's todos and
 * share row, but messages and parts can't be cascade-expanded without
 * tracking parent_id in `knownRows` (a larger refactor). They flow
 * through as "unexpected" but should not trigger a halt — the user
 * really did mean to delete them.
 *
 * 0.95 still catches "DB wiped while a delete happened to be in
 * flight" because total wipe means 100% > 95%.
 */
const TOMBSTONE_THRESHOLD_FRACTION_PERMISSIVE = 0.95;

/**
 * A pending-tombstone candidate must remain missing for at least this
 * long before we actually emit the tombstone. Protects against
 * transient conditions — mid-migration, SQLITE_BUSY recovery, a
 * restore-in-progress — where the row is temporarily absent but about
 * to reappear.
 *
 * 30s is comfortably larger than the default 15s sync interval so the
 * confirmation takes at least two cycles (first sees → waits; second
 * sees after delay → emits). On fast intervals the delay still gates
 * on wall-clock time, not cycle count.
 */
const TOMBSTONE_CONFIRMATION_DELAY_MS = 30_000;

function rowStateKey(kind: SyncKind, id: string): string {
  return `${kind}:${id}`;
}

/**
 * Extract the parent session id for child row kinds. Used by M1's
 * `rowParents` secondary index so `markExpectedDeletion` can cascade-
 * expand a deleted session into its messages/parts/todos/shares
 * rather than leaving them in unexpectedCandidates and tripping the
 * deletion-safety threshold.
 *
 * Returns undefined for kinds with no session parent (project,
 * session, permission) or if the envelope's data lacks a session_id.
 */
function envelopeParentSession(envelope: SyncEnvelope): string | undefined {
  if (
    envelope.kind !== "message" &&
    envelope.kind !== "part" &&
    envelope.kind !== "todo" &&
    envelope.kind !== "session_share"
  ) {
    return undefined;
  }
  const data = envelope.data as Record<string, unknown> | null | undefined;
  if (!data || typeof data !== "object") return undefined;
  // session_share's rowPrimaryKey is the session id itself, but the
  // data also carries it via `session_id` in the schema. Prefer the
  // data field for consistency.
  const sid = data["session_id"];
  return typeof sid === "string" && sid.length > 0 ? sid : undefined;
}

/** Exported for test access only. Parses rowKey back into kind+id. */
export function parseRowStateKey(rowKey: string): { kind: SyncKind; id: string } | null {
  const separator = rowKey.indexOf(":");
  // `separator === 0` means an empty kind ("" + ":id"); reject along with
  // "no separator" so callers get a consistent null for any malformed key.
  // See FINDINGS.md M7.
  if (separator <= 0) return null;

  const kind = rowKey.slice(0, separator);
  const id = rowKey.slice(separator + 1);

  if (!id) return null;
  // Validate kind against the runtime SYNC_KINDS set — the downstream
  // cast-to-SyncKind would otherwise silently propagate unknown kinds
  // into push/heads traffic where they'd be rejected server-side.
  if (!(SYNC_KINDS as readonly string[]).includes(kind)) return null;
  return { kind: kind as SyncKind, id };
}

// ── Sync orchestrator ──────────────────────────────────────────────

export class SessionSync {
  private dbReader: DbReader;
  private dbWriter: DbWriter;
  private client: SyncClient;
  private stateManager: StateManager;
  private machineId: string;
  private log: (msg: string, data?: Record<string, unknown>) => void;

  /**
   * In-memory set of rowKeys the plugin has been told are "really"
   * being deleted — populated by `markExpectedDeletion` from the
   * `session.deleted` event hook. Rows in this set bypass the
   * two-cycle confirmation AND the server cross-check (they still
   * count toward the threshold but are subtracted before comparison
   * so legitimate big-cascade deletes aren't halted).
   *
   * Intentionally NOT persisted — on a plugin restart we want to fall
   * back to the conservative path. The next `session.deleted` event
   * (if any) will re-populate.
   */
  private expectedDeletions: Set<string> = new Set();

  /**
   * In-memory mirror of the on-disk halt marker, set immediately when
   * `buildDeletionEnvelopes` trips the threshold so a re-entrant
   * `pushSession` later in the same cycle can't slip tombstones past
   * the guard before the marker write has finished.
   *
   * Authoritative state lives on disk (`isSyncHalted()`); this flag is
   * a fast-path cache only. Always check both via `isHaltedNow()` —
   * the flag is `false` after a process restart even when the marker
   * exists, and it never sees fingerprint-mismatch halts (those write
   * the marker from `index.ts` without touching `SessionSync`).
   */
  private halted: boolean = false;

  constructor(
    dbReader: DbReader,
    dbWriter: DbWriter,
    client: SyncClient,
    stateManager: StateManager,
    machineId: string,
    log: (msg: string, data?: Record<string, unknown>) => void,
  ) {
    this.dbReader = dbReader;
    this.dbWriter = dbWriter;
    this.client = client;
    this.stateManager = stateManager;
    this.machineId = machineId;
    this.log = log;
  }

  /**
   * Mark a row as "expected to be deleted" — the next
   * `buildDeletionEnvelopes` call will fast-path its tombstone (no
   * server cross-check, no two-cycle wait) and exclude it from the
   * threshold calculation.
   *
   * Called from `hooks.ts` when opencode emits a deletion event we
   * initiated locally. Also call this for any cascade children you
   * know about (todos belonging to a deleted session, etc.) — we
   * auto-expand known `todo:<sessionId>:*` keys but messages/parts
   * have to be named explicitly.
   */
  markExpectedDeletion(rowKey: string): void {
    this.expectedDeletions.add(rowKey);

    // Auto-expand: if a whole session is being deleted, every row
    // whose `rowParents` entry points at this session should also be
    // marked expected. Without this (M1), users who delete their
    // biggest session would see messages/parts cascade through the
    // FK and land in unexpectedCandidates, tripping the 95% threshold
    // halt on a routine action.
    const parsed = parseRowStateKey(rowKey);
    if (parsed?.kind === "session") {
      const sessionId = parsed.id;
      for (const [childKey, parentSessionId] of Object.entries(
        this.stateManager.state.rowParents,
      )) {
        if (parentSessionId === sessionId) {
          this.expectedDeletions.add(childKey);
        }
      }
      // Belt-and-braces for the transition window before `rowParents`
      // is populated (e.g. first sync after upgrade): the legacy
      // todo-prefix scan + the directly-keyed session_share entry
      // still fire even if rowParents is empty for this session.
      const todoPrefix = `todo:${sessionId}:`;
      for (const knownKey of Object.keys(this.stateManager.state.knownRows)) {
        if (knownKey.startsWith(todoPrefix)) {
          this.expectedDeletions.add(knownKey);
        }
      }
      this.expectedDeletions.add(`session_share:${sessionId}`);
    }
  }

  /**
   * True iff the deletion-safety guard has halted row sync — either via
   * a same-process trip (`this.halted`) or via the persistent on-disk
   * marker that survives plugin restarts and also captures halts
   * written from outside `SessionSync` (e.g. fingerprint-mismatch in
   * `index.ts`). Used by tests, introspection, and event hooks that
   * need to know whether to bother queueing work.
   */
  isHalted(): boolean {
    return this.isHaltedNow();
  }

  /**
   * Internal halt check — every push/pull entry point consults this so
   * a halt that was set after the SessionSync instance was constructed
   * (or from a different code path) still blocks re-entry. The disk
   * `existsSync` check is cheap and safer than caching a boot-time
   * snapshot of the marker state.
   */
  private isHaltedNow(): boolean {
    return this.halted || isSyncHalted();
  }

  /**
   * Push local changes for a specific session and all its related data.
   *
   * Skipped entirely while the deletion-safety halt is in effect — even
   * though `pushSession` only emits live envelopes (never tombstones),
   * pushing live rows during a halt extends `knownRows` with rows that
   * the broken push path will then try to tombstone once the marker is
   * cleared. The conservative answer is to freeze ALL row sync until
   * the user has confirmed the local DB is in the state they expect.
   */
  async pushSession(sessionId: string): Promise<void> {
    if (this.isHaltedNow()) {
      this.log("pushSession skipped — sync halted by deletion-safety guard", { sessionId });
      return;
    }

    const full = this.dbReader.readSessionFull(sessionId);
    if (!full) {
      this.log("session not found locally, skipping push", { sessionId });
      return;
    }

    const envelopes = this.buildSessionEnvelopes(full);

    if (envelopes.length === 0) return;

    // Filter out already-pushed envelopes
    const toPush = envelopes.filter(
      (e) => !this.stateManager.state.lastPushedRowIds.has(`${e.kind}:${e.id}:${e.time_updated}`),
    );

    if (toPush.length === 0) return;

    // Batch the two state mutations (`markPushed` + `rememberRows`) into one
    // disk write.
    await this.stateManager.withBatch(async () => {
      const res = await this.client.push(this.machineId, toPush);
      this.stateManager.markPushed(
        toPush.map((e) => `${e.kind}:${e.id}:${e.time_updated}`),
      );
      this.rememberAcceptedRows(toPush, res.stale);

      this.log("pushed session", {
        sessionId,
        accepted: res.accepted.length,
        stale: res.stale.length,
      });
    });
  }

  /**
   * Push all local changes — full reconciliation across all tables.
   *
   * Streams rows from SQLite one envelope at a time and pushes in
   * batches of `PUSH_BATCH_SIZE`. Memory is bounded by one batch + the
   * deletion envelopes (which are PK-only and small). Previously this
   * loaded every row from every table into memory before batching the
   * network calls — fine for trickle syncs, but on a fresh peer with a
   * cursor of 0 it could easily consume gigabytes (each `part` row
   * carries a full JSON conversation blob).
   *
   * Halt behaviour, in order of precedence:
   *
   *   - On entry, if the deletion-safety halt is in effect (either this
   *     process already tripped it, or the on-disk marker exists from a
   *     prior run / fingerprint-mismatch / manual write), skip the
   *     entire cycle. No pushes of any kind.
   *
   *   - If the guard trips *inside* this cycle (threshold fires from
   *     within `buildDeletionEnvelopes`), any live-row batches that
   *     were already flushed before the trip remain pushed — the
   *     network calls can't be unwound. Any live rows buffered but not
   *     yet flushed are also sent, so the user's actual data doesn't
   *     get stranded mid-cycle. Tombstone emission is suppressed and
   *     the halt is latched; the NEXT call will short-circuit at the
   *     entry guard above.
   */
  async pushAll(options: SessionSyncOptions = {}): Promise<void> {
    if (this.isHaltedNow()) {
      this.log("pushAll skipped — sync halted by deletion-safety guard");
      options.progress?.("halted by deletion-safety guard");
      return;
    }

    // Persist state as each batch completes. Startup pushes can be long, and
    // an interrupted process should resume from the last confirmed batch
    // instead of replaying everything from the beginning.
    const since = this.stateManager.pushReadSince();
    options.progress?.(`scanning local changes since ${since}`);

    let totalSeen = 0;
    let totalAccepted = 0;
    let totalStale = 0;
    let totalTombstones = 0;
    let totalAlreadySynced = 0;
    let maxPushedTime = 0;
    let maxCheckedLiveTime = 0;
    let pendingBatch: SyncEnvelope[] = [];

    const flushBatch = async (): Promise<void> => {
      if (pendingBatch.length === 0) return;
      const batch = pendingBatch;
      pendingBatch = [];
      let batchMaxLiveTime = 0;

      // Filter already-pushed inside the flush so the streaming loop
      // doesn't have to know about the dedup set. Most batches will
      // pass through unchanged on a steady-state sync; on a re-sync
      // after a crash the dedup avoids re-pushing rows we already
      // confirmed.
      const toPush: SyncEnvelope[] = [];
      for (const e of batch) {
        if (!e.deleted && e.time_updated > batchMaxLiveTime) {
          batchMaxLiveTime = e.time_updated;
        }

        const pushedKey = `${e.kind}:${e.id}:${e.time_updated}`;
        if (this.stateManager.state.lastPushedRowIds.has(pushedKey)) {
          totalAlreadySynced++;
          continue;
        }

        const knownTime = this.stateManager.state.knownRows[rowStateKey(e.kind, e.id)];
        if (knownTime !== undefined && knownTime >= e.time_updated) {
          totalAlreadySynced++;
          continue;
        }

        toPush.push(e);
      }

      if (toPush.length === 0) {
        if (batchMaxLiveTime > 0) {
          this.stateManager.advancePushedRowTime(batchMaxLiveTime);
        }
        return;
      }

      const res = await this.client.push(this.machineId, toPush);

      await this.stateManager.withBatch(async () => {
        this.stateManager.markPushed(
          toPush.map((e) => `${e.kind}:${e.id}:${e.time_updated}`),
        );

        this.rememberAcceptedRows(toPush, res.stale);
        this.forgetAcceptedTombstones(toPush, res.stale);

        if (batchMaxLiveTime > 0) {
          this.stateManager.advancePushedRowTime(batchMaxLiveTime);
        }
      });

      totalAccepted += res.accepted.length;
      totalStale += res.stale.length;

      for (const env of toPush) {
        if (env.time_updated > maxPushedTime) maxPushedTime = env.time_updated;
      }

      options.progress?.(
        `push: accepted ${totalAccepted}/${totalSeen} checked` +
          (totalStale > 0 ? `, ${totalStale} stale` : "") +
          (totalAlreadySynced > 0 ? `, ${totalAlreadySynced} already synced` : ""),
      );
    };

    // Stream rows from every kind. Generator yields one envelope at a
    // time, so peak memory is roughly PUSH_BATCH_SIZE envelopes plus
    // whatever bun:sqlite buffers internally for the active statement.
    for (const env of this.dbReader.iterateAllEnvelopes(since, this.machineId)) {
      pendingBatch.push(env);
      totalSeen++;
      if (env.time_updated > maxCheckedLiveTime) maxCheckedLiveTime = env.time_updated;
      if (pendingBatch.length >= PUSH_BATCH_SIZE) {
        await flushBatch();
      }
    }

    // Compute deletion envelopes through the safety-guarded async path.
    // Returns null when the guard halted the cycle (threshold tripped,
    // fingerprint mismatch, etc.) — see buildDeletionEnvelopes for the
    // full ordering. We still flush the live-row batches above so the
    // user's actual data keeps making it to the server.
    options.progress?.("checking for deleted rows");
    const tombstones = await this.buildDeletionEnvelopes();
    if (tombstones === null) {
      // Halted. Flush any pending live rows, then return.
      await flushBatch();
      if (totalSeen > 0) {
        const maxSyncedTime = Math.max(maxPushedTime, maxCheckedLiveTime);
        if (maxSyncedTime > 0) {
          this.stateManager.advancePushedRowTime(maxSyncedTime);
        }
        this.log("pushAll partial — live rows pushed, deletions halted", {
          total: totalSeen,
          accepted: totalAccepted,
          stale: totalStale,
        });
      }
      options.progress?.(
        `push: accepted ${totalAccepted}/${totalSeen} checked; deletions halted`,
      );
      return;
    }

    options.progress?.(`queued ${tombstones.length} deleted rows`);

    // Tombstones come from comparing knownRows to live PKs — bounded by
    // the size of knownRows (PKs only, no row data), so loading them
    // all is fine.
    for (const env of tombstones) {
      pendingBatch.push(env);
      totalSeen++;
      totalTombstones++;
      if (pendingBatch.length >= PUSH_BATCH_SIZE) {
        await flushBatch();
      }
    }

    // Final partial batch.
    await flushBatch();

    // Successful tombstone emission means the candidates won't recur
    // next cycle (server has them, knownRows lost them via
    // forgetAcceptedTombstones). Drain expectedDeletions to avoid
    // unbounded growth from accumulated session.deleted events.
    if (totalTombstones > 0) {
      this.expectedDeletions.clear();
    }

    if (totalSeen === 0) {
      this.log("pushAll: nothing new to push");
      options.progress?.("push: already up to date");
      return;
    }

    // Advance the push cursor so the next pushAll's delta-read can skip
    // everything older than this batch (with a safety margin for clock
    // skew). See StateManager.pushReadSince for details.
    const maxSyncedTime = Math.max(maxPushedTime, maxCheckedLiveTime);
    if (maxSyncedTime > 0) {
      this.stateManager.advancePushedRowTime(maxSyncedTime);
    }

    this.log("pushAll complete", {
      total: totalSeen,
      accepted: totalAccepted,
      stale: totalStale,
      tombstones: totalTombstones,
    });
    options.progress?.(
      `push: accepted ${totalAccepted}/${totalSeen} checked` +
        (totalTombstones > 0 ? `, ${totalTombstones} deleted` : "") +
        (totalStale > 0 ? `, ${totalStale} stale` : "") +
        (totalAlreadySynced > 0 ? `, ${totalAlreadySynced} already synced` : ""),
    );
  }

  /**
   * Pull remote changes and apply them to the local DB.
   */
  async pull(options: SessionSyncOptions = {}): Promise<{ applied: number; conflicts: number; errors: number }> {
    // Wrap the whole paginated pull so per-page state mutations
    // (`rememberRows`, `forgetRows`, `updateSeq`) collapse into a single
    // state.json write at the end. The dozens of redundant writes per pull
    // were measurable on large backlogs.
    return this.stateManager.withBatch(async () => {
      return this.pullInternal(options.progress);
    });
  }

  private async pullInternal(progress?: SessionSyncProgress): Promise<{ applied: number; conflicts: number; errors: number }> {
    let applied = 0;
    let conflicts = 0;
    let errors = 0;
    let pulled = 0;
    let hasMore = true;

    while (hasMore) {
      progress?.(`pulling remote rows after #${this.stateManager.state.lastPulledSeq}`);
      const res = await this.client.pull(
        this.stateManager.state.lastPulledSeq,
        this.machineId,
      );
      pulled += res.envelopes.length;

      const rememberedRows: Record<string, { time_updated: number; parent?: string }> = {};
      const forgottenRows = new Set<string>();
      const convergedPushedIds: string[] = [];

      // Track the last server_seq we successfully processed BEFORE the
      // first error in this page. If anything errors, we advance the
      // cursor only to that point and stop pagination — re-pulling next
      // cycle gives the failed envelope a chance to apply.
      //
      // Why not just advance to the page tail like before: the previous
      // behaviour silently dropped errored rows because their seq landed
      // below the cursor and the server only re-sends rows with seq >
      // cursor. Transient errors (FK violation when parent hasn't
      // arrived yet, SQLITE_BUSY, etc.) became permanent data loss.
      //
      // H3: to prevent a permanently-bad envelope from blocking all
      // subsequent pulls forever, each failing envelope has a persisted
      // retry counter keyed by `${kind}:${id}:${server_seq}`. After
      // `PULL_POISON_THRESHOLD` retries, the envelope is recorded in
      // `poisonedEnvelopes` and skipped past — the cursor advances so
      // subsequent pulls can proceed. See FINDINGS.md H3.
      let lastGoodSeq = this.stateManager.state.lastPulledSeq;
      let firstErrorSeq: number | null = null;

      for (const envelope of res.envelopes) {
        const envelopeKey = `${envelope.kind}:${envelope.id}:${envelope.server_seq}`;
        
        // Skip already-poisoned envelopes to avoid re-applying and duplicate
        // entries. Advance cursor if no prior error blocked us.
        const alreadyPoisoned = this.stateManager.state.poisonedEnvelopes.some(
          (p) =>
            p.kind === envelope.kind &&
            p.id === envelope.id &&
            p.server_seq === envelope.server_seq,
        );
        if (alreadyPoisoned) {
          if (firstErrorSeq === null) {
            lastGoodSeq = envelope.server_seq;
          }
          continue;
        }
        
        let result: ApplyResult;
        let thrownError: string | null = null;
        try {
          result = this.dbWriter.applyEnvelope(envelope);
        } catch (err) {
          // Defensive: applyEnvelope is supposed to return "error" rather
          // than throw, but anything that does escape lands here.
          result = "error";
          thrownError = String(err);
          this.log("envelope apply threw (will retry next cycle)", {
            kind: envelope.kind,
            id: envelope.id,
            error: thrownError,
          });
        }

        if (result === "applied" || result === "skipped") {
          const rowKey = rowStateKey(envelope.kind, envelope.id);
          convergedPushedIds.push(`${envelope.kind}:${envelope.id}:${envelope.time_updated}`);

          if (envelope.deleted) {
            forgottenRows.add(rowKey);
          } else {
            const parent = envelopeParentSession(envelope);
            rememberedRows[rowKey] = parent !== undefined
              ? { time_updated: envelope.time_updated, parent }
              : { time_updated: envelope.time_updated };
          }

          if (result === "applied") {
            applied++;
          }

          // Clear any prior error counter on success or idempotent convergence.
          if (envelopeKey in this.stateManager.state.pullErrorCounts) {
            this.stateManager.clearPullErrorCount(envelopeKey);
          }
        } else if (result === "conflict") {
          // Local row is strictly newer than the remote — preserve local
          // and report as a real conflict (per SPEC §6.3 step 4).
          conflicts++;
          // Conflict is a successful LWW outcome — clear any counter.
          if (envelopeKey in this.stateManager.state.pullErrorCounts) {
            this.stateManager.clearPullErrorCount(envelopeKey);
          }
        } else {
          // result === "error"
          errors++;
          const attempts = this.stateManager.incrementPullErrorCount(envelopeKey);
          if (attempts >= PULL_POISON_THRESHOLD) {
            // Permanent skip: record for operator audit, clear the
            // counter, treat as "applied-for-cursor-purposes" so the
            // loop continues past it.
            this.log("POISON ENVELOPE — skipping after N retries", {
              kind: envelope.kind,
              id: envelope.id,
              server_seq: envelope.server_seq,
              attempts,
              lastError: thrownError ?? undefined,
            });
            this.stateManager.recordPoisonedEnvelope({
              kind: envelope.kind,
              id: envelope.id,
              server_seq: envelope.server_seq,
              ...(thrownError ? { lastError: thrownError } : {}),
            });
            this.stateManager.clearPullErrorCount(envelopeKey);
            // Advance lastGoodSeq so the cursor crosses the poisoned
            // envelope and subsequent pulls can proceed.
            if (firstErrorSeq === null) {
              lastGoodSeq = envelope.server_seq;
            }
            continue;
          }
          if (firstErrorSeq === null) firstErrorSeq = envelope.server_seq;
          this.log("envelope apply failed (will retry next cycle)", {
            kind: envelope.kind,
            id: envelope.id,
            time_updated: envelope.time_updated,
            server_seq: envelope.server_seq,
            attempts,
          });
          // Don't advance lastGoodSeq past the error.
          continue;
        }

        // Track the highest seq successfully processed up to (but not
        // including) the first error. After the first error, we still
        // process the rest of the page so error counts are accurate, but
        // we don't extend lastGoodSeq.
        if (firstErrorSeq === null) {
          lastGoodSeq = envelope.server_seq;
        }
      }

      if (Object.keys(rememberedRows).length > 0) {
        this.stateManager.rememberRows(rememberedRows);
      }
      if (forgottenRows.size > 0) {
        this.stateManager.forgetRows([...forgottenRows]);
      }
      if (convergedPushedIds.length > 0) {
        this.stateManager.markPushed(convergedPushedIds);
      }

      // Note: do NOT advance lastPushedRowTime here based on remote envelope
      // timestamps. Doing so clamps to Date.now() (via advancePushedRowTime)
      // and causes pushReadSince(cursor - 60_000) to skip local-only rows
      // older than ~60s on a fresh-state machine. The dedup checks in
      // flushBatch (lastPushedRowIds + knownRows[knownTime] >= e.time_updated)
      // are sufficient to suppress echoes without advancing the push cursor.

      if (firstErrorSeq !== null) {
        // Advance only past the prefix of cleanly-applied envelopes (if
        // any) and stop pagination. Subsequent pages would be at higher
        // seqs — fetching them would force the cursor past the failed
        // envelope on success. Better to retry this page from the same
        // cursor next cycle.
        if (lastGoodSeq > this.stateManager.state.lastPulledSeq) {
          this.stateManager.updateSeq(lastGoodSeq);
        }
        break;
      }

      if (res.envelopes.length > 0) {
        // Advance the cursor to the last envelope in this batch — NOT to
        // res.server_seq, which is the server's global high-watermark across
        // the entire ledger. When a backlog exceeds the page limit, jumping
        // to the global max would skip every row between the batch tail and
        // the global max on the next pagination request.
        // pullRows() guarantees ascending server_seq order.
        const batchMaxSeq = res.envelopes[res.envelopes.length - 1]!.server_seq;
        this.stateManager.updateSeq(batchMaxSeq);
      } else if (res.more) {
        // Defensive: the current server can't produce {envelopes:[], more:true}
        // (its `more` flag is `rows.length > limit`, so empty rows implies
        // more=false), but if a future server bug or proxy did send that
        // shape we'd loop forever requesting the same `since` cursor.
        // Break out and let the next sync cycle retry from the same cursor.
        this.log("pull: server returned more=true with no envelopes; breaking", {
          since: this.stateManager.state.lastPulledSeq,
          server_seq: res.server_seq,
        });
        break;
      } else {
        // No envelopes and nothing more to fetch — we're caught up to the
        // server's global high-watermark; persist that so the cursor reflects
        // reality even when no rows changed.
        this.stateManager.updateSeq(res.server_seq);
      }

      progress?.(
        `pull: pulled ${pulled} remote rows, applied ${applied}, conflicts ${conflicts}, errors ${errors}` +
          (res.more ? "; fetching more" : ""),
      );

      hasMore = res.more;
    }

    if (applied > 0 || conflicts > 0 || errors > 0) {
      this.log("pull complete", { applied, conflicts, errors });
    }

    return { applied, conflicts, errors };
  }

  /**
   * Full sync cycle: pull first (to get latest remote state), then push
   * local changes.
   *
   * Skipped entirely when the deletion-safety halt is in effect. Pull
   * has to be gated alongside push because pulling extends `knownRows`
   * with rows the (still-broken) push path would then try to tombstone
   * once the halt is cleared. The timer path in `index.ts` already
   * blocks `runRowSync` on the same condition; this entry guard makes
   * event-driven callers (`server.connected`, hook-driven sync) match.
   */
  async sync(options: SessionSyncOptions = {}): Promise<void> {
    if (this.isHaltedNow()) {
      this.log("sync skipped — sync halted by deletion-safety guard");
      options.progress?.("halted by deletion-safety guard");
      return;
    }
    options.progress?.("pulling remote changes");
    await this.pull(options);
    options.progress?.("pushing local changes");
    await this.pushAll(options);
  }

  private buildSessionEnvelopes(
    full: { session: unknown; messages: unknown[]; parts: unknown[]; todos: unknown[] },
  ): SyncEnvelope[] {
    return [
      ...this.dbReader.toEnvelopes("session", [full.session as Record<string, unknown>], this.machineId),
      ...this.dbReader.toEnvelopes("message", full.messages as Record<string, unknown>[], this.machineId),
      ...this.dbReader.toEnvelopes("part", full.parts as Record<string, unknown>[], this.machineId),
      ...this.dbReader.toEnvelopes("todo", full.todos as Record<string, unknown>[], this.machineId),
    ];
  }

  /**
   * Compute the set of tombstone envelopes to emit this cycle, gated by
   * the deletion-safety guard. Returns `null` if the guard halts the
   * cycle (caller should suppress tombstone emission entirely but may
   * continue pushing live rows).
   *
   * The safety layers run in this order. Each is independently
   * sufficient to halt or filter a candidate; later layers only see
   * candidates that earlier layers approved.
   *
   *   1. PK scan & expected-deletions partition. Compute candidates =
   *      knownRows \ liveKeys. Split into "expected" (rows we
   *      affirmatively know are being deleted via session.deleted hooks)
   *      vs "unexpected" (rows that just disappeared).
   *
   *   2. Threshold halt. If the unexpected set exceeds
   *      TOMBSTONE_THRESHOLD_FRACTION of knownRows AND knownRows is
   *      large enough to be statistically meaningful, write the halt
   *      marker and return null. Future syncs are blocked until the
   *      user clears the marker.
   *
   *   3. Server cross-check. Ask the server for its current head state
   *      on each unexpected candidate. Drop any whose server head is
   *      newer than what we last knew — pulling that newer version is
   *      preferable to overwriting it with our tombstone. Falls back
   *      gracefully if the server doesn't expose /sync/heads.
   *
   *   4. Two-cycle confirmation. New unexpected candidates go into
   *      `pendingTombstones` and are NOT tombstoned this cycle. Only
   *      candidates that have been continuously missing for at least
   *      TOMBSTONE_CONFIRMATION_DELAY_MS get emitted. Reappeared
   *      candidates are dropped from the pending buffer.
   *
   *   5. Expected deletions bypass steps 3 and 4 — they're emitted
   *      immediately. They still count toward the tombstones returned,
   *      but were excluded from the threshold in step 2.
   *
   * Uses a PK-only scan rather than the (possibly delta-filtered)
   * `currentEnvelopes` set. Otherwise an old, unchanged row would be
   * absent from the delta and we'd falsely tombstone it.
   */
  private async buildDeletionEnvelopes(): Promise<SyncEnvelope[] | null> {
    const liveKeys = this.dbReader.readAllRowKeys();
    const knownRows = this.stateManager.state.knownRows;
    const knownSize = Object.keys(knownRows).length;

    // ── Step 1: partition candidates ──
    const expectedCandidates: Array<{ rowKey: string; knownTimeUpdated: number }> = [];
    const unexpectedCandidates: Array<{ rowKey: string; knownTimeUpdated: number }> = [];
    for (const [rowKey, knownTimeUpdated] of Object.entries(knownRows)) {
      if (liveKeys.has(rowKey)) {
        // Reappeared (or never gone) — clear any stale pending entry.
        if (rowKey in this.stateManager.state.pendingTombstones) {
          this.stateManager.removePendingTombstones([rowKey]);
        }
        continue;
      }
      if (this.expectedDeletions.has(rowKey)) {
        expectedCandidates.push({ rowKey, knownTimeUpdated });
      } else {
        unexpectedCandidates.push({ rowKey, knownTimeUpdated });
      }
    }

    // ── Step 2: threshold halt ──
    // Adaptive threshold: stricter (50%) when we have NO evidence the
    // user is actively deleting anything; permissive (95%) when at
    // least one expected deletion is present. The permissive mode
    // exists so a single "delete this big session" action — which
    // cascades to many uncategorised message/part rows that flow
    // through as "unexpected" — doesn't trip the guard.
    const fraction = expectedCandidates.length > 0
      ? TOMBSTONE_THRESHOLD_FRACTION_PERMISSIVE
      : TOMBSTONE_THRESHOLD_FRACTION_STRICT;
    if (
      knownSize >= TOMBSTONE_THRESHOLD_MIN_KNOWN &&
      unexpectedCandidates.length > knownSize * fraction
    ) {
      const sample = unexpectedCandidates
        .slice(0, 20)
        .map((c) => c.rowKey);
      this.log("DELETION-SAFETY HALT — too many unexpected tombstones", {
        unexpected: unexpectedCandidates.length,
        expected: expectedCandidates.length,
        known: knownSize,
        threshold_fraction: fraction,
        sample,
      });
      writeHaltMarker({
        triggeredAt: Date.now(),
        reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
        message:
          `Would have emitted ${unexpectedCandidates.length} unexpected tombstones ` +
          `(${((unexpectedCandidates.length / knownSize) * 100).toFixed(1)}% of ` +
          `${knownSize} known rows; threshold ${(fraction * 100).toFixed(0)}%). ` +
          `This usually means the local opencode.db has been wiped, restored ` +
          `from an older backup, or was opened from the wrong path. Sync push ` +
          `is halted until you remove the marker file.`,
        candidateCount: unexpectedCandidates.length,
        knownRowsSize: knownSize,
        sampleCandidates: sample,
        extra: { expected_count: expectedCandidates.length, threshold_fraction: fraction },
      });
      this.halted = true;
      // Drop any in-flight pending entries — when the user resolves the
      // underlying problem and clears the marker, we want a clean slate
      // rather than firing accumulated pending entries on first cycle.
      this.stateManager.clearPendingTombstones();
      return null;
    }

    // ── Step 3: server cross-check (best-effort) ──
    let heads: Map<string, { time_updated: number; deleted: boolean }> | null = null;
    if (unexpectedCandidates.length > 0) {
      // Drop rowKeys that don't parse as `kind:id` — `parseRowStateKey`
      // requires a `:` separator and a non-empty id. Under normal
      // operation `knownRows` is only ever populated via `rowStateKey`
      // which always emits valid keys, so a malformed entry here means
      // either a manually-edited state.json or a future-schema rowKey
      // we don't recognise. Log so the corruption is visible.
      const headsRequest: Array<{ kind: SyncKind; id: string }> = [];
      const malformed: string[] = [];
      for (const candidate of unexpectedCandidates) {
        const parsed = parseRowStateKey(candidate.rowKey);
        if (parsed) {
          headsRequest.push({ kind: parsed.kind, id: parsed.id });
        } else {
          malformed.push(candidate.rowKey);
        }
      }
      if (malformed.length > 0) {
        this.log("WARN: skipping malformed rowKeys during heads cross-check", {
          count: malformed.length,
          sample: malformed.slice(0, 10),
        });
      }
      try {
        heads = await this.client.getHeads(this.machineId, headsRequest);
      } catch (err) {
        if (err instanceof EndpointMissingError) {
          // Older server without /sync/heads — degrade gracefully.
          // Two-cycle confirmation still runs, just without server input.
          this.log("server lacks /sync/heads endpoint, skipping cross-check");
        } else {
          // Network error — be conservative and SKIP this cycle's
          // unexpected tombstones rather than emit blind. Also clear
          // the pending-confirmation buffer: if we left it intact, a
          // long outage would drift `firstSeenAt` far past the 30s
          // confirmation window and the first recovered cycle would
          // emit tombstones without any post-recovery re-check. The
          // detection itself is re-derived cheaply from knownRows vs
          // live DB on every cycle, so clearing only costs us at most
          // one cycle of progress through the confirmation window.
          // Expected deletions still proceed below. See FINDINGS.md H2.
          this.log("getHeads failed, deferring unexpected tombstones this cycle", {
            error: String(err),
          });
          this.stateManager.clearPendingTombstones();
          return this.formatTombstones(expectedCandidates);
        }
      }
    }

    // ── Step 4: two-cycle confirmation + server-newer filter ──
    const now = Date.now();
    const tombstoneRows: Array<{ rowKey: string; knownTimeUpdated: number }> = [];
    const reappeared: string[] = [];
    for (const candidate of unexpectedCandidates) {
      // Server has a newer version — prefer the remote, drop the tombstone.
      const head = heads?.get(candidate.rowKey);
      if (head && !head.deleted && head.time_updated > candidate.knownTimeUpdated) {
        // Also drop any pending entry for this row — it's not actually
        // gone everywhere, just locally outdated.
        if (candidate.rowKey in this.stateManager.state.pendingTombstones) {
          reappeared.push(candidate.rowKey);
        }
        continue;
      }

      const pending = this.stateManager.state.pendingTombstones[candidate.rowKey];
      if (!pending) {
        // First sighting — record and wait.
        this.stateManager.addPendingTombstone(
          candidate.rowKey,
          candidate.knownTimeUpdated,
        );
        continue;
      }

      // Already pending — check the confirmation timer.
      if (now - pending.firstSeenAt < TOMBSTONE_CONFIRMATION_DELAY_MS) {
        continue;
      }

      // Confirmed: emit the tombstone. We use the originally-recorded
      // `knownTimeUpdated` (from when it first went missing) so racing
      // updates that happened to slip into knownRows after detection
      // don't subtly change the LWW comparison the server will do.
      tombstoneRows.push({
        rowKey: candidate.rowKey,
        knownTimeUpdated: pending.knownTimeUpdated,
      });
    }

    if (reappeared.length > 0) {
      this.stateManager.removePendingTombstones(reappeared);
    }

    // ── Step 5: format output ──
    // Expected deletions are emitted immediately and unconditionally
    // (subject only to the threshold above). Confirmed unexpected
    // deletions join them. Pending entries that successfully made it
    // out as tombstones are cleared from the buffer up-front; if the
    // push later fails, `forgetAcceptedTombstones` won't fire — the
    // entry will be re-detected and re-pended on the next cycle, which
    // is the correct behaviour.
    if (tombstoneRows.length > 0) {
      this.stateManager.removePendingTombstones(
        tombstoneRows.map((t) => t.rowKey),
      );
    }

    return this.formatTombstones([...expectedCandidates, ...tombstoneRows]);
  }

  /**
   * Convert a list of (rowKey, knownTimeUpdated) pairs into tombstone
   * envelopes. Stamps each tombstone with `Math.max(now, knownTime + 1)`
   * — guarantees LWW wins over the version we last saw.
   */
  private formatTombstones(
    rows: Array<{ rowKey: string; knownTimeUpdated: number }>,
  ): SyncEnvelope[] {
    const out: SyncEnvelope[] = [];
    for (const { rowKey, knownTimeUpdated } of rows) {
      const parsed = parseRowStateKey(rowKey);
      if (!parsed) continue;
      out.push({
        kind: parsed.kind,
        id: parsed.id,
        machine_id: this.machineId,
        time_updated: Math.max(Date.now(), knownTimeUpdated + 1),
        server_seq: 0,
        deleted: true,
        data: null,
      });
    }
    return out;
  }

  private rememberAcceptedRows(
    envelopes: SyncEnvelope[],
    stale: Array<{ kind: SyncKind; id: string }>,
  ): void {
    const staleKeys = new Set(stale.map((entry) => rowStateKey(entry.kind, entry.id)));
    const rememberedRows: Record<string, { time_updated: number; parent?: string }> = {};
    for (const envelope of envelopes) {
      if (envelope.deleted) continue;
      const rowKey = rowStateKey(envelope.kind, envelope.id);
      if (staleKeys.has(rowKey)) continue;
      const parent = envelopeParentSession(envelope);
      rememberedRows[rowKey] = parent !== undefined
        ? { time_updated: envelope.time_updated, parent }
        : { time_updated: envelope.time_updated };
    }

    if (Object.keys(rememberedRows).length > 0) {
      this.stateManager.rememberRows(rememberedRows);
    }
  }

  private forgetAcceptedTombstones(
    envelopes: SyncEnvelope[],
    stale: Array<{ kind: SyncKind; id: string }>,
  ): void {
    const staleKeys = new Set(stale.map((entry) => rowStateKey(entry.kind, entry.id)));
    const deletedKeys = envelopes
      .filter((envelope) => envelope.deleted)
      .map((envelope) => rowStateKey(envelope.kind, envelope.id))
      .filter((rowKey) => !staleKeys.has(rowKey));

    if (deletedKeys.length > 0) {
      this.stateManager.forgetRows(deletedKeys);
    }
  }
}
