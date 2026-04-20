/**
 * Session sync orchestration — push/pull/reconcile session data
 * between the local opencode DB and the remote sync server.
 */
import type { SyncEnvelope, SyncKind } from "@opencode-sync/shared";
import type { DbReader } from "./db-read.js";
import type { ApplyResult, DbWriter } from "./db-write.js";
import type { SyncClient } from "./client.js";
import type { StateManager } from "./state.js";

// ── Constants ──────────────────────────────────────────────────────

const PUSH_BATCH_SIZE = 100;

function rowStateKey(kind: SyncKind, id: string): string {
  return `${kind}:${id}`;
}

function parseRowStateKey(rowKey: string): { kind: SyncKind; id: string } | null {
  const separator = rowKey.indexOf(":");
  if (separator === -1) return null;

  const kind = rowKey.slice(0, separator) as SyncKind;
  const id = rowKey.slice(separator + 1);

  if (!id) return null;
  return { kind, id };
}

// ── Sync orchestrator ──────────────────────────────────────────────

export class SessionSync {
  private dbReader: DbReader;
  private dbWriter: DbWriter;
  private client: SyncClient;
  private stateManager: StateManager;
  private machineId: string;
  private log: (msg: string, data?: Record<string, unknown>) => void;

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
   * Push local changes for a specific session and all its related data.
   */
  async pushSession(sessionId: string): Promise<void> {
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
   */
  async pushAll(): Promise<void> {
    // Wrap the entire push so per-batch state mutations (`markPushed`,
    // `rememberRows`, `forgetRows`, `advancePushedRowTime`) only trigger a
    // single state.json write at the end instead of one per batch.
    await this.stateManager.withBatch(async () => {
      const since = this.stateManager.pushReadSince();

      let totalSeen = 0;
      let totalAccepted = 0;
      let totalStale = 0;
      let maxPushedTime = 0;
      let pendingBatch: SyncEnvelope[] = [];

      const flushBatch = async (): Promise<void> => {
        if (pendingBatch.length === 0) return;
        const batch = pendingBatch;
        pendingBatch = [];

        // Filter already-pushed inside the flush so the streaming loop
        // doesn't have to know about the dedup set. Most batches will
        // pass through unchanged on a steady-state sync; on a re-sync
        // after a crash the dedup avoids re-pushing rows we already
        // confirmed.
        const toPush = batch.filter(
          (e) => !this.stateManager.state.lastPushedRowIds.has(
            `${e.kind}:${e.id}:${e.time_updated}`,
          ),
        );

        if (toPush.length === 0) return;

        const res = await this.client.push(this.machineId, toPush);

        this.stateManager.markPushed(
          toPush.map((e) => `${e.kind}:${e.id}:${e.time_updated}`),
        );

        totalAccepted += res.accepted.length;
        totalStale += res.stale.length;

        for (const env of toPush) {
          if (env.time_updated > maxPushedTime) maxPushedTime = env.time_updated;
        }

        this.rememberAcceptedRows(toPush, res.stale);
        this.forgetAcceptedTombstones(toPush, res.stale);
      };

      // Stream rows from every kind. Generator yields one envelope at a
      // time, so peak memory is roughly PUSH_BATCH_SIZE envelopes plus
      // whatever bun:sqlite buffers internally for the active statement.
      for (const env of this.dbReader.iterateAllEnvelopes(since, this.machineId)) {
        pendingBatch.push(env);
        totalSeen++;
        if (pendingBatch.length >= PUSH_BATCH_SIZE) {
          await flushBatch();
        }
      }

      // Tombstones come from comparing knownRows to live PKs — bounded by
      // the size of knownRows (PKs only, no row data), so loading them
      // all is fine.
      for (const env of this.buildDeletionEnvelopes()) {
        pendingBatch.push(env);
        totalSeen++;
        if (pendingBatch.length >= PUSH_BATCH_SIZE) {
          await flushBatch();
        }
      }

      // Final partial batch.
      await flushBatch();

      if (totalSeen === 0) {
        this.log("pushAll: nothing new to push");
        return;
      }

      // Advance the push cursor so the next pushAll's delta-read can skip
      // everything older than this batch (with a safety margin for clock
      // skew). See StateManager.pushReadSince for details.
      if (maxPushedTime > 0) {
        this.stateManager.advancePushedRowTime(maxPushedTime);
      }

      this.log("pushAll complete", {
        total: totalSeen,
        accepted: totalAccepted,
        stale: totalStale,
      });
    });
  }

  /**
   * Pull remote changes and apply them to the local DB.
   */
  async pull(): Promise<{ applied: number; conflicts: number; errors: number }> {
    // Wrap the whole paginated pull so per-page state mutations
    // (`rememberRows`, `forgetRows`, `updateSeq`) collapse into a single
    // state.json write at the end. The dozens of redundant writes per pull
    // were measurable on large backlogs.
    return this.stateManager.withBatch(async () => {
      return this.pullInternal();
    });
  }

  private async pullInternal(): Promise<{ applied: number; conflicts: number; errors: number }> {
    let applied = 0;
    let conflicts = 0;
    let errors = 0;
    let hasMore = true;

    while (hasMore) {
      const res = await this.client.pull(
        this.stateManager.state.lastPulledSeq,
        this.machineId,
      );

      const rememberedRows: Record<string, number> = {};
      const forgottenRows = new Set<string>();

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
      // Trade-off: a permanently-bad envelope (e.g. unknown kind on an
      // older client, malformed data) blocks all sync progress until
      // resolved. A persisted attempt-counter would handle that
      // gracefully — left as a follow-up.
      let lastGoodSeq = this.stateManager.state.lastPulledSeq;
      let firstErrorSeq: number | null = null;

      for (const envelope of res.envelopes) {
        let result: ApplyResult;
        try {
          result = this.dbWriter.applyEnvelope(envelope);
        } catch (err) {
          // Defensive: applyEnvelope is supposed to return "error" rather
          // than throw, but anything that does escape lands here.
          result = "error";
          this.log("envelope apply threw (will retry next cycle)", {
            kind: envelope.kind,
            id: envelope.id,
            error: String(err),
          });
        }

        if (result === "applied") {
          applied++;
          const rowKey = rowStateKey(envelope.kind, envelope.id);
          if (envelope.deleted) {
            forgottenRows.add(rowKey);
          } else {
            rememberedRows[rowKey] = envelope.time_updated;
          }
        } else if (result === "conflict") {
          // Local row is strictly newer than the remote — preserve local
          // and report as a real conflict (per SPEC §6.3 step 4).
          conflicts++;
        } else if (result === "error") {
          errors++;
          if (firstErrorSeq === null) firstErrorSeq = envelope.server_seq;
          this.log("envelope apply failed (will retry next cycle)", {
            kind: envelope.kind,
            id: envelope.id,
            time_updated: envelope.time_updated,
            server_seq: envelope.server_seq,
          });
          // Don't advance lastGoodSeq past the error.
          continue;
        }
        // "skipped" is the normal idempotent no-op when local == remote.

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

      hasMore = res.more;
    }

    if (applied > 0 || conflicts > 0 || errors > 0) {
      this.log("pull complete", { applied, conflicts, errors });
    }

    return { applied, conflicts, errors };
  }

  /**
   * Full sync cycle: pull first (to get latest remote state), then push local changes.
   */
  async sync(): Promise<void> {
    await this.pull();
    await this.pushAll();
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

  private buildDeletionEnvelopes(): SyncEnvelope[] {
    // Use a PK-only scan for deletion detection rather than the (possibly
    // delta-filtered) `currentEnvelopes` set. Otherwise an old, unchanged
    // row would be absent from the delta and we'd falsely tombstone it.
    const liveKeys = this.dbReader.readAllRowKeys();

    return Object.entries(this.stateManager.state.knownRows).flatMap(([knownRowKey, timeUpdated]) => {
      if (liveKeys.has(knownRowKey)) return [];

      const parsed = parseRowStateKey(knownRowKey);
      if (!parsed) return [];

      return [{
        kind: parsed.kind,
        id: parsed.id,
        machine_id: this.machineId,
        time_updated: Math.max(Date.now(), timeUpdated + 1),
        server_seq: 0,
        deleted: true,
        data: null,
      } satisfies SyncEnvelope];
    });
  }

  private rememberAcceptedRows(
    envelopes: SyncEnvelope[],
    stale: Array<{ kind: SyncKind; id: string }>,
  ): void {
    const staleKeys = new Set(stale.map((entry) => rowStateKey(entry.kind, entry.id)));
    const rememberedRows = Object.fromEntries(
      envelopes
        .filter((envelope) => !envelope.deleted)
        .filter((envelope) => !staleKeys.has(rowStateKey(envelope.kind, envelope.id)))
        .map((envelope) => [rowStateKey(envelope.kind, envelope.id), envelope.time_updated]),
    );

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
