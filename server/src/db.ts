/**
 * SQLite ledger database — stores sync rows, file manifest, and manages blob paths.
 */

import { Database } from "bun:sqlite";
import { mkdirSync, existsSync, unlinkSync, statfsSync } from "node:fs";
import { join } from "node:path";
import type { SyncEnvelope, SyncKind, FileManifestEntry } from "@opencode-sync/shared";
import type { Logger } from "./log.js";
import { RowBlobStore, sha256Utf8 } from "./row-blobs.js";

type SyncRow = {
  kind: string;
  id: string;
  machine_id: string;
  time_updated: number;
  server_seq: number;
  deleted: number;
  data: string | null;
  received_at: number;
  data_sha: string | null;
  parent_kind: string | null;
  parent_id: string | null;
};

// ── Schema migrations ──────────────────────────────────────────────

const MIGRATIONS = [
  // Sync ledger
  `CREATE TABLE IF NOT EXISTS sync_row (
    kind          TEXT    NOT NULL,
    id            TEXT    NOT NULL,
    machine_id    TEXT    NOT NULL,
    time_updated  INTEGER NOT NULL,
    server_seq    INTEGER NOT NULL,
    deleted       INTEGER NOT NULL DEFAULT 0,
    data          TEXT,
    received_at   INTEGER NOT NULL,
    PRIMARY KEY (kind, id)
  )`,
  `CREATE INDEX IF NOT EXISTS sync_row_seq_idx ON sync_row(server_seq)`,
  `CREATE INDEX IF NOT EXISTS sync_row_time_seq_idx ON sync_row(time_updated, server_seq)`,

  // Server state
  `CREATE TABLE IF NOT EXISTS server_state (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
  )`,
  `INSERT OR IGNORE INTO server_state (k, v) VALUES ('next_seq', '1')`,

  // File manifest
  `CREATE TABLE IF NOT EXISTS file_manifest (
    relpath     TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    machine_id  TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0
  )`,
];

// ── LedgerDB class ─────────────────────────────────────────────────

function payloadParent(kind: string, data: unknown): { kind: SyncKind; id: string } | null {
  if (!data || typeof data !== "object") return null;
  const rec = data as Record<string, unknown>;
  const idOf = (k: SyncKind, field: string): { kind: SyncKind; id: string } | null => {
    const id = rec[field];
    return typeof id === "string" && id.length > 0 ? { kind: k, id } : null;
  };
  switch (kind) {
    case "session":
      return idOf("project", "project_id");
    case "message":
      return idOf("session", "session_id");
    case "part":
      return idOf("message", "message_id");
    case "todo":
    case "session_share":
      return idOf("session", "session_id");
    case "permission":
      return idOf("project", "project_id");
    default:
      return null;
  }
}

export type LegacyMigratePause = "disk" | "max-rows" | "enospc";

export type LegacyMigrateResult = {
  migrated: number;
  done: boolean;
  paused?: LegacyMigratePause;
};

export type LegacyMigrateOptions = {
  /** Stop after this many successful conversions. Default: no limit. */
  maxRows?: number;
  /** Pause when data-dir free space is below this. Default 1 GiB. 0 disables. */
  minFreeBytes?: number;
  /** Override free-space probe (tests). */
  freeBytes?: () => number;
  /** SELECT page size. Default 50. */
  batchSize?: number;
};

const LEGACY_MIGRATE_CURSOR_KEY = "legacy_migrate_rowid";
const DEFAULT_MIN_FREE_BYTES = 1024 * 1024 * 1024;

function isNoSpaceError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const e = err as { code?: unknown; message?: unknown };
  if (e.code === "ENOSPC") return true;
  return typeof e.message === "string" && /ENOSPC|no space left/i.test(e.message);
}

function freeBytesAt(dir: string): number {
  const s = statfsSync(dir);
  return Number(s.bavail) * Number(s.bsize);
}

export class LedgerDB {
  private db: Database;
  private dataDir: string;
  private blobDir: string;
  private logger: Logger;
  private rowBlobs: RowBlobStore;

  // Prepared statements
  private stmtGetNextSeq;
  private stmtSetNextSeq;
  private stmtGetRow;
  private stmtGetRowExclude;
  private stmtInsertRow;
  private stmtUpdateRow;
  private stmtPullRows;
  private stmtPullRowsExclude;
  private stmtPullRowsMinTime;
  private stmtPullRowsMinTimeExclude;
  private stmtGetManifest;
  private stmtGetManifestEntry;
  private stmtUpsertManifest;
  private stmtCountLiveRefsBySha;
  private stmtClearTombstoneSha;
  private stmtCountRowBlobRefs;
  private stmtClearLegacyData;
  private stmtGetState;
  private stmtSetState;
  private pendingRowBlobGc: string[] = [];

  // Batch transaction wrapper — see upsertBatch().
  private txUpsertBatch: (
    envelopes: SyncEnvelope[],
  ) => Array<{ accepted: boolean; stale?: { server_time_updated: number } }>;

  constructor(dataDir: string, logger: Logger) {
    this.logger = logger;

    mkdirSync(dataDir, { recursive: true });
    this.dataDir = dataDir;
    this.blobDir = join(dataDir, "blobs");
    mkdirSync(this.blobDir, { recursive: true });

    // Open database
    const dbPath = join(dataDir, "ledger.sqlite");
    this.db = new Database(dbPath);
    this.rowBlobs = new RowBlobStore(join(dataDir, "row-blobs"));

    // Enable WAL mode for concurrent reads
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA foreign_keys = ON");

    // Run migrations
    for (const sql of MIGRATIONS) {
      this.db.exec(sql);
    }
    this.ensurePayloadColumns();
    logger.info("Database initialized", { path: dbPath });

    // Prepare statements
    this.stmtGetNextSeq = this.db.prepare<{ v: string }, []>(
      "SELECT v FROM server_state WHERE k = 'next_seq'",
    );

    this.stmtSetNextSeq = this.db.prepare(
      "UPDATE server_state SET v = ? WHERE k = 'next_seq'",
    );

    this.stmtGetRow = this.db.prepare<SyncRow, [string, string]>(
      "SELECT * FROM sync_row WHERE kind = ? AND id = ?",
    );

    this.stmtGetRowExclude = this.db.prepare<SyncRow, [string, string, string]>(
      "SELECT * FROM sync_row WHERE kind = ? AND id = ? AND machine_id != ?",
    );

    this.stmtInsertRow = this.db.prepare(
      `INSERT INTO sync_row (kind, id, machine_id, time_updated, server_seq, deleted, data, received_at, data_sha, parent_kind, parent_id)
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
    );

    this.stmtUpdateRow = this.db.prepare(
      `UPDATE sync_row SET machine_id = ?, time_updated = ?, server_seq = ?, deleted = ?, data = NULL, received_at = ?, data_sha = ?, parent_kind = ?, parent_id = ?
       WHERE kind = ? AND id = ?`,
    );

    this.stmtPullRows = this.db.prepare<SyncRow, [number, number]>(
      `SELECT * FROM sync_row WHERE server_seq > ? ORDER BY server_seq ASC LIMIT ?`,
    );

    this.stmtPullRowsExclude = this.db.prepare<SyncRow, [number, string, number]>(
      `SELECT * FROM sync_row WHERE server_seq > ? AND machine_id != ? ORDER BY server_seq ASC LIMIT ?`,
    );

    this.stmtPullRowsMinTime = this.db.prepare<SyncRow, [number, number, number]>(
      `SELECT * FROM sync_row WHERE server_seq > ? AND time_updated >= ? ORDER BY server_seq ASC LIMIT ?`,
    );

    this.stmtPullRowsMinTimeExclude = this.db.prepare<SyncRow, [number, string, number, number]>(
      `SELECT * FROM sync_row WHERE server_seq > ? AND machine_id != ? AND time_updated >= ? ORDER BY server_seq ASC LIMIT ?`,
    );

    this.stmtGetManifest = this.db.prepare<
      { relpath: string; sha256: string; size: number; mtime: number; machine_id: string; deleted: number },
      []
    >("SELECT * FROM file_manifest");

    this.stmtGetManifestEntry = this.db.prepare<
      { relpath: string; sha256: string; size: number; mtime: number; machine_id: string; deleted: number },
      [string]
    >("SELECT * FROM file_manifest WHERE relpath = ?");

    this.stmtUpsertManifest = this.db.prepare(
      `INSERT INTO file_manifest (relpath, sha256, size, mtime, machine_id, deleted)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(relpath) DO UPDATE SET sha256 = excluded.sha256, size = excluded.size, mtime = excluded.mtime, machine_id = excluded.machine_id, deleted = excluded.deleted`,
    );

    // H5: ref-counted blob GC. On every manifest upsert, count how many
    // live (deleted=0) rows still reference the previous sha; if the
    // count drops to zero, the blob is orphaned and we unlink it.
    this.stmtCountLiveRefsBySha = this.db.prepare<{ n: number }, [string]>(
      "SELECT COUNT(*) AS n FROM file_manifest WHERE sha256 = ? AND deleted = 0",
    );
    this.stmtClearTombstoneSha = this.db.prepare(
      "UPDATE file_manifest SET sha256 = '', size = 0 WHERE deleted = 1 AND sha256 = ?",
    );

    this.stmtCountRowBlobRefs = this.db.prepare<{ n: number }, [string]>(
      "SELECT COUNT(*) AS n FROM sync_row WHERE data_sha = ?",
    );
    this.stmtClearLegacyData = this.db.prepare(
      `UPDATE sync_row SET data = NULL, data_sha = ?, parent_kind = ?, parent_id = ?
       WHERE kind = ? AND id = ? AND (data_sha IS NULL OR data_sha = '')`,
    );
    this.stmtGetState = this.db.prepare<{ v: string }, [string]>(
      "SELECT v FROM server_state WHERE k = ?",
    );
    this.stmtSetState = this.db.prepare(
      `INSERT INTO server_state (k, v) VALUES (?, ?)
       ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
    );

    // Wrap the batch upsert in a SQLite transaction. Provides:
    // (1) all-or-nothing atomicity — a failure mid-batch rolls back any
    //     partially-applied rows, so callers never observe a half-applied push;
    // (2) safe sequence allocation — the read-then-write inside allocSeq() is
    //     serialised at the SQLite layer, so even a future caller running
    //     pushes from another process or worker can't observe a duplicate
    //     server_seq;
    // (3) significant write throughput — one journal flush per batch instead
    //     of one per row.
    this.txUpsertBatch = this.db.transaction((envelopes: SyncEnvelope[]) =>
      envelopes.map((envelope) => this.upsertRow(envelope)),
    );
  }

  /**
   * Look up current head state for a batch of (kind, id) pairs.
   *
   * Returns one entry per row the server has on file; rows the server
   * has never seen are omitted (not returned with a sentinel — see
   * HeadsResponse in shared/protocol.ts).
   *
   * Used by the plugin's deletion-safety guard: before tombstoning a
   * row that's gone missing locally, it cross-checks here to confirm
   * the server hasn't received a newer version from another peer
   * (in which case pulling that version is preferable to overwriting
   * it with a tombstone).
   *
   * Built dynamically rather than as a prepared statement because the
   * IN-list size varies per request. SQLite's parameter limit is
   * 32766 by default, so the route caller caps inputs well below that.
   */
  getHeads(
    rowKeys: Array<{ kind: SyncKind; id: string }>,
  ): Array<{ kind: SyncKind; id: string; time_updated: number; deleted: boolean }> {
    if (rowKeys.length === 0) return [];

    // Group by kind so we can use a single IN-clause per kind. Avoids
    // an OR-of-AND-pairs query that SQLite can't index well.
    const byKind = new Map<SyncKind, string[]>();
    for (const { kind, id } of rowKeys) {
      let ids = byKind.get(kind);
      if (!ids) {
        ids = [];
        byKind.set(kind, ids);
      }
      ids.push(id);
    }

    const results: Array<{ kind: SyncKind; id: string; time_updated: number; deleted: boolean }> = [];

    for (const [kind, ids] of byKind) {
      const placeholders = ids.map(() => "?").join(",");
      const rows = this.db
        .query<{ id: string; time_updated: number; deleted: number }, string[]>(
          `SELECT id, time_updated, deleted FROM sync_row WHERE kind = ? AND id IN (${placeholders})`,
        )
        .all(kind, ...ids);
      for (const row of rows) {
        results.push({
          kind,
          id: row.id,
          time_updated: row.time_updated,
          deleted: row.deleted === 1,
        });
      }
    }

    return results;
  }

  /** Read the current next_seq value. */
  getNextSeq(): number {
    const row = this.stmtGetNextSeq.get();
    return row ? parseInt(row.v, 10) : 1;
  }

  /**
   * Allocate and return a sequence number, then increment.
   *
   * NOTE: This method is *not* atomic on its own — it does a read followed
   * by a write. It MUST run inside a SQLite transaction (e.g. via
   * `upsertBatch`) to guarantee strictly-monotonic, unique sequence numbers
   * under concurrent writers. Marked `private` so external callers can't
   * accidentally call it outside that transaction context.
   */
  private allocSeq(): number {
    const seq = this.getNextSeq();
    this.stmtSetNextSeq.run(String(seq + 1));
    return seq;
  }

  /**
   * Apply a batch of envelopes atomically. Returns one result per envelope
   * in the same order as input. If any single upsert throws, the entire
   * batch rolls back and the exception propagates to the caller.
   *
   * Prefer this over calling `upsertRow` in a loop: it is the only way to
   * guarantee that `server_seq` allocations remain strictly monotonic when
   * pushes overlap.
   */
  upsertBatch(
    envelopes: SyncEnvelope[],
  ): Array<{ accepted: boolean; stale?: { server_time_updated: number } }> {
    this.pendingRowBlobGc = [];
    try {
      const results = this.txUpsertBatch(envelopes);
      const toGc = this.pendingRowBlobGc;
      this.pendingRowBlobGc = [];
      for (const sha of toGc) {
        try {
          this.gcRowBlob(sha);
        } catch (err) {
          this.logger.warn("row blob gc failed", {
            sha256: sha,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      }
      return results;
    } catch (err) {
      this.pendingRowBlobGc = [];
      throw err;
    }
  }

  /**
   * Upsert a sync row using LWW (last-writer-wins) with machine_id tie-breaking.
   *
   * §6.2 Push logic:
   * 1. No existing row → insert with fresh server_seq → accepted
   * 2. existing.time_updated < incoming.time_updated → update → accepted
   * 3. existing.time_updated > incoming.time_updated → reject → stale
   * 4. Equal time_updated → tie-break by machine_id (lexicographic); higher wins; equal = idempotent
   *
   * Marked `private` because callers MUST run this inside a SQLite
   * transaction to keep `allocSeq` strictly monotonic. The only public
   * entry point is `upsertBatch`, which provides that transaction context.
   */
  private upsertRow(
    envelope: SyncEnvelope,
  ): { accepted: boolean; stale?: { server_time_updated: number } } {
    const { kind, id, machine_id, time_updated, deleted, data } = envelope;
    const now = Date.now();
    const incomingDeleted = deleted ? 1 : 0;
    const incomingJson = data != null ? JSON.stringify(data) : null;
    const incomingSha = incomingJson != null ? this.rowBlobs.putJson(incomingJson) : null;
    const parent = payloadParent(kind, data);

    const existing = this.stmtGetRow.get(kind, id);

    if (!existing) {
      const seq = this.allocSeq();
      this.stmtInsertRow.run(
        kind,
        id,
        machine_id,
        time_updated,
        seq,
        incomingDeleted,
        now,
        incomingSha,
        parent?.kind ?? null,
        parent?.id ?? null,
      );
      return { accepted: true };
    }

    if (existing.time_updated < time_updated) {
      const seq = this.allocSeq();
      const oldSha = existing.data_sha;
      this.stmtUpdateRow.run(
        machine_id,
        time_updated,
        seq,
        incomingDeleted,
        now,
        incomingSha,
        parent?.kind ?? null,
        parent?.id ?? null,
        kind,
        id,
      );
      this.queueRowBlobGc(oldSha);
      return { accepted: true };
    }

    if (existing.time_updated > time_updated) {
      this.queueRowBlobGc(incomingSha);
      return { accepted: false, stale: { server_time_updated: existing.time_updated } };
    }

    // Case 4: Equal timestamps. Exact content echoes are idempotent even when
    // they arrive from a different machine. Without this guard, a peer that
    // pulls rows and then accidentally push-scans them back can churn
    // `machine_id` and allocate fresh `server_seq`s for unchanged data.
    //
    // Note: this is a byte-exact JSON string compare (not deep equality). It
    // relies on every peer producing identical key ordering for the same
    // logical row — which holds because (a) all peers run the same opencode
    // binary against the same SQLite schema, (b) `SELECT *` returns columns in
    // DDL order, (c) `JSON.stringify` preserves insertion order, and (d) the
    // server's JSON.parse → JSON.stringify roundtrip preserves order too. If
    // any of those ever changes (schema migration with reordered columns,
    // building envelopes from a typed object instead of a SQL row, switching
    // SQLite drivers), this fast path silently turns into the machine_id
    // tie-break below — that's still correct (no data loss), just one extra
    // server_seq allocation per echoed row. Deep / sorted-key equality would
    // remove the assumption but doubles the per-push CPU cost on a hot path
    // whose entire purpose is to *save* server work, so we accept the
    // assumption and rely on the plugin-side `lastPushedRowIds` dedup as the
    // primary defense (this server check is a fallback for state-reset peers).
    const existingSha = existing.data_sha ?? (existing.data != null ? sha256Utf8(existing.data) : null);
    if (existing.deleted === incomingDeleted && existingSha === incomingSha) {
      this.queueRowBlobGc(incomingSha === existing.data_sha ? null : incomingSha);
      return { accepted: true };
    }

    if (machine_id >= existing.machine_id) {
      if (machine_id === existing.machine_id) {
        this.queueRowBlobGc(incomingSha);
        return { accepted: true };
      }
      const seq = this.allocSeq();
      const oldSha = existing.data_sha;
      this.stmtUpdateRow.run(
        machine_id,
        time_updated,
        seq,
        incomingDeleted,
        now,
        incomingSha,
        parent?.kind ?? null,
        parent?.id ?? null,
        kind,
        id,
      );
      this.queueRowBlobGc(oldSha);
      return { accepted: true };
    }

    this.queueRowBlobGc(incomingSha);
    return { accepted: false, stale: { server_time_updated: existing.time_updated } };
  }

  /**
   * Pull rows with server_seq > since.
   * Optionally exclude rows from a specific machine_id.
   * Returns up to `limit` rows (default 500) plus a `more` flag.
   */
  pullRows(
    since: number,
    exclude?: string,
    limit: number = 500,
    minTimeUpdated?: number,
  ): { envelopes: SyncEnvelope[]; more: boolean; server_seq: number; cursor_seq: number; dependency_closure: boolean } {
    // Fetch limit+1 to detect whether there are more
    const fetchLimit = limit + 1;

    let rows: SyncRow[];

    if (minTimeUpdated !== undefined) {
      rows = exclude
        ? this.stmtPullRowsMinTimeExclude.all(since, exclude, minTimeUpdated, fetchLimit)
        : this.stmtPullRowsMinTime.all(since, minTimeUpdated, fetchLimit);
    } else if (exclude) {
      rows = this.stmtPullRowsExclude.all(since, exclude, fetchLimit);
    } else {
      rows = this.stmtPullRows.all(since, fetchLimit);
    }

    const more = rows.length > limit;
    if (more) {
      rows = rows.slice(0, limit);
    }
    const cursorSeq = rows.length > 0 ? rows[rows.length - 1]!.server_seq : since;
    rows = this.withDependencyClosure(rows, exclude);

    const envelopes: SyncEnvelope[] = rows.map((row) => this.rowToEnvelope(row));

    // server_seq is the max seq we've seen, which is next_seq - 1
    const serverSeq = this.getNextSeq() - 1;

    return {
      envelopes,
      more,
      server_seq: serverSeq,
      cursor_seq: cursorSeq,
      dependency_closure: true,
    };
  }

  private withDependencyClosure(rows: SyncRow[], exclude?: string): SyncRow[] {
    const byKey = new Map(rows.map((row) => [`${row.kind}:${row.id}`, row]));
    const queue = [...rows];

    for (let index = 0; index < queue.length; index++) {
      const row = queue[index]!;
      if (row.deleted === 1) continue;

      for (const dep of this.rowDependencies(row)) {
        const key = `${dep.kind}:${dep.id}`;
        if (byKey.has(key)) continue;

        const depRow = exclude
          ? this.stmtGetRowExclude.get(dep.kind, dep.id, exclude)
          : this.stmtGetRow.get(dep.kind, dep.id);
        if (!depRow || depRow.deleted === 1) continue;

        byKey.set(key, depRow);
        queue.push(depRow);
      }
    }

    return [...byKey.values()].sort((a, b) => a.server_seq - b.server_seq);
  }

  private rowDependencies(row: SyncRow): Array<{ kind: SyncKind; id: string }> {
    if (row.parent_kind && row.parent_id) {
      return [{ kind: row.parent_kind as SyncKind, id: row.parent_id }];
    }
    const parent = payloadParent(row.kind, this.rowPayload(row));
    return parent ? [parent] : [];
  }

  private rowPayload(row: SyncRow): unknown {
    if (row.deleted === 1) return null;
    if (row.data_sha) {
      const json = this.rowBlobs.getJson(row.data_sha);
      if (json == null) {
        this.logger.warn("row blob missing", { kind: row.kind, id: row.id, sha256: row.data_sha });
        return row.data != null ? JSON.parse(row.data) : null;
      }
      return JSON.parse(json);
    }
    if (row.data != null) return JSON.parse(row.data);
    return null;
  }

  private rowToEnvelope(row: SyncRow): SyncEnvelope {
    return {
      kind: row.kind as SyncEnvelope["kind"],
      id: row.id,
      machine_id: row.machine_id,
      time_updated: row.time_updated,
      server_seq: row.server_seq,
      deleted: row.deleted === 1,
      data: this.rowPayload(row) as SyncEnvelope["data"],
    };
  }

  private queueRowBlobGc(sha256: string | null): void {
    if (sha256) this.pendingRowBlobGc.push(sha256);
  }

  private gcRowBlob(sha256: string | null): void {
    if (!sha256) return;
    const refs = this.stmtCountRowBlobRefs.get(sha256);
    if (!refs || refs.n > 0) return;
    this.rowBlobs.unlink(sha256);
  }

  private ensurePayloadColumns(): void {
    const cols = new Set(
      this.db.prepare<{ name: string }, []>("PRAGMA table_info(sync_row)").all().map((c) => c.name),
    );
    if (!cols.has("data_sha")) this.db.exec("ALTER TABLE sync_row ADD COLUMN data_sha TEXT");
    if (!cols.has("parent_kind")) this.db.exec("ALTER TABLE sync_row ADD COLUMN parent_kind TEXT");
    if (!cols.has("parent_id")) this.db.exec("ALTER TABLE sync_row ADD COLUMN parent_id TEXT");
  }

  async migrateLegacyPayloads(opts: LegacyMigrateOptions = {}): Promise<LegacyMigrateResult> {
    type LegacyRow = SyncRow & { rowid: number };
    const batchSize = opts.batchSize && opts.batchSize > 0 ? opts.batchSize : 50;
    const maxRows = opts.maxRows && opts.maxRows > 0 ? opts.maxRows : Number.POSITIVE_INFINITY;
    const minFreeBytes = opts.minFreeBytes ?? DEFAULT_MIN_FREE_BYTES;
    const freeBytes = opts.freeBytes ?? (() => freeBytesAt(this.dataDir));
    const stmt = this.db.prepare<LegacyRow, [number, number]>(
      `SELECT rowid, * FROM sync_row
       WHERE rowid > ? AND data IS NOT NULL AND (data_sha IS NULL OR data_sha = '')
       ORDER BY rowid
       LIMIT ?`,
    );
    let migrated = 0;
    let afterRowid = this.readMigrateCursor();
    const persistCursor = (): void => {
      this.stmtSetState.run(LEGACY_MIGRATE_CURSOR_KEY, String(afterRowid));
    };

    for (;;) {
      if (minFreeBytes > 0 && freeBytes() < minFreeBytes) {
        persistCursor();
        this.logger.warn("legacy payload migration paused: low disk", {
          migrated,
          minFreeBytes,
          afterRowid,
        });
        return { migrated, done: false, paused: "disk" };
      }
      const rows = stmt.all(afterRowid, batchSize);
      if (rows.length === 0) {
        this.stmtSetState.run(LEGACY_MIGRATE_CURSOR_KEY, "0");
        if (migrated > 0) {
          this.logger.info("migrated legacy row payloads to compressed blobs", { migrated });
        }
        return { migrated, done: true };
      }
      for (const row of rows) {
        if (row.data == null) continue;
        if (minFreeBytes > 0 && freeBytes() < minFreeBytes) {
          persistCursor();
          this.logger.warn("legacy payload migration paused: low disk", {
            migrated,
            minFreeBytes,
            afterRowid,
          });
          return { migrated, done: false, paused: "disk" };
        }
        try {
          const sha = this.rowBlobs.putJson(row.data);
          let parent: { kind: SyncKind; id: string } | null = null;
          try {
            parent = payloadParent(row.kind, JSON.parse(row.data));
          } catch {
            parent = null;
          }
          this.stmtClearLegacyData.run(
            sha,
            parent?.kind ?? null,
            parent?.id ?? null,
            row.kind,
            row.id,
          );
        } catch (err) {
          persistCursor();
          if (isNoSpaceError(err)) {
            this.logger.warn("legacy payload migration paused: ENOSPC", {
              migrated,
              afterRowid,
              error: err instanceof Error ? err.message : String(err),
            });
            return { migrated, done: false, paused: "enospc" };
          }
          throw err;
        }
        afterRowid = row.rowid;
        migrated++;
        if (migrated % 5000 === 0) {
          persistCursor();
          this.logger.info("migrating legacy row payloads", { migrated, afterRowid });
        }
        if (migrated >= maxRows) {
          persistCursor();
          this.logger.info("legacy payload migration chunk complete", { migrated, afterRowid });
          return { migrated, done: false, paused: "max-rows" };
        }
        await Bun.sleep(0);
      }
      persistCursor();
    }
  }

  private readMigrateCursor(): number {
    const row = this.stmtGetState.get(LEGACY_MIGRATE_CURSOR_KEY);
    if (!row) return 0;
    const n = Number.parseInt(row.v, 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  /**
   * Get the full file manifest, including tombstones (deleted=true entries).
   * Tombstones are returned so clients can replay deletions; callers that
   * only want live files should filter `entry.deleted === false`.
   */
  getManifest(): FileManifestEntry[] {
    const rows = this.stmtGetManifest.all();
    return rows.map((row) => ({
      relpath: row.relpath,
      sha256: row.sha256,
      size: row.size,
      mtime: row.mtime,
      machine_id: row.machine_id,
      deleted: row.deleted === 1,
    }));
  }

  /**
   * Look up a single manifest entry by relpath, or null if absent.
   * Used by the file PUT/DELETE handlers to compare incoming mtime against
   * the server's current mtime for LWW conflict rejection.
   */
  getManifestEntry(relpath: string): FileManifestEntry | null {
    const row = this.stmtGetManifestEntry.get(relpath);
    if (!row) return null;
    return {
      relpath: row.relpath,
      sha256: row.sha256,
      size: row.size,
      mtime: row.mtime,
      machine_id: row.machine_id,
      deleted: row.deleted === 1,
    };
  }

  /**
   * Insert or update a file manifest entry, garbage-collecting any blob
   * that no longer has any live (deleted=0) manifest row pointing at it.
   *
   * GC strategy (H5): after the upsert, if the previous row pointed at
   * a DIFFERENT sha than the new row's sha, check whether any other
   * manifest row still references the old sha as a live entry. If not,
   * unlink the blob file AND clear the sha on any tombstone rows that
   * still mention it (so clients don't see a dangling sha they can't
   * fetch). Unlinking happens AFTER the transaction so a crash mid-way
   * leaves an orphan blob rather than a dangling reference.
   *
   * See FINDINGS.md H5. Without this, blobs accumulate forever — a
   * serious concern for `auth_json` sync, where every rotated token's
   * blob stays fetchable by anyone with OPENCODE_SYNC_TOKEN.
   */
  upsertManifestEntry(entry: FileManifestEntry): void {
    // Capture the previous sha BEFORE the upsert so we can check
    // whether it becomes orphaned.
    const prev = this.stmtGetManifestEntry.get(entry.relpath);
    const prevSha = prev?.sha256 ?? "";

    this.stmtUpsertManifest.run(
      entry.relpath,
      entry.sha256,
      entry.size,
      entry.mtime,
      entry.machine_id,
      entry.deleted ? 1 : 0,
    );

    if (!prevSha) return;
    if (prevSha === entry.sha256 && !entry.deleted) return;

    // Is the previous sha still referenced by any live row?
    const refs = this.stmtCountLiveRefsBySha.get(prevSha);
    if (!refs || refs.n > 0) return;

    // Orphaned: clear tombstone references then unlink the blob file.
    this.stmtClearTombstoneSha.run(prevSha);

    const blobPath = this.getBlobPath(prevSha);
    try {
      if (existsSync(blobPath)) {
        unlinkSync(blobPath);
        this.logger.info("blob gc'd", { sha256: prevSha });
      }
    } catch (err) {
      // Blob unlink is best-effort: a failure (e.g. EBUSY on Windows,
      // EACCES if somehow the mode is bad) leaves an orphan blob, which
      // is strictly safer than the inverse — a future manual /admin/gc
      // pass or the next overwrite of the same sha will reclaim it.
      this.logger.warn("blob gc failed", {
        sha256: prevSha,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /** Get the full filesystem path for a blob by its sha256. */
  getBlobPath(sha256: string): string {
    // Use first 2 chars as subdirectory for fan-out
    const prefix = sha256.slice(0, 2);
    return join(this.blobDir, prefix, sha256);
  }

  /** Check if a blob file exists on disk. */
  hasBlobFile(sha256: string): boolean {
    return existsSync(this.getBlobPath(sha256));
  }

  /** Close the database connection. */
  close(): void {
    this.db.close();
    this.logger.info("Database closed");
  }
}
