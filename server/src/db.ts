/**
 * SQLite ledger database — stores sync rows, file manifest, and manages blob paths.
 */

import { Database } from "bun:sqlite";
import { mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import type { SyncEnvelope, FileManifestEntry } from "@opencode-sync/shared";
import type { Logger } from "./log.js";

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

export class LedgerDB {
  private db: Database;
  private blobDir: string;
  private logger: Logger;

  // Prepared statements
  private stmtGetNextSeq;
  private stmtSetNextSeq;
  private stmtGetRow;
  private stmtInsertRow;
  private stmtUpdateRow;
  private stmtPullRows;
  private stmtPullRowsExclude;
  private stmtGetManifest;
  private stmtGetManifestEntry;
  private stmtUpsertManifest;

  // Batch transaction wrapper — see upsertBatch().
  private txUpsertBatch: (
    envelopes: SyncEnvelope[],
  ) => Array<{ accepted: boolean; stale?: { server_time_updated: number } }>;

  constructor(dataDir: string, logger: Logger) {
    this.logger = logger;

    // Ensure directories exist
    mkdirSync(dataDir, { recursive: true });
    this.blobDir = join(dataDir, "blobs");
    mkdirSync(this.blobDir, { recursive: true });

    // Open database
    const dbPath = join(dataDir, "ledger.sqlite");
    this.db = new Database(dbPath);

    // Enable WAL mode for concurrent reads
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA foreign_keys = ON");

    // Run migrations
    for (const sql of MIGRATIONS) {
      this.db.exec(sql);
    }
    logger.info("Database initialized", { path: dbPath });

    // Prepare statements
    this.stmtGetNextSeq = this.db.prepare<{ v: string }, []>(
      "SELECT v FROM server_state WHERE k = 'next_seq'",
    );

    this.stmtSetNextSeq = this.db.prepare(
      "UPDATE server_state SET v = ? WHERE k = 'next_seq'",
    );

    this.stmtGetRow = this.db.prepare<
      { kind: string; id: string; machine_id: string; time_updated: number; server_seq: number; deleted: number; data: string | null; received_at: number },
      [string, string]
    >("SELECT * FROM sync_row WHERE kind = ? AND id = ?");

    this.stmtInsertRow = this.db.prepare(
      `INSERT INTO sync_row (kind, id, machine_id, time_updated, server_seq, deleted, data, received_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    );

    this.stmtUpdateRow = this.db.prepare(
      `UPDATE sync_row SET machine_id = ?, time_updated = ?, server_seq = ?, deleted = ?, data = ?, received_at = ?
       WHERE kind = ? AND id = ?`,
    );

    this.stmtPullRows = this.db.prepare<
      { kind: string; id: string; machine_id: string; time_updated: number; server_seq: number; deleted: number; data: string | null; received_at: number },
      [number, number]
    >(
      `SELECT * FROM sync_row WHERE server_seq > ? ORDER BY server_seq ASC LIMIT ?`,
    );

    this.stmtPullRowsExclude = this.db.prepare<
      { kind: string; id: string; machine_id: string; time_updated: number; server_seq: number; deleted: number; data: string | null; received_at: number },
      [number, string, number]
    >(
      `SELECT * FROM sync_row WHERE server_seq > ? AND machine_id != ? ORDER BY server_seq ASC LIMIT ?`,
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
    return this.txUpsertBatch(envelopes);
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

    const existing = this.stmtGetRow.get(kind, id);

    if (!existing) {
      // Case 1: No existing row — insert
      const seq = this.allocSeq();
      this.stmtInsertRow.run(
        kind,
        id,
        machine_id,
        time_updated,
        seq,
        deleted ? 1 : 0,
        data != null ? JSON.stringify(data) : null,
        now,
      );
      return { accepted: true };
    }

    if (existing.time_updated < time_updated) {
      // Case 2: Incoming is newer — update
      const seq = this.allocSeq();
      this.stmtUpdateRow.run(
        machine_id,
        time_updated,
        seq,
        deleted ? 1 : 0,
        data != null ? JSON.stringify(data) : null,
        now,
        kind,
        id,
      );
      return { accepted: true };
    }

    if (existing.time_updated > time_updated) {
      // Case 3: Server has newer — reject
      return { accepted: false, stale: { server_time_updated: existing.time_updated } };
    }

    // Case 4: Equal timestamps — tie-break by machine_id
    if (machine_id >= existing.machine_id) {
      // Incoming wins or is same (idempotent)
      if (machine_id === existing.machine_id) {
        // Truly idempotent — same machine, same timestamp, no-op
        return { accepted: true };
      }
      const seq = this.allocSeq();
      this.stmtUpdateRow.run(
        machine_id,
        time_updated,
        seq,
        deleted ? 1 : 0,
        data != null ? JSON.stringify(data) : null,
        now,
        kind,
        id,
      );
      return { accepted: true };
    }

    // Existing machine_id wins tie-break
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
  ): { envelopes: SyncEnvelope[]; more: boolean; server_seq: number } {
    // Fetch limit+1 to detect whether there are more
    const fetchLimit = limit + 1;

    let rows: Array<{
      kind: string;
      id: string;
      machine_id: string;
      time_updated: number;
      server_seq: number;
      deleted: number;
      data: string | null;
      received_at: number;
    }>;

    if (exclude) {
      rows = this.stmtPullRowsExclude.all(since, exclude, fetchLimit);
    } else {
      rows = this.stmtPullRows.all(since, fetchLimit);
    }

    const more = rows.length > limit;
    if (more) {
      rows = rows.slice(0, limit);
    }

    const envelopes: SyncEnvelope[] = rows.map((row) => ({
      kind: row.kind as SyncEnvelope["kind"],
      id: row.id,
      machine_id: row.machine_id,
      time_updated: row.time_updated,
      server_seq: row.server_seq,
      deleted: row.deleted === 1,
      data: row.data != null ? JSON.parse(row.data) : null,
    }));

    // server_seq is the max seq we've seen, which is next_seq - 1
    const serverSeq = this.getNextSeq() - 1;

    return { envelopes, more, server_seq: serverSeq };
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

  /** Insert or update a file manifest entry. */
  upsertManifestEntry(entry: FileManifestEntry): void {
    this.stmtUpsertManifest.run(
      entry.relpath,
      entry.sha256,
      entry.size,
      entry.mtime,
      entry.machine_id,
      entry.deleted ? 1 : 0,
    );
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
