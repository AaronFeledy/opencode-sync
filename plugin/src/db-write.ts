/**
 * Write-path abstraction for applying pulled envelopes to the local opencode DB.
 *
 * For v0.1-v0.2 we write directly via SQLite during idle windows.
 */
import { Database, type SQLQueryBindings } from "bun:sqlite";
import type { SyncEnvelope, SyncKind } from "@opencode-sync/shared";
import { parseRowPrimaryKey } from "@opencode-sync/shared";
import { logger } from "./logger.js";

// ── Column definitions per kind ────────────────────────────────────

const TABLE_COLUMNS: Record<SyncKind, string[]> = {
  project: [
    "id", "worktree", "vcs", "name", "icon_url", "icon_color",
    "time_created", "time_updated", "time_initialized",
    "sandboxes", "commands",
  ],
  session: [
    "id", "project_id", "parent_id", "slug", "directory", "title",
    "version", "share_url", "summary_additions", "summary_deletions",
    "summary_files", "summary_diffs", "revert", "permission",
    "time_created", "time_updated", "time_compacting", "time_archived",
    "workspace_id",
  ],
  message: ["id", "session_id", "time_created", "time_updated", "data"],
  part: ["id", "message_id", "session_id", "time_created", "time_updated", "data"],
  todo: [
    "session_id", "content", "status", "priority", "position",
    "time_created", "time_updated",
  ],
  permission: ["project_id", "time_created", "time_updated", "data"],
  session_share: ["session_id", "id", "secret", "url", "time_created", "time_updated"],
};

/** Primary key column(s) for each kind — used for DELETE and conflict checks */
const PK_COLUMNS: Record<SyncKind, string[]> = {
  project: ["id"],
  session: ["id"],
  message: ["id"],
  part: ["id"],
  todo: ["session_id", "position"],
  permission: ["project_id"],
  session_share: ["session_id"],
};

// ── Apply result ───────────────────────────────────────────────────

/**
 * Outcome of applying a single pulled envelope to the local DB.
 *
 * - `applied`  — the local row was inserted, updated, or deleted.
 * - `skipped`  — local row exists with the same `time_updated`; no work needed.
 * - `conflict` — local row exists with a STRICTLY NEWER `time_updated`; the
 *                remote version was rejected to preserve the local edit. The
 *                caller should surface this to the user.
 * - `error`    — applying failed (e.g. SQL constraint violation, malformed
 *                envelope). Caller should log; not the same as a conflict.
 */
export type ApplyResult = "applied" | "skipped" | "conflict" | "error";

// ── Writer ─────────────────────────────────────────────────────────

export class DbWriter {
  private db: Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath);
    // 5s busy_timeout absorbs transient lock contention with opencode's
    // own writer. Without this the default is 0ms — any concurrent write
    // (very common, since opencode streams part rows during chat) throws
    // SQLITE_BUSY immediately and the envelope is dropped as an "error".
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA journal_mode = WAL");
  }

  /**
   * Apply a pulled envelope to the local DB. See `ApplyResult` for the
   * outcome semantics — distinguishes a true conflict (local strictly newer)
   * from the normal idempotent no-op (local equal).
   */
  applyEnvelope(envelope: SyncEnvelope): ApplyResult {
    const { kind, deleted, data } = envelope;

    // Defensive guard: kind is typed as SyncKind, but envelopes come off the
    // wire as JSON, so an old/buggy/misbehaving server (or future kind we
    // don't know about yet) could send something we can't process. Without
    // this check, the downstream PK_COLUMNS[kind]! and TABLE_COLUMNS[kind]!
    // dereferences would throw a TypeError that escapes applyEnvelope, when
    // the contract is to return "error" instead.
    if (!(kind in TABLE_COLUMNS)) {
      logger.error(`unknown envelope kind: ${kind}`);
      return "error";
    }

    // Re-validate `time_updated` — the server also validates on push
    // (routes/sync.ts), but an older/buggy/malicious server, direct
    // ledger corruption, or a third-party client bypassing the server
    // API could still emit pathological values. `0` and negatives
    // break the LWW comparison semantics: `knownTimeUpdated = 0`
    // compares equal to any freshly-zeroed local row, silently
    // treating the envelope as skipped even when content differs; a
    // negative `time_updated` on a tombstone would stamp an impossible
    // pushed-rowtime on downstream peers. See FINDINGS.md M8.
    if (
      typeof envelope.time_updated !== "number" ||
      !Number.isFinite(envelope.time_updated) ||
      envelope.time_updated <= 0
    ) {
      logger.error(
        `invalid envelope time_updated: ${envelope.time_updated} (kind=${kind}, id=${envelope.id})`,
      );
      return "error";
    }

    if (deleted) {
      return this.deleteRow(kind, envelope);
    }

    if (!data) return "error";

    const row = data as unknown as Record<string, SQLQueryBindings>;

    const localUpdated = this.getLocalTimeUpdated(kind, row);
    if (localUpdated !== null) {
      if (localUpdated > envelope.time_updated) return "conflict";
      if (localUpdated === envelope.time_updated) return "skipped";
    }

    return this.upsertRow(kind, row) ? "applied" : "error";
  }

  close(): void {
    this.db.close();
  }

  // ── Private helpers ─────────────────────────────────────────────

  private getLocalTimeUpdated(
    kind: SyncKind,
    data: Record<string, SQLQueryBindings>,
  ): number | null {
    const pkCols = PK_COLUMNS[kind]!;
    const params = pkCols.map((col) => data[col]) as SQLQueryBindings[];

    return this.getLocalTimeUpdatedForPk(kind, params);
  }

  private getLocalTimeUpdatedForPk(
    kind: SyncKind,
    pkValues: SQLQueryBindings[],
  ): number | null {
    const pkCols = PK_COLUMNS[kind]!;
    const where = pkCols.map((col) => `${col} = ?`).join(" AND ");

    const row = this.db
      .query<{ time_updated: number }, SQLQueryBindings[]>(
        `SELECT time_updated FROM ${kind} WHERE ${where}`,
      )
      .get(...pkValues);

    return row?.time_updated ?? null;
  }

  private upsertRow(kind: SyncKind, data: Record<string, SQLQueryBindings>): boolean {
    const columns = TABLE_COLUMNS[kind]!;
    const pkCols = PK_COLUMNS[kind]!;
    const pkSet = new Set(pkCols);
    const nonPkCols = columns.filter((c) => !pkSet.has(c));

    // True UPSERT — NOT `INSERT OR REPLACE`. SQLite's REPLACE conflict
    // resolution DELETEs the existing row before inserting, which with
    // `PRAGMA foreign_keys = ON` cascades to every child row (messages,
    // parts, todos, session_share for session; sessions/permissions for
    // project). A routine cross-peer session title bump or
    // time_compacting tick would silently wipe the entire conversation.
    //
    // `ON CONFLICT(<pk>) DO UPDATE SET ...` performs an in-place UPDATE
    // on conflict, leaving child rows intact. Composite PKs (todo's
    // (session_id, position)) are handled by listing all PK columns in
    // the conflict target. If a table has only PK columns and no
    // non-PK columns, there's nothing to update on conflict — degrade
    // to DO NOTHING so the INSERT becomes a no-op rather than a parse
    // error on an empty SET list. (No table in SYNC_KINDS currently
    // falls into that case, but the guard is free.)
    const placeholders = columns.map(() => "?").join(", ");
    const colList = columns.join(", ");
    const pkList = pkCols.join(", ");
    const setList = nonPkCols.map((c) => `${c} = excluded.${c}`).join(", ");
    const onConflict =
      nonPkCols.length === 0
        ? `ON CONFLICT(${pkList}) DO NOTHING`
        : `ON CONFLICT(${pkList}) DO UPDATE SET ${setList}`;
    const sql = `INSERT INTO ${kind} (${colList}) VALUES (${placeholders}) ${onConflict}`;

    const params: SQLQueryBindings[] = columns.map((col) => {
      const val = data[col];
      return val === undefined ? null : val;
    });

    try {
      this.db.run(sql, params);
      return true;
    } catch (err) {
      logger.error(`SQL upsert error for ${kind}:`, err);
      return false;
    }
  }

  private deleteRow(kind: SyncKind, envelope: SyncEnvelope): ApplyResult {
    const pkCols = PK_COLUMNS[kind]!;

    // Use the shared parser instead of `envelope.id.split(":")` — the latter
    // silently drops the deletion whenever a single-PK id happens to contain
    // a colon (e.g. a session id like "ses_part:1234"). parseRowPrimaryKey
    // knows which kinds are composite and splits accordingly.
    const parsed = parseRowPrimaryKey(kind, envelope.id);
    if (!parsed || parsed.length !== pkCols.length) return "error";

    const pkValues: SQLQueryBindings[] = parsed;

    const localUpdated = this.getLocalTimeUpdatedForPk(kind, pkValues);
    if (localUpdated !== null) {
      // Local is strictly newer than the tombstone — preserve the local edit.
      if (localUpdated > envelope.time_updated) return "conflict";
      // Local equals the tombstone time — re-applying delete is idempotent
      // but we still execute it so a partially-applied state converges.
    } else {
      // Already gone locally — the tombstone is a no-op.
      return "skipped";
    }

    const where = pkCols.map((col) => `${col} = ?`).join(" AND ");
    const sql = `DELETE FROM ${kind} WHERE ${where}`;

    try {
      this.db.run(sql, pkValues);
      return "applied";
    } catch (err) {
      logger.error(`SQL delete error for ${kind}:`, err);
      return "error";
    }
  }
}
