/**
 * Write-path abstraction for applying pulled envelopes to the local opencode DB.
 *
 * For v0.1-v0.2 we write directly via SQLite during idle windows.
 */
import { Database, type SQLQueryBindings } from "bun:sqlite";
import { SYNC_KINDS, parseRowPrimaryKey, type SyncEnvelope, type SyncKind } from "@opencode-sync/shared";
import { logger } from "./logger.js";
import {
  quoteIdent,
  readTableSchema,
  requiredColumnMissing,
  schemaSignature,
  writableColumns,
  type TableSchema,
} from "./local-schema.js";

// ── Apply result ───────────────────────────────────────────────────

/**
 * Outcome of applying a single pulled envelope to the local DB.
 *
 * - `applied`  — the local row was inserted, updated, or deleted.
 * - `skipped`  — local row exists with the same `time_updated`; no work needed.
 * - `incompatible` — envelope fields cannot satisfy the local table
 *                (schema drift). Skip without retry/poison; do not remember.
 * - `conflict` — local row exists with a STRICTLY NEWER `time_updated`; the
 *                remote version was rejected to preserve the local edit. The
 *                caller should surface this to the user.
 * - `error`    — applying failed (e.g. SQL constraint violation, malformed
 *                envelope). Caller should log; not the same as a conflict.
 */
export type ApplyResult = "applied" | "skipped" | "incompatible" | "conflict" | "error";

// ── Writer ─────────────────────────────────────────────────────────

export class DbWriter {
  private db: Database;
  private readonly schemas = new Map<SyncKind, TableSchema | null>();

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
    // don't know about yet) could send something we can't process.
    if (!(SYNC_KINDS as readonly string[]).includes(kind)) {
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
    const schema = this.schemaFor(kind);
    if (!schema) {
      logger.error(`missing local table for kind: ${kind}`);
      return "error";
    }
    if (requiredColumnMissing(schema, row)) {
      logger.log("skipping envelope incompatible with local schema", {
        kind,
        id: envelope.id,
      });
      return "incompatible";
    }

    const localUpdated = this.getLocalTimeUpdated(kind, row);
    if (localUpdated !== null) {
      if (localUpdated > envelope.time_updated) return "conflict";
      if (localUpdated === envelope.time_updated) return "skipped";
    }

    return this.upsertRow(kind, row) ? "applied" : "error";
  }

  missingDependencies(envelope: SyncEnvelope): Array<{ kind: SyncKind; id: string }> {
    if (envelope.deleted || !envelope.data) return [];
    const data = envelope.data as unknown as Record<string, SQLQueryBindings>;
    const missing: Array<{ kind: SyncKind; id: string }> = [];

    const check = (kind: SyncKind, id: SQLQueryBindings | undefined): void => {
      if (typeof id !== "string" || id.length === 0) return;
      if (!this.rowExists(kind, [id])) {
        missing.push({ kind, id });
      }
    };

    switch (envelope.kind) {
      case "session":
        check("project", data["project_id"]);
        break;
      case "message":
        check("session", data["session_id"]);
        break;
      case "part":
        check("message", data["message_id"]);
        break;
      case "todo":
      case "session_share":
        check("session", data["session_id"]);
        break;
      case "permission":
        check("project", data["project_id"]);
        break;
    }

    return missing;
  }

  close(): void {
    this.db.close();
  }

  // ── Private helpers ─────────────────────────────────────────────

  schemaSignature(kind: SyncKind): string | null {
    const schema = this.schemaFor(kind);
    return schema ? schemaSignature(schema) : null;
  }

  private schemaFor(kind: SyncKind): TableSchema | null {
    if (!this.schemas.has(kind)) {
      this.schemas.set(kind, readTableSchema(this.db, kind));
    }
    return this.schemas.get(kind) ?? null;
  }

  private pkColumns(kind: SyncKind): readonly string[] | null {
    const schema = this.schemaFor(kind);
    if (!schema || schema.pkColumns.length === 0) return null;
    return schema.pkColumns;
  }

  private getLocalTimeUpdated(
    kind: SyncKind,
    data: Record<string, SQLQueryBindings>,
  ): number | null {
    const pkCols = this.pkColumns(kind);
    if (!pkCols) return null;
    const params = pkCols.map((col) => data[col]) as SQLQueryBindings[];
    if (params.some((value) => value === undefined || value === null)) return null;

    return this.getLocalTimeUpdatedForPk(kind, params);
  }

  private getLocalTimeUpdatedForPk(
    kind: SyncKind,
    pkValues: SQLQueryBindings[],
  ): number | null {
    const pkCols = this.pkColumns(kind);
    if (!pkCols) return null;
    const where = pkCols.map((col) => `${quoteIdent(col)} = ?`).join(" AND ");

    const row = this.db
      .query<{ time_updated: number }, SQLQueryBindings[]>(
        `SELECT time_updated FROM ${quoteIdent(kind)} WHERE ${where}`,
      )
      .get(...pkValues);

    return row?.time_updated ?? null;
  }

  private rowExists(kind: SyncKind, pkValues: SQLQueryBindings[]): boolean {
    const pkCols = this.pkColumns(kind);
    if (!pkCols || pkValues.length !== pkCols.length) return false;
    const where = pkCols.map((col) => `${quoteIdent(col)} = ?`).join(" AND ");
    const row = this.db
      .query<{ n: number }, SQLQueryBindings[]>(
        `SELECT 1 AS n FROM ${quoteIdent(kind)} WHERE ${where}`,
      )
      .get(...pkValues);
    return row !== null && row !== undefined;
  }

  private upsertRow(kind: SyncKind, data: Record<string, SQLQueryBindings>): boolean {
    const schema = this.schemaFor(kind);
    if (!schema) return false;
    const columns = writableColumns(schema, data);
    const pkCols = schema.pkColumns;
    if (columns.length === 0 || pkCols.length === 0) return false;
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
    const colList = columns.map(quoteIdent).join(", ");
    const pkList = pkCols.map(quoteIdent).join(", ");
    const setList = nonPkCols.map((c) => `${quoteIdent(c)} = excluded.${quoteIdent(c)}`).join(", ");
    const onConflict =
      nonPkCols.length === 0
        ? `ON CONFLICT(${pkList}) DO NOTHING`
        : `ON CONFLICT(${pkList}) DO UPDATE SET ${setList}`;
    const sql = `INSERT INTO ${quoteIdent(kind)} (${colList}) VALUES (${placeholders}) ${onConflict}`;

    const params: SQLQueryBindings[] = columns.map((col) => {
      const val = data[col];
      return val === undefined ? null : val;
    });

    try {
      this.db.run(sql, params);
      return true;
    } catch (err) {
      if (!isForeignKeyError(err)) {
        logger.error(`SQL upsert error for ${kind}:`, err);
      }
      return false;
    }
  }

  private deleteRow(kind: SyncKind, envelope: SyncEnvelope): ApplyResult {
    const pkCols = this.pkColumns(kind);
    if (!pkCols) return "error";

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

    const where = pkCols.map((col) => `${quoteIdent(col)} = ?`).join(" AND ");
    const sql = `DELETE FROM ${quoteIdent(kind)} WHERE ${where}`;

    try {
      this.db.run(sql, pkValues);
      return "applied";
    } catch (err) {
      if (!isForeignKeyError(err)) {
        logger.error(`SQL delete error for ${kind}:`, err);
      }
      return "error";
    }
  }

  /**
   * Apply a page of envelopes in one transaction with deferred FK checks
   * so child-before-parent order inside the page can still commit.
   * Falls back to per-row apply if the transaction rolls back.
   */
  applyPage(envelopes: SyncEnvelope[]): ApplyResult[] {
    if (envelopes.length === 0) return [];
    try {
      return this.db.transaction(() => {
        this.db.exec("PRAGMA defer_foreign_keys = ON");
        try {
          const results = envelopes.map((envelope) => this.applyEnvelope(envelope));
          const violations = this.db.query("PRAGMA foreign_key_check").all();
          if (violations.length > 0) {
            throw new Error("foreign key check failed");
          }
          return results;
        } finally {
          this.db.exec("PRAGMA defer_foreign_keys = OFF");
        }
      })();
    } catch {
      return envelopes.map((envelope) => this.applyEnvelope(envelope));
    }
  }
}

function isForeignKeyError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.includes("FOREIGN KEY") || msg.includes("SQLITE_CONSTRAINT_FOREIGNKEY");
}
