/**
 * Read-only access to opencode's local SQLite database.
 *
 * The plugin NEVER writes to opencode.db — this module only reads.
 * Opens the database in read-only mode with WAL journal.
 */
import { Database } from "bun:sqlite";
import type {
  SyncKind,
  SyncEnvelope,
  Project,
  Session,
  Message,
  Part,
  Todo,
  Permission,
  SessionShare,
} from "@opencode-sync/shared";
import { rowPrimaryKey } from "@opencode-sync/shared";
import { quoteIdent, readTableSchema } from "./local-schema.js";

// ── Reader ─────────────────────────────────────────────────────────

export class DbReader {
  private db: Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath, { readonly: true });
    // WAL mode is set by opencode itself; read-only connections can't change it.
    // Just verify it's accessible.
    //
    // busy_timeout is per-handle, not per-database, so set it here too:
    // even with WAL letting readers and writers overlap, a reader can
    // still hit SQLITE_BUSY when opencode's writer holds the WAL lock for
    // checkpointing. 5s is generous enough that opencode finishes any
    // realistic write before we give up.
    this.db.exec("PRAGMA busy_timeout = 5000");
  }

  // ── Table readers ───────────────────────────────────────────────

  readProjects(since?: number): Project[] {
    return this.readTable<Project>("project", since);
  }

  readSessions(since?: number): Session[] {
    return this.readTable<Session>("session", since);
  }

  readMessages(since?: number): Message[] {
    return this.readTable<Message>("message", since);
  }

  readParts(since?: number): Part[] {
    return this.readTable<Part>("part", since);
  }

  readTodos(since?: number): Todo[] {
    return this.readTable<Todo>("todo", since);
  }

  readPermissions(since?: number): Permission[] {
    return this.readTable<Permission>("permission", since);
  }

  readSessionShares(since?: number): SessionShare[] {
    return this.readTable<SessionShare>("session_share", since);
  }

  // ── Deletion detection ──────────────────────────────────────────

  /**
   * True when the local `permission` table carries an `id` column —
   * i.e. opencode has migrated permissions from the legacy
   * `project_id`-keyed blob shape to per-rule rows keyed by `id`.
   * `rowPrimaryKey` mirrors this: it prefers `id` when present.
   */
  permissionUsesIdKey(): boolean {
    return readTableSchema(this.db, "permission")?.columnNames.has("id") === true;
  }

  /**
   * Return the set of all live row keys across every synced table, in the
   * `${kind}:${primaryKey}` format used by `state.knownRows`.
   *
   * This is intentionally a PK-only scan (no row data, no JSON deserialise)
   * so it stays cheap even when `pushAll` uses a `since` cursor on the data
   * read. Without this, an unchanged old row would be absent from the delta
   * read and `buildDeletionEnvelopes` would falsely tombstone it.
   */
  readAllRowKeys(): Set<string> {
    const keys = new Set<string>();

    const permissionSchema = readTableSchema(this.db, "permission");
    const permissionIdSql = permissionSchema?.columnNames.has("id")
      ? `SELECT ${quoteIdent("id")} AS id FROM ${quoteIdent("permission")}`
      : `SELECT ${quoteIdent("project_id")} AS id FROM ${quoteIdent("permission")}`;

    const singlePkQueries: Array<[SyncKind, string]> = [
      ["project", "SELECT id FROM project"],
      ["session", "SELECT id FROM session"],
      ["message", "SELECT id FROM message"],
      ["part", "SELECT id FROM part"],
      ["permission", permissionIdSql],
      ["session_share", "SELECT session_id AS id FROM session_share"],
    ];

    for (const [kind, sql] of singlePkQueries) {
      const rows = this.db.query<{ id: string }, []>(sql).all();
      for (const row of rows) {
        keys.add(`${kind}:${row.id}`);
      }
    }

    const todos = this.db
      .query<{ session_id: string; position: number }, []>(
        "SELECT session_id, position FROM todo",
      )
      .all();
    for (const todo of todos) {
      keys.add(`todo:${todo.session_id}:${todo.position}`);
    }

    return keys;
  }

  // ── Streaming readers ───────────────────────────────────────────

  /**
   * Lazily yield envelopes for every synced kind whose `time_updated >
   * since`. Streams from SQLite via `Statement.iterate()` so memory
   * consumption stays bounded by a single row regardless of DB size.
   *
   * Used by `pushAll` on a fresh-state cursor=0 sync where loading every
   * `part`/`message` row at once would easily reach multiple GB
   * (each row carries a full JSON conversation blob).
   *
   * Order is project → session → message → part → todo → permission →
   * session_share. Within each kind rows arrive in SQLite scan order
   * (no explicit ORDER BY) — callers must not depend on cross-kind
   * ordering.
   *
   * `sessionScoped` restricts message/part/todo rows to sessions whose
   * own `time_updated > since`. opencode.db has no index on
   * `time_updated`, so the plain `WHERE time_updated > ?` is a full
   * table scan — on a multi-GB DB with millions of `part` rows that is
   * minutes of launch-blocking I/O. The session table is small and the
   * child tables all carry a `session_id` index, so the scoped variant
   * touches only the pages belonging to recently-active sessions.
   * opencode bumps `session.time_updated` on every write to a session,
   * so in practice nothing is missed; any stragglers are still covered
   * by the unscoped background sync, which keeps its own push cursor.
   */
  *iterateAllEnvelopes(
    since: number,
    machineId: string,
    options: { sessionScoped?: boolean } = {},
  ): Generator<SyncEnvelope> {
    const kinds: SyncKind[] = [
      "project",
      "session",
      "message",
      "part",
      "todo",
      "permission",
      "session_share",
    ];

    const scopedKinds = new Set<SyncKind>(["message", "part", "todo"]);

    for (const kind of kinds) {
      // session_share's table column is shared with its kind, so the
      // template-literal SQL is safe — kinds are a closed enum.
      const sql = options.sessionScoped && scopedKinds.has(kind)
        ? `SELECT * FROM ${kind} WHERE session_id IN ` +
          `(SELECT id FROM session WHERE time_updated > ?1) AND time_updated > ?1`
        : `SELECT * FROM ${kind} WHERE time_updated > ?1`;
      const stmt = this.db.query<Record<string, unknown>, [number]>(sql);
      for (const row of stmt.iterate(since)) {
        yield {
          id: rowPrimaryKey(kind, row),
          kind,
          machine_id: machineId,
          time_updated: (row["time_updated"] as number) ?? Date.now(),
          server_seq: 0,
          deleted: false,
          data: row as any,
        };
      }
    }
  }

  // ── Composite reader ────────────────────────────────────────────

  readSessionFull(
    sessionId: string,
  ): { session: Session; messages: Message[]; parts: Part[]; todos: Todo[] } | null {
    const session = this.db
      .query<Session, [string]>("SELECT * FROM session WHERE id = ?")
      .get(sessionId);

    if (!session) return null;

    const messages = this.db
      .query<Message, [string]>("SELECT * FROM message WHERE session_id = ?")
      .all(sessionId);

    const parts = this.db
      .query<Part, [string]>("SELECT * FROM part WHERE session_id = ?")
      .all(sessionId);

    const todos = this.db
      .query<Todo, [string]>("SELECT * FROM todo WHERE session_id = ?")
      .all(sessionId);

    return { session, messages, parts, todos };
  }

  // ── Envelope builder ────────────────────────────────────────────

  toEnvelopes(
    kind: SyncKind,
    rows: Record<string, unknown>[],
    machineId: string,
  ): SyncEnvelope[] {
    return rows.map((row) => ({
      id: rowPrimaryKey(kind, row),
      kind,
      machine_id: machineId,
      time_updated: (row["time_updated"] as number) ?? Date.now(),
      server_seq: 0, // assigned by the server
      deleted: false,
      data: row as any,
    }));
  }

  close(): void {
    this.db.close();
  }

  // ── Private helpers ─────────────────────────────────────────────

  private readTable<T>(table: string, since?: number): T[] {
    if (since !== undefined) {
      return this.db
        .query<T, [number]>(`SELECT * FROM ${table} WHERE time_updated > ?`)
        .all(since);
    }
    return this.db.query<T, []>(`SELECT * FROM ${table}`).all();
  }
}
