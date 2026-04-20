/**
 * Core types mirroring opencode's SQLite schema.
 * These represent the row payloads sent inside SyncEnvelopes.
 */

// ── Row types (matching opencode.db schema) ────────────────────────

export interface Project {
  id: string;
  worktree: string;
  vcs: string | null;
  name: string | null;
  icon_url: string | null;
  icon_color: string | null;
  time_created: number;
  time_updated: number;
  time_initialized: number | null;
  sandboxes: string; // JSON
  commands: string | null; // JSON
}

export interface Session {
  id: string;
  project_id: string;
  parent_id: string | null;
  slug: string;
  directory: string;
  title: string;
  version: string;
  share_url: string | null;
  summary_additions: number | null;
  summary_deletions: number | null;
  summary_files: number | null;
  summary_diffs: string | null;
  revert: string | null;
  permission: string | null;
  time_created: number;
  time_updated: number;
  time_compacting: number | null;
  time_archived: number | null;
  workspace_id: string | null;
}

export interface Message {
  id: string;
  session_id: string;
  time_created: number;
  time_updated: number;
  data: string; // JSON blob
}

export interface Part {
  id: string;
  message_id: string;
  session_id: string;
  time_created: number;
  time_updated: number;
  data: string; // JSON blob
}

export interface Todo {
  session_id: string;
  content: string;
  status: string;
  priority: string;
  position: number;
  time_created: number;
  time_updated: number;
}

export interface Permission {
  project_id: string;
  time_created: number;
  time_updated: number;
  data: string; // JSON blob
}

export interface SessionShare {
  session_id: string;
  id: string;
  secret: string;
  url: string;
  time_created: number;
  time_updated: number;
}

// ── Sync kinds ─────────────────────────────────────────────────────

export const SYNC_KINDS = [
  "project",
  "session",
  "message",
  "part",
  "todo",
  "permission",
  "session_share",
] as const;

export type SyncKind = (typeof SYNC_KINDS)[number];

// ── Map from kind → row type ───────────────────────────────────────

export interface SyncKindMap {
  project: Project;
  session: Session;
  message: Message;
  part: Part;
  todo: Todo;
  permission: Permission;
  session_share: SessionShare;
}

/**
 * Get the primary key for a given kind/row.
 * Most rows use `id`; todo uses `session_id:position` composite.
 */
export function rowPrimaryKey(kind: SyncKind, row: Record<string, unknown>): string {
  if (kind === "todo") {
    return `${row.session_id}:${row.position}`;
  }
  if (kind === "permission") {
    return row.project_id as string;
  }
  if (kind === "session_share") {
    return row.session_id as string;
  }
  return row.id as string;
}

/**
 * Parse an envelope id back into its primary-key components, in the same
 * column order as the local SQLite schema. Inverse of `rowPrimaryKey`.
 *
 * Single-PK kinds (`project`, `session`, `message`, `part`, `permission`,
 * `session_share`) return a one-element array — even when the id itself
 * contains colons (avoiding silent data loss from a naive `id.split(":")`).
 *
 * `todo` is the only composite kind: format is `<session_id>:<position>`,
 * where position is a non-negative integer. We split on the LAST colon so
 * a session_id containing colons still parses correctly.
 *
 * Returns null if the id can't be parsed for the given kind (e.g. a todo
 * id with no colon at all).
 */
export function parseRowPrimaryKey(
  kind: SyncKind,
  id: string,
): string[] | null {
  if (kind === "todo") {
    const lastColon = id.lastIndexOf(":");
    if (lastColon === -1) return null;
    return [id.slice(0, lastColon), id.slice(lastColon + 1)];
  }
  return [id];
}
