/**
 * Sync protocol types — push/pull request & response shapes.
 */

import type { SyncKind, SyncKindMap } from "./types.js";

// ── Sync envelope ──────────────────────────────────────────────────

export interface SyncEnvelope<K extends SyncKind = SyncKind> {
  /** Row primary key */
  id: string;
  /** Row kind discriminator */
  kind: K;
  /** Machine that wrote this row locally */
  machine_id: string;
  /** From opencode row; drives conflict resolution */
  time_updated: number;
  /** Monotonic sequence; assigned by server; clients cursor by this */
  server_seq: number;
  /** Tombstone flag */
  deleted: boolean;
  /** Full row payload (null iff deleted) */
  data: SyncKindMap[K] | null;
}

// ── Push ────────────────────────────────────────────────────────────

export interface PushRequest {
  machine_id: string;
  envelopes: SyncEnvelope[];
}

export interface StaleEntry {
  kind: SyncKind;
  id: string;
  server_time_updated: number;
}

export interface PushResponse {
  /** Current max server_seq after processing */
  server_seq: number;
  /** IDs that were accepted */
  accepted: string[];
  /** Envelopes that were rejected because the server has a newer version */
  stale: StaleEntry[];
}

// ── Pull ────────────────────────────────────────────────────────────

export interface PullQuery {
  /** Pull rows with server_seq > since */
  since: number;
  /** Exclude rows authored by this machine (avoids echoing back) */
  exclude?: string;
  /** Max rows to return (default 500) */
  limit?: number;
  /**
   * When set, only envelopes with `time_updated >= min_time_updated` are
   * returned (still ordered by `server_seq`). Used by the plugin's
   * launch-blocking recent-session pull so a peer does not have to walk
   * the full ledger before opencode can start. Older servers ignore
   * unknown query params — clients MUST also see `FEATURE_PULL_MIN_TIME`
   * in `/health` before sending this, otherwise they would re-download
   * the entire backlog.
   */
  min_time_updated?: number;
}

export interface PullResponse {
  /** Current max server_seq */
  server_seq: number;
  /**
   * Cursor clients should persist after this page. Normally this is the last
   * envelope from the base seq window. It can differ from the final envelope
   * when the server appends dependency rows with later server_seq values so
   * children can apply without skipping intervening ledger rows.
   */
  cursor_seq?: number;
  /** True when the server included currently-known parent rows for children. */
  dependency_closure?: boolean;
  /** Envelopes in server_seq order */
  envelopes: SyncEnvelope[];
  /** True if there are more rows to pull */
  more: boolean;
}

// ── Heads (deletion-safety cross-check) ─────────────────────────────

/**
 * Request payload for `POST /sync/heads`. The plugin sends a list of
 * `(kind, id)` pairs it's considering tombstoning; the server replies
 * with the current `time_updated` and `deleted` state for each row it
 * has on file. Rows the server has never seen are simply omitted from
 * the response (not returned with a sentinel) — keeps the response
 * compact when most candidates are unknown.
 */
export interface HeadsRequest {
  machine_id: string;
  row_keys: Array<{ kind: SyncKind; id: string }>;
}

export interface HeadEntry {
  kind: SyncKind;
  id: string;
  time_updated: number;
  deleted: boolean;
}

export interface HeadsResponse {
  heads: HeadEntry[];
}

// ── Health ──────────────────────────────────────────────────────────

/**
 * Capability advertised by a server that can inflate gzip-encoded request
 * bodies (`Content-Encoding: gzip`). Clients MUST NOT gzip request bodies
 * unless the server lists this in `HealthResponse.features` — older servers
 * omit it and would otherwise try to `JSON.parse` compressed bytes and 400.
 * Response compression needs no such flag: it's negotiated per-request via
 * the standard `Accept-Encoding`/`Content-Encoding` headers.
 */
export const FEATURE_GZIP_REQUEST = "gzip-request";

/**
 * Capability advertised by a server that honours `min_time_updated` on
 * `GET /sync/pull`. Clients MUST NOT send that query param unless the
 * server lists this feature — older servers ignore unknown params and
 * would return the unfiltered ledger, defeating the recent-session
 * startup path.
 */
export const FEATURE_PULL_MIN_TIME = "pull-min-time";

/**
 * Capability advertised by a server that accepts and returns MessagePack
 * bodies (`Content-Type: application/msgpack`). Clients MUST NOT send
 * MessagePack unless the server lists this — older servers would
 * JSON.parse binary bytes and 400.
 */
export const FEATURE_MSGPACK = "msgpack";

export const MSGPACK_CONTENT_TYPE = "application/msgpack";

export interface HealthResponse {
  ok: boolean;
  version: string;
  time: number;
  /**
   * Optional capability tokens the server supports (e.g.
   * `FEATURE_GZIP_REQUEST`). Absent on older servers — treat missing as
   * "no optional capabilities".
   */
  features?: string[];
}
