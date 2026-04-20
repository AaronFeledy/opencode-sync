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
}

export interface PullResponse {
  /** Current max server_seq */
  server_seq: number;
  /** Envelopes in server_seq order */
  envelopes: SyncEnvelope[];
  /** True if there are more rows to pull */
  more: boolean;
}

// ── Health ──────────────────────────────────────────────────────────

export interface HealthResponse {
  ok: boolean;
  version: string;
  time: number;
}
