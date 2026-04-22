/**
 * POST /sync/push  — accept envelopes from a client machine.
 * GET  /sync/pull  — return envelopes newer than a given server_seq.
 */

import type { Logger } from "../log.js";
import type { LedgerDB } from "../db.js";
import {
  SYNC_KINDS,
  type HeadsResponse,
  type PushResponse,
  type PullResponse,
  type StaleEntry,
  type SyncEnvelope,
  type SyncKind,
} from "@opencode-sync/shared";

const VALID_KINDS = new Set<string>(SYNC_KINDS);

/**
 * Type-narrowing predicate so callers don't have to `as SyncKind` cast
 * every time they check membership against the runtime set. Mirrors
 * `VALID_KINDS.has` but tells TypeScript the result.
 */
function isSyncKind(value: string): value is SyncKind {
  return VALID_KINDS.has(value);
}

/**
 * Cap on the number of (kind, id) pairs allowed in a single
 * `POST /sync/heads` request. Keeps the SQLite query under the default
 * SQLITE_MAX_VARIABLE_NUMBER (32766) with comfortable headroom even
 * after grouping by kind blows up the parameter count slightly.
 */
const HEADS_MAX_BATCH = 5000;

/**
 * Cap on the number of envelopes accepted in a single POST /sync/push.
 * Bounds the SQLite write-transaction size (upsertBatch runs in one tx)
 * so a malicious or buggy client can't wedge the lock for arbitrary
 * durations. The plugin's PUSH_BATCH_SIZE is 100 (plugin/src/sessions.ts),
 * so legitimate clients are nowhere near this cap. See FINDINGS.md H4.
 */
const PUSH_MAX_BATCH = 5000;

// ── Push ────────────────────────────────────────────────────────────

export async function handleSyncPush(
  req: Request,
  db: LedgerDB,
  logger: Logger,
): Promise<Response> {
  // Parse body
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return Response.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  if (!body || typeof body !== "object") {
    return Response.json({ error: "Request body must be a JSON object" }, { status: 400 });
  }

  const { machine_id, envelopes } = body as Record<string, unknown>;

  if (typeof machine_id !== "string" || machine_id.length === 0) {
    return Response.json(
      { error: "machine_id is required and must be a non-empty string" },
      { status: 400 },
    );
  }

  if (!Array.isArray(envelopes)) {
    return Response.json({ error: "envelopes must be an array" }, { status: 400 });
  }

  if (envelopes.length === 0) {
    return Response.json({ error: "envelopes array must not be empty" }, { status: 400 });
  }

  if (envelopes.length > PUSH_MAX_BATCH) {
    return Response.json(
      { error: `envelopes array exceeds maximum batch size of ${PUSH_MAX_BATCH}` },
      { status: 400 },
    );
  }

  // Validate every envelope before touching the database — `upsertBatch`
  // applies all-or-nothing, so we want to reject malformed input up-front
  // rather than starting a transaction we'd just have to roll back.
  //
  // We validate every field that downstream code dereferences without
  // re-checking. In particular, an unknown `kind` would be inserted into
  // sync_row (no CHECK constraint at the schema level) and then become a
  // poison pill for every peer that pulls it: the plugin's applyEnvelope
  // indexes TABLE_COLUMNS[kind] → undefined → TypeError. The throw is
  // caught upstream and the cursor still advances, so it's self-limiting,
  // but the row pollutes the ledger forever and produces one error log
  // per peer per first sight. Reject it at the API boundary instead.
  for (const raw of envelopes) {
    if (!raw || typeof raw !== "object") {
      return Response.json(
        { error: "Each envelope must be an object" },
        { status: 400 },
      );
    }
    const envelope = raw as Record<string, unknown>;
    if (typeof envelope.id !== "string" || envelope.id.length === 0) {
      return Response.json(
        { error: "envelope.id must be a non-empty string" },
        { status: 400 },
      );
    }
    if (typeof envelope.kind !== "string" || !VALID_KINDS.has(envelope.kind)) {
      return Response.json(
        { error: `envelope.kind must be one of: ${SYNC_KINDS.join(", ")}` },
        { status: 400 },
      );
    }
    if (typeof envelope.machine_id !== "string" || envelope.machine_id.length === 0) {
      return Response.json(
        { error: "envelope.machine_id must be a non-empty string" },
        { status: 400 },
      );
    }
    if (
      typeof envelope.time_updated !== "number" ||
      !Number.isFinite(envelope.time_updated) ||
      envelope.time_updated <= 0
    ) {
      // Reject 0 explicitly: `time_updated = 0` breaks LWW comparison
      // on the plugin side (any freshly-zeroed local row would compare
      // equal and silently skip content differences). Keep the server
      // and plugin in lockstep so a stray 0 can't poison the ledger.
      // See FINDINGS.md M8.
      return Response.json(
        { error: "envelope.time_updated must be a positive finite number" },
        { status: 400 },
      );
    }
    if (typeof envelope.deleted !== "boolean") {
      return Response.json(
        { error: "envelope.deleted must be a boolean" },
        { status: 400 },
      );
    }
  }

  const accepted: string[] = [];
  const stale: StaleEntry[] = [];

  const results = db.upsertBatch(envelopes as SyncEnvelope[]);

  for (let i = 0; i < envelopes.length; i++) {
    const envelope = (envelopes as SyncEnvelope[])[i]!;
    const result = results[i]!;

    if (result.accepted) {
      accepted.push(envelope.id);
    } else {
      stale.push({
        kind: envelope.kind,
        id: envelope.id,
        server_time_updated: result.stale!.server_time_updated,
      });
    }
  }

  const serverSeq = db.getNextSeq() - 1;

  logger.info("sync push", {
    machine_id,
    total: envelopes.length,
    accepted: accepted.length,
    stale: stale.length,
  });

  const response: PushResponse = {
    server_seq: serverSeq,
    accepted,
    stale,
  };

  return Response.json(response);
}

// ── Heads (deletion-safety cross-check) ─────────────────────────────

/**
 * POST /sync/heads — return the server's current `time_updated` and
 * `deleted` state for a batch of `(kind, id)` pairs. Used by the
 * plugin's deletion-safety guard to confirm a row is "really" gone
 * (or stale) before propagating a tombstone for it.
 *
 * Rows the server has never seen are omitted from the response — the
 * caller treats absence as "server doesn't know this row, safe to
 * tombstone if local says so."
 */
export async function handleSyncHeads(
  req: Request,
  db: LedgerDB,
  logger: Logger,
): Promise<Response> {
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return Response.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  if (!body || typeof body !== "object") {
    return Response.json({ error: "Request body must be a JSON object" }, { status: 400 });
  }

  const { machine_id, row_keys } = body as Record<string, unknown>;

  if (typeof machine_id !== "string" || machine_id.length === 0) {
    return Response.json(
      { error: "machine_id is required and must be a non-empty string" },
      { status: 400 },
    );
  }

  if (!Array.isArray(row_keys)) {
    return Response.json({ error: "row_keys must be an array" }, { status: 400 });
  }

  if (row_keys.length === 0) {
    // Vacuous request — return immediately with empty heads. Avoids
    // round-tripping an obviously-empty response through the DB layer.
    const response: HeadsResponse = { heads: [] };
    return Response.json(response);
  }

  if (row_keys.length > HEADS_MAX_BATCH) {
    return Response.json(
      { error: `row_keys array exceeds maximum batch size of ${HEADS_MAX_BATCH}` },
      { status: 400 },
    );
  }

  // Validate every entry up-front. Keep this strict — an invalid `kind`
  // making it into the SQL builder would just be filtered out by the
  // primary-key index, but reject loudly so misbehaving clients learn.
  const validated: Array<{ kind: SyncKind; id: string }> = [];
  for (const raw of row_keys) {
    if (!raw || typeof raw !== "object") {
      return Response.json(
        { error: "Each row_keys entry must be an object" },
        { status: 400 },
      );
    }
    const entry = raw as Record<string, unknown>;
    if (typeof entry.kind !== "string" || !isSyncKind(entry.kind)) {
      return Response.json(
        { error: `row_keys entry has invalid kind; must be one of: ${SYNC_KINDS.join(", ")}` },
        { status: 400 },
      );
    }
    if (typeof entry.id !== "string" || entry.id.length === 0) {
      return Response.json(
        { error: "row_keys entry id must be a non-empty string" },
        { status: 400 },
      );
    }
    validated.push({ kind: entry.kind, id: entry.id });
  }

  const heads = db.getHeads(validated);

  logger.debug("sync heads", {
    machine_id,
    requested: validated.length,
    returned: heads.length,
  });

  // No cast needed: `db.getHeads` returns `SyncKind` directly now that
  // the validated input is typed.
  const response: HeadsResponse = {
    heads: heads.map((h) => ({
      kind: h.kind,
      id: h.id,
      time_updated: h.time_updated,
      deleted: h.deleted,
    })),
  };

  return Response.json(response);
}

// ── Pull ────────────────────────────────────────────────────────────

export function handleSyncPull(
  req: Request,
  db: LedgerDB,
  logger: Logger,
): Response {
  const url = new URL(req.url);

  // Parse `since` — required, must be a non-negative integer
  const sinceRaw = url.searchParams.get("since");
  if (sinceRaw === null) {
    return Response.json({ error: "since query parameter is required" }, { status: 400 });
  }
  const since = Number(sinceRaw);
  if (!Number.isFinite(since) || since < 0 || !Number.isInteger(since)) {
    return Response.json({ error: "since must be a non-negative integer" }, { status: 400 });
  }

  // Parse `exclude` — optional machine_id to filter out
  const exclude = url.searchParams.get("exclude") ?? undefined;

  // Parse `limit` — optional, defaults to 500
  const limitRaw = url.searchParams.get("limit");
  let limit = 500;
  if (limitRaw !== null) {
    limit = Number(limitRaw);
    if (!Number.isFinite(limit) || limit < 1 || !Number.isInteger(limit)) {
      return Response.json({ error: "limit must be a positive integer" }, { status: 400 });
    }
    // Cap at a reasonable max
    limit = Math.min(limit, 5000);
  }

  const result = db.pullRows(since, exclude, limit);

  logger.debug("sync pull", {
    since,
    exclude,
    limit,
    returned: result.envelopes.length,
    more: result.more,
  });

  const response: PullResponse = {
    server_seq: result.server_seq,
    envelopes: result.envelopes,
    more: result.more,
  };

  return Response.json(response);
}
