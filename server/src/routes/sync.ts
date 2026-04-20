/**
 * POST /sync/push  — accept envelopes from a client machine.
 * GET  /sync/pull  — return envelopes newer than a given server_seq.
 */

import type { Logger } from "../log.js";
import type { LedgerDB } from "../db.js";
import {
  SYNC_KINDS,
  type PushResponse,
  type PullResponse,
  type StaleEntry,
  type SyncEnvelope,
} from "@opencode-sync/shared";

const VALID_KINDS = new Set<string>(SYNC_KINDS);

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
      envelope.time_updated < 0
    ) {
      return Response.json(
        { error: "envelope.time_updated must be a non-negative finite number" },
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
