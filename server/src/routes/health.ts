/**
 * GET /health — unauthenticated health check.
 */

import type { HealthResponse } from "@opencode-sync/shared";
import type { LedgerDB } from "../db.js";
import type { Logger } from "../log.js";

export function handleHealth(
  _request: Request,
  _db: LedgerDB,
  _logger: Logger,
  version: string,
): Response {
  const body: HealthResponse = {
    ok: true,
    version,
    time: Date.now(),
  };

  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
