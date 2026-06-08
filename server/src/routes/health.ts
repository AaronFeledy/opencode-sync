/**
 * GET /health — unauthenticated health check.
 */

import { FEATURE_GZIP_REQUEST, type HealthResponse } from "@opencode-sync/shared";
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
    // Advertise that this server can inflate gzip-encoded request bodies so
    // clients only compress uploads when it's safe (older servers omit this).
    features: [FEATURE_GZIP_REQUEST],
  };

  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
