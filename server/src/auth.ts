/**
 * Bearer token authentication.
 */

import { timingSafeEqual } from "node:crypto";

/**
 * Constant-time string equality. Returns false immediately on length mismatch
 * (lengths are not secret), otherwise uses crypto.timingSafeEqual to avoid
 * leaking the byte index of the first mismatch via response timing.
 */
function safeStringEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, "utf-8");
  const bBuf = Buffer.from(b, "utf-8");
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

/**
 * Check the Authorization header against the expected token.
 * Returns a 401 Response if auth fails, or null if auth passes.
 *
 * Accepts the format `Bearer <token>` where `<token>` is the entire
 * remainder of the header after `Bearer `. We deliberately do NOT split on
 * whitespace and reject multi-part tokens, because users can configure
 * arbitrary strings (passphrases, etc.) as their token via plugin config —
 * a token containing spaces should authenticate, not be silently rejected
 * as a malformed header. RFC 6750 reserves whitespace handling for the
 * server, and treating everything after `Bearer ` as opaque is consistent
 * with most production HTTP servers.
 */
export function checkAuth(request: Request, token: string): Response | null {
  const header = request.headers.get("authorization");
  if (!header) {
    return new Response(JSON.stringify({ error: "Missing Authorization header" }), {
      status: 401,
      headers: { "content-type": "application/json" },
    });
  }

  const PREFIX = "Bearer ";
  // Case-sensitive prefix per RFC 6750 §2.1 (the scheme is "Bearer", and
  // although schemes are technically case-insensitive in RFC 7235, sticking
  // to the canonical casing avoids ambiguity and matches our client).
  if (!header.startsWith(PREFIX)) {
    return new Response(
      JSON.stringify({ error: 'Invalid Authorization header format. Expected "Bearer <token>"' }),
      {
        status: 401,
        headers: { "content-type": "application/json" },
      },
    );
  }

  const presented = header.slice(PREFIX.length);
  if (presented.length === 0) {
    return new Response(
      JSON.stringify({ error: 'Invalid Authorization header format. Expected "Bearer <token>"' }),
      {
        status: 401,
        headers: { "content-type": "application/json" },
      },
    );
  }

  if (!safeStringEqual(presented, token)) {
    return new Response(JSON.stringify({ error: "Invalid token" }), {
      status: 401,
      headers: { "content-type": "application/json" },
    });
  }

  return null;
}
