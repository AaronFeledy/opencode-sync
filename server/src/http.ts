/**
 * HTTP helpers: gzip-aware JSON request parsing and response building.
 *
 * Why: a peer that's weeks behind transfers large JSON payloads (pull
 * pages, file manifest, big push/heads batches). These are highly
 * compressible text (prompts, code, tool output) and gzip typically
 * shrinks them 5-10x, which is the dominant cost when catching up a
 * large backlog over the network.
 *
 * Two directions, handled differently because Bun treats them
 * asymmetrically:
 *
 * - Responses: we gzip the body only when the client advertised
 *   `Accept-Encoding: gzip` AND the payload clears a size threshold
 *   (tiny bodies aren't worth the CPU and can even grow). Bun's global
 *   `fetch` auto-decompresses responses with `Content-Encoding: gzip`,
 *   so our own plugin client needs no changes to read them, and plain
 *   HTTP clients (curl) that don't send `Accept-Encoding` still get
 *   uncompressed JSON.
 *
 * - Requests: `Bun.serve` does NOT auto-decompress request bodies, so
 *   we detect `Content-Encoding: gzip` and `Bun.gunzipSync` before
 *   `JSON.parse`. Callers keep their existing try/catch so a malformed
 *   or undecodable body still surfaces as a 400.
 */

/**
 * Below this many bytes, response compression isn't worth the CPU (and
 * gzip framing overhead can make very small payloads larger). Pull
 * pages and manifests are far above this; push/heads acks are usually
 * below it and ship uncompressed.
 */
const RESPONSE_COMPRESS_THRESHOLD_BYTES = 1024;

/**
 * Parse a JSON request body, transparently inflating a gzip-encoded
 * body first. Throws on invalid JSON or a corrupt gzip stream — callers
 * wrap this in try/catch and return 400, mirroring the previous
 * `await req.json()` behaviour.
 */
export async function readJsonBody(req: Request): Promise<unknown> {
  const encoding = (req.headers.get("content-encoding") ?? "").toLowerCase();
  if (encoding.includes("gzip")) {
    const buf = await req.arrayBuffer();
    const inflated = Bun.gunzipSync(new Uint8Array(buf));
    return JSON.parse(new TextDecoder().decode(inflated));
  }
  return req.json();
}

/** True when the client's Accept-Encoding lists gzip. */
function clientAcceptsGzip(req: Request): boolean {
  const accept = req.headers.get("accept-encoding") ?? "";
  // Match `gzip` as a whole token so we don't false-positive on a future
  // `x-gzip-foo` or substring. Quality values (`gzip;q=0`) are rare for
  // our own Bun client; treating any listed gzip as acceptance is fine.
  return /(?:^|,)\s*gzip(?:\s*;|\s*,|\s*$)/i.test(accept);
}

/**
 * Build a JSON Response, gzipping the body when the client supports it
 * and the payload is large enough to benefit. Sets `Vary: Accept-Encoding`
 * so intermediaries cache the compressed and uncompressed variants
 * separately.
 */
export function jsonResponse(req: Request, body: unknown, status = 200): Response {
  const json = JSON.stringify(body);
  if (clientAcceptsGzip(req) && json.length >= RESPONSE_COMPRESS_THRESHOLD_BYTES) {
    const gzipped = Bun.gzipSync(json);
    return new Response(gzipped, {
      status,
      headers: {
        "content-type": "application/json",
        "content-encoding": "gzip",
        "vary": "Accept-Encoding",
      },
    });
  }
  return new Response(json, {
    status,
    headers: { "content-type": "application/json" },
  });
}
