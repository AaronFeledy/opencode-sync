/**
 * HTTP helpers: gzip + MessagePack-aware body parsing and response building.
 *
 * Pull pages and push batches are highly compressible structured data.
 * MessagePack shrinks them before gzip; gzip is still applied on top when
 * the client advertises it. Older clients that omit Accept: application/msgpack
 * still get JSON.
 *
 * Requests: Bun.serve does not auto-decompress, so we gunzip then unpack
 * msgpack or JSON.parse. Callers wrap this in try/catch and return 400.
 */
import {
  acceptsMsgpack,
  decodeMsgpack,
  encodeMsgpack,
  isMsgpackContentType,
  MSGPACK_CONTENT_TYPE,
} from "@opencode-sync/shared";

const RESPONSE_COMPRESS_THRESHOLD_BYTES = 1024;

export async function readJsonBody(req: Request): Promise<unknown> {
  const encoding = (req.headers.get("content-encoding") ?? "").toLowerCase();
  const contentType = req.headers.get("content-type");
  const wantsMsgpack = isMsgpackContentType(contentType);

  if (!encoding.includes("gzip") && !wantsMsgpack) {
    return req.json();
  }

  let bytes = new Uint8Array(await req.arrayBuffer());
  if (encoding.includes("gzip")) {
    bytes = new Uint8Array(Bun.gunzipSync(bytes));
  }
  if (wantsMsgpack) {
    return decodeMsgpack(bytes);
  }
  return JSON.parse(new TextDecoder().decode(bytes));
}

function clientAcceptsGzip(req: Request): boolean {
  const accept = req.headers.get("accept-encoding") ?? "";
  return /(?:^|,)\s*gzip(?:\s*;|\s*,|\s*$)/i.test(accept);
}

export function jsonResponse(req: Request, body: unknown, status = 200): Response {
  const useMsgpack = acceptsMsgpack(req.headers.get("accept"));
  const payload = new Uint8Array(
    useMsgpack ? encodeMsgpack(body) : new TextEncoder().encode(JSON.stringify(body)),
  );
  const contentType = useMsgpack ? MSGPACK_CONTENT_TYPE : "application/json";
  const headers: Record<string, string> = {
    "content-type": contentType,
    vary: "Accept, Accept-Encoding",
  };

  if (clientAcceptsGzip(req) && payload.byteLength >= RESPONSE_COMPRESS_THRESHOLD_BYTES) {
    headers["content-encoding"] = "gzip";
    return new Response(new Uint8Array(Bun.gzipSync(payload)), { status, headers });
  }
  return new Response(payload, { status, headers });
}
