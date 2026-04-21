/**
 * HTTP client for the opencode-sync server.
 * All methods authenticate via Bearer token and retry transient failures.
 */
import type {
  PushRequest,
  PushResponse,
  PullResponse,
  HealthResponse,
  HeadsRequest,
  HeadsResponse,
  HeadEntry,
  FileManifest,
  SyncEnvelope,
  SyncKind,
} from "@opencode-sync/shared";
import { sleep } from "./util.js";

// ── Constants ──────────────────────────────────────────────────────

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 500;

// ── Errors ─────────────────────────────────────────────────────────

/**
 * Thrown for HTTP 409 responses — the server has a strictly-newer version
 * of the resource (LWW reject). Callers should treat this as the documented
 * "your write was stale, pull and retry" path rather than a hard failure
 * (and log it accordingly to avoid misleading "failed to upload" lines on
 * what is normal multi-machine convergence).
 */
export class StaleError extends Error {
  constructor(
    public method: string,
    public urlPath: string,
    public body: string,
  ) {
    super(`opencode-sync: ${method} ${urlPath} rejected as stale (409): ${body}`);
    this.name = "StaleError";
  }
}

/**
 * Thrown for any non-2xx HTTP response that isn't already routed to a
 * more specific error type (`StaleError` for 409, `EndpointMissingError`
 * for `getHeads` 404). Exposes `status` so callers can branch on the
 * code instead of substring-matching the message.
 */
export class HttpError extends Error {
  constructor(
    public status: number,
    public method: string,
    public urlPath: string,
    public body: string,
  ) {
    super(`opencode-sync: ${method} ${urlPath} failed with ${status}: ${body}`);
    this.name = "HttpError";
  }
}

/**
 * Thrown by `getHeads` when the server doesn't expose `/sync/heads`
 * (404). Older servers — pre deletion-safety — return 404 here, and
 * the deletion-safety guard treats this as "fall back to local-only
 * confirmation" rather than failing the whole sync. Distinct error
 * type so the caller can branch on it cleanly.
 *
 * Scoped to `getHeads` only. Other endpoints that legitimately 404
 * (e.g. `GET /files/blob/:sha256` for a missing blob) surface as a
 * generic `HttpError` and are handled by their own callers — using
 * `EndpointMissingError` for those would mis-imply "this server lacks
 * the endpoint" when the server actually has it but the resource is
 * missing.
 */
export class EndpointMissingError extends Error {
  constructor(
    public method: string,
    public urlPath: string,
  ) {
    super(`opencode-sync: ${method} ${urlPath} not found on server (404)`);
    this.name = "EndpointMissingError";
  }
}

// ── Client ─────────────────────────────────────────────────────────

export class SyncClient {
  private baseUrl: string;
  private token: string;

  constructor(serverUrl: string, token: string) {
    // Normalise — strip trailing slash
    this.baseUrl = serverUrl.replace(/\/+$/, "");
    this.token = token;
  }

  // ── Public API ──────────────────────────────────────────────────

  async health(): Promise<HealthResponse> {
    return this.fetchJson<HealthResponse>("GET", "/health");
  }

  async push(machineId: string, envelopes: SyncEnvelope[]): Promise<PushResponse> {
    const body: PushRequest = { machine_id: machineId, envelopes };
    return this.fetchJson<PushResponse>("POST", "/sync/push", body);
  }

  async pull(since: number, exclude?: string, limit?: number): Promise<PullResponse> {
    const params = new URLSearchParams();
    params.set("since", String(since));
    if (exclude) params.set("exclude", exclude);
    if (limit !== undefined) params.set("limit", String(limit));
    return this.fetchJson<PullResponse>("GET", `/sync/pull?${params.toString()}`);
  }

  /**
   * Cross-check head state for a batch of `(kind, id)` pairs. Used by
   * the deletion-safety guard before emitting tombstones — if the server
   * has a strictly-newer version of a candidate, we'd rather pull that
   * version than overwrite it with a tombstone.
   *
   * Returns a Map keyed by `${kind}:${id}` for ergonomic lookup.
   *
   * Older servers (pre deletion-safety) don't expose `/sync/heads` and
   * will return 404. We surface that as `EndpointMissingError` so the
   * caller can fall back to local-only confirmation rather than aborting
   * sync.
   */
  async getHeads(
    machineId: string,
    rowKeys: Array<{ kind: SyncKind; id: string }>,
  ): Promise<Map<string, HeadEntry>> {
    if (rowKeys.length === 0) return new Map();

    const body: HeadsRequest = { machine_id: machineId, row_keys: rowKeys };

    let res: HeadsResponse;
    try {
      res = await this.fetchJson<HeadsResponse>("POST", "/sync/heads", body);
    } catch (err) {
      // Endpoint-scoped 404 translation — happens here (not in `request()`)
      // so a 404 from a different endpoint (e.g. blob-not-found) doesn't
      // accidentally surface as `EndpointMissingError` and confuse its
      // callers.
      if (err instanceof HttpError && err.status === 404) {
        throw new EndpointMissingError("POST", "/sync/heads");
      }
      throw err;
    }

    const map = new Map<string, HeadEntry>();
    for (const head of res.heads) {
      map.set(`${head.kind}:${head.id}`, head);
    }
    return map;
  }

  async getManifest(): Promise<FileManifest> {
    return this.fetchJson<FileManifest>("GET", "/files/manifest");
  }

  async getBlob(sha256: string): Promise<ArrayBuffer> {
    const res = await this.request("GET", `/files/blob/${sha256}`);
    return res.arrayBuffer();
  }

  async putFile(relpath: string, data: Uint8Array, machineId: string, mtime: number): Promise<void> {
    const encodedPath = encodeURIComponent(relpath);
    await this.request("PUT", `/files/${encodedPath}`, data, "application/octet-stream", {
      "X-Machine-ID": machineId,
      "X-Mtime": String(mtime),
    });
  }

  async deleteFile(relpath: string, machineId: string, mtime: number): Promise<void> {
    const encodedPath = encodeURIComponent(relpath);
    await this.request("DELETE", `/files/${encodedPath}`, undefined, undefined, {
      "X-Machine-ID": machineId,
      "X-Mtime": String(mtime),
    });
  }

  // ── Internal helpers ────────────────────────────────────────────

  private headers(contentType?: string): Record<string, string> {
    const h: Record<string, string> = {};
    if (this.token) {
      h["Authorization"] = `Bearer ${this.token}`;
    }
    if (contentType) {
      h["Content-Type"] = contentType;
    }
    return h;
  }

  /**
   * Core fetch with retry logic for transient errors (5xx / network).
   */
  private async request(
    method: string,
    urlPath: string,
    body?: unknown,
    contentType?: string,
    extraHeaders?: Record<string, string>,
  ): Promise<Response> {
    const url = `${this.baseUrl}${urlPath}`;
    const ct = contentType ?? (body !== undefined && !(body instanceof Uint8Array) ? "application/json" : undefined);
    const headers = { ...this.headers(ct), ...extraHeaders };

    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        const delay = BASE_DELAY_MS * Math.pow(2, attempt - 1);
        await sleep(delay);
      }

      try {
        const res = await fetch(url, {
          method,
          headers,
          body: body instanceof Uint8Array
            ? body
            : body !== undefined
              ? JSON.stringify(body)
              : undefined,
        });

        // 409 Conflict — LWW-stale writes. Surface as a typed error so
        // callers can distinguish "server rejected as stale, pull & retry"
        // from genuine failures. Without this, expected stale-write races
        // get logged as "failed to upload file" and look like hard errors.
        if (res.status === 409) {
          const text = await res.text().catch(() => "");
          throw new StaleError(method, urlPath, text);
        }

        // Non-retryable client errors (including 404). Surface as a
        // typed `HttpError` exposing `status` so endpoint-specific
        // callers can translate to a more meaningful error type at
        // their boundary (e.g. `getHeads` translates 404 →
        // `EndpointMissingError`). Doing the translation here would
        // make 404 ambiguous: "server lacks the endpoint" vs. "the
        // resource at this endpoint is missing" (e.g. blob 404).
        if (res.status >= 400 && res.status < 500) {
          const text = await res.text().catch(() => "");
          throw new HttpError(res.status, method, urlPath, text);
        }

        // Retryable server errors
        if (res.status >= 500) {
          lastError = new Error(
            `opencode-sync: ${method} ${urlPath} returned ${res.status}`,
          );
          continue;
        }

        return res;
      } catch (err) {
        // Network errors are retryable. fetch() only throws TypeError for
        // connection-level failures (DNS, refused, reset); aborts surface
        // as DOMException/AbortError (different constructor), so matching
        // on TypeError alone is strictly correct here and avoids fragile
        // substring checks against platform-specific message text.
        if (err instanceof TypeError) {
          lastError = err;
          continue;
        }
        // Re-throw non-retryable errors (e.g. our 4xx throws, StaleError)
        throw err;
      }
    }

    throw lastError ?? new Error(`opencode-sync: ${method} ${urlPath} failed after ${MAX_RETRIES} retries`);
  }

  private async fetchJson<T>(method: string, urlPath: string, body?: unknown): Promise<T> {
    const res = await this.request(method, urlPath, body);
    return (await res.json()) as T;
  }
}
