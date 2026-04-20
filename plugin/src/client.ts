/**
 * HTTP client for the opencode-sync server.
 * All methods authenticate via Bearer token and retry transient failures.
 */
import type {
  PushRequest,
  PushResponse,
  PullResponse,
  HealthResponse,
  FileManifest,
  SyncEnvelope,
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

        // Non-retryable client errors
        if (res.status >= 400 && res.status < 500) {
          const text = await res.text().catch(() => "");
          throw new Error(
            `opencode-sync: ${method} ${urlPath} failed with ${res.status}: ${text}`,
          );
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
