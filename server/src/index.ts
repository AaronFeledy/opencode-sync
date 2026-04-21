/**
 * opencode-sync server — main entry point.
 * Bun.serve() HTTP server with SQLite ledger, bearer auth, and file sync.
 */

import { loadConfig } from "./config.js";
import { createLogger } from "./log.js";
import { LedgerDB } from "./db.js";
import { checkAuth } from "./auth.js";
import { handleHealth } from "./routes/health.js";
import { handleSyncPush, handleSyncPull, handleSyncHeads } from "./routes/sync.js";
import {
  handleFilesManifest,
  handleFileBlob,
  handleFilePut,
  handleFileDelete,
} from "./routes/files.js";

// ── Bootstrap ──────────────────────────────────────────────────────

const config = loadConfig();
const logger = createLogger(config.logLevel);
const db = new LedgerDB(config.dataDir, logger);

// ── Parse listen address ───────────────────────────────────────────

function parseListenAddress(listen: string): { hostname: string; port: number } {
  const lastColon = listen.lastIndexOf(":");
  if (lastColon === -1) {
    throw new Error(`Invalid listen address "${listen}". Expected "host:port".`);
  }
  const hostname = listen.slice(0, lastColon);
  const port = parseInt(listen.slice(lastColon + 1), 10);
  if (!hostname) {
    // Bun's behaviour with hostname:"" is undocumented (may bind 0.0.0.0,
    // may error). Reject explicitly so misconfiguration is loud rather
    // than ambiguous.
    throw new Error(
      `Invalid listen address "${listen}". Hostname must not be empty (use "0.0.0.0:${port}" to bind all interfaces).`,
    );
  }
  if (isNaN(port) || port < 1 || port > 65535) {
    throw new Error(`Invalid port in listen address "${listen}".`);
  }
  return { hostname, port };
}

const { hostname, port } = parseListenAddress(config.listen);

// ── Router ─────────────────────────────────────────────────────────

async function handleRequest(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  // GET /health — no auth required
  if (method === "GET" && path === "/health") {
    return handleHealth(request, db, logger, config.version);
  }

  // All other routes require auth
  const authError = checkAuth(request, config.token);
  if (authError) return authError;

  // POST /sync/push
  if (method === "POST" && path === "/sync/push") {
    return handleSyncPush(request, db, logger);
  }

  // GET /sync/pull
  if (method === "GET" && path === "/sync/pull") {
    return handleSyncPull(request, db, logger);
  }

  // POST /sync/heads — deletion-safety cross-check
  if (method === "POST" && path === "/sync/heads") {
    return handleSyncHeads(request, db, logger);
  }

  // GET /files/manifest
  if (method === "GET" && path === "/files/manifest") {
    return handleFilesManifest(request, db, logger);
  }

  // GET /files/blob/:sha256
  if (method === "GET" && path.startsWith("/files/blob/")) {
    const sha256 = path.slice("/files/blob/".length);
    return handleFileBlob(request, db, logger, sha256);
  }

  // PUT /files/:path — upload a file
  if (method === "PUT" && path.startsWith("/files/")) {
    const relpath = decodeURIComponent(path.slice("/files/".length));
    if (!relpath || relpath.startsWith("blob/") || relpath === "manifest") {
      return new Response(JSON.stringify({ error: "Reserved path" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
    }
    return handleFilePut(request, db, logger, relpath);
  }

  // DELETE /files/:path — delete a file
  if (method === "DELETE" && path.startsWith("/files/")) {
    const relpath = decodeURIComponent(path.slice("/files/".length));
    if (!relpath || relpath.startsWith("blob/") || relpath === "manifest") {
      return new Response(JSON.stringify({ error: "Reserved path" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
    }
    return handleFileDelete(request, db, logger, relpath);
  }

  // 404 — unknown route
  return new Response(JSON.stringify({ error: "Not found" }), {
    status: 404,
    headers: { "content-type": "application/json" },
  });
}

// ── Start server ───────────────────────────────────────────────────

const server = Bun.serve({
  hostname,
  port,
  fetch: handleRequest,
  error(error: Error) {
    logger.error("Unhandled server error", { message: error.message, stack: error.stack });
    return new Response(JSON.stringify({ error: "Internal server error" }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
  },
});

logger.info(`opencode-sync server v${config.version} listening`, {
  address: `${server.hostname}:${server.port}`,
  dataDir: config.dataDir,
  logLevel: config.logLevel,
});

// ── Graceful shutdown ──────────────────────────────────────────────

function shutdown(signal: string) {
  logger.info(`Received ${signal}, shutting down...`);
  server.stop(true); // close_immediately = true
  db.close();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
