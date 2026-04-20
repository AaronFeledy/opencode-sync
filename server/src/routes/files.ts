/**
 * File sync routes — manifest, blob storage, put/delete.
 *
 * GET    /files/manifest       — full file manifest
 * GET    /files/blob/:sha256   — download blob by hash
 * PUT    /files/:path          — upload or overwrite a file
 * DELETE /files/:path          — mark a file as deleted (tombstone)
 */

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import type { Logger } from "../log.js";
import type { LedgerDB } from "../db.js";
import type { FileManifestEntry } from "@opencode-sync/shared";

// ── Path validation ────────────────────────────────────────────────

/**
 * Validate a manifest relpath. Rejects empty paths, traversal (`..`),
 * absolute paths, and embedded NUL bytes (which can bypass naive string
 * checks and be interpreted as path terminators by some syscalls).
 *
 * Returns null on success or a 400 Response on failure.
 */
function validateRelpath(relpath: string): Response | null {
  if (!relpath || relpath.length === 0) {
    return Response.json({ error: "File path is required" }, { status: 400 });
  }
  if (relpath.includes("..") || relpath.startsWith("/") || relpath.includes("\0")) {
    return Response.json({ error: "Invalid file path" }, { status: 400 });
  }
  return null;
}

// ── GET /files/manifest ─────────────────────────────────────────────

export function handleFilesManifest(
  req: Request,
  db: LedgerDB,
  logger: Logger,
): Response {
  const manifest = db.getManifest();
  logger.debug("files manifest", { entries: manifest.length });
  return Response.json(manifest);
}

// ── GET /files/blob/:sha256 ─────────────────────────────────────────

export async function handleFileBlob(
  req: Request,
  db: LedgerDB,
  logger: Logger,
  sha256: string,
): Promise<Response> {
  // Validate sha256 — must be a 64-char lowercase hex string
  if (!/^[0-9a-f]{64}$/.test(sha256)) {
    return Response.json({ error: "Invalid sha256 hash" }, { status: 400 });
  }

  const blobPath = db.getBlobPath(sha256);

  if (!existsSync(blobPath)) {
    logger.warn("blob not found", { sha256 });
    return Response.json({ error: "Blob not found" }, { status: 404 });
  }

  // Use Bun.file for streaming + non-blocking I/O. The runtime sets
  // Content-Length and Content-Type from the file; we override Content-Type
  // so clients always see octet-stream regardless of any sniffing, and add
  // ETag + immutable Cache-Control because blobs are content-addressable.
  const file = Bun.file(blobPath);
  logger.debug("blob served", { sha256, size: file.size });

  return new Response(file, {
    status: 200,
    headers: {
      "Content-Type": "application/octet-stream",
      "ETag": `"${sha256}"`,
      "Cache-Control": "public, immutable, max-age=31536000",
    },
  });
}

// ── PUT /files/:path ────────────────────────────────────────────────

export async function handleFilePut(
  req: Request,
  db: LedgerDB,
  logger: Logger,
  relpath: string,
): Promise<Response> {
  const pathError = validateRelpath(relpath);
  if (pathError) return pathError;

  const machineId = req.headers.get("X-Machine-ID");
  if (!machineId || machineId.length === 0) {
    return Response.json({ error: "X-Machine-ID header is required" }, { status: 400 });
  }

  const mtimeRaw = req.headers.get("X-Mtime");
  if (!mtimeRaw) {
    return Response.json({ error: "X-Mtime header is required" }, { status: 400 });
  }
  const mtime = Number(mtimeRaw);
  if (!Number.isFinite(mtime) || mtime < 0) {
    return Response.json(
      { error: "X-Mtime must be a non-negative number (ms epoch)" },
      { status: 400 },
    );
  }

  // LWW guard — reject writes that are strictly older than what the server
  // already has. Without this, an offline client coming back online could
  // PUT a stale version and silently clobber a newer file uploaded by
  // another machine. Equal mtime is allowed (idempotent re-upload).
  const existing = db.getManifestEntry(relpath);
  if (existing && existing.mtime > mtime) {
    logger.info("file put rejected (stale)", {
      relpath,
      incoming_mtime: mtime,
      server_mtime: existing.mtime,
      machine_id: machineId,
    });
    return Response.json(
      { error: "stale", server: existing },
      { status: 409 },
    );
  }

  // Read request body as binary
  const body = await req.arrayBuffer();
  if (body.byteLength === 0) {
    return Response.json({ error: "Request body must not be empty" }, { status: 400 });
  }

  const buffer = Buffer.from(body);

  // Compute SHA-256 of the content
  const sha256 = createHash("sha256").update(buffer).digest("hex");

  // Write blob to disk if not already present (content-addressable)
  const blobPath = db.getBlobPath(sha256);
  if (!existsSync(blobPath)) {
    const blobDir = dirname(blobPath);
    mkdirSync(blobDir, { recursive: true });
    writeFileSync(blobPath, buffer);
    logger.debug("blob written", { sha256, size: buffer.byteLength });
  }

  // Upsert manifest entry
  const entry: FileManifestEntry = {
    relpath,
    sha256,
    size: buffer.byteLength,
    mtime,
    machine_id: machineId,
    deleted: false,
  };

  db.upsertManifestEntry(entry);

  logger.info("file put", {
    relpath,
    sha256,
    size: buffer.byteLength,
    machine_id: machineId,
  });

  return Response.json(entry);
}

// ── DELETE /files/:path ─────────────────────────────────────────────

export function handleFileDelete(
  req: Request,
  db: LedgerDB,
  logger: Logger,
  relpath: string,
): Response {
  const pathError = validateRelpath(relpath);
  if (pathError) return pathError;

  const machineId = req.headers.get("X-Machine-ID");
  if (!machineId || machineId.length === 0) {
    return Response.json({ error: "X-Machine-ID header is required" }, { status: 400 });
  }

  // X-Mtime is now required for DELETE. Previously the server stamped the
  // tombstone with `Date.now()`, which always beat any client's "the file
  // was last seen at T" knowledge — letting a stale offline client tombstone
  // a file that another machine had updated more recently. The client now
  // supplies the wall-clock time at which it observed the deletion, and the
  // server applies the same LWW rule used for PUT.
  const mtimeRaw = req.headers.get("X-Mtime");
  if (!mtimeRaw) {
    return Response.json({ error: "X-Mtime header is required" }, { status: 400 });
  }
  const mtime = Number(mtimeRaw);
  if (!Number.isFinite(mtime) || mtime < 0) {
    return Response.json(
      { error: "X-Mtime must be a non-negative number (ms epoch)" },
      { status: 400 },
    );
  }

  const existing = db.getManifestEntry(relpath);

  // LWW guard — reject tombstones strictly older than the server's current
  // entry. Equal mtime is allowed so a re-sent tombstone is idempotent.
  if (existing && existing.mtime > mtime) {
    logger.info("file delete rejected (stale)", {
      relpath,
      incoming_mtime: mtime,
      server_mtime: existing.mtime,
      machine_id: machineId,
    });
    return Response.json(
      { error: "stale", server: existing },
      { status: 409 },
    );
  }

  const entry: FileManifestEntry = {
    relpath,
    // Preserve the previous content hash/size in the tombstone so clients
    // pulling the manifest can recognise *which* version was deleted.
    sha256: existing?.sha256 ?? "",
    size: existing?.size ?? 0,
    mtime,
    machine_id: machineId,
    deleted: true,
  };

  db.upsertManifestEntry(entry);

  logger.info("file deleted", { relpath, machine_id: machineId, mtime });

  return Response.json(entry);
}
