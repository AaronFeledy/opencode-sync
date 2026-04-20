import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { LedgerDB } from "../db.js";
import type { Logger } from "../log.js";
import {
  handleFileBlob,
  handleFileDelete,
  handleFilePut,
  handleFilesManifest,
} from "./files.js";

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

const tempDirs: string[] = [];

function createDataDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-sync-files-test-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop()!;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function putRequest(body: Uint8Array, headers: Record<string, string>): Request {
  return new Request("http://localhost/files/agents/foo.md", {
    method: "PUT",
    headers,
    body,
  });
}

function deleteRequest(headers: Record<string, string>): Request {
  return new Request("http://localhost/files/agents/foo.md", {
    method: "DELETE",
    headers,
  });
}

test("handleFilePut accepts a fresh upload and returns the manifest entry", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);
  const body = new TextEncoder().encode("hello world\n");

  const res = await handleFilePut(
    putRequest(body, { "X-Machine-ID": "desktop", "X-Mtime": "1000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(res.status).toBe(200);
  const entry = (await res.json()) as { mtime: number; deleted: boolean };
  expect(entry.mtime).toBe(1000);
  expect(entry.deleted).toBe(false);

  db.close();
});

test("handleFilePut rejects a strictly older upload with 409 and preserves server state", async () => {
  // Regression test for the file-sync LWW gap: previously the server would
  // unconditionally upsert any PUT, letting an offline client clobber a
  // newer file uploaded by another machine.
  const db = new LedgerDB(createDataDir(), silentLogger);
  const newer = new TextEncoder().encode("newer content\n");
  const older = new TextEncoder().encode("older content\n");

  // Newer upload at mtime=2000 from machine B.
  await handleFilePut(
    putRequest(newer, { "X-Machine-ID": "laptop", "X-Mtime": "2000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  // Stale upload at mtime=1000 from machine A — must be rejected.
  const staleRes = await handleFilePut(
    putRequest(older, { "X-Machine-ID": "desktop", "X-Mtime": "1000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(staleRes.status).toBe(409);
  const errBody = (await staleRes.json()) as {
    error: string;
    server: { mtime: number; machine_id: string };
  };
  expect(errBody.error).toBe("stale");
  expect(errBody.server.mtime).toBe(2000);
  expect(errBody.server.machine_id).toBe("laptop");

  // Server still has the newer entry; the stale PUT did not clobber it.
  const entry = db.getManifestEntry("agents/foo.md");
  expect(entry?.mtime).toBe(2000);
  expect(entry?.machine_id).toBe("laptop");

  db.close();
});

test("handleFilePut accepts an idempotent re-upload at the same mtime", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);
  const body = new TextEncoder().encode("same content\n");

  await handleFilePut(
    putRequest(body, { "X-Machine-ID": "desktop", "X-Mtime": "1000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  const res = await handleFilePut(
    putRequest(body, { "X-Machine-ID": "desktop", "X-Mtime": "1000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(res.status).toBe(200);

  db.close();
});

test("handleFileDelete requires X-Mtime header", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = handleFileDelete(
    deleteRequest({ "X-Machine-ID": "desktop" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toContain("X-Mtime");

  db.close();
});

test("handleFileDelete rejects a stale tombstone with 409", async () => {
  // Regression test for: previously DELETE used Date.now() server-side, so
  // a stale offline client's DELETE always beat a newer upload from another
  // machine, even if the upload happened seconds before the DELETE arrived.
  const db = new LedgerDB(createDataDir(), silentLogger);
  const body = new TextEncoder().encode("recent content\n");

  await handleFilePut(
    putRequest(body, { "X-Machine-ID": "laptop", "X-Mtime": "5000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  // Tombstone with mtime=3000 — older than the upload, so it must be rejected.
  const res = handleFileDelete(
    deleteRequest({ "X-Machine-ID": "desktop", "X-Mtime": "3000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(res.status).toBe(409);

  // The file is still NOT a tombstone — the upload survived.
  const entry = db.getManifestEntry("agents/foo.md");
  expect(entry?.deleted).toBe(false);
  expect(entry?.mtime).toBe(5000);

  db.close();
});

test("handleFileDelete accepts a tombstone newer than the existing entry", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);
  const body = new TextEncoder().encode("existing\n");

  await handleFilePut(
    putRequest(body, { "X-Machine-ID": "laptop", "X-Mtime": "1000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  const res = handleFileDelete(
    deleteRequest({ "X-Machine-ID": "desktop", "X-Mtime": "2000" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  expect(res.status).toBe(200);

  const entry = db.getManifestEntry("agents/foo.md");
  expect(entry?.deleted).toBe(true);
  expect(entry?.mtime).toBe(2000);
  // Tombstone preserves the previous sha256 so clients can match what was deleted.
  expect(entry?.sha256.length).toBeGreaterThan(0);

  db.close();
});

test("handleFilePut/Delete reject paths containing NUL bytes", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);
  const body = new TextEncoder().encode("x");

  // The router would normally extract relpath from the URL; we pass it
  // directly here to exercise the validation path.
  const putRes = await handleFilePut(
    new Request("http://localhost/files/x", {
      method: "PUT",
      headers: { "X-Machine-ID": "desktop", "X-Mtime": "1" },
      body,
    }),
    db,
    silentLogger,
    "agents/foo\0bar.md",
  );
  expect(putRes.status).toBe(400);

  const delRes = handleFileDelete(
    new Request("http://localhost/files/x", {
      method: "DELETE",
      headers: { "X-Machine-ID": "desktop", "X-Mtime": "1" },
    }),
    db,
    silentLogger,
    "agents/foo\0bar.md",
  );
  expect(delRes.status).toBe(400);

  db.close();
});

test("handleFilesManifest returns tombstones (deleted=true) so clients can replay deletions", async () => {
  // Regression test for the previous docstring claiming "non-deleted entries":
  // the server MUST return tombstones in the manifest, otherwise clients
  // would never see remote deletions.
  const db = new LedgerDB(createDataDir(), silentLogger);

  await handleFilePut(
    putRequest(new TextEncoder().encode("x"), { "X-Machine-ID": "desktop", "X-Mtime": "1" }),
    db,
    silentLogger,
    "agents/foo.md",
  );
  handleFileDelete(
    deleteRequest({ "X-Machine-ID": "desktop", "X-Mtime": "2" }),
    db,
    silentLogger,
    "agents/foo.md",
  );

  const res = handleFilesManifest(
    new Request("http://localhost/files/manifest"),
    db,
    silentLogger,
  );
  const manifest = (await res.json()) as Array<{ relpath: string; deleted: boolean }>;
  const entry = manifest.find((e) => e.relpath === "agents/foo.md");
  expect(entry).toBeDefined();
  expect(entry?.deleted).toBe(true);

  db.close();
});

test("handleFileBlob serves a previously-uploaded blob", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);
  const content = new TextEncoder().encode("hello blob\n");

  const putRes = await handleFilePut(
    putRequest(content, { "X-Machine-ID": "desktop", "X-Mtime": "1" }),
    db,
    silentLogger,
    "agents/foo.md",
  );
  const entry = (await putRes.json()) as { sha256: string };

  const blobRes = await handleFileBlob(
    new Request(`http://localhost/files/blob/${entry.sha256}`),
    db,
    silentLogger,
    entry.sha256,
  );

  expect(blobRes.status).toBe(200);
  expect(blobRes.headers.get("ETag")).toBe(`"${entry.sha256}"`);
  const got = new Uint8Array(await blobRes.arrayBuffer());
  expect(Buffer.from(got).toString("utf-8")).toBe("hello blob\n");

  db.close();
});
