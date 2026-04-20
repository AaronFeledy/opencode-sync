import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { LedgerDB } from "../db.js";
import type { Logger } from "../log.js";
import { handleSyncPush } from "./sync.js";

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

const tempDirs: string[] = [];

function createDataDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-sync-routes-test-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop()!;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function pushRequest(body: unknown): Request {
  return new Request("http://localhost/sync/push", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// Regression: handleSyncPush used to validate only `!envelope.id ||
// !envelope.kind`. An authenticated client could send `kind: "anything"`
// and the server would persist it (sync_row has no CHECK on kind), poisoning
// every peer's pull — applyEnvelope would then dereference TABLE_COLUMNS[kind]
// → undefined → TypeError on each pull. Reject malformed envelopes at the
// API boundary so the ledger never accepts them.

test("handleSyncPush rejects an envelope with an unknown kind", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncPush(
    pushRequest({
      machine_id: "m1",
      envelopes: [
        {
          id: "x",
          kind: "definitely_not_a_real_kind",
          machine_id: "m1",
          time_updated: 1,
          server_seq: 0,
          deleted: false,
          data: { id: "x" },
        },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/envelope\.kind/);

  // Critically: nothing was persisted.
  const pulled = db.pullRows(0, undefined, 100);
  expect(pulled.envelopes).toHaveLength(0);

  db.close();
});

test("handleSyncPush rejects envelopes with non-numeric time_updated", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncPush(
    pushRequest({
      machine_id: "m1",
      envelopes: [
        {
          id: "x",
          kind: "session",
          machine_id: "m1",
          time_updated: "not a number",
          server_seq: 0,
          deleted: false,
          data: { id: "x" },
        },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/time_updated/);

  db.close();
});

test("handleSyncPush rejects envelopes with non-string machine_id", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncPush(
    pushRequest({
      machine_id: "m1",
      envelopes: [
        {
          id: "x",
          kind: "session",
          machine_id: null,
          time_updated: 1,
          server_seq: 0,
          deleted: false,
          data: { id: "x" },
        },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/machine_id/);

  db.close();
});

test("handleSyncPush rejects envelopes with non-boolean deleted flag", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncPush(
    pushRequest({
      machine_id: "m1",
      envelopes: [
        {
          id: "x",
          kind: "session",
          machine_id: "m1",
          time_updated: 1,
          server_seq: 0,
          deleted: "true",
          data: { id: "x" },
        },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/deleted/);

  db.close();
});
