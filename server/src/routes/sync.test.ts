import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { SyncEnvelope } from "@opencode-sync/shared";
import { LedgerDB } from "../db.js";
import type { Logger } from "../log.js";
import { handleSyncPush, handleSyncHeads, handleSyncPull } from "./sync.js";

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

function headsRequest(body: unknown): Request {
  return new Request("http://localhost/sync/heads", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

function pullRequest(since: number, limit?: number): Request {
  const url = new URL("http://localhost/sync/pull");
  url.searchParams.set("since", String(since));
  if (limit !== undefined) url.searchParams.set("limit", String(limit));
  return new Request(url.toString());
}

/**
 * Helper for /sync/heads tests: insert a few rows directly via push so
 * we have known head state to query.
 */
async function seedRows(db: LedgerDB, envelopes: SyncEnvelope[]): Promise<void> {
  const res = await handleSyncPush(
    pushRequest({ machine_id: "seed-machine", envelopes }),
    db,
    silentLogger,
  );
  if (res.status !== 200) {
    const body = await res.text();
    throw new Error(`seedRows push failed: ${res.status} ${body}`);
  }
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

test("handleSyncPull includes later parent dependencies without advancing cursor past the page", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  await seedRows(db, [
    {
      kind: "project",
      id: "proj_1",
      machine_id: "m1",
      time_updated: 1_000,
      server_seq: 0,
      deleted: false,
      data: {
        id: "proj_1",
        worktree: "/tmp/project",
        vcs: "git",
        name: "Project",
        icon_url: null,
        icon_color: null,
        time_created: 1,
        time_updated: 1_000,
        time_initialized: 1,
        sandboxes: "[]",
        commands: null,
      } as SyncEnvelope["data"],
    },
    {
      kind: "session",
      id: "ses_1",
      machine_id: "m1",
      time_updated: 2_000,
      server_seq: 0,
      deleted: false,
      data: {
        id: "ses_1",
        project_id: "proj_1",
        parent_id: null,
        slug: "s",
        directory: "/tmp/project",
        title: "Session",
        version: "1",
        share_url: null,
        summary_additions: null,
        summary_deletions: null,
        summary_files: null,
        summary_diffs: null,
        revert: null,
        permission: null,
        time_created: 1,
        time_updated: 2_000,
        time_compacting: null,
        time_archived: null,
        workspace_id: null,
      } as SyncEnvelope["data"],
    },
    {
      kind: "message",
      id: "msg_1",
      machine_id: "m1",
      time_updated: 3_000,
      server_seq: 0,
      deleted: false,
      data: {
        id: "msg_1",
        session_id: "ses_1",
        time_created: 1,
        time_updated: 3_000,
        data: "{}",
      } as SyncEnvelope["data"],
    },
    {
      kind: "part",
      id: "prt_1",
      machine_id: "m1",
      time_updated: 4_000,
      server_seq: 0,
      deleted: false,
      data: {
        id: "prt_1",
        message_id: "msg_1",
        session_id: "ses_1",
        time_created: 1,
        time_updated: 4_000,
        data: "{}",
      } as SyncEnvelope["data"],
    },
  ]);

  await seedRows(db, [
    {
      kind: "session",
      id: "ses_1",
      machine_id: "m2",
      time_updated: 5_000,
      server_seq: 0,
      deleted: false,
      data: {
        id: "ses_1",
        project_id: "proj_1",
        parent_id: null,
        slug: "s",
        directory: "/tmp/project",
        title: "Session renamed",
        version: "1",
        share_url: null,
        summary_additions: null,
        summary_deletions: null,
        summary_files: null,
        summary_diffs: null,
        revert: null,
        permission: null,
        time_created: 1,
        time_updated: 5_000,
        time_compacting: null,
        time_archived: null,
        workspace_id: null,
      } as SyncEnvelope["data"],
    },
  ]);

  const res = handleSyncPull(pullRequest(3, 1), db, silentLogger);
  expect(res.status).toBe(200);
  const body = (await res.json()) as { cursor_seq: number; dependency_closure: boolean; envelopes: SyncEnvelope[]; more: boolean };

  expect(body.cursor_seq).toBe(4);
  expect(body.dependency_closure).toBe(true);
  expect(body.more).toBe(true);
  expect(body.envelopes.map((e) => `${e.kind}:${e.id}`)).toEqual([
    "project:proj_1",
    "message:msg_1",
    "part:prt_1",
    "session:ses_1",
  ]);

  db.close();
});

// ── /sync/heads ────────────────────────────────────────────────────
//
// The heads endpoint backs the plugin's deletion-safety cross-check.
// Before tombstoning a row that's missing locally, the plugin asks the
// server "what's your head state for these row keys?" — a strictly-newer
// server head means another peer beat us to an update and we should
// pull rather than tombstone. Coverage focuses on the API contract
// (request validation, response shape, missing-row semantics) rather
// than re-litigating the LWW behaviour exercised in db.test.ts.

test("handleSyncHeads returns head entries for known rows and omits unknown ones", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  // Seed two rows; ask for three (one unknown).
  await seedRows(db, [
    {
      kind: "session",
      id: "ses_a",
      machine_id: "seed-machine",
      time_updated: 1_000,
      server_seq: 0,
      deleted: false,
      data: { id: "ses_a" } as SyncEnvelope["data"],
    },
    {
      kind: "session",
      id: "ses_b",
      machine_id: "seed-machine",
      time_updated: 2_000,
      server_seq: 0,
      deleted: true, // tombstone — should still be returned with deleted=true
      data: null,
    },
  ]);

  const res = await handleSyncHeads(
    headsRequest({
      machine_id: "m1",
      row_keys: [
        { kind: "session", id: "ses_a" },
        { kind: "session", id: "ses_b" },
        { kind: "session", id: "ses_unknown" },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(200);
  const body = (await res.json()) as { heads: Array<{ kind: string; id: string; time_updated: number; deleted: boolean }> };

  // Two heads, sorted for stability.
  const sorted = [...body.heads].sort((a, b) => a.id.localeCompare(b.id));
  expect(sorted).toEqual([
    { kind: "session", id: "ses_a", time_updated: 1_000, deleted: false },
    { kind: "session", id: "ses_b", time_updated: 2_000, deleted: true },
  ]);

  // ses_unknown is OMITTED — caller's contract is "absence == server
  // doesn't have it", which is the safe-to-tombstone signal.
  expect(body.heads.find((h) => h.id === "ses_unknown")).toBeUndefined();

  db.close();
});

test("handleSyncHeads accepts an empty row_keys array (vacuous OK)", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncHeads(
    headsRequest({ machine_id: "m1", row_keys: [] }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(200);
  const body = (await res.json()) as { heads: unknown[] };
  expect(body.heads).toEqual([]);

  db.close();
});

test("handleSyncHeads rejects an unknown kind", async () => {
  // Same defence as handleSyncPush: an unknown kind shouldn't make it
  // into the SQL builder. Reject at the API boundary.
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncHeads(
    headsRequest({
      machine_id: "m1",
      row_keys: [{ kind: "definitely_not_a_real_kind", id: "x" }],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/kind/);

  db.close();
});

test("handleSyncHeads rejects a row_keys batch larger than the cap", async () => {
  // Batch size cap protects against denial-of-service via huge IN-lists
  // and keeps SQLite parameter usage well under the default
  // SQLITE_MAX_VARIABLE_NUMBER. Verify the boundary.
  const db = new LedgerDB(createDataDir(), silentLogger);

  const oversized = Array.from({ length: 5001 }, (_, i) => ({
    kind: "session",
    id: `ses_${i}`,
  }));

  const res = await handleSyncHeads(
    headsRequest({ machine_id: "m1", row_keys: oversized }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/maximum batch/);

  db.close();
});

test("handleSyncHeads rejects missing or non-string machine_id", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncHeads(
    headsRequest({ row_keys: [] }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/machine_id/);

  db.close();
});

test("handleSyncHeads rejects non-array row_keys", async () => {
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncHeads(
    headsRequest({ machine_id: "m1", row_keys: "not an array" }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/row_keys/);

  db.close();
});

test("H4: handleSyncPush rejects batches exceeding PUSH_MAX_BATCH", async () => {
  // Protects the SQLite write transaction from being wedged by a
  // malicious or buggy client that tries to submit millions of
  // envelopes in one call. Legitimate clients use PUSH_BATCH_SIZE=100,
  // well below the 5000 cap. See FINDINGS.md H4.
  const db = new LedgerDB(createDataDir(), silentLogger);

  const envelopes = Array.from({ length: 5001 }, (_, i) => ({
    id: `s-${i}`,
    kind: "session",
    machine_id: "m1",
    time_updated: 1000 + i,
    deleted: false,
    data: {
      id: `s-${i}`,
      project_id: "p1",
      parent_id: null,
      slug: "s",
      directory: "/tmp",
      title: "T",
      version: "1",
      share_url: null,
      summary_additions: null,
      summary_deletions: null,
      summary_files: null,
      summary_diffs: null,
      revert: null,
      permission: null,
      time_created: 1,
      time_updated: 1000 + i,
      time_compacting: null,
      time_archived: null,
      workspace_id: null,
    },
  }));

  const res = await handleSyncPush(
    pushRequest({ machine_id: "m1", envelopes }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/maximum batch size/);

  db.close();
});

test("M8: handleSyncPush rejects time_updated <= 0", async () => {
  // Tightened from `< 0` to `<= 0` so zero can't poison LWW. Plugin
  // enforces the same. See FINDINGS.md M8.
  const db = new LedgerDB(createDataDir(), silentLogger);

  const res = await handleSyncPush(
    pushRequest({
      machine_id: "m1",
      envelopes: [
        {
          id: "s1",
          kind: "session",
          machine_id: "m1",
          time_updated: 0,
          deleted: false,
          data: {},
        },
      ],
    }),
    db,
    silentLogger,
  );

  expect(res.status).toBe(400);
  const body = (await res.json()) as { error: string };
  expect(body.error).toMatch(/positive/);

  db.close();
});
