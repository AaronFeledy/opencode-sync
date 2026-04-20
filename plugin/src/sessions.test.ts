import { afterEach, beforeEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { PullResponse, SyncEnvelope, SyncKind } from "@opencode-sync/shared";
import type { SyncClient } from "./client.js";
import { DbReader } from "./db-read.js";
import { DbWriter } from "./db-write.js";
import { SessionSync } from "./sessions.js";
import { StateManager } from "./state.js";

// Module-level guard — throws at import time if HOME is not a test sandbox.
// State files live under ~/.local/share/opencode/opencode-sync/, so a wrong
// HOME would clobber the developer's real sync state. This runs BEFORE any
// test body, so a misconfigured HOME can never reach a destructive call.
const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load session sync tests against non-test HOME: ${HOME}. ` +
      `Run via the workspace 'bun test' script (which sets HOME), or export ` +
      `HOME to a directory containing 'opencode-sync-test-home'.`,
  );
}

function createDbPath(): string {
  return path.join(os.tmpdir(), `opencode-sync-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`);
}

function initDb(dbPath: string): void {
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.exec(`
    CREATE TABLE project (
      id TEXT PRIMARY KEY,
      worktree TEXT NOT NULL,
      vcs TEXT,
      name TEXT,
      icon_url TEXT,
      icon_color TEXT,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      time_initialized INTEGER,
      sandboxes TEXT NOT NULL,
      commands TEXT
    );

    CREATE TABLE session (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
      parent_id TEXT,
      slug TEXT NOT NULL,
      directory TEXT NOT NULL,
      title TEXT NOT NULL,
      version TEXT NOT NULL,
      share_url TEXT,
      summary_additions INTEGER,
      summary_deletions INTEGER,
      summary_files INTEGER,
      summary_diffs TEXT,
      revert TEXT,
      permission TEXT,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      time_compacting INTEGER,
      time_archived INTEGER,
      workspace_id TEXT
    );

    CREATE TABLE message (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      data TEXT NOT NULL
    );

    CREATE TABLE part (
      id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL REFERENCES message(id) ON DELETE CASCADE,
      session_id TEXT NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      data TEXT NOT NULL
    );

    CREATE TABLE todo (
      session_id TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
      content TEXT NOT NULL,
      status TEXT NOT NULL,
      priority TEXT NOT NULL,
      position INTEGER NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      PRIMARY KEY (session_id, position)
    );

    CREATE TABLE permission (
      project_id TEXT PRIMARY KEY REFERENCES project(id) ON DELETE CASCADE,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      data TEXT NOT NULL
    );

    CREATE TABLE session_share (
      session_id TEXT PRIMARY KEY REFERENCES session(id) ON DELETE CASCADE,
      id TEXT NOT NULL,
      secret TEXT NOT NULL,
      url TEXT NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL
    );
  `);
  db.close();
}

function seedSessionTree(dbPath: string, sessionUpdatedAt: number = 1_000): void {
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");

  db.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_1", "/tmp/project", "git", "Test", null, null, 1, 1, 1, "[]", null],
  );
  db.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      "ses_1",
      "proj_1",
      null,
      "session-1",
      "/tmp/project",
      "Session 1",
      "1",
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      1,
      sessionUpdatedAt,
      null,
      null,
      null,
    ],
  );
  db.run(
    "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
    ["msg_1", "ses_1", 1, sessionUpdatedAt, '{"role":"user"}'],
  );
  db.run(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ["part_1", "msg_1", "ses_1", 1, sessionUpdatedAt, '{"type":"text"}'],
  );
  db.run(
    "INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
    ["ses_1", "Fix bug", "pending", "high", 0, 1, sessionUpdatedAt],
  );

  db.close();
}

function countRows(dbPath: string, table: string): number {
  const db = new Database(dbPath, { readonly: true });
  const row = db.query<{ count: number }, []>(`SELECT COUNT(*) AS count FROM ${table}`).get();
  db.close();
  return row?.count ?? 0;
}

class MockClient {
  pushes: SyncEnvelope[][] = [];
  pullResponses: PullResponse[] = [];
  pullCalls: Array<{ since: number; exclude?: string }> = [];

  async push(_machineId: string, envelopes: SyncEnvelope[]) {
    this.pushes.push(envelopes.map((envelope) => ({ ...envelope })));
    return {
      server_seq: this.pushes.length,
      accepted: envelopes.map((envelope) => envelope.id),
      stale: [],
    };
  }

  async pull(since: number = 0, exclude?: string): Promise<PullResponse> {
    this.pullCalls.push({ since, exclude });
    return this.pullResponses.shift() ?? { server_seq: 0, envelopes: [], more: false };
  }
}

beforeEach(() => {
  if (!os.homedir().includes("opencode-sync-test-home")) {
    throw new Error(`HOME changed mid-suite to a non-test path: ${os.homedir()}`);
  }
});

afterEach(() => {
  fs.rmSync(path.join(HOME, ".local", "share", "opencode", "opencode-sync"), {
    recursive: true,
    force: true,
  });
});

test("pushAll emits tombstones for rows deleted locally", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const client = new MockClient();
  const stateManager = new StateManager("desktop");
  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pushAll();

  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();

  await sync.pushAll();

  const tombstones = client.pushes[1] ?? [];
  expect(tombstones.every((envelope) => envelope.deleted)).toBe(true);
  expect(tombstones.map((envelope) => `${envelope.kind}:${envelope.id}`).sort()).toEqual([
    "message:msg_1",
    "part:part_1",
    "session:ses_1",
    "todo:ses_1:0",
  ]);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull keeps a newer local row when the remote tombstone is older", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath, 5_000);

  const client = new MockClient();
  client.pullResponses = [{
    server_seq: 1,
    more: false,
    envelopes: [{
      kind: "session",
      id: "ses_1",
      machine_id: "laptop",
      time_updated: 4_000,
      server_seq: 1,
      deleted: true,
      data: null,
    }],
  }];

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    new StateManager("desktop"),
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  expect(result.conflicts).toBe(1);
  expect(countRows(dbPath, "session")).toBe(1);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pulling a session tombstone cascades to dependent rows", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath, 1_000);

  const client = new MockClient();
  client.pullResponses = [{
    server_seq: 1,
    more: false,
    envelopes: [{
      kind: "session",
      id: "ses_1",
      machine_id: "laptop",
      time_updated: 6_000,
      server_seq: 1,
      deleted: true,
      data: null,
    }],
  }];

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    new StateManager("desktop"),
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  expect(result.applied).toBe(1);
  expect(countRows(dbPath, "session")).toBe(0);
  expect(countRows(dbPath, "message")).toBe(0);
  expect(countRows(dbPath, "part")).toBe(0);
  expect(countRows(dbPath, "todo")).toBe(0);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull paginates by batch max server_seq, not by global server_seq", async () => {
  // Regression test for: when a backlog exceeds the page limit, the cursor
  // must advance to the last envelope's server_seq in the batch — NOT to
  // res.server_seq (the server's global high-watermark). Otherwise the
  // second pull request would skip every row between the batch tail and
  // the global max.
  const dbPath = createDbPath();
  initDb(dbPath);
  // Seed an existing session that the batch tombstone targets, so applyEnvelope
  // has a row to operate on. (Tombstones with no local row are still applied.)
  seedSessionTree(dbPath, 1_000);

  const client = new MockClient();
  const stateManager = new StateManager("desktop");

  // First page: returns one envelope at server_seq=3, but the global max
  // (res.server_seq) is 100 — simulating a backlog far larger than the batch.
  client.pullResponses.push({
    server_seq: 100,
    more: true,
    envelopes: [{
      kind: "session",
      id: "ses_1",
      machine_id: "laptop",
      time_updated: 9_000,
      server_seq: 3,
      deleted: true,
      data: null,
    }],
  });
  // Second page: returns nothing more; server_seq=100 (global max).
  client.pullResponses.push({
    server_seq: 100,
    more: false,
    envelopes: [],
  });

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pull();

  // Two pull calls were made (because more=true on the first).
  expect(client.pullCalls.length).toBe(2);
  // First call started at since=0 (initial state).
  expect(client.pullCalls[0]!.since).toBe(0);
  // Second call MUST start at since=3 (the batch's max), not since=100.
  // If this asserts 100, the bug is back and rows 4..99 would be silently skipped.
  expect(client.pullCalls[1]!.since).toBe(3);
  // After the loop drains (more=false on second page with empty envelopes),
  // the cursor advances to the global high-watermark.
  expect(stateManager.state.lastPulledSeq).toBe(100);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull does not count an idempotent no-op as a conflict", async () => {
  // Regression test for #5: when the local row's time_updated is EQUAL
  // to the incoming envelope's, applyEnvelope should report "skipped" and
  // pull() should not increment the conflict counter.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath, 5_000);

  const client = new MockClient();
  client.pullResponses = [{
    server_seq: 1,
    more: false,
    envelopes: [{
      kind: "session",
      id: "ses_1",
      machine_id: "laptop",
      // Same time_updated as the local row — pure no-op.
      time_updated: 5_000,
      server_seq: 1,
      deleted: false,
      data: {
        id: "ses_1",
        project_id: "proj_1",
        parent_id: null,
        slug: "session-1",
        directory: "/tmp/project",
        title: "Session 1",
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
      },
    }],
  }];

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    new StateManager("desktop"),
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  // Equal time_updated → "skipped" → neither applied nor a conflict.
  expect(result.applied).toBe(0);
  expect(result.conflicts).toBe(0);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pushAll uses the time-based cursor on the second run", async () => {
  // Regression test for #3: after a successful pushAll, the next pushAll
  // should advance lastPushedRowTime so subsequent reads can filter out
  // already-pushed rows. Without this, every periodic sync would re-read
  // every row in every table.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath, 1_000);

  const client = new MockClient();
  const stateManager = new StateManager("desktop");
  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  expect(stateManager.state.lastPushedRowTime).toBe(0);

  await sync.pushAll();

  // After first pushAll, the cursor advanced to the max time_updated of
  // any row we just sent (the seed sets all times to 1_000).
  expect(stateManager.state.lastPushedRowTime).toBe(1_000);

  // Second pushAll with no new local data: still nothing to push.
  client.pushes = [];
  await sync.pushAll();
  expect(client.pushes.length).toBe(0);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull persists state once per call rather than per-page", async () => {
  // Regression test for #10: with withBatch wrapping pull, paginated pulls
  // must call save() exactly once at the end, regardless of how many pages
  // were processed (and how many rememberRows/forgetRows/updateSeq calls
  // happened along the way).
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath, 1_000);

  const client = new MockClient();
  const stateManager = new StateManager("desktop");

  // Three pages, each applying a tombstone for a different (already-absent)
  // row so we exercise both rememberRows and forgetRows paths.
  for (let page = 1; page <= 3; page++) {
    client.pullResponses.push({
      server_seq: page * 10,
      more: page < 3,
      envelopes: [{
        kind: "session",
        id: `ses_remote_${page}`,
        machine_id: "laptop",
        time_updated: 9_000 + page,
        server_seq: page * 10,
        deleted: true,
        data: null,
      }],
    });
  }

  let saveCount = 0;
  const originalSave = stateManager.save.bind(stateManager);
  stateManager.save = (...args: Parameters<typeof originalSave>) => {
    saveCount++;
    return originalSave(...args);
  };

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pull();

  // Without batching, this would be 3+ saves (one per page-mutation).
  // With withBatch, exactly one save at the end of pull().
  expect(saveCount).toBe(1);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull breaks out of the loop when server returns more=true with empty envelopes", async () => {
  // Defensive test: the current server can't produce {envelopes:[], more:true},
  // but if it ever did (proxy bug, future server change), pullInternal would
  // request the same `since` cursor forever. Verify we break instead of looping.
  const dbPath = createDbPath();
  initDb(dbPath);

  const client = new MockClient();
  // Pathological response: claims more pages exist but returns nothing.
  client.pullResponses.push({
    server_seq: 50,
    more: true,
    envelopes: [],
  });
  // If the loop didn't break, a subsequent identical response would be
  // requested. We push a second one as well; the test then asserts only ONE
  // pull call happened.
  client.pullResponses.push({
    server_seq: 50,
    more: true,
    envelopes: [],
  });

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const stateManager = new StateManager("desktop");
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pull();

  // Exactly one pull — we broke out, did not loop.
  expect(client.pullCalls.length).toBe(1);
  // Cursor was NOT advanced past the unconsumed range (since the page held
  // no envelopes we trust). Next sync will retry from the same cursor.
  expect(stateManager.state.lastPulledSeq).toBe(0);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("DbWriter.applyEnvelope returns 'error' for an unknown kind without throwing", async () => {
  // Regression: applyEnvelope used to dereference TABLE_COLUMNS[kind] and
  // PK_COLUMNS[kind] unconditionally. For an envelope with a kind we don't
  // know about (e.g. an older client pulling rows of a future kind, or a
  // misbehaving server), both lookups returned undefined and the next
  // `.map(...)`/`.length` access threw TypeError. The throw escaped the
  // method entirely, violating the documented `ApplyResult` contract that
  // says failures should return "error".
  //
  // pullInternal happens to wrap applyEnvelope in a defensive try/catch, so
  // the throw was previously caught and counted as an error there too —
  // but anything else calling applyEnvelope directly would crash. Test the
  // contract at its proper boundary.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

  const writer = new DbWriter(dbPath);

  let result: unknown;
  expect(() => {
    result = writer.applyEnvelope({
      kind: "definitely_not_a_real_kind" as SyncKind,
      id: "x",
      machine_id: "laptop",
      time_updated: 1,
      server_seq: 1,
      deleted: false,
      data: null,
    });
  }).not.toThrow();
  expect(result).toBe("error");

  // Same path on the deletion branch (different code path inside applyEnvelope
  // — deleteRow vs the upsert path — so worth exercising both).
  expect(() => {
    result = writer.applyEnvelope({
      kind: "definitely_not_a_real_kind" as SyncKind,
      id: "x",
      machine_id: "laptop",
      time_updated: 1,
      server_seq: 1,
      deleted: true,
      data: null,
    });
  }).not.toThrow();
  expect(result).toBe("error");

  console.error = originalError;
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull surfaces apply errors via the returned `errors` count", async () => {
  // Regression: previously applyEnvelope's "error" results were silently
  // swallowed and the pull cursor advanced past them — making it impossible
  // to detect drift from the orchestrator. Verify the count is now exposed.
  const dbPath = createDbPath();
  initDb(dbPath);

  // db-write.ts logs SQL errors via console.error before returning "error".
  // Silence that for the duration of this test so the suite output stays
  // clean — we deliberately trigger an FK violation below.
  const originalError = console.error;
  console.error = () => {};

  const client = new MockClient();
  // Two envelopes: one valid (applies cleanly), one referencing a
  // non-existent project_id (FK violation → applyEnvelope returns "error").
  client.pullResponses.push({
    server_seq: 2,
    more: false,
    envelopes: [
      {
        kind: "session",
        id: "ses_orphan",
        machine_id: "laptop",
        time_updated: 9_000,
        server_seq: 2,
        deleted: false,
        data: {
          id: "ses_orphan",
          // project_id intentionally references a row that doesn't exist
          // locally — FK with ON DELETE CASCADE means INSERT will fail.
          project_id: "proj_does_not_exist",
          parent_id: null,
          slug: "orphan",
          directory: "/tmp",
          title: "Orphan",
          version: "1",
          share_url: null,
          summary_additions: null,
          summary_deletions: null,
          summary_files: null,
          summary_diffs: null,
          revert: null,
          permission: null,
          time_created: 1,
          time_updated: 9_000,
          time_compacting: null,
          time_archived: null,
          workspace_id: null,
        },
      },
    ],
  });

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    new StateManager("desktop"),
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  expect(result.errors).toBe(1);
  expect(result.applied).toBe(0);
  expect(result.conflicts).toBe(0);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull does NOT advance the cursor past an errored envelope", async () => {
  // Regression: previously the cursor advanced to the page-tail seq even
  // when envelopes errored, so transient FK/BUSY failures became permanent
  // data loss. The cursor should now stay before the first error so the
  // next pull cycle re-requests the failed envelope.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

  const client = new MockClient();
  client.pullResponses.push({
    server_seq: 5,
    more: false,
    envelopes: [
      {
        kind: "session",
        id: "ses_orphan",
        machine_id: "laptop",
        time_updated: 9_000,
        server_seq: 5,
        deleted: false,
        data: {
          id: "ses_orphan",
          project_id: "proj_does_not_exist",
          parent_id: null,
          slug: "orphan",
          directory: "/tmp",
          title: "Orphan",
          version: "1",
          share_url: null,
          summary_additions: null,
          summary_deletions: null,
          summary_files: null,
          summary_diffs: null,
          revert: null,
          permission: null,
          time_created: 1,
          time_updated: 9_000,
          time_compacting: null,
          time_archived: null,
          workspace_id: null,
        },
      },
    ],
  });

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const stateManager = new StateManager("desktop");
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  expect(result.errors).toBe(1);
  // Cursor stays at 0 — NOT advanced to 5 (the errored envelope's seq) or
  // to res.server_seq=5. Next pull cycle will re-request from since=0.
  expect(stateManager.state.lastPulledSeq).toBe(0);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pull advances cursor only to last successful seq before an error", async () => {
  // Regression: when a page contains a successful prefix followed by an
  // error, the cursor should advance past the prefix but stop at the
  // last-good seq — pagination halts so the failed envelope is retried
  // next cycle without skipping anything in between.
  const dbPath = createDbPath();
  initDb(dbPath);

  // Seed a project so the first envelope's session insert succeeds.
  const seedDb = new Database(dbPath);
  seedDb.exec("PRAGMA foreign_keys = ON");
  seedDb.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_existing", "/tmp", "git", "Existing", null, null, 1, 1, 1, "[]", null],
  );
  seedDb.close();

  const originalError = console.error;
  console.error = () => {};

  const client = new MockClient();
  client.pullResponses.push({
    server_seq: 20,
    more: true, // claim more pages exist — we'll verify pagination STOPS
    envelopes: [
      // Envelope A — succeeds (project exists, FK satisfied).
      {
        kind: "session",
        id: "ses_good",
        machine_id: "laptop",
        time_updated: 9_000,
        server_seq: 10,
        deleted: false,
        data: {
          id: "ses_good", project_id: "proj_existing", parent_id: null,
          slug: "good", directory: "/tmp", title: "Good", version: "1",
          share_url: null, summary_additions: null, summary_deletions: null,
          summary_files: null, summary_diffs: null, revert: null, permission: null,
          time_created: 1, time_updated: 9_000, time_compacting: null,
          time_archived: null, workspace_id: null,
        },
      },
      // Envelope B — errors (FK violation).
      {
        kind: "session",
        id: "ses_orphan",
        machine_id: "laptop",
        time_updated: 9_500,
        server_seq: 15,
        deleted: false,
        data: {
          id: "ses_orphan", project_id: "proj_does_not_exist", parent_id: null,
          slug: "orphan", directory: "/tmp", title: "Orphan", version: "1",
          share_url: null, summary_additions: null, summary_deletions: null,
          summary_files: null, summary_diffs: null, revert: null, permission: null,
          time_created: 1, time_updated: 9_500, time_compacting: null,
          time_archived: null, workspace_id: null,
        },
      },
      // Envelope C — would succeed if reached, but cursor advancement
      // logic should still allow it to apply (we process the whole page
      // for accurate counts) — it just won't advance the cursor past it.
      {
        kind: "session",
        id: "ses_after_error",
        machine_id: "laptop",
        time_updated: 9_900,
        server_seq: 20,
        deleted: false,
        data: {
          id: "ses_after_error", project_id: "proj_existing", parent_id: null,
          slug: "after", directory: "/tmp", title: "After", version: "1",
          share_url: null, summary_additions: null, summary_deletions: null,
          summary_files: null, summary_diffs: null, revert: null, permission: null,
          time_created: 1, time_updated: 9_900, time_compacting: null,
          time_archived: null, workspace_id: null,
        },
      },
    ],
  });

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const stateManager = new StateManager("desktop");
  const sync = new SessionSync(
    reader,
    writer,
    client as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  const result = await sync.pull();

  // Two applies (A and C), one error (B).
  expect(result.applied).toBe(2);
  expect(result.errors).toBe(1);
  // Cursor advances only to A's seq (10) — NOT to C's (20) or page tail.
  // Next pull will re-request since=10, getting B and C again. C is
  // idempotent on the time_updated equality check, B retries.
  expect(stateManager.state.lastPulledSeq).toBe(10);
  // Pagination stopped despite more=true: only one pull call made.
  expect(client.pullCalls.length).toBe(1);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("advancePushedRowTime clamps to Date.now() to defend against forward clock skew", async () => {
  // Regression: a peer with a forward-skewed clock (or a tombstone
  // stamped via Math.max(Date.now(), prev + 1) where prev came from such
  // a peer) could push lastPushedRowTime arbitrarily into the future.
  // Combined with pushReadSince's 60s margin, that would silently filter
  // newly-written local rows out of the delta read until wall clock
  // caught up — potentially hours.
  const stateManager = new StateManager("desktop");

  // Simulate a tombstone stamped 1 hour in the future by a clock-skewed peer.
  const oneHourAhead = Date.now() + 60 * 60 * 1000;
  stateManager.advancePushedRowTime(oneHourAhead);

  // Cursor is clamped to wall clock — NOT pushed an hour into the future.
  expect(stateManager.state.lastPushedRowTime).toBeLessThanOrEqual(Date.now());
  expect(stateManager.state.lastPushedRowTime).toBeGreaterThan(0);

  // pushReadSince stays within sensible bounds (clamp - 60s margin).
  const since = stateManager.pushReadSince();
  expect(since).toBeLessThan(Date.now());
  expect(since).toBeGreaterThanOrEqual(0);

  // Sanity: a normal (non-skewed) advance still works as before.
  const past = Date.now() - 5_000;
  // Past values shouldn't move the cursor backward (still monotonic).
  const before = stateManager.state.lastPushedRowTime;
  stateManager.advancePushedRowTime(past);
  expect(stateManager.state.lastPushedRowTime).toBe(before);
});
