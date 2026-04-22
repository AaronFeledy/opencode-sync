import { afterEach, beforeEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { HeadEntry, PullResponse, SyncEnvelope, SyncKind } from "@opencode-sync/shared";
import { EndpointMissingError, type SyncClient } from "./client.js";
import { DbReader } from "./db-read.js";
import { DbWriter } from "./db-write.js";
import { SessionSync, parseRowStateKey } from "./sessions.js";
import { StateManager } from "./state.js";
import { isSyncHalted, clearHaltMarker, readHaltDetails, HALT_MARKER_PATH } from "./halt.js";

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

// Mirrors the production constant `TOMBSTONE_THRESHOLD_MIN_KNOWN` from
// sessions.ts. Kept here (not imported) so tests don't grow a dependency
// on a private module export. If this drifts from the production value
// the tests asserting "knownRows is well above the threshold floor"
// could become trivially true; review both together if either changes.
const TOMBSTONE_MIN_KNOWN_FOR_TEST = 50;

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

  /**
   * Heads-endpoint behaviour control. Default = empty Map (server has
   * no record of any candidate, so cross-check is permissive). Tests
   * that want to simulate "server has a newer version" set entries
   * here. Tests that want to simulate "server doesn't expose
   * /sync/heads" set `headsThrowEndpointMissing = true`. Tests that
   * want to simulate a transient network blip set
   * `headsThrowError = true`.
   */
  headsResponses: Map<string, HeadEntry> = new Map();
  headsThrowEndpointMissing = false;
  headsThrowError = false;
  headsCalls: Array<Array<{ kind: SyncKind; id: string }>> = [];

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

  async getHeads(
    _machineId: string,
    rowKeys: Array<{ kind: SyncKind; id: string }>,
  ): Promise<Map<string, HeadEntry>> {
    this.headsCalls.push(rowKeys);
    if (this.headsThrowEndpointMissing) {
      throw new EndpointMissingError("POST", "/sync/heads");
    }
    if (this.headsThrowError) {
      throw new Error("simulated network blip");
    }
    // Return the configured subset that overlaps with the request.
    const result = new Map<string, HeadEntry>();
    for (const { kind, id } of rowKeys) {
      const key = `${kind}:${id}`;
      const head = this.headsResponses.get(key);
      if (head) result.set(key, head);
    }
    return result;
  }
}

beforeEach(() => {
  if (!os.homedir().includes("opencode-sync-test-home")) {
    throw new Error(`HOME changed mid-suite to a non-test path: ${os.homedir()}`);
  }
});

afterEach(() => {
  // Clear deletion-safety halt marker if a test left one behind (the
  // marker is sticky by design — survives plugin restart — so without
  // this, a single tripped test would block every subsequent test).
  clearHaltMarker();
  fs.rmSync(path.join(HOME, ".local", "share", "opencode", "opencode-sync"), {
    recursive: true,
    force: true,
  });
});

test("pushAll defers tombstones for unexpected deletions until the confirmation window elapses", async () => {
  // After the deletion-safety refactor, any row that disappears
  // WITHOUT being marked via markExpectedDeletion must spend at least
  // TOMBSTONE_CONFIRMATION_DELAY_MS (30s) in the pending buffer before
  // being tombstoned. This protects against transient mid-migration /
  // mid-restore states where rows briefly disappear and reappear.
  //
  // We exercise this by mocking Date.now to step past the window
  // without sleeping the test.
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

  // Cycle 1: seed knownRows with the live data.
  await sync.pushAll();
  expect(client.pushes[0]?.every((env) => !env.deleted)).toBe(true);

  // Out-of-band local deletion (simulating opencode wiping a session
  // through a path that didn't fire a session.deleted event we caught).
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();

  // Cycle 2: candidates land in pendingTombstones, NO tombstones emitted.
  client.pushes = [];
  await sync.pushAll();
  expect(client.pushes).toEqual([]);
  expect(Object.keys(stateManager.state.pendingTombstones).sort()).toEqual([
    "message:msg_1",
    "part:part_1",
    "session:ses_1",
    "todo:ses_1:0",
  ]);

  // Advance wall clock past the 30s confirmation window.
  const realNow = Date.now;
  const future = realNow() + 60_000;
  Date.now = () => future;
  try {
    // Cycle 3: confirmation window elapsed, tombstones emitted.
    client.pushes = [];
    await sync.pushAll();
    const tombstones = client.pushes[0] ?? [];
    expect(tombstones.every((env) => env.deleted)).toBe(true);
    expect(tombstones.map((env) => `${env.kind}:${env.id}`).sort()).toEqual([
      "message:msg_1",
      "part:part_1",
      "session:ses_1",
      "todo:ses_1:0",
    ]);
    // Pending buffer was drained for the rows we just tombstoned.
    expect(Object.keys(stateManager.state.pendingTombstones)).toEqual([]);
  } finally {
    Date.now = realNow;
  }

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pushAll fast-paths tombstones when the row is marked as an expected deletion", async () => {
  // The session.deleted hook calls markExpectedDeletion before invoking
  // pushAll. Marked rows skip BOTH the server cross-check and the
  // two-cycle confirmation — they're emitted on the same cycle the
  // deletion is observed.
  //
  // markExpectedDeletion('session:X') also auto-expands to include the
  // session's todos (which share the prefix `todo:X:`) and any
  // session_share — verifies the cascade-helper logic works.
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

  // Seed knownRows.
  await sync.pushAll();

  // Delete locally, mark as expected.
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();
  sync.markExpectedDeletion("session:ses_1");

  // Cycle 2: every cascade child of the deleted session should fast-
  // path through the tombstone pipeline. After M1 added the rowParents
  // secondary index, messages/parts/todos are all discovered via their
  // parent session and flow through on the same cycle as the session
  // itself (session_share isn't seeded by seedSessionTree so it's not
  // in this list). Before M1, messages and parts were stuck in
  // pendingTombstones awaiting two-cycle confirmation.
  client.pushes = [];
  await sync.pushAll();
  const fastPath = (client.pushes[0] ?? []).map((e) => `${e.kind}:${e.id}`).sort();
  expect(fastPath).toEqual([
    "message:msg_1",
    "part:part_1",
    "session:ses_1",
    "todo:ses_1:0",
  ]);

  // With full cascade-expansion there should be NO pending entries
  // left for this session's descendants.
  expect(Object.keys(stateManager.state.pendingTombstones)).toEqual([]);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pushAll halts when unexpected tombstones exceed the threshold (DB wipe scenario)", async () => {
  // The catastrophic scenario: opencode.db is wiped/restored/replaced,
  // state.json still has hundreds of knownRows. Without the threshold
  // guard, buildDeletionEnvelopes would tombstone the entire fleet's
  // data. WITH the guard, sync halts and writes a marker file the
  // user must manually clear after investigating.
  const dbPath = createDbPath();
  initDb(dbPath);

  // Seed a knownRows population large enough to trigger the threshold
  // (TOMBSTONE_THRESHOLD_MIN_KNOWN = 50). Use the seed helper N times
  // with distinct project/session ids.
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_big", "/tmp/big", "git", "Big", null, null, 1, 1, 1, "[]", null],
  );
  for (let i = 0; i < 100; i++) {
    db.run(
      `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [`ses_${i}`, "proj_big", null, `s${i}`, "/tmp/big", `S${i}`, "1", null, null, null, null, null, null, null, 1, 1_000 + i, null, null, null],
    );
  }
  db.close();

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

  // Cycle 1: seed knownRows.
  await sync.pushAll();
  expect(Object.keys(stateManager.state.knownRows).length).toBeGreaterThan(50);

  // Wipe the DB out from under us — simulates `rm opencode.db` or a
  // restore-from-backup that replaced the file with an empty one.
  const wipe = new Database(dbPath);
  wipe.exec("DELETE FROM session; DELETE FROM project;");
  wipe.close();

  // Cycle 2: should halt.
  client.pushes = [];
  await sync.pushAll();

  // No tombstones emitted; halt marker present; halted flag set.
  expect(client.pushes.flat().filter((e) => e.deleted)).toEqual([]);
  expect(isSyncHalted()).toBe(true);
  expect(sync.isHalted()).toBe(true);

  // Marker file contents include the threshold reason.
  const details = readHaltDetails();
  expect(details).not.toBeNull();
  expect(details!.reason).toBe("tombstone_threshold");
  expect(details!.candidateCount).toBeGreaterThan(50);
  expect(details!.sampleCandidates?.length).toBeGreaterThan(0);

  // Subsequent pushAll calls remain a no-op even after the threshold
  // condition would technically resolve — only manual marker removal
  // re-enables push.
  client.pushes = [];
  await sync.pushAll();
  expect(client.pushes).toEqual([]);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
  fs.rmSync(HALT_MARKER_PATH, { force: true });
});

test("threshold is permissive (95%) when at least one expected deletion is present", async () => {
  // Adaptive threshold: when the user is actively deleting things
  // (session.deleted hook fired → markExpectedDeletion was called),
  // we trust them more and only halt on near-total disappearance
  // (>95%). Otherwise a single "delete this big session" action with
  // hundreds of cascade message/part rows would trip the strict 50%
  // threshold even though nothing is wrong.
  //
  // To distinguish strict-vs-permissive we need an unexpected fraction
  // BETWEEN 50% and 95% — a single huge session deletion produces
  // ~99% which would halt under both. Layout: two sessions, one big
  // (101 rows including cascade) gets deleted, one smaller (61 rows)
  // stays live. 101 / 163 ≈ 62% — strict would halt, permissive does
  // not.
  const dbPath = createDbPath();
  initDb(dbPath);

  const seed = new Database(dbPath);
  seed.exec("PRAGMA foreign_keys = ON");
  seed.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_y", "/tmp/y", "git", "Y", null, null, 1, 1, 1, "[]", null],
  );
  // Session A: deleted later (1 + 50msg + 50part = 101 rows go missing).
  seed.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["ses_a", "proj_y", null, "a", "/tmp/y", "A", "1", null, null, null, null, null, null, null, 1, 1_000, null, null, null],
  );
  for (let i = 0; i < 50; i++) {
    seed.run(
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      [`msgA_${i}`, "ses_a", 1, 1_000, '{}'],
    );
    seed.run(
      "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
      [`partA_${i}`, `msgA_${i}`, "ses_a", 1, 1_000, '{}'],
    );
  }
  // Session B: stays (1 + 30msg + 30part = 61 rows remain live).
  seed.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["ses_b", "proj_y", null, "b", "/tmp/y", "B", "1", null, null, null, null, null, null, null, 1, 1_000, null, null, null],
  );
  for (let i = 0; i < 30; i++) {
    seed.run(
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      [`msgB_${i}`, "ses_b", 1, 1_000, '{}'],
    );
    seed.run(
      "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
      [`partB_${i}`, `msgB_${i}`, "ses_b", 1, 1_000, '{}'],
    );
  }
  seed.close();

  const stateManager = new StateManager("desktop");
  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    new MockClient() as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  // Cycle 1: seed knownRows. Total = 1 proj + 2 ses + 160 msg+part = 163.
  await sync.pushAll();
  expect(Object.keys(stateManager.state.knownRows).length).toBeGreaterThan(
    TOMBSTONE_MIN_KNOWN_FOR_TEST,
  );

  // Delete session A — cascades to 100 children.
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_a"]);
  db.close();
  // 101 missing / 163 known = 62%.
  // STRICT (50%) → would halt. PERMISSIVE (95%) → would NOT halt.

  sync.markExpectedDeletion("session:ses_a");

  await sync.pushAll();

  // After M1 (rowParents cascade): every descendant of ses_a is marked
  // expected at deletion time, so there are zero unexpectedCandidates
  // from ses_a and the permissive-branch check never even needs to
  // fire. No halt, no pending entries for ses_a children, and all
  // cascade rows fast-path straight to tombstones.
  expect(isSyncHalted()).toBe(false);
  expect(sync.isHalted()).toBe(false);
  // With cascade expansion working, the pending buffer is empty for
  // ses_a's descendants — they all fast-path.
  for (const k of Object.keys(stateManager.state.pendingTombstones)) {
    // If anything landed in pending, it must NOT be from ses_a.
    const parent = stateManager.state.rowParents[k];
    expect(parent).not.toBe("ses_a");
  }

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("threshold guard does NOT halt when knownRows is below the minimum trigger size", async () => {
  // TOMBSTONE_THRESHOLD_MIN_KNOWN exists so a small knownRows set
  // doesn't trip the percentage guard on every legitimate cleanup.
  // A user with 4 sessions deleting 3 of them shouldn't get halted.
  // The two-cycle confirmation still applies — they just don't get
  // an immediate halt.
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
  // Only ~5 knownRows — below the 50-row floor.
  expect(Object.keys(stateManager.state.knownRows).length).toBeLessThan(50);

  // Wipe the DB. With the threshold floor in effect, this should NOT
  // halt — pending confirmation is still required, but no marker.
  const db = new Database(dbPath);
  db.exec("DELETE FROM todo; DELETE FROM part; DELETE FROM message; DELETE FROM session; DELETE FROM project;");
  db.close();

  client.pushes = [];
  await sync.pushAll();

  expect(isSyncHalted()).toBe(false);
  expect(sync.isHalted()).toBe(false);
  // Candidates buffered, no tombstones yet.
  expect(client.pushes).toEqual([]);
  expect(Object.keys(stateManager.state.pendingTombstones).length).toBeGreaterThan(0);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("server cross-check drops tombstones for rows the server has newer", async () => {
  // If another peer updated a row since we last synced, we'd rather
  // pull that update than overwrite it with our tombstone. The
  // /sync/heads cross-check exists to detect this case.
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

  await sync.pushAll();
  // knownRows knows ses_1 at time 1_000.

  // Server reports ses_1 at time 5_000 — a peer updated it after our
  // last sync. We delete it locally — but the server knows newer; we
  // should NOT tombstone.
  client.headsResponses.set("session:ses_1", {
    kind: "session",
    id: "ses_1",
    time_updated: 5_000,
    deleted: false,
  });

  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();

  // Cycle 2 (after enough time for confirmation to fire if it would).
  const realNow = Date.now;
  Date.now = () => realNow() + 60_000;
  try {
    client.pushes = [];
    await sync.pushAll();
    // No tombstone for ses_1 — server has newer. (Cascade rows
    // message/part/todo also dropped because their rowKeys aren't in
    // the headsResponses map AND the test doesn't seed them as
    // server-newer; but they would still need confirmation. Either
    // way the test focuses on the SES_1 cross-check.)
    const ses1Tombstones = client.pushes
      .flat()
      .filter((e) => e.kind === "session" && e.id === "ses_1");
    expect(ses1Tombstones).toEqual([]);
  } finally {
    Date.now = realNow;
  }

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("server cross-check degrades gracefully when /sync/heads is missing (older server)", async () => {
  // EndpointMissingError means the server is older than the
  // deletion-safety release. We should still emit tombstones using
  // local-only confirmation — just without the server-newer filter.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const client = new MockClient();
  client.headsThrowEndpointMissing = true;
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

  // Cycle 2: pending registered (no error from the missing endpoint).
  client.pushes = [];
  await sync.pushAll();
  expect(client.pushes).toEqual([]);
  expect(Object.keys(stateManager.state.pendingTombstones).length).toBeGreaterThan(0);

  // Cycle 3 after delay: tombstones emitted regardless of the missing endpoint.
  const realNow = Date.now;
  Date.now = () => realNow() + 60_000;
  try {
    client.pushes = [];
    await sync.pushAll();
    const tombstones = client.pushes.flat().filter((e) => e.deleted);
    expect(tombstones.length).toBeGreaterThan(0);
  } finally {
    Date.now = realNow;
  }

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("server cross-check defers tombstones on transient network failure", async () => {
  // A non-EndpointMissing failure (DNS, 5xx, connection reset) means
  // we can't trust the cross-check. Better to defer this cycle's
  // unexpected tombstones than to barrel through blindly. Expected
  // tombstones still flow.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const client = new MockClient();
  client.headsThrowError = true;
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

  // Delete a message directly without marking expected — this creates
  // a genuine unexpectedCandidate that exercises the getHeads-failure
  // defer path. (After M1, deleting a whole session would cascade
  // every descendant into expectedDeletions via rowParents, leaving
  // nothing unexpected to defer.)
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM message WHERE id = ?", ["msg_1"]);
  db.close();

  client.pushes = [];
  await sync.pushAll();
  // The unexpected message tombstone was deferred by the getHeads
  // failure — NOT emitted this cycle.
  const emitted = client.pushes.flat().map((e) => `${e.kind}:${e.id}`).sort();
  expect(emitted).not.toContain("message:msg_1");

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("two-cycle confirmation is reset when the row reappears in the live DB", async () => {
  // Defends against false positives: a row briefly disappears (e.g.
  // mid-migration, opencode is rewriting a table) but reappears.
  // The pending entry should be cleared so we don't tombstone on the
  // next cycle — even if the row goes missing AGAIN later, the
  // confirmation timer must restart.
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

  // Cycle 2: delete, observe pending entry.
  let db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["session:ses_1"]).toBeDefined();
  const firstSeenAt = stateManager.state.pendingTombstones["session:ses_1"]!.firstSeenAt;

  // Restore the row (simulating "the migration completed and the row
  // came back").
  db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["ses_1", "proj_1", null, "session-1", "/tmp/project", "Session 1", "1", null, null, null, null, null, null, null, 1, 2_000, null, null, null],
  );
  db.close();

  // Cycle 3: pending entry should be cleared.
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["session:ses_1"]).toBeUndefined();

  // Delete it AGAIN. Cycle 4 should re-pend with a NEW firstSeenAt
  // (timer restarts; row doesn't get tombstoned just because it was
  // missing once before).
  db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_1"]);
  db.close();
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["session:ses_1"]).toBeDefined();
  // It's a fresh entry; firstSeenAt is recent (>= the original one).
  expect(stateManager.state.pendingTombstones["session:ses_1"]!.firstSeenAt).toBeGreaterThanOrEqual(firstSeenAt);
  // No tombstone yet.
  expect(client.pushes.flat().filter((e) => e.deleted)).toEqual([]);

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

// ── Halt-bypass coverage ───────────────────────────────────────────
//
// These tests cover paths where the on-disk halt marker exists but
// `SessionSync.halted` is `false` — which happens after a plugin
// restart (in-memory flag does not survive process exit) and when the
// fingerprint-mismatch halt in `index.ts` writes the marker without
// touching the SessionSync instance. Before the fix, event-driven
// callers (`session.deleted` → `pushAll`, `server.connected` →
// `sync.sync()`) would slip past the halt because they only consulted
// the in-memory flag.

import { writeHaltMarker, HALT_REASONS } from "./halt.js";

test("pushAll respects an externally-written halt marker even if this.halted is false", async () => {
  // Simulates a plugin restart: marker on disk, fresh SessionSync
  // instance with no in-memory halt state. Before the fix, pushAll
  // would happily push (and delete) anything.
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

  // Halt was set by a previous process / fingerprint trip / threshold
  // trip — `this.halted` is `false` because this is a fresh instance.
  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.DB_FINGERPRINT_MISMATCH,
    message: "simulated previous-run halt",
  });
  expect(isSyncHalted()).toBe(true);

  await sync.pushAll();

  // No network calls — neither live rows nor tombstones — should have
  // been emitted.
  expect(client.pushes).toEqual([]);
  // Public introspection reflects the halt.
  expect(sync.isHalted()).toBe(true);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("pushSession respects an externally-written halt marker", async () => {
  // Same scenario as above but for the per-session push path. Before
  // the fix, an `session.idle` event after a restart would slip a
  // live-row push past the halt — extending knownRows with rows the
  // (still-broken) push path would later try to tombstone.
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

  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "simulated previous-run halt",
  });

  await sync.pushSession("ses_1");

  expect(client.pushes).toEqual([]);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("sync() does NOT pull when an externally-written halt marker is present", async () => {
  // Pull has to be gated alongside push because pulling extends
  // knownRows with new rows that the broken push path would then try
  // to tombstone once the halt is cleared. The timer path in index.ts
  // already enforces this; this test verifies the entry guard inside
  // SessionSync.sync() catches event-driven callers (server.connected)
  // that bypass the timer.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const client = new MockClient();
  // If pull WERE called, this response would be consumed; assert
  // afterwards that it wasn't.
  client.pullResponses.push({
    server_seq: 99,
    more: false,
    envelopes: [],
  });

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

  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.DB_FINGERPRINT_MISMATCH,
    message: "simulated previous-run halt",
  });

  await sync.sync();

  // Pull was never called — pullCalls is empty, pullResponses still has
  // the unconsumed response.
  expect(client.pullCalls).toEqual([]);
  expect(client.pullResponses.length).toBe(1);
  // Push also skipped.
  expect(client.pushes).toEqual([]);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("isHalted() returns true based on the on-disk marker even without an in-process trip", async () => {
  // Verifies the public introspection method reflects disk state,
  // which is what hooks.ts and the test suite rely on to decide
  // whether to bother queueing per-session work after a restart.
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    new MockClient() as unknown as SyncClient,
    new StateManager("desktop"),
    "desktop",
    () => {},
  );

  expect(sync.isHalted()).toBe(false);

  writeHaltMarker({
    triggeredAt: Date.now(),
    reason: HALT_REASONS.TOMBSTONE_THRESHOLD,
    message: "external halt",
  });

  expect(sync.isHalted()).toBe(true);

  // Clearing the marker (which is what manual recovery does) flips it
  // back without any in-process state change.
  clearHaltMarker();
  expect(sync.isHalted()).toBe(false);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("a once-halted SessionSync stays halted even after the marker is cleared", async () => {
  // Defence in depth: the in-memory `halted` flag is a one-way latch
  // for the current process. Manual marker clear is meant to be a
  // human action signalling "I've inspected, restore the data, you can
  // resume" — but it doesn't unwind any pending tombstone state that
  // was prepared mid-cycle. To prevent racy resume scenarios within
  // the same process, once `this.halted` flips true we keep refusing
  // to push until the process restarts (which clears the in-memory
  // latch and re-evaluates the marker fresh).
  const dbPath = createDbPath();
  initDb(dbPath);

  // Seed enough rows to trip the threshold, then wipe.
  const seed = new Database(dbPath);
  seed.exec("PRAGMA foreign_keys = ON");
  seed.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_z", "/tmp/z", "git", "Z", null, null, 1, 1, 1, "[]", null],
  );
  for (let i = 0; i < 100; i++) {
    seed.run(
      `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [`ses_z_${i}`, "proj_z", null, `s${i}`, "/tmp/z", `S${i}`, "1", null, null, null, null, null, null, null, 1, 1_000 + i, null, null, null],
    );
  }
  seed.close();

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

  // Cycle 1: seed knownRows.
  await sync.pushAll();
  // Wipe → cycle 2 trips the threshold.
  const wipe = new Database(dbPath);
  wipe.exec("DELETE FROM session; DELETE FROM project;");
  wipe.close();
  client.pushes = [];
  await sync.pushAll();
  expect(sync.isHalted()).toBe(true);
  expect(isSyncHalted()).toBe(true);

  // Manually clear the marker as a user would after restoring data.
  clearHaltMarker();
  expect(isSyncHalted()).toBe(false);

  // Same process: in-memory latch still set, push remains a no-op.
  client.pushes = [];
  await sync.pushAll();
  expect(client.pushes).toEqual([]);
  expect(sync.isHalted()).toBe(true);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("getHeads still throws EndpointMissingError on a 404 from /sync/heads", async () => {
  // The 404 → EndpointMissingError translation moved out of the generic
  // `request()` helper into `getHeads` itself (so a 404 from a
  // different endpoint — e.g. `GET /files/blob/:sha256` for a missing
  // blob — surfaces as a generic HttpError, not an `EndpointMissingError`
  // misleading other callers). Verify the externally-visible behaviour
  // for getHeads is unchanged.
  const { SyncClient: RealSyncClient } = await import("./client.js");

  // Spin up a tiny one-shot server that 404s on /sync/heads.
  const server = Bun.serve({
    port: 0,
    fetch: () => new Response("Not Found", { status: 404 }),
  });
  try {
    const client = new RealSyncClient(`http://127.0.0.1:${server.port}`, "tok");
    let caught: unknown = null;
    try {
      await client.getHeads("desktop", [{ kind: "session" as SyncKind, id: "x" }]);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(EndpointMissingError);
  } finally {
    server.stop(true);
  }
});

test("H2: getHeads failure clears pendingTombstones to prevent instant-tombstone on long recovery", async () => {
  // Before the fix, a multi-minute network outage would park pending
  // entries with a stale firstSeenAt; the first recovered cycle saw
  // `now - firstSeenAt >> 30s` and emitted tombstones without any
  // post-recovery confirmation. See FINDINGS.md H2.
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

  // Cycle 1: establish knownRows.
  await sync.pushAll();

  // Delete a row (not expected) to create an unexpected candidate.
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM message WHERE id = ?", ["msg_1"]);
  db.close();

  // Cycle 2: first observation while getHeads is WORKING → pending
  // entry is created.
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["message:msg_1"]).toBeDefined();

  // Cycle 3: getHeads now fails (simulate a network outage starting).
  // Pending entry must be cleared so a long outage doesn't drift its
  // firstSeenAt past the confirmation window.
  client.headsThrowError = true;
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["message:msg_1"]).toBeUndefined();
  // And we did NOT emit a tombstone for the unexpected candidate.
  expect(
    client.pushes.flat().find((e) => e.deleted && e.id === "msg_1"),
  ).toBeUndefined();

  // Cycle 4: network recovers. Row is still gone. First recovered
  // cycle should create a FRESH pending entry, not instant-tombstone.
  client.headsThrowError = false;
  client.pushes = [];
  await sync.pushAll();
  expect(stateManager.state.pendingTombstones["message:msg_1"]).toBeDefined();
  expect(
    client.pushes.flat().find((e) => e.deleted && e.id === "msg_1"),
  ).toBeUndefined();

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("H3: poison envelope is skipped past after PULL_POISON_THRESHOLD retries", async () => {
  // Before the fix, a permanently-bad envelope (unknown kind, malformed
  // data, FK-ordering inversion that never resolves) blocked all
  // subsequent pulls forever because the pull loop broke on the first
  // error without advancing the cursor. Now the envelope's error
  // counter persists across cycles; once it exceeds the threshold, the
  // envelope is recorded in poisonedEnvelopes, the cursor advances, and
  // subsequent pulls proceed. See FINDINGS.md H3.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

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

  // A single envelope that will always fail to apply (unknown kind).
  const poisonEnvelope = {
    kind: "workspace" as SyncKind, // not in SYNC_KINDS — applyEnvelope returns "error"
    id: "ws_1",
    machine_id: "laptop",
    time_updated: 9_000,
    server_seq: 10,
    deleted: false,
    data: {},
  };

  // Simulate 10 pull cycles. Each cycle re-queues the same poison page.
  // Before the 10th cycle, the cursor stays at 0 and poisonedEnvelopes
  // is empty. On the 10th (threshold) the envelope is skipped past.
  for (let i = 0; i < 10; i++) {
    client.pullResponses.push({
      server_seq: 10,
      more: false,
      envelopes: [{ ...poisonEnvelope } as unknown as SyncEnvelope],
    });
  }

  // Cycles 1-9: counter increments, cursor does NOT advance.
  for (let i = 0; i < 9; i++) {
    await sync.pull();
    expect(stateManager.state.lastPulledSeq).toBe(0);
    expect(stateManager.state.poisonedEnvelopes.length).toBe(0);
    expect(stateManager.state.pullErrorCounts["workspace:ws_1:10"]).toBe(i + 1);
  }

  // Cycle 10: threshold hit. Envelope is recorded as poisoned and
  // skipped past; the cursor advances so subsequent pulls work.
  await sync.pull();
  expect(stateManager.state.poisonedEnvelopes.length).toBe(1);
  expect(stateManager.state.poisonedEnvelopes[0]?.kind as string).toBe("workspace");
  expect(stateManager.state.poisonedEnvelopes[0]?.id).toBe("ws_1");
  expect(stateManager.state.poisonedEnvelopes[0]?.server_seq).toBe(10);
  expect(stateManager.state.pullErrorCounts["workspace:ws_1:10"]).toBeUndefined();
  expect(stateManager.state.lastPulledSeq).toBe(10);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("H3: already-poisoned envelope is skipped on re-pull, not re-counted (bugbot regression)", async () => {
  // Regression for the cursor-blocked re-poison bug bugbot caught:
  //
  // Scenario: a batch `[env_A_transient, env_B_poisonable]` where env_A
  // is a transient error (FK ordering that persists) and env_B reaches
  // threshold while env_A is still failing. When env_B hits threshold
  // on cycle N, `firstErrorSeq` has already been set to env_A.seq. The
  // poison-skip path tries `if (firstErrorSeq === null) lastGoodSeq =
  // env_B.seq` — but firstErrorSeq is NOT null, so the cursor does
  // NOT advance past env_B.
  //
  // Next pull re-delivers [env_A, env_B] at the same cursor. Without
  // bugbot's fix, applyEnvelope is called on env_B, the counter
  // increments from 1 (was cleared on poison), and env_B needs 10 MORE
  // cycles to re-poison — producing a duplicate poisonedEnvelopes entry.
  // With bugbot's fix, env_B is detected as already-poisoned at the
  // top of the loop and skipped cleanly.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

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

  // env_A: FK violation (references a project that doesn't exist) →
  // applyEnvelope returns "error" persistently.
  const envA: SyncEnvelope = {
    kind: "session",
    id: "ses_orphan",
    machine_id: "laptop",
    time_updated: 5_000,
    server_seq: 5,
    deleted: false,
    data: {
      id: "ses_orphan",
      project_id: "proj_missing",
      parent_id: null,
      slug: "o",
      directory: "/tmp",
      title: "O",
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
  };
  // env_B: poison (unknown kind).
  const envB = {
    kind: "workspace" as SyncKind,
    id: "ws_1",
    machine_id: "laptop",
    time_updated: 9_000,
    server_seq: 10,
    deleted: false,
    data: {},
  };

  // Cycles 1-9: only env_B in the batch. It fails every cycle → counter=9.
  for (let i = 0; i < 9; i++) {
    client.pullResponses.push({
      server_seq: 10,
      more: false,
      envelopes: [{ ...envB } as unknown as SyncEnvelope],
    });
    await sync.pull();
  }
  expect(stateManager.state.pullErrorCounts["workspace:ws_1:10"]).toBe(9);
  expect(stateManager.state.poisonedEnvelopes.length).toBe(0);

  // Cycle 10: env_A appears BEFORE env_B in the batch. env_A fails
  // first (transient, counter=1, firstErrorSeq set). env_B then fails
  // and reaches threshold (counter=10 → POISON). At poison time,
  // firstErrorSeq is already set to env_A.seq, so the cursor CANNOT
  // advance past env_B.
  client.pullResponses.push({
    server_seq: 10,
    more: false,
    envelopes: [envA, { ...envB } as unknown as SyncEnvelope],
  });
  await sync.pull();

  // env_B poisoned exactly once.
  expect(stateManager.state.poisonedEnvelopes.length).toBe(1);
  expect(stateManager.state.poisonedEnvelopes[0]?.id).toBe("ws_1");
  const firstPoisonedAt = stateManager.state.poisonedEnvelopes[0]?.skippedAt;
  // env_B counter cleared at poison time.
  expect(stateManager.state.pullErrorCounts["workspace:ws_1:10"]).toBeUndefined();
  // env_A counter = 1 (it's transient).
  expect(stateManager.state.pullErrorCounts["session:ses_orphan:5"]).toBe(1);
  // Cursor stayed at 0 (couldn't advance past env_B because env_A was
  // the first error in the batch).
  expect(stateManager.state.lastPulledSeq).toBe(0);

  // Cycle 11: re-deliver both envelopes. env_A still fails (transient),
  // env_B is already poisoned → MUST be skipped at the top of the loop
  // WITHOUT re-invoking applyEnvelope and WITHOUT re-incrementing the
  // counter. Bugbot's fix is what makes this pass.
  client.pullResponses.push({
    server_seq: 10,
    more: false,
    envelopes: [envA, { ...envB } as unknown as SyncEnvelope],
  });
  await sync.pull();

  // No duplicate poisonedEnvelopes entry for env_B.
  expect(stateManager.state.poisonedEnvelopes.length).toBe(1);
  expect(stateManager.state.poisonedEnvelopes[0]?.skippedAt).toBe(firstPoisonedAt);
  // env_B counter is NOT re-incremented (stays undefined, not 1).
  expect(stateManager.state.pullErrorCounts["workspace:ws_1:10"]).toBeUndefined();
  // env_A counter continued to grow (2 now).
  expect(stateManager.state.pullErrorCounts["session:ses_orphan:5"]).toBe(2);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("H3: transient error clears counter on successful retry", async () => {
  // A truly transient error (FK-ordering inversion that resolves when
  // the parent arrives) should NOT accumulate toward the poison
  // threshold. On successful apply, the counter is cleared.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

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

  // Cycle 1: envelope references a non-existent parent project → FK fails.
  const sessionEnv = {
    kind: "session" as SyncKind,
    id: "ses_new",
    machine_id: "laptop",
    time_updated: 9_000,
    server_seq: 5,
    deleted: false,
    data: {
      id: "ses_new",
      project_id: "proj_not_yet_synced",
      parent_id: null,
      slug: "s",
      directory: "/tmp",
      title: "New",
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
  };
  client.pullResponses.push({ server_seq: 5, more: false, envelopes: [sessionEnv] });
  await sync.pull();
  expect(stateManager.state.pullErrorCounts["session:ses_new:5"]).toBe(1);
  expect(stateManager.state.lastPulledSeq).toBe(0);

  // Cycle 2: parent project arrives first in a separate pull, then session.
  // After the project lands, a retry of the session succeeds.
  {
    const db = new Database(dbPath);
    db.exec("PRAGMA foreign_keys = ON");
    db.run(
      `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ["proj_not_yet_synced", "/tmp/p", "git", "P", null, null, 1, 1, 1, "[]", null],
    );
    db.close();
  }
  client.pullResponses.push({ server_seq: 5, more: false, envelopes: [sessionEnv] });
  await sync.pull();
  // Counter should be cleared, cursor advanced, no poison recorded.
  expect(stateManager.state.pullErrorCounts["session:ses_new:5"]).toBeUndefined();
  expect(stateManager.state.poisonedEnvelopes.length).toBe(0);
  expect(stateManager.state.lastPulledSeq).toBe(5);

  console.error = originalError;
  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("M8: applyEnvelope rejects time_updated = 0", async () => {
  // `time_updated = 0` poisons LWW comparisons (compares equal to any
  // zero-stamped local row → skipped branch silently ignores content
  // differences). Plugin rejects stricter than server. See FINDINGS.md M8.
  const dbPath = createDbPath();
  initDb(dbPath);

  const originalError = console.error;
  console.error = () => {};

  const writer = new DbWriter(dbPath);
  const result = writer.applyEnvelope({
    kind: "session",
    id: "ses_bad",
    machine_id: "laptop",
    time_updated: 0,
    server_seq: 1,
    deleted: false,
    data: {
      id: "ses_bad",
      project_id: "proj_x",
      parent_id: null,
      slug: "s",
      directory: "/tmp",
      title: "Bad",
      version: "1",
      share_url: null,
      summary_additions: null,
      summary_deletions: null,
      summary_files: null,
      summary_diffs: null,
      revert: null,
      permission: null,
      time_created: 1,
      time_updated: 0,
      time_compacting: null,
      time_archived: null,
      workspace_id: null,
    },
  });
  expect(result).toBe("error");
  writer.close();
  console.error = originalError;
  fs.rmSync(dbPath, { force: true });
});

test("M8: applyEnvelope rejects negative time_updated", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  const originalError = console.error;
  console.error = () => {};

  const writer = new DbWriter(dbPath);
  const result = writer.applyEnvelope({
    kind: "session",
    id: "ses_bad",
    machine_id: "laptop",
    time_updated: -1,
    server_seq: 1,
    deleted: false,
    data: null,
  });
  expect(result).toBe("error");
  writer.close();
  console.error = originalError;
  fs.rmSync(dbPath, { force: true });
});

test("non-getHeads 404s surface as HttpError, not EndpointMissingError", async () => {
  // Regression: previously any 404 anywhere (e.g. a missing blob)
  // would surface as `EndpointMissingError`, which mis-implies
  // "server lacks this endpoint" when the server actually has it but
  // the resource is missing. With the scoping fix, only `getHeads`
  // produces `EndpointMissingError`; other callers see `HttpError`
  // exposing a `status` field they can branch on.
  const { SyncClient: RealSyncClient, HttpError } = await import("./client.js");

  const server = Bun.serve({
    port: 0,
    fetch: () => new Response("Blob not found", { status: 404 }),
  });
  try {
    const client = new RealSyncClient(`http://127.0.0.1:${server.port}`, "tok");
    let caught: unknown = null;
    try {
      await client.getBlob("nonexistent_sha256");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(HttpError);
    expect(caught).not.toBeInstanceOf(EndpointMissingError);
    expect((caught as InstanceType<typeof HttpError>).status).toBe(404);
  } finally {
    server.stop(true);
  }
});

// ── M7: parseRowStateKey bounds ──

test("M7: parseRowStateKey rejects an empty kind prefix", () => {
  expect(parseRowStateKey(":foo")).toBeNull();
});

test("M7: parseRowStateKey rejects a rowKey with no separator", () => {
  expect(parseRowStateKey("foo")).toBeNull();
});

test("M7: parseRowStateKey rejects an empty id", () => {
  expect(parseRowStateKey("session:")).toBeNull();
});

test("M7: parseRowStateKey rejects an unknown kind not in SYNC_KINDS", () => {
  expect(parseRowStateKey("workspace:x")).toBeNull();
  expect(parseRowStateKey("nonsense:y")).toBeNull();
});

test("M7: parseRowStateKey accepts valid kind+id (including composite todo)", () => {
  expect(parseRowStateKey("session:ses_1")).toEqual({ kind: "session", id: "ses_1" });
  expect(parseRowStateKey("todo:ses_1:0")).toEqual({ kind: "todo", id: "ses_1:0" });
});

// ── M1: rowParents secondary index enables cascade-expansion ──

test("M1: rowParents is populated for message/part/todo/session_share on push", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const stateManager = new StateManager("desktop");
  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    new MockClient() as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pushAll();

  // message, part, todo should all point at ses_1 in rowParents.
  expect(stateManager.state.rowParents["message:msg_1"]).toBe("ses_1");
  expect(stateManager.state.rowParents["part:part_1"]).toBe("ses_1");
  expect(stateManager.state.rowParents["todo:ses_1:0"]).toBe("ses_1");

  // session / project should NOT have parent entries (they're roots).
  expect(stateManager.state.rowParents["session:ses_1"]).toBeUndefined();
  expect(stateManager.state.rowParents["project:proj_1"]).toBeUndefined();

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("M1: session deletion of a large session does not halt (cascade marked expected)", async () => {
  // The motivating scenario: a user with one session dominating
  // knownRows deletes it. 100+ messages+parts cascade via FK. Before
  // M1, only session/todo/share were auto-expanded as expected →
  // msgs+parts landed in unexpectedCandidates → 95% threshold trips →
  // HALT on a routine user action.
  //
  // After M1, rowParents maps each child to its session at remember-
  // time, so markExpectedDeletion walks the index and marks them all
  // expected. No halt, no pending entries for the cascade.
  const dbPath = createDbPath();
  initDb(dbPath);

  const seed = new Database(dbPath);
  seed.exec("PRAGMA foreign_keys = ON");
  seed.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_m1", "/tmp/m1", "git", "M1", null, null, 1, 1, 1, "[]", null],
  );
  seed.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["ses_big", "proj_m1", null, "big", "/tmp/m1", "Big", "1", null, null, null, null, null, null, null, 1, 1_000, null, null, null],
  );
  for (let i = 0; i < 80; i++) {
    seed.run(
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      [`bm_${i}`, "ses_big", 1, 1_000, '{}'],
    );
    seed.run(
      "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
      [`bp_${i}`, `bm_${i}`, "ses_big", 1, 1_000, '{}'],
    );
  }
  seed.close();

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

  // Cycle 1: populate knownRows and rowParents.
  await sync.pushAll();
  const knownBefore = Object.keys(stateManager.state.knownRows).length;
  expect(knownBefore).toBeGreaterThan(TOMBSTONE_MIN_KNOWN_FOR_TEST);

  // Delete the big session — FK cascades to all 160 children.
  const db = new Database(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.run("DELETE FROM session WHERE id = ?", ["ses_big"]);
  db.close();

  sync.markExpectedDeletion("session:ses_big");

  client.pushes = [];
  await sync.pushAll();

  // No halt — this is the whole point of M1.
  expect(isSyncHalted()).toBe(false);
  expect(sync.isHalted()).toBe(false);

  // All descendants fast-pathed to tombstones.
  const emitted = client.pushes.flat().filter((e) => e.deleted).map((e) => `${e.kind}:${e.id}`).sort();
  expect(emitted).toContain("session:ses_big");
  expect(emitted.filter((k) => k.startsWith("message:")).length).toBe(80);
  expect(emitted.filter((k) => k.startsWith("part:")).length).toBe(80);

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});

test("M1: rowParents is forgotten when the row is forgotten", async () => {
  const dbPath = createDbPath();
  initDb(dbPath);
  seedSessionTree(dbPath);

  const stateManager = new StateManager("desktop");
  const reader = new DbReader(dbPath);
  const writer = new DbWriter(dbPath);
  const sync = new SessionSync(
    reader,
    writer,
    new MockClient() as unknown as SyncClient,
    stateManager,
    "desktop",
    () => {},
  );

  await sync.pushAll();
  expect(stateManager.state.rowParents["message:msg_1"]).toBe("ses_1");

  stateManager.forgetRows(["message:msg_1"]);
  expect(stateManager.state.rowParents["message:msg_1"]).toBeUndefined();

  reader.close();
  writer.close();
  fs.rmSync(dbPath, { force: true });
});
