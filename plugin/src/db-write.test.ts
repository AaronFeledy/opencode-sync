/**
 * Regression tests for DbWriter — specifically the UPSERT path that
 * replaced `INSERT OR REPLACE` after finding C1 showed the REPLACE form
 * cascade-deletes child rows via FK ON DELETE CASCADE whenever a
 * cross-peer update lands.
 */
import { Database } from "bun:sqlite";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import type { SyncEnvelope } from "@opencode-sync/shared";
import { DbWriter } from "./db-write.js";

const tempPaths: string[] = [];

function tempDbPath(): string {
  const p = path.join(
    os.tmpdir(),
    `opencode-sync-dbwrite-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`,
  );
  tempPaths.push(p);
  return p;
}

function initSchema(dbPath: string): void {
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
    CREATE TABLE session_share (
      session_id TEXT PRIMARY KEY REFERENCES session(id) ON DELETE CASCADE,
      id TEXT NOT NULL,
      secret TEXT NOT NULL,
      url TEXT NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL
    );
    CREATE TABLE permission (
      project_id TEXT PRIMARY KEY REFERENCES project(id) ON DELETE CASCADE,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      data TEXT NOT NULL
    );
  `);

  // Seed a project + session + 3 messages + 2 parts + 1 todo + 1 share.
  db.run(
    `INSERT INTO project (id, worktree, vcs, name, icon_url, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["proj_1", "/tmp/p", "git", "Test", null, null, 1, 1, 1, "[]", null],
  );
  db.run(
    `INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived, workspace_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ["ses_1", "proj_1", null, "s1", "/tmp/p", "Old Title", "1", null, null, null, null, null, null, null, 1, 100, null, null, null],
  );
  for (const [id, tc] of [
    ["msg_1", 110],
    ["msg_2", 120],
    ["msg_3", 130],
  ] as const) {
    db.run(
      "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
      [id, "ses_1", 1, tc, '{"role":"user"}'],
    );
  }
  db.run(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ["part_1", "msg_1", "ses_1", 1, 111, '{"type":"text"}'],
  );
  db.run(
    "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
    ["part_2", "msg_2", "ses_1", 1, 121, '{"type":"text"}'],
  );
  db.run(
    "INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
    ["ses_1", "todo", "pending", "high", 0, 1, 150],
  );
  db.run(
    "INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
    ["ses_1", "share_1", "secret", "https://...", 1, 160],
  );
  db.close();
}

function countRows(dbPath: string, table: string): number {
  const db = new Database(dbPath, { readonly: true });
  const row = db.query<{ n: number }, []>(`SELECT COUNT(*) as n FROM ${table}`).get();
  db.close();
  return row?.n ?? 0;
}

function sessionTitle(dbPath: string): string | null {
  const db = new Database(dbPath, { readonly: true });
  const row = db.query<{ title: string }, []>("SELECT title FROM session WHERE id = 'ses_1'").get();
  db.close();
  return row?.title ?? null;
}

afterEach(() => {
  while (tempPaths.length > 0) {
    const p = tempPaths.pop()!;
    fs.rmSync(p, { force: true });
  }
});

test("C1: session UPDATE via applyEnvelope preserves message/part/todo/share children", () => {
  // This is the core regression for finding C1. Before the UPSERT fix,
  // `INSERT OR REPLACE INTO session (...)` would DELETE the old row
  // (cascading to children via ON DELETE CASCADE) before inserting the
  // new one, silently wiping every message/part/todo/share for the
  // session each time a remote peer sent a routine title / summary /
  // time_compacting update.
  const dbPath = tempDbPath();
  initSchema(dbPath);

  // Baseline counts.
  expect(countRows(dbPath, "session")).toBe(1);
  expect(countRows(dbPath, "message")).toBe(3);
  expect(countRows(dbPath, "part")).toBe(2);
  expect(countRows(dbPath, "todo")).toBe(1);
  expect(countRows(dbPath, "session_share")).toBe(1);

  const writer = new DbWriter(dbPath);

  // Apply a newer session envelope (simulates a pull from a peer who
  // updated the session title).
  const envelope: SyncEnvelope = {
    kind: "session",
    id: "ses_1",
    machine_id: "remote-peer",
    server_seq: 1,
    time_updated: 200,
    deleted: false,
    data: {
      id: "ses_1",
      project_id: "proj_1",
      parent_id: null,
      slug: "s1",
      directory: "/tmp/p",
      title: "New Title",
      version: "1",
      share_url: null,
      summary_additions: null,
      summary_deletions: null,
      summary_files: null,
      summary_diffs: null,
      revert: null,
      permission: null,
      time_created: 1,
      time_updated: 200,
      time_compacting: null,
      time_archived: null,
      workspace_id: null,
    },
  };
  const result = writer.applyEnvelope(envelope);
  expect(result).toBe("applied");

  writer.close();

  // Children MUST be intact.
  expect(countRows(dbPath, "message")).toBe(3);
  expect(countRows(dbPath, "part")).toBe(2);
  expect(countRows(dbPath, "todo")).toBe(1);
  expect(countRows(dbPath, "session_share")).toBe(1);
  // And the session title WAS updated.
  expect(sessionTitle(dbPath)).toBe("New Title");
});

test("C1: project UPDATE preserves session children (and their grandchildren)", () => {
  const dbPath = tempDbPath();
  initSchema(dbPath);

  const writer = new DbWriter(dbPath);

  const envelope: SyncEnvelope = {
    kind: "project",
    id: "proj_1",
    machine_id: "remote-peer",
    server_seq: 1,
    time_updated: 500,
    deleted: false,
    data: {
      id: "proj_1",
      worktree: "/tmp/p",
      vcs: "git",
      name: "Renamed",
      icon_url: null,
      icon_color: null,
      time_created: 1,
      time_updated: 500,
      time_initialized: 1,
      sandboxes: "[]",
      commands: null,
    },
  };
  expect(writer.applyEnvelope(envelope)).toBe("applied");
  writer.close();

  expect(countRows(dbPath, "project")).toBe(1);
  expect(countRows(dbPath, "session")).toBe(1);
  expect(countRows(dbPath, "message")).toBe(3);
  expect(countRows(dbPath, "part")).toBe(2);
});

test("C1: composite-PK todo UPDATE does not disturb other rows", () => {
  const dbPath = tempDbPath();
  initSchema(dbPath);

  // Add a second todo so we can see that UPSERTing position=0 doesn't
  // nuke position=1.
  {
    const db = new Database(dbPath);
    db.run(
      "INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ["ses_1", "second", "pending", "low", 1, 1, 151],
    );
    db.close();
  }
  expect(countRows(dbPath, "todo")).toBe(2);

  const writer = new DbWriter(dbPath);
  const envelope: SyncEnvelope = {
    kind: "todo",
    id: "ses_1:0",
    machine_id: "remote-peer",
    server_seq: 1,
    time_updated: 300,
    deleted: false,
    data: {
      session_id: "ses_1",
      content: "todo-updated",
      status: "done",
      priority: "high",
      position: 0,
      time_created: 1,
      time_updated: 300,
    },
  };
  expect(writer.applyEnvelope(envelope)).toBe("applied");
  writer.close();

  expect(countRows(dbPath, "todo")).toBe(2);
  const db = new Database(dbPath, { readonly: true });
  const p0 = db.query<{ content: string; status: string }, []>(
    "SELECT content, status FROM todo WHERE session_id = 'ses_1' AND position = 0",
  ).get();
  const p1 = db.query<{ content: string; status: string }, []>(
    "SELECT content, status FROM todo WHERE session_id = 'ses_1' AND position = 1",
  ).get();
  db.close();
  expect(p0?.content).toBe("todo-updated");
  expect(p0?.status).toBe("done");
  expect(p1?.content).toBe("second");
});

test("C1: DELETE envelope still cascades (expected behaviour for real deletions)", () => {
  // Counter-regression: ensure the UPSERT fix didn't accidentally change
  // the semantics of tombstone application. Real session deletions
  // MUST cascade to children — that's the point of ON DELETE CASCADE
  // in the opencode schema.
  const dbPath = tempDbPath();
  initSchema(dbPath);

  const writer = new DbWriter(dbPath);
  const envelope: SyncEnvelope = {
    kind: "session",
    id: "ses_1",
    machine_id: "remote-peer",
    server_seq: 1,
    time_updated: 200,
    deleted: true,
    data: null,
  };
  expect(writer.applyEnvelope(envelope)).toBe("applied");
  writer.close();

  expect(countRows(dbPath, "session")).toBe(0);
  expect(countRows(dbPath, "message")).toBe(0);
  expect(countRows(dbPath, "part")).toBe(0);
  expect(countRows(dbPath, "todo")).toBe(0);
  expect(countRows(dbPath, "session_share")).toBe(0);
});

test("C1: equal time_updated is a skipped no-op (does not touch the row or children)", () => {
  const dbPath = tempDbPath();
  initSchema(dbPath);

  const writer = new DbWriter(dbPath);
  const envelope: SyncEnvelope = {
    kind: "session",
    id: "ses_1",
    machine_id: "remote-peer",
    server_seq: 1,
    time_updated: 100, // matches seed
    deleted: false,
    data: {
      id: "ses_1",
      project_id: "proj_1",
      parent_id: null,
      slug: "s1",
      directory: "/tmp/p",
      title: "New Title But Same Time",
      version: "1",
      share_url: null,
      summary_additions: null,
      summary_deletions: null,
      summary_files: null,
      summary_diffs: null,
      revert: null,
      permission: null,
      time_created: 1,
      time_updated: 100,
      time_compacting: null,
      time_archived: null,
      workspace_id: null,
    },
  };
  expect(writer.applyEnvelope(envelope)).toBe("skipped");
  writer.close();

  expect(sessionTitle(dbPath)).toBe("Old Title");
  expect(countRows(dbPath, "message")).toBe(3);
});
