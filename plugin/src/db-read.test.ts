import { Database } from "bun:sqlite";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { DbReader } from "./db-read.js";

const tempPaths: string[] = [];

function tempDbPath(): string {
  const p = path.join(
    os.tmpdir(),
    `opencode-sync-dbread-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`,
  );
  tempPaths.push(p);
  return p;
}

afterEach(() => {
  while (tempPaths.length > 0) {
    const p = tempPaths.pop();
    if (p) fs.rmSync(p, { force: true });
  }
});

test("readAllRowKeys uses permission.id when the column exists", () => {
  const dbPath = tempDbPath();
  const db = new Database(dbPath);
  db.exec(`
    CREATE TABLE project (
      id TEXT PRIMARY KEY,
      worktree TEXT NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      sandboxes TEXT NOT NULL
    );
    CREATE TABLE permission (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      action TEXT NOT NULL,
      resource TEXT NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL
    );
    CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT, slug TEXT, directory TEXT, title TEXT, version TEXT, time_created INTEGER, time_updated INTEGER);
    CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
    CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
    CREATE TABLE todo (session_id TEXT, content TEXT, status TEXT, priority TEXT, position INTEGER, time_created INTEGER, time_updated INTEGER, PRIMARY KEY (session_id, position));
    CREATE TABLE session_share (session_id TEXT PRIMARY KEY, id TEXT, secret TEXT, url TEXT, time_created INTEGER, time_updated INTEGER);
  `);
  db.run(
    "INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES (?, ?, ?, ?, ?)",
    ["proj_1", "/tmp/p", 1, 1, "[]"],
  );
  db.run(
    "INSERT INTO permission (id, project_id, action, resource, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
    ["perm_a", "proj_1", "edit", "*", 1, 1],
  );
  db.run(
    "INSERT INTO permission (id, project_id, action, resource, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
    ["perm_b", "proj_1", "bash", "*", 1, 2],
  );
  db.close();

  const reader = new DbReader(dbPath);
  const keys = reader.readAllRowKeys();
  reader.close();

  expect(keys.has("permission:perm_a")).toBe(true);
  expect(keys.has("permission:perm_b")).toBe(true);
  expect(keys.has("permission:proj_1")).toBe(false);
});

test("iterateAllEnvelopes sessionScoped limits child rows to recently-updated sessions", () => {
  const dbPath = tempDbPath();
  const db = new Database(dbPath);
  db.exec(`
    CREATE TABLE project (id TEXT PRIMARY KEY, worktree TEXT, time_created INTEGER, time_updated INTEGER, sandboxes TEXT);
    CREATE TABLE permission (id TEXT PRIMARY KEY, project_id TEXT, action TEXT, resource TEXT, time_created INTEGER, time_updated INTEGER);
    CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT, slug TEXT, directory TEXT, title TEXT, version TEXT, time_created INTEGER, time_updated INTEGER);
    CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
    CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
    CREATE TABLE todo (session_id TEXT, content TEXT, status TEXT, priority TEXT, position INTEGER, time_created INTEGER, time_updated INTEGER, PRIMARY KEY (session_id, position));
    CREATE TABLE session_share (session_id TEXT PRIMARY KEY, id TEXT, secret TEXT, url TEXT, time_created INTEGER, time_updated INTEGER);
  `);
  const since = 1000;
  // ses_old is stale, but has a child part touched after `since`.
  db.run("INSERT INTO session (id, time_created, time_updated) VALUES (?, ?, ?)", ["ses_old", 1, 500]);
  db.run("INSERT INTO session (id, time_created, time_updated) VALUES (?, ?, ?)", ["ses_new", 1, 2000]);
  db.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", ["msg_old", "ses_old", 1, 1500, "{}"]);
  db.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", ["msg_new", "ses_new", 1, 1500, "{}"]);
  db.run("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)", ["msg_new_stale", "ses_new", 1, 100, "{}"]);
  db.run("INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)", ["prt_old", "msg_old", "ses_old", 1, 1500, "{}"]);
  db.run("INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)", ["prt_new", "msg_new", "ses_new", 1, 1500, "{}"]);
  db.run("INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)", ["ses_old", "x", "pending", "low", 0, 1, 1500]);
  db.run("INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)", ["ses_new", "y", "pending", "low", 0, 1, 1500]);
  db.close();

  const reader = new DbReader(dbPath);
  const scoped = [...reader.iterateAllEnvelopes(since, "m", { sessionScoped: true })]
    .map((e) => `${e.kind}:${e.id}`)
    .sort();
  const unscoped = [...reader.iterateAllEnvelopes(since, "m")]
    .map((e) => `${e.kind}:${e.id}`)
    .sort();
  reader.close();

  expect(scoped).toEqual([
    "message:msg_new",
    "part:prt_new",
    "session:ses_new",
    "todo:ses_new:0",
  ]);
  expect(unscoped).toEqual([
    "message:msg_new",
    "message:msg_old",
    "part:prt_new",
    "part:prt_old",
    "session:ses_new",
    "todo:ses_new:0",
    "todo:ses_old:0",
  ]);
});
