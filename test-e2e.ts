/**
 * End-to-end integration test for opencode-sync.
 *
 * Tests the full server API and plugin client working together:
 * 1. Server health check
 * 2. Push session data from "desktop"
 * 3. Pull session data as "laptop"
 * 4. LWW conflict resolution (newer wins)
 * 5. Stale detection (older gets rejected)
 * 6. Tombstone handling (delete + resurrection guard)
 * 7. File upload, download, delete
 * 8. File manifest management
 * 9. Multiple row types (project, session, message, part, todo)
 * 10. Batch push with mixed results
 * 11. Pagination (pull with limit)
 *
 * Usage: OPENCODE_SYNC_TOKEN=test123 bun run test-e2e.ts
 */

import { SyncClient } from "./plugin/src/client.js";
import { AUTH_SYNC_PATH, type SyncEnvelope, type Project, type Session, type Message, type Todo } from "./shared/src/index.js";

const SERVER_URL = "http://127.0.0.1:14455";
const TOKEN = process.env.OPENCODE_SYNC_TOKEN ?? "test123";

const client = new SyncClient(SERVER_URL, TOKEN);

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  ✅ ${message}`);
    passed++;
  } else {
    console.error(`  ❌ ${message}`);
    failed++;
  }
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual === expected) {
    console.log(`  ✅ ${message}`);
    passed++;
  } else {
    console.error(`  ❌ ${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    failed++;
  }
}

// ── Test data ──────────────────────────────────────────────────────

const testProject: Project = {
  id: "proj_test1",
  worktree: "/home/user/projects/test",
  vcs: "git",
  name: "test-project",
  icon_url: null,
  icon_color: null,
  time_created: 1000,
  time_updated: 1000,
  time_initialized: 1000,
  sandboxes: "[]",
  commands: null,
};

const testSession: Session = {
  id: "ses_test1",
  project_id: "proj_test1",
  parent_id: null,
  slug: "test-session",
  directory: "/home/user/projects/test",
  title: "Test Session",
  version: "1",
  share_url: null,
  summary_additions: 10,
  summary_deletions: 5,
  summary_files: 3,
  summary_diffs: null,
  revert: null,
  permission: null,
  time_created: 1000,
  time_updated: 1000,
  time_compacting: null,
  time_archived: null,
  workspace_id: null,
};

const testMessage: Message = {
  id: "msg_test1",
  session_id: "ses_test1",
  time_created: 1000,
  time_updated: 1000,
  data: JSON.stringify({ role: "user", content: "Hello, world!" }),
};

const testTodo: Todo = {
  session_id: "ses_test1",
  content: "Fix the bug",
  status: "pending",
  priority: "high",
  position: 0,
  time_created: 1000,
  time_updated: 1000,
};

function envelope<T>(kind: string, id: string, machineId: string, timeUpdated: number, data: T, deleted = false): SyncEnvelope {
  return {
    id,
    kind: kind as any,
    machine_id: machineId,
    time_updated: timeUpdated,
    server_seq: 0,
    deleted,
    data: data as any,
  };
}

// ── Tests ──────────────────────────────────────────────────────────

async function testHealth() {
  console.log("\n🔍 Test: Health check");
  const health = await client.health();
  assert(health.ok === true, "health.ok is true");
  assert(typeof health.version === "string", "health.version is a string");
  assert(typeof health.time === "number", "health.time is a number");
}

async function testPushAndPull() {
  console.log("\n🔍 Test: Push from desktop, pull as laptop");

  // Push project + session + message
  const res = await client.push("desktop", [
    envelope("project", "proj_test1", "desktop", 1000, testProject),
    envelope("session", "ses_test1", "desktop", 1000, testSession),
    envelope("message", "msg_test1", "desktop", 1000, testMessage),
    envelope("todo", "ses_test1:0", "desktop", 1000, testTodo),
  ]);

  assertEqual(res.accepted.length, 4, "4 envelopes accepted");
  assertEqual(res.stale.length, 0, "0 stale envelopes");
  assert(res.server_seq > 0, `server_seq > 0 (got ${res.server_seq})`);

  // Pull as laptop — should get everything
  const pull = await client.pull(0, "laptop", 100);
  assert(pull.envelopes.length >= 4, `pulled ≥ 4 envelopes (got ${pull.envelopes.length})`);
  assert(!pull.more, "no more data");

  // Verify session data
  const sessionEnv = pull.envelopes.find(e => e.kind === "session" && e.id === "ses_test1");
  assert(sessionEnv !== undefined, "found session in pull");
  if (sessionEnv?.data) {
    assertEqual((sessionEnv.data as Session).title, "Test Session", "session title matches");
  }

  // Pull excluding desktop — should get nothing since desktop is the only writer
  const pullExclude = await client.pull(0, "desktop", 100);
  assertEqual(pullExclude.envelopes.length, 0, "excluding desktop yields 0 envelopes");
}

async function testLWWConflict() {
  console.log("\n🔍 Test: LWW conflict resolution");

  // Update session from desktop with newer timestamp
  const updatedSession = { ...testSession, title: "Updated from desktop", time_updated: 2000 };
  const res1 = await client.push("desktop", [
    envelope("session", "ses_test1", "desktop", 2000, updatedSession),
  ]);
  assertEqual(res1.accepted.length, 1, "newer update accepted");

  // Try to push from laptop with OLDER timestamp
  const staleSession = { ...testSession, title: "Stale from laptop", time_updated: 1500 };
  const res2 = await client.push("laptop", [
    envelope("session", "ses_test1", "laptop", 1500, staleSession),
  ]);
  assertEqual(res2.accepted.length, 0, "stale update rejected");
  assertEqual(res2.stale.length, 1, "1 stale entry returned");
  assertEqual(res2.stale[0]!.server_time_updated, 2000, "stale entry has server's timestamp");

  // Push from laptop with NEWER timestamp — should win
  const newerSession = { ...testSession, title: "Newer from laptop", time_updated: 3000 };
  const res3 = await client.push("laptop", [
    envelope("session", "ses_test1", "laptop", 3000, newerSession),
  ]);
  assertEqual(res3.accepted.length, 1, "newer laptop update accepted");

  // Verify the latest version
  const pull = await client.pull(0, undefined, 100);
  const latest = pull.envelopes.find(e => e.kind === "session" && e.id === "ses_test1");
  assertEqual((latest?.data as Session)?.title, "Newer from laptop", "latest title is laptop's version");
}

async function testTombstone() {
  console.log("\n🔍 Test: Tombstone handling");

  // Delete the session
  const res1 = await client.push("desktop", [
    envelope("session", "ses_test1", "desktop", 4000, null, true),
  ]);
  assertEqual(res1.accepted.length, 1, "tombstone accepted");

  // Try to resurrect with older timestamp — should be rejected
  const resurrect = { ...testSession, title: "Resurrected!", time_updated: 3500 };
  const res2 = await client.push("laptop", [
    envelope("session", "ses_test1", "laptop", 3500, resurrect, false),
  ]);
  assertEqual(res2.stale.length, 1, "resurrection rejected (stale)");

  // Resurrect with NEWER timestamp — should work
  const newResurrect = { ...testSession, title: "Properly resurrected", time_updated: 5000 };
  const res3 = await client.push("laptop", [
    envelope("session", "ses_test1", "laptop", 5000, newResurrect, false),
  ]);
  assertEqual(res3.accepted.length, 1, "resurrection with newer timestamp accepted");
}

async function testPagination() {
  console.log("\n🔍 Test: Pull pagination");

  // Push several messages
  const messages: SyncEnvelope[] = [];
  for (let i = 0; i < 10; i++) {
    messages.push(envelope("message", `msg_page_${i}`, "desktop", 6000 + i, {
      id: `msg_page_${i}`,
      session_id: "ses_test1",
      time_created: 6000 + i,
      time_updated: 6000 + i,
      data: JSON.stringify({ role: "assistant", content: `Message ${i}` }),
    }));
  }
  const res = await client.push("desktop", messages);
  assertEqual(res.accepted.length, 10, "10 paginated messages accepted");

  // Pull with limit=3
  const pull1 = await client.pull(0, undefined, 3);
  assertEqual(pull1.envelopes.length, 3, "first page has 3 envelopes");
  assert(pull1.more === true, "more=true for first page");

  // Pull next page
  const lastSeq1 = pull1.envelopes[pull1.envelopes.length - 1]!.server_seq;
  const pull2 = await client.pull(lastSeq1, undefined, 3);
  assertEqual(pull2.envelopes.length, 3, "second page has 3 envelopes");

  // Make sure there's no overlap
  const ids1 = new Set(pull1.envelopes.map(e => e.id));
  const overlap = pull2.envelopes.filter(e => ids1.has(e.id));
  assertEqual(overlap.length, 0, "no overlap between pages");
}

async function testFileSync() {
  console.log("\n🔍 Test: File upload, download, manifest, delete");

  // Upload a file
  const content = new TextEncoder().encode("# My Agent\n\nA custom agent definition.\n");
  await client.putFile("agents/custom.md", content, "desktop", Date.now());

  // Check manifest
  const manifest = await client.getManifest();
  const entry = manifest.find(e => e.relpath === "agents/custom.md");
  assert(entry !== undefined, "file appears in manifest");
  assertEqual(entry?.deleted, false, "file is not deleted");
  assertEqual(entry?.machine_id, "desktop", "file machine_id is desktop");
  assertEqual(entry?.size, content.length, `file size matches (${content.length})`);

  // Download blob
  if (entry) {
    const blob = await client.getBlob(entry.sha256);
    const downloaded = new TextDecoder().decode(blob);
    assertEqual(downloaded, "# My Agent\n\nA custom agent definition.\n", "blob content matches");
  }

  // Upload another file
  const content2 = new TextEncoder().encode('{ "model": "claude-sonnet-4-20250514" }\n');
  await client.putFile("opencode.json", content2, "laptop", Date.now());

  // Upload auth.json using the canonical relpath the plugin uses
  const authContent = new TextEncoder().encode('{"token":"secret"}\n');
  await client.putFile(AUTH_SYNC_PATH, authContent, "desktop", Date.now());

  // Verify manifest has both
  const manifest2 = await client.getManifest();
  assert(manifest2.filter(e => !e.deleted).length >= 2, "manifest has ≥ 2 non-deleted entries");
  assert(manifest2.some(e => e.relpath === AUTH_SYNC_PATH), "manifest contains auth.json relpath");

  // Delete a file — pass current wall-clock as the tombstone mtime; the
  // server requires X-Mtime since the LWW change.
  await client.deleteFile("agents/custom.md", "desktop", Date.now());

  // Verify tombstone
  const manifest3 = await client.getManifest();
  const deleted = manifest3.find(e => e.relpath === "agents/custom.md");
  assertEqual(deleted?.deleted, true, "deleted file is tombstoned");
}

async function testBatchMixed() {
  console.log("\n🔍 Test: Batch push with mixed accept/reject");

  // Push new message + stale session update in one batch
  const res = await client.push("vps", [
    // New message — should be accepted
    envelope("message", "msg_new_batch", "vps", 7000, {
      id: "msg_new_batch",
      session_id: "ses_test1",
      time_created: 7000,
      time_updated: 7000,
      data: JSON.stringify({ role: "user", content: "From VPS" }),
    }),
    // Stale session — should be rejected (current is at time_updated=5000)
    envelope("session", "ses_test1", "vps", 4500, {
      ...testSession,
      title: "Too old",
      time_updated: 4500,
    }),
  ]);

  assertEqual(res.accepted.length, 1, "1 accepted in mixed batch");
  assertEqual(res.stale.length, 1, "1 stale in mixed batch");
  assert(res.accepted.includes("msg_new_batch"), "new message was accepted");
  assertEqual(res.stale[0]!.id, "ses_test1", "session was stale");
}

async function testEqualTimestamp() {
  console.log("\n🔍 Test: Equal timestamp tie-breaking");

  // Push a fresh message
  const ts = 8000;
  const res1 = await client.push("desktop", [
    envelope("message", "msg_tiebreak", "desktop", ts, {
      id: "msg_tiebreak",
      session_id: "ses_test1",
      time_created: ts,
      time_updated: ts,
      data: JSON.stringify({ content: "from desktop" }),
    }),
  ]);
  assertEqual(res1.accepted.length, 1, "initial push accepted");

  // Push same timestamp from laptop — tie-break by machine_id lex order
  const res2 = await client.push("laptop", [
    envelope("message", "msg_tiebreak", "laptop", ts, {
      id: "msg_tiebreak",
      session_id: "ses_test1",
      time_created: ts,
      time_updated: ts,
      data: JSON.stringify({ content: "from laptop" }),
    }),
  ]);
  // "laptop" > "desktop" lexically, so laptop should win the tie-break
  assertEqual(res2.accepted.length, 1, "tie-break: laptop accepted (lexically higher)");
}

// ── Runner ─────────────────────────────────────────────────────────

async function main() {
  console.log("🚀 opencode-sync E2E integration tests\n");
  console.log(`Server: ${SERVER_URL}`);
  console.log(`Token:  ${TOKEN.slice(0, 4)}...`);

  try {
    await testHealth();
    await testPushAndPull();
    await testLWWConflict();
    await testTombstone();
    await testPagination();
    await testFileSync();
    await testBatchMixed();
    await testEqualTimestamp();
  } catch (err) {
    console.error("\n💥 Fatal error:", err);
    process.exit(1);
  }

  console.log(`\n${"─".repeat(40)}`);
  console.log(`Results: ${passed} passed, ${failed} failed`);

  if (failed > 0) {
    process.exit(1);
  }
  console.log("\n🎉 All tests passed!");
}

main();
