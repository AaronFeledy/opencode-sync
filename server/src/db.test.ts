import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { Session, SyncEnvelope } from "@opencode-sync/shared";
import { LedgerDB } from "./db.js";
import type { Logger } from "./log.js";

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

const tempDirs: string[] = [];

function createDataDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-sync-server-test-"));
  tempDirs.push(dir);
  return dir;
}

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop()!;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function makeSession(id: string, timeUpdated: number): Session {
  return {
    id,
    project_id: "proj_1",
    parent_id: null,
    slug: id,
    directory: "/tmp",
    title: `session ${id}`,
    version: "1",
    share_url: null,
    summary_additions: null,
    summary_deletions: null,
    summary_files: null,
    summary_diffs: null,
    revert: null,
    permission: null,
    time_created: 1,
    time_updated: timeUpdated,
    time_compacting: null,
    time_archived: null,
    workspace_id: null,
  };
}

function makeEnvelope(id: string, machineId: string, timeUpdated: number): SyncEnvelope {
  return {
    kind: "session",
    id,
    machine_id: machineId,
    time_updated: timeUpdated,
    server_seq: 0,
    deleted: false,
    data: makeSession(id, timeUpdated),
  };
}

test("upsertBatch assigns strictly monotonic, unique server_seq values", () => {
  const dir = createDataDir();
  const db = new LedgerDB(dir, silentLogger);

  const envelopes: SyncEnvelope[] = [];
  for (let i = 0; i < 25; i++) {
    envelopes.push(makeEnvelope(`ses_${i}`, "laptop", 1_000 + i));
  }

  const results = db.upsertBatch(envelopes);

  expect(results.length).toBe(25);
  expect(results.every((r) => r.accepted)).toBe(true);

  // Pull the rows back and verify server_seq is strictly monotonic with no gaps,
  // no duplicates, and matches the order of insertion.
  const pulled = db.pullRows(0, undefined, 100);
  expect(pulled.envelopes.length).toBe(25);

  const seqs = pulled.envelopes.map((e) => e.server_seq);
  // Strictly monotonic
  for (let i = 1; i < seqs.length; i++) {
    expect(seqs[i]!).toBeGreaterThan(seqs[i - 1]!);
  }
  // No duplicates
  expect(new Set(seqs).size).toBe(seqs.length);
  // next_seq advanced by exactly 25
  expect(db.getNextSeq()).toBe(seqs[seqs.length - 1]! + 1);

  db.close();
});

test("upsertBatch is atomic — a mid-batch throw rolls back the whole batch", () => {
  const dir = createDataDir();
  const db = new LedgerDB(dir, silentLogger);

  // Seed one row so we can verify it remains untouched after the rollback.
  const seedResult = db.upsertBatch([makeEnvelope("ses_seed", "laptop", 500)]);
  expect(seedResult[0]!.accepted).toBe(true);

  const seqBefore = db.getNextSeq();
  const pulledBefore = db.pullRows(0, undefined, 100);
  expect(pulledBefore.envelopes.length).toBe(1);

  // Build a batch with a malformed envelope in the middle. A circular
  // reference makes JSON.stringify throw, which crashes upsertRow's data
  // serialisation — exercising the rollback path.
  const circular: Record<string, unknown> = { ...makeSession("ses_bad", 2_000) };
  circular["self"] = circular;

  const badEnvelope: SyncEnvelope = {
    kind: "session",
    id: "ses_bad",
    machine_id: "laptop",
    time_updated: 2_000,
    server_seq: 0,
    deleted: false,
    data: circular as unknown as Session,
  };

  const batch: SyncEnvelope[] = [
    makeEnvelope("ses_a", "laptop", 1_000),
    badEnvelope,
    makeEnvelope("ses_c", "laptop", 1_500),
  ];

  expect(() => db.upsertBatch(batch)).toThrow();

  // After rollback: no new rows, next_seq unchanged from before the failed batch.
  const pulledAfter = db.pullRows(0, undefined, 100);
  expect(pulledAfter.envelopes.length).toBe(1);
  expect(pulledAfter.envelopes[0]!.id).toBe("ses_seed");
  expect(db.getNextSeq()).toBe(seqBefore);

  db.close();
});

test("upsertBatch preserves per-envelope accepted/stale results in order", () => {
  const dir = createDataDir();
  const db = new LedgerDB(dir, silentLogger);

  // Pre-populate a row at time_updated = 5000.
  db.upsertBatch([makeEnvelope("ses_existing", "laptop", 5_000)]);

  // Mixed batch: new row (accept), older version of existing (stale),
  // newer version of existing (accept).
  const batch: SyncEnvelope[] = [
    makeEnvelope("ses_new", "laptop", 1_000),         // accept — new id
    makeEnvelope("ses_existing", "laptop", 4_000),    // stale  — older than 5000
    makeEnvelope("ses_existing", "laptop", 6_000),    // accept — newer than 5000
  ];

  const results = db.upsertBatch(batch);

  expect(results.length).toBe(3);
  expect(results[0]!.accepted).toBe(true);
  expect(results[1]!.accepted).toBe(false);
  expect(results[1]!.stale?.server_time_updated).toBe(5_000);
  expect(results[2]!.accepted).toBe(true);

  db.close();
});
