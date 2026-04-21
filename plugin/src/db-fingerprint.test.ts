import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { captureDbFingerprint, compareFingerprints } from "./db-fingerprint.js";

const tempPaths: string[] = [];

function tempPath(): string {
  const p = path.join(os.tmpdir(), `opencode-sync-fp-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
  tempPaths.push(p);
  return p;
}

afterEach(() => {
  while (tempPaths.length > 0) {
    const p = tempPaths.pop()!;
    fs.rmSync(p, { force: true });
  }
});

test("captureDbFingerprint returns null for a missing file", () => {
  const result = captureDbFingerprint("/no/such/path/should/exist.db");
  expect(result).toBeNull();
});

test("captureDbFingerprint returns inode/mtime/size for an existing file", () => {
  const p = tempPath();
  fs.writeFileSync(p, "hello world");
  const fp = captureDbFingerprint(p);

  expect(fp).not.toBeNull();
  expect(typeof fp!.inode).toBe("number");
  expect(typeof fp!.mtime).toBe("number");
  expect(fp!.size).toBe(11);
});

test("compareFingerprints treats no-previous as same (first-observation no-op)", () => {
  // Bootstrap path: state.json had no fingerprint, capture starts now.
  // Must NOT trigger the halt — that would block sync on first run.
  const cmp = compareFingerprints(null, { inode: 1, mtime: 1, size: 1 });
  expect(cmp.same).toBe(true);
  expect(cmp.reason).toBe("no_previous");
});

test("compareFingerprints flags a missing current file (DB deleted)", () => {
  const cmp = compareFingerprints({ inode: 1, mtime: 1, size: 100 }, null);
  expect(cmp.same).toBe(false);
  expect(cmp.reason).toBe("db_missing");
});

test("compareFingerprints flags an inode change (atomic rename / restore-from-backup)", () => {
  const previous = { inode: 100, mtime: 1_000, size: 4096 };
  const current = { inode: 200, mtime: 1_000, size: 4096 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(false);
  expect(cmp.reason).toBe("inode_changed");
  expect(cmp.details["previous_inode"]).toBe(100);
  expect(cmp.details["current_inode"]).toBe(200);
});

test("compareFingerprints flags a size shrink of >50%", () => {
  // Catches `VACUUM INTO + partial reload` / truncation scenarios where
  // inode survived but the data is mostly gone.
  const previous = { inode: 1, mtime: 1, size: 1_000_000 };
  const current = { inode: 1, mtime: 2, size: 100_000 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(false);
  expect(cmp.reason).toBe("size_shrunk");
});

test("compareFingerprints does NOT flag normal size growth (opencode is busy)", () => {
  // SQLite WAL pages, new sessions, message blobs all grow the file.
  // Normal use is "size monotonically increases over time" — must NOT
  // trip the guard.
  const previous = { inode: 1, mtime: 1, size: 1_000_000 };
  const current = { inode: 1, mtime: 2, size: 1_500_000 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(true);
  expect(cmp.reason).toBe("ok");
});

test("compareFingerprints does NOT flag a small size drop within tolerance", () => {
  // Compaction / VACUUM can shrink the file modestly without losing
  // data. 30% shrink is normal — only >50% trips.
  const previous = { inode: 1, mtime: 1, size: 1_000_000 };
  const current = { inode: 1, mtime: 2, size: 700_000 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(true);
});

test("compareFingerprints flags a large mtime regression (in-place restore)", () => {
  const previous = { inode: 1, mtime: 10_000_000, size: 1000 };
  // Mtime jumped backward by an hour — this is an in-place restore.
  const current = { inode: 1, mtime: 10_000_000 - 60 * 60 * 1000, size: 1000 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(false);
  expect(cmp.reason).toBe("mtime_regressed");
});

test("compareFingerprints does NOT flag tiny mtime regression (clock noise)", () => {
  // FS clock skew, NTP jitter, sub-second resolution differences. The
  // 5-minute tolerance must not be tripped by normal noise.
  const previous = { inode: 1, mtime: 10_000_000, size: 1000 };
  const current = { inode: 1, mtime: 10_000_000 - 1_000, size: 1000 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(true);
});

test("size guard does not divide by zero on a fresh empty DB", () => {
  // First sync after install: previous.size could legitimately be 0
  // (DB created but no data yet). A naive `next.size < previous.size * 0.5`
  // check would pass for any next.size — incorrect. Verify the guard is
  // a no-op when previous.size == 0.
  const previous = { inode: 1, mtime: 1, size: 0 };
  const current = { inode: 1, mtime: 2, size: 0 };
  const cmp = compareFingerprints(previous, current);
  expect(cmp.same).toBe(true);
});
