import { afterEach, beforeEach, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { INSTANCE_LOCK_PATH, tryAcquireSyncEngineLock } from "./instance-lock.js";

const HOME = os.homedir();
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load instance-lock tests against non-test HOME: ${HOME}.`,
  );
}

function cleanLock(): void {
  fs.rmSync(INSTANCE_LOCK_PATH, { force: true });
}

function stopChild(child: ChildProcess): void {
  try {
    child.kill("SIGTERM");
  } catch {
    // Best-effort test cleanup.
  }
}

beforeEach(() => {
  fs.mkdirSync(path.dirname(INSTANCE_LOCK_PATH), { recursive: true });
  cleanLock();
});

afterEach(() => {
  cleanLock();
});

test("duplicate entry in the same process cannot own the sync engine lock", () => {
  const logs: string[] = [];
  const log = (msg: string) => logs.push(msg);

  const owner = tryAcquireSyncEngineLock(log);
  expect(owner).not.toBeNull();

  const duplicate = tryAcquireSyncEngineLock(log);
  expect(duplicate).toBeNull();
  expect(logs).toContain("sync engine disabled — already active in this opencode process");

  owner?.release();

  const nextOwner = tryAcquireSyncEngineLock(log);
  expect(nextOwner).not.toBeNull();
  nextOwner?.release();
});

test("stale lock files are removed and replaced", () => {
  const impossiblePid = 999_999_999;
  fs.writeFileSync(
    INSTANCE_LOCK_PATH,
    JSON.stringify({ pid: impossiblePid, startedAt: Date.now() - 60_000, argv: ["opencode"] }),
  );

  const logs: string[] = [];
  const owner = tryAcquireSyncEngineLock((msg) => logs.push(msg));

  expect(owner).not.toBeNull();
  expect(logs).toContain("removing stale sync engine lock");

  const current = JSON.parse(fs.readFileSync(INSTANCE_LOCK_PATH, "utf-8")) as { pid: number };
  expect(current.pid).toBe(process.pid);

  owner?.release();
});

test("live-pid lock files with stale heartbeat are removed and replaced", async () => {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    stdio: "ignore",
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(child.pid).toBeNumber();

    fs.writeFileSync(
      INSTANCE_LOCK_PATH,
      JSON.stringify({ pid: child.pid, startedAt: Date.now() - 600_000, argv: ["opencode"] }),
    );
    const stale = new Date(Date.now() - 10 * 60_000);
    fs.utimesSync(INSTANCE_LOCK_PATH, stale, stale);

    const logs: string[] = [];
    const owner = tryAcquireSyncEngineLock((msg) => logs.push(msg));

    expect(owner).not.toBeNull();
    expect(logs).toContain("removing stuck sync engine lock");

    const current = JSON.parse(fs.readFileSync(INSTANCE_LOCK_PATH, "utf-8")) as { pid: number };
    expect(current.pid).toBe(process.pid);

    owner?.release();
  } finally {
    stopChild(child);
  }
});
