/**
 * Process-wide guard for the sync engine.
 *
 * opencode can initialize multiple service instances in one process and can
 * also run multiple processes from the same user config. Without this guard,
 * every plugin entry invocation runs startup sync and starts its own periodic
 * timer against the same state.json/opencode.db.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

const LOCK_DIR = path.join(os.homedir(), ".local", "share", "opencode", "opencode-sync");
export const INSTANCE_LOCK_PATH = path.join(LOCK_DIR, "sync-engine.lock");
const LOCK_HEARTBEAT_MS = 30_000;
const LOCK_STUCK_MS = 5 * 60_000;

interface LockFile {
  pid: number;
  startedAt: number;
  argv: string[];
}

export interface SyncEngineLock {
  readonly path: string;
  release: () => void;
}

function lockAgeMs(): number | null {
  try {
    return Date.now() - fs.statSync(INSTANCE_LOCK_PATH).mtimeMs;
  } catch {
    return null;
  }
}

function isLockHeartbeatStale(): boolean {
  const age = lockAgeMs();
  return age !== null && age > LOCK_STUCK_MS;
}

function isProcessAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function readLock(): LockFile | null {
  try {
    const raw = fs.readFileSync(INSTANCE_LOCK_PATH, "utf-8");
    const parsed = JSON.parse(raw) as Partial<LockFile>;
    return typeof parsed.pid === "number" && typeof parsed.startedAt === "number"
      ? { pid: parsed.pid, startedAt: parsed.startedAt, argv: Array.isArray(parsed.argv) ? parsed.argv.map(String) : [] }
      : null;
  } catch {
    return null;
  }
}

function writeNewLock(): SyncEngineLock {
  fs.mkdirSync(LOCK_DIR, { recursive: true });

  const lock: LockFile = {
    pid: process.pid,
    startedAt: Date.now(),
    argv: process.argv,
  };

  const fd = fs.openSync(INSTANCE_LOCK_PATH, "wx");
  try {
    fs.writeFileSync(fd, JSON.stringify(lock));
  } finally {
    fs.closeSync(fd);
  }

  const heartbeat = setInterval(() => {
    const current = readLock();
    if (current?.pid !== process.pid) return;
    const now = new Date();
    try {
      fs.utimesSync(INSTANCE_LOCK_PATH, now, now);
    } catch {
      // If the heartbeat fails, a future contender can treat the lock as stuck.
    }
  }, LOCK_HEARTBEAT_MS);
  heartbeat.unref?.();

  return {
    path: INSTANCE_LOCK_PATH,
    release() {
      clearInterval(heartbeat);
      const current = readLock();
      if (current?.pid !== process.pid) return;
      try {
        fs.rmSync(INSTANCE_LOCK_PATH, { force: true });
      } catch {
        // Best-effort cleanup; stale locks are removed on the next startup.
      }
    },
  };
}

export function tryAcquireSyncEngineLock(
  log: (msg: string, data?: Record<string, unknown>) => void,
): SyncEngineLock | null {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const lock = writeNewLock();
      log("acquired sync engine lock", { pid: process.pid, lockPath: INSTANCE_LOCK_PATH });
      return lock;
    } catch (err) {
      if (!(err instanceof Error) || !("code" in err) || err.code !== "EEXIST") {
        log("sync engine disabled — failed to acquire lock", {
          pid: process.pid,
          lockPath: INSTANCE_LOCK_PATH,
          error: String(err),
        });
        return null;
      }

      const owner = readLock();
      if (owner && isProcessAlive(owner.pid)) {
        if (owner.pid === process.pid) {
          log("sync engine disabled — already active in this opencode process", {
            pid: process.pid,
            ownerStartedAt: owner.startedAt,
            lockPath: INSTANCE_LOCK_PATH,
          });
          return null;
        }

        if (isLockHeartbeatStale()) {
          log("removing stuck sync engine lock", {
            pid: process.pid,
            ownerPid: owner.pid,
            ownerStartedAt: owner.startedAt,
            lockAgeMs: lockAgeMs(),
            lockPath: INSTANCE_LOCK_PATH,
          });
          try {
            fs.rmSync(INSTANCE_LOCK_PATH, { force: true });
          } catch (rmErr) {
            log("sync engine disabled — failed to remove stuck lock", {
              pid: process.pid,
              ownerPid: owner.pid,
              lockPath: INSTANCE_LOCK_PATH,
              error: String(rmErr),
            });
            return null;
          }
          continue;
        }

        log("sync engine disabled — another opencode-sync process owns lock", {
          pid: process.pid,
          ownerPid: owner.pid,
          ownerStartedAt: owner.startedAt,
          lockAgeMs: lockAgeMs(),
          lockPath: INSTANCE_LOCK_PATH,
        });
        return null;
      }

      log("removing stale sync engine lock", {
        pid: process.pid,
        staleOwnerPid: owner?.pid ?? null,
        lockPath: INSTANCE_LOCK_PATH,
      });
      try {
        fs.rmSync(INSTANCE_LOCK_PATH, { force: true });
      } catch (rmErr) {
        log("sync engine disabled — failed to remove stale lock", {
          pid: process.pid,
          lockPath: INSTANCE_LOCK_PATH,
          error: String(rmErr),
        });
        return null;
      }
    }
  }

  log("sync engine disabled — lock acquisition retry exhausted", {
    pid: process.pid,
    lockPath: INSTANCE_LOCK_PATH,
  });
  return null;
}
