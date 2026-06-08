/**
 * File sync orchestration — compare local vs remote file manifests,
 * push/pull changed files, handle conflicts.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type {
  FileSyncConfig,
  FileManifestEntry,
} from "@opencode-sync/shared";
import {
  ANTHROPIC_ACCOUNTS_SYNC_PATH,
  AUTH_LOCK_SYNC_PATH,
  AUTH_SYNC_PATH,
  FILE_SYNC_PATHS,
  FILE_SYNC_IGNORE,
  isHomeRootedRelpath,
} from "@opencode-sync/shared";
import { StaleError, type SyncClient } from "./client.js";
import type { StateManager } from "./state.js";
import { sha256Hex, atomicWriteFile } from "./util.js";

// ── Constants ──────────────────────────────────────────────────────

const HOME_BASE = os.homedir();
const CONFIG_BASE = path.join(HOME_BASE, ".config", "opencode");
const CONFLICTS_DIR = path.join(CONFIG_BASE, ".sync-conflicts");
const AUTH_LOCK_STALE_MS = 30_000;
const AUTH_LOCK_RETRY_MS = 100;
type ReleaseLock = () => Promise<void>;

type AuthJsonProvider = {
  type?: unknown;
  access?: unknown;
  refresh?: unknown;
  expires?: unknown;
};

type AuthJsonShape = Record<string, AuthJsonProvider | unknown>;

const AUTH_SYNC_RELPATHS = new Set([
  AUTH_SYNC_PATH,
  ANTHROPIC_ACCOUNTS_SYNC_PATH,
]);

// ── File sync ──────────────────────────────────────────────────────

export class FileSync {
  private client: SyncClient;
  private machineId: string;
  private config: FileSyncConfig;
  private stateManager: StateManager;
  private log: (msg: string, data?: Record<string, unknown>) => void;

  constructor(
    client: SyncClient,
    machineId: string,
    config: FileSyncConfig,
    stateManager: StateManager,
    log: (msg: string, data?: Record<string, unknown>) => void,
  ) {
    this.client = client;
    this.machineId = machineId;
    this.config = config;
    this.stateManager = stateManager;
    this.log = log;
  }

  /**
   * Compute the local file manifest for all configured sync paths.
   *
   * Reuses the previously-cached SHA when a file's `(mtime, size)` pair
   * matches the last sync — avoids re-reading and re-hashing every
   * tracked file on every sync cycle (default 15s). Standard
   * rsync/make-style heuristic; the only case it can miss is a same-ms
   * edit that doesn't change file length, which is already
   * indistinguishable to the rest of the sync protocol since LWW
   * resolution also keys on mtime.
   *
   * Returns synchronously (despite the `async` signature) — kept as
   * `async` for forward-compat in case future implementations want to
   * parallelise hashing.
   */
  async computeLocalManifest(): Promise<FileManifestEntry[]> {
    const entries: FileManifestEntry[] = [];
    const cache = this.stateManager.state.knownFiles;

    for (const [flag, paths] of Object.entries(FILE_SYNC_PATHS) as [keyof FileSyncConfig, string[]][]) {
      if (!this.config[flag]) continue;

      for (const relRoot of paths) {
        const releaseLock = this.isAuthRelpath(relRoot)
          ? await this.acquireAuthLock()
          : undefined;
        if (this.isAuthRelpath(relRoot) && !releaseLock) {
          // OpenCode holds the auth lock (mid token-refresh). Don't read a
          // possibly half-written file — but don't drop it from the manifest
          // either. Dropping it makes the sync loop treat the path as
          // remote-only (clobbering on-disk creds) and makes replaceKnownFiles
          // forget its tracked state. Carry forward the last-known entry so
          // this cycle treats it as present/unchanged and reconciles later.
          const cached = cache[relRoot];
          if (cached) {
            entries.push({
              relpath: relRoot,
              sha256: cached.sha256,
              size: cached.size,
              mtime: cached.mtime,
              machine_id: this.machineId,
              deleted: false,
            });
          }
          continue;
        }

        try {
          const absPath = this.resolveConfiguredPath(relRoot);

          if (!fs.existsSync(absPath)) continue;

          // `lstatSync` so a symlinked configured root is visible as a
          // symlink and can be refused. Following it could leak arbitrary
          // filesystem paths (e.g. /etc/passwd, a git-managed dotfiles
          // repo with secrets) into the sync stream. See FINDINGS.md M5.
          const stat = fs.lstatSync(absPath);
          if (stat.isSymbolicLink()) {
            this.log("configured sync path is a symlink, skipping", { path: absPath });
            continue;
          }
          if (stat.isDirectory()) {
            this.walkDir(absPath, this.baseDirFor(relRoot), entries, cache);
          } else if (stat.isFile()) {
            const relpath = this.toManifestRelpath(absPath, relRoot);
            if (this.shouldIgnore(relpath)) continue;

            entries.push(this.buildManifestEntry(absPath, relpath, stat, cache));
          }
        } finally {
          await releaseLock?.();
        }
      }
    }

    return entries;
  }

  /**
   * Build a single manifest entry, reusing the cached SHA if `(mtime,
   * size)` matches the last sync.
   */
  private buildManifestEntry(
    absPath: string,
    relpath: string,
    stat: fs.Stats,
    cache: Record<string, { sha256: string; mtime: number; size: number }>,
  ): FileManifestEntry {
    const cached = cache[relpath];
    if (
      cached &&
      cached.mtime === stat.mtimeMs &&
      cached.size === stat.size
    ) {
      return {
        relpath,
        sha256: cached.sha256,
        size: stat.size,
        mtime: stat.mtimeMs,
        machine_id: this.machineId,
        deleted: false,
      };
    }

    const content = fs.readFileSync(absPath);
    return {
      relpath,
      sha256: sha256Hex(content),
      size: stat.size,
      mtime: stat.mtimeMs,
      machine_id: this.machineId,
      deleted: false,
    };
  }

  /**
   * Full file sync: compare local vs remote, push/pull as needed.
   */
  async sync(): Promise<{ uploaded: number; downloaded: number; conflicts: number }> {
    let uploaded = 0;
    let downloaded = 0;
    let conflicts = 0;

    let remoteManifest: FileManifestEntry[];
    try {
      remoteManifest = (await this.client.getManifest()).filter((entry) => {
        return this.isConfiguredRelpath(entry.relpath) && !this.shouldIgnore(entry.relpath);
      });
    } catch (err) {
      this.log("failed to fetch remote manifest, skipping file sync", {
        error: String(err),
      });
      return { uploaded: 0, downloaded: 0, conflicts: 0 };
    }

    const localManifest = await this.computeLocalManifest();
    const previousLocalFiles = new Map(
      Object.entries(this.stateManager.state.knownFiles).filter(([relpath]) => {
        return this.isConfiguredRelpath(relpath) && !this.shouldIgnore(relpath);
      }),
    );
    const justDeletedRemote = new Set<string>();
    // Paths whose remote-delete failed transiently this cycle. Used to
    // skip the remote→local download branch so the file we're trying
    // to delete doesn't get silently resurrected before the retry.
    const pendingRemoteDeletes = new Set<string>();

    // Track the post-sync state of each file as we go, so we can persist
    // `knownFiles` without a second filesystem walk + hash pass at the end.
    // Seeded from the initial local manifest; mutated for each download /
    // local-delete we apply.
    const postSyncByPath = new Map<string, FileManifestEntry>();
    for (const entry of localManifest) {
      postSyncByPath.set(entry.relpath, entry);
    }

    // Build lookup maps
    const remoteByPath = new Map<string, FileManifestEntry>();
    for (const entry of remoteManifest) {
      remoteByPath.set(entry.relpath, entry);
    }

    const localByPath = new Map<string, FileManifestEntry>();
    for (const entry of localManifest) {
      localByPath.set(entry.relpath, entry);
    }

    // ── Push local deletions observed since the last successful sync ──

    for (const [relpath, previous] of previousLocalFiles) {
      if (localByPath.has(relpath)) continue;

      const remote = remoteByPath.get(relpath);
      if (!remote || remote.deleted) continue;

      if (this.isAuthRelpath(relpath)) continue;

      // Only emit a tombstone when the server still has the version we last synced.
      // If the remote file changed while we were offline, prefer the remote copy.
      if (remote.sha256 !== previous.sha256) continue;

      // Stamp the tombstone with the current wall-clock time. The server
      // applies LWW:
      //   - "ok":    remote accepted our tombstone, drop the entry.
      //   - "stale": another machine uploaded a newer version since we
      //              last pulled. Drop the entry; next pull will pick up
      //              the remote and re-download.
      //   - "error": transient failure (network blip, 5xx). Retain the
      //              previous entry in `postSyncByPath` so next cycle's
      //              `previousLocalFiles` still contains it, the
      //              deletion-detection loop re-fires, and the user's
      //              delete survives the failure. Without this, the
      //              remote-only download branch would silently resurrect
      //              the file — see FINDINGS.md H1.
      const result = await this.deleteRemoteFile(relpath, Date.now());
      if (result === "ok") {
        justDeletedRemote.add(relpath);
        uploaded++;
      } else if (result === "error") {
        // previousLocalFiles entries only carry {sha256, mtime, size} —
        // rebuild a full FileManifestEntry so replaceKnownFiles can
        // persist it on the next save.
        postSyncByPath.set(relpath, {
          relpath,
          sha256: previous.sha256,
          mtime: previous.mtime,
          size: previous.size,
          machine_id: this.machineId,
          deleted: false,
        });
        pendingRemoteDeletes.add(relpath);
      }
      // "stale": deliberately drop — next pull reconciles to remote.
    }

    // ── Process remote entries (download or conflict) ──

    for (const remote of remoteManifest) {
      if (justDeletedRemote.has(remote.relpath)) continue;
      // Skip downloads for paths whose tombstone upload we're retrying —
      // otherwise the remote-only download branch would silently
      // resurrect the file the user just deleted (FINDINGS.md H1).
      if (pendingRemoteDeletes.has(remote.relpath)) continue;

      const local = localByPath.get(remote.relpath);

      if (remote.deleted) {
        if (!local) continue;

        if (this.isAuthRelpath(remote.relpath)) {
          const kept = await this.keepLocalAuthOverRemoteTombstone(remote, local);
          if (kept.uploaded) {
            uploaded++;
          }
          postSyncByPath.set(remote.relpath, kept.entry);
          continue;
        }

        // Tombstones win equal-mtime ties (strict `>` here, `<=` in the
        // local-entries pass below). Live-vs-live conflicts at equal mtime
        // are stashed under .sync-conflicts/ instead — but a tombstone has
        // no symmetric "stash" option, and biasing toward propagating
        // deletes prevents tombstones from being silently shadowed when a
        // delete and an unrelated edit happen to land on the same ms.
        if (local.mtime > remote.mtime) continue;

        const ok = await this.deleteLocalFile(remote.relpath, local);
        if (ok) {
          downloaded++;
          postSyncByPath.delete(remote.relpath);
        }
        continue;
      }

      if (!local) {
        // Remote-only: download
        const ok = await this.downloadFile(remote, local);
        if (ok) {
          downloaded++;
          postSyncByPath.set(remote.relpath, { ...remote, machine_id: this.machineId });
        }
        continue;
      }

      // Same content — nothing to do
      if (local.sha256 === remote.sha256) continue;

      // Remote is newer — download
      if (remote.mtime > local.mtime) {
        const ok = await this.downloadFile(remote, local);
        if (ok) {
          downloaded++;
          postSyncByPath.set(remote.relpath, { ...remote, machine_id: this.machineId });
        }
        continue;
      }

      // Local is newer — will be uploaded in the next pass
      if (local.mtime > remote.mtime) continue;

      // Same mtime but different content — conflict
      this.log("conflict detected, keeping local copy", { relpath: remote.relpath });
      await this.saveConflict(remote);
      conflicts++;
    }

    // ── Process local entries (upload new/modified) ──

    for (const local of localManifest) {
      const remote = remoteByPath.get(local.relpath);

      if (!remote) {
        // Local-only: upload
        const ok = await this.uploadFile(local);
        if (ok) uploaded++;
        continue;
      }

      if (remote.deleted) {
        // Remote tombstone is newer or equal — keep it.
        if (local.mtime <= remote.mtime) continue;

        const ok = await this.uploadFile(local);
        if (ok) uploaded++;
        continue;
      }

      // Same content — skip
      if (local.sha256 === remote.sha256) continue;

      // Local is newer — upload
      if (local.mtime > remote.mtime) {
        const ok = await this.uploadFile(local);
        if (ok) uploaded++;
      }
    }

    if (uploaded > 0 || downloaded > 0 || conflicts > 0) {
      this.log("file sync complete", { uploaded, downloaded, conflicts });
    }

    // Persist the tracked post-sync state instead of re-walking the
    // filesystem and re-hashing every configured file.
    this.stateManager.replaceKnownFiles([...postSyncByPath.values()]);

    return { uploaded, downloaded, conflicts };
  }

  // ── Private helpers ─────────────────────────────────────────────

  private async downloadFile(
    entry: FileManifestEntry,
    expectedLocal?: FileManifestEntry,
  ): Promise<boolean> {
    try {
      const blob = await this.client.getBlob(entry.sha256);
      const absPath = this.resolveLocalPath(entry.relpath);
      const releaseLock = this.isAuthRelpath(entry.relpath)
        ? await this.acquireAuthLock()
        : undefined;
      if (this.isAuthRelpath(entry.relpath) && !releaseLock) return false;
      let keepLocalAuth = false;
      try {
        const remoteData = Buffer.from(blob);
        keepLocalAuth = this.shouldKeepLocalAuth(entry.relpath, absPath, remoteData);
        if (
          !keepLocalAuth &&
          this.isAuthRelpath(entry.relpath) &&
          !expectedLocal &&
          fs.existsSync(absPath)
        ) {
          // Local auth file exists but was absent from this cycle's manifest
          // (lock-skipped with no cached entry). Don't overwrite credentials
          // we never compared against — let a later cycle reconcile.
          this.log("local auth present but untracked, skipping remote download", {
            relpath: entry.relpath,
          });
          return false;
        }
        if (
          !keepLocalAuth &&
          expectedLocal &&
          this.isAuthRelpath(entry.relpath) &&
          !this.currentFileMatches(entry.relpath, expectedLocal)
        ) {
          this.log("auth file changed during sync, skipping remote download", {
            relpath: entry.relpath,
          });
          return false;
        }
        if (!keepLocalAuth) {
          fs.mkdirSync(path.dirname(absPath), { recursive: true });
          await atomicWriteFile(absPath, remoteData);
          if (this.isAuthRelpath(entry.relpath)) fs.chmodSync(absPath, 0o600);
          fs.utimesSync(absPath, new Date(entry.mtime), new Date(entry.mtime));
        }
      } finally {
        await releaseLock?.();
      }
      if (keepLocalAuth) {
        const pushed = await this.uploadCurrentFile(
          entry.relpath,
          Math.max(Date.now(), entry.mtime + 1),
        );
        if (pushed) {
          this.log("kept local auth file over stale remote", { relpath: entry.relpath });
        }
        return false;
      }
      this.log("downloaded", { relpath: entry.relpath });
      return true;
    } catch (err) {
      this.log("failed to download file", {
        relpath: entry.relpath,
        error: String(err),
      });
      return false;
    }
  }

  private async uploadFile(entry: FileManifestEntry): Promise<boolean> {
    try {
      const absPath = this.resolveLocalPath(entry.relpath);
      const releaseLock = this.isAuthRelpath(entry.relpath)
        ? await this.acquireAuthLock()
        : undefined;
      if (this.isAuthRelpath(entry.relpath) && !releaseLock) return false;
      let data: Uint8Array;
      let mtime = entry.mtime;
      try {
        data = new Uint8Array(fs.readFileSync(absPath));
        if (this.isAuthRelpath(entry.relpath)) {
          mtime = fs.statSync(absPath).mtimeMs;
        }
      } finally {
        await releaseLock?.();
      }
      await this.client.putFile(entry.relpath, data, this.machineId, mtime);
      this.log("uploaded", { relpath: entry.relpath });
      return true;
    } catch (err) {
      if (err instanceof StaleError) {
        // Expected: another machine uploaded a newer version; the next pull
        // will bring it down. Not an error condition.
        this.log("upload rejected as stale, will resync from remote", {
          relpath: entry.relpath,
        });
        return false;
      }
      this.log("failed to upload file", {
        relpath: entry.relpath,
        error: String(err),
      });
      return false;
    }
  }

  private async deleteRemoteFile(
    relpath: string,
    mtime: number,
  ): Promise<"ok" | "stale" | "error"> {
    try {
      await this.client.deleteFile(relpath, this.machineId, mtime);
      this.log("deleted remote file", { relpath, mtime });
      return "ok";
    } catch (err) {
      if (err instanceof StaleError) {
        // Expected: the remote file changed since we last pulled. The next
        // pull will reconcile by either re-downloading the live version or
        // applying a newer tombstone.
        this.log("delete rejected as stale, will resync from remote", { relpath });
        return "stale";
      }
      this.log("failed to delete remote file", {
        relpath,
        error: String(err),
      });
      return "error";
    }
  }

  private async deleteLocalFile(
    relpath: string,
    expectedLocal?: FileManifestEntry,
  ): Promise<boolean> {
    try {
      const absPath = this.resolveLocalPath(relpath);
      const releaseLock = this.isAuthRelpath(relpath)
        ? await this.acquireAuthLock()
        : undefined;
      if (this.isAuthRelpath(relpath) && !releaseLock) return false;
      try {
        if (!fs.existsSync(absPath)) return true;
        if (
          expectedLocal &&
          this.isAuthRelpath(relpath) &&
          !this.currentFileMatches(relpath, expectedLocal)
        ) {
          this.log("auth file changed during sync, skipping local delete", { relpath });
          return false;
        }
        fs.unlinkSync(absPath);
      } finally {
        await releaseLock?.();
      }
      this.log("deleted local file", { relpath });
      return true;
    } catch (err) {
      this.log("failed to delete local file", {
        relpath,
        error: String(err),
      });
      return false;
    }
  }

  private async keepLocalAuthOverRemoteTombstone(
    remote: FileManifestEntry,
    local: FileManifestEntry,
  ): Promise<{ entry: FileManifestEntry; uploaded: boolean }> {
    try {
      const pushed = await this.uploadCurrentFile(
        remote.relpath,
        Math.max(Date.now(), remote.mtime + 1),
      );
      if (!pushed) {
        // Lock contention prevented the upload — the server still holds its
        // tombstone. Report no upload and keep the prior tracked entry so the
        // sync counters and knownFiles state stay truthful; retry next cycle.
        this.log("deferred keeping local auth over remote tombstone (lock busy)", {
          relpath: remote.relpath,
        });
        return { entry: local, uploaded: false };
      }
      const absPath = this.resolveLocalPath(remote.relpath);
      const stat = fs.statSync(absPath);
      this.log("kept local auth file over remote tombstone", { relpath: remote.relpath });
      return {
        entry: {
          relpath: remote.relpath,
          sha256: sha256Hex(fs.readFileSync(absPath)),
          size: stat.size,
          mtime: stat.mtimeMs,
          machine_id: this.machineId,
          deleted: false,
        },
        uploaded: true,
      };
    } catch (err) {
      this.log("failed to keep local auth file over remote tombstone", {
        relpath: remote.relpath,
        error: String(err),
      });
      return { entry: local, uploaded: false };
    }
  }

  private async saveConflict(remote: FileManifestEntry): Promise<void> {
    try {
      // Name the conflict copy after the remote sha256 (not wall-clock time)
      // so repeated detections of the SAME unresolved conflict are
      // idempotent. With a timestamp in the name, every periodic sync
      // (default 15s) would write a fresh file forever — the equal-mtime
      // conflict branch above doesn't mutate either side, so the same
      // conflict re-fires on every cycle until the user resolves it.
      const ext = path.extname(remote.relpath);
      const base = path.basename(remote.relpath, ext);
      const shortSha = remote.sha256.slice(0, 12);
      const conflictName = `${base}.conflict-${remote.machine_id}-${shortSha}${ext}`;
      const conflictPath = path.join(CONFLICTS_DIR, conflictName);

      // Already saved this exact (path, remote-version) pair — skip the
      // network round-trip and the disk write.
      if (fs.existsSync(conflictPath)) return;

      const blob = await this.client.getBlob(remote.sha256);
      fs.mkdirSync(CONFLICTS_DIR, { recursive: true });
      await atomicWriteFile(conflictPath, Buffer.from(blob));
      this.log("saved conflict copy", { path: conflictPath });
    } catch (err) {
      this.log("failed to save conflict copy", {
        relpath: remote.relpath,
        error: String(err),
      });
    }
  }

  private shouldIgnore(relpath: string): boolean {
    if (relpath === AUTH_LOCK_SYNC_PATH || relpath.startsWith(`${AUTH_LOCK_SYNC_PATH}/`)) {
      return true;
    }
    for (const pattern of FILE_SYNC_IGNORE) {
      if (pattern.endsWith("/")) {
        // Directory prefix match — must match the full directory name
        if (relpath.startsWith(pattern) || relpath === pattern.slice(0, -1)) {
          return true;
        }
      } else {
        // Exact filename match
        if (relpath === pattern || path.basename(relpath) === pattern) {
          return true;
        }
      }
    }
    return false;
  }

  private baseDirFor(relpath: string): string {
    return isHomeRootedRelpath(relpath) ? HOME_BASE : CONFIG_BASE;
  }

  private resolveConfiguredPath(relRoot: string): string {
    return path.resolve(this.baseDirFor(relRoot), relRoot);
  }

  private resolveLocalPath(relpath: string): string {
    return path.join(this.baseDirFor(relpath), relpath);
  }

  private toManifestRelpath(absPath: string, relRoot: string): string {
    return path.relative(this.baseDirFor(relRoot), absPath);
  }

  private walkDir(
    dirPath: string,
    baseDir: string,
    entries: FileManifestEntry[],
    cache: Record<string, { sha256: string; mtime: number; size: number }>,
    depth: number = 0,
  ): void {
    // Defense-in-depth against pathological directory trees. With the
    // symlink skip below, cycles are already prevented; this just caps
    // cost for users with unusually deep real trees.
    if (depth > 16) {
      this.log("walkDir depth limit exceeded, skipping", { dirPath });
      return;
    }

    let dirEntries: fs.Dirent[];
    try {
      dirEntries = fs.readdirSync(dirPath, { withFileTypes: true });
    } catch {
      return;
    }

    for (const dirent of dirEntries) {
      // Refuse to follow symlinks — both to prevent leaking arbitrary
      // filesystem paths into the sync stream and to prevent recursion
      // cycles from symlinks-to-ancestors. See FINDINGS.md M5.
      if (dirent.isSymbolicLink()) continue;

      const fullPath = path.join(dirPath, dirent.name);
      const relpath = path.relative(baseDir, fullPath);

      if (this.shouldIgnore(relpath)) continue;

      if (dirent.isDirectory()) {
        this.walkDir(fullPath, baseDir, entries, cache, depth + 1);
      } else if (dirent.isFile()) {
        // `lstatSync` so a swap-to-symlink race between readdir and stat
        // doesn't slip through. Belt-and-braces with the dirent check.
        let stat: fs.Stats;
        try {
          stat = fs.lstatSync(fullPath);
        } catch {
          continue;
        }
        if (stat.isSymbolicLink()) continue;
        entries.push(this.buildManifestEntry(fullPath, relpath, stat, cache));
      }
    }
  }

  private isAuthRelpath(relpath: string): boolean {
    return AUTH_SYNC_RELPATHS.has(relpath);
  }

  private isConfiguredRelpath(relpath: string): boolean {
    for (const [flag, roots] of Object.entries(FILE_SYNC_PATHS) as [keyof FileSyncConfig, string[]][]) {
      if (!this.config[flag]) continue;
      for (const root of roots) {
        if (relpath === root || relpath.startsWith(`${root}/`)) return true;
      }
    }
    return false;
  }

  private authLockPath(): string {
    return this.resolveLocalPath(AUTH_LOCK_SYNC_PATH);
  }

  /**
   * Returns the lock's mtime when it looks abandoned (older than the stale
   * window), otherwise null. The caller re-checks this exact mtime right
   * before stealing so a holder that refreshed its lock in the meantime is
   * not robbed mid-write.
   */
  private staleAuthLockMtime(): number | null {
    try {
      const stats = fs.statSync(this.authLockPath());
      return Date.now() - stats.mtimeMs > AUTH_LOCK_STALE_MS ? stats.mtimeMs : null;
    } catch {
      return null;
    }
  }

  private async acquireAuthLock(): Promise<ReleaseLock | undefined> {
    const waitMs = Number(process.env.OPENCODE_SYNC_AUTH_LOCK_WAIT_MS ?? 5_000);
    const deadline = Date.now() + waitMs;
    fs.mkdirSync(path.dirname(this.authLockPath()), { recursive: true });
    while (true) {
      try {
        fs.mkdirSync(this.authLockPath(), { recursive: false });
        return async () => {
          await fs.promises.rm(this.authLockPath(), { force: true, recursive: true }).catch(() => {});
        };
      } catch (error) {
        const code = error instanceof Error ? (error as NodeJS.ErrnoException).code : undefined;
        if (code !== "EEXIST") throw error;
      }

      const staleMtime = this.staleAuthLockMtime();
      if (staleMtime !== null) {
        // Re-stat immediately before stealing: only remove the lock if its
        // mtime is unchanged since we judged it stale. If the holder bumped
        // the mtime (still alive, still writing), back off instead of
        // stealing and racing a concurrent auth write.
        let current: number | null = null;
        try {
          current = fs.statSync(this.authLockPath()).mtimeMs;
        } catch {
          current = null;
        }
        if (current === staleMtime) {
          await fs.promises
            .rm(this.authLockPath(), { force: true, recursive: true })
            .catch(() => {});
        }
        continue;
      }

      if (Date.now() >= deadline) {
        this.log("auth lock active, skipping auth file this cycle", {
          lock: this.authLockPath(),
        });
        return undefined;
      }
      await new Promise((resolve) => setTimeout(resolve, AUTH_LOCK_RETRY_MS));
    }
  }

  private shouldKeepLocalAuth(relpath: string, localPath: string, remoteData: Buffer): boolean {
    if (!fs.existsSync(localPath)) return false;
    if (relpath === ANTHROPIC_ACCOUNTS_SYNC_PATH) {
      return this.localAccountStoreHasFresherToken(localPath, remoteData);
    }
    if (relpath !== AUTH_SYNC_PATH) return false;
    const local = this.parseAuthJson(fs.readFileSync(localPath, "utf-8"));
    const remote = this.parseAuthJson(remoteData.toString("utf-8"));
    const localAnthropic = this.oauthProvider(local?.anthropic);
    const remoteAnthropic = this.oauthProvider(remote?.anthropic);
    if (!localAnthropic || !remoteAnthropic) return false;
    // Freshness is decided purely by `expires`, which covers both a refresh
    // rotation (different refresh token) and a plain access-token refresh
    // (same refresh token, later expiry). Short-circuiting on equal refresh
    // would let a stale remote with a newer mtime overwrite a still-valid
    // local access token.
    return localAnthropic.expires > remoteAnthropic.expires;
  }

  private currentFileMatches(relpath: string, expected: FileManifestEntry): boolean {
    const absPath = this.resolveLocalPath(relpath);
    if (!fs.existsSync(absPath)) return false;
    const stat = fs.statSync(absPath);
    if (stat.size !== expected.size || stat.mtimeMs !== expected.mtime) return false;
    return sha256Hex(fs.readFileSync(absPath)) === expected.sha256;
  }

  private localAccountStoreHasFresherToken(localPath: string, remoteData: Buffer): boolean {
    const local = this.parseAccountStore(fs.readFileSync(localPath, "utf-8"));
    const remote = this.parseAccountStore(remoteData.toString("utf-8"));
    if (!local.length || !remote.length) return false;
    const remoteByRefresh = new Map(remote.map((account) => [account.refresh, account.expires]));
    for (const account of local) {
      const remoteExpires = remoteByRefresh.get(account.refresh);
      if (typeof remoteExpires === "undefined") {
        if (account.expires > Math.max(...remote.map((remoteAccount) => remoteAccount.expires))) {
          return true;
        }
        continue;
      }
      if (account.expires > remoteExpires) return true;
    }
    return false;
  }

  private parseAuthJson(text: string): AuthJsonShape | undefined {
    try {
      const value = JSON.parse(text) as unknown;
      if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
      return value as AuthJsonShape;
    } catch {
      return undefined;
    }
  }

  private oauthProvider(value: unknown): { refresh: string; expires: number } | undefined {
    if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
    const provider = value as AuthJsonProvider;
    if (provider.type !== "oauth") return undefined;
    if (typeof provider.refresh !== "string") return undefined;
    if (typeof provider.expires !== "number") return undefined;
    return { refresh: provider.refresh, expires: provider.expires };
  }

  private parseAccountStore(text: string): Array<{ refresh: string; expires: number }> {
    try {
      const value = JSON.parse(text) as { accounts?: unknown };
      if (!Array.isArray(value.accounts)) return [];
      return value.accounts.flatMap((account) => {
        const provider = this.oauthProvider(account);
        return provider ? [provider] : [];
      });
    } catch {
      return [];
    }
  }

  private async uploadCurrentFile(relpath: string, mtime: number): Promise<boolean> {
    const absPath = this.resolveLocalPath(relpath);
    const releaseLock = this.isAuthRelpath(relpath) ? await this.acquireAuthLock() : undefined;
    if (this.isAuthRelpath(relpath) && !releaseLock) return false;
    let data: Uint8Array;
    try {
      data = new Uint8Array(fs.readFileSync(absPath));
      fs.utimesSync(absPath, new Date(mtime), new Date(mtime));
    } finally {
      await releaseLock?.();
    }
    await this.client.putFile(relpath, data, this.machineId, mtime);
    return true;
  }
}
