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
import { AUTH_SYNC_PATH, FILE_SYNC_PATHS, FILE_SYNC_IGNORE } from "@opencode-sync/shared";
import { StaleError, type SyncClient } from "./client.js";
import type { StateManager } from "./state.js";
import { sha256Hex, atomicWriteFile } from "./util.js";

// ── Constants ──────────────────────────────────────────────────────

const CONFIG_BASE = path.join(os.homedir(), ".config", "opencode");
const CONFLICTS_DIR = path.join(CONFIG_BASE, ".sync-conflicts");
const AUTH_FILE_PATH = path.join(os.homedir(), ".local", "share", "opencode", "auth.json");

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
        const absPath = this.resolveConfiguredPath(relRoot);

        if (!fs.existsSync(absPath)) continue;

        const stat = fs.statSync(absPath);
        if (stat.isDirectory()) {
          this.walkDir(absPath, CONFIG_BASE, entries, cache);
        } else if (stat.isFile()) {
          const relpath = this.toManifestRelpath(absPath, relRoot);
          if (this.shouldIgnore(relpath)) continue;

          entries.push(this.buildManifestEntry(absPath, relpath, stat, cache));
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
      remoteManifest = await this.client.getManifest();
    } catch (err) {
      this.log("failed to fetch remote manifest, skipping file sync", {
        error: String(err),
      });
      return { uploaded: 0, downloaded: 0, conflicts: 0 };
    }

    const localManifest = await this.computeLocalManifest();
    const previousLocalFiles = new Map(Object.entries(this.stateManager.state.knownFiles));
    const justDeletedRemote = new Set<string>();

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

      // Only emit a tombstone when the server still has the version we last synced.
      // If the remote file changed while we were offline, prefer the remote copy.
      if (remote.sha256 !== previous.sha256) continue;

      // Stamp the tombstone with the current wall-clock time. The server
      // applies LWW: if another machine has uploaded a newer version since
      // we last pulled, the server returns 409 and `deleteRemoteFile`
      // returns false — we then leave knownFiles untouched and pick up the
      // remote on the next pull.
      const ok = await this.deleteRemoteFile(relpath, Date.now());
      if (ok) {
        justDeletedRemote.add(relpath);
        uploaded++;
        // The file was already absent locally; nothing to mutate in post-sync.
      }
    }

    // ── Process remote entries (download or conflict) ──

    for (const remote of remoteManifest) {
      if (justDeletedRemote.has(remote.relpath)) continue;

      const local = localByPath.get(remote.relpath);

      if (remote.deleted) {
        if (!local) continue;

        // Tombstones win equal-mtime ties (strict `>` here, `<=` in the
        // local-entries pass below). Live-vs-live conflicts at equal mtime
        // are stashed under .sync-conflicts/ instead — but a tombstone has
        // no symmetric "stash" option, and biasing toward propagating
        // deletes prevents tombstones from being silently shadowed when a
        // delete and an unrelated edit happen to land on the same ms.
        if (local.mtime > remote.mtime) continue;

        const ok = await this.deleteLocalFile(remote.relpath);
        if (ok) {
          downloaded++;
          postSyncByPath.delete(remote.relpath);
        }
        continue;
      }

      if (!local) {
        // Remote-only: download
        const ok = await this.downloadFile(remote);
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
        const ok = await this.downloadFile(remote);
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

  private async downloadFile(entry: FileManifestEntry): Promise<boolean> {
    try {
      const blob = await this.client.getBlob(entry.sha256);
      const absPath = this.resolveLocalPath(entry.relpath);
      fs.mkdirSync(path.dirname(absPath), { recursive: true });
      await atomicWriteFile(absPath, Buffer.from(blob));
      fs.utimesSync(absPath, new Date(entry.mtime), new Date(entry.mtime));
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
      const data = new Uint8Array(fs.readFileSync(absPath));
      await this.client.putFile(entry.relpath, data, this.machineId, entry.mtime);
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

  private async deleteRemoteFile(relpath: string, mtime: number): Promise<boolean> {
    try {
      await this.client.deleteFile(relpath, this.machineId, mtime);
      this.log("deleted remote file", { relpath, mtime });
      return true;
    } catch (err) {
      if (err instanceof StaleError) {
        // Expected: the remote file changed since we last pulled. The next
        // pull will reconcile by either re-downloading the live version or
        // applying a newer tombstone.
        this.log("delete rejected as stale, will resync from remote", { relpath });
        return false;
      }
      this.log("failed to delete remote file", {
        relpath,
        error: String(err),
      });
      return false;
    }
  }

  private async deleteLocalFile(relpath: string): Promise<boolean> {
    try {
      const absPath = this.resolveLocalPath(relpath);
      if (!fs.existsSync(absPath)) return true;
      fs.unlinkSync(absPath);
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

  private resolveConfiguredPath(relRoot: string): string {
    if (relRoot === AUTH_SYNC_PATH) return AUTH_FILE_PATH;
    return path.resolve(CONFIG_BASE, relRoot);
  }

  private resolveLocalPath(relpath: string): string {
    if (relpath === AUTH_SYNC_PATH) return AUTH_FILE_PATH;
    return path.join(CONFIG_BASE, relpath);
  }

  private toManifestRelpath(absPath: string, relRoot: string): string {
    if (relRoot === AUTH_SYNC_PATH) return AUTH_SYNC_PATH;
    return path.relative(CONFIG_BASE, absPath);
  }

  private walkDir(
    dirPath: string,
    baseDir: string,
    entries: FileManifestEntry[],
    cache: Record<string, { sha256: string; mtime: number; size: number }>,
  ): void {
    let dirEntries: fs.Dirent[];
    try {
      dirEntries = fs.readdirSync(dirPath, { withFileTypes: true });
    } catch {
      return;
    }

    for (const dirent of dirEntries) {
      const fullPath = path.join(dirPath, dirent.name);
      const relpath = path.relative(baseDir, fullPath);

      if (this.shouldIgnore(relpath)) continue;

      if (dirent.isDirectory()) {
        this.walkDir(fullPath, baseDir, entries, cache);
      } else if (dirent.isFile()) {
        const stat = fs.statSync(fullPath);
        entries.push(this.buildManifestEntry(fullPath, relpath, stat, cache));
      }
    }
  }
}
