import { beforeEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { StaleError, type SyncClient } from "./client.js";
import { FileSync } from "./files.js";
import { StateManager } from "./state.js";
import { sha256Hex } from "./util.js";
import type { FileManifestEntry, FileSyncConfig } from "@opencode-sync/shared";
import { AUTH_SYNC_PATH } from "@opencode-sync/shared";

const HOME = os.homedir();

// Module-level guard — throws at import time if HOME is not a test sandbox.
// File sync writes into ~/.config/opencode/ and ~/.local/share/opencode/, so
// a wrong HOME would clobber the developer's real config. This runs BEFORE
// any test body and complements the per-test beforeEach guard below.
if (!HOME.includes("opencode-sync-test-home")) {
  throw new Error(
    `Refusing to load file sync tests against non-test HOME: ${HOME}. ` +
      `Run via the workspace 'bun test' script (which sets HOME), or export ` +
      `HOME to a directory containing 'opencode-sync-test-home'.`,
  );
}

const CONFIG_BASE = path.join(HOME, ".config", "opencode");
const CONFLICTS_DIR = path.join(CONFIG_BASE, ".sync-conflicts");
const DATA_BASE = path.join(HOME, ".local", "share", "opencode");
const AUTH_FILE_PATH = path.join(DATA_BASE, "auth.json");
const BASE_CONFIG: FileSyncConfig = {
  agents: false,
  commands: false,
  skills: false,
  modes: false,
  agents_md: false,
  opencode_json: false,
  tui_json: false,
  auth_json: false,
};

class MockClient {
  manifest: FileManifestEntry[] = [];
  blobs = new Map<string, Uint8Array>();
  uploads: Array<{ relpath: string; data: string; mtime: number }> = [];
  deletes: string[] = [];
  deleteMtimes: number[] = [];

  /**
   * Optional failure injection for `deleteFile`. Test code can set this
   * to a function that throws (e.g. `StaleError` or a generic `Error`)
   * to exercise the H1 failed-delete-retention path and the stale-drop
   * path. Default behaviour records the call and succeeds.
   */
  deleteFileImpl?: (
    relpath: string,
    machineId: string,
    mtime: number,
  ) => Promise<void>;

  async getManifest(): Promise<FileManifestEntry[]> {
    return this.manifest.map((entry) => ({ ...entry }));
  }

  async getBlob(sha256: string): Promise<ArrayBuffer> {
    const blob = this.blobs.get(sha256);
    if (!blob) throw new Error(`missing blob: ${sha256}`);

    return Uint8Array.from(blob).buffer;
  }

  async putFile(
    relpath: string,
    data: Uint8Array,
    _machineId: string,
    mtime: number,
  ): Promise<void> {
    this.uploads.push({ relpath, data: Buffer.from(data).toString("utf-8"), mtime });
  }

  async deleteFile(relpath: string, machineId: string, mtime: number): Promise<void> {
    if (this.deleteFileImpl) {
      await this.deleteFileImpl(relpath, machineId, mtime);
      return;
    }
    this.deletes.push(relpath);
    this.deleteMtimes.push(mtime);
  }
}

function writeTrackedFile(filePath: string, content: string, mtime: number): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
  fs.utimesSync(filePath, new Date(mtime), new Date(mtime));
}

function toManifestEntry(
  relpath: string,
  content: string,
  mtime: number,
  machineId: string,
  deleted: boolean = false,
): FileManifestEntry {
  return {
    relpath,
    sha256: sha256Hex(content),
    size: Buffer.byteLength(content),
    mtime,
    machine_id: machineId,
    deleted,
  };
}

beforeEach(() => {
  if (!HOME.includes("opencode-sync-test-home")) {
    throw new Error(`Refusing to run file sync tests against non-test HOME: ${HOME}`);
  }

  fs.rmSync(CONFIG_BASE, { recursive: true, force: true });
  fs.rmSync(DATA_BASE, { recursive: true, force: true });
});

test("applies remote tombstones without re-uploading the deleted file", async () => {
  const relpath = "agents/custom.md";
  const absPath = path.join(CONFIG_BASE, relpath);
  writeTrackedFile(absPath, "local copy\n", 1_000);

  const client = new MockClient();
  client.manifest = [toManifestEntry(relpath, "local copy\n", 2_000, "laptop", true)];

  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    new StateManager("desktop"),
    () => {},
  );

  await fileSync.sync();

  expect(fs.existsSync(absPath)).toBe(false);
  expect(client.uploads).toHaveLength(0);
});

test("pushes a tombstone for a locally deleted file and does not redownload it in the same sync", async () => {
  const relpath = "agents/custom.md";
  const previousEntry = toManifestEntry(relpath, "hello\n", 1_000, "desktop");

  const stateManager = new StateManager("desktop");
  stateManager.replaceKnownFiles([previousEntry]);

  const client = new MockClient();
  client.manifest = [{ ...previousEntry }];

  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    stateManager,
    () => {},
  );

  await fileSync.sync();

  expect(client.deletes).toEqual([relpath]);
  expect(client.uploads).toHaveLength(0);
  expect(fs.existsSync(path.join(CONFIG_BASE, relpath))).toBe(false);
});

test("repeated syncs of an unresolved conflict write the conflict copy only once", async () => {
  // Regression: saveConflict used to embed `new Date().toISOString()` in the
  // filename. The conflict branch doesn't mutate either side (local stays,
  // remote stays, both still equal-mtime+different-content), so every periodic
  // sync (default 15s) would land in the same branch and write a fresh
  // .conflict-* file forever — ~240/hour per unresolved conflict. The fix
  // names the file after the remote sha256 so re-detection is idempotent.
  const relpath = "agents/custom.md";
  const absPath = path.join(CONFIG_BASE, relpath);
  const localContent = "local edit\n";
  const remoteContent = "remote edit\n";
  const equalMtime = 5_000;

  writeTrackedFile(absPath, localContent, equalMtime);

  const client = new MockClient();
  const remoteEntry = toManifestEntry(relpath, remoteContent, equalMtime, "laptop");
  client.manifest = [remoteEntry];
  client.blobs.set(remoteEntry.sha256, new TextEncoder().encode(remoteContent));

  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    new StateManager("desktop"),
    () => {},
  );

  const first = await fileSync.sync();
  expect(first.conflicts).toBe(1);
  const afterFirst = fs.readdirSync(CONFLICTS_DIR);
  expect(afterFirst).toHaveLength(1);

  // Second sync: nothing changed on either side, the conflict still exists.
  // Without idempotent naming, this would produce a second .conflict-* file.
  const second = await fileSync.sync();
  expect(second.conflicts).toBe(1);
  const afterSecond = fs.readdirSync(CONFLICTS_DIR);
  expect(afterSecond).toEqual(afterFirst);

  // Local file untouched, conflict copy contains the remote content.
  expect(fs.readFileSync(absPath, "utf-8")).toBe(localContent);
  expect(fs.readFileSync(path.join(CONFLICTS_DIR, afterFirst[0]!), "utf-8")).toBe(
    remoteContent,
  );
});

test("maps auth.json to a server-safe relpath", async () => {
  writeTrackedFile(AUTH_FILE_PATH, '{"token":"secret"}\n', 1_000);

  const client = new MockClient();
  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, auth_json: true },
    new StateManager("desktop"),
    () => {},
  );

  await fileSync.sync();

  expect(client.uploads).toHaveLength(1);
  expect(client.uploads[0]?.relpath).toBe(AUTH_SYNC_PATH);
});

// ── H1 regression: transient delete failure must retain knownFiles entry ──

test("H1: transient delete failure retains knownFiles entry and retries next cycle", async () => {
  // Before the fix, a non-stale error from deleteFile dropped the
  // relpath from postSyncByPath → replaceKnownFiles cleared it →
  // next cycle the remote-only download branch silently resurrected
  // the file. See FINDINGS.md H1.
  const relpath = "agents/custom.md";
  const previousEntry = toManifestEntry(relpath, "hello\n", 1_000, "desktop");

  const stateManager = new StateManager("desktop");
  stateManager.replaceKnownFiles([previousEntry]);

  const client = new MockClient();
  client.manifest = [{ ...previousEntry }];
  client.blobs.set(previousEntry.sha256, new TextEncoder().encode("hello\n"));

  let failNext = true;
  client.deleteFileImpl = async (rp, _machineId, mtime) => {
    if (failNext) {
      failNext = false;
      throw new Error("ECONNREFUSED");
    }
    client.deletes.push(rp);
    client.deleteMtimes.push(mtime);
  };

  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    stateManager,
    () => {},
  );

  // First cycle: delete throws → counts as no-op upload but the
  // previous entry must stay in knownFiles.
  await fileSync.sync();
  expect(client.deletes).toEqual([]);
  expect(stateManager.state.knownFiles[relpath]).toBeDefined();
  expect(stateManager.state.knownFiles[relpath]?.sha256).toBe(previousEntry.sha256);
  // File must NOT have been resurrected locally by the download branch.
  expect(fs.existsSync(path.join(CONFIG_BASE, relpath))).toBe(false);

  // Second cycle: network recovers, delete succeeds, entry is finally
  // dropped.
  await fileSync.sync();
  expect(client.deletes).toEqual([relpath]);
  expect(stateManager.state.knownFiles[relpath]).toBeUndefined();
});

test("H1: stale delete response does NOT cause a retry next cycle", async () => {
  // When the server 409s a delete because the remote has moved on,
  // our delete intent should be dropped (not retried). The next pull's
  // remote-only download branch picks up the fresh remote; if we
  // retained the delete intent instead, we'd repeatedly retry a delete
  // that can never succeed.
  const relpath = "agents/custom.md";
  const oldContent = "hello\n";
  const newContent = "hello from another machine\n";
  const previousEntry = toManifestEntry(relpath, oldContent, 1_000, "desktop");
  const newRemoteEntry = toManifestEntry(relpath, newContent, 5_000, "laptop");

  const stateManager = new StateManager("desktop");
  stateManager.replaceKnownFiles([previousEntry]);

  const client = new MockClient();
  // Remote has advanced to a newer version (different sha + mtime).
  client.manifest = [{ ...newRemoteEntry }];
  client.blobs.set(newRemoteEntry.sha256, new TextEncoder().encode(newContent));

  let deleteCalls = 0;
  client.deleteFileImpl = async () => {
    deleteCalls++;
    throw new StaleError("DELETE", `/files/manifest/${relpath}`, "stale");
  };

  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    stateManager,
    () => {},
  );

  // Cycle 1: sha differs between previous and remote, so the tombstone
  // loop's `remote.sha256 !== previous.sha256` guard short-circuits
  // before even calling deleteFile — we correctly defer to remote.
  await fileSync.sync();
  expect(deleteCalls).toBe(0);
  // File was downloaded from remote — knownFiles now holds the new sha.
  expect(stateManager.state.knownFiles[relpath]?.sha256).toBe(newRemoteEntry.sha256);

  // Cycle 2: another pass should also not attempt delete — local now
  // matches remote, so we're in the "same content" branch.
  await fileSync.sync();
  expect(deleteCalls).toBe(0);
});

// ── M5 regression: walkDir must refuse to follow symlinks ──

test("M5: walkDir skips symlinks to files (no leak, no upload)", async () => {
  const real = path.join(CONFIG_BASE, "agents", "real.md");
  writeTrackedFile(real, "real content\n", 1_000);

  // Create a symlink whose target is outside the sync tree — if we
  // followed it, its contents would be uploaded.
  const secret = path.join(os.tmpdir(), `secret-${Date.now()}.txt`);
  fs.writeFileSync(secret, "SECRET DATA — must not leak\n");
  const link = path.join(CONFIG_BASE, "agents", "link.md");
  fs.symlinkSync(secret, link);

  const client = new MockClient();
  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    new StateManager("desktop"),
    () => {},
  );

  const manifest = await fileSync.computeLocalManifest();
  const paths = manifest.map((e) => e.relpath).sort();
  expect(paths).toEqual(["agents/real.md"]); // link excluded

  fs.unlinkSync(secret);
});

test("M5: walkDir skips symlinks to directories (prevents recursion cycles)", async () => {
  const agentsDir = path.join(CONFIG_BASE, "agents");
  fs.mkdirSync(agentsDir, { recursive: true });
  writeTrackedFile(path.join(agentsDir, "a.md"), "a\n", 1_000);
  // Classic loop: agents/loop -> agents/
  fs.symlinkSync(agentsDir, path.join(agentsDir, "loop"));

  const client = new MockClient();
  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    new StateManager("desktop"),
    () => {},
  );

  // Must complete without stack overflow.
  const manifest = await fileSync.computeLocalManifest();
  expect(manifest.map((e) => e.relpath)).toEqual(["agents/a.md"]);
});

test("M5: symlinked configured root is refused", async () => {
  // Simulate `~/.config/opencode/agents` being a user-created symlink
  // to an external dotfiles repo. We should NOT descend it and upload
  // unrelated content.
  const external = fs.mkdtempSync(path.join(os.tmpdir(), "ext-agents-"));
  writeTrackedFile(path.join(external, "ext.md"), "ext content\n", 1_000);
  fs.mkdirSync(CONFIG_BASE, { recursive: true });
  fs.symlinkSync(external, path.join(CONFIG_BASE, "agents"));

  const client = new MockClient();
  const fileSync = new FileSync(
    client as unknown as SyncClient,
    "desktop",
    { ...BASE_CONFIG, agents: true },
    new StateManager("desktop"),
    () => {},
  );

  const manifest = await fileSync.computeLocalManifest();
  expect(manifest).toHaveLength(0);

  fs.rmSync(external, { recursive: true, force: true });
});
