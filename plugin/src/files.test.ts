import { beforeEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { SyncClient } from "./client.js";
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

  async deleteFile(relpath: string, _machineId: string, mtime: number): Promise<void> {
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
