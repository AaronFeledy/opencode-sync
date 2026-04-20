/**
 * Plugin configuration — loaded from ~/.config/opencode/opencode-sync.jsonc
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DEFAULT_FILE_SYNC_CONFIG, type FileSyncConfig } from "@opencode-sync/shared";
import { stripJsoncComments } from "./util.js";

// ── Public interface ───────────────────────────────────────────────

export interface PluginConfig {
  /** Sync server URL (required) */
  serverUrl: string;
  /** Bearer token for server auth */
  token: string;
  /** Unique identifier for this machine */
  machineId: string;
  /** Whether to sync auth.json (API keys) — default false */
  includeAuth: boolean;
  /** Interval between background sync cycles, in seconds — default 15 */
  syncIntervalSec: number;
  /** Per-category file sync toggles */
  fileSync: FileSyncConfig;
}

// ── Config loading ─────────────────────────────────────────────────

const CONFIG_PATH = path.join(os.homedir(), ".config", "opencode", "opencode-sync.jsonc");

export function loadPluginConfig(): PluginConfig {
  let raw: Record<string, unknown> = {};

  if (fs.existsSync(CONFIG_PATH)) {
    const text = fs.readFileSync(CONFIG_PATH, "utf-8");
    try {
      raw = JSON.parse(stripJsoncComments(text)) as Record<string, unknown>;
    } catch (err) {
      throw new Error(`opencode-sync: failed to parse config at ${CONFIG_PATH}: ${err}`);
    }
  }

  // ── server_url ──
  const serverUrl =
    (raw["server_url"] as string | undefined) ??
    process.env["OPENCODE_SYNC_SERVER_URL"] ??
    "";

  if (!serverUrl) {
    throw new Error(
      `opencode-sync: "server_url" must be set in ${CONFIG_PATH} or via OPENCODE_SYNC_SERVER_URL env var`,
    );
  }

  // ── token ──
  let token = (raw["token"] as string | undefined) ?? "";
  if (!token && typeof raw["token_env"] === "string") {
    token = process.env[raw["token_env"]] ?? "";
  }
  if (!token) {
    token = process.env["OPENCODE_SYNC_TOKEN"] ?? "";
  }
  token = token.trim();

  if (!token) {
    throw new Error(
      `opencode-sync: "token" must be set in ${CONFIG_PATH}, via "token_env", or via OPENCODE_SYNC_TOKEN env var`,
    );
  }

  // ── machine_id ──
  const machineId =
    (raw["machine_id"] as string | undefined) ??
    process.env["OPENCODE_SYNC_MACHINE_ID"] ??
    os.hostname();

  // ── includeAuth ──
  const includeAuth = (raw["include_auth"] as boolean | undefined) ?? false;

  // ── syncIntervalSec ──
  const syncIntervalSec = (raw["sync_interval_sec"] as number | undefined) ?? 15;

  // ── fileSync ──
  const fileSyncRaw = (raw["file_sync"] as Partial<FileSyncConfig> | undefined) ?? {};
  const fileSync: FileSyncConfig = {
    ...DEFAULT_FILE_SYNC_CONFIG,
    ...fileSyncRaw,
  };

  // Honour includeAuth override
  if (includeAuth) {
    fileSync.auth_json = true;
  }

  return {
    serverUrl,
    token,
    machineId,
    includeAuth,
    syncIntervalSec,
    fileSync,
  };
}
