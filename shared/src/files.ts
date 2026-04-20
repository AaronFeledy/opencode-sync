/**
 * File manifest types for config/agent file sync.
 */

// ── Manifest entry ─────────────────────────────────────────────────

export interface FileManifestEntry {
  /** Relative path, e.g. "agents/planner.md" */
  relpath: string;
  /** SHA-256 hex of current content */
  sha256: string;
  /** File size in bytes */
  size: number;
  /** Last modified time, ms epoch */
  mtime: number;
  /** Machine that last wrote this file */
  machine_id: string;
  /** Tombstone flag */
  deleted: boolean;
}

export type FileManifest = FileManifestEntry[];

// ── File sync scope ────────────────────────────────────────────────

export interface FileSyncConfig {
  agents: boolean;
  commands: boolean;
  skills: boolean;
  modes: boolean;
  agents_md: boolean;
  opencode_json: boolean;
  tui_json: boolean;
  auth_json: boolean;
}

export const AUTH_SYNC_PATH = ".local/share/opencode/auth.json";

export const DEFAULT_FILE_SYNC_CONFIG: FileSyncConfig = {
  agents: true,
  commands: true,
  skills: true,
  modes: true,
  agents_md: true,
  opencode_json: true,
  tui_json: true,
  auth_json: false, // off by default — contains API keys
};

/**
 * Directories and files to sync, keyed by config flag.
 * Paths are relative to ~/.config/opencode/
 */
export const FILE_SYNC_PATHS: Record<keyof FileSyncConfig, string[]> = {
  agents: ["agents"],
  commands: ["commands"],
  skills: ["skills"],
  modes: ["modes"],
  agents_md: ["AGENTS.md"],
  opencode_json: ["opencode.json", "opencode.jsonc"],
  tui_json: ["tui.json", "tui.jsonc"],
  auth_json: [AUTH_SYNC_PATH],
};

/**
 * Files that should never be synced (hardcoded).
 */
export const FILE_SYNC_IGNORE = [
  "opencode-sync.jsonc",
  "opencode-sync.overrides.jsonc",
  "plugins/",
] as const;
