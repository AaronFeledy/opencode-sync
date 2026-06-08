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
  /**
   * Sync `~/.agents/` (the cross-tool agent home directory used by
   * Claude Code and other agent runners — typically holds shared
   * skills under `~/.agents/skills/`). Off by default because it
   * lives outside opencode's config root.
   */
  home_agents: boolean;
}

export const AUTH_SYNC_PATH = ".local/share/opencode/auth.json";
export const ANTHROPIC_ACCOUNTS_SYNC_PATH = ".local/share/opencode/anthropic-oauth-accounts.json";
export const AUTH_LOCK_SYNC_PATH = `${AUTH_SYNC_PATH}.lock`;

/**
 * Sync prefix for `~/.agents/` — manifest paths look like
 * `.agents/skills/<skill-name>/SKILL.md`. Treated as a "home-rooted"
 * path: resolved relative to `$HOME` rather than `~/.config/opencode/`.
 */
export const HOME_AGENTS_SYNC_PATH = ".agents";

/**
 * Manifest-path prefixes that are resolved relative to `$HOME` instead
 * of the opencode config base. The plugin uses this to map manifest
 * relpaths back to filesystem paths and to choose the right `baseDir`
 * when walking an external sync root.
 */
export const HOME_ROOTED_PATH_PREFIXES: readonly string[] = [
  AUTH_SYNC_PATH,
  ANTHROPIC_ACCOUNTS_SYNC_PATH,
  AUTH_LOCK_SYNC_PATH,
  HOME_AGENTS_SYNC_PATH,
];

export function isHomeRootedRelpath(relpath: string): boolean {
  return HOME_ROOTED_PATH_PREFIXES.some(
    (prefix) => relpath === prefix || relpath.startsWith(prefix + "/"),
  );
}

export const DEFAULT_FILE_SYNC_CONFIG: FileSyncConfig = {
  agents: true,
  commands: true,
  skills: true,
  modes: true,
  agents_md: true,
  opencode_json: true,
  tui_json: true,
  auth_json: false, // off by default — contains API keys
  home_agents: false, // off by default — opt-in cross-tool agent home
};

/**
 * Directories and files to sync, keyed by config flag.
 *
 * Most paths are relative to `~/.config/opencode/`. Entries listed in
 * `HOME_ROOTED_PATH_PREFIXES` (currently `auth_json` and `home_agents`)
 * are resolved relative to `$HOME` instead.
 */
export const FILE_SYNC_PATHS: Record<keyof FileSyncConfig, string[]> = {
  agents: ["agents"],
  commands: ["commands"],
  skills: ["skill"],
  modes: ["modes"],
  agents_md: ["AGENTS.md"],
  opencode_json: [
    "opencode.json",
    "opencode.jsonc",
    "oh-my-openagent.json",
    "oh-my-openagent.jsonc",
  ],
  tui_json: ["tui.json", "tui.jsonc"],
  auth_json: [AUTH_SYNC_PATH, ANTHROPIC_ACCOUNTS_SYNC_PATH],
  home_agents: [HOME_AGENTS_SYNC_PATH],
};

/**
 * Config flags whose files directly affect opencode's running behaviour —
 * its configuration (`opencode.json` / `tui.json`) and credentials
 * (`auth.json`). These are synced ahead of everything else, and when a
 * change to one of them is *pulled down* from another machine the running
 * opencode process must be restarted to pick it up (opencode reads these
 * at startup). Less-critical scopes (agents, commands, skills, modes,
 * `AGENTS.md`, `~/.agents`) are hot-reloaded or read per-invocation, so
 * they neither need to go first nor require a restart.
 */
export const FUNCTIONAL_FILE_SYNC_FLAGS: readonly (keyof FileSyncConfig)[] = [
  "opencode_json",
  "tui_json",
  "auth_json",
];

/**
 * True when `relpath` belongs to a functionality-affecting category
 * (config or auth). Used to (a) sync these paths before anything else and
 * (b) decide whether a pulled-down change warrants a "restart opencode"
 * notice.
 */
export function isFunctionalRelpath(relpath: string): boolean {
  for (const flag of FUNCTIONAL_FILE_SYNC_FLAGS) {
    for (const root of FILE_SYNC_PATHS[flag]) {
      if (relpath === root || relpath.startsWith(`${root}/`)) return true;
    }
  }
  return false;
}

/**
 * Files that should never be synced (hardcoded).
 */
export const FILE_SYNC_IGNORE = [
  "opencode-sync.jsonc",
  "opencode-sync.overrides.jsonc",
  AUTH_LOCK_SYNC_PATH,
  "plugins/",
] as const;
