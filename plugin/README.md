# opencode-sync plugin

An opencode plugin that syncs sessions, messages, and config files across machines through the opencode-sync server. Install it on every machine where you run opencode.

## Installation

This plugin is **not published to npm** — it lives in this repo and is installed by cloning the repo and linking the workspace into opencode's plugin loader.

### 1. Clone and build the repo on the machine

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/projects/opencode-sync
cd ~/projects/opencode-sync
bun install
bun run --cwd plugin build
```

### 2. Link the plugin into opencode

`bun link` registers the package globally on the machine, then opencode's `node_modules` resolves it.

```bash
# Register the workspace as a globally linkable package
cd ~/projects/opencode-sync/plugin
bun link

# Consume the link from opencode's config dir
cd ~/.config/opencode
bun link opencode-sync-plugin
```

### 3. Enable it in `opencode.json` (or `opencode.jsonc`)

```jsonc
{
  "plugins": [
    "opencode-sync-plugin"
  ]
}
```

### Updating

Pull the repo and rebuild — the link stays valid:

```bash
cd ~/projects/opencode-sync && git pull && bun install && bun run --cwd plugin build
```

## Configuration

Create `~/.config/opencode/opencode-sync.jsonc`:

```jsonc
{
  // Sync server URL — use your VPS's ZeroTier IP
  "server_url": "http://100.x.x.x:4455",

  // Auth token — must match OPENCODE_SYNC_TOKEN on the server.
  // Use "token_env" to read from an env var, or "token" for inline.
  "token_env": "OPENCODE_SYNC_TOKEN",
  // "token": "abc123...",

  // Unique name for this machine (defaults to hostname)
  "machine_id": "desktop",

  // Sync interval in seconds (default: 15)
  "sync_interval_sec": 15,

  // Control which config files are synced
  "file_sync": {
    "agents": true,
    "commands": true,
    "skills": true,
    "modes": true,
    "agents_md": true,
    "opencode_json": true,
    "tui_json": true,
    "auth_json": false        // ⚠ opt-in — sends API keys to the server
  }
}
```

> **⚠ `auth_json`**: Enabling this syncs `~/.local/share/opencode/auth.json` (API keys, OAuth tokens) through the server. The data is stored unencrypted on your VPS. Only enable if you trust every machine on the network and the VPS disk.

## Per-machine overrides

Create `~/.config/opencode/opencode-sync.overrides.jsonc` on any machine that needs settings different from the synced `opencode.json`:

```jsonc
// This file is NEVER synced — it stays local to this machine.
{
  "server": { "port": 4096 },
  "model": "anthropic/claude-haiku-4-5"
}
```

Overrides are shallow-merged over synced config at plugin load time. Use this for per-machine ports, model preferences, local paths, or anything that shouldn't propagate.

## What gets synced

### Session data (row-level sync)

| Entity | Synced |
|---|---|
| Sessions | ✓ |
| Messages | ✓ |
| Parts (tool calls, results) | ✓ |
| Todos | ✓ |
| Projects | ✓ |
| Permissions | ✓ |
| Session shares | ✓ |
| Accounts / auth tokens | ✗ (per-machine) |
| Events / event sequences | ✗ (internal log, too noisy) |
| Workspaces / session entries | ✗ (per-machine state) |

### Config files (file-level sync)

| Path | Default |
|---|---|
| `~/.config/opencode/agents/**` | synced |
| `~/.config/opencode/commands/**` | synced |
| `~/.config/opencode/skills/**` | synced |
| `~/.config/opencode/modes/**` | synced |
| `~/.config/opencode/AGENTS.md` | synced |
| `~/.config/opencode/opencode.json` / `.jsonc` | synced |
| `~/.config/opencode/tui.json` / `.jsonc` | synced |
| `~/.local/share/opencode/auth.json` | **off** (opt-in) |

### Never synced

- `~/.local/share/opencode/opencode.db` — rows are synced through the row-level protocol, not by copying the DB file
- `~/.config/opencode/plugins/` — plugins install themselves from npm
- `opencode-sync.overrides.jsonc` — local-only by design
- Plugin state files under `~/.local/share/opencode/opencode-sync/`

## Conflict resolution

**Session data:** Last-writer-wins by `time_updated`. If a row was edited on two machines between syncs, the version with the later timestamp is kept. The losing side's local changes are preserved (not overwritten) and will win on the next push if they're genuinely newer. A toast notification appears in the TUI when a conflict is detected.

**Config files:** Last-writer-wins by mtime. If both sides modified the same file, the local version is kept and the remote version is saved to `~/.local/share/opencode/opencode-sync/conflicts/` for manual review.

**Deletions:** Tombstones are sticky on the server. A machine that was offline can't accidentally resurrect a session deleted on another machine — the server rejects the push unless the resurrecting row has a strictly newer `time_updated`.

## Troubleshooting

### State file location

The plugin persists its sync cursor and metadata at:

```
~/.local/share/opencode/opencode-sync/state.json
```

### Check sync health

Verify the server is reachable from this machine:

```bash
curl http://100.x.x.x:4455/health
```

### Reset sync state

To force a full re-sync from scratch, delete the state file and restart opencode:

```bash
rm ~/.local/share/opencode/opencode-sync/state.json
```

The plugin will pull all rows from the server on the next `server.connected` event.

### Conflict log

All conflict events are appended to:

```
~/.local/share/opencode/opencode-sync/conflicts.log
```

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| Plugin doesn't connect | Wrong `server_url` or ZeroTier not running | Check `zerotier-cli listnetworks` and verify the server IP |
| `401 Unauthorized` | Token mismatch | Ensure `token` / `token_env` matches `OPENCODE_SYNC_TOKEN` on the server |
| Sessions appear on server but not locally | Plugin hasn't pulled yet | Wait for the next sync interval or restart opencode |
| Stale files keep reappearing | Clock skew between machines | Sync system clocks with NTP; check `mtime` values |
