# opencode-sync

Cross-machine session and config sync for [opencode](https://opencode.ai), coordinated through a self-hosted server on your VPS.

> **Status:** alpha / personal project. Expect breakage.

## What it does

- **Sessions, messages, parts, todos, projects** — synced at the SQLite row level between machines (not as a giant whole-DB blob). Per-session conflicts resolve last-writer-wins by `time_updated`, with a TUI toast when a conflict was auto-resolved.
- **Custom agents, commands, skills, modes, `AGENTS.md`** — synced as files with per-file SHA-256 manifest.
- **`opencode.json` / `tui.json`** — synced, with a local-only `opencode-sync.overrides.jsonc` for per-machine tweaks.
- **`auth.json`** — optional; synced over your private network if you enable it.

## Architecture

```
┌──────────┐        ┌─────────────────┐        ┌──────────┐
│ desktop  │◄──────►│      VPS        │◄──────►│  laptop  │
│  plugin  │  HTTP  │  sync-server    │  HTTP  │  plugin  │
│          │  + ZT  │  (systemd unit) │  + ZT  │          │
└──────────┘        │  + opencode     │        └──────────┘
                    │    serve        │
                    └─────────────────┘
```

All traffic runs over [ZeroTier](https://www.zerotier.com/), so HTTP is fine on the wire — ZT provides the encryption. The VPS runs two things:

1. `opencode serve` (its regular always-on instance, used via the web UI)
2. `opencode-sync-server` (this project's ledger + sync coordinator)

## Repository layout

```
opencode-sync/
├── shared/     # TypeScript types shared between plugin and server
├── server/     # Bun HTTP server — runs as a systemd unit on the VPS
└── plugin/     # opencode plugin — loads on all three machines
```

## Requirements

- [Bun](https://bun.com) 1.1+
- opencode 0.15+
- ZeroTier (or any other private network between the three machines)

## Installation

> **Not on npm.** This is a personal project — both the plugin and the server are installed by cloning this repo on each machine. The plugin loads from the clone directory directly (no build step); the server compiles to a single binary deployed via systemd.

The order matters: **set up the server first**, then add the plugin entry on every machine that runs opencode (including the VPS itself, since it runs `opencode serve`).

### 1. VPS — server + plugin

The server is meant to run as a systemd unit. A helper script does the full install:

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/opencode-sync
cd ~/opencode-sync
bun run install-server
```

The script (`scripts/install-server.sh`) builds the binary, creates the service user, generates the bearer token, writes `/etc/opencode-sync/env`, installs the unit, and verifies `/health`. It's idempotent — re-run it after `git pull` to redeploy. Save the token it prints; desktop and laptop both need it.

For a guided/manual walkthrough of the same steps, see [`server/README.md`](./server/README.md). Then follow [`plugin/README.md`](./plugin/README.md) to register the plugin in the VPS's own `opencode.json` (the VPS is also a sync client of itself).

### 2. Desktop and laptop — plugin only

On each machine:

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/projects/opencode-sync
cd ~/projects/opencode-sync
bun install
```

Then add the clone path to `~/.config/opencode/opencode.json`'s `plugin` array:

```jsonc
{
  "plugin": [
    "/home/you/projects/opencode-sync"
  ]
}
```

And create `~/.config/opencode/opencode-sync.jsonc` per [`plugin/README.md`](./plugin/README.md). Use a unique `machine_id` on each machine (e.g. `desktop`, `laptop`, `vps`).

### 3. Updating

On plugin-only machines:

```bash
cd ~/projects/opencode-sync
git pull
bun install
```

On the VPS, re-run the installer instead — it rebuilds the binary, replaces the file at `/usr/local/bin/opencode-sync-server`, and restarts the unit:

```bash
cd ~/opencode-sync && git pull && bun run install-server
```

## License

MIT
