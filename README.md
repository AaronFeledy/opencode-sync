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

> **Not on npm.** This is a personal project — both the plugin and the server are installed by cloning this repo on each machine and using `bun link` for the plugin and a `bun build` + systemd unit for the server.

The order matters: **set up the server first**, then link the plugin on every machine that runs opencode (including the VPS itself, since it runs `opencode serve`).

### 1. VPS — server + plugin

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/opencode-sync
cd ~/opencode-sync
bun install
bun run build
```

Then follow [`server/README.md`](./server/README.md) to install the systemd unit, and [`plugin/README.md`](./plugin/README.md) to link the plugin into the VPS's own opencode.

Save the `OPENCODE_SYNC_TOKEN` you generate during server setup — desktop and laptop both need it.

### 2. Desktop and laptop — plugin only

On each machine:

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/projects/opencode-sync
cd ~/projects/opencode-sync
bun install
bun run --cwd plugin build

# Register and consume the link
cd plugin && bun link
cd ~/.config/opencode && bun link opencode-sync-plugin
```

Then add the plugin to the `"plugin"` array in `~/.config/opencode/opencode.json` and create `~/.config/opencode/opencode-sync.jsonc` per [`plugin/README.md`](./plugin/README.md). Use a unique `machine_id` on each machine (e.g. `desktop`, `laptop`, `vps`).

### 3. Updating

On every machine:

```bash
cd ~/opencode-sync   # or wherever you cloned
git pull
bun install
bun run build        # or `bun run --cwd plugin build` on client-only machines
```

On the VPS, also redeploy the server binary and restart the unit (see [`server/README.md`](./server/README.md#updating)).

## License

MIT
