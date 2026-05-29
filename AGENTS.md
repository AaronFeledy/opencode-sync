# AGENTS.md

Keep this file updated when you identify new repo-specific gotchas or commands that are easy to guess wrong.

`opencode-sync` is a Bun workspace with three packages:

- `shared/`: sync protocol, file-sync scope, and row primary-key helpers shared by the plugin and server.
- `plugin/`: the opencode plugin. Source entry is `plugin/src/index.ts`; the root `package.json` exposes it via `"main": "./plugin/src/index.ts"`, so opencode loads the plugin by adding the clone directory (e.g. `/home/you/projects/opencode-sync`) to its `plugin` array. `bun run --cwd plugin build` still produces a standalone bundle at `plugin/dist/index.js`, but the bundle is not on the load path — don't rely on it for testing the install flow.
- `server/`: the Bun HTTP sync server, compiled to `server/dist/opencode-sync-server` and backed by a SQLite ledger plus blob store under `OPENCODE_SYNC_DATA_DIR` (default `./data`).

## Gotchas

- **VPS deploys go through `bun run install-server`** (alias for `./scripts/install-server.sh`). Idempotent — first install and re-deploy share the same path. `bun run server:dev` / `server:start` are for working on the server itself, not for serving real peers (no sandboxing, no systemd supervision, won't survive logout).
- **Never symlink the binary into `/usr/local/bin/opencode-sync-server`.** The systemd unit at `server/systemd/opencode-sync.service` sets `ProtectHome=true`, so a symlink that resolves into `/home/...` is invisible inside the service's namespace and the unit crash-loops with `status=203/EXEC`. Use `install -m 0755 …` (a real file) — the script does this and explicitly removes any pre-existing symlink first.
