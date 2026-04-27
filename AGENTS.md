# AGENTS.md

Keep this file updated when you identify new repo-specific gotchas or commands that are easy to guess wrong.

`opencode-sync` is a Bun workspace with three packages:

- `shared/`: sync protocol, file-sync scope, and row primary-key helpers shared by the plugin and server.
- `plugin/`: the opencode plugin. Source entry is `plugin/src/index.ts`; the root `package.json` exposes it via `"main": "./plugin/src/index.ts"`, so opencode loads the plugin by adding the clone directory (e.g. `/home/you/projects/opencode-sync`) to its `plugin` array. `bun run --cwd plugin build` still produces a standalone bundle at `plugin/dist/index.js`, but the bundle is not on the load path — don't rely on it for testing the install flow.
- `server/`: the Bun HTTP sync server, compiled to `server/dist/opencode-sync-server` and backed by a SQLite ledger plus blob store under `OPENCODE_SYNC_DATA_DIR` (default `./data`).
