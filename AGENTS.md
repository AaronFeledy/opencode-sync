# AGENTS.md

Keep this file updated when you identify new repo-specific gotchas or commands that are easy to guess wrong.

`opencode-sync` is a Bun workspace with three packages:

- `shared/`: sync protocol, file-sync scope, and row primary-key helpers shared by the plugin and server.
- `plugin/`: the opencode plugin, bundled to `plugin/dist/index.js` and loaded by opencode from `~/.config/opencode/plugins/*.js`.
- `server/`: the Bun HTTP sync server, compiled to `server/dist/opencode-sync-server` and backed by a SQLite ledger plus blob store under `OPENCODE_SYNC_DATA_DIR` (default `./data`).
