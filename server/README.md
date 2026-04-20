# opencode-sync server

HTTP ledger server that coordinates session and config file sync across machines running opencode. Stores sync envelopes in a SQLite database and file blobs on disk. Designed to run on a VPS accessible via ZeroTier.

## Prerequisites

- **Bun ≥ 1.1**
- All machines on the same **ZeroTier** network

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENCODE_SYNC_TOKEN` | **yes** | — | Shared bearer token for authenticating clients. Generate with `openssl rand -hex 32`. |
| `OPENCODE_SYNC_LISTEN` | no | `0.0.0.0:4455` | `host:port` to bind. Use your ZeroTier IP to avoid exposing on public interfaces. |
| `OPENCODE_SYNC_DATA_DIR` | no | `./data` | Directory for the ledger SQLite DB and content-addressed file blobs. |
| `OPENCODE_SYNC_LOG_LEVEL` | no | `info` | Minimum log level: `debug`, `info`, `warn`, `error`. |

## Development

```bash
# Clone the repo
git clone https://github.com/AaronFeledy/opencode-sync.git ~/projects/opencode-sync
cd ~/projects/opencode-sync

# From the repo root — install all workspace deps
bun install

# Start with hot reload
OPENCODE_SYNC_TOKEN=$(openssl rand -hex 32) bun run server:dev

# Or from the server directory
cd server
OPENCODE_SYNC_TOKEN=dev-token-here bun run dev
```

The server writes its ledger DB and blob store into `OPENCODE_SYNC_DATA_DIR` (defaults to `./data` relative to CWD).

## Production deployment (systemd)

### 1. Clone and build on the VPS

```bash
git clone https://github.com/AaronFeledy/opencode-sync.git ~/opencode-sync
cd ~/opencode-sync
bun install
bun run --cwd server build
# Produces server/dist/opencode-sync-server
sudo cp server/dist/opencode-sync-server /usr/local/bin/opencode-sync-server
```

> The VPS also runs the [opencode-sync plugin](../plugin/README.md) inside its own `opencode serve` instance — link it from the same clone after the server is up.

### 2. Create the service user

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin opencode-sync
```

### 3. Create the env file

```bash
sudo mkdir -p /etc/opencode-sync
sudo tee /etc/opencode-sync/env > /dev/null <<'EOF'
OPENCODE_SYNC_TOKEN=<paste output of: openssl rand -hex 32>
OPENCODE_SYNC_LISTEN=100.x.x.x:4455
OPENCODE_SYNC_DATA_DIR=/var/lib/opencode-sync
OPENCODE_SYNC_LOG_LEVEL=info
EOF
sudo chmod 0600 /etc/opencode-sync/env
sudo chown root:opencode-sync /etc/opencode-sync/env
```

Replace `100.x.x.x` with your VPS's ZeroTier IP.

### 4. Install the systemd unit

```bash
sudo cp ~/opencode-sync/server/systemd/opencode-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now opencode-sync
```

### 5. Verify

```bash
sudo systemctl status opencode-sync
curl http://100.x.x.x:4455/health
```

### Updating

```bash
cd ~/opencode-sync
git pull
bun install
bun run --cwd server build
sudo cp server/dist/opencode-sync-server /usr/local/bin/opencode-sync-server
sudo systemctl restart opencode-sync
```

## API endpoints

All endpoints except `/health` require `Authorization: Bearer <token>`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check. Returns `{ ok, version, time }`. No auth required. |
| `POST` | `/sync/push` | Upload sync envelopes. Body: `{ machine_id, envelopes[] }`. Returns `{ server_seq, accepted[], stale[] }`. |
| `GET` | `/sync/pull?since=<seq>&exclude=<machine_id>&limit=<n>` | Pull envelopes with `server_seq > since`. `exclude` filters out a machine's own rows. `limit` defaults to 500, max 5000. Returns `{ server_seq, envelopes[], more }`. |
| `GET` | `/files/manifest` | Full file manifest array. |
| `GET` | `/files/blob/:sha256` | Download a file blob by content hash. Immutable, cached forever. |
| `PUT` | `/files/:path` | Upload a file. Requires `X-Machine-ID` and `X-Mtime` headers. Body is raw binary. |
| `DELETE` | `/files/:path` | Tombstone a file. Requires `X-Machine-ID` header. |

## Security notes

- **Bind to ZeroTier IP only.** Set `OPENCODE_SYNC_LISTEN=100.x.x.x:4455` so the server is not reachable on public interfaces.
- **ZeroTier encrypts all wire traffic.** No TLS termination is needed on this endpoint.
- **The bearer token is the only auth layer.** Guard the env file (`chmod 0600`) and the plugin config on each client machine.
- **Data is plaintext at rest.** Session content, messages, and (optionally) API keys in `auth.json` are stored unencrypted in the ledger DB and blob store. If your VPS is untrusted, encrypt the filesystem or the data directory.
- **The systemd unit applies hardening:** `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp=true`, dedicated user/group, restricted state directory.
