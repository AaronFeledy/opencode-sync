#!/usr/bin/env bash
#
# install-server.sh — first-time install (or in-place re-install) of the
# opencode-sync ledger server as a systemd unit.
#
# What it does, in order:
#   1. Verifies prereqs (bun, systemd, openssl, curl).
#   2. Prompts for the listen address (host:port).
#   3. Builds server/dist/opencode-sync-server from the current checkout.
#   4. Creates the `opencode-sync` system user if missing.
#   5. Writes /etc/opencode-sync/env (preserves an existing token if one
#      is already present — safe to re-run for upgrades).
#   6. Installs the binary as a real file at /usr/local/bin/opencode-sync-server.
#      Explicitly refuses to leave a symlink there: the systemd unit ships
#      with `ProtectHome=true`, so a symlink into $HOME would resolve to
#      nothing visible to the service and the unit would crash-loop with
#      status=203/EXEC.
#   7. Installs the systemd unit, enables it, starts it.
#   8. Health-checks the listen address.
#
# Idempotent. Run again after `git pull` to redeploy.
#
# Run as your normal user — the script invokes sudo for the steps that
# need it. Don't run with `sudo bash install-server.sh` (the build step
# would then run as root and chown your node_modules / dist tree).

set -euo pipefail

# ── Locate repo root ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
UNIT_SRC="$REPO_ROOT/server/systemd/opencode-sync.service"
BIN_SRC="$REPO_ROOT/server/dist/opencode-sync-server"

# ── Pre-flight ───────────────────────────────────────────────────────

if [[ $EUID -eq 0 ]]; then
  echo "error: don't run this as root — it uses sudo for the parts that need it." >&2
  echo "       running the bun build as root would chown your node_modules tree." >&2
  exit 1
fi

for cmd in bun systemctl openssl curl install sudo; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command not found: $cmd" >&2
    exit 1
  fi
done

if [[ ! -f "$REPO_ROOT/package.json" ]]; then
  echo "error: cannot find package.json at $REPO_ROOT — script must live in scripts/ inside the repo" >&2
  exit 1
fi

if [[ ! -f "$UNIT_SRC" ]]; then
  echo "error: $UNIT_SRC missing — clone may be incomplete" >&2
  exit 1
fi

echo "==> opencode-sync server installer"
echo "    repo:        $REPO_ROOT"
echo "    unit source: $UNIT_SRC"
echo

# Prime sudo once, so the prompts below don't interleave with build output.
sudo -v

# ── Listen address ───────────────────────────────────────────────────

echo "Network interfaces on this host:"
ip -o -4 addr show 2>/dev/null \
  | awk '$2 != "lo" {split($4, a, "/"); printf "    %-12s %s\n", $2, a[1]}'
echo
echo "Pick the address the server should bind to. Use your private-network"
echo "(ZeroTier / Tailscale) IP — never a public interface, since the only"
echo "auth layer is a bearer token."
echo
read -rp "Listen address (host:port, e.g. 100.x.x.x:4455): " LISTEN

if [[ -z "${LISTEN:-}" ]]; then
  echo "error: listen address is required" >&2
  exit 1
fi
if [[ "$LISTEN" != *:* ]]; then
  echo "error: listen address must be host:port (got '$LISTEN')" >&2
  exit 1
fi

# ── Build ────────────────────────────────────────────────────────────

echo
echo "==> Installing workspace dependencies"
( cd "$REPO_ROOT" && bun install )

echo
echo "==> Building server binary"
( cd "$REPO_ROOT" && bun run --cwd server build )

if [[ ! -x "$BIN_SRC" ]]; then
  echo "error: build did not produce an executable at $BIN_SRC" >&2
  exit 1
fi

# ── Service user ─────────────────────────────────────────────────────

echo
if id opencode-sync >/dev/null 2>&1; then
  echo "==> Service user 'opencode-sync' already exists — skipping"
else
  echo "==> Creating service user 'opencode-sync'"
  sudo useradd --system --no-create-home --shell /usr/sbin/nologin opencode-sync
fi

# ── Env file ─────────────────────────────────────────────────────────

ENV_DIR=/etc/opencode-sync
ENV_FILE="$ENV_DIR/env"

sudo mkdir -p "$ENV_DIR"

# Preserve an existing token across re-runs. Generate a new one only if
# the env file is missing or doesn't have a token line.
TOKEN=""
if sudo test -s "$ENV_FILE"; then
  TOKEN="$(sudo sed -n 's/^OPENCODE_SYNC_TOKEN=//p' "$ENV_FILE" | head -n 1 || true)"
fi

if [[ -z "$TOKEN" ]]; then
  echo
  echo "==> Generating new bearer token"
  TOKEN="$(openssl rand -hex 32)"
  TOKEN_IS_NEW=1
else
  echo
  echo "==> Reusing existing token from $ENV_FILE"
  TOKEN_IS_NEW=0
fi

sudo tee "$ENV_FILE" >/dev/null <<EOF
OPENCODE_SYNC_TOKEN=$TOKEN
OPENCODE_SYNC_LISTEN=$LISTEN
OPENCODE_SYNC_DATA_DIR=/var/lib/opencode-sync
OPENCODE_SYNC_LOG_LEVEL=info
EOF

# Group-readable so the service user can be granted read via group
# membership; systemd itself reads the file as root before dropping
# privileges, so even 0600 root:root would technically work — but 0640
# matches the apparent intent of the chown.
sudo chown root:opencode-sync "$ENV_FILE"
sudo chmod 0640 "$ENV_FILE"

# ── Binary install ───────────────────────────────────────────────────

DEST=/usr/local/bin/opencode-sync-server

echo
echo "==> Installing binary to $DEST"

# A symlink here breaks the unit: ProtectHome=true makes /home invisible
# to the service, so a symlink pointing into the user's checkout would
# resolve to nothing accessible and exec would fail with 203/EXEC.
if [[ -L "$DEST" ]]; then
  echo "    removing existing symlink (this would break the systemd unit)"
  sudo rm "$DEST"
fi

sudo install -m 0755 "$BIN_SRC" "$DEST"

# ── Unit ─────────────────────────────────────────────────────────────

echo
echo "==> Installing systemd unit"
sudo install -m 0644 "$UNIT_SRC" /etc/systemd/system/opencode-sync.service
sudo systemctl daemon-reload

if systemctl is-enabled --quiet opencode-sync 2>/dev/null; then
  echo "==> Restarting opencode-sync"
  sudo systemctl restart opencode-sync
else
  echo "==> Enabling and starting opencode-sync"
  sudo systemctl enable --now opencode-sync
fi

# ── Verify ───────────────────────────────────────────────────────────

echo
echo "==> Waiting for /health on http://$LISTEN ..."
HEALTH_OK=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sSf --max-time 2 "http://$LISTEN/health" >/dev/null 2>&1; then
    HEALTH_OK=1
    break
  fi
  sleep 1
done

if [[ $HEALTH_OK -ne 1 ]]; then
  echo "error: server did not respond on http://$LISTEN/health within 10s" >&2
  echo "       check: sudo journalctl -u opencode-sync -n 50 --no-pager" >&2
  exit 1
fi

echo "    OK — server is up"
echo

# ── Wrap-up ──────────────────────────────────────────────────────────

systemctl status opencode-sync --no-pager -l | sed -n '1,8p'
echo

if [[ $TOKEN_IS_NEW -eq 1 ]]; then
  cat <<EOF
─────────────────────────────────────────────────────────────────────
Bearer token (NEW — save this; it's the only auth layer):

    $TOKEN

Add it to ~/.config/opencode/opencode-sync.jsonc on every client
machine, alongside server_url = "http://$LISTEN":

    {
      "server_url": "http://$LISTEN",
      "token": "$TOKEN",
      "machine_id": "<unique-name-per-machine>"
    }
─────────────────────────────────────────────────────────────────────
EOF
else
  echo "Token unchanged (already present in $ENV_FILE)."
  echo "Clients should already have it in their opencode-sync.jsonc."
fi
