#!/usr/bin/env bash
set -euo pipefail

SERVICE_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
SERVICE_USER="$(id -un)"
WHISPERX_DIR="${WHISPERX_DIR:-/Users/valkary/Documents/whisperx}"
WHISPERX_HOST="${WHISPERX_HOST:-whisperx.orb.local}"
GATEWAY_PUBLIC_HOST="${GATEWAY_PUBLIC_HOST:-transcription-gateway.orb.local}"

sudo apt update
sudo apt install -y \
  build-essential \
  ca-certificates \
  curl \
  git \
  libssl-dev \
  openssl \
  pkg-config \
  sqlite3

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi

export PATH="$HOME/.cargo/bin:$PATH"

cd "$SERVICE_DIR"
cargo build --release

read_whisperx_api_key() {
  if [[ -f "$WHISPERX_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    set -a
    source "$WHISPERX_DIR/.env"
    set +a
    printf '%s' "${WHISPERX_API_KEY:-}"
  fi
}

ensure_env() {
  local whisperx_api_key
  whisperx_api_key="$(read_whisperx_api_key)"
  if [[ -z "$whisperx_api_key" ]]; then
    echo "WHISPERX_API_KEY is missing. Set it in $WHISPERX_DIR/.env or export it before setup." >&2
    exit 1
  fi

  umask 077
  if [[ ! -f .env ]]; then
    cat > .env <<EOF
GATEWAY_HOST=0.0.0.0
GATEWAY_PORT=8080
GATEWAY_API_KEY=$(openssl rand -hex 32)
GATEWAY_WORKER_KEY=$(openssl rand -hex 32)
DATA_DIR=$SERVICE_DIR/data
DATABASE_URL=sqlite://$SERVICE_DIR/data/lookup.db
WHISPERX_BASE_URL=http://$WHISPERX_HOST:8000
WHISPERX_PUBLIC_BASE_URL=http://$WHISPERX_HOST:8000
WHISPERX_API_KEY=$whisperx_api_key
WHISPERX_UPLOAD_SECRET=$(openssl rand -hex 32)
EOF
    return
  fi

  upsert_local_env() {
    local key="$1"
    local value="$2"
    if grep -q "^${key}=" .env; then
      sed -i "s|^${key}=.*|${key}=${value}|" .env
    else
      printf '%s=%s\n' "$key" "$value" >>.env
    fi
  }

  upsert_local_env GATEWAY_HOST 0.0.0.0
  upsert_local_env GATEWAY_PORT 8080
  upsert_local_env DATA_DIR "$SERVICE_DIR/data"
  upsert_local_env DATABASE_URL "sqlite://$SERVICE_DIR/data/lookup.db"
  upsert_local_env WHISPERX_BASE_URL "http://$WHISPERX_HOST:8000"
  upsert_local_env WHISPERX_PUBLIC_BASE_URL "http://$WHISPERX_HOST:8000"
  upsert_local_env WHISPERX_API_KEY "$whisperx_api_key"

  if ! grep -q '^GATEWAY_API_KEY=' .env; then
    upsert_local_env GATEWAY_API_KEY "$(openssl rand -hex 32)"
  fi
  if ! grep -q '^GATEWAY_WORKER_KEY=' .env; then
    upsert_local_env GATEWAY_WORKER_KEY "$(openssl rand -hex 32)"
  fi
  if ! grep -q '^WHISPERX_UPLOAD_SECRET=' .env; then
    upsert_local_env WHISPERX_UPLOAD_SECRET "$(openssl rand -hex 32)"
  fi
}

ensure_env

set -a
# shellcheck disable=SC1091
source .env
set +a

mkdir -p "$DATA_DIR/tmp" "$DATA_DIR/transcripts"

link_whisperx_env() {
  local whisperx_env="$WHISPERX_DIR/.env"
  [[ -f "$whisperx_env" ]] || return 0

  upsert_env() {
    local file="$1"
    local key="$2"
    local value="$3"
    if grep -q "^${key}=" "$file"; then
      sed -i "s|^${key}=.*|${key}=${value}|" "$file"
    else
      printf '%s=%s\n' "$key" "$value" >>"$file"
    fi
  }

  upsert_env "$whisperx_env" GATEWAY_BASE_URL "http://$GATEWAY_PUBLIC_HOST:8080"
  upsert_env "$whisperx_env" GATEWAY_WORKER_KEY "$GATEWAY_WORKER_KEY"
  upsert_env "$whisperx_env" WHISPERX_UPLOAD_SECRET "$WHISPERX_UPLOAD_SECRET"
  if ! grep -q '^WHISPERX_DATA_DIR=' "$whisperx_env"; then
    printf 'WHISPERX_DATA_DIR=%s/data\n' "$WHISPERX_DIR" >>"$whisperx_env"
  fi
}

link_whisperx_env

UNIT_CONTENT="$(
  sed \
    -e "s|@USER@|$SERVICE_USER|g" \
    -e "s|@SERVICE_DIR@|$SERVICE_DIR|g" \
    transcription-gateway.service
)"
printf '%s\n' "$UNIT_CONTENT" | sudo tee /etc/systemd/system/transcription-gateway.service >/dev/null

sudo systemctl daemon-reload
sudo systemctl enable --now transcription-gateway.service

if systemctl is-active --quiet whisperx.service 2>/dev/null; then
  sudo systemctl restart whisperx.service
fi

echo "Transcription gateway is installed."
echo "Client API key: $GATEWAY_API_KEY"
echo "Worker key: $GATEWAY_WORKER_KEY"
echo "Upload secret: $WHISPERX_UPLOAD_SECRET"
echo "Health check: curl -s http://$GATEWAY_PUBLIC_HOST:8080/health"
echo "Logs: sudo journalctl -u transcription-gateway.service -f"
