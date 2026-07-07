#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY_REPO="${ZEPPELIN_PY_REPO:-"$ROOT/../zepdb/zeppelin-py"}"
TS_REPO="${ZEPPELIN_TS_REPO:-"$ROOT/../zepdb/zeppelin-typescript"}"
PORT="${ZEPPELIN_E2E_PORT:-8080}"
ZEPPELIN_URL="http://127.0.0.1:$PORT"
MINIO_PORT="${ZEPPELIN_E2E_MINIO_PORT:-19000}"
MINIO_CONSOLE_PORT="${ZEPPELIN_E2E_MINIO_CONSOLE_PORT:-19001}"
MAX_BATCH_SIZE="${ZEPPELIN_E2E_MAX_BATCH_SIZE:-8}"
BUCKET="${ZEPPELIN_E2E_BUCKET:-stormcrow-test}"
COMPOSE_PROJECT="${ZEPPELIN_E2E_COMPOSE_PROJECT:-zeppelin-e2e}"
ENGINE_LOG="${ZEPPELIN_E2E_ENGINE_LOG:-"$ROOT/target/e2e-zeppelin.log"}"
CACHE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/zeppelin-e2e-cache.XXXXXX")"
COMPOSE_OVERRIDE="$CACHE_DIR/docker-compose.override.yml"
ENGINE_PID=""

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "required command not found: $1" >&2
        exit 1
    fi
}

require_dir() {
    if [ ! -d "$1" ]; then
        echo "required directory not found: $1" >&2
        exit 1
    fi
}

if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    echo "required command not found: docker compose or docker-compose" >&2
    exit 1
fi
COMPOSE_FILES=(-f "$ROOT/docker-compose.test.yml" -f "$COMPOSE_OVERRIDE")

cat >"$COMPOSE_OVERRIDE" <<YAML
services:
  minio:
    ports: !override
      - "$MINIO_PORT:9000"
      - "$MINIO_CONSOLE_PORT:9001"
YAML

cleanup() {
    status=$?
    set +e
    if [ -n "$ENGINE_PID" ] && kill -0 "$ENGINE_PID" >/dev/null 2>&1; then
        kill "$ENGINE_PID"
        wait "$ENGINE_PID" >/dev/null 2>&1
    fi
    "${COMPOSE[@]}" -p "$COMPOSE_PROJECT" "${COMPOSE_FILES[@]}" down -v --remove-orphans >/dev/null 2>&1
    rm -rf "$CACHE_DIR"
    exit "$status"
}
trap cleanup EXIT INT TERM

require_cmd cargo
require_cmd curl
require_cmd docker
require_cmd node
require_cmd npm
require_cmd uv
require_dir "$PY_REPO"
require_dir "$TS_REPO"

echo "==> Starting MinIO"
"${COMPOSE[@]}" -p "$COMPOSE_PROJECT" "${COMPOSE_FILES[@]}" down -v --remove-orphans
"${COMPOSE[@]}" -p "$COMPOSE_PROJECT" "${COMPOSE_FILES[@]}" up -d minio-init

for i in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$MINIO_PORT/minio/health/live" >/dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "MinIO did not become ready" >&2
        exit 1
    fi
    sleep 1
done

echo "==> Building engine"
cargo build --manifest-path "$ROOT/Cargo.toml" --bin zeppelin

echo "==> Starting engine at $ZEPPELIN_URL"
mkdir -p "$(dirname "$ENGINE_LOG")"
env \
    STORAGE_BACKEND=s3 \
    S3_BUCKET="$BUCKET" \
    S3_ENDPOINT=http://localhost:"$MINIO_PORT" \
    AWS_ACCESS_KEY_ID=minioadmin \
    AWS_SECRET_ACCESS_KEY=minioadmin \
    AWS_REGION=us-east-1 \
    S3_ALLOW_HTTP=true \
    ZEPPELIN_HOST=127.0.0.1 \
    ZEPPELIN_PORT="$PORT" \
    ZEPPELIN_CACHE_DIR="$CACHE_DIR" \
    ZEPPELIN_MAX_BATCH_SIZE="$MAX_BATCH_SIZE" \
    ZEPPELIN_DEFAULT_NUM_CENTROIDS=4 \
    ZEPPELIN_DEFAULT_NPROBE=4 \
    ZEPPELIN_RATE_LIMIT_RPS=1000000 \
    ZEPPELIN_RATE_LIMIT_BURST=1000000 \
    ZEPPELIN_WRITE_RATE_LIMIT_RPS=1000000 \
    ZEPPELIN_WRITE_RATE_LIMIT_BURST=1000000 \
    RUST_LOG="${RUST_LOG:-info}" \
    "$ROOT/target/debug/zeppelin" >"$ENGINE_LOG" 2>&1 &
ENGINE_PID=$!

ready=0
for _ in $(seq 1 120); do
    if curl -fsS "$ZEPPELIN_URL/readyz" >/dev/null 2>&1; then
        ready=1
        break
    fi
    if ! kill -0 "$ENGINE_PID" >/dev/null 2>&1; then
        echo "engine exited before readiness; log follows" >&2
        cat "$ENGINE_LOG" >&2
        exit 1
    fi
    sleep 0.5
done
if [ "$ready" -ne 1 ]; then
    echo "engine did not become ready; log follows" >&2
    cat "$ENGINE_LOG" >&2
    exit 1
fi

echo "==> Running Python integration suite"
(
    cd "$PY_REPO"
    UV_NO_CONFIG=1 \
    ZEPPELIN_URL="$ZEPPELIN_URL" \
    ZEPPELIN_E2E_MAX_BATCH_SIZE="$MAX_BATCH_SIZE" \
    uv run --extra dev pytest tests/test_integration.py -q
)

echo "==> Running TypeScript integration suite"
(
    cd "$TS_REPO"
    ZEPPELIN_URL="$ZEPPELIN_URL" \
    ZEPPELIN_E2E_MAX_BATCH_SIZE="$MAX_BATCH_SIZE" \
    npx vitest run tests/integration.test.ts
)

echo "==> Building TypeScript client for parity harness"
(
    cd "$TS_REPO"
    npm run build
)

echo "==> Running cross-language parity harness"
(
    cd "$PY_REPO"
    UV_NO_CONFIG=1 \
    ZEPPELIN_URL="$ZEPPELIN_URL" \
    ZEPPELIN_PY_REPO="$PY_REPO" \
    ZEPPELIN_TS_REPO="$TS_REPO" \
    uv run --extra dev python "$ROOT/contract/e2e/parity.py"
)

echo "==> Client e2e conformance passed"
