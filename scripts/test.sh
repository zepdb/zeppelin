#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/test.sh                    # all tests (uses TEST_BACKEND from env, default: s3)
#   ./scripts/test.sh storage            # only storage tests
#   ./scripts/test.sh wal                # only WAL tests
#   ./scripts/test.sh namespace          # only namespace tests
#   ./scripts/test.sh index              # only index tests
#   ./scripts/test.sh api                # only API tests
#   ./scripts/test.sh e2e                # only end-to-end tests
#   TEST_BACKEND=minio ./scripts/test.sh # run with MinIO

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env if present
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

BACKEND="${TEST_BACKEND:-s3}"
SUBSET="${1:-all}"

echo "=== Zeppelin Test Runner ==="
echo "Backend: $BACKEND"
echo "Subset:  $SUBSET"
echo ""

# If using MinIO, ensure it's running
if [ "$BACKEND" = "minio" ]; then
    if ! curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "MinIO not running. Starting via docker-compose..."
        docker-compose -f "$PROJECT_ROOT/docker-compose.test.yml" up -d
        echo "Waiting for MinIO to be ready..."
        sleep 3

        # Retry health check
        for i in {1..10}; do
            if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
                echo "MinIO is ready."
                break
            fi
            if [ "$i" -eq 10 ]; then
                echo "ERROR: MinIO failed to start after 10 attempts."
                exit 1
            fi
            sleep 2
        done
    else
        echo "MinIO already running."
    fi
fi

# Run tests
case "$SUBSET" in
    all)
        echo "Running all tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" -- --nocapture
        ;;
    storage)
        echo "Running storage tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test storage_tests -- --nocapture
        ;;
    wal)
        echo "Running WAL tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test wal_tests -- --nocapture
        ;;
    namespace)
        echo "Running namespace tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test namespace_tests -- --nocapture
        ;;
    index)
        echo "Running index tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test index_tests -- --nocapture
        ;;
    api)
        echo "Running API tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test api_tests -- --nocapture
        ;;
    e2e)
        echo "Running end-to-end tests..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --test e2e_tests -- --nocapture
        ;;
    unit)
        echo "Running unit tests only (no S3)..."
        cargo test --manifest-path "$PROJECT_ROOT/Cargo.toml" --lib -- --nocapture
        ;;
    *)
        echo "Unknown test subset: $SUBSET"
        echo "Available: all, storage, wal, namespace, index, api, e2e, unit"
        exit 1
        ;;
esac

echo ""
echo "=== Tests complete ==="
