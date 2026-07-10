#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CAPTURE="false"
NIGHTLY="false"

usage() {
    cat <<'USAGE'
Usage: scripts/perf-contract.sh [--capture] [--nightly]

Runs the deterministic performance-contract catalog against MinIO. The normal
mode is gating. --capture writes run-local proposals for human review and never
modifies checked-in contracts. --nightly is reserved for Phase 4 and currently
has no effect.

CI gating command:
  TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored

Capture must never run in CI.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --capture)
            CAPTURE="true"
            ;;
        --nightly)
            NIGHTLY="true"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

cd "$PROJECT_ROOT"

ensure_minio() {
    if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        echo "MinIO already running."
        return
    fi
    echo "Starting MinIO via docker compose..."
    docker compose -f "$PROJECT_ROOT/docker-compose.test.yml" up -d
    for _ in $(seq 1 20); do
        if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
            echo "MinIO is ready."
            return
        fi
        sleep 2
    done
    echo "MinIO did not become healthy." >&2
    exit 1
}

prune_old_runs() {
    mkdir -p "$PROJECT_ROOT/target/perf-contract"
    find "$PROJECT_ROOT/target/perf-contract" \
        -maxdepth 1 \
        -type d \
        -name 'run-*' \
        -mtime +14 \
        -exec find {} -type f -delete \; \
        -exec find {} -depth -type d -empty -delete \;
}

latest_report() {
    find "$PROJECT_ROOT/target/perf-contract" \
        -path '*/report.md' \
        -type f \
        -print0 | xargs -0 ls -t 2>/dev/null | head -n 1
}

ensure_minio
prune_old_runs

if [ "$NIGHTLY" = "true" ]; then
    echo "--nightly is reserved; running the Tier-1 catalog unchanged."
fi

echo "Building tests before performance-contract execution..."
if ! cargo build --tests; then
    exit 1
fi

if [ "$CAPTURE" = "true" ]; then
    echo "Capturing run-local performance-contract proposals..."
    TEST_BACKEND=minio \
    ZEPPELIN_PERF_CAPTURE=1 \
        cargo test --test perf_contract_tests capture -- --ignored --nocapture
    cargo_status="$?"
else
    echo "Running the gating performance-contract catalog..."
    TEST_BACKEND=minio \
        cargo test --test perf_contract_tests contracts -- --ignored --nocapture
    cargo_status="$?"
fi

report="$(latest_report)"
if [ -z "$report" ]; then
    echo "No performance-contract report.md found." >&2
    exit "${cargo_status:-1}"
fi

cp "$report" "$PROJECT_ROOT/tasks/perf-contract-report.md"
failed="$(sed -n 's/^- scenarios failed: \([0-9][0-9]*\)$/\1/p' "$report" | tail -n 1)"
if [ -z "$failed" ]; then
    echo "Report omitted the failed-scenario count: $report" >&2
    exit 1
fi

echo "Latest report: $report"
echo "Copied report: $PROJECT_ROOT/tasks/perf-contract-report.md"
echo "Failed scenarios: $failed"

if [ "$cargo_status" -ne 0 ] && [ "$failed" -eq 0 ]; then
    exit "$cargo_status"
fi
exit "$failed"
