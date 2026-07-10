#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<'USAGE'
Usage: scripts/perf-contract.sh [--capture] [--nightly]

Runs the deterministic performance-contract catalog against MinIO. The normal
mode is gating. --capture writes run-local proposals for human review and never
modifies checked-in contracts. --nightly runs Tier 1 contracts, Tier 2
prediction validation, and advisory Tier 3 latency validation in order.

CI gating command:
  TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored

Capture must never run in CI.
USAGE
}

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
        \( -name 'run-*' -o -name 'invocation-*' \) \
        -mtime +14 \
        -exec find {} -type f -delete \; \
        -exec find {} -depth -type d -empty -delete \;
}

entry_report() {
    local artifact_root="$1"
    local reports=()
    if [ ! -d "$artifact_root" ]; then
        echo "Artifact root does not exist: $artifact_root" >&2
        return 1
    fi
    while IFS= read -r -d '' report; do
        reports+=("$report")
    done < <(find "$artifact_root" -type f -name report.md -print0)
    if [ "${#reports[@]}" -ne 1 ]; then
        echo "Expected exactly one report.md under $artifact_root; found ${#reports[@]}." >&2
        return 1
    fi
    printf '%s\n' "${reports[0]}"
}

main() {
    local capture="false"
    local nightly="false"
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --capture)
                capture="true"
                ;;
            --nightly)
                nightly="true"
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                echo "unknown argument: $1" >&2
                usage >&2
                return 2
                ;;
        esac
        shift
    done
    if [ "$capture" = "true" ] && [ "$nightly" = "true" ]; then
        echo "--capture and --nightly cannot be combined." >&2
        return 2
    fi

    cd "$PROJECT_ROOT"
    ensure_minio
    prune_old_runs
    local invocation_root="$PROJECT_ROOT/target/perf-contract/invocation-$(date +%s)-$$"

    echo "Building tests before performance-contract execution..."
    if ! cargo build --tests; then
        return 1
    fi

    if [ "$nightly" = "true" ]; then
        local contracts_root="$invocation_root/contracts"
        local predict_root="$invocation_root/predict"
        local latency_root="$invocation_root/latency"
        local contracts_report=""

        echo "Running the gating Tier 1 performance-contract catalog..."
        TEST_BACKEND=minio \
        ZEPPELIN_PERF_ARTIFACTS="$contracts_root" \
            cargo test --test perf_contract_tests contracts -- --ignored --nocapture
        local contracts_status="$?"
        if ! contracts_report="$(entry_report "$contracts_root")"; then
            echo "Tier 1 did not produce a report for this invocation." >&2
        fi

        echo "Running Tier 2 prediction validation..."
        TEST_BACKEND=minio \
        ZEPPELIN_PERF_ARTIFACTS="$predict_root" \
            cargo test --test perf_contract_tests predict -- --ignored --nocapture
        local predict_status="$?"
        if ! entry_report "$predict_root" >/dev/null; then
            echo "Tier 2 did not produce a report for this invocation." >&2
            predict_status=1
        fi

        echo "Running advisory Tier 3 latency validation..."
        TEST_BACKEND=minio \
        ZEPPELIN_PERF_ARTIFACTS="$latency_root" \
            cargo test --test perf_contract_tests latency_validate -- --ignored --nocapture
        local latency_status="$?"
        if [ "$latency_status" -ne 0 ] || ! entry_report "$latency_root" >/dev/null; then
            echo "Tier 3 latency validation failed; nightly gate remains Tier 1/2 only." >&2
        fi

        if [ -z "$contracts_report" ]; then
            if [ "$contracts_status" -ne 0 ]; then
                return "$contracts_status"
            fi
            return 1
        fi
        cp "$contracts_report" "$PROJECT_ROOT/tasks/perf-contract-report.md"
        echo "Tier 1 report: $contracts_report"
        echo "Copied report: $PROJECT_ROOT/tasks/perf-contract-report.md"

        if [ "$contracts_status" -ne 0 ]; then
            return "$contracts_status"
        fi
        return "$predict_status"
    fi

    local entry="contracts"
    if [ "$capture" = "true" ]; then
        entry="capture"
        echo "Capturing run-local performance-contract proposals..."
        TEST_BACKEND=minio \
        ZEPPELIN_PERF_CAPTURE=1 \
        ZEPPELIN_PERF_ARTIFACTS="$invocation_root/$entry" \
            cargo test --test perf_contract_tests capture -- --ignored --nocapture
    else
        echo "Running the gating performance-contract catalog..."
        TEST_BACKEND=minio \
        ZEPPELIN_PERF_ARTIFACTS="$invocation_root/$entry" \
            cargo test --test perf_contract_tests contracts -- --ignored --nocapture
    fi
    local cargo_status="$?"
    local report=""
    if ! report="$(entry_report "$invocation_root/$entry")"; then
        echo "No report was produced for this performance-contract invocation." >&2
        if [ "$cargo_status" -ne 0 ]; then
            return "$cargo_status"
        fi
        return 1
    fi

    cp "$report" "$PROJECT_ROOT/tasks/perf-contract-report.md"
    local failed
    failed="$(sed -n 's/^- scenarios failed: \([0-9][0-9]*\)$/\1/p' "$report" | tail -n 1)"
    if [ -z "$failed" ]; then
        echo "Report omitted the failed-scenario count: $report" >&2
        return 1
    fi

    echo "Invocation report: $report"
    echo "Copied report: $PROJECT_ROOT/tasks/perf-contract-report.md"
    echo "Failed scenarios: $failed"

    if [ "$cargo_status" -ne 0 ] && [ "$failed" -eq 0 ]; then
        return "$cargo_status"
    fi
    return "$failed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
