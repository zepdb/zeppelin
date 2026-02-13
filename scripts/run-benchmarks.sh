#!/bin/bash
# run-benchmarks.sh — Automated benchmark suite with CPU profiling
#
# Runs all benchmark scenarios against a Zeppelin instance, capturing:
#   - Benchmark JSON results
#   - CPU flamegraph SVGs (via /debug/pprof/cpu)
#   - Prometheus metrics snapshots
#   - Server logs
#
# Usage:
#   ./scripts/run-benchmarks.sh --target http://<ip>:8080 --run-id 007
#   ./scripts/run-benchmarks.sh --target http://localhost:8080 --skip-profiling
#   ./scripts/run-benchmarks.sh --help
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ─── Defaults ───────────────────────────────────────────────────────────────
TARGET="http://localhost:8080"
RUN_ID=""
VECTORS=10000
DIMENSIONS=128
DURATION=60
BATCH_SIZE=500
NPROBE=4
TOP_K=10
CONCURRENCY=4
SKIP_PROFILING=false
PROFILE_SECONDS=30
SCENARIOS="ingest,compaction,query_latency,query_throughput,filtered_query,mixed_workload,bm25"

# ─── Parse args ─────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --target URL          Zeppelin server URL (default: http://localhost:8080)
  --run-id ID           Run identifier (default: auto-detect next run number)
  --scenarios LIST      Comma-separated scenarios (default: all 7)
  --vectors N           Number of vectors to ingest (default: 10000)
  --dimensions N        Vector dimensions (default: 128)
  --duration N          Duration in seconds for sustained tests (default: 60)
  --batch-size N        Batch size for ingestion (default: 500)
  --nprobe N            IVF probe count (default: 4)
  --top-k N             Results per query (default: 10)
  --concurrency N       Concurrent clients (default: 4)
  --skip-profiling      Disable CPU profiling capture
  --profile-seconds N   Profile duration per scenario (default: 30)
  --help                Show this message
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)          TARGET="$2"; shift 2 ;;
        --run-id)          RUN_ID="$2"; shift 2 ;;
        --scenarios)       SCENARIOS="$2"; shift 2 ;;
        --vectors)         VECTORS="$2"; shift 2 ;;
        --dimensions)      DIMENSIONS="$2"; shift 2 ;;
        --duration)        DURATION="$2"; shift 2 ;;
        --batch-size)      BATCH_SIZE="$2"; shift 2 ;;
        --nprobe)          NPROBE="$2"; shift 2 ;;
        --top-k)           TOP_K="$2"; shift 2 ;;
        --concurrency)     CONCURRENCY="$2"; shift 2 ;;
        --skip-profiling)  SKIP_PROFILING=true; shift ;;
        --profile-seconds) PROFILE_SECONDS="$2"; shift 2 ;;
        --help)            usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ─── Auto-detect run ID ────────────────────────────────────────────────────
if [ -z "$RUN_ID" ]; then
    LAST_RUN=$(ls -d "$PROJECT_ROOT/benchmarks/results/run-"* 2>/dev/null | \
        sed 's/.*run-//' | sort -n | tail -1 || echo "0")
    LAST_NUM=$(echo "$LAST_RUN" | sed 's/^0*//' || echo "0")
    NEXT_NUM=$((LAST_NUM + 1))
    RUN_ID=$(printf "%03d" "$NEXT_NUM")
fi

RESULTS_DIR="$PROJECT_ROOT/benchmarks/results/run-$RUN_ID"
PROFILES_DIR="$RESULTS_DIR/profiles"
mkdir -p "$PROFILES_DIR"

# ─── Helpers ────────────────────────────────────────────────────────────────
log() { echo "[$(date +%H:%M:%S)] $*"; }

check_health() {
    if ! curl -sf "$TARGET/healthz" > /dev/null 2>&1; then
        log "ERROR: Server at $TARGET is not healthy"
        exit 1
    fi
}

capture_metrics() {
    local label="$1"
    log "Capturing metrics: $label"
    curl -sf "$TARGET/metrics" > "$RESULTS_DIR/metrics_${label}.txt" 2>/dev/null || \
        log "WARNING: Failed to capture metrics ($label)"
}

check_profiling_available() {
    if [ "$SKIP_PROFILING" = true ]; then
        return 1
    fi
    # Check if the profiling endpoint exists by doing a quick 1-second profile
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$TARGET/debug/pprof/cpu?seconds=1" 2>/dev/null)
    if [ "$http_code" = "200" ]; then
        return 0
    else
        log "WARNING: Profiling endpoint returned HTTP $http_code, falling back to --skip-profiling"
        SKIP_PROFILING=true
        return 1
    fi
}

run_scenario() {
    local scenario="$1"
    local extra_args="${2:-}"

    log "Running scenario: $scenario"
    local output_file="$RESULTS_DIR/${scenario}.json"

    cargo run --manifest-path "$PROJECT_ROOT/benchmarks/Cargo.toml" --release -- \
        --target "$TARGET" \
        --scenario "$scenario" \
        --vectors "$VECTORS" \
        --dimensions "$DIMENSIONS" \
        --duration "$DURATION" \
        --batch-size "$BATCH_SIZE" \
        --concurrency "$CONCURRENCY" \
        --top-k "$TOP_K" \
        ${NPROBE:+--nprobe "$NPROBE"} \
        $extra_args \
        --output "$output_file" || {
            log "WARNING: Scenario $scenario failed (exit $?), continuing..."
            return 1
        }

    log "Results written to $output_file"
    return 0
}

run_scenario_with_profile() {
    local scenario="$1"
    local profile_secs="${2:-$PROFILE_SECONDS}"
    local extra_args="${3:-}"

    if [ "$SKIP_PROFILING" = true ]; then
        run_scenario "$scenario" "$extra_args"
        return $?
    fi

    log "Starting CPU profile ($profile_secs s) for $scenario..."
    local svg_file="$PROFILES_DIR/${scenario}.svg"

    # Start profiling in background
    curl -sf "$TARGET/debug/pprof/cpu?seconds=$profile_secs" -o "$svg_file" &
    local profile_pid=$!

    # Small delay to ensure profiler is sampling before benchmark starts
    sleep 1

    # Run the benchmark
    run_scenario "$scenario" "$extra_args"
    local bench_status=$?

    # Wait for profile to complete
    log "Waiting for CPU profile to finish..."
    if wait "$profile_pid" 2>/dev/null; then
        local svg_size
        svg_size=$(wc -c < "$svg_file" 2>/dev/null || echo 0)
        if [ "$svg_size" -gt 100 ]; then
            log "Flamegraph: $svg_file ($svg_size bytes)"
        else
            log "WARNING: Profile SVG is suspiciously small ($svg_size bytes)"
            rm -f "$svg_file"
        fi
    else
        log "WARNING: Profile capture failed for $scenario"
        rm -f "$svg_file"
    fi

    return $bench_status
}

capture_server_logs() {
    log "Capturing server logs..."
    # Try SSH to EC2 instance (extract host from TARGET URL)
    local host
    host=$(echo "$TARGET" | sed 's|https\?://||;s|:[0-9]*||;s|/.*||')

    if [ "$host" = "localhost" ] || [ "$host" = "127.0.0.1" ]; then
        # Local docker
        docker logs zeppelin > "$RESULTS_DIR/server_logs.txt" 2>&1 || \
            log "WARNING: Could not capture local docker logs"
    else
        # Try SSH (assumes ec2-user and key from deploy env)
        local key_file="${EC2_KEY_FILE:-}"
        if [ -n "$key_file" ]; then
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
                -i "$key_file" "ec2-user@$host" \
                "docker logs zeppelin 2>&1" > "$RESULTS_DIR/server_logs.txt" 2>/dev/null || \
                log "WARNING: Could not capture remote docker logs"
        else
            log "WARNING: EC2_KEY_FILE not set, skipping server log capture"
        fi
    fi
}

generate_summary() {
    log "Generating summary..."
    local summary_file="$RESULTS_DIR/summary.md"
    cat > "$summary_file" <<SUMMARY_EOF
# Benchmark Run $RUN_ID — $(date +%Y-%m-%d)

## Configuration

| Parameter | Value |
|-----------|-------|
| Target | $TARGET |
| Vectors | $VECTORS |
| Dimensions | $DIMENSIONS |
| Duration | ${DURATION}s |
| Batch size | $BATCH_SIZE |
| Concurrency | $CONCURRENCY |
| Nprobe | $NPROBE |
| Top-k | $TOP_K |
| Profiling | $([ "$SKIP_PROFILING" = true ] && echo "disabled" || echo "enabled (${PROFILE_SECONDS}s)") |

## Scenarios Run

SUMMARY_EOF

    IFS=',' read -ra SCENARIO_LIST <<< "$SCENARIOS"
    for s in "${SCENARIO_LIST[@]}"; do
        if [ -f "$RESULTS_DIR/${s}.json" ]; then
            echo "- **$s**: completed" >> "$summary_file"
        else
            echo "- **$s**: FAILED or skipped" >> "$summary_file"
        fi
    done

    echo "" >> "$summary_file"
    echo "## Flamegraphs" >> "$summary_file"
    echo "" >> "$summary_file"
    if [ "$SKIP_PROFILING" = true ]; then
        echo "Profiling was disabled for this run." >> "$summary_file"
    else
        local has_profiles=false
        for svg in "$PROFILES_DIR"/*.svg; do
            [ -f "$svg" ] || continue
            has_profiles=true
            local name
            name=$(basename "$svg" .svg)
            echo "- [$name](profiles/$(basename "$svg"))" >> "$summary_file"
        done
        if [ "$has_profiles" = false ]; then
            echo "No flamegraphs were captured." >> "$summary_file"
        fi
    fi

    echo "" >> "$summary_file"
    echo "## Files" >> "$summary_file"
    echo "" >> "$summary_file"
    echo '```' >> "$summary_file"
    ls -la "$RESULTS_DIR/" >> "$summary_file" 2>/dev/null
    echo '```' >> "$summary_file"

    log "Summary written to $summary_file"
}

# ─── Main ───────────────────────────────────────────────────────────────────
log "=== Zeppelin Benchmark Suite — Run $RUN_ID ==="
log "Target:     $TARGET"
log "Results:    $RESULTS_DIR"
log "Scenarios:  $SCENARIOS"
log "Vectors:    $VECTORS  Dims: $DIMENSIONS  Duration: ${DURATION}s"
log ""

# Pre-flight checks
check_health
log "Server is healthy"

# Check profiling availability (runs 1s test profile)
if [ "$SKIP_PROFILING" = false ]; then
    log "Checking profiling endpoint..."
    check_profiling_available || true
fi

# Capture initial metrics
capture_metrics "initial"

# Parse scenario list
IFS=',' read -ra SCENARIO_LIST <<< "$SCENARIOS"

for scenario in "${SCENARIO_LIST[@]}"; do
    echo ""
    log "━━━ Phase: $scenario ━━━"

    case "$scenario" in
        ingest)
            # I/O bound — not CPU interesting, skip profiling
            run_scenario "ingest"
            capture_metrics "post_ingest"
            ;;
        compaction)
            # k-means + index build — use small batches to trigger more compactions
            _saved_batch_size="$BATCH_SIZE"
            if [ "$VECTORS" -ge 100000 ]; then
                BATCH_SIZE=200   # 100K/200 = 500 fragments (manageable)
            else
                BATCH_SIZE=50    # 10K/50 = 200 fragments (existing behavior)
            fi
            run_scenario_with_profile "compaction" 60
            BATCH_SIZE="$_saved_batch_size"
            capture_metrics "post_compaction"
            ;;
        query_latency)
            run_scenario_with_profile "query_latency" 30
            ;;
        query_throughput)
            run_scenario_with_profile "query_throughput" 60
            ;;
        filtered_query)
            run_scenario_with_profile "filtered_query" 30
            ;;
        mixed_workload)
            run_scenario_with_profile "mixed_workload" 30
            ;;
        bm25)
            run_scenario_with_profile "bm25" 30
            ;;
        scale_test)
            run_scenario_with_profile "scale_test" 60
            ;;
        index_comparison)
            run_scenario_with_profile "index_comparison" 45
            ;;
        *)
            log "Unknown scenario: $scenario, skipping"
            ;;
    esac
done

# Final metrics + logs
echo ""
log "━━━ Collecting final data ━━━"
capture_metrics "final"
capture_server_logs

# Summary
generate_summary

echo ""
log "=== Benchmark Run $RUN_ID Complete ==="
log "Results: $RESULTS_DIR"
ls -1 "$RESULTS_DIR/"
if [ -d "$PROFILES_DIR" ] && ls "$PROFILES_DIR"/*.svg >/dev/null 2>&1; then
    echo ""
    log "Flamegraphs:"
    ls -1 "$PROFILES_DIR/"*.svg
fi
