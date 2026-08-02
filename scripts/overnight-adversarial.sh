#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SECONDS_BUDGET="43200"
INSTALL_NIGHTLY="false"

usage() {
    cat <<'USAGE'
Usage: scripts/overnight-adversarial.sh [SECONDS] [--install-nightly]
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --install-nightly)
            INSTALL_NIGHTLY="true"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        ''|*[!0-9]*)
            echo "invalid argument: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            SECONDS_BUDGET="$1"
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

install_nightly() {
    local plist="$HOME/Library/LaunchAgents/com.zeppelin.adversarial.plist"
    mkdir -p "$(dirname "$plist")" "$PROJECT_ROOT/target/adversarial"
    cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.zeppelin.adversarial</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PROJECT_ROOT/scripts/overnight-adversarial.sh</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>22</integer>
    <key>Minute</key><integer>0</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>$PROJECT_ROOT/target/adversarial/launchd.log</string>
  <key>StandardErrorPath</key>
  <string>$PROJECT_ROOT/target/adversarial/launchd.log</string>
  <key>WorkingDirectory</key>
  <string>$PROJECT_ROOT</string>
</dict>
</plist>
PLIST
    launchctl unload "$plist" >/dev/null 2>&1 || true
    launchctl load "$plist"
    echo "Installed launchd job: $plist"
    echo "Unload with: launchctl unload $plist"
}

prune_old_runs() {
    mkdir -p "$PROJECT_ROOT/target/adversarial"
    find "$PROJECT_ROOT/target/adversarial" \
        -maxdepth 1 \
        -type d \
        -name 'run-*' \
        -mtime +14 \
        -exec find {} -type f -delete \; \
        -exec find {} -depth -type d -empty -delete \;
}

ensure_minio
prune_old_runs
if [ "$INSTALL_NIGHTLY" = "true" ]; then
    install_nightly
    exit 0
fi

echo "Building tests before starting the budget clock..."
cargo build --tests

RUN_CMD=(
    env
    TEST_BACKEND=minio
    ZEPPELIN_ADVERSARIAL_SECONDS="$SECONDS_BUDGET"
    ZEPPELIN_ADVERSARIAL_MAX_OPS="${ZEPPELIN_ADVERSARIAL_MAX_OPS:-500}"
    ZEPPELIN_ADVERSARIAL_MODE=mixed
    cargo test --test adversarial_workload_tests overnight -- --ignored --nocapture
)

if [ "$(uname -s)" = "Darwin" ] && command -v caffeinate >/dev/null 2>&1; then
    caffeinate -dimsu "${RUN_CMD[@]}"
else
    "${RUN_CMD[@]}"
fi

# Late-interaction profiles run as an explicit extra batch: the mixed-mode
# nine-slot seed table is replay-load-bearing and must not be reshuffled.
LATE_SECONDS="${ZEPPELIN_ADVERSARIAL_LATE_SECONDS:-420}"
for late_profile in late late-stream late-content late-crash; do
    LATE_CMD=(
        env
        TEST_BACKEND=minio
        ZEPPELIN_ADVERSARIAL_SECONDS="$LATE_SECONDS"
        ZEPPELIN_ADVERSARIAL_MAX_OPS="${ZEPPELIN_ADVERSARIAL_MAX_OPS:-500}"
        ZEPPELIN_ADVERSARIAL_PROFILE="$late_profile"
        ZEPPELIN_ADVERSARIAL_SEEDS=8
        cargo test --test adversarial_workload_tests smoke -- --ignored --nocapture
    )
    if [ "$(uname -s)" = "Darwin" ] && command -v caffeinate >/dev/null 2>&1; then
        caffeinate -dimsu "${LATE_CMD[@]}" || echo "late profile $late_profile FAILED" >&2
    else
        "${LATE_CMD[@]}" || echo "late profile $late_profile FAILED" >&2
    fi
done

latest_report="$(find "$PROJECT_ROOT/target/adversarial" \
    -path '*/report.md' \
    -type f \
    -print0 | xargs -0 ls -t 2>/dev/null | head -n 1 || true)"

if [ -z "$latest_report" ]; then
    echo "No adversarial report.md found." >&2
    exit 1
fi

cp "$latest_report" "$PROJECT_ROOT/tasks/overnight-adversarial-report.md"
failed="$(grep -E '^Summary:' "$latest_report" | sed -E 's/.*failed=([0-9]+).*/\1/' | tail -n 1)"
failed="${failed:-0}"
echo "Latest report: $latest_report"
echo "Failed seeds: $failed"
exit "$failed"
