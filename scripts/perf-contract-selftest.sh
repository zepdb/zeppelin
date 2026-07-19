#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/perf-contract.sh"

root="$(mktemp -d)"
cleanup() {
    find "$root" -type f -delete
    find "$root" -depth -type d -empty -delete
}
trap cleanup EXIT

mkdir -p "$root/tier1/run-a" "$root/tier3/run-b"
printf '# Tier 1\n' > "$root/tier1/run-a/report.md"
printf '# Tier 3\n' > "$root/tier3/run-b/report.md"
touch -t 202607100100 "$root/tier1/run-a/report.md"
touch -t 202607100101 "$root/tier3/run-b/report.md"

resolved="$(entry_report "$root/tier1")"
expected="$root/tier1/run-a/report.md"
if [ "$resolved" != "$expected" ]; then
    echo "entry_report selected $resolved; expected $expected" >&2
    exit 1
fi

mkdir -p "$root/missing"
if entry_report "$root/missing" >/dev/null 2>&1; then
    echo "entry_report accepted an invocation with no report" >&2
    exit 1
fi

mkdir -p "$root/ambiguous/run-a" "$root/ambiguous/run-b"
printf '# First\n' > "$root/ambiguous/run-a/report.md"
printf '# Second\n' > "$root/ambiguous/run-b/report.md"
if entry_report "$root/ambiguous" >/dev/null 2>&1; then
    echo "entry_report accepted an invocation with multiple reports" >&2
    exit 1
fi

mkdir -p "$root/bin" "$root/project/tasks"
cat > "$root/bin/curl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$root/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -eu
if [[ " $* " == *" build --tests "* ]]; then
    exit 0
fi
case " $* " in
    *" branching_census "*) title="Branching" ;;
    *" contracts "*) title="Tier 1" ;;
    *" predict "*) title="Tier 2" ;;
    *" latency_validate "*) title="Tier 3" ;;
    *) echo "unexpected fake cargo arguments: $*" >&2; exit 2 ;;
esac
artifact_root="${ZEPPELIN_PERF_ARTIFACTS:?missing artifact root}"
mkdir -p "$artifact_root/run-fake"
printf '# %s\n\n- scenarios failed: 0\n' "$title" \
    > "$artifact_root/run-fake/report.md"
EOF
chmod +x "$root/bin/curl" "$root/bin/cargo"
mkdir -p "$root/project/target/perf-contract/run-stale"
printf '# Stale report\n' \
    > "$root/project/target/perf-contract/run-stale/report.md"

PROJECT_ROOT="$root/project"
PATH="$root/bin:$PATH" main --nightly
copied="$PROJECT_ROOT/tasks/perf-contract-report.md"
if ! grep -q '^# Tier 1$' "$copied"; then
    echo "nightly copied a non-gating report: $copied" >&2
    exit 1
fi

PROJECT_ROOT="$root/project"
PATH="$root/bin:$PATH" main --branching
if ! grep -q '^# Branching$' "$PROJECT_ROOT/target/perf-contract"/invocation-*/branching/run-fake/report.md; then
    echo "branching mode did not preserve its dedicated report" >&2
    exit 1
fi

echo "perf-contract driver selftest passed"
