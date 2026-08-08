#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TLA_DIR="$REPO_ROOT/formal-verifications/tla"

if [ -n "${JAVA:-}" ]; then
    JAVA_BIN="$JAVA"
elif [ -x /opt/homebrew/opt/openjdk/bin/java ]; then
    JAVA_BIN=/opt/homebrew/opt/openjdk/bin/java
else
    JAVA_BIN="$(command -v java || true)"
fi

if [ -z "$JAVA_BIN" ] || [ ! -x "$JAVA_BIN" ]; then
    echo "ERROR: Java runtime not found. Set JAVA=/path/to/java." >&2
    exit 1
fi

TLA_JAR="${TLA_JAR:-$HOME/Downloads/tla2tools.jar}"
if [ ! -f "$TLA_JAR" ]; then
    echo "ERROR: tla2tools.jar not found at $TLA_JAR. Set TLA_JAR=/path/to/tla2tools.jar." >&2
    exit 1
fi

read -r -a JAVA_OPTS_ARRAY <<< "${JAVA_OPTS:--XX:+UseParallelGC}"
TLC_WORKERS="${TLC_WORKERS:-1}"

run_tlc() {
    local spec="$1"
    local config="$2"
    local log="$3"

    echo "PASS expected: $spec ($config)"
    (
        cd "$TLA_DIR"
        "$JAVA_BIN" "${JAVA_OPTS_ARRAY[@]}" -jar "$TLA_JAR" \
            -workers "$TLC_WORKERS" \
            -config "$config" "$spec.tla" > "$log" 2>&1
    )

    if ! grep -q "No error has been found" "$TLA_DIR/$log"; then
        echo "ERROR: $spec did not complete cleanly. Log follows:" >&2
        cat "$TLA_DIR/$log" >&2
        exit 1
    fi
}

run_negative_tlc() {
    local spec="$1"
    local config="$2"
    local log="$3"

    echo "VIOLATION expected: $spec ($config)"
    set +e
    (
        cd "$TLA_DIR"
        "$JAVA_BIN" "${JAVA_OPTS_ARRAY[@]}" -jar "$TLA_JAR" \
            -workers "$TLC_WORKERS" \
            -config "$config" "$spec.tla" > "$log" 2>&1
    )
    local status=$?
    set -e

    if ! grep -Eq "Error: Invariant .* is violated" "$TLA_DIR/$log"; then
        echo "ERROR: $spec negative config did not violate an invariant (exit $status). Log follows:" >&2
        cat "$TLA_DIR/$log" >&2
        exit 1
    fi
}

run_tlc PitrHistoryRetention PitrHistoryRetention.cfg PitrHistoryRetention.tlc.log
run_tlc TwoPassGcSafety TwoPassGcSafety.cfg TwoPassGcSafety.tlc.log
run_tlc RestoreCloneSafety RestoreCloneSafety.cfg RestoreCloneSafety.tlc.log
run_tlc IncrementalArtifactClosure IncrementalArtifactClosure.cfg IncrementalArtifactClosure.tlc.log
run_tlc GroupCommitWalWriter GroupCommitWalWriter.cfg GroupCommitWalWriter.tlc.log
run_tlc MultiWriterLease MultiWriterLease.cfg MultiWriterLease.tlc.log
run_tlc SecurityPolicy SecurityPolicy.cfg SecurityPolicy.tlc.log

run_negative_tlc PitrHistoryRetention PitrHistoryRetention.bug.cfg PitrHistoryRetention.bug.tlc.log
run_negative_tlc TwoPassGcSafety TwoPassGcSafety.no-revalidate.cfg TwoPassGcSafety.no-revalidate.tlc.log
run_negative_tlc TwoPassGcSafety TwoPassGcSafety.stale-history.cfg TwoPassGcSafety.stale-history.tlc.log
run_negative_tlc TwoPassGcSafety TwoPassGcSafety.history-pending.cfg TwoPassGcSafety.history-pending.tlc.log
run_negative_tlc RestoreCloneSafety RestoreCloneSafety.bug.cfg RestoreCloneSafety.bug.tlc.log
run_negative_tlc IncrementalArtifactClosure IncrementalArtifactClosure.bug.cfg IncrementalArtifactClosure.bug.tlc.log
run_negative_tlc GroupCommitWalWriter GroupCommitWalWriter.bug.cfg GroupCommitWalWriter.bug.tlc.log
run_negative_tlc SecurityPolicy SecurityPolicy.no-cas.cfg SecurityPolicy.no-cas.tlc.log
run_negative_tlc SecurityPolicy SecurityPolicy.stale-open.cfg SecurityPolicy.stale-open.tlc.log

echo "All July TLA checks matched their expected result."
