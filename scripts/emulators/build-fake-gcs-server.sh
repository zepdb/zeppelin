#!/usr/bin/env bash
# Builds the patched fake-gcs-server Zeppelin's GCS test gates run against.
#
# Why a patch: object_store 0.11.2 speaks the GCS XML API, and stock
# fake-gcs-server has no XML PUT route (fsouza/fake-gcs-server#331 open since
# 2023; PR #1164 unmerged, and it ignores preconditions). Without the patch,
# every conditional write fails outright — or worse, with the PR as-is, CAS
# tests would pass vacuously. The patch adds XML PUT/DELETE routes that honor
# x-goog-if-generation-match via the upstream generationCondition plumbing and
# emit the ETag / x-goog-generation headers real GCS sends.
#
# Fidelity is enforced by tests/emulator_fidelity_probe.rs (P1–P11); rerun it
# after any change to the pinned version or the patch.
#
# Requires: git, go (any recent toolchain; 1.26.x used for the pinned build).
set -euo pipefail

PINNED_TAG="v1.55.1"
SRC_DIR="${FAKE_GCS_SRC_DIR:-$HOME/.local/share/fake-gcs-server-src}"
OUT_BIN="${FAKE_GCS_OUT_BIN:-$HOME/.local/bin/fake-gcs-server-zeppelin}"
PATCH_FILE="$(cd "$(dirname "$0")" && pwd)/fake-gcs-server-xml-api.patch"

if [ ! -d "$SRC_DIR/.git" ]; then
    git clone --depth 1 --branch "$PINNED_TAG" \
        https://github.com/fsouza/fake-gcs-server "$SRC_DIR"
fi

cd "$SRC_DIR"
git checkout -q "$PINNED_TAG" 2>/dev/null || true
git checkout -q -- . # drop any previous patch application
git apply "$PATCH_FILE"

GOFLAGS=-mod=mod go build -o "$OUT_BIN" .
echo "built $OUT_BIN from fake-gcs-server $PINNED_TAG + $(basename "$PATCH_FILE")"
