# Phase 5 Product RCA — Corrupt WAL and Manifest Acknowledgement

**Status**: product fixes implemented and regression-tested. This is a
supporting fix task for Phase 5, not one of the numbered phase commits.

## Trigger

The first 12-seed content-profile acceptance run after the Phase 5 harness
was made taint-aware preserved seven product failures:

`target/adversarial/phase5-content-12-after-taint-fix/run-1783764307`

| Seed | Injected fault | First visible failure |
| --- | --- | --- |
| 0 | BitFlip on a live WAL delete fragment at op 399 | I2 at op 457: deleted `v33` returned |
| 1 | MisdirectedWrite on `manifest.json` at op 11 | I4 at op 199: acknowledged vectors missing |
| 5 | MisdirectedWrite on `manifest.json` at op 266, then WrongObject on WAL | I16 at quiescence: manifest count behind acknowledged model |
| 6 | BitFlip on a live WAL upsert fragment at op 129 | I7 at op 132: comma-corrupted vector ID returned |
| 7 | MisdirectedWrite on `manifest.json` at op 235 | I4 at op 261: acknowledged vectors missing |
| 9 | WrongObject on `manifest.json` at op 371 | I4 + I20 at op 375: successful divergent fetch |
| 10 | WrongObject on one manifest, then MisdirectedWrite on another | I4 + I20 at op 243: successful divergent fetch |

These were not oracle false positives. Each failure followed a successful
response that consumed corrupt bytes or acknowledged a manifest pointer that
S3 had not actually made authoritative.

## Root cause 1: consumed WAL reads skipped integrity checks

`WalReader` exposed checked and unchecked batch-read paths. Query, vector
fetch, eventual tombstone, and compaction callers used the unchecked path,
which decoded with `WalFragment::from_bytes_unchecked`. A one-bit mutation that
remained valid MessagePack therefore became trusted domain state.

The cache made the blast radius persistent. `read_fragment_bytes` populated
the immutable WAL cache before decoding, so a one-shot corrupt GET could poison
later reads even after object storage returned clean bytes.

The fragment checksum also intentionally excludes the fragment ULID. Without
an explicit key/payload identity check, a valid payload stored under the wrong
fragment key could pass checksum validation.

### Fix

- All `WalReader` consumed-read entry points now validate checksums, including
  historical methods whose names contain `unchecked`.
- The reader compares the decoded fragment ULID with the ULID in the requested
  object key.
- A decode, checksum, or identity failure evicts that cache entry and returns
  the original error. The same request never retries or falls back; a later
  request may fetch repaired authoritative bytes.

## Root cause 2: successful manifest PUTs were trusted blindly

`Manifest::write` and `Manifest::write_conditional` advanced their in-memory
generation immediately after a successful object-store PUT. When the storage
boundary acknowledged a payload written to `manifest.json.misdirected`, the
writer returned success even though the old S3 live pointer remained
authoritative.

### Fix

Both publication paths now perform one read-after-write GET and compare the
exact live bytes with the committed candidate before advancing or returning
success. A missing or different live object is a loud error. S3 remains the
source of truth; local candidate state is never accepted as proof of commit.

## Root cause 3: valid manifests had no namespace identity

A manifest returned for another namespace could still be valid MessagePack.
The decoder had no persisted namespace identity to reject it, so a WrongObject
GET could turn unrelated but structurally valid state into a successful query
snapshot.

### Fix and compatibility

New live and history manifests append an optional namespace binding as the last
MessagePack field. Namespace-aware live, versioned, history, and strong-cache
reads reject a different binding. Pre-existing manifests decode with `None` for
wire compatibility and acquire a binding on their next successful write.

Appending the field preserves positional MessagePack compatibility. No field
was inserted or reordered.

## RED evidence

The focused regressions failed before their corresponding fixes:

1. A real-store WAL object was changed from `checksum_target` to
   `dhecksum_target` without updating its checksum. Checked decoding returned
   `ChecksumMismatch`, while the consumed reader returned `Ok`.
2. After a corrupt WAL read populated the cache, restoring the correct S3 bytes
   still left the next read failing from the poisoned cache entry.
3. Valid manifest bytes written for namespace A were accepted when placed at
   namespace B's live key.
4. A conditional manifest PUT redirected to a sibling key returned `Ok`,
   advanced the candidate generation, and left the old live manifest in S3.

## Focused GREEN validation

```bash
TEST_BACKEND=memory cargo test --release \
  --test no_silent_partials_tests -- --nocapture

TEST_BACKEND=memory cargo test --release \
  --test manifest_history_atomicity_tests -- --nocapture

cargo test --release --lib wal::manifest::tests -- --nocapture
```

Results:

- `no_silent_partials_tests`: 10 passed, including checksum rejection,
  key/payload identity, and cache recovery.
- `manifest_history_atomicity_tests`: 8 passed.
- `wal::manifest::tests`: 26 passed, including legacy positional decoding.

## Content-profile validation

The exact 12-seed matrix passed with the product fixes and the dirty Phase 5
harness:

```bash
TEST_BACKEND=memory \
ZEPPELIN_ADVERSARIAL_PROFILE=content \
ZEPPELIN_ADVERSARIAL_SEEDS=12 \
ZEPPELIN_ADVERSARIAL_SECONDS=420 \
ZEPPELIN_ADVERSARIAL_ARTIFACTS=\
target/adversarial/phase5-content-12-product-fixes-precommit \
cargo test --release --test adversarial_workload_tests \
  smoke -- --ignored --nocapture
```

Artifact:

`target/adversarial/phase5-content-12-product-fixes-precommit/run-1783765600`

Result: 12 seeds passed, 0 failed, 6,274 operations, 54 explicit
compactions, and 0 violations. The two WAL corruptions surfaced as
`DATA_CORRUPTION`; manifest misdirection became an indeterminate loud failure
and resolved authoritatively as not applied.
