# Formal Verification for Zeppelin

This directory contains formal verification artifacts for Zeppelin's concurrent protocols and property-based tests for sequential logic.

## Directory Structure

```
formal-verifications/
  tla/                              # TLA+ specifications
    S3Model.tla                     # Shared S3 consistency model (operators + axioms)
    CompactionSafety.tla + .cfg     # P0: Writer + Compactor data loss race
    QueryReadConsistency.tla + .cfg # P1: Query + Compactor 404 race
    NamespaceDeletion.tla + .cfg    # P2: Non-atomic namespace deletion
    ULIDOrdering.tla + .cfg        # P3: ULID ordering under clock skew
```

Property-based tests live in the main test suite as `tests/proptest_*.rs` (see below).

## TLA+ Specifications

### Prerequisites

Install the TLA+ tools:

```bash
# Download tla2tools.jar (the TLC model checker)
wget https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

# Or use the TLA+ Toolbox IDE (includes TLC)
# https://lamport.azurewebsites.net/tla/toolbox.html
```

This workspace also supports the local jar at `~/Downloads/tla2tools.jar`.
When `/usr/bin/java` has no configured runtime, use Homebrew OpenJDK directly:

```bash
JAVA="${JAVA:-/opt/homebrew/opt/openjdk/bin/java}"
"$JAVA" -XX:+UseParallelGC -jar "$HOME/Downloads/tla2tools.jar" \
  -config <Spec>.cfg <Spec>.tla
```

### Running the Model Checker

Each spec has a matching `.cfg` file. Run TLC from the `tla/` directory:

```bash
cd formal-verifications/tla/

# P0: Compaction Safety (EXPECTED TO FAIL — finds data loss bug)
java -jar tla2tools.jar -config CompactionSafety.cfg CompactionSafety.tla

# P1: Query Read Consistency (EXPECTED TO FAIL — finds 404 race)
java -jar tla2tools.jar -config QueryReadConsistency.cfg QueryReadConsistency.tla

# P2: Namespace Deletion (EXPECTED TO FAIL — finds partial deletion)
java -jar tla2tools.jar -config NamespaceDeletion.cfg NamespaceDeletion.tla

# P3: ULID Ordering (EXPECTED TO FAIL with clock_skew=TRUE)
java -jar tla2tools.jar -config ULIDOrdering.cfg ULIDOrdering.tla
```

Run the current July 2026 durability/write-path specs and their negative
variants with:

```bash
./scripts/run-july-tla.sh
```

The scheduled/manual GitHub workflow `.github/workflows/tla-nightly.yml` runs
the same script.

### What Each Spec Verifies

| Spec | Invariant | Expected | Bug Found |
|------|-----------|----------|-----------|
| `CompactionSafety` | `NoSilentDataLoss` | VIOLATED | Compactor overwrites writer's manifest update, losing committed fragments |
| `QueryReadConsistency` | `QueryNeverHitsOrphan` | VIOLATED | Query reads stale manifest, compactor deletes fragments, query gets 404 |
| `NamespaceDeletion` | `QueryNeverSeesPartialState` | VIOLATED | Non-atomic prefix deletion exposes half-deleted state to queries |
| `ULIDOrdering` | `LastCommitterWins` | VIOLATED (with skew) | Clock skew reverses ULID order, wrong value wins in merge |

### July 2026 Durability and Writer Specs

These specs model the newer PITR, GC, restore, incremental compaction, and
native group-commit code added after July 3, 2026. The default configs are
expected to pass. Each spec also has a deliberately buggy variant, enabled by
a config constant, to prove the invariant can catch the target failure mode.

The July 7 tightening split several previously atomic actions into their real
publish steps. That exposed one product gap: PITR `as_of` readers treated a
pre-CAS `history[N]` object as committed before the live manifest reached `N`.
The query and restore-as-clone resolvers now cap history by the current live
manifest generation, and `PitrHistoryRetention` includes that invariant.

| Spec | Default TLC result | Negative variant |
|------|--------------------|------------------|
| `PitrHistoryRetention` | PASS: 25,055,355 generated; 1,644,558 distinct; depth 36 | `AllowBuggyCommit = TRUE` violates `LiveHasHistory` |
| `TwoPassGcSafety` | PASS: 7,748,544 generated; 646,806 distinct; depth 25 | `AllowBuggySweepWithoutRevalidate = TRUE` and `AllowBuggyStaleHistorySweep = TRUE` both violate `NoReachableKeyDeleted` |
| `RestoreCloneSafety` | PASS: 6,833 generated; 1,632 distinct; depth 13 | `AllowBuggyPublish = TRUE` violates `VisibleTargetRefsExist` |
| `IncrementalArtifactClosure` | PASS: 284 generated; 72 distinct; depth 9 | `AllowBuggyPrefixGc = TRUE` violates `ManifestReachableArtifactsExist` |
| `GroupCommitWalWriter` | PASS: 9,883 generated; 3,523 distinct; depth 16 | `AllowBuggyMixedTokenDeadlock = TRUE` violates `NoMixedTokenLeaderDeadlock`; adding strict `StrictFailedAppendLeavesNoOrphan` violates under best-effort cleanup failure |

### Namespace branching lifecycle and GC

The branching packet is split deliberately:

- [`NamespaceBranching.tla`](tla/NamespaceBranching.tla) with
  [`NamespaceBranching.cfg`](tla/NamespaceBranching.cfg) checks the namespace
  graph, split history/live publication, policy activation guard, preservation
  rechecks, crash recovery, and ordered deletion lifecycle.
- [`NamespaceBranchingGc.tla`](tla/NamespaceBranchingGc.tla) with
  [`NamespaceBranchingGc.cfg`](tla/NamespaceBranchingGc.cfg) checks retained
  generations, two-pass source GC, target-owned pending deletes, admitted reads,
  durable reader grace, nested children, sibling roots, and source deletion.

The acceptance checklist in the originating plan still says “all eight”
negative variants. That wording is stale: the packet has twelve one-hot
negative controls, and all twelve are required below.

#### RED-first evidence

Before the full packet, the lifecycle file contained only `Creating`, target
manifest visibility, and source-root presence. Its deliberately unsafe
`PublishTargetManifest` action set visibility without a root; activation was the
next enabled lifecycle action. This exact command produced the required RED:

```bash
cd formal-verifications/tla
JAVA="${JAVA:-/opt/homebrew/opt/openjdk/bin/java}"
"$JAVA" -XX:+UseParallelGC -jar "$HOME/Downloads/tla2tools.jar" \
  -config NamespaceBranching.cfg NamespaceBranching.tla
```

TLC exited `12` at `PublishTargetManifest` with
`targetManifestVisible = TRUE` and `sourceRootPresent = FALSE`:

```text
Error: Invariant VisibleManifestRequiresRoot is violated.
2 states generated, 2 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 2.
```

The complete lifecycle model retains the same negative path behind
`AllowPublishWithoutRoot`, but now the trace performs target reservation and the
generation-one history write before the unsafe live-manifest publication.

#### Head-only root-selection regression

A later contract audit found that the first complete model accidentally allowed
the root action to select an already-retained historical generation. V1 permits
only the exact live predecessor observed under the source lease. The lifecycle
invariant was strengthened to require the rooted generation/digest to equal the
pre-CAS head, and the GC model gained `RootCreatedFromLiveHead`.

Before narrowing the actions, TLC produced both required RED counterexamples:

| Model | Violated invariant | Generated | Distinct | Queue | Depth |
|-------|--------------------|----------:|---------:|------:|------:|
| `NamespaceBranching` | `RootPinsExactPredecessorGeneration` | 965 | 426 | 189 | 7 |
| `NamespaceBranchingGc` | `RootCreatedFromLiveHead` | 56 | 34 | 13 | 4 |

The lifecycle trace reserved generation 0, advanced the source to generation 1,
then historically rooted generation 0 while publishing generation 2. The GC
trace likewise reserved generation 0, advanced the source to generation 1, and
then created the stale root. Both actions are now disabled: lease acquisition
refreshes the provisional view to the current head, and root publication
requires that same head through the history PUT and live CAS.

#### Default GREEN commands and results

These are the exact exhaustive commands used from `formal-verifications/tla`:

```bash
JAVA="${JAVA:-/opt/homebrew/opt/openjdk/bin/java}"

"$JAVA" -XX:+UseParallelGC -jar "$HOME/Downloads/tla2tools.jar" \
  -workers auto -config NamespaceBranching.cfg NamespaceBranching.tla

"$JAVA" -XX:+UseParallelGC -jar "$HOME/Downloads/tla2tools.jar" \
  -workers auto -config NamespaceBranchingGc.cfg NamespaceBranchingGc.tla
```

| Model | Result | Generated | Distinct | Queue | Depth |
|-------|--------|----------:|---------:|------:|------:|
| `NamespaceBranching` | PASS | 14,366 | 3,595 | 0 | 33 |
| `NamespaceBranchingGc` | PASS | 430 | 171 | 0 | 18 |

Both completed without an invariant, deadlock, parser, or configuration error.
Each includes a stuttering action, so terminal protocol states are intentional
rather than TLC deadlocks.

#### Reproducing all twelve negative runs

The negative runs use a temporary directory, set exactly one control to
`TRUE`, and check `TypeOK` plus only the intended invariant. Restricting the
temporary config to that invariant prevents a related, overlapping invariant
from masking the expected counterexample name. Run this exact shell block from
`formal-verifications/tla`:

```bash
JAVA="${JAVA:-/opt/homebrew/opt/openjdk/bin/java}"

run_negative() {
  model="$1"
  base_cfg="$2"
  number="$3"
  toggle="$4"
  invariant="$5"
  dir="$(mktemp -d "/tmp/${model}-${number}.XXXXXX")"
  cfg="$dir/negative.cfg"

  awk -v toggle="$toggle" -v invariant="$invariant" '
    /^INVARIANT / { next }
    {
      if ($1 == toggle) sub(/= FALSE/, "= TRUE")
      print
    }
    END {
      print ""
      print "INVARIANT TypeOK"
      print "INVARIANT " invariant
    }
  ' "$base_cfg" > "$cfg"

  "$JAVA" -XX:+UseParallelGC -jar "$HOME/Downloads/tla2tools.jar" \
    -workers auto -config "$cfg" "$model.tla"
  rc=$?
  rm -f "$cfg"
  rmdir "$dir"
  test "$rc" -eq 12
}

run_negative NamespaceBranching NamespaceBranching.cfg 01 \
  AllowPublishWithoutRoot VisibleManifestRequiresRoot
run_negative NamespaceBranching NamespaceBranching.cfg 02 \
  AllowDeleteWithRoots SourceFenceExcludesRoots
run_negative NamespaceBranching NamespaceBranching.cfg 03 \
  AllowRootRemovalBeforeVisibilityGone RootRemovalRequiresTargetVisibilityGone
run_negative NamespaceBranching NamespaceBranching.cfg 04 \
  AllowRootRemovalBeforeReaderGrace RootRemovalRequiresReaderGrace
run_negative NamespaceBranching NamespaceBranching.cfg 05 \
  AllowActivateBeforeSubsystems ActivationRequiresBranchSafeSubsystems
run_negative NamespaceBranching NamespaceBranching.cfg 06 \
  AllowVisibilityRemovalWithoutEvidence VisibilityRemovalRequiresDestructionEvidence
run_negative NamespaceBranchingGc NamespaceBranchingGc.cfg 07 \
  AllowIgnoreBranchPin BranchPinnedGenerationRetained
run_negative NamespaceBranchingGc NamespaceBranchingGc.cfg 08 \
  AllowDeleteForeignPendingKey TargetGcDeletesOnlyTargetOwnedKeys
run_negative NamespaceBranching NamespaceBranching.cfg 09 \
  AllowActivationWithoutPolicyGuard ActivationUsesOneFencedPolicyHead
run_negative NamespaceBranching NamespaceBranching.cfg 10 \
  AllowPolicyWritePastActivationGuard PolicyMutationCannotPassActivationGuard
run_negative NamespaceBranching NamespaceBranching.cfg 11 \
  AllowGuardRemovalBeforeNonceRevocation GuardRemovalRevokesStaleActivationOrObservesActive
run_negative NamespaceBranching NamespaceBranching.cfg 12 \
  AllowDestructionWithStalePreservation EachDestructiveBoundaryUsesFreshPreservationHead
```

Observed counterexamples:

| # | One-hot control | Intended violated invariant | Generated | Distinct | Queue | Depth |
|--:|-----------------|-----------------------------|----------:|---------:|------:|------:|
| 01 | `AllowPublishWithoutRoot` | `VisibleManifestRequiresRoot` | 327 | 204 | 114 | 6 |
| 02 | `AllowDeleteWithRoots` | `SourceFenceExcludesRoots` | 2,420 | 904 | 328 | 9 |
| 03 | `AllowRootRemovalBeforeVisibilityGone` | `RootRemovalRequiresTargetVisibilityGone` | 14,523 | 3,652 | 11 | 32 |
| 04 | `AllowRootRemovalBeforeReaderGrace` | `RootRemovalRequiresReaderGrace` | 13,535 | 3,421 | 17 | 23 |
| 05 | `AllowActivateBeforeSubsystems` | `ActivationRequiresBranchSafeSubsystems` | 6,027 | 1,841 | 385 | 11 |
| 06 | `AllowVisibilityRemovalWithoutEvidence` | `VisibilityRemovalRequiresDestructionEvidence` | 1,369 | 586 | 263 | 8 |
| 07 | `AllowIgnoreBranchPin` | `BranchPinnedGenerationRetained` | 93 | 66 | 16 | 6 |
| 08 | `AllowDeleteForeignPendingKey` | `TargetGcDeletesOnlyTargetOwnedKeys` | 152 | 91 | 20 | 7 |
| 09 | `AllowActivationWithoutPolicyGuard` | `ActivationUsesOneFencedPolicyHead` | 5,654 | 1,760 | 402 | 11 |
| 10 | `AllowPolicyWritePastActivationGuard` | `PolicyMutationCannotPassActivationGuard` | 8,958 | 2,486 | 315 | 13 |
| 11 | `AllowGuardRemovalBeforeNonceRevocation` | `GuardRemovalRevokesStaleActivationOrObservesActive` | 8,818 | 2,471 | 327 | 13 |
| 12 | `AllowDestructionWithStalePreservation` | `EachDestructiveBoundaryUsesFreshPreservationHead` | 251 | 150 | 80 | 6 |

Every negative run exited `12` for its intended invariant. No temporary config
is retained in the repository.

#### Model bounds and limitations

- Both models use three incarnations: `root`, `branch`, and `child`. The
  lifecycle model explores the concrete nested `root -> branch -> child` path;
  the GC model also has a sibling-root scenario so dropping one root cannot
  release another branch.
- To avoid a meaningless Cartesian state explosion, lifecycle concerns are
  partitioned into six initial scenario modes (`fork`, `nested`, `delete`,
  `policy`, `preservation`, and `cancel`), while GC uses four (`nested`,
  `siblings`, `reader`, and `gc`). TLC exhausts the union of those finite modes,
  but it does not prove arbitrary graph size or every cross-product of unrelated
  administrative operations.
- Generation digests are abstracted as an injective finite identity
  (`Digest(g) = g`); exact MessagePack/JSON bytes, SHA-256, and S3 ETag strings
  remain Rust-test obligations. The lifecycle stores the lease-observed head
  identity separately from the pre-CAS head, requires them to agree, and splits
  the source history PUT from the live root CAS.
- Target generation-one history and live publication are separate actions.
  Vector contents, IVF math, HTTP JSON, cache bodies, and object-copy mechanics
  are intentionally outside this packet.
- `ReaderGrace = 2` is a finite abstraction of the configured
  cache-TTL + request-lifetime + compaction-upload-window + skew floor. Time
  decrements admitted-read lifetime, but root release and grace reachability use
  only the durable deadline; no deletion action consults an in-flight-reader
  oracle.
- Object keys are finite representatives of source-owned, target-owned, live,
  historical, and pending-delete artifacts. Rust/MinIO integration tests must
  enumerate every concrete artifact family and verify origin-aware key routing.

The `.tlc.log` files in `formal-verifications/tla/` contain the raw run
summaries. They are generated artifacts; commit the specs/configs and summarize
the logs unless a workflow explicitly asks to preserve the raw logs.

#### Spec-to-Code Cross-Reference

When changing any referenced Rust function, re-check the matching spec action
and rerun `./scripts/run-july-tla.sh` if the abstraction may have drifted.

| Spec action(s) | Rust entry points modeled |
|----------------|---------------------------|
| `PitrHistoryRetention`: `StartCommitArtifacts`, `WriteHistorySnapshot`, `PublishLivePointer` | `src/wal/manifest.rs:616` `Manifest::write`; `src/wal/manifest.rs:658` `Manifest::write_conditional`; `src/wal/manifest.rs:793` `write_history_snapshot_for_commit` |
| `PitrHistoryRetention`: `CreateSnapshotPin`, `DeleteSnapshotPin`, `PruneHistory` | `src/wal/manifest.rs:927` `NamedSnapshot::create`; `src/wal/manifest.rs:749` `Manifest::prune_history_with_retention`; `src/wal/manifest.rs:686` `Manifest::list_history`; `src/wal/manifest.rs:707` `Manifest::read_history` |
| `PitrHistoryRetention`: `ResolveAsOfGeneration`, `ResolveAsOfTimestamp`, `ResolveAsOfSnapshot` | `src/server/handlers/as_of.rs:8` `resolve_manifest`; `src/server/handlers/as_of.rs:50` `read_retained_history_generation`; `src/server/handlers/as_of.rs:66` `resolve_history_at_or_before_timestamp` |
| `PitrHistoryRetention`: `GcSweep` | `src/compaction/gc.rs:140` `retained_manifest_history_reachable_keys`; `src/compaction/gc.rs:544` `run_gc_cycle`; `src/compaction/gc.rs:841` `should_delete_candidate` |
| `TwoPassGcSafety`: `MarkCandidates`, `DrainPendingDelete*`, `SweepCandidate*`, buggy sweep variants | `src/compaction/gc.rs:48` `reachable_keys`; `src/compaction/gc.rs:493` `mark_gc_candidates`; `src/compaction/gc.rs:544` `run_gc_cycle`; `src/compaction/gc.rs:841` `should_delete_candidate` |
| `TwoPassGcSafety`: `CommitStagedUploads`, `ExpireStaging`, future/history root protection | `src/compaction/gc.rs:54` `reachable_keys_with_staging`; `src/compaction/gc.rs:140` `retained_manifest_history_reachable_keys`; `src/compaction/gc.rs:1047` `active_staged_keys` |
| `RestoreCloneSafety`: `ResolveSourceHistory*`, `CreateTargetNamespace`, `RewriteManifestToTarget` | `src/server/handlers/namespace.rs:512` `clone_namespace`; `src/server/handlers/as_of.rs:8` `resolve_manifest`; `src/server/handlers/namespace.rs:740` `rewrite_manifest_stored_keys` |
| `RestoreCloneSafety`: `CopyOneObject*`, `PublishTargetManifest*`, `BuggyPublishBeforeCopy` | `src/server/handlers/namespace.rs:701` `materialize_clone_manifest`; `src/server/handlers/namespace.rs:726` `clone_copy_map`; `src/storage/store.rs:468` `ZeppelinStore::copy_if_not_exists`; `src/wal/manifest.rs:616` `Manifest::write` |
| `RestoreCloneSafety`: failure cleanup and source protection | `src/server/handlers/namespace.rs:612` `release_internal_clone_pin`; `src/server/handlers/namespace.rs:624` `cleanup_failed_clone_target`; `src/wal/manifest.rs:927` `NamedSnapshot::create`; `src/server/handlers/namespace.rs:512` `clone_namespace` |
| `IncrementalArtifactClosure`: `IncrementalCompactTouchCluster`, `IncrementalCompactTouchSecondCluster` | `src/compaction/mod.rs:1572` `write_incremental_segment`; `src/compaction/mod.rs:2173` `incremental_cluster_objects`; `src/wal/manifest.rs:242` `SegmentRef::cluster_owner` |
| `IncrementalArtifactClosure`: `DropOldSegmentRef`, `RetainHistory`, `PruneHistory`, `GcExactReachability`, `BuggyPrefixGc` | `src/compaction/gc.rs:48` `reachable_keys`; `src/compaction/gc.rs:140` `retained_manifest_history_reachable_keys`; `src/compaction/gc.rs:544` `run_gc_cycle`; `src/wal/manifest.rs:749` `Manifest::prune_history_with_retention` |
| `GroupCommitWalWriter`: `UploadFragment`, `EnqueueAppend`, `ElectLeader`, `CommitCompatibleBatch`, `FailBatch*` | `src/wal/writer.rs:100` `append`; `src/wal/writer.rs:122` `append_with_lease`; `src/wal/writer.rs:231` `commit_pending_group` |
| `GroupCommitWalWriter`: `ExternalManifestAdvance`, `DeleteNamespace`, `BuggyMixedTokenDeadlock` | `src/wal/lease.rs:68` `LeaseManager::acquire`; `src/wal/lease.rs:131` `LeaseManager::renew`; `src/wal/lease.rs:173` `LeaseManager::release`; `src/wal/writer.rs:231` `commit_pending_group` |

### Interpreting Counterexamples

When TLC finds an invariant violation, it prints a **counterexample trace** — a sequence of states from Init to the violation. Each state shows all variable values. Read the trace bottom-up to understand the interleaving that caused the bug.

Example trace for `CompactionSafety`:

```
State 1: Init
  manifest_frags = {1, 2}, committed = {1, 2}, c_pc = "idle"

State 2: C_ReadManifest
  c_snap = {1, 2}, c_pc = "manifest_read"

State 3: W1_AcquireMutex
  mutex = "w1", w1_pc = "acquired"

State 4: W1_WriteFragmentToS3
  s3_frag_data = {1, 2, 3}, w1_pc = "frag_written"

State 5: W1_ReadManifest
  w1_snap = {1, 2}, w1_pc = "manifest_read"

State 6: W1_WriteManifestAndRelease
  manifest_frags = {1, 2, 3}, committed = {1, 2, 3}, w1_pc = "done"
  ^ Fragment 3 is now committed and in the manifest

State 7: C_BuildSegment
  c_seg_contents = {1, 2}, c_pc = "seg_built"

State 8: C_WriteManifest
  manifest_frags = {}, manifest_seg = {1, 2}
  ^ VIOLATION: 3 is in committed but not in manifest_frags or manifest_seg
```

### Verifying Candidate Fixes

Each spec includes commented-out fix variants. To verify a fix:

1. Uncomment the fix action (e.g., `C_WriteManifestCAS` in CompactionSafety.tla)
2. Replace the buggy action in the `Next` formula
3. Re-run TLC — it should now find **zero violations**

Candidate fixes included:
- **CAS manifest write**: Abort if manifest changed since snapshot
- **Re-read and merge**: Re-read manifest before writing, merge new fragments
- **Deferred fragment deletion**: Don't delete .wal files immediately; use a grace period
- **Tombstone-based deletion**: Write a tombstone before deleting keys

## Property-Based Tests (proptest)

The property-based tests live in `tests/` as `proptest_*.rs` and run as part of the normal test suite.

### Running

```bash
# Run all property tests
cargo test --test 'proptest_*'

# Run a specific property test
cargo test --test proptest_merge_dedup
cargo test --test proptest_checksum_stability
cargo test --test proptest_filter_eval
cargo test --test proptest_ivf_recall
cargo test --test proptest_namespace_validation
cargo test --test proptest_multi_writer_lease
```

### What Each Test Verifies

| Test File | What It Tests | Method |
|-----------|--------------|--------|
| `tests/proptest_merge_dedup.rs` | WAL merge/dedup logic produces correct surviving vectors | Compare production merge vs HashMap reference impl |
| `tests/proptest_checksum_stability.rs` | WAL fragment checksums survive JSON round-trips | Serialize, deserialize, recompute checksum |
| `tests/proptest_filter_eval.rs` | Filter evaluation (Eq, Range, In, And) matches reference | Compare production evaluator vs simple boolean logic |
| `tests/proptest_ivf_recall.rs` | IVF-Flat search has acceptable recall vs brute-force | Build index, search, measure recall fraction |
| `tests/proptest_namespace_validation.rs` | Namespace name validation handles all edge cases | Compare byte-level validator vs char-level reference |
| `tests/proptest_multi_writer_lease.rs` | Lease protocol state machine (acquire/renew/fence) | Random operation sequences vs protocol invariants |

### TLA+ vs proptest: When to Use Which

| Concern | Tool | Why |
|---------|------|-----|
| Concurrent protocol races | TLA+ | Bugs depend on interleaving order, not input values |
| Sequential logic correctness | proptest | Bugs depend on input edge cases (boundary values, special chars) |
| Manifest read-modify-write | TLA+ | Multi-process interleaving |
| Merge/dedup with arbitrary ops | proptest | Combinatorial input space |
| Fragment checksum stability | proptest | Serialization edge cases |
| Filter evaluation | proptest | Arbitrary filter trees + attribute maps |

## Philosophy

These specs model what the **client expects**, not what the code does. If we transcribed the Rust code into TLA+, it would trivially satisfy itself. Instead:

1. Define the client contract as invariants (durability, visibility, ordering)
2. Model the abstract protocol (the steps, not the Rust implementation)
3. Let TLC find interleavings that violate the contract
4. Use counterexamples to guide implementation fixes
5. Model candidate fixes in TLA+, re-verify before changing Rust code
