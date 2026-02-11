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
  proptest/                         # Property-based test files (Rust)
    merge_dedup.rs                  # Merge/dedup correctness vs reference
    checksum_stability.rs           # WAL fragment round-trip checksum
    filter_eval.rs                  # Filter tree evaluation vs simple evaluator
    ivf_recall.rs                   # IVF-Flat recall vs brute-force
    namespace_validation.rs         # Namespace name boundary cases
```

## TLA+ Specifications

### Prerequisites

Install the TLA+ tools:

```bash
# Download tla2tools.jar (the TLC model checker)
wget https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

# Or use the TLA+ Toolbox IDE (includes TLC)
# https://lamport.azurewebsites.net/tla/toolbox.html
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

### What Each Spec Verifies

| Spec | Invariant | Expected | Bug Found |
|------|-----------|----------|-----------|
| `CompactionSafety` | `NoSilentDataLoss` | VIOLATED | Compactor overwrites writer's manifest update, losing committed fragments |
| `QueryReadConsistency` | `QueryNeverHitsOrphan` | VIOLATED | Query reads stale manifest, compactor deletes fragments, query gets 404 |
| `NamespaceDeletion` | `QueryNeverSeesPartialState` | VIOLATED | Non-atomic prefix deletion exposes half-deleted state to queries |
| `ULIDOrdering` | `LastCommitterWins` | VIOLATED (with skew) | Clock skew reverses ULID order, wrong value wins in merge |

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

### Prerequisites

Add `proptest` to dev-dependencies in `Cargo.toml`:

```toml
[dev-dependencies]
proptest = "1"
```

### Running

Copy the proptest files to the `tests/` directory and run:

```bash
# Copy all proptest files
for f in formal-verifications/proptest/*.rs; do
  cp "$f" "tests/proptest_$(basename "$f")"
done

# Run all property tests
cargo test --test 'proptest_*'

# Run a specific property test
cargo test --test proptest_merge_dedup
cargo test --test proptest_checksum
cargo test --test proptest_filter
cargo test --test proptest_ivf_recall
cargo test --test proptest_namespace
```

### What Each Test Verifies

| Test File | What It Tests | Method |
|-----------|--------------|--------|
| `merge_dedup.rs` | WAL merge/dedup logic produces correct surviving vectors | Compare production merge vs HashMap reference impl |
| `checksum_stability.rs` | WAL fragment checksums survive JSON round-trips | Serialize, deserialize, recompute checksum |
| `filter_eval.rs` | Filter evaluation (Eq, Range, In, And) matches reference | Compare production evaluator vs simple boolean logic |
| `ivf_recall.rs` | IVF-Flat search has acceptable recall vs brute-force | Build index, search, measure recall fraction |
| `namespace_validation.rs` | Namespace name validation handles all edge cases | Compare byte-level validator vs char-level reference |

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
