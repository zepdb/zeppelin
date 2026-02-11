# Failing Tests & Root Causes

Last run: 2026-02-10 | 3 proptest failures + 4 TLA+ invariant violations

---

## 1. `proptest_merge_dedup::merge_matches_reference` — FAIL

**Minimal reproduction**:
A single fragment containing `Upsert("vec_5")` followed by `Delete("vec_5")`.

**What happens**:
The production merge in `src/compaction/mod.rs:107-120` processes all deletes in a fragment before all upserts. This reorders intra-fragment operations:

```
Fragment ops (original order): [Upsert("vec_5"), Delete("vec_5")]

Production (deletes-first):   Delete("vec_5") → no-op, then Upsert("vec_5") → vec_5 SURVIVES
Reference  (sequential):      Upsert("vec_5") → inserted, then Delete("vec_5") → vec_5 REMOVED
```

**Root cause**: `src/compaction/mod.rs` loops `fragment.deletes` before `fragment.vectors`, but `WalFragment` stores them as two separate `Vec`s with no ordering between them. When the WAL writer appends an upsert-then-delete in the same fragment, the compaction merge reverses the intent.

**Severity**: P0 — Deleted vectors can silently reappear after compaction.

---

## 2. `proptest_checksum_stability::checksum_stable_after_roundtrip` — FAIL

**Minimal reproduction**:
A fragment with an attribute `Float(-99.528...)` that round-trips through JSON.

**What happens**:
`AttributeValue` in `src/types.rs:26` uses `#[serde(untagged)]`. After JSON round-trip, serde tries each variant in order during deserialization. A float value like `-99.52844334257749` could:
- Serialize as `-99.52844334257749` (JSON number)
- Deserialize back, but serde tries `Integer(i64)` before `Float(f64)` — if the number happens to be integer-like after precision loss, it deserializes as the wrong variant

The checksum is computed from the in-memory representation via `serde_json::to_vec`. Different variant = different serialization = different checksum.

**Root cause**: `#[serde(untagged)]` on `AttributeValue` makes JSON round-trips non-identity for type discrimination. The checksum at write time and the checksum recomputed after read can diverge.

**Severity**: P0 — Spurious checksum validation failures on WAL fragment reads from S3.

---

## 3. `proptest_checksum_stability::checksum_independent_of_insertion_order` — FAIL

**Minimal reproduction**:
Two attributes with the same key `"l"` but different values `String("a")` vs `String("b")`.

**What happens**:
The test inserts `(k1, v1)` then `(k2, v2)` into one HashMap, and `(k2, v2)` then `(k1, v1)` into another. When `k1 == k2`, the second insert overwrites the first — so the two maps have genuinely different content (different surviving value).

**Root cause**: Test strategy doesn't filter `k1 != k2`. This is a **test bug**, not a production bug. The BTreeMap canonicalization in `WalFragment::compute_checksum` is correct for maps with identical content.

**Severity**: Low — Test-only issue. Fix by adding `prop_filter("distinct keys", |(_, _, k1, _, k2, _)| k1 != k2)`.

---

## Passing Proptest Suites

| Suite | Tests | Notes |
|---|---|---|
| `proptest_filter_eval` | 4/4 | Filter evaluation logic is correct |
| `proptest_ivf_recall` | 3/3 | IVF-Flat recall properties hold |
| `proptest_namespace_validation` | 10/10 | All boundary cases handled |

---

## TLA+ Invariant Violations (all 4 expected)

Run with: OpenJDK 21.0.10, TLC 2.19

### 4. `CompactionSafety` — `NoSilentDataLoss` VIOLATED

**Counterexample** (8 states):
```
State 1: Init — manifest has fragments {1, 2}, both committed
State 2: W1 acquires mutex
State 3: W1 writes fragment 3 to S3
State 4: W1 reads manifest (sees {1, 2})
State 5: Compactor reads manifest (sees {1, 2}) — snapshots stale view
State 6: W1 writes manifest {1, 2, 3} and releases mutex — fragment 3 committed
State 7: Compactor builds segment from its snapshot {1, 2}
State 8: Compactor writes manifest: frags={}, seg={1, 2}
         *** Fragment 3 is in committed but NOT in manifest_frags or manifest_seg ***
```

**Bug**: The compactor reads the manifest, does expensive work (segment building), then overwrites the manifest without checking if it changed. A concurrent writer's committed fragment is silently lost.

**Fix**: CAS (compare-and-swap) on manifest writes — abort compaction if manifest changed since snapshot.

---

### 5. `QueryReadConsistency` — `QueryNeverHitsOrphan` VIOLATED

**Counterexample** (7 states):
```
State 1: Init — manifest has fragments {1, 2, 3}
State 2: Compactor reads manifest (snapshots {1, 2, 3})
State 3: Compactor builds segment from {1, 2, 3}
State 4: Query reads manifest (sees fragments {1, 2, 3})
State 5: Compactor writes new manifest: frags={}, seg={1,2,3}
State 6: Compactor deletes old fragment files from S3 — s3_frag_data={}
State 7: Query tries to read fragments {1, 2, 3} from S3 — q_frag_read_ok=FALSE (404!)
```

**Bug**: Query snapshots the manifest listing fragments, then compaction deletes those fragment files before the query reads them. The query gets S3 404 errors.

**Fix**: Deferred fragment deletion — don't delete .wal files immediately. Use a grace period or reference counting so in-flight queries can complete.

---

### 6. `NamespaceDeletion` — `QueryNeverSeesPartialState` VIOLATED

**Counterexample** (7 states):
```
State 1: Init — namespace has manifest, frag1, frag2, meta all present
State 2: Deleter checks namespace exists
State 3: Deleter lists keys to delete: {meta, manifest, frag1, frag2}
State 4: Deleter deletes frag1 (s3_frag1=FALSE, others still exist)
State 5: Query reads manifest (still exists) — expects frag1 + frag2
State 6: Query reads frag1 — MISSING (already deleted)
State 7: Query reads frag2 — exists. q_partial=TRUE
         *** Query saw manifest but only got frag2, not frag1 — partial results ***
```

**Bug**: `delete_namespace` deletes S3 keys one-by-one (non-atomic). A concurrent query can read the manifest (not yet deleted) but then find some fragments already deleted, returning partial/corrupt results.

**Fix**: Tombstone-based deletion — write a "deleting" tombstone first, then queries check for it and abort. Or delete the manifest first so queries fail cleanly with "namespace not found."

---

### 7. `ULIDOrdering` — `LastCommitterWins` VIOLATED

**Counterexample** (7 states):
```
State 1: Init — clock_skew=TRUE, both writers at clock=10
State 2: W1 acquires mutex, gets ULID with timestamp 10
State 3: W1 writes fragment [ulid=10, value=1], commits, releases mutex. Clock advances to 11.
State 4: W2 acquires mutex, but clock_skew is active — W2's clock stays at 10, gets ULID=10
State 5: W2 writes fragment [ulid=10, value=2], commits. commit_order=<<"w1","w2">>
State 6: Query starts
State 7: Query merges by ULID order — both ULIDs are 10, W1's fragment appears first
         in manifest_frags, so W1's value (1) wins. q_result=1
         *** W2 committed last but W1's value is returned ***
```

**Bug**: Under clock skew, W2 (the later writer) can get a ULID that equals or is less than W1's ULID. The "latest ULID wins" merge logic then picks the wrong value. The client expects the last-committed write to win.

**Fix**: Use monotonic ULID generation (track the last issued ULID and always increment), or add a sequence number to the manifest that doesn't depend on wall-clock time.

---

## MultiWriterLease Protocol Verification

Run with: OpenJDK 21.0.10, TLC 2.19 | Spec: `MultiWriterLease.tla`

Unlike the above specs (which model known bugs), this spec models the **proposed fix** — an S3-based lease with fencing tokens. TLC checks that the protocol is correct. Two protocol flaws were discovered and fixed before writing any implementation code.

### 8. `MultiWriterLease` — `FencingPreventsZombie` VIOLATED (initial formulation)

**Counterexample** (11 states):
```
State 1:  Init — manifest_fencing=0, both writers idle
State 2:  W1 acquires lease (token=1)
State 3:  Clock expires W1's lease
State 4:  W1 writes fragment 3 to S3 (doesn't know lease expired)
State 5:  W1 reads manifest — snapshots {1,2}, etag=1
State 6:  W1 passes CheckFencing — manifest_fencing=0 ≤ token=1 ✓
State 7:  W2 acquires lease (token=2, takeover of expired W1 lease)
State 8:  W2 writes fragment 4 to S3
State 9:  W2 reads manifest — snapshots {1,2}, etag=1
State 10: W2 passes CheckFencing — manifest_fencing=0 ≤ token=2 ✓
State 11: W2 commits — manifest_frags={1,2,4}, manifest_fencing=2, etag=2
          *** W1 is at "fencing_ok" but manifest_fencing=2 > W1's token=1 ***
```

**Bug**: TOCTOU (time-of-check-to-time-of-use) gap between `CheckFencing` and `WriteManifest`. Between these two steps, another writer can commit and bump `manifest_fencing` above the zombie's token. The zombie passes the fencing check but the invariant is violated.

**Why it's actually safe**: The CAS catches this — W1's CAS fails (etag changed from 1 to 2). On retry, W1 re-reads the manifest, re-checks fencing (manifest_fencing=2 > token=1), and aborts. The **two-layer defense** (CheckFencing + CAS) prevents the zombie from committing.

**Fix (invariant)**: The invariant was too strict — it checked at the "fencing_ok" state. The correct invariant checks at "committed": `(w1_pc = "committed" => manifest_fencing >= w1_local_token)`.

**Fix (protocol insight)**: Neither CheckFencing nor CAS alone is sufficient. Both layers are needed for safety:
- Layer 1 (CheckFencing): Catches zombies when another writer has already committed
- Layer 2 (CAS): Catches the TOCTOU gap when another writer commits between check and write

**Test**: `tests/multi_writer_lease_tests.rs::test_tla_toctou_fencing_gap_cas_catches_zombie`

---

### 9. `MultiWriterLease` — Deadlock in `ReleaseLease` after lease expiry

**Counterexample** (17 states, abbreviated):
```
State 2:  W1 acquires lease (token=1)
State 3:  Clock expires W1's lease
State 6:  W2 acquires lease (token=2, takeover)
State 7:  Clock expires W2's lease
State 8:  C (compactor) acquires lease (token=3, takeover)
State 14: C commits, writes manifest, releases lease — lease_holder="none"
State 15: W1 at "aborted" — tries ReleaseLease, but lease_holder="none" (not "w1")
State 16: W2 at "aborted" — tries ReleaseLease, but lease_holder="none" (not "w2")
          *** DEADLOCK — W1 and W2 can never transition to "done" ***
```

**Bug**: When a process is aborted (fencing check failed or CAS exhausted retries) and its lease was taken over by another process (due to expiry), the `ReleaseLease` step's guard `lease_holder = "w1"` is never satisfied. The process is stuck forever — it can't release a lease it doesn't hold.

**Fix**: Made `ReleaseLease` conditional — if the process still holds the lease, release it; if the lease was taken over, skip the release and move directly to "done":
```tla
W1_ReleaseLease ==
    /\ w1_pc \in {"committed", "aborted"}
    /\ IF lease_holder = "w1"
       THEN /\ lease_holder' = "none" /\ lease_expired' = FALSE /\ lease_etag' = lease_etag + 1
       ELSE /\ UNCHANGED <<lease_holder, lease_expired, lease_etag>>
    /\ w1_pc' = "done"
```

**Implementation requirement**: `LeaseManager::release()` must be best-effort. When the lease was already taken over, it should return `Ok(())` or a non-fatal warning — not a hard error that blocks the caller.

**Test**: `tests/multi_writer_lease_tests.rs::test_tla_graceful_release_after_lease_expiry`

---

### Final TLC Result (after fixes)

```
Model checking completed. No error has been found.
24965 states generated, 9532 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 21.
Finished in 00s.
```

All 4 invariants hold: `NoSilentDataLoss`, `LeaseExclusivity`, `FencingPreventsZombie`, `TypeOK`.
