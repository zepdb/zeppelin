# Failing Tests & Root Causes

Last run: 2026-02-10 | 3 failures across 2 test suites

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

## Passing Suites

| Suite | Tests | Notes |
|---|---|---|
| `proptest_filter_eval` | 4/4 | Filter evaluation logic is correct |
| `proptest_ivf_recall` | 3/3 | IVF-Flat recall properties hold |
| `proptest_namespace_validation` | 10/10 | All boundary cases handled |
