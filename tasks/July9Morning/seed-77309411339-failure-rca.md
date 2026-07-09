# Seed 77309411339 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-77309411339`
- Mode: deterministic
- Failing op: 250, `Query`
- Namespace: `test-7af7e871-03be-4009-8860-86c8c56729f1-adv-77309411339-3`
- Violations: `I1StrongExact` x14, `I2DeletedNeverReturned` x14

## What Happened

After a prior segment was built at op 128, later delete operations removed the
remaining visible ids. Op 242 compacted the delete-only state and returned
`segment_id: null`, `vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3NJVQXKP678ABW0KQTJMEZ`.

The failing strong query at op 250 returned fourteen ids that the model had
already deleted. The same ids also appeared outside the visible strong set,
producing the paired I1 violations.

## Root Cause

The all-deleted compaction branch compacted away tombstones but left the old
segment as the manifest's active segment. Once the tombstones were gone, strong
query merge logic had no WAL delete set available to suppress the stale segment
hits.

The I1 violations were secondary evidence of the same issue: returned ids were
outside the model's visible set because they came from a segment the manifest
should no longer expose.

## Fix

Remove the old active segment reference when compaction produces zero surviving
vectors. Strong queries after a delete-only compaction will then return no stale
segment hits.
