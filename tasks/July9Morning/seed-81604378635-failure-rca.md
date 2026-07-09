# Seed 81604378635 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-81604378635`
- Mode: deterministic
- Failing op: 108, `FetchVectors`
- Namespace: `test-6f0d1489-17d8-4b9a-ac58-a4e7077b887a-adv-81604378635-1`
- Violations: `I2DeletedNeverReturned` x2, `I4FetchExact` x2
- Returned deleted ids: `v1`, `v11`

## What Happened

Deletes at ops 100 and 102 removed the remaining live ids, including `v1` and
`v11`. Op 105 compacted the tombstones and returned `segment_id: null`,
`vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3NP3NNGKCFBEHGFTGBKQ21`.

The fetch at op 108 requested `v1` and `v11`. The server returned both from the
old segment even though the model expected them to be missing.

## Root Cause

The compactor's all-deleted branch did not update the manifest to clear the old
active segment. It only removed compacted WAL fragments and queued the segment
objects for deletion. Fetch by id then followed the stale active segment
membership and returned deleted rows.

## Fix

Clear and remove the old active segment from the manifest in the all-deleted
compaction branch before removing compacted fragments. This makes fetch by id
observe the no-active-segment state after the delete tombstones are made durable.
