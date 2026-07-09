# Seed 98784247809 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-98784247809`
- Mode: deterministic
- Failing op: 134, `Query`
- Namespace: `test-a4fc08c3-6c4c-46fd-bf5b-a7929a1a8fdb-adv-98784247809-1`
- Violations: `I1StrongExact` x10, `I2DeletedNeverReturned` x10

## What Happened

After compaction built a replacement segment at op 91, deletes at ops 99 and
100 removed the visible ids from that segment. Op 105 compacted the delete-only
state and returned `segment_id: null`, `vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3P2H0TNY95MHK83C9XNPGK`.

Later upserts added new WAL-only rows. The failing strong query at op 134 scanned
both WAL and the stale active segment, returning ten ids that had already been
deleted from the old segment.

## Root Cause

The all-deleted compaction path failed to clear the old active segment from the
manifest. The subsequent WAL-only upserts were valid, but reads still also
searched the stale segment because it remained active. With the tombstones
removed by compaction, strong query merge had no delete set for those old ids.

The I1 violations were a downstream consequence: the same stale ids were outside
the visible strong set.

## Fix

When an all-deleted compaction has no surviving vectors, remove the old active
segment reference from the manifest. New WAL-only rows then remain the only
visible data until the next segment build.
