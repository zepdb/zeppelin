# Seed 47244640259 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-47244640259`
- Mode: deterministic
- Failing op: 165, `PaginateAll`
- Namespace: `test-05e6053f-2332-4dff-9089-78152ad82665-adv-47244640259-1`
- Violations: `I2DeletedNeverReturned` x3
- Returned deleted ids: `v13`, `v16`, `v10`

## What Happened

The namespace was compacted at op 150 after all prior live vectors had been
deleted. The compactor returned `segment_id: null`, `vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3MMEFESV5KSF9CHN0AYC3A`.

After that, ops 151, 155, and 156 upserted new vectors into the WAL. The failing
pagination query at op 165 scanned one segment plus three WAL fragments and
returned old deleted ids from the stale segment.

## Root Cause

The compaction all-deleted branch removed the compacted WAL fragments and queued
the old segment's objects for deletion, but it did not clear the manifest's
`active_segment` or remove the old `SegmentRef`.

Once the delete tombstones were compacted away, strong queries no longer had WAL
tombstones to suppress the stale active segment. The query path correctly served
the manifest's active segment, but the manifest still pointed at a segment that
should have been retired.

## Fix

When all surviving vectors are deleted and no replacement segment is produced,
remove the old active segment from the manifest before removing the compacted WAL
fragments. This leaves subsequent reads to use only new WAL fragments until a new
segment is built.
