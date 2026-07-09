# Seed 55834574855 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-55834574855`
- Mode: deterministic
- Failing op: 148, `FetchVectors`
- Namespace: `test-ef8f48d1-fd42-4eb3-91f3-46a84bfcbacc-adv-55834574855-1`
- Violations: `I2DeletedNeverReturned` x1, `I4FetchExact` x2
- Returned deleted id: `v12`

## What Happened

Op 143 deleted `v12`. Op 144 immediately compacted that tombstone and returned
`segment_id: null`, `vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3MY4BAH6YTYRAXV5045GP0`.

The fetch at op 148 requested `v12`. The oracle expected it to be missing, but
the server returned the vector from the old segment.

## Root Cause

The all-deleted compaction path did not retire the old active segment in the
manifest. It removed the WAL tombstone fragment, leaving no strong-read tombstone
to suppress the segment result, while the manifest still advertised the segment
that contained `v12`.

This was a production manifest update bug, not a fetch-specific bug. Fetch by id
uses the active segment membership artifact exactly as the manifest instructs.

## Fix

Retire the old active segment in the manifest when compaction produces no
replacement segment. Fetches after an all-deleted compaction then see no active
segment and return the deleted id in `missing`.
