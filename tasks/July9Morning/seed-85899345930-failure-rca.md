# Seed 85899345930 Failure RCA

## Summary

- Run: `target/adversarial/one-hour/run-1783606073`
- Seed dir: `target/adversarial/one-hour/run-1783606073/seed-85899345930`
- Mode: deterministic
- Failing op: 169, `BatchQuery`
- Namespace: `test-41de8177-7763-4446-b87b-896bb6f135aa-adv-85899345930-2`
- Violations: `I2DeletedNeverReturned` x32

## What Happened

The namespace used product quantization with bitmap indexing enabled. Op 168
compacted the namespace after deletes removed all surviving vectors from the
active segment. The compactor returned `segment_id: null`,
`vectors_compacted: 0`, and
`old_segment_removed: seg_01KX3NSR0GMKBFADTVH485E9CD`.

The following batch query returned many ids that were previously deleted.

## Root Cause

This was the same stale active segment bug as the fetch/query seeds, not a
product-quantization-specific scoring bug. The all-deleted compaction path queued
the old product-quantized segment for deletion but left it active in the manifest
after removing the WAL tombstones.

The batch query path then read the manifest and searched the old active segment.
Because the delete tombstones had been compacted away, the merge step could not
suppress those stale results.

## Fix

Retire the old active segment when compaction produces no replacement segment.
The manifest must represent the empty compacted state, independent of
quantization mode.
