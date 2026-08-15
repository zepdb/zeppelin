# Late-interaction performance evidence

These measurements are workload-specific operating points, not cloud-object-
storage latency claims. They were recorded on an Apple M3 Max against loopback
MinIO in a release build. Latencies are milliseconds; logical and planned bytes
are decimal megabytes per query at p50.

## Text query profiles

The full replay contained 1,109 queries. The f16 control preserves the current
general default. INT8-G32 is an operator-selected, qualified text-lane format;
the wider 1,200-row frontier recovered the f16 control's measured recall.

| Profile | Matrix dtype | Candidate K | Gap | p50 / p95 / p99 | Scan p50 | Truth p50 | GETs | Logical / planned MB | Recall@10 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| General control | f16 | 1,000 | 64 KiB | 72 / 88 / 104 | 4 | 68 | 730 | 54.2 / 58.1 | 0.980072 |
| High-density latency | f16 | 1,000 | 896 KiB | 52 / 64 / 69 | 4 | 47 | 211 | 54.2 / 188.3 | 0.980072 |
| Speed-biased | INT8-G32 | 1,000 | 512 KiB | 48 / 61 / 68 | 4 | 43 | 199 | 28.8 / 103.3 | 0.974662 |
| Quality-preserving | INT8-G32 | 1,200 | 768 KiB | 53 / 70 / 79 | 4 | 48 | 142 | 34.8 / 127.2 | 0.980703 |

Large coalescing gaps did not transfer to a sparse 50,000-unit workload: a
256-KiB gap removed only 7.4% of GETs while adding 83% planned bytes. The
general default therefore remains 64 KiB. Select a larger gap only as part of
a workload-qualified profile.

## Incremental-compaction equivalence

The MinIO-backed
`late_segment_incremental_compaction_matches_full_rebuild` integration test
compacts the same logical appends, updates, and deletes through an incremental
path and a forced full rebuild.

| Property | Result |
| --- | --- |
| Query comparison | Identical ranked results for four queries at a full frontier |
| Incremental storage | Untouched old-generation matrix blocks carried by reference |
| Reachability | Every carried block remains readable after publication |
| Later rebuild | Results remain identical after a subsequent forced full rebuild |

The `incremental_max_changed_fraction = 0.2` default bounds when Zeppelin
reuses the previous segment's calibration and immutable blocks. Setting it to
`0.0` forces a full rebuild.
