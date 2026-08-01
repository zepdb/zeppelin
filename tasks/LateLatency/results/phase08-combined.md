# Phase 08 — Combined-levers confirmation

Status: **PASS**. The definitive 1,109-query f16 run used the combined
streaming and CPU levers. No fallback was used: the JSON records
`pipeline_enabled=true` and `truth_score_workers=16`.

## Configuration and integration

| Setting | Value |
| --- | ---: |
| Read concurrency | 16 |
| Gap budget | 262,144 B (256 KiB) |
| Maximum request | 8,388,608 B (8 MiB) |
| Queries | 1,109 |
| Candidate K | 1,000 |
| Matrix dtype | f16 |
| Pipeline | enabled |
| Scoped scoring workers | 16 |

Merge commit `441600c` combines `d5f5140` and `dc73ad7`. Streamed
read-plan completions retain the phase-04 logical back-map and dispatch each
ready candidate by candidate index to the phase-07 scoped scoring pool. Each
worker owns one reusable `MatrixDecodeScratch`, so the slice f16 decode path
and removal of the redundant matrix revalidation remain active. Indexed
worker results are restored to candidate order before the benchmark's final
score ordering.

The pipeline remains test/bench-only and opt-in through
`MMLI_FLAT_BENCH_PIPELINE=1`; production defaults and the production streamed
read path are unchanged.

Timing semantics in schema 4:

- score wall is the elapsed time from the first candidate score starting to
  the last candidate score finishing across the scoped pool;
- decode and MaxSim are the maximum accumulated time on one worker, matching
  phase 07's critical-worker reporting;
- fetch and score wall overlap, so their percentiles must not be added to the
  truth-wave percentile.

## Reference comparison

All timing columns are p50 milliseconds. Planned bytes use decimal MB.

| Reference | E2E | Truth | Fetch | Decode | MaxSim | Score | SQ8 | GETs | Planned MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Phase 01 original baseline | 455 | 417 | 62.685 | 175.999 | 177.595 | 353.902 | 38 | 730 | 58.068 |
| Phase 07 baseline rerun | 461 | 421 | 62.625 | 177.483 | 180.096 | 358.165 | 40 | 730 | 58.068 |
| Phase 04 pipelined confirm | 404 | 363 | 36.198 | 180.635 | 180.556 | 360.743 | 40 | 461 | 95.696 |
| Phase 07 levers 1+2 | 136 | 95 | 61.519 | 13.774 | 19.826 | 32.581 | 41 | 730 | 58.068 |
| **Phase 08 combined** | **102** | **59** | **57.341** | **12.625** | **16.785** | **57.658** | **42** | **461** | **95.696** |

Phase 04's score value is its one scorer's active duration. Phase 07 and
phase 08 report parallel stage wall, with phase 08 including streamed-input
availability inside the stage span.

The combined run is 4.46x faster than the 455 ms original baseline and 4.52x
faster than the 461 ms same-day rerun. It improves phase 04 by 302 ms (74.8%)
and phase 07 levers 1+2 by 34 ms (25.0%).

## Combined timing split

Milliseconds:

| Component | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: |
| End to end | 102.000 | 121.000 | 132.000 | 103.224 |
| SQ8 resident scan | 42.000 | 45.000 | 46.000 | 42.069 |
| Truth wave | 59.000 | 77.000 | 88.000 | 60.745 |
| Fetch | 57.341 | 73.771 | 83.258 | 58.723 |
| Decode, critical worker | 12.625 | 14.991 | 20.178 | 12.951 |
| MaxSim, critical worker | 16.785 | 30.708 | 37.279 | 18.255 |
| Score wall | 57.658 | 75.125 | 84.854 | 59.118 |

The independently computed percentiles do not add exactly.

## GET and byte shape

| Measure | p50 | Mean | Total |
| --- | ---: | ---: | ---: |
| Logical ranges | 2,000 | 2,000.000 | — |
| Physical GETs | 461 | 460.328 | 510,504 |
| Request waves | 29 | 29.234 | 32,420 |
| Logical bytes | 54,204,336 | 54,461,271.589 | — |
| Planned bytes | 95,695,896 | 95,868,281.136 | — |
| Gap-waste bytes | 41,408,622 | 41,407,009.547 | — |

The selected knee therefore remains about 461 GETs and 95.9 MB planned per
query (91.4 MiB mean). Every query's observed GET count equaled its plan; the
510,504 recorded GET latencies also equal the total planned GET count.

## Composed-model check

The pre-run composition used phase 07's non-truth payer and CPU wall with
phase 04's optimized fetch:

```text
non-truth payer       = 136 - 95 = 41 ms
optimized fetch       = 36.198208 ms
parallel score wall   = 32.581250 ms
ideal pipelined E2E   = 41 + max(36.198208, 32.581250)
                       = 77.198208 ms
```

That supported the rough 75–100 ms expectation. Actual p50 was 102 ms, 2 ms
above that range and 24.802 ms above the ideal point. The gap is explained by
contention in the composed run:

```text
fetch inflation       = 57.341041 - 36.198208 = 21.142833 ms
score-wall inflation  = 57.657917 - 32.581250 = 25.076667 ms
pipeline residual     = 59 - max(57.341041, 57.657917)
                       = 1.342083 ms
actual composition    ≈ 43 + 57.657917 + 1.342083 = 102 ms
```

The 16 scoring workers contend with the C16 MinIO request stack for CPU and
memory bandwidth, stretching both fetch and the scoring-stage span. The small
1.342 ms truth residual shows that streaming still overlaps the inflated
stages effectively; the gap is contention, not a restored collect barrier.

## Acceptance and validation

| Gate | Result |
| --- | --- |
| Recall | **10,869 / 11,090 exactly** (`0.9800721370604147`) |
| Tail minimum | **6 / 10** |
| Queries below 8 / 10 | **10** (pin: at most 12) |
| Queries below 5 / 10 | **0** |
| Observed GETs vs planned | **equal** |
| Ordered top-10 shape | 1,109 query lists, 10 IDs each |
| Filtered arm | 20 / 20 queries valid |
| Decode bit identity | 1 passed |
| Sequential/parallel/pipelined score identity | 1 passed |
| Segment-search module | 7 passed |
| Read-plan module | 5 passed |
| Formatting and diff whitespace | clean |
| Strict library clippy | clean |
| All-target clippy | clean with only the documented diagnostic-file baseline allowance |

Both full release executions used the required
`/private/tmp/zeppelin-bench-lock` acquisition loop and released the lock
immediately afterward. The first pass was discarded because it exposed the
pre-reporting instrumentation mismatch between critical-worker active time
and literal score wall. The definitive pass ran after `53e4b44`, completed in
125.86 seconds, cleaned its MinIO namespace, and wrote
`phase08-combined.json`.

`phase9_routing_diagnostic.rs`, dense IVF files, production defaults,
`stash@{0}`, `.pi-subagents/`, and `target-worktree/` were untouched.
