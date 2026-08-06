# Phase 09 — Resident SQ8 scan parallelization

Status: **WIN — all win criteria hold; committed on `ll-09-scan-parallel`.**

Branch `ll-09-scan-parallel` off `main` at `a159c4b`. The pre-existing
exploratory branch of the same name (source of the earlier
`phase09-scanpar-*` / `phase09-int8-*` probe JSONs) was preserved
untouched as `ll-09-scan-parallel-probe`; nothing in it was reused as a
measurement.

## What changed

`select_flat_top_k` in `src/index/late_interaction/flat_candidate.rs`
chunks the resident SQ8 scan across scoped standard-library workers —
the same mechanism the phase 07 truth wave uses — with one bounded
top-K heap per chunk and a final cross-chunk merge. Ordering and
tie-breaks follow the pinned ascending `(total_cmp score, row index)`
comparator everywhere, so the selection is bit-identical to the
sequential scan-sort-truncate path. Production `select_candidates`
routes through the seam with its exclusion/filter admission applied
per row before heap insertion (finiteness is still checked pre-filter,
preserving the sequential error semantics; the lowest-indexed
non-finite row wins). Worker count resolves as
`available_parallelism().min(rows.div_ceil(256)).max(1)` = 16 on this
host.

Unit tests pin exact equality against a sequential reference on a
fixed 4,096-row fragment built from 64 distinct patterns (64-way exact
score ties, so the whole selection is decided by tie-breaking), with
and without an admission filter, plus a K-wider-than-corpus arm and
invalid-shape rejection.

The bench (`phase9_flat_sq8_bench.rs`) times the parallel seam as the
scan, then runs an **untimed** sequential `flat_selection_order` pass
per query and errors on any rank where the candidate index or score
bits diverge — 1,109 SciFact + 300 synthetic + 20 filtered queries all
passed the bitwise check (`scan_parallel_equals_sequential: true`).
The 50k corpus is the phase 05 generator, same seeds; its document,
query, SQ8-code, and calibration digests reproduced the phase 05
values exactly. The SciFact query-tensor digest re-pin from the
2026-08-05 regeneration (documented in the probe's `442026e`) was
applied because `/private/tmp` had lost the 2026-07-30 tensors; every
behavioral pin reproduced exactly. Report schema bumped (BenchReport
4→5, synthetic 1→2) for the additive scan fields.

## Configuration

Both runs: K=1000, C16, gap 262,144 B, max request 8 MiB, streamed
truth wave (pipeline always on since phase 06), f16 truth matrices.
The 256 KiB gap matches the phase 08 combined baseline (and the
phase 05 arm-b shape) so the ±3 ms e2e comparison is like-for-like;
the production gap default remains 64 KiB and is untouched. The scan
term itself is knob-independent. Bench lock held around the release
build and both timed runs; single-run walls were 124.7 s and 218.9 s.

## Before / after

All values milliseconds. "Seq scan" is the in-run untimed sequential
verification pass (same process, same resident codes); prior-run
baselines shown for cross-checks. Fetch and score wall overlap
(schema-4 semantics) — do not add their percentiles to truth.

### SciFact (5,183 docs, 1,109 queries) — `phase09-scifact.json`

| Metric | Before (phase 08 / in-run seq) | After | Delta |
| --- | ---: | ---: | ---: |
| Scan p50 | 42 (ph08) / 40.474 (seq) | **4.454** | **9.09x** |
| Scan p95 | 45 (ph08) / 41.765 (seq) | 5.524 | 7.56x |
| Truth p50 | 59 (ph08) | 56 | −3 |
| Truth p95 | 77 (ph08) | 69 | −8 |
| E2E p50 | 102 (ph08) | **61** | **−41** |
| E2E p95 | 121 (ph08) | 73 | −48 |

Critical-worker scoring p50 3.801; merge p50 0.103 (2.3% of scan
wall); 16 workers. GETs p50 460 (mean 460.04 vs ph08 460.33), planned
bytes p50 95,704,420 — request shape unchanged.

### 50k synthetic (50,000 units, 300 queries) — `phase09-synth50k.json`

| Metric | Before (phase 05 arm b / in-run seq) | After | Delta |
| --- | ---: | ---: | ---: |
| Scan p50 | 393 (ph05) / 392.567 (seq) | **33.799** | **11.61x** |
| Scan p95 | 402.008 (seq) | 38.550 | 10.43x |
| Truth p50 | 64 (ph05, no pipeline) | 60 | −4 |
| E2E p50 | 458 (ph05) | **95** | **−363** |
| E2E p95 | — | 103 | — |

Critical-worker scoring p50 33.044; merge p50 0.321 (0.9% of scan
wall); 16 workers. GETs p50 894 and planned bytes p50 22,560,662 —
byte-identical to phase 05 arm b, confirming the same corpus and plan.

## Scaling story

Sequential/N at 16 workers would be 24.535 ms at 50k; measured 33.799
ms = 72.6% parallel efficiency (SciFact: 2.530 ideal vs 4.454 = 56.8%,
spawn/merge floor visible at the smaller corpus). The gap to ideal is
in the scoring stage itself (critical worker 33.044 of the 33.799 ms
wall), i.e. chunk-scan skew and memory bandwidth, not the merge: merge
cost is 0.10–0.32 ms (≤2.3% of the wall) because each chunk hands back
at most K=1000 pre-sorted entries and the final sort touches ≤16,000
entries. Effective scan traversal rate rose from 1.30 GB/s
(sequential) to 15.15 GB/s (parallel) over the 512 MB code block. The
50k scan therefore approaches, but does not reach, sequential/N; the
shortfall is bandwidth/skew-bound, and at e2e level the scan is no
longer the dominant payer (33.8 ms scan vs 60 ms truth wave).

## Win criteria — verdict: **WIN**

| Criterion | Result |
| --- | --- |
| Bit-identical candidates everywhere | **PASS** — per-query bitwise index+score assertions on all 1,109 + 300 + 20 filtered queries; recall exactly **10,869/11,090** (0.980072), hits 10,708/10,869/10,993 at K=700/1000/1500, tail min 6/10, 10 queries <8/10 (pin ≤12), 0 <5/10 |
| 50k scan p50 ≥3x, no SciFact e2e regression beyond ±3 ms | **PASS** — 11.61x at 50k; SciFact e2e 102→61 ms (improvement, not regression) |
| No new dependencies; observed==planned GETs untouched | **PASS** — no Cargo.toml change; `observed_get_requests_equal_planned: true` in both runs (510,184 and 267,860 total GETs respectively) |

Kill criteria: none fired (merge overhead ≤2.3% of the scan wall; no
divergence anywhere; no change outside the late-interaction scan path
and its bench).

## Research note — an IVF-routed resident lane (prose only, fenced)

What it would look like: partition the document FDE space with the
production k-means seam (as the dense side and the dormant
`candidate.rs` wave-one IVF already do), store per-cluster SQ8 code
blocks plus centroids in the resident artifact, and at query time
score centroids first, then scan only the top-`nprobe` clusters'
codes before the same top-K merge. The scan term would drop from
O(rows) to O(probed rows), turning the 50k scan's 33.8 ms into a few
ms and changing the corpus-size scaling law from linear to sublinear.

What it would trade: candidate selection would no longer be exhaustive,
so the bit-identity guarantee this phase preserved is gone by
construction — a recall gate on a representative workload becomes
mandatory (SciFact's 19% density cannot decide it, per the standing
dataset caveat). It also needs a new artifact version (cluster
directory + reordered codes), a build-time clustering cost, and an
nprobe policy with the same scale-awareness rules the dense side pins.

What measurement would justify it: a corpus large enough that the
parallel scan is again a co-payer (≥200k units at ~135 ms projected
parallel scan), a recall curve over nprobe against the exhaustive
frontier on that workload showing ≥0.98 of exhaustive recall at a
probe fraction ≤1/4, and an e2e win after the truth wave's ~60 ms
floor is accounted for. Below that scale the flat scan at 15 GB/s is
simpler and already sufficient.

## Validation

| Gate | Result |
| --- | --- |
| Recall + tails (SciFact full replay) | exact pins, see above |
| Bitwise parallel==sequential, all queries both corpora + filtered arm | pass (run-fatal check) |
| Unit: `parallel_scan_matches_sequential_selection_exactly` (tie fixture) | pass |
| Unit: `flat_scan_rejects_invalid_shapes` | pass |
| `cargo test --lib` | 799 passed, 0 failed, 4 ignored |
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --all-targets -- -D warnings` | clean |
| Bench-lock protocol | held for the release build and both runs; released after |
| Namespace cleanup | both run-owned prefixes deleted by the harness |

`phase9_routing_diagnostic.rs`, production defaults, prior results
files, `stash@{0}`, and `.pi-subagents/` untouched. Phase 10 not
started per the STOP rule.
