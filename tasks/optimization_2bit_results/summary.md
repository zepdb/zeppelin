# TwoBit warm dense-query optimization — results ledger

**Executed:** 2026-08-05
**Plan:** `tasks/optimization_plan_2bit.md`
**Base commit:** `9f0f4119f61463ee3c8b52c8e95c58921f4f99a3` (changes uncommitted in working tree)
**Host:** Apple M3 Max, macOS 27.0, rustc 1.93.0 (254b5960), loopback MinIO (`/tmp/zeppelin-profile-minio`)
**Primary cell (W1 repeat-range-warm):** dbpedia100k (100,000 x 1,536 cosine), one compacted
TwoBit/ZBP5 segment, 256 clusters, omitted-nprobe policy resolving to 48, `top_k = 10`, strong
consistency, no filter, direct query layer (`execute_query`), 201 queries with the first
(bootstrap-cold) query excluded, `CARGO_INCREMENTAL=0` release builds.
**Harness:** `/tmp/zeppelin-default-runner` (`devbench_eval`), namespace
`ragbench-dbpedia100k-015f3f02-345d-4a7c-9fbc-96ef93d7526c` (kept for reuse), run JSON under
`/tmp/opt2bit_*_np48.json`, logs `/tmp/opt2bit-*.log`. The harness emits a FNV-1a
`results_hash` over `(query_id, result ids, score bits)` for every run; equal hashes across
builds prove bit-identical results.

## Headline

| Cell | Baseline (mean / p50 / p95 ms) | Final (mean / p50 / p95 ms) | Delta | Exact parity |
| --- | --- | --- | --- | --- |
| W1 dev queries 0-200, top10 | 11.380 / 11.351 / 12.958 (3 runs) | 8.000 / 7.979 / 9.281 (3 runs) | **mean -29.7%, p50 -29.7%, p95 -28.4%** | hash `b4e445426cd1af80` identical |
| W1 held-out queries 500-699, top10 | 11.857 / 11.903 / 13.320 | 8.314 / 8.336 / 9.531 | mean **-29.9%** | hash `924e5ce98cf329fd` identical |
| W1 top100 sentinel, queries 0-200 | 16.075 / 15.923 / 20.346 | 11.523 / 11.001 / 15.789 | mean -28.3%, p95 -22.4% | hash `21b16bce84b8c2c0` identical |

p99 (dev cell, 3-run averages): 13.60 -> 9.68 ms (-28.9%). Run spread <=0.8% in both arms.
recall@10 = 0.9885572 (dev) and 0.9850 (held-out) unchanged to full precision; top100
recall@100 = 0.98164 unchanged. GETs/query (37.68) and bytes/query (16.256 MB) unchanged in
every accepted slice — the entire win is CPU work removal plus I/O/CPU overlap, at an
unchanged fetch plan. First-query (bootstrap-cold) latency ~46-50 ms is unchanged.

## Accepted slices

| Slice | Change | Dev-cell mean before -> after | Decision |
| --- | --- | --- | --- |
| S1a | `RqClusterCodes(Only)::from_bytes`: clamped `with_capacity` + `extend` word decode (`src/index/quantization/rq.rs`) | bundled with S1 | ACCEPT |
| S1b | Reuse the resident sketch's validated `StructuredRotation` and pre-rotated centroids in `scan_clusters_rq`; loud error on geometry disagreement; `ResidentSketch::rabitq_geometry` accessor (`search.rs`, `sketch.rs`) | bundled with S1 | ACCEPT |
| S1c | Fused single-pass two-bit kernel `QueryAdc4::two_bit_grid_dot_and_agreement` (11 popcounts in one word loop; identical integer totals and float expression order) used by `estimate_residual_dot_two_bit_parts` (`rabitq.rs`) | bundled with S1 | ACCEPT |
| S1d | Word-batched `prepare_query_adc4` packing (register-accumulated planes, identical per-coordinate arithmetic and dither draw sequence; `step == 0` early return draws nothing, as before) (`rabitq.rs`) | 11.380 -> 8.433 (**-25.9%**) | ACCEPT |
| S3 | Consume coarse objects via `FuturesUnordered` as range reads complete, overlapping decode/ADC/scoring with outstanding reads; probe-coverage and duplicate checks preserved; non-probe grouped siblings still decoded-but-unscored (`search.rs`) | 8.433 -> 8.302 (-1.6%) | ACCEPT (below the preregistered 5% scheduling gate; kept because it is bit-exact, consistently non-overlapping with the prior distribution, and structurally hides CPU behind slower object tails on higher-latency backends) |
| S4 | `RqCoarseRows::take_id`: move ZBP5 codes-only IDs into candidates instead of cloning (~26.6k String clones/query removed); legacy inline-ID variant keeps its clone; `reserve` before the row loop (`search.rs`) | 8.302 -> 7.980 (**-3.9%**) | ACCEPT |

Slice attribution used interleaved A/B runs against the same ingested namespace; every
accepted slice reproduced the baseline `results_hash` on the dev cell, and the final build
also matched on held-out and top100 cells.

## Measured and rejected / knob-only

| Item | Result | Decision |
| --- | --- | --- |
| S2 rerank coalescing-gap sweep (env knob, no code change): 1 MiB 8.45 mean / 37.7 GETs / 16.26 MB; 256 KiB 7.79 / 44.2 / 12.92; 128 KiB 7.84 / 48.9 / 12.03; 64 KiB 7.99 / 54.1 / 11.54; 32 KiB 8.22; 0 8.88. All hashes identical. | Local latency knee at 256 KiB (-7.9% mean, -10.5% p95) | NO PRODUCT CHANGE. The 1 MiB default is the S3 request-cost choice (`src/config.rs` rationale); the existing `query.cost_latency_profile = "low_latency"` (128 KiB) already captures ~-7% mean / -10% p95 on byte-dominated deployments such as loopback MinIO. Recorded as a deployment recommendation only. |
| S5 batch row scorer (`asymmetric_distances_into`: validate once per cluster, exact-stride row walk) | score component 1.498 -> 1.457 ms (-3%), end-to-end 7.98 -> 7.97 (noise) | REVERTED. Scoring already overlaps the coarse I/O wave after S3, and the fused kernel dominates per-row cost; misses the >=10% component and >=2% end-to-end gates. |

## Phase decomposition (temporary `ZEPPELIN_RQ_PHASE_STATS` instrumentation, since removed)

Mean per query, dev cell:

| Phase | Baseline-era estimate | After S4 |
| --- | --- | --- |
| coarse total (`scan_clusters_rq` before rerank) | ~5.4 + wave 3.5 | 4.75 |
| — I/O wait inside coarse stream | — | 2.71 |
| — ADC preparation (67 clusters) | ~2.9 (34.8% of CPU samples) | 0.41 |
| — row scoring + candidate push (26.6k rows) | ~1.7 | 1.50 |
| rerank (frontier select + range wave + exact) | 2.43 | 2.42 |
| outside scan (manifest/centroid rank/merge) | ~0.8 | ~0.8 |

Remaining latency is dominated by the coarse range wave (10.77 MB of fully-useful
coarse+ID bytes re-read per warm query in W1), the rerank range wave at the default 1 MiB
gap, and the strong-manifest round trip — all fetch-plan/policy territory (plan workstreams
B/C/D), not further exact CPU work.

## Correctness / quality evidence

- Bit-identical results in every accepted state: FNV `results_hash` equality across baseline
  and final builds on dev (top10), held-out offset-500 (top10), and top100 cells; identical
  recall to full float precision; identical GET/byte counts.
- `CARGO_INCREMENTAL=0 TEST_BACKEND=minio ZEPPELIN_LICENSE_TEST_BUCKET=zeppelin-license-test
  cargo test --lib`: **770 passed / 0 failed**.
- `TEST_BACKEND=minio cargo test --no-fail-fast --test rq_scan_tests --test warm_range_tests
  --test warm_parity_tests --test hydration_tests`: 12 + 14 + 15 + 26 passed, 0 failed.
- `TEST_BACKEND=minio cargo test --test get_count_bench`: 16 passed (pinned GET counts
  unchanged).
- `cargo clippy --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean;
  `git diff --check` clean.
- Recall gate: the scale-aware omitted-nprobe policy and `partition_vectors` are untouched,
  and all changes are proven bit-identical, so the pinned `ivf_recall_gate` values
  (recall@100 0.9688 / 0.9814) are unaffected by construction. The gate scores exact f32
  through the partition seam and does not execute the modified kernels; production-path
  recall was instead verified directly on real data (recall@10/@100 unchanged above).

## Limitations

- All numbers are loopback-MinIO W1 measurements on one host; they are not cloud-S3 latency
  claims. The S3-relevant statement is the removed CPU work and the unchanged fetch plan.
- The overlap slice (S3) was kept below its preregistered 5% gate — documented deviation.
- Temporary phase instrumentation and the harness `results_hash` live only in the scratch
  harness / this ledger; no permanent attribution surface was added to the repo.
- W3/hydrated and 1M/2M scale cells, and workstreams B/C/D, were not executed in this
  session; the plan's M0/M6 framework still applies to them.
