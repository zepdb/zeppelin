# Phase 09 — parallel flat scan and INT8-G32 truth-wave recomposition

**Status:** PASS (2026-08-05)  
**Machine:** Apple M3 Max, 12 performance + 4 efficiency cores, loopback MinIO  
**Branch:** `ll-09-scan-parallel`  
**Accepted implementation:** `4f5ac17` (parallel scan)  
**Measurement harness:** `b29de38` (dtype and candidate-K arms)

## Result

The original production-default f16 cell was **109 ms p50**. Parallelizing the
resident flat-SQ8 scan reduced its p50 from **42 to 4 ms** (10.5x) and reduced
end-to-end latency to **72 ms** without changing a score bit, candidate, result,
or physical truth read.

Recomposing that scan with the already-qualified INT8-G32 truth codec and a
wider frontier produced a quality-preserving cell at **53 ms p50**. The
speed-biased K=1000 cell reached **48 ms p50** with the previously known 0.54
percentage-point absolute recall cost.

All numbers below are full 1,109-query runs. Latencies are milliseconds.
Planned/logical bytes are decimal MB per query at p50.

| Cell | K | Gap | p50 / p95 / p99 | Scan | Truth | GETs | Logical / planned MB | Hits / 11,090 | Recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f16 control, sequential scan | 1000 | 64 KiB | 109 / 120 / 128 | 42 | 67 | 730 | 54.2 / 58.1 | 10,869 | 0.980072 |
| f16, parallel scan | 1000 | 64 KiB | 72 / 88 / 104 | 4 | 68 | 730 | 54.2 / 58.1 | 10,869 | 0.980072 |
| f16, parallel scan, high-density latency profile | 1000 | 896 KiB | 52 / 64 / 69 | 4 | 47 | 211 | 54.2 / 188.3 | 10,869 | 0.980072 |
| INT8-G32, speed profile | 1000 | 512 KiB | **48 / 61 / 68** | 4 | 43 | 199 | 28.8 / 103.3 | 10,809 | 0.974662 |
| INT8-G32, middle profile | 1100 | 512 KiB | **51 / 66 / 76** | 4 | 46 | 199 | 31.8 / 108.8 | 10,844 | 0.977818 |
| INT8-G32, quality profile | 1200 | 512 KiB | 54 / 72 / 80 | 4 | 49 | 198 | 34.8 / 113.7 | 10,876 | **0.980703** |
| INT8-G32, quality + latency profile | 1200 | 768 KiB | **53 / 70 / 79** | 4 | 48 | 142 | 34.8 / 127.2 | 10,876 | **0.980703** |

Relative to the paired f16 control, the final quality-preserving profile is
**2.06x faster** and reduces p50 by **51.4%**. It returns seven more gold
memberships than f16 K=1000. The speed profile is **2.27x faster** and reduces
p50 by **56.0%**, at the known 60-membership / 0.005410 absolute recall cost.

## Correctness and quality

- The parallel scan uses the unchanged scalar `asymmetric_dot_product` for
  every row. A constructed-tie unit test compares every score bit and the full
  `(score, row-index)` order against the sequential oracle.
- Every f16 replay query returned the same ordered top-10 IDs as the control.
- All full runs had observed GETs exactly equal to planned GETs.
- f16 pins stayed exact: K700/K1000/K1500 memberships
  `10,708 / 10,869 / 10,993`; tail minimum 6/10; 10 queries below 8/10; none
  below 5/10.
- INT8-G32 K=1000 reproduced its prior control exactly: 10,809 memberships,
  minimum 6/10, 10 below 8/10, none below 5/10.
- INT8-G32 K=1200 improved tails: 10,876 memberships, minimum 6/10, 5 below
  8/10, none below 5/10.

The lost `/private/tmp` query tensors were regenerated from the pinned model,
dataset, and lab code. Document tensors reproduced byte-for-byte. Query tensor
and query-FDE bytes drifted, but their 1,109 IDs and 23,540-row tokenization
were exact; a full control reproduced every historical recall/tail pin and
ordered result. The new query digests are isolated in `442026e`.

## Gap and concurrency findings

The old fixed 256 KiB knee was measured before streamed, parallel truth
scoring. After recomposition, read and scoring complete at nearly the same
wall time. On this high-density replay (K=1000 touches 19% of 5,183 rows),
larger gap budgets reduce request-stack work enough to win despite byte
amplification. For INT8 K=1000, the 400-query sweep was:

| Gap | p50 | GETs | Planned MB |
| ---: | ---: | ---: | ---: |
| 64 KiB | 65 (full) | 622 | 35.8 |
| 128 KiB | 57 | 475 | 49.0 |
| 256 KiB | 53 | 322 | 73.7 |
| 384 KiB | 49 | 246 | 90.5 |
| 512 KiB | 47 | 199 | 103.2 |
| 896 KiB | 47 | 126 | 123.9 |

C8/C12/C16/C24/C32 sensitivity did not beat C16 robustly. The temporary score
worker override also showed no win and was removed.

**Do not make the high gap a universal default from this cell.** Phase 05's
50k-unit, ~2%-density shape check showed that even 256 KiB saved only 7.4% of
GETs while adding 83% bytes. Keep the current 64 KiB general default unless a
workload-specific latency profile is explicitly selected or an adaptive
request/byte planner is separately qualified.

## Operational choice

INT8-G32 is already a qualified, immutable artifact format and this benchmark
uses its production encoder, ranged reader, decoder, checksum validation, and
MaxSim scorer. Selecting it remains an operator decision:

- `text_matrix_dtype = "int8_g32"` is captured into the namespace epoch;
- the qualification stamp remains mandatory and fail-closed;
- changing an existing f16 namespace is rejected by the one-way dtype fence;
  create/rebuild under the selected epoch rather than rewriting artifacts;
- select `segment.candidate_k = 1200` for f16-equivalent-or-better quality;
- on this dense latency profile, select
  `segment.read_gap_budget_bytes = 786432` and keep read concurrency 16.

If the product accepts the explicitly approved 0.54 percentage-point recall
trade, K=1000 / 512 KiB is the lowest measured full-run point at 48 ms. If
quality must not regress, K=1200 / 768 KiB is the accepted 53 ms point.

## Commands

Representative full run:

```bash
until mkdir /private/tmp/zeppelin-bench-lock 2>/dev/null; do sleep 30; done
CARGO_INCREMENTAL=0 TEST_BACKEND=minio \
MMLI_REAL_MATRIX_DIR=/private/tmp/mmli2-phase2-work \
MMLI_FLAT_BENCH_OUTPUT=tasks/LateLatency/results/phase09-int8-k1200-gap768-final.json \
MMLI_FLAT_BENCH_DTYPE=int8_g32 \
MMLI_FLAT_BENCH_CANDIDATE_K=1200 \
MMLI_FLAT_BENCH_CONCURRENCY=16 \
MMLI_FLAT_BENCH_GAP_BUDGET=786432 \
cargo test --release --lib \
  index::late_interaction::phase9_flat_sq8_bench::phase9_flat_sq8_benchmark \
  -- --exact --ignored --nocapture --test-threads=1
rmdir /private/tmp/zeppelin-bench-lock
```

Focused validation:

```bash
CARGO_INCREMENTAL=0 cargo test --release --lib flat_candidate
CARGO_INCREMENTAL=0 cargo clippy --release --lib -- -D warnings
cargo fmt --all -- --check
```
