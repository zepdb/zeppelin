# Phase 07 — Truth-wave CPU levers

All three arms used the pinned 1,109-query f16 workload at K=1000 against
native MinIO. Read-plan concurrency was the production default of 8. The
baseline was sequential; both parallel arms used 16 scoped scoring workers.

Times are `p50 / p95 / p99` in milliseconds.

| Arm | Truth wave | Fetch | Decode | MaxSim | E2E |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline, sequential | 421.000 / 575.000 / 648.000 | 62.625 / 66.267 / 74.147 | 177.483 / 189.976 / 198.913 | 180.096 / 331.930 / 395.700 | 461.000 / 614.000 / 690.000 |
| Lever 1, parallel scoring | 99.000 / 116.000 / 128.000 | 61.861 / 70.746 / 79.538 | 17.027 / 22.579 / 25.484 | 18.448 / 32.500 / 38.291 | 140.000 / 159.000 / 173.000 |
| Levers 1+2, parallel + slice decode | 95.000 / 108.000 / 116.000 | 61.519 / 64.924 / 67.259 | 13.774 / 19.193 / 20.931 | 19.826 / 32.751 / 39.651 | 136.000 / 150.000 / 159.000 |

The baseline decode and MaxSim fields are sequential accumulated durations.
For both parallel arms they are the maximum accumulated duration of any one
worker: critical-worker wall contribution, not summed CPU time. The score
field used below is the complete scoring-stage wall time. Independently
computed percentiles do not add exactly.

## Per-lever attribution

Lever 1 is the baseline-to-parallel delta.

| p50 metric | Before → after | Saved | Reduction | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Score wall | 358.165 → 35.707 ms | 322.458 ms | 90.0% | 10.03x |
| Truth wave | 421.000 → 99.000 ms | 322.000 ms | 76.5% | 4.25x |
| E2E | 461.000 → 140.000 ms | 321.000 ms | 69.6% | 3.29x |

Lever 2 is the incremental parallel-to-parallel-plus-decode delta.

| p50 metric | Before → after | Saved | Reduction | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Decode | 17.027 → 13.774 ms | 3.253 ms | 19.1% | 1.24x |
| Score wall | 35.707 → 32.581 ms | 3.126 ms | 8.8% | 1.10x |
| Truth wave | 99.000 → 95.000 ms | 4.000 ms | 4.0% | 1.04x |
| E2E | 140.000 → 136.000 ms | 4.000 ms | 2.9% | 1.03x |

Cumulatively, score p50 fell 358.165 → 32.581 ms (10.99x), truth-wave
p50 fell 421 → 95 ms (4.43x), and E2E p50 fell 461 → 136 ms (3.39x).
Fetch and MaxSim were unchanged by Lever 2; their movement between runs is
not attributed to the decoder.

## Prediction check

- Lever 1's predicted score reduction from about 354 ms to about 50 ms held
  and was exceeded: the measured score p50 was 35.707 ms.
- Lever 2 improved critical-worker decode p50 by 3.253 ms, but the predicted
  5–10 ms decode range did not hold. The measured value was 13.774 ms. This
  stage still includes the unchanged SHA-256 and attribute decode/validation
  work in addition to matrix widening.

## Acceptance

| Arm | Queries | Recall | Min hits | Queries <8/10 | Queries <5/10 | Planned = observed GETs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 1,109 | 10,869/11,090 | 6 | 10 | 0 | yes (809,057) |
| Lever 1 | 1,109 | 10,869/11,090 | 6 | 10 | 0 | yes (809,057) |
| Levers 1+2 | 1,109 | 10,869/11,090 | 6 | 10 | 0 | yes (809,057) |

Recall is bit-stable in all arms. Every arm meets the pinned tail limits:
minimum 6/10, no more than 12 queries below 8/10, and zero below 5/10.
Every observed GET count matched the read plan.

Lever 2 keeps artifact bytes and SHA-256 policy unchanged. It validates f16
exponent bits once, converts the aligned little-endian `u16` scratch slice
through `half`, and reuses one u16/f32 scratch pair per scoped worker. The
scoring site no longer revalidates the decoded matrix.

The scalar-versus-slice decode bit test and the sequential-versus-parallel
score bit test passed. The complete embedding-artifact and matrix-block unit
suites passed, as did `cargo fmt --all -- --check` and production
`cargo clippy --lib -- -D warnings`. Test-target clippy also passed at
`-D warnings` with only `clippy::needless_range_loop` allowed because the
strict all-target attempt found two pre-existing instances in the explicitly
out-of-scope `phase9_routing_diagnostic.rs`; that file was not changed.
