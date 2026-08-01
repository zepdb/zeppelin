# Phase 06 — Productize defaults

Status: **PASS with the phase-authorized visual-lane skip**. The binding text
replay passed every recall and tail tripwire with the streamed production truth
wave. All four pinned visual tensor files were absent, so the visual replay was
not run and the tensors were not regenerated.

Phase step 4 defines missing pinned visual tensors as an explicit skip. No
replay failed, so the chosen block's failure rollback to the collect path was
not triggered; no visual non-regression claim is made.

The branch started from `main` at `fa0c18f`.

## Production defaults

| Surface | Old | New | Decision basis |
| --- | ---: | ---: | --- |
| `mmli.segment.read_max_concurrency` | 8 | **16** | Phase 02 knee: SciFact fetch p50 64.3→46.8 ms from 8→16; the 50k heavy-tail corpus transferred at fetch −19% with an identical plan. |
| `mmli.segment.read_gap_budget_bytes` | 65,536 B | **65,536 B (unchanged)** | The 256 KiB SciFact knee paid +64% bytes for −37% GETs, but out of regime removed only 7.4% of GETs while paying +83% planned bytes. |
| `mmli.segment.read_max_request_bytes` | 8,388,608 B | **8,388,608 B (unchanged)** | It never bound; the 16 MiB arm produced byte-identical plans. |
| Late truth wave | Collect barrier | **Streamed reads + parallel scoring** | The text acceptance replay passed exactly. The phase's conditional visual replay was skipped because its pinned tensors were absent. |

`src/config.rs` documents each selected or deliberately unchanged value, its
measured basis, and the effect of moving it in either direction.

## Full-harness acceptance replays

The old values come from `tasks/MMLI-2/results/segments.md`. Milliseconds:

| Lane | Old p50 / p95 | New p50 / p95 | Delta p50 / p95 | Result |
| --- | ---: | ---: | ---: | --- |
| Text | 596 / 883 | **241 / 254** | **−355 (−59.6%) / −629 (−71.2%)** | PASS |
| Visual | 694 / 919 | — / — | — / — | **SKIPPED — pinned tensors absent; not regenerated** |

The text run completed in 321.77 seconds. It reported 58,350,461 mean planned
truth bytes, 730 mean planned truth requests, and 5,535 ms compaction time.

The visual presence check found all four required files absent:
`visual-documents.f16`, `visual-documents.json`, `visual-queries.f16`, and
`visual-queries.json`. No visual latency or non-regression claim is made.

## Gate results

| Lane | Gate | Required | Observed | Result |
| --- | --- | ---: | ---: | --- |
| Text | Recall | exactly 10,869/11,090 = 0.980072 | **10,869/11,090 = 0.980072** | PASS |
| Text | Minimum hits | at least 6/10 | **6/10** | PASS |
| Text | Queries below 8/10 | at most 12 | **10** | PASS |
| Text | Queries below 5/10 | 0 | **0** | PASS |
| Text | Candidate-wave per-query reads | 0 | **0, asserted for every query** | PASS |
| Visual | Recall and tails | recall ≥0.90; 4,817 expected hits; at most 62 below 8/10 and 7 below 5/10 | Not run: pinned tensors absent | SKIP per phase step 4 |

## Validation

| Gate | Result |
| --- | --- |
| `CARGO_INCREMENTAL=0 cargo test --lib` | **764 passed, 0 failed, 4 ignored** |
| Locked release text replay | **1 passed**, exact recall/tails above |
| Locked release visual replay | **Skipped**, all pinned visual tensors absent |
| MinIO `enrichment_tests` | **15 passed, 0 failed** |
| MinIO `late_interaction_query_tests` | **13 passed, 0 failed** |
| MinIO `late_segment_tests` | **16 passed, 0 failed, 2 known ignored** |
| `cargo fmt --all -- --check` | Clean |
| `cargo clippy --lib -- -D warnings` | Clean |
| `cargo clippy --all-targets -- -D warnings -A clippy::needless-range-loop` | Clean; allowance is only for the two protected baseline diagnostics |
| `git diff --check` | Clean |
| `git diff main -- src/index/ivf_flat/` | Empty |
| Dense `read_plan.rs` `DEFAULT_*` constants | Unchanged at 64 KiB / 8 MiB / 8 |
| `phase9_routing_diagnostic.rs` | Untouched |

The release replay acquired `/private/tmp/zeppelin-bench-lock` before its
release build and timed run, then released it after the test exited.

## Diff summary

- `src/config.rs` changes the late-segment concurrency default and its pinned
  test from 8 to 16, while adding the required measured rustdoc for all three
  selected defaults.
- `src/index/late_interaction/segment_search.rs` makes streamed read
  completions with scoped parallel scoring the sole late truth-wave production
  path. Benchmark-only timing fields remain test-only.
- `src/index/late_interaction/phase9_flat_sq8_bench.rs` removes
  `MMLI_FLAT_BENCH_PIPELINE` and the collect-barrier arm; benchmark reports now
  always record the productionized streamed path.
- `src/storage/read_plan.rs` promotes the existing physical-to-logical inverse
  map from test-only to production so streamed completions can restore exact
  caller-order slices. The collect executor remains for non-truth callers.
- No dependency changed. Dense IVF files, dense read-plan defaults,
  `phase9_routing_diagnostic.rs`, `stash@{0}`, and `.pi-subagents/` were
  untouched.
