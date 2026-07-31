# INT8 document-matrix ranking investigation

Date: 2026-07-30

## Question

For text and visual retrieval, compare f32 exact rankings with the same model
outputs represented as:

- f16;
- global-scale INT8;
- affine per-row INT8;
- affine per-row INT8 plus whole-row L2 renormalization;
- symmetric per-row INT8 plus whole-row L2 renormalization;
- groupwise-32 symmetric INT8 plus whole-row L2 renormalization;
- groupwise-16 symmetric INT8 plus whole-row L2 renormalization.

Queries remain f16 for every INT8 document candidate.

The decision evidence is product-visible ranking behavior:

- exact equality of the top-10 result set per query;
- exact equality of the ordered top-10 list per query;
- same document at each rank from 1 through 10;
- aggregate f32 top-10 recovery;
- payload bytes and savings versus f16.

Raw MaxSim numerical error is not a decision gate for this follow-up.

## Previously measured evidence

These values landed in `results/lab.md` with commit `1c888ea`.

| Lane | Representation | f32 top-1 same | f32 top-10 recovered |
| --- | --- | ---: | ---: |
| Text | Global-scale INT8 | 0.965735 | 0.921641 |
| Text | Affine per-row INT8 | 0.988278 | 0.980613 |
| Visual | f16 | 0.996248 | 1.000000 |
| Visual | Global-scale INT8 | 0.981238 | 0.981801 |
| Visual | Affine per-row INT8 | 0.992495 | 0.995872 |

The earlier report did not retain text f32-to-f16 ranking measurements,
per-query exact set equality, complete ordered-list equality, or
position-by-position agreement. The raw tensors were deleted after that
gate, so the missing values cannot be recovered from the committed
artifacts.

## Interpretation before the new run

- Visual affine per-row INT8 already retains 5,308 of 5,330 f32 top-10
  memberships. That is strong ranking evidence despite missing the former
  0.999 threshold.
- Text affine per-row INT8 retains 10,875 of 11,090 memberships. Whether the
  215 changes matter depends on which queries and ranks changed.
- Global text INT8 is the clearly weak baseline.
- The new audit must distinguish result-set changes from harmless reordering
  inside the same top-10 set.

## New text result

Measured on 1,109 queries and 5,183 documents from the same encoder pass.
Percentages below compare each representation's exact top-10 against f32.

| Representation | Exact set/query | Exact order/query | Top-10 recovered | Same top-1 | Missed memberships | KiB/doc | Saving vs f16 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f16 | 99.098% | 95.852% | 99.910% | 99.910% | 10 | 57.48 | — |
| Global INT8 | 39.134% | 5.140% | 92.164% | 96.573% | 869 | 28.74 | 50.000% |
| Affine per-row INT8 | 80.884% | 40.938% | 98.061% | 98.828% | 215 | 30.53 | 46.875% |
| Affine per-row + renorm | 93.508% | 71.235% | 99.351% | 99.729% | 72 | 30.53 | 46.875% |
| Symmetric per-row + renorm | 93.417% | 68.981% | 99.342% | 99.549% | 73 | 29.19 | 49.219% |
| Groupwise-32 symmetric + renorm | 93.688% | 73.850% | 99.369% | 99.820% | 70 | 30.53 | 46.875% |
| **Groupwise-16 symmetric + renorm** | **94.139%** | **74.482%** | **99.414%** | **99.820%** | **65** | 32.33 | 43.750% |

Same-document agreement by rank for the f16 reference and the two visual
finalists:

| Representation | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 | R10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f16 | 99.910% | 99.910% | 99.729% | 99.459% | 99.369% | 99.279% | 98.918% | 98.828% | 98.738% | 98.287% |
| Groupwise-32 + renorm | 99.820% | 98.738% | 97.024% | 96.032% | 95.311% | 94.500% | 93.417% | 91.885% | 89.901% | 89.089% |
| Groupwise-16 + renorm | 99.820% | 98.738% | 96.754% | 96.032% | 96.123% | 94.319% | 92.426% | 91.794% | 91.975% | 90.442% |

The previous global and affine results reproduced exactly. Whole-row
renormalization is the important move: it cuts affine text misses from 215
to 72 without changing its payload size. Groupwise-16 is the best measured
text ranking, while groupwise-32 is the second-best quality/cost point; both
advance to visual.

Decode cost is recorded as exact analytic operation counts, separate from
MaxSim. Groupwise-32 reads four f16 scales per 128-coordinate row;
groupwise-16 reads eight. Both perform one scale multiplication per
coordinate, then one norm accumulation and one normalization division per
coordinate plus one square root per row. These counts exclude MaxSim scoring
and are not synthetic wall-clock timings. They describe the measured lab
path. The canonical production `int8_sym_v1` format folds normalization into
the stored scales, removing those norm operations from reads; its exact
stored-byte arithmetic still requires the Phase-6 qualification run.

## New visual result

Measured on 533 queries and 2,000 pages from one powered batch-8 encoder
pass. Percentages compare each representation's exact top-10 against f32.

| Representation | Exact set/query | Exact order/query | Top-10 recovered | Same top-1 | Missed memberships | KiB/page | Saving vs f16 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f16 | 100.000% | 98.874% | 100.000% | 99.625% | 0 | 254.24 | — |
| Global INT8 | 82.176% | 37.523% | 98.180% | 98.124% | 97 | 127.12 | 50.000% |
| Affine per-row INT8 | 95.872% | 78.049% | 99.587% | **99.250%** | 22 | 135.07 | 46.875% |
| Groupwise-32 symmetric + renorm | **97.373%** | 80.675% | **99.737%** | 98.874% | **14** | 135.07 | 46.875% |
| Groupwise-16 symmetric + renorm | **97.373%** | **82.927%** | **99.737%** | 98.687% | **14** | 143.01 | 43.750% |

Same-document agreement by rank:

| Representation | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 | R10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| f16 | 99.625% | 99.437% | 99.812% | 100.000% | 100.000% | 99.625% | 99.625% | 99.812% | 99.812% | 100.000% |
| Affine per-row | 99.250% | 97.561% | 97.373% | 94.934% | 93.996% | 95.872% | 93.246% | 91.557% | 94.559% | 94.559% |
| Groupwise-32 + renorm | 98.874% | 96.811% | 96.248% | 96.811% | 96.248% | 94.559% | 95.310% | 95.310% | 95.310% | 95.685% |
| Groupwise-16 + renorm | 98.687% | 97.373% | 97.373% | 96.998% | 96.435% | 95.497% | 95.310% | 95.310% | 96.998% | 96.435% |

Both groupwise variants improve visual membership recovery and complete-set
equality over affine per-row. Groupwise-16 produces the most exactly ordered
queries, but groupwise-32 stores eight fewer metadata bytes per row and has
one fewer top-1 flip. Every representation retained the f32 top result
somewhere in its top 10 for every visual query.

## Format recommendation

If Phase 6 uses one INT8 format across both lanes, groupwise-32 symmetric
with whole-row renormalization is the balanced choice:

- it keeps the same 46.875% payload saving as affine per-row;
- on visual it cuts missed top-10 memberships from 22 to 14 and raises
  exact-set queries from 511 to 519, at the cost of two additional top-1
  flips;
- on text it reaches 99.369% aggregate recovery, 93.688% exact-set queries,
  73.850% exact-order queries, and 99.820% same top-1;
- groupwise-16 buys five more text memberships and 12 more exactly ordered
  visual queries, but saves only 43.750% and adds one visual top-1 flip.

The Phase-2a production decision is dual-format: f16 remains the default,
while `int8_sym_v1 { group_size }` is fail-closed until its production-path
qualification stamp exists. Groupwise-32 is the recommended first
qualification profile. The current qualification draft requires same-top-1
≥ 99.5%; neither visual groupwise cell meets that in the lab (98.874% for
groupwise-32 and 98.687% for groupwise-16). The production folded-scale path
must therefore be measured, and the final per-lane thresholds remain
operator-owned; this report does not silently lower them.

## Execution notes

- Phase 6 product work remains stashed as
  `WIP MMLI-2 Phase 6 pending matrix format decision`.
- The ranking audit is lab-only. It does not modify Phase 6, Phase 7, or
  Phase 9 product formats.
- The audit code now covers all four approved experiment groups: existing
  baselines, affine per-row plus renormalization, symmetric per-row plus
  renormalization, and groupwise-32/groupwise-16 symmetric plus
  renormalization.
- The Rust audit tests currently pass (`4 passed, 0 failed`), including
  ranking-accounting, payload-accounting, deterministic quantization, and
  fail-loud invalid-row coverage.
- The text lane runs all fixed candidates first. Only the best one or two
  new candidates proceed to the visual lane.
- The first visual attempt used batch 1 and was stopped at 1,140/2,000 pages
  after throughput collapsed from roughly 30 pages/minute to about 1.25.
  It produced no tensor or result artifact. The rerun uses the previously
  measured batch-8 path (~2.11 wall-seconds/page, ~44.8 GiB peak RSS), which
  fits the 128 GiB host.
- The host was then found to be running on battery under heavy load. After it
  was connected to power and unnecessary processes were closed, the
  throttled batch-8 process was restarted at its first 80-page checkpoint so
  the final measurement runs entirely in the stable power state.
- Full raw measurements are in `int8-ranking.json`; this file summarizes the
  measured values and preserves the pending matrix-format decision.
- The 18 temporary text/visual f16/f32 tensor, sidecar, and job files were
  deleted after both result lanes became durable, as required by the phase.

## Run status

- Text measurement: complete and durable in `int8-ranking.json`.
- Visual measurement: complete and durable in `int8-ranking.json`.
- Matrix-format decision: dual-format with f16 default and fail-closed
  `int8_sym_v1`; Phase 6 stays stashed pending operator direction on the
  visual same-top-1 qualification result.

An independent pre-result review found the ranking comparisons and
quantization math correct. Its three workflow findings are fixed: visual can
score only the selected finalists, cleanup happens only after atomic result
persistence, and retrieval-time decode work is explicitly recorded.
