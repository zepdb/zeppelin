# Phase 4 slice 9 fixed-stride f32 result

Supersedes the `859e738` measurement, which was taken against a defective
implementation and a fixture that could not test what it claimed to. See
"What the first attempt got wrong" at the bottom.

## Verdict

**Do not ship the bypass.** F4's prewritten rule is met: physical bytes fell
1.45x, below its 2x threshold, and result quality moved materially at the same
time. The bypass is committed but **off by default**
(`[query] resident_row_bypass`, default `false`) so the evidence stays
reproducible from `tests/fixed_stride_f32_tests.rs`.

## Fixture and method

- Corpus: 20,000 deterministic synthetic rows plus one held-out query, 256
  dimensions, seed 42, four 5,000-row WAL fragments with disjoint ID pools.
- Backend: native MinIO at `127.0.0.1:9000`.
- Rerank coalescing gap: the unchanged production default, 1 MiB. Slice 6.3
  settled that the default stays there, so no other gap was measured.
- Default `Scalar` quantization, SQ8 coarse payloads, resident v4 two-bit
  sketch, maximum grouping arity 3, unchanged `4 * fetch_k` frontier.
- **Control and treatment are the same unfiltered query**, differing only in
  `resident_row_bypass`. The previous run used an `all_rows = 1` filter to
  force the coarse path, which also changed the filter/oversample path and
  confounded the comparison.
- Ground truth: exact Euclidean distance over all 20,000 in-memory vectors,
  sorted by distance then ID. Returned distances were separately recomputed
  from the original f32 and compared bit-for-bit.

Two shapes, because they answer different questions.

| shape | clusters | nprobe | top_k | frontier | why |
| --- | ---: | ---: | ---: | ---: | --- |
| `QUALITY` | 8 | 8 | 100 | 400 | recall@100 is only meaningful at `top_k = 100` |
| `PRODUCTION_RATIO` | 64 | 12 | 10 | 40 | 18.75% of clusters probed, against production's 63/334 |

`QUALITY` **cannot** test request count: a 400-row frontier over 8 clusters
touches all 8 by construction. Winner concentration is a property of the
probe/frontier *ratios*, not of absolute corpus size, so `PRODUCTION_RATIO`
reproduces those ratios inside the slice's measurement budget.

## Measured census — `PRODUCTION_RATIO`, 38 objects total

| path | GETs/q | objects touched | header B/q | coarse B/q | ID B/q | rerank B/q | recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v5 coarse path | 20 | 10 | 0 | 4,148,448 | 259,376 | 8,355,840 | 1.000000 |
| resident-row bypass | 18 | 9 | 0 | 0 | 184,560 | 8,625,152 | 0.800000 |

Physical cluster-object bytes: 12,763,664 -> 8,809,712, a **1.45x** reduction.
Useful f32 floor is `40 * 256 * 4 = 40,960` bytes, so the bypass still ran at
210.6x rerank amplification: the 1 MiB gap fuses sparse rows into broad spans,
and that term now dominates because the coarse term is gone.

`QUALITY` recall@100: coarse **1.000000**, bypass **0.940000**.

## Interpretation

**Winner dispersion is near-total, even at production ratios.** A 40-row
frontier over a 3,750-row probe set still landed in 9 of the 10 objects the
coarse path touched. The projection in §3 assumed the winning rows would
concentrate into a handful of objects; at these ratios they do not. That is the
central empirical result, and it is what caps the request-count benefit at
20 -> 18 GETs.

**The resident two-bit sketch is a materially weaker selector than SQ8 coarse
codes.** Both paths retain the same `4 * fetch_k` frontier from the same probe
set, and both recompute exact f32 distances, so the gap is purely selection
quality: 1.00 -> 0.80 at `top_k = 10`, 1.00 -> 0.94 at `top_k = 100`. Missing 2
of 10 true neighbours is not a rounding difference.

**The byte win is real but lands in the wrong currency.** Slice 6.3 established
that on S3 in-region bytes are free while GETs cost $0.40/M. A 1.45x byte
reduction bought at 0.9x the requests is worth close to nothing there, and the
recall cost is not free.

This fixture does not establish production-scale behaviour. It is 20k rows at
256 dimensions; production is 1M rows at 768. It does establish the *shape* of
the trade at production probe/frontier ratios, and that shape is unfavourable.

## Falsification verdicts

- **F1 survived.** Fixed stride plus manifest row layouts removed coarse and
  header reads as offset prerequisites. The bypass fetched zero header bytes
  and zero coarse bytes while returning exact f32 distances.
- **F2 failed.** Recall moved 1.00 -> 0.80 at `top_k = 10` and 1.00 -> 0.94 at
  `top_k = 100` against an identical query on the identical corpus. Per §7's
  recall contract this is recorded and stopped here: no frontier, `nprobe`, or
  default was touched in response. The pinned `wikidpr1m`/`wikidpr2m` gate was
  not run, because a 20-point drop at production-like `top_k` does not warrant
  spending it.
- **F3 marginally survived.** GETs/query fell 20 -> 18 (1.11x) and objects
  touched 10 -> 9. The direction is right and the sign is no longer an artifact
  of per-cluster ID fetching, but the margin is small enough that dispersion,
  not addressing, is the binding constraint.
- **F4 failed.** Physical bytes/query fell 1.45x, below the 2x threshold, and
  result quality moved at the same time. §9's rule for a falsified F4 is
  explicit: the extra format and selector complexity is not justified; do not
  ship the bypass.

## What the first attempt got wrong

`859e738` recorded F3 failed at 14 vs 12 GETs and F4 failed at 1.27x. Three
defects, all fixed here:

1. **One GET per winning cluster's ID block instead of one span per object.**
   A `ZBP5` object stores ID blocks contiguously
   (`coarse0..n | ids0..n | vectors0..n`), and slice 9.2's
   `fetch_object_row_layout_range` already reads them as a single span. The
   bypass discarded that, costing 8 GETs where 3 would do. Its own test
   asserted `range == ids` per block, pinning the defect instead of catching
   it.

2. **The frontier was scored over the bare centroid probe set while the coarse
   path scans whole grouped objects.** The coarse path therefore chose from a
   strictly larger candidate pool — up to 3x more rows at arity 3 — so the
   recall comparison was not like-for-like.
   `ResidentSketch::frontier_rows` now covers the difference in a pass that
   scores no row twice.

3. **The fixture could not test F3**, as described above, and used a filtered
   control that confounded the comparison.

With all three fixed the verdict changed in both directions: F3 went from
failed to marginally survived, and F4's byte ratio improved from 1.27x to 1.45x
but still misses the threshold. The conclusion is unchanged, and is now
supported by a measurement that means what it says.

## If this is picked up again

The binding constraint is selection quality, not addressing. Ideas in rough
order of expected value:

1. A wider sketch (more bits per component) would attack F2 directly. §7 forbids
   changing the frontier inside 9.3; changing the sketch is a separate slice
   with its own recall gate.
2. Winner dispersion may concentrate at real production scale — 63 of 334
   clusters is a much sparser probe than 12 of 64. Testing that needs a corpus
   this plan's budget does not allow, so it is a claim, not a result.
3. The 210x rerank amplification is the dominant remaining byte term and is a
   coalescing question, which Slice 6.3 has already answered for S3.
