# Phase 4 slice 9 fixed-stride f32 result

## Fixture and method

- Corpus: 20,000 deterministic synthetic rows plus one held-out query,
  256 dimensions, seed 42, and four 5,000-row WAL fragments with disjoint ID
  pools.
- Index: default `Scalar` quantization with SQ8 coarse payloads, `nlist = 8`,
  `nprobe = 8`, `top_k = 100`, a fixed `4 * fetch_k = 400` row frontier, and
  maximum grouping arity 3.
- Backend: native MinIO at `127.0.0.1:9000`.
- Rerank coalescing gap: the unchanged production default, 1 MiB.
- Control: an `all_rows = 1` filter matching the complete corpus disables the
  resident-row bypass while `oversample_factor = 1` keeps `fetch_k = 100`.
  Supporting manifest, bootstrap, bitmap, and attribute reads are excluded from
  the cluster-object census.
- Ground truth: exact Euclidean distance over all 20,000 in-memory corpus
  vectors, sorted by distance and then ID. Returned distances were separately
  recomputed from the original f32 vectors and compared bit-for-bit.

## Measured census

| path | GETs/q | header B/q | coarse B/q | ID B/q | rerank B/q | recall@100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| current v5 coarse path | 12 | 0 | 5,120,064 | 320,032 | 19,769,344 | 1.000000 |
| resident-row bypass | 14 | 0 | 0 | 320,032 | 19,574,784 | 0.940000 |

The bypass removed every grouped header and coarse-region read. It fetched one
whole ID block for each of the eight clusters containing frontier rows, then
used manifest row coordinates to issue only coalesced fixed-stride f32 ranges.
Metadata or payload disagreement after dispatch remains an error; it does not
fall back to coarse decoding.

## Interpretation

Winner rows did **not** concentrate as projected in this fixture. The 400-row
frontier touched all eight probed clusters, so every ID block was read. The
useful f32 floor was:

`400 * 256 * 4 = 409,600 bytes/query`.

The measured rerank payload was 19,574,784 bytes, for 47.79x
physical/logical f32 amplification. The 1 MiB policy fused sparse row ranges
into broad spans. Total measured cluster-object bytes fell from 25,209,440 to
19,894,816 (1.267136x, or 21.082%), while GETs rose from 12 to 14.

The approximately 0.2-0.5 MB/query projection did **not** survive this fixture:
the measured bypass cluster payload was 19.895 MB/query. Exact-distance
provenance survived, but approximate selection recall moved from 1.00 to 0.94.

This fixture does not establish production-scale behavior. It is a 20k-row,
full-probe (`8/8`) corpus at 256 dimensions; production arithmetic is roughly
63/334 probes at one million rows and 768 dimensions. The result proves the
production query can bypass headers/coarse payloads and exposes the small-shape
winner dispersion and coalescing cost. It does not predict S3 latency, request
cost, production winner concentration, or either pinned WikiDPR recall result.

## Falsification verdicts

- **F1 survived.** Fixed stride plus manifest row layouts removed coarse and
  header reads as offset prerequisites.
- **F2 is unresolved at the authority gate.** The small sentinel recorded
  recall@100 of 0.94; it cannot waive or replace the scheduled
  `wikidpr1m`/`wikidpr2m` gate.
- **F3 failed on this fixture.** Winner dispersion made bypass GETs higher
  (14 vs 12), not lower.
- **F4 failed on this fixture.** Bytes fell only 1.267136x, below 2x, and
  result quality also moved.

Per the slice's prewritten stop rule, these measurements do not justify
shipping the bypass without Anup deciding whether a separately measured
frontier/coalescing experiment is warranted. No frontier, `nprobe`, default, or
perf-contract value was changed here.

## Reverted, and why the F3/F4 verdicts above are not final

The implementation these numbers describe (`859e738`) was reverted. Two
reasons, in order of weight.

**1. It shipped on by default before its authority gate.** The bypass
dispatched for every unfiltered SQ8 or two-bit query on a `ZBP5` segment
with a v4 sketch. There was no config knob. F2 above is unresolved and the
pinned `wikidpr1m`/`wikidpr2m` gate is Anup's to schedule, so a selector that
moved recall from 1.00 to 0.94 on this fixture was reaching every production
query before anything authoritative had passed. Slice plan §7 and §10.6 both
forbid exactly that, and §9's F4 rule says outright: do not ship the bypass.

**2. It regressed a pinned GET contract, unreported.** `tests/get_count_bench.rs`
passes 14/14 at `35d9f4d` and fails 4 of 14 at `859e738`: cluster GETs 4 -> 6,
total 10 -> 12. That is the whole of slice 9.2's measured GET reduction handed
back.

Both failures share one root cause, and it is fixable:

**`load_resident_frontier_candidates` issued one GET per winning cluster's ID
block instead of one span per object.** In a `ZBP5` object the ID blocks are
contiguous (`coarse0..n | ids0..n | vectors0..n`), which is exactly why slice
9.2's coarse path reads them as a single `coarse..ids` span per object. The
bypass discarded that. The arithmetic is direct in `get_count_bench`: 4
clusters in 2 objects became 4 ID GETs plus 2 coalesced rerank GETs = 6, where
per-object coalescing gives 2 + 2 = 4 — matching the coarse path's GET count
while reading strictly fewer bytes, since no coarse codes are fetched at all.

So **F3's verdict is an artifact of that defect, not a property of the design.**
This fixture reported "one whole ID block for each of the eight clusters
containing frontier rows" — 8 ID GETs at grouping arity 3, where per-object
coalescing needs about 3. Bypass 14 vs coarse 12 would have been roughly 9 vs
12, and F3 would have survived. F4's 1.267x is understated for the same
reason.

`tests/fixed_stride_f32_tests.rs` asserted `range == ids` for every non-vector
range, so it pinned the un-coalesced shape rather than catching it. A redo must
change that assertion.

### What a redo owes

1. Coalesce ID-block reads per object, the way `fetch_object_row_layout_range`
   already does for the coarse path.
2. Re-measure the table above. Only then are F3 and F4 answerable.
3. Keep `get_count_bench` green, or state the new profile and why.
4. Put the dispatch behind a config knob that defaults off. §10.6 contemplates
   a default to flip; there was none to flip.
5. Flip it only after the two-dataset recall gate, per §7's recall contract.
