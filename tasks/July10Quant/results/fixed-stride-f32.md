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
