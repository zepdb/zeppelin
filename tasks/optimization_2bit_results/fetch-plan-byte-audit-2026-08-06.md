# Warm dense-query fetch-plan byte audit — results ledger

**Executed:** 2026-08-06
**Goal brief:** reduce wasted S3 bytes per warm dense query at equal-or-fewer
GETs with bit-identical results (FNV `results_hash` parity), confirmed by
closed-loop QPS at the ~410 MB/s loopback-MinIO wall.
**Base commit:** `8d44717` (tree clean; no product change was accepted — see
Findings).
**Host:** Apple M3 Max, macOS, release builds, `CARGO_INCREMENTAL=0`.
**Cell:** dbpedia100k (100,000 x 1,536 cosine), one compacted TwoBit/ZBP5
segment, 256 clusters in 137 grouped objects (mean arity 1.87, max 3), omitted
nprobe resolving to 48, `top_k = 10`, strong consistency, no filter, direct
`execute_query`, first (bootstrap-cold) query excluded.
**Harness:** `/tmp/zeppelin-default-runner` (`devbench_eval`), reused namespace
`ragbench-dbpedia100k-015f3f02-345d-4a7c-9fbc-96ef93d7526c` served by a second
native MinIO over `/tmp/zeppelin-profile-minio` at `127.0.0.1:9100`. Run JSON
under `/tmp/opt_bytes_*`, `/tmp/opt_shadow_*`, `/tmp/opt_holdout_*`; shadow
logs `/tmp/opt_shadow_dev.log`.

## Headline

**No byte-reduction slice was accepted, and that is the measured result, not a
failure to look.** Both candidate levers named by the brief are disproven by
direct measurement on the canonical cell:

1. The "non-probe grouped siblings decoded-but-unscored" byte pool is **empty**
   in the current default path. Every fetched cluster is scored; coarse-wave
   slack is 0 bytes.
2. The executed rerank fetch plan is **exactly byte-minimal for its GET
   count** on 200/200 warm queries (0 excess bytes vs the per-query optimum).
   Every plan with fewer bytes provably requires more GETs, which the brief
   forbids and the S2 gap-sweep already rejected as a knob change.

Under the constraint set {bit-identical results, GETs/query not increased,
default knobs frozen, no partitioning/probe-policy change, per-object
contiguous S3 ranges}, warm dense-query bytes are already at their
information-theoretic minimum: **16.04 MB/query steady-state** (25.6 QPS at
the 410 MB/s wall). Byte reductions require levers the brief excludes
(retrieval-policy change = workstream C with the recall gate; cross-query
reuse = workstream D / hydration; deployment gap-profile knob = recorded
recommendation only).

## Baseline reproduction (environment integrity)

| Cell | results_hash | GETs/q | bytes/q | vs prior ledger |
| --- | --- | --- | --- | --- |
| dev 0-200, top10 (n=201) | `b4e445426cd1af80` | 37.68 | 16.256 MB | identical hash, GETs, bytes |
| held-out 500-699, top10 (n=200) | `924e5ce98cf329fd` | 37.82 | 15.97 MB | identical hash |

recall@10 = 0.9885572 (dev), unchanged to full precision. The 16.256 MB
headline includes the one-time 40.77 MB bootstrap read amortized over 201
queries (202.9 KB/q); steady-state warm traffic is **16.036 MB/query**.

A top100 sentinel run on this build measured 59.15 GETs / 42.78 MB per query
(hash `8a25b4004f267c1b`; not comparable to the prior ledger's top100 hash
because the harness's retrieval-depth semantics changed after that session —
no cross-build parity claim is made or needed, since no code changed).

## Per-query byte attribution (dev cell, n=200 warm, `ZEPPELIN_SQ_BYTE_STATS`)

| Phase | GETs/q | physical bytes/q | logical bytes/q | slack/q |
| --- | --- | --- | --- | --- |
| coarse wave (`sq`) | 29.0 | 10.770 MB | 10.770 MB | **0** |
| exact rerank | 7.7 | 5.265 MB | 0.246 MB | **5.020 MB** |
| manifest (strong) | 1.0 | 181 B | 181 B | 0 |

Scan shape: 65.3 clusters selected/scored per query across 29.0 objects;
coarse candidates = rerank candidates = 40 (`fetch_k * 4`); rerank rows span
7.1 clusters in 5.7 objects. The strong-consistency manifest read is already
an ETag `If-None-Match` conditional GET (304 -> ~0 bytes; the 36,409-byte body
transferred exactly once in 201 queries), so there is no manifest-byte slice
to take.

## Finding 1 — the sibling byte pool is empty (brief premise (b) disproven)

The brief's primary target was grouped-object siblings "dragged into the
coarse wave but never scored". Measured and code-traced:

- `select_scan_clusters` returns a **whole-object closure** on every path that
  can reach the grouped TwoBit scan: at np48 the adaptive sketch budget covers
  all probes (`sketch_adaptive_cluster_cap(48) = 48`), so the scan set is
  `expand_clusters_to_objects(probes)`; the `select_grouped_object_clusters`
  path likewise admits each selected object's complete membership.
- `ZEPPELIN_SKETCH_SCAN_STATS` confirms the branch: `object_gets=28..32`,
  `clusters_covered=63..74`, `grouped=true` — and `selected_clusters` in the
  byte stats equals the closure size, which equals the scored set. The
  `!probe_set.contains(...)` skip in `scan_clusters_rq` (the source of the
  prior ledger's "decoded-but-unscored" phrasing) is **unreachable** at the
  grouped default path: the probe set it checks is the already-expanded scan
  set.
- Consequently coarse physical bytes == coarse logical bytes (slack 0): the
  `ZBP5` span `[min coarse.start .. max ids.end]` tiles exactly the coarse and
  ID blocks of clusters that are all decoded, scored, and eligible to place
  rows in the top-40 rerank window (their IDs also break approximate-score
  ties in `coarse_quantized_candidate_cmp`).

Range-reading "only the probed cluster's span" therefore saves bytes **only**
if the 17.3 closure siblings per query (65.3 scanned - 48 probed) stop being
scored — a retrieval-policy change that alters the candidate window and the
results hash. Auto-rejected under the brief's own parity rule; it belongs to
workstream C (plan section 9) with its recall gates, where "closure-elision
... may not save range bytes" is already anticipated.

## Finding 2 — the rerank plan is byte-optimal at its GET count

Method: temporary env-gated shadow instrumentation
(`ZEPPELIN_RERANK_PLAN_SHADOW`, added to `fetch_rerank_vectors_by_range`,
removed after the run; no permanent surface) dumped every query's logical row
ranges and executed physical ranges per object. Offline replay computed, for
each query, the minimum achievable bytes for any plan with the same number of
contiguous per-object ranges (split at the globally largest gaps, >=1 range
per touched object).

- **Executed bytes == optimal bytes on 200/200 queries (total excess: 0).**
  The gap-threshold coalescer splits exactly at all gaps >= 1 MiB, which are
  by definition the largest gaps — the optimal split set for the GET count it
  produces, per object and globally.
- Monotonicity: fewer requests can only merge more gaps, so bytes are
  non-increasing in GETs. Any "identical-or-fewer requests" plan fetches
  **>= 5.265 MB** rerank bytes per query. The 5.02 MB slack (28.2 bridged
  gaps/q; mean 174 KiB, p50 84 KiB, p95 666 KiB, max 1020 KiB) is the price of
  the 1 MiB request-cost default, not plan inefficiency.
- Measured per-query Pareto frontier (what any GET increase would buy — all
  points forbidden by the brief, shown for the record; consistent with the
  rejected S2 knob sweep):

| extra rerank GETs/q | rerank bytes/q | total bytes/q |
| --- | --- | --- |
| +0 (7.7) | 5.265 MB | 16.036 MB |
| +2 (9.7) | 3.938 MB | 14.709 MB |
| +4 (11.7) | 3.050 MB | 13.820 MB |
| +8 (15.7) | 1.891 MB | 12.662 MB |
| +16 (23.7) | 0.740 MB | 11.511 MB |

"Overshoot-byte caps" or any other merge policy cannot beat the threshold plan
without landing on this frontier's +GET side.

## Rejected slices (preregistered gate: >=10% total bytes at equal GETs and identical hash)

| Slice | Result | Decision |
| --- | --- | --- |
| R1 probed-span-only coarse range reads | 0 bytes available without dropping scored closure siblings (results change) | REJECT — infeasible at hash parity |
| R2 overshoot-capped / smarter rerank span merging | executed plan already byte-minimal at equal GETs (200/200 queries, 0 excess) | REJECT — mathematically dominated |
| R3 skip-decode of unscored sibling bytes | no unscored sibling bytes exist at the default path | REJECT — empty target |
| R4 manifest byte trim (conditional GET) | already implemented (`get_if_none_match`, 181 B/q) | NO-OP |

## Where warm bytes can actually go (out of this brief's scope, on record)

1. **Workstream C (plan §9):** score fewer closure siblings under the recall
   gate — a retrieval-policy experiment, never hash-preserving. Frees up to
   ~2.85 MB/q of coarse bytes (17.3 of 65.3 clusters) if quality gates pass.
2. **Workstream D (plan §10) / hydration (W3):** the 10.77 MB coarse wave is
   byte-identical immutable state re-read every query; cross-query reuse
   (decoded coarse cache or full-object hydration) eliminates it locally at
   *fewer* S3 GETs. Parked behind its own memory-contract gates; converts the
   workload to the W3 treatment, which the plan measures separately.
3. **Deployment profile knob:** `query.cost_latency_profile = "low_latency"`
   (128 KiB gap) reaches ~12.03 MB/q at +11 GETs/q — already recorded as a
   deployment recommendation only; re-proposing the default change is
   explicitly out of bounds.

## Closed-loop statement

No accepted change: the W1 ceiling stays 410 / 16.04 ≈ **25.6 QPS** at the
loopback wall. This audit establishes that the ceiling is a property of the
frozen result set + request budget + storage layout, not of fetch-plan
implementation quality.

## Validation

- No product code changed; `git diff` clean at `8d44717`. The shadow
  instrumentation was session-local and reverted before this ledger was
  written.
- Baseline hashes reproduced against the prior ledger on dev and held-out
  cells (table above) before and after the shadow build, proving the
  measurement environment (namespace, MinIO data, harness) is the same one
  that produced the 16.256 MB / 37.68 GETs baseline.
