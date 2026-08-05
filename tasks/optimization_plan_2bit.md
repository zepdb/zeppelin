# TwoBit warm dense-query optimization plan

**Status:** planned, unexecuted
**Written:** 2026-08-05
**Planning baseline:** Zeppelin `9f0f4119f61463ee3c8b52c8e95c58921f4f99a3`
**Primary question:** how much of the current warm, ordinary flat-IVF TwoBit
query can be removed in practice, while keeping exact-rerank correctness,
retrieval quality, S3 authority, and bounded memory?

This is an execution plan, not a latency claim. Estimated ceilings below are
work bounds. Only the final recomposed benchmark in [M6](#m6) may be
quoted as an achieved cumulative improvement.

## 1. Scope and frozen vocabulary

### 1.1 Primary workload

Freeze this cell before implementing a treatment:

- ordinary, non-hierarchical dense IVF-Flat;
- persisted/default `TwoBit` coarse payload and exact `f32` rerank;
- cosine, `top_k = 10`, omitted `nprobe`, strong consistency;
- no caller filter, mandatory security filter, facet, group, cursor, explicit
  reranker, or visible WAL;
- one compacted active segment; and
- `dbpedia100k`: 100,000 rows, 1,536 dimensions, 256 centroids. Omitted
  `nprobe` therefore resolves to `ceil(3/16 * 256) = 48`.

The API default is not “np16/top100.” `48` is also not universal: the omitted
policy is scale-aware, with a floor of 32 and a cap of 256. The expected 1M and
2M cells are about 334/63 and 667/126 clusters/probes. Explicit `nprobe` keeps
its current contract.

The primary server benchmark uses the real HTTP query endpoint with only the
vector supplied. A direct-library lane is retained for CPU attribution, not as
an HTTP latency substitute. Initial local work may use `SecurityMode::OpenUnsafe`;
that means authentication, policy evaluation, and mandatory-filter overhead are
not measured. A production-security canary is a separate final check.

### 1.2 Warm states are different treatments

Every result must name one of these states; never report only “warm.”

| Name | Required proof | What still reads object storage? |
| --- | --- | --- |
| `W0 bootstrap-cold` | fresh process/namespace and empty local caches | manifest, bootstrap, coarse and rerank reads |
| `W1 repeat-range-warm` | first query excluded; metadata/index resident; complete cluster objects absent from `DiskCache` | strong manifest verification plus coarse and rerank range waves on every query; range results are not admitted to the raw cache |
| `W2 hydrated-disk-first` | incarnation-qualified active-segment objects hydrated; memory tier cleared/restarted | strong manifest; cluster ranges slice complete disk-cached objects |
| `W3 hydrated-memory-steady` | W2 plus one untimed promotion pass; zero segment S3 GETs and nonzero local bytes asserted | strong manifest only |
| `W4 decoded-coarse-warm` | the bounded cache from workstream D has admitted named coarse objects; its hit rate and resident bytes are reported | misses plus exact-rerank ranges unless full objects are also hydrated |

`W1` is the ordinary repeat-warm target. `W3` is the CPU ceiling lane. `W4` is
a new cache policy and must not be relabeled as ordinary repeat-warm.

### 1.3 Explicit non-goals

- Late-interaction, hierarchical, filtered, scoped-ANN, hybrid, facet, group,
  cursor, and explicit reranking paths are not performance targets here. They
  remain correctness sentinels.
- Do not weaken strong manifest verification. Its local measured cost is small
  and the S3 manifest is authoritative.
- Do not change `4 * top_k`, exact `f32` reranking, or row visibility inside an
  I/O optimization.
- Do not re-propose one-bit codes or the resident-sketch row-frontier bypass.
  The latter was built and rejected in
  `tasks/July10Quant/results/fixed-stride-f32.md`: winners were dispersed and
  recall materially fell.
- Do not put partial raw objects into `DiskCache`, silently reinterpret an
  explicit `nprobe`, or use a fallback after a planner/cache error.
- Do not tune grouping arity or rewrite ZBP5 in the object-planning experiment.
  Existing artifacts are immutable and the cap-3 grouping work was already
  measured in `tasks/opt-sketch-log.md`.
- Repairing the known 128-dimensional TwoBit payload-quality gap is a separate
  correctness track. Exact refactors here must be bit-identical on that cell;
  numerical changes are parked until the gap is resolved.

## 2. Evidence that motivates the plan

These are scratch investigation results on an Apple M3 Max, macOS, Rust 1.93,
loopback MinIO, and the planning commit. M0 must reproduce and preserve them;
they are not portable cloud-S3 claims.

### 2.1 Current-default measurements

| Lane | Measured result | Relevant work |
| --- | --- | --- |
| W1 direct query layer, strong, 200 queries after first | mean **11.905 ms**, p50 11.919, p95 13.627, p99 14.597; recall@10 0.98856 | 0.808 ms manifest/setup; 3.454 ms coarse range wave; **5.426 ms decode/ADC/RQ/top-k planning**; 2.122 ms rerank range wave; 0.095 ms exact parse/score/finalize |
| W3 real HTTP, 3 x 200 | mean **9.063 ms**; p50 9.028-9.224; p95 10.127-10.349 | handler about 8.924 ms; dense source 8.411; IVF scan 8.023; router/client/JSON outside handler about 0.335 ms |
| W3 instrumented RQ | client about 9.259 ms | cached fetch/decode 2.430 ms; coarse score 5.052; rerank 0.437; 66.7 logical clusters and 27,537.7 coarse rows per query |
| current TwoBit top100 comparison | mean 15.819, p50 15.491, p95 **20.340** | explains why “about 20 ms” is credible for top100 p95, not the current top10 default |

The W1 boundary partition overlaps decode with outstanding GETs. It is useful
wall attribution, not a sum of pure I/O and pure CPU.

W1 averaged 28.99 coarse objects/10.770 MB and 7.695 exact ranges/5.265 MB.
Forty logical 1,536-dimensional vectors are only 245,760 bytes, so the 1 MiB
rerank gap caused about **21.4x** physical-byte amplification in this cell.

A symbol sample, among 2,008 symbolized Zeppelin exclusive leaf samples, found:

- `prepare_query_adc4`: 34.8%;
- `estimate_residual_dot_two_bit_parts`: 32.6%;
- `StructuredRotation::rotate_in_place`: 10.3%;
- `RqClusterCodesOnly::from_bytes`: 9.6%; and
- exact `compute_distance`: 2.6%.

Sampling materially perturbed latency. These percentages identify suspects;
they are not additive wall-time shares.

### 2.2 Honest ceilings, not promises

| Lever | Measured payer or hard bound | It cannot remove |
| --- | --- | --- |
| A: RQ CPU | W1’s 5.426 ms processing partition; W3’s 8.023 ms scan | manifest, object-store RTT, exact-range bytes, HTTP |
| B: rerank read planning | 2.122 ms and 5.265 MB in W1; useful minimum 245,760 B for 40 unique rows | logical candidate count or exact scoring; fewer bytes may require more GETs |
| C: object-aware planning | 48 selected centroids expand to 66.7 clusters and 29.5 objects in W3; cap 3 is only a loose bound | manifest and exact rerank; covering the same top-48 clusters fixes the touched-object set, so fewer objects requires a changed cluster set |
| D: decoded coarse reuse | 2.430 ms fetch/decode bucket and 9.6% sampled parser leaf; a hit may also avoid the coarse range GET | row scoring, query ADC, exact rerank, and misses under a finite budget |

A zero-cost phase is an impossible Amdahl bound, not an expected speedup.
Object planning must compute its actual per-query lower bound from the manifest
layout; `3x` is not an optimization forecast.

## 3. Existing work to preserve rather than redo

Read these before the named workstream:

- TwoBit selection, codec, format, scan, and rollout:
  `tasks/quantization-research.md`, `tasks/July10Quant/phase4/`,
  `tasks/July10Quant/results/storage-2bit.md`,
  `tasks/July10Quant/results/bakeoff.md`,
  `tasks/upgrade-infra-to-2bit.md`, and `tasks/two-bit-bycatch-plan.md`.
- Exact-range coalescing:
  `tasks/July10Quant/phase4/06-coalescing-gap.md` and
  `tasks/July10Quant/results/coalescing-gap.md`. Their 128 KiB loopback knee
  and 1 MiB S3-cost choice are priors, not an answer for dense top10.
- Grouping/sketch work: `tasks/opt-sketch-log.md`,
  `tasks/opt-qps-log.md`, and `tasks/July10Quant/precompute-research.md`.
  Production omitted probes are outside the narrow range in which the current
  resident-sketch budget prunes, so it is normally a no-op at 48/63/126.
- ZBP5 addressing and the rejected resident-row bypass:
  `tasks/July10Quant/results/fixed-stride-f32.md`.
- Whole-object hydration/local-first range behavior:
  `tasks/new-cache.md`, `tasks/new-cache-tasks.md`,
  `tasks/new-cache-task-9.md`, and `tasks/new-cache-todo.md`. Historical 77-79
  QPS was pre-TwoBit and is not a baseline for this plan; Task 9's whole-segment
  decoded tier remains gated.
- Decoded-cache precedents:
  `tasks/perf_optimizations/06-decoded-artifact-memo.md` and
  `src/cache/decoded_cache.rs`. The existing FTS/scoped decoded budget defaults
  to 64 MiB and must not be silently consumed by large RQ objects.
- Measurement/rebaseline discipline:
  `tasks/todo.md` (“Method”), `tasks/July10Quant/phase4/00-README.md`,
  `tasks/LateLatency/full_plan.md`, and
  `tasks/backlog/02-perf-contract-rebaseline.md`.

Existing warm-query performance contracts pin SQ8. A TwoBit contract is new
reviewed evidence, not permission to recapture or widen those bands.

## 4. Execution contract and dependency graph

```text
M0 benchmark + attribution
       |
       +----------+-----------+-----------+-----------+
       |          |           |           |
       v          v           v           v
   A RQ CPU    B rerank I/O  C object plan  D decoded reuse
       |          |           |           |
       +----------+-----------+-----------+
                          |
                          v
                  M6 direct recomposition
```

A-D branch from the same accepted M0 commit and are measured independently.
Do not let a win in one workstream hide a regression in another. Integrate only
accepted slices, then measure their interactions directly.

For every slice:

1. State one hypothesis and one convicted payer.
2. Add a sensitivity RED or shadow measurement before product behavior.
3. Implement the smallest change behind the existing domain seam.
4. Run the focused correctness gate and the frozen primary performance cell.
5. Append exact commands, SHAs, configuration, raw artifact paths, and a
   PASS/FAIL/REVERT decision to the results ledger created by M0.
6. Revert rejected implementation code. Do not accumulate speculative flags or
   a silent compatibility fallback.

Use one reviewed commit per slice, prefix Cargo work with `CARGO_INCREMENTAL=0`,
and never rebaseline a contract merely because it turned red. Errors from an
active new planner/cache propagate. A default-off shadow computation may emit
a blocking diagnostic without changing the baseline result.

## 5. M0 — durable benchmark and attribution foundation

### M0.1 Freeze a clean grader

The scratch runners used for the motivating numbers are not a durable grader.
Create a clean, pinned benchmark worktree and commit these capabilities before
any treatment:

- runtime selection and verification of a real TwoBit/ZBP5 segment;
- current omitted `nprobe` resolution rather than a hardcoded 48;
- library and real-HTTP modes using the same query corpus;
- explicit W0-W4 cache-state setup and assertions;
- `top_k` 10 and 100;
- workers 1/4/8/16;
- strong and eventual consistency;
- per-query latency plus phase/counter JSON; and
- exact result/score-bit hashes and recall against fixed ground truth.

The counting adapter must wrap the exact `ZeppelinStore` constructed and used
by `build_app`; a counter attached to a sibling harness store is not evidence
about HTTP manifest or segment requests.

Do not build on the dirty external qpsbench checkout. Record both Zeppelin and
harness SHAs and clean-tree proofs. Store compact run JSON and summaries under
`tasks/optimization_2bit_results/`; store large profiles under
`target/optimization_2bit/<run-id>/` and commit their SHA-256 manifest, not the
large files.

Each run record must include:

- full command/environment and resolved config (`nlist`, `nprobe`, code dims,
  grouping histogram, rerank frontier/gap/policy);
- dataset and query-file digests plus query offset/range;
- cache capacity, contents, hydration completion, memory/disk promotion state,
  and decoded-cache state;
- GET attempts/successes and returned bytes by manifest/coarse/rerank/attrs and
  local source tier;
- OS/CPU/RAM/power mode, disk, MinIO or S3 version/location, network shaping;
- `rustc -vV`, Cargo.lock digest, features, target features and RUSTFLAGS; and
- raw samples, p50/p95/p99/mean, QPS, RSS/allocations, recall and result hash.

Use a fresh namespace per quantization/layout treatment. Keep `TempDir` handles
alive, use unique IDs across fragments, clean stale MinIO prefixes, and run
under a benchmark lock with no soak or compaction workload beside it.

### M0.2 Add low-perturbation attribution

Extend the existing query-local diagnostics rather than scattering public
knobs. Keep detailed values in sampled/debug JSON or structured spans; use only
bounded-cardinality Prometheus labels.

Time at nanosecond precision:

1. manifest/index/bootstrap load;
2. centroid rank and physical-object planning;
3. rotation construction, query rotation, and centroid preparation separately;
4. ADC4 preparation;
5. coarse local/range fetch wait;
6. RQ header/plane/factor, ID, and vector-range decoding separately;
7. row scoring;
8. bounded-frontier/ID materialization;
9. rerank range planning and range wait;
10. exact parse/distance; and
11. attribute/finalization work.

Count:

- requested/effective probes, sketch-selected clusters, closure clusters,
  objects, rows decoded/scored, ADC builds, ID materializations;
- logical useful bytes, planned/returned bytes, slack, amplification, ranges,
  GETs, maximum in-flight reads, and cache source;
- decoded-cache hits/misses/admissions/evictions/waiters/live bytes; and
- approximate candidates, frontier rows, exact rows and final rows.

Do not rename the old SQ diagnostics in the same slice. Generalize them in a
separate mechanical commit only if needed. Measure headline latency with verbose
profiling off; require profiling-off overhead <=1% or keep the collector solely
in benchmark/debug builds.

### M0.3 Microbenchmarks

Add Criterion coverage for:

- `StructuredRotation::new`, one query rotation, and N centroid rotations;
- `prepare_query_adc4` at 256/768/1,536 dimensions;
- one-row and batch TwoBit scoring at the same dimensions;
- `RqClusterCodesOnly::from_bytes` at 390/3,000/9,000 rows;
- ID/range decoding and frontier materialization; and
- the complete CPU-only coarse scan.

`rabitq.rs` is deliberately crate-independent and can be included in a
benchmark as `quant_bakeoff` does. Do not widen the production interface solely
for Criterion. Save Criterion baselines and compiler-generated assembly for the
hot kernels.

### M0.4 Baseline matrix and STOP rule

Development uses 200 timed queries after an explicit warmup; acceptance uses at
least 1,000 fixed queries and a disjoint held-out set. Final p99 uses at least
5,000. Run at least three paired/interleaved repetitions (five in M6), randomize
A/B order, and require run spread <=5% before deciding.

Primary cells:

1. W1 direct-library strong and W1 real HTTP strong, dbpedia100k/top10/default
   probe;
2. W3 direct-library eventual for CPU isolation and W3 real HTTP strong for
   product latency;
3. top100 sentinel; and
4. W0 first-query recorded separately but excluded from warm distributions.

Scale confirmation is deferred to M6, but M0 must prepare 768-dimensional
334/63 and 667/126 fixtures. MinIO is valid for local attribution; it is not
real-S3 latency evidence.

**STOP:** do not implement a CPU sublever unless its measured component is at
least 10% of the relevant W3 scan or its work-count reduction projects at least
3% end-to-end headroom. Do not implement an I/O/cache treatment whose shadow
plan has no non-dominated point.

## 6. Common correctness, quality, and performance gates

### 6.1 Exact/authority gate

For a semantics-preserving slice, compare baseline and treatment per query:

- identical ordered IDs, `score.to_bits()`, attributes and underfill behavior;
- identical effective probe/frontier/row visibility and result tie breaking;
- unchanged object-store keys, manifests, GET/byte plan unless the slice
  explicitly owns that plan; and
- the same loud errors for malformed dimensions, non-finite factors, bad
  layouts, short ranges, wrong cache types and missing objects.

S3 and the selected manifest remain authoritative. Cache identity must be the
incarnation-qualified physical origin, never the logical namespace. Clearing a
new cache mid-run may change CPU/I/O only, never results.

### 6.2 Retrieval-quality gate

`tests/ivf_recall_gate.rs` remains the partition/probe authority and must run on
both `wikidpr1m` and `wikidpr2m` whenever omitted-probe or partition behavior
moves:

```bash
CARGO_INCREMENTAL=0 ZEPPELIN_RECALL_GATE_DATA=<dataset-dir> \
  cargo test --release --test ivf_recall_gate -- --ignored --nocapture
```

Pinned recall@100 is 0.9688/0.9814. The gate scores exact f32 and is blind to a
corrupt or numerically changed TwoBit payload. Therefore any kernel numerical,
frontier, or object-plan change must also run the production `quant_bakeoff`
seam and a production-path TwoBit recall evaluation. The retained 768d
margin-4 reference is r@10 0.988200 and r@100 0.980040 at nprobe126, recovering
99.857% of the exact 0.981440 ceiling. Freeze the current treatment-control
values before comparison; do not substitute an all-cluster synthetic cell.

Exact A/B/D changes require bit identity and therefore zero recall movement. C
may change clusters; it must pass both official gates and the stricter C gate
below. Any accepted numerical change needs its own explicit quality decision.

### 6.3 Performance decision rule

For a small exact slice:

- convicted micro/component speedup >=10%;
- primary p50/mean improvement >=2% with a paired 95% bootstrap confidence
  interval excluding zero;
- no p95 or p99 regression >3%; and
- no unexplained change in allocations, RSS, GETs, bytes, rows or recall.

Promote a whole workstream only if it improves its primary latency or QPS by at
least 5%, or is a necessary enabler for an already measured larger win. Reject
Pareto-dominated treatments. Never add percentage improvements or phase timers
to claim cumulative benefit.

## 7. Workstream A — exact RaBitQ/ADC4 CPU work

**Files/seams:** `src/index/ivf_flat/search.rs::scan_clusters_rq`,
`src/index/ivf_flat/sketch.rs::{ResidentSketch, ResidentEncoding}`,
`src/index/quantization/rabitq.rs`, `src/index/quantization/rq.rs`, and
`src/index/topk.rs`.

### A1 — reuse immutable rotation geometry

**Hypothesis.** `scan_clusters_rq` rebuilds `StructuredRotation` per query and
rotates every scanned centroid even though `ResidentSketch::with_centroids`
already owns the validated rotation and pre-rotated centroids.

**Implementation.** Add one crate-private concrete prepared-RQ module, not a
new trait. Its small interface prepares a query once and returns validated
cluster residual state while hiding rotation seed, code dimension, scratch, and
centroid ownership. Borrow/share the existing `Arc<StructuredRotation>` and
rotated centroids; rotate only the query. Make a seed/dimension/centroid mismatch
a loud index error.

Keep the existing purpose-specific ADC seeds. `rq_query_adc_seed` and the
sketch seed derivation differ; unifying them would be a numerical change. Dot
may share one ADC, while cosine/Euclidean still require one centroid-relative
ADC per cluster.

**RED/tests.** Count one rotation construction plus query and centroid
rotations today; assert treatment performs zero construction, one query
rotation and zero centroid rotations per query. Differential-test every
prepared residual and approximate score bit. Run cosine/euclidean/dot, padded
128->256, 768 and 1,536 dimensions.

**Accept/reject.** Apply the common exact gate. Require >=3% W3 scan improvement
or reject unless the module measurably enables A2/A3 without increasing the
interface surface. Rollback is a normal code revert; no artifact changes.

### A2 — reusable, bit-exact ADC4 workspace

**Hypothesis.** `prepare_query_adc4` allocates four `Vec<u64>` values per
cluster and makes separate finite/min/max/packing passes.

**Implementation.** Hide caller-reused planes in an `Adc4Workspace`. Validate
and compute min/max in one ordered pass, clear/repack existing words, and pack
64 coordinates at a time only if it preserves:

- SplitMix draw count/order and seed;
- f64 conversion, division, dither, `floor`, clamp and cast;
- plane and bit order; and
- `step == 0`, signed-zero and error behavior.

Do not replace division with a reciprocal or change stochastic rounding in this
slice. Reuse a cluster ADC between sketch selection and coarse scanning only in
the rare path where both actually score the same cluster with the same purpose
seed; prove the identity first.

**RED/tests.** Oracle old vs new planes/lower/step/code_sum and row scores over
random/adversarial vectors, constant queries, quantization boundaries, NaN/Inf,
and all dimensions. Count allocations/ADC and ADC builds/query.

**Accept/reject.** Common exact gate plus >=10% ADC microbench and >=2% primary
W3 improvement. Rollback is code-only.

### A3 — once-validated fused batch scorer

**Hypothesis.** Each row repeats dimension/bounds/metric dispatch and traverses
packed planes about eleven times: stored sums, eight stored/query intersections,
and agreement count.

**Implementation.** Put the deep interface in `rabitq.rs`: a prepared TwoBit
scorer validates query/dimension once and scores a validated row or batch.
Adapters in `rq.rs` and `sketch.rs` supply storage. In the scalar oracle, load
low/high and query words once per word and accumulate all integer counts in one
loop. Hoist metric/query constants and row-stride math. Preserve the existing
integer and floating association initially, including the difference between
coarse L2’s cluster offset and resident-sketch scoring.

First validate non-finite/nonnegative/inconsistent correction factors in both
RQ decoders; ZSK1 already does this, while the current RQ readers do not. A new
view must fail loud rather than make corruption faster.

Only inspect NEON/AVX after scalar fusion remains dominant in both profile and
assembly. Dispatch once per query, keep scalar as oracle, and benchmark every
supported architecture. No architecture-specific default based on the M3
alone.

**RED/tests.** Randomized score-bit comparison against the old scalar path for
zero/nonzero residuals and all metrics; malformed row/stride/factor corpus;
`rabitq` statistical tests; `rq` margin tests; sketch direct-ADC/determinism;
`tests/rq_scan_tests.rs`; the TwoBit fuzz target; MinIO ranged-read parity.

**Accept/reject.** Common exact gate. Scalar fusion requires >=10% batch-score
micro improvement and >=2% W3 improvement. SIMD requires >=20% component and
>=10% end-to-end on both arm64 and x86_64 with no unsupported-target slowdown.
Rollback selects the scalar implementation explicitly; runtime dispatch errors
must not fall back silently.

### A4 — bound frontier state and defer ID/range materialization

**Hypothesis.** The RQ loop currently clones an ID for about 27.5k coarse rows,
builds every vector range, then keeps only 40. The shared streaming `TopK`
already exists; this is not another partial-sort project.

**Implementation.** Carry a compact, query-local locator
`(object, cluster, row, score)` through the existing `TopK` of size
`4 * fetch_k`. Keep decoded object/ID bytes alive through selection and
materialize owned IDs and vector ranges only for retained rows. Preserve ID
ascending tie-breaking: a lazy-ID design must decode on equal-score comparison
or otherwise prove the same comparator. Filtering remains before truncation.

Prototype a validated fixed-stride, query-local RQ byte view before adding an
owned copy. It may borrow only the fetched coarse span for the duration of the
query; never retain a small `Bytes::slice` in a process cache if that pins an
entire hydrated f32 object. Validate header, exact length, factors and alignment
once. Little-endian/unaligned reads must work on all supported targets.

Split this into two commits: A4a locator/frontier, A4b byte-backed decode. If A4b
makes decode negligible, D must be re-convicted rather than assumed useful.

**RED/tests.** Old materialized-vector + `partial_topk_by` is the oracle across
score ties, duplicate IDs, filters and corrupt ID blocks. Assert materialized
IDs/ranges are bounded by the frontier except tie checks, results are bit
identical, and resident memory does not pin full f32 payloads.

**Accept/reject.** Common exact gate, >=80% reduction in ID allocations/range
objects, and >=3% W3 improvement. Reject a shallow abstraction that merely
moves the same allocations. Rollback is code-only.

### A5 — pipeline or parallelize only after work removal

**Hypothesis.** W1 can leave row scoring after the slowest coarse fetch, while a
low-concurrency W3 query may leave cores idle. Scheduling is useful only after
A1-A4 reduce avoidable work; it is not a substitute for them.

Run two separate sensitivities:

1. replace all-results `join_all` processing with bounded
   `FuturesUnordered` consumption so an object's validated decode/score can run
   while other object GETs remain outstanding; and
2. only if workers 1/4/8 show CPU headroom and poor scale, score independent
   object batches on a bounded global query-CPU pool.

The first may reduce W1 latency without adding CPU. The second can reduce one
query's latency while harming node QPS and p99. Do not spawn one blocking task
per cluster/query, oversubscribe Tokio, add a dependency without approval, or
change compaction's CPU budget. The old SQ8 `spawn_blocking` idea was rejected
under an I/O-bound profile; fresh TwoBit evidence is required.

Deterministic locators and the shared comparator must make completion order
irrelevant. Test cancellation, first error, concurrency permits, server
shutdown, workers 1/4/8/16, CPU utilization and queueing. Accept only if W1 or
W3 p50 improves >=5%, node QPS does not fall, and p95/p99 do not regress >3%
at any supported worker count. Otherwise keep the sequential scalar path and
remove the scheduling code.

### A6 — numerical/persisted precompute is parked by default

A query-independent grid norm or residual-dot scale could remove the agreement
pass, square root and divisions. It changes rounding and may require a new row
format; TwoBit compaction already measured +31.39% encode overhead versus SQ8.
Do not start A6 unless A1-A5 leave scoring dominant and a shadow calculation
projects >=10% end-to-end.

If separately approved, first derive grid norm in decoded memory without a
format change, then consider a versioned immutable field. Require <=2% row-size
growth, compaction/encode budgets, old-format explicit decoding, all
payload-aware quality gates, and >=10% end-to-end on both architectures. Never
rewrite old artifacts or silently treat an old row as a new one.

## 8. Workstream B — exact-rerank I/O planning

**Files/seams:** `RerankRangeRequest`, `coalesce_rerank_ranges`,
`fetch_rerank_vectors_by_range`, and `load_full_clusters_for_rerank` in
`src/index/ivf_flat/search.rs`; `QueryConfig`, `CostLatencyProfile`, and
`DEFAULT_RERANK_COALESCE_GAP_BYTES` in `src/config.rs`; storage request counts
at `ZeppelinStore::get_range`.

Candidate generation, filter order, frontier and exact scoring are frozen in
this workstream.

### B1 — instrument the legacy plan and compute shadows

For each object, record logical unique row bytes, adjacent-gap distribution,
planned ranges/bytes, cumulative bridged slack, amplification, maximum span,
GETs and maximum concurrency. Attribute actual returned bodies and physical
requests at `ZeppelinStore::get_range`; `get_ranges` may fan out and must not be
counted as one request.

Compute alternatives without I/O beside the executed 1 MiB plan. A hydrated
hit must stay zero segment S3 GETs for every policy.

### B2 — introduce a pure planner seam

Add a typed, deterministic `RerankReadPlanner` whose output is physical spans
plus the exact original-request back-map. Keep `LegacyFixed { gap_bytes: 1 MiB }`
bit-for-bit as the control. Invalid ranges, overflow, invalid policy, or an
unmappable response are errors, not reasons to run legacy automatically.

Use `src/storage/read_plan.rs` as a design precedent for cumulative gap budgets,
maximum request bytes, bounded concurrency and caller-order restoration. Reuse
it only if exact-rerank cache validation/accounting and mapping remain deep
behind one suitable interface; do not layer two planners.

### B3 — preregister the Pareto sweep

Run the same logical requests through:

1. fixed gaps: 0, 8, 32, 64, 96, 128, 256, 512 and 1,024 KiB;
2. a cumulative slack/amplification budget plus hard maximum span;
3. a target request count that merges the smallest gaps first, minimizing added
   bytes for that request count; and
4. an explicit cost planner minimizing
   `physical_gets * request_cost + returned_bytes * byte_cost`, with hard
   amplification/span/concurrency ceilings.

A chain of individually small gaps must not bypass the cumulative caps. Bound
execution with a query-wide semaphore/`buffer_unordered`; current nested
`join_all` must not open an unbounded number of range requests.

Do not nominate 128 KiB in advance. Fit request/byte parameters separately for:

- in-memory/counting storage for planner correctness only;
- W1 loopback MinIO;
- request-dominated, byte-dominated and balanced MinIO through a real network
  shaper/latency profile; and
- at least one actual S3-compatible deployment before changing a global
  profile/default.

Run top10 and top100, 100k/1M/2M, W1 and W3. Report GETs, bytes and latency
separately even when a weighted cost improves. Transfer may be free
intra-region but bytes still consume bandwidth; cross-region and S3 Express
have different economics.

### B4 — correctness, acceptance, rollout

Planner property tests cover empty/reversed/overflow, unsorted, adjacent,
overlap, duplicate, small-gap chains, multi-object, cap boundaries,
deterministic ties and exact back-mapping. MinIO tests cover missing/short data,
wrong-length full-cache eviction, memory/disk/hydrated states and bounded
concurrency. Results and exact score bits must match legacy.

Reject any policy with:

- a correctness mismatch, uncapped span/concurrency, cache-admission change, or
  nonzero hydrated segment GETs;
- p95 regression >5%; or
- Pareto domination on `(bytes, physical GETs, p95)`.

Promote a `CostLatencyProfile` only with >=10% measured profile-cost reduction
and held-out evidence. For a byte-dominated profile, preregister >=50% p95
amplification reduction. A request-dominated profile may correctly retain 1
MiB. Do not change the global default unless one policy wins robustly across
supported deployments; otherwise require explicit profile selection.

Roll out as instrumentation -> default-off shadows -> explicit MinIO/S3
profile -> canary. Rollback atomically selects `LegacyFixed(1 MiB)`; no artifact
or cache migration is required. Keep the legacy planner until a rollback drill
and canary soak pass.

## 9. Workstream C — physical-object-aware probe/scan planning

**Files/seams:** centroid selection and `select_scan_clusters`,
`cluster_fetch_objects`, `expand_clusters_to_objects`, and
`fetch_object_row_layout_range` in `src/index/ivf_flat/search.rs`; production
`partition_vectors`; ZBP5 `ClusterRowLayoutRef`/`ClusterDataObjectRef`.

Current order selects the closest logical centroids and then closes over whole
objects. At default nprobe the resident sketch is normally a no-op. Because the
object mapping is a partition, covering the exact same top-48 clusters touches
exactly the same objects. Any object-count reduction substitutes/drops ranked
clusters and is a retrieval-policy experiment, not an exact I/O refactor.

### C1 — measure the attainable set-cover frontier in shadow

For every query, preserve centroid distances and compute:

- logical probes -> distinct objects -> closure clusters/rows/bytes;
- group occupancy and ranks/distances of free siblings;
- the exact minimum objects possible for 48 clusters under the current object
  sizes (a work bound, not a quality-feasible plan);
- best achievable retained centroid-rank mass for each object/row/byte budget;
  and
- treatment objects/clusters added/dropped relative to production.

Do not execute a treatment until this shadow shows a useful Pareto point.
Never substitute resident-sketch row scores for on-storage coarse scores.

### C2 — pure planner and eligible treatments

Place one pure planner after production centroid ranking and before physical
fetch. It returns selected objects and the logical member clusters to score.
Eligibility is exact and checked before dispatch:

- omitted `nprobe`;
- unfiltered, flat, TwoBit;
- ZBP5 with validated row layouts; and
- not the full/all-cluster sentinel.

Explicit `nprobe`, any caller or mandatory policy filter, legacy/SQ8/f32,
hierarchical/scoped ANN, and full-probe sentinel remain byte-for-byte on the
current path. An eligible treatment with inconsistent metadata fails loudly; it
does not switch planners mid-query.

Shadow and then test these independent policies:

1. **Closure-elision:** fetch the current touched objects but decode/score only
   the originally selected logical clusters. This isolates free-sibling CPU and
   recall value; it may not save range bytes because ZBP5 coarse and ID blocks
   span the object.
2. **Object-first/best-member:** rank each object by its best production
   centroid rank/distance, scan its closure, and stop at a logical-cluster or
   object budget.
3. **Cost-aware coverage:** maximize frozen centroid-rank/distance utility per
   query-independent cost (`row_count` and coarse+ID bytes), under a control
   row/byte budget.
4. **Reduced-object sensitivity:** only after shadow safety, execute 10% and
   20% fewer objects and report every dropped top rank and added sibling.

Do not tune max clusters/object, sketch cutoffs, or query-specific grouping in
this workstream.

### C3 — quality and performance gate

Extend the offline evaluator through the production object-planner seam; do not
copy IVF or grouping logic into the benchmark. On both `wikidpr1m` and
`wikidpr2m` require:

- the official exact-f32 IVF recall gate remains green;
- payload-aware TwoBit margin-4 recall remains above its frozen control;
- no deterministic recall@10 or recall@100 decrease on the fixed primary query
  set; and
- the paired held-out 95% confidence lower bound for treatment-control recall
  is no worse than -0.001 absolute. Any measured trade needs separate human
  approval and cannot become the omitted default under this plan.

Also prove explicit probes, filters/mandatory filters and the all-cluster
sentinel are unchanged.

Promote only with >=10% fewer W1 physical coarse reads **and** >=5% warm p95 or
QPS improvement, no >10% increase in rows/bytes, and no >2% cold/p99
regression. For W3, where segment GETs are already zero, report local bytes,
decode/scoring rows and latency rather than claiming an S3 win. Reject if the
quality-safe frontier has no qualifying point.

Rollout is default-off shadow -> explicit omitted-probe treatment -> scale
canary. Rollback selects the existing logical-centroid planner. No format,
manifest or compaction change is permitted.

## 10. Workstream D — bounded decoded immutable coarse-object reuse

**Files/seams:** `load_rq_object_for_coarse` and `CoarseObjectRqFetch` in
`src/index/ivf_flat/search.rs`, `RqClusterCodesOnly` in
`src/index/quantization/rq.rs`, `IvfFlatIndex::artifact_cache_key`,
`src/cache/decoded_cache.rs`, and startup/server cache wiring.

The whole-segment decoded tier in `tasks/new-cache-task-9.md` was correctly
parked and remains parked: the current profile does not establish that decode
dominates the warm path. D is a narrower, coarse-only, default-off sensitivity
motivated by the new 2.430 ms fetch/decode bucket and 9.6% parser leaf. Do not
implement it until M0 confirms D's own payer threshold (or explicit review
approves reopening the old gate); the current evidence is not acceptance for
any cache, bounded or otherwise.

### D1 — define one safe cached value and identity

Cache only query-independent, validated ZBP5 coarse state for one physical
object:

- per-cluster packed RQ planes/factors or a compact validated owned view;
- hoisted ID data/offsets; and
- deterministic vector ranges/layout needed by retained rows.

Do not cache query ADC/scorer state, filters/visibility, candidates, attrs,
exact vectors, results, or a small `Bytes` slice that pins the complete f32
object.

Use a typed identity containing:

- `IvfFlatIndex::artifact_cache_key(object.key)` (physical origin plus namespace
  incarnation, not logical namespace or raw S3 key alone);
- artifact kind/version (`ZBP5` TwoBit coarse); and
- validated dimension, object size, and authoritative row-layout identity.

A fork shares only when it resolves to the same physical origin. Delete/recreate
with the same logical/S3 spelling must miss. This is a decoded derived-section
cache, not a raw partial-object entry in `DiskCache`.

Do **not** put these values in `DiskCache::decoded`: that map is unbounded and
supports one decoded type per key. Either extend `DecodedArtifactCache` with a
separate weighted TwoBit admission class or add a dedicated
`DecodedTwoBitCoarseCache`. Keep the caller interface deep:

```text
get_or_decode(identity, weight_hint, fetch_and_validate) -> Arc<DecodedRqObject>
```

The cache decides singleflight, reservation, decode, admission and eviction.
The caller supplies only a manifest-selected immutable identity and fetch
closure.

### D2 — hard memory and concurrency contract

Add a separate config budget, default **0/off**. Screen 64 MiB, then 128 MiB
only with container headroom. Do not borrow the existing 64 MiB FTS/scoped
budget or the 256 MiB raw memory-cache budget.

Base code bytes alone are approximately:

- 392 B/row at 1,536 dimensions (~39.2 MB, 37.4 MiB, per 100k rows);
- 200 B/row at 768 dimensions (200 MB/190.7 MiB per 1M rows and
  400 MB/381.5 MiB per 2M),

before IDs, range vectors, keys, structs and allocator overhead. ResidentSketch
already retains comparable codes, so RSS must report both copies.

Required mechanics:

- calculate weight from actual allocation capacities, ID bytes, range/layout
  vectors and key/entry overhead;
- enforce global and per-entry caps; an oversized decode may serve its current
  query but is not admitted;
- reserve the estimated bytes before decode and charge live `Arc`s until their
  last owner drops, even after LRU eviction;
- singleflight and recheck per key; concurrent same-key misses decode once;
- bound simultaneous different-key decodes; cancellation releases reservations
  and locks; and
- never cache errors or negative lookups.

The sampled weighted-LRU pattern in `decoded_cache.rs` is a starting point, but
its current duplicate concurrent decodes and post-insert accounting are not
sufficient for these large objects.

### D3 — correctness, origin, churn, and performance tests

Test disabled, miss/fill, hit, eviction/refill, clear-mid-run, oversized,
cancellation and decode-error states. Require exact ID/order/score-bit parity.
Fuzz/truncate headers, factors, IDs and layouts and preserve loud errors.

With real MinIO and branching support prove:

- same logical key/different incarnation cannot hit;
- a carried/shared physical origin may hit;
- delete/recreate misses;
- N concurrent same-key queries perform one decode/allocation; and
- many-key churn never exceeds the configured live-byte cap plus a documented
  allocator/measurement tolerance.

Metrics: hit/miss/admit/oversize/evict; leader/waiter; decode count/time; entry,
resident, reserved and live-after-eviction bytes; hit rate; coarse GET/range
bytes avoided; raw source; RSS and latency/QPS. No namespace/object-key labels.

Benchmark budget 0/64/128 in W1, W3 and W4 under sequential hot, concurrent
same-key, random working-set and adversarial churn loads, at 100k/1M/2M. Keep A
and C off for the independent D decision; then repeat with accepted A in M6.
SQ8/legacy/ineligible queries are controls.

Reject if, on the target working set:

- decode is <5% of warm CPU after accepted A work;
- steady hit rate is <70%;
- primary warm p95/QPS improves <5%;
- cold/p99 regresses >2%; or
- live memory, origin, concurrency or exact-parity gates fail.

A hit can avoid both decode and the coarse range read; report those benefits
separately. Rollback sets the budget to zero and restarts/clears disposable
entries. No artifact migration is needed and no reader may depend on cache
presence.

<a id="m6"></a>
## 11. M6 — recompose and measure the achievable cumulative gain

### M6.1 Integration order and interaction design

Start again from the frozen M0 baseline and integrate only accepted commits in
this order:

1. exact A1-A5 stack;
2. D decoded cache at its accepted budget;
3. B planner for each accepted cost profile; and
4. C object planner last, because it changes work and may change recall.

For every step run the same baseline and cumulative build in paired/interleaved
order. Record both incremental and baseline-relative change. Then run final
ablations with one workstream disabled at a time. At minimum evaluate the full
2x2 interaction for A4 byte-view vs D decoded cache and B rerank planner vs C
object planner. If all four remain independently toggleable during measurement,
run the 16-cell factorial once; do not keep benchmark-only switches in the
shipping interface after the decision.

Choose the non-dominated final stack. For example, if A4 removes decode cheaply
and D adds little beyond it, the “achievable” build omits D rather than summing
both claimed wins.

### M6.2 Final matrix

Run five paired repetitions, at least 5,000 timed queries for p99, using fixed
and held-out query sets:

| Axis | Required cells |
| --- | --- |
| scale | dbpedia100k 256/48; wikidpr1m 334/63; wikidpr2m 667/126 |
| cache | W1, W2, W3; W4 if D accepted; W0 separately |
| API | direct library and real HTTP |
| consistency | eventual CPU isolation and strong product path |
| depth | top10 primary, top100 sentinel |
| concurrency | workers 1/4/8/16; report serial latency and closed-loop QPS separately |
| backend | memory/counting correctness, loopback MinIO, shaped request/byte profiles, actual S3 canary |
| architecture | arm64 and x86_64 for kernel changes |

For actual S3, record region/topology/storage class and do not extrapolate from
MinIO. Strong W1 has three dependent remote phases (manifest, coarse, rerank);
if deployment TTFB makes a latency target structurally impossible, report the
request-depth bound rather than hiding it in an average.

Final production-security canary repeats the accepted HTTP cell with enforced
security configuration. It validates composition; it does not retroactively
attribute security overhead to A-D.

### M6.3 Cumulative arithmetic

For percentile `p`, frozen baseline `L0,p`, cumulative state `Li,p`, and QPS
`Q`:

```text
cumulative speedup       = L0,p / Li,p
cumulative reduction     = 1 - Li,p / L0,p
incremental speedup i    = L(i-1),p / Li,p
QPS uplift               = Qi / Q0 - 1
```

Calculate these only from direct measurements in the same paired campaign. Do
not multiply per-slice speedups, add percentile reductions, add overlapping
phase timers, or combine the historical SQ8 hydration result with TwoBit.
Report confidence intervals and raw requests/bytes/rows/RSS beside latency.

Also report these distinct journeys:

1. current W1 -> optimized W1;
2. current W3 -> optimized W3 (CPU/product-code improvement);
3. current W1 -> current W3 (existing hydration contribution); and
4. current W1 -> optimized W3/W4 (total achievable local-warm stack).

The cumulative report is complete even if the winning stack is small or empty.
The answer is the measured Pareto frontier, not a preselected target. A useful
productization bar is >=15% primary W3 p50 reduction or QPS gain with no p95,
quality, memory or cost regression; record a smaller honest result rather than
moving the bar.

### M6.4 Release/rollback drill

Before calling a stack accepted:

- switch B back to legacy planning and C to current logical planning at runtime;
- set D budget to zero and clear/restart it;
- run the scalar/non-SIMD A oracle on both architectures;
- verify exact results during each transition;
- verify cache eviction/corruption still reads authoritative S3 explicitly; and
- leave immutable old artifacts readable through their explicit versioned
  decoder.

No active error may cause an automatic fallback. Rollback is an operator or
release action selecting the old implementation/config, followed by a proved
query.

## 12. Validation ladder

Focused first, then integration:

```bash
CARGO_INCREMENTAL=0 cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings
CARGO_INCREMENTAL=0 cargo check --tests --features branching-test-support

CARGO_INCREMENTAL=0 TEST_BACKEND=minio \
  cargo test --no-fail-fast --test rq_scan_tests --test warm_range_tests \
  --test warm_parity_tests --test hydration_tests

CARGO_INCREMENTAL=0 TEST_BACKEND=minio \
  cargo test --no-fail-fast --features branching-test-support \
  --test artifact_origin_tests --test branch_root_tests \
  --test branch_fork_tests --test branching_tests -- --test-threads=1

CARGO_INCREMENTAL=0 cargo test --lib
```

The documented three Local-backend CAS failures in `cargo test --lib` are not a
license to ignore a new failure; identify them explicitly. Add planner property
tests and decoded-cache concurrency/capacity tests to the focused commands as
their files land.

For C or any numerical/partition change, run the dual-dataset recall and
payload-aware gates from section 6. For every accepted workstream, run the
existing get-count/perf contracts without recapturing bands. A new default
TwoBit perf contract requires separate approval and three stable release
captures.

## 13. Results ledger template

M0 creates `tasks/optimization_2bit_results/summary.md` with one row per
decision:

| Slice/SHA | Hypothesis | Benchmark cell | Baseline -> treatment | GET/bytes/rows/RSS | Correctness/recall | Decision/reason | Rollback proved |
| --- | --- | --- | --- | --- | --- | --- | --- |

Each entry links raw JSON/profile checksums and records rejected attempts too.
End with one final table containing W1 and W3 baseline, accepted cumulative
state, direct delta/confidence interval, scale/S3 qualifications, and every
remaining limitation.

## 14. Definition of done

- M0’s clean, pinned TwoBit grader reproduces the baseline and proves each warm
  state.
- Every practical workstream has an independent sensitivity result and an
  explicit accept/reject decision; no rejected code remains.
- Exact, authority, corruption, branch-origin, concurrency, memory and quality
  gates pass as applicable.
- The accepted stack is rebuilt from baseline and directly measured at 100k,
  1M and 2M, locally and on an actual S3-compatible canary.
- M6 reports incremental effects, interactions, ablations and the direct
  cumulative improvement without adding ratios.
- Runtime/config rollback is drilled; no artifact was rewritten and no error is
  silently degraded.
- The results ledger contains exact SHAs, commands, configs, environment,
  dataset digests, raw artifact checksums and honest limitations.
