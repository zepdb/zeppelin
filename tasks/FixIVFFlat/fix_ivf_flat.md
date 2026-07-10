# Fix IVF-Flat Recall: Scale-Aware Probes + Balanced Training

## Authorized no-spill revision (2026-07-09)

The Phase 0 sweep found no declared spill point satisfying the original
G1/G2/G3 conjunction. Anup authorized the following binding revision after
reviewing `results/phase0.md`; it overrides later spill-specific text:

- Drop multi-assignment spill. Every logical row is stored exactly once;
  there is no spill config, manifest tag, dedup migration, or spill delete
  path.
- Use the measured scale-aware/balanced policy:
  `target_rows_per_cluster = 3000`, `default_probe_fraction = 3/16`,
  `default_nprobe = 32` as the floor, `balance_max_ratio = 4.0`, and
  `balance_repair_rounds = 8`.
- Revise G2 to mean scored-row fraction ≤ **0.20**. Keep G1 ≥ 0.96,
  G3 ≤ 1.5×, G4 ≥ 0.999, and G5 determinism unchanged. With no spill,
  expected storage inflation is exactly 1.0×.
- Preserve four commits: (1) partition seam plus RED gate, (2) scale-aware
  nlist and balanced training, (3) per-segment scale-aware default-probe
  resolution plus explicit no-spill invariants, and (4) sketch/integration
  validation and docs.

Measured selection evidence: 1M k=334/np=63 gives recall 0.968810 at
scan 0.18603; 2M balanced k=667/np=126 gives recall 0.981420 at scan
0.18227. Both store 1.00000× logical rows.

Self-contained implementation plan for main. Prereq reading, in order:

1. `tasks/FixIVFFlat/investigation.md` — the July 9 evidence (production
   path map, controlled matrix, causal explanation). All numbers cited
   below come from it or from `tasks/July10Quant/results/bakeoff.md`.
2. `tasks/FixIVFFlat/top2_experiment.md` — Phase 0. **Phase 0 must run
   first**; it pins the numeric constants marked `[P0]` below. Do not
   guess them.
3. `AGENTS.md` / `CLAUDE.md` — all style and architecture rules apply.

Datasets referenced throughout live at
`~/Documents/code/zeppelin-devbench/data/` (`wikidpr1m`, `wikidpr2m`,
`dbpedia100k`). They are immutable, SHA-256-pinned via their `meta.json`.
Never modify them; fail loud on any size/shape mismatch.

## Why (one paragraph)

Zeppelin's coarse IVF stage caps recall@100 below 0.96 for e5-class cosine
embeddings at every tested configuration: with production defaults
(256 centroids, nprobe 16, 25 mini-batch iterations) the *exact* in-probe
ceiling on wiki_dpr_e5 is 0.854 @ 100k rows, 0.897 @ 500k, 0.917 @ 1M,
0.928 @ 2M — while scanning ~12.5% of the corpus per query because
occupancy skew (max/median ≈ 6×) makes probes land on mega-clusters. The
cause is measured, not hypothesized: each query's 100 true neighbors are
spread over a median of 13–21 clusters, and covering 96 of them takes an
oracle 9–17 clusters (p90 20–28). Geometry (L2-vs-cosine) is a proven
non-factor (≤0.005). The July 10 quantization bake-off failed its gate
*because of this ceiling* (`bakeoff.md`: ceiling 0.9252 @ np16, so no
encoder could pass 0.96). This plan raises the coarse ceiling to ≥ 0.96 at
today's read cost, so the quantization program can be re-gated on top.

## The three levers (do all three; they compound)

1. **Scale-aware probe budget** — default nprobe becomes a function of
   cluster count instead of the constant 16. Buys recall with probes.
2. **Balance-repairing k-means** — fixes the degenerate occupancy that
   currently makes every probe ~2× more expensive than it should be
   (measured: 12.7% scan where balanced clusters would scan 6.25%).
   Buys the bytes back that lever 1 spends.
3. **Multi-assignment spill (top-2)** — boundary vectors are additionally
   stored in their second-nearest cluster. The only lever that raises
   recall at a fixed probe count; directly attacks the measured neighbor
   spread. Costs storage (×1.1–1.4, bounded by `[P0]`).

## Binding acceptance (the RED→GREEN gate)

A new committed test, `tests/ivf_recall_gate.rs`, ignored by default
(real-dataset, ~30–60 min), run explicitly:

```bash
ZEPPELIN_RECALL_GATE_DATA=~/Documents/code/zeppelin-devbench/data \
  cargo test --release --test ivf_recall_gate -- --ignored --nocapture
```

For **each** of `wikidpr1m` and `wikidpr2m` the test must assert, using
the production partition/probe code (see Phase A seam) at the **default**
policy (no per-request overrides):

- G1: exact in-probe recall@100 ≥ **0.96** (mean over the 1,000 queries,
  against the dataset's pinned exact ground truth).
- G2: mean scored-rows fraction ≤ **0.20** — scored rows *include* spill
  duplicates, divided by logical corpus rows. This forbids passing G1 by
  brute-force scanning.
- G3: storage inflation from spill ≤ **1.5×** logical rows (report the
  actual value; the expected value from Phase 0 is nearer 1.1–1.3×).
- G4: full-probe sentinel — probing all clusters yields recall@100
  ≥ 0.999 (harness correctness).
- G5: determinism — building the partition twice from identical input
  yields identical assignments (compare a hash of the assignment vector).

The test also *reports* (no assertion) the same metrics on a 100k prefix
and recall@10, for trend tracking.

**RED today**: the current code measures 0.9166 (1M) and 0.9278 (2M) on
G1. The test must be written and committed in Phase A, observed RED, and
left RED in the tree until the levers land (do not skip/disable it; do
not weaken thresholds to make it pass — per repo rule 1, fail loud).

If, after all phases, G1+G2+G3 cannot be met simultaneously, **stop and
report the best measured operating point** — do not silently relax a
threshold. That decision belongs to Anup.

## Non-goals

- No quantization changes (RaBitQ/SQ8/PQ are untouched; the quant rerun
  happens after this lands, on the quant thread).
- No graph/HNSW/hierarchical redesign. `hierarchical: true` namespaces
  are out of scope; spill applies to the flat path only.
- No change to WAL, lease, CAS, or manifest CAS semantics.
- No new dependencies. Everything here is loops and floats.

## Phase 0 — operating-point sweep (offline, before any src/ change)

Run per `tasks/FixIVFFlat/top2_experiment.md`. It produces
`tasks/FixIVFFlat/results/phase0.md` pinning these constants:

| constant | meaning | sweep range | placeholder |
| --- | --- | --- | --- |
| `[P0.tau]` | spill threshold on d2/d1 in squared-L2 units (0 = off) | {off, 1.2, 1.44, 1.7, 2.0} | ~1.44 |
| `[P0.frac]` | default probe fraction of cluster count | {1/16, 1/8, 3/16} | 1/8 |
| `[P0.np_floor]` | minimum effective default nprobe | {16, 24, 32} | 32 |
| `[P0.rpc]` | target rows per cluster for scale-aware nlist | {3000, fixed-256-off} | 3000 |
| `[P0.bal]` | balance repair on/off + max-occupancy ratio | {off, 4×, 6× mean} | 4× |

The sweep must show the chosen point clears G1/G2/G3 on both gate
datasets *in the harness* before production wiring begins. If no point
clears, stop and report (see gate section).

## Phase A — partition seam + gate test (commit 1, leaves gate RED)

### A1. Extract a pure partition function

Today `build_ivf_flat` (src/index/ivf_flat/build.rs:2671) interleaves
training, assignment, and S3 writes. Extract the CPU-only core into a
pure, store-free function in `src/index/ivf_flat/build.rs` (or a new
`partition.rs` in the same module):

```rust
/// Deterministic CPU-only IVF partition: trains centroids and assigns
/// every vector (with optional top-2 spill). No I/O.
#[must_use]
pub struct IvfPartition {
    pub centroids: Vec<Vec<f32>>,
    /// clusters[c] = row indices into the input slice, primary + spilled.
    pub clusters: Vec<Vec<u32>>,
    /// Primary cluster per row (canonical membership).
    pub primary: Vec<u32>,
    /// Count of spilled copies (for stats/inflation accounting).
    pub spilled: usize,
}

pub fn partition_vectors(
    vectors: &[&[f32]],
    dim: usize,
    config: &IndexingConfig,
) -> Result<IvfPartition>
```

`build_ivf_flat` then consumes `IvfPartition` and keeps doing exactly
what it does today (serialization, sidecars, sketch, bootstrap). The
refactor alone must be behavior-preserving: same centroids, same
assignments, same artifacts (spill defaults OFF until Phase C). Verify
with the existing unit tests plus one new test asserting the refactored
build produces identical cluster assignments to a pinned small input.

This seam is what the gate test, the Phase-0 harness (eventually), and
the quant bake-off driver will all call, so the gate can never drift
from production behavior again.

Keep the existing buddy-affinity computation (build.rs:2747-2752) — note
it already computes second-nearest clusters; spill (Phase C) reuses that
loop rather than adding a second pass.

### A2. The gate test

`tests/ivf_recall_gate.rs`, `#[ignore]`, structured as:

1. Read `ZEPPELIN_RECALL_GATE_DATA`; **error with instructions if unset
   or dataset dirs missing** (no fallback path).
2. Load `corpus_vectors.f32`, `query_vectors.f32`,
   `ground_truth_top100.u32`, validating byte sizes against `meta.json`
   (`corpus_n × dims × 4`, etc.). The reader is ~30 lines; the reference
   implementation is in `tasks/FixIVFFlat/harness/ivf_diag.rs`
   (`read_exact_f32` / `read_exact_u32`).
3. Build `Vec<VectorEntry>`-equivalent refs and call `partition_vectors`
   with `IndexingConfig::default()` equivalents (the same defaults the
   server resolves).
4. Probe exactly as production does: rank all centroids with
   `compute_distance(query, centroid, DistanceMetric::Cosine)`
   (src/index/ivf_flat/search.rs:1109-1123 semantics: stable sort, ties
   keep index order), take the **default-policy** nprobe (Phase B
   function), exact-score every row in the probed clusters with cosine,
   dedup by row id keeping best score (spill!), take top-100 with
   tie-break score-desc/row-asc, compute recall vs GT.
5. Assert G1–G5. Print a per-dataset metrics table (`--nocapture`).

Parallelize over queries with `std::thread::scope` (12 threads; the
harness shows the pattern). Keep all reductions deterministic (join
worker results in worker order).

Commit 1 = A1 + A2 + a run log showing the gate RED with today's
numbers. Include the measured values in the commit message body.

## Phase B — scale-aware nlist/nprobe + balanced training (commit 2)

### B1. Config additions (src/config.rs, IndexingConfig)

New fields, all `#[serde(default = ...)]` so existing config files parse
unchanged (`deny_unknown_fields` only rejects unknown keys):

- `target_rows_per_cluster: usize` — default `[P0.rpc]`. Effective
  centroid count becomes
  `nlist = clamp(ceil(n / target_rows_per_cluster), default_num_centroids, max_num_centroids)`
  where `max_num_centroids: usize` (new, default 4096) bounds resident
  centroid memory (4096 × 768 × 4 B ≈ 12.6 MB per segment). Wire into
  `build_ivf_flat`'s current `k = config.default_num_centroids.min(n)`
  (build.rs:2699), preserving the `min(n)` guard.
- `default_probe_fraction: f64` — default `[P0.frac]`. Add
  `#[must_use] pub fn effective_default_nprobe(&self, cluster_count: usize) -> usize`
  on `IndexingConfig`:
  `clamp(ceil(fraction × cluster_count), [P0.np_floor], max_nprobe)`.
- `spill_ratio_sq: f32` — default `[P0.tau]`; `0.0` disables spill.
  Named `_sq` because it thresholds d2/d1 of **squared** L2 distances.
- `balance_max_ratio: f64` (default `[P0.bal]`, e.g. 4.0) and
  `balance_repair_rounds: usize` (default 8). `balance_max_ratio = 0.0`
  disables repair.

Every new field gets the doc-comment treatment the file already uses.
Update `default_max_nprobe_covers_default_centroid_count`-style config
tests accordingly.

### B2. Resolve the default nprobe where cluster count is known

Today the query handler resolves `req.nprobe.unwrap_or(knobs.default_nprobe)`
at src/server/handlers/query.rs:1102, before the segment is loaded. The
actual cluster count is available from the manifest (recall_eval already
resolves `--nprobe all` to "the actual compacted cluster count" — follow
that same source). Change the resolution so that an *omitted* nprobe is
resolved per-segment via `effective_default_nprobe(cluster_count)` at the
point where the segment's cluster count is known (query planning /
`search_ivf_flat` call site), while an explicit request nprobe keeps
today's semantics (validated against `max_nprobe`, `nprobe ≥ 1`).
Preserve the multi-source/algebra validation rules around
query.rs:1157-1276 untouched.

### B3. Balanced training (src/index/ivf_flat/kmeans.rs)

Two changes, both inside the existing `train_kmeans` seam (signature
unchanged — it already takes `k`, iters, epsilon):

1. **Scale the mini-batch budget with k.** `DEFAULT_BATCH_SIZE = 1024`
   (kmeans.rs:79) starves high-k training (measured: at k=667/2M the
   median cluster ends with 6 rows). Make the per-iteration batch
   `max(1024, 32 × k)` (cap at `n`). At k=256 → 8,192/iter; k=667 →
   21,344/iter. Keep 25 iterations. Cost stays trivially small next to
   k-means++ init, which is already O(n·k).
2. **Post-training balance repair** (new function, called from
   `train_kmeans` after Lloyd/mini-batch, before returning):

```text
repeat up to balance_repair_rounds times:
  assign all rows to nearest centroid (parallel, deterministic reduce)
  mean_occ = n / k
  overfull = clusters with occ > balance_max_ratio × mean_occ
  if none: stop
  for each overfull cluster (largest first):
    donor slot = an empty cluster, else the smallest cluster
      (its rows will re-home naturally next round)
    new centroid = the member row of the overfull cluster farthest
      from its centroid (deterministic: lowest row index on ties)
    replace donor slot's centroid with that row's values
  recompute the two touched centroids as member means next round
```

   This is the standard split-largest repair; it is deterministic,
   allocation-light, and needs no new math. The full-corpus assignment
   pass it needs is the same cost as the one `build_ivf_flat` already
   performs afterwards — reuse: have `partition_vectors` do repair
   rounds and final assignment in one flow so the corpus is scanned
   O(rounds+1) times, parallel with `std::thread::scope`, workers over
   contiguous row ranges, partial sums merged in worker order
   (determinism; see the pattern in
   `tasks/FixIVFFlat/harness/ivf_diag.rs`).

   Note: parallelizing assignment changes no results (it is a pure
   argmin per row) but the reduction order for centroid means must stay
   fixed — accumulate per worker, then fold workers in index order,
   f64 accumulators.

3. **Do not touch** the geometry: squared-L2 training/assignment stays
   (investigation proved metric alignment is worth ≤ 0.005 and the
   spherical variant measured *worse*, 0.8587 — see
   `tasks/July10Quant/results/bakeoff-spherical-rejected.md`).

Existing kmeans unit tests must keep passing; add tests: (a) batch-size
scaling formula, (b) repair eliminates a synthetic 10:1 skew within the
round budget, (c) determinism of repair (two runs, identical centroids).

### B4. Measure

Re-run the gate test. Expected movement (from the investigation matrix):
G2 scan fraction drops toward ~6–8% at np16-equivalent; G1 rises with
`[P0.np_floor]`/fraction but likely lands 0.95–0.97 — possibly still RED
on the 1M dataset. That is expected; spill is the remaining lever.
Record actual numbers in the commit body.

## Phase C — top-2 spill (commit 3, gate should go GREEN here)

### C1. Build side (`partition_vectors`)

In the assignment loop (which already tracks best and second-best,
build.rs:2730-2756 semantics): after computing `d1 = best`, `d2 =
second`, if `config.spill_ratio_sq > 0.0 && d2 <= config.spill_ratio_sq
* d1 && second_cluster != best_cluster`, also push the row into
`clusters[second_cluster]`, incrementing `spilled`. Guard `d1 == 0.0`
(exact-centroid rows never spill). `primary` records only the best
cluster. Buddy-affinity counting is unchanged (it already reads
best+second).

Downstream in `build_ivf_flat`, nothing changes structurally: cluster
serialization, SQ8 encoding, attrs sidecars, bitmaps, and the resident
sketch all operate per-cluster on whatever rows the cluster contains —
spilled copies are literally duplicate (id, vector, attrs) rows in a
second cluster object. That is the entire storage design: **no new
artifact type, no layout change**. Immutability rule 3 is untouched.

### C2. Manifest + format tagging

Add `spill_ratio_sq: f32` (or an `ivf_format: u32` version field —
pick one, document it) to `SegmentRef` in src/wal/manifest.rs, written
at publish. Readers use it for two things:

1. Enable the query-side dedup path (C3) — dedup unconditionally is
   also acceptable and simpler; if so, the field is informational, but
   still required for (2).
2. **Forced retrain trigger**: in the compaction decision
   (src/compaction/mod.rs:1137-1167), if the old segment's recorded
   spill/format value differs from the current config, set
   `should_retrain = true`. Without this, the incremental path
   (retrain only when WAL adds > 5× existing rows,
   `retrain_imbalance_threshold`, src/config.rs:1575) means existing
   namespaces would **never** pick up the fix. This also finally gives
   ops a knob: bump config, next compaction rebuilds.

MessagePack-serialized manifest: new field must be optional/defaulted so
old manifests deserialize (existing pattern: `has_global_fts` on
SegmentRef was added the same way).

### C3. Query side: dedup by id

With spill, the same vector id can be scored in two probed clusters.
Invariant: **a vector id appears at most once in any result set, scored
by its best (exact) distance**. Apply at the point where per-cluster
candidates merge before the final top-k, in all scan paths:

- `scan_clusters_sq` (search.rs:2091) — dedup the SQ frontier before
  `rerank_count = fetch_k * 4` truncation (search.rs:2248), otherwise a
  duplicated id wastes two frontier slots.
- `scan_clusters_flat` and `scan_clusters_pq` — same merge point.
- BM25 / FTS result assembly — audit and apply the same invariant.

Also audit **deletes**: compaction currently filters `deleted_ids` when
rebuilding/carrying clusters. Verify (and add an adversarial scenario
proving) that a deleted id disappears from **both** its primary and
spill clusters across: full retrain, incremental rebuild, and bounded
incremental carry-over (src/compaction/mod.rs:1365-1543). If bounded
carry-over would preserve a stale spill copy in an untouched cluster
object while the primary is rewritten, either (a) the id-level delete
filtering at read/compaction time already covers it — document where —
or (b) disable per-cluster carry-over for rows whose spill partner
cluster is touched. Do not guess; trace it and write the invariant into
a test.

`vectors_compacted` and any row-count metrics count **logical** rows
(`primary.len()`), never primary+spill.

### C4. Measure

Gate test again. This is the commit where G1 ≥ 0.96 must hold on both
gate datasets with G2/G3 intact, per the Phase-0 sweep's prediction. If
the production-path numbers diverge from Phase 0's harness numbers by
more than ~0.005, stop and reconcile before proceeding (same algorithm
must produce the same partition — diff the assignment hashes).

## Phase D — sketch budget + integration validation (commit 4)

1. **Sketch budget vs the new probe counts.** The resident sketch caps
   selected clusters below nprobe (`adaptive_sketch_budget`:
   search.rs:1796-1822; cap(16) = 14 — test search.rs:4177). With the
   new default nprobe (≈ `[P0.np_floor]`+), read the budget curve and
   ensure it does not silently eat the recall this plan just bought:
   acceptance = on dbpedia100k via the devbench recall sentinel
   (MinIO), sketch-ON recall@100 at the new default nprobe within
   **0.01** of sketch-OFF (`nprobe ≥ 128` sentinel semantics), at ≤ the
   GET budget the sketch currently maintains at np16 (~7 grouped
   objects — scale proportionally). Tune the curve constants
   (search.rs:131-145) if violated; keep the existing
   `sketch_cluster_budget(16) == 7`-era tests updated to the new curve
   with justification in comments.
2. **Full validation battery** (all must pass; record outputs):
   - `cargo test` — full unit suite (226+ tests).
   - `cargo test --test proptest_ivf_recall` — full-probe + monotonicity
     properties.
   - The RED→GREEN gate (both datasets) — now GREEN; paste the metrics
     table into `tasks/FixIVFFlat/results/gate_green.md`.
   - Devbench production-path sentinel on dbpedia100k over MinIO
     (`docker compose -f docker-compose.test.yml up -d`; harness:
     `~/Documents/code/zeppelin-devbench/runner/`, see its README;
     devbench Cargo.toml path-depends on this checkout, so it picks up
     the working tree automatically). Expect recall@100 at default
     policy ≥ **0.95** (historical np16 baseline was 0.849–0.892).
   - Adversarial smoke (correctness backstop per repo convention) —
     spill touches compaction and deletes; the nightly soak should
     follow.
   - `cargo clippy --release -- -D warnings` and `cargo fmt --check`.
3. **Docs**: update CLAUDE.md/AGENTS.md learnings with the new config
   knobs and the "gate test is the recall authority" rule.

## What "done" means / what to re-run

Done = commits 1–4 on main, gate GREEN, battery green, results files
committed under `tasks/FixIVFFlat/results/`. Then, **outside this plan**
(Anup / quant thread): re-run the RaBitQ bake-off
(`quant/phase1-bakeoff` branch) against the fixed partitions — the
driver should be pointed at the new `partition_vectors` seam so it
measures the shipped algorithm rather than its own IVF construction.

Expectation setting for that rerun (from `bakeoff.md` retention data):

- The exact ceiling at the default operating point rises to ≥ 0.96, so
  **SQ8 ≈ ceiling** and **2-bit RaBitQ ≈ 0.993–0.998 × ceiling** should
  both clear ~0.955–0.96 end-to-end.
- **1-bit will improve in absolute terms but keeps its ~4–6 point
  retention tax on e5-768** (0.939–0.966 × ceiling at 4–5× margin;
  dimension/score-gap bound, not an implementation defect — it measured
  99.98% retention on 1536-dim dbpedia). It will still trail 2-bit.
  If 1-bit at np-default/4× is still wanted, the rescore margin must
  scale with the in-probe candidate pool (retention measurably degrades
  as the pool grows at fixed ×100 margins).

## Guardrails

- All CLAUDE.md rules: no fallbacks (fail loud), thiserror variants,
  tracing structured fields, strong types, `#[must_use]`,
  `bytes::Bytes` between layers, tests against real storage for S3
  paths.
- No new dependencies. The repair/spill/gate code is loops over floats.
- Determinism is part of the contract: identical input ⇒ identical
  partition (G5). Any parallelism must use ordered reductions.
- Immutability: never rewrite existing segment artifacts; new behavior
  arrives only via newly built segments + the forced-retrain trigger.
- Old segments stay readable: manifest field defaults, dedup path safe
  on non-spilled segments (dedup of a set with no duplicates is a
  no-op).
- Commit messages: imperative mood, wrapped at 70 chars, one commit per
  phase, measured numbers in the body.
- Do not tune anything against the 1,000 evaluation queries beyond the
  Phase-0 sweep's declared grid; never train on query ground truth.

## Environment prerequisites (check before starting)

- Datasets present: `wikidpr1m` (3.1 GB), `wikidpr2m` (5.7 GB),
  `dbpedia100k` under `~/Documents/code/zeppelin-devbench/data/`.
- ≥ 16 GB free RAM for the 2M gate run (corpus 6.1 GB resident +
  partitions); machine used for baseline had 16 cores / 128 GB.
- MinIO only needed for Phase D's devbench sentinel
  (`docker-compose.test.yml`), not for Phases 0–C.
- Gate runtime budget: ~30–60 min for both datasets in `--release`
  (k-means++ init is O(n·k) and single-threaded today — at 2M/667
  that's ~10 min; do not "optimize" it in this plan beyond the parallel
  assignment/repair passes already specified).

## Risks and stop conditions

1. **Phase 0 finds no operating point meeting G1+G2+G3** → stop, write
   the best pareto row into phase0.md, escalate to Anup (options: relax
   G2 to 0.20, accept 0.95, or add a probe-and-rescore second stage —
   his call, not the implementer's).
2. **Spill × bounded-incremental carry-over × deletes** is the riskiest
   correctness surface. If the delete-invariant trace (C3) shows stale
   spill copies can survive carry-over, prefer correctness over
   cleverness: disable carry-over on spilled segments (a measured perf
   cost) and note it — never ship a path where a deleted vector can be
   returned.
3. **Sketch budget fights the new probe counts** (Phase D.1). If tuning
   cannot hold the 0.01 parity bound within the GET budget, surface the
   trade-off table rather than picking silently.
4. Gate runtime creep: if the 2M cell exceeds ~90 min, gate on 1M and
   demote 2M to a weekly job — but only with Anup's sign-off, since 2M
   is the quant program's reference scale.
