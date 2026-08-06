# Zeppelin Hardware & Deployment Tuning

Authored from code 2026-08-05 (the doc did not previously exist; every claim
below carries a `file:line` provenance or a measurement citation). This is the
authority for which configuration knobs depend on deployment hardware — CPU,
RAM, local disk/NVMe, network, object-store latency/pricing — and the data
shape, and for how they couple. `tests/config_tuning_doc_tests.rs` asserts that
every backticked `section.field` path in this document exists in the real
`Config` (or is explicitly allowlisted as env-only/hidden), so the doc cannot
silently rot.

Provenance labels: **[M]** = measured (a cited experiment produced the number);
**[E]** = engineered (a reasoned default, no dedicated measurement behind it).

## 1. How configuration loads

- Path: `ZEPPELIN_CONFIG` env, else `./zeppelin.toml` if present
  (`src/startup.rs:260-268`). No file → boot error: the `[security]` section
  with an explicit `security.mode` is **mandatory** (`src/config.rs:3344`,
  `:3358-3376`). There is no defaults-only mode.
- Precedence: **env > TOML > compiled default** (`apply_env_overrides`,
  `src/config.rs:4163-4572`). A present-but-malformed env value is a hard
  load-time error, never a fallback (`src/config.rs:4614-4631`).
- Every config struct is `#[serde(deny_unknown_fields)]` — a typo'd key is a
  boot error, not an ignored knob.
- `Config::validate()` (`src/config.rs:3421-3933`) accumulates **all**
  violations into one message. It enforces the cross-section couplings in §7.
- Runtime-mutable overlay: five query knobs can change without restart via
  `GET/PATCH/PUT /v1/config/query` (`src/runtime_config.rs:64`):
  `query.rerank_coalesce_gap_bytes`, `indexing.default_nprobe`,
  `server.default_top_k`, `indexing.bm25_max_full_scan_clusters`,
  `indexing.bm25_max_full_scan_vectors`. Runtime values are process-local and
  reset on restart; boot config sets their ceilings.
- Per-namespace freeze: `nlist`, `quantization`, `pq_m`, `hierarchical`,
  `fts_index`, `bitmap_index` are copied from `[indexing]` at namespace
  creation into the namespace's `meta.json` and are thereafter per-namespace
  (`src/namespace/manager.rs:142-155`, `PATCH /v1/namespaces/:ns/index_config`).
  Changing process defaults does not retune existing namespaces.

## 2. CPU-sensitive

| Knob | Default | Env | Scale with | Notes |
| --- | --- | --- | --- | --- |
| `server.max_concurrent_queries` | 64 | `ZEPPELIN_MAX_CONCURRENT_QUERIES` | vCPUs and S3 connection budget | Admission semaphore (`src/startup.rs:733`); overflow is shed with 503, not queued. Keep ≤ the hardcoded 64-connection S3 pool (§9) — above it you add queueing, not throughput. [E] |
| `ZEPPELIN_COMPACTION_WORKERS` (env-only) | `(cpus/4).max(1)` | itself | vCPUs | Dedicated compaction runtime so it cannot starve queries (`src/config.rs:3284`, `src/compaction/CLAUDE.md`). Raising trades query latency for compaction throughput — measure first. [E] |
| `indexing.kmeans_max_iterations` | 25 | — | corpus size, rebuild cadence | Compaction-time k-means budget. [E] |
| `indexing.balance_max_ratio` / `indexing.balance_repair_rounds` | 4.0 / 8 | — | cluster skew | `0.0` disables repair. [E] |
| `compaction.retrain_imbalance_threshold` | 5.0 | — | ingest churn | Below it centroids are reused instead of retrained (cheaper CPU). [E] |
| `server.max_query_batch_size` | 256 | `ZEPPELIN_MAX_QUERY_BATCH_SIZE` | vCPUs | Absent from `zeppelin.toml.example` (§10). [E] |

**Inert (do not tune, documented for honesty):** `ZEPPELIN_QUERY_WORKERS`
(default `max(4, cpus*2)`) and `ZEPPELIN_RAYON_THREADS` (default `cpus`) are
parsed and logged but **not applied** — `build_app` only wires
`compaction_workers` (TODO at `src/startup.rs:83-87`). Setting them today
changes a log line and nothing else.

## 3. Memory-sensitive

| Knob | Default | Env | Scale with | Notes |
| --- | --- | --- | --- | --- |
| `cache.memory_cache_max_mb` | 256 | `ZEPPELIN_MEMORY_CACHE_MAX_MB` | node RAM | The headline RAM lever; tiered memory→disk→S3 cluster cache. `0` disables. Default 256 wastes large boxes. Missing from `zeppelin.toml.example` (§10). [E] |
| `cache.wal_fragment_cache_max_mb` | 128 | `ZEPPELIN_WAL_FRAGMENT_CACHE_MAX_MB` | RAM, ingest rate | Decoded-WAL memo; strong reads replay WAL. `0` disables. [E] |
| `cache.decoded_artifact_cache_max_mb` | 64 | `ZEPPELIN_DECODED_ARTIFACT_CACHE_MAX_MB` | RAM | FTS decode memo. `0` disables. [E] |
| `server.max_request_body_mb` | 512 | `ZEPPELIN_MAX_REQUEST_BODY_MB` | RAM ÷ concurrent writes | Each in-flight body can hold this much. [E] |
| `server.max_batch_size` | 50,000 | `ZEPPELIN_MAX_BATCH_SIZE` | RAM | Vectors per upsert/WAL fragment. [E] |
| `compaction.max_wal_bytes_before_compact` | 64 MiB | `ZEPPELIN_MAX_WAL_BYTES` | RAM | Biggest dial for compaction working-set RAM; larger batches more WAL per cycle (fewer PUTs) at higher peak RSS. [E] |
| `server.rate_limit_idle_ttl_secs` | 600 | `ZEPPELIN_RATE_LIMIT_IDLE_TTL_SECS` | client-IP cardinality | Bounds token-bucket map growth. [E] |
| `mmli.max_overlay_bytes_per_query` | 512 MiB | `ZEPPELIN_MMLI_MAX_OVERLAY_BYTES_PER_QUERY` | RAM | MMLI only. [E] |
| `mmli.segment.max_resident_bootstrap_bytes` | 128 MiB | `ZEPPELIN_MMLI_SEGMENT_MAX_RESIDENT_BOOTSTRAP_BYTES` | RAM | MMLI only. [E] |

Centroid residency is implicit RAM: pinned centroids cost roughly
`nlist × dims × 4` bytes per active flat segment (f32); budget for it when
raising `indexing.max_num_centroids` beyond the 4096 default.

## 4. Disk/NVMe-sensitive

| Knob | Default | Env | Scale with | Notes |
| --- | --- | --- | --- | --- |
| `cache.dir` | `/var/cache/zeppelin` | `ZEPPELIN_CACHE_DIR` | NVMe mount | Point at instance-store NVMe when present; the cache is disposable by contract (`src/cache/CLAUDE.md`). [E] |
| `cache.max_size_gb` | 50 | `ZEPPELIN_CACHE_MAX_SIZE_GB` | NVMe/EBS size | Hydration refusal advice explicitly says "raise `cache.max_size_gb` with a larger NVMe/node" (`src/cache/hydration.rs:1061`). [E] |
| `cache.hydration_enabled` | false | — (TOML only) | disk presence | Dark-launch flag; only useful with a real cache device. [E] |
| `cache.hydration_parallelism` | 4 | — (TOML only) | NIC, disk write speed | `buffer_unordered` segment downloads (`src/cache/hydration.rs:993`). [E] |
| `cache.hydration_max_segment_fraction` | 0.5 | — (TOML only) | cache size ÷ segment size | One segment may take at most this fraction of the cache; exceeding emits `zeppelin_hydration_refused`. [E] |
| `gc.manifest_history_keep_count` | 128 | `ZEPPELIN_GC_MANIFEST_HISTORY_KEEP_COUNT` | object-store clutter | Live manifest roots retained. [E] |
| `compaction.max_old_segments` | 10 | `ZEPPELIN_MAX_OLD_SEGMENTS` | object-store clutter | The only real prune cap (see dead `compaction.max_pending_deletes`, §10). [E] |
| `mmli.worker.scratch_dir` / `mmli.worker.bundle_cache_dir` | "" | `ZEPPELIN_MMLI_WORKER_SCRATCH_DIR` / `_BUNDLE_CACHE_DIR` | NVMe | Model bundles are large; put on fast disk. [E] |

## 5. Network / object-store-sensitive

| Knob | Default | Env | Scale with | Notes |
| --- | --- | --- | --- | --- |
| `query.rerank_coalesce_gap_bytes` | 1 MiB | `ZEPPELIN_RERANK_COALESCE_GAP_BYTES` | request price vs latency target | THE cost/latency dial; see measured table below. Runtime-mutable. `0` = exact ranges. [M] |
| `query.cost_latency_profile` | unset | — | same | Sugar: `low_cost`/`balanced` → 1 MiB, `low_latency` → 128 KiB (`src/config.rs:942-949`). Mutually exclusive with the byte value — setting both is a boot error (`src/config.rs:4083-4089`). |
| `indexing.default_nprobe` | 32 (floor) | `ZEPPELIN_DEFAULT_NPROBE` | GETs/query budget, recall | A *floor*, not the value: omitted nprobe resolves to `ceil(3/16 × nlist)` clamped to [32, `indexing.max_nprobe`] (`src/config.rs:2703-2736`). Runtime-mutable. [M — recall gate] |
| `indexing.max_nprobe` | 256 | — | hard cost ceiling | Bounds runtime-mutable nprobe. [E] |
| `indexing.bm25_max_full_scan_clusters` / `indexing.bm25_max_full_scan_vectors` | 500 / 100,000 | `ZEPPELIN_BM25_MAX_FULL_SCAN_CLUSTERS` / `_VECTORS` | corpus size | Circuit breakers for segments missing the global FTS index; `0` disables. Runtime-mutable. [E] |
| `server.request_timeout_secs` | 30 | `ZEPPELIN_REQUEST_TIMEOUT_SECS` | store latency | Feeds the GC floor (§7) AND the hydration job timeout — raising it on a slow store has safety consequences, not just UX. [E] |
| `cache.manifest_cache_ttl_ms` | 500 | `ZEPPELIN_MANIFEST_CACHE_TTL_MS` | staleness tolerance vs GET rate | Feeds the GC floor (§7). Missing from the example TOML as a settable line (§10). [E] |
| `cache.namespace_registry_ttl_ms` | 5000 | `ZEPPELIN_NAMESPACE_REGISTRY_TTL_MS` | same | Dominant GC-floor term at defaults (§7). [E] |
| `security.policy_refresh_secs` | 5 | — | store request price | Background S3 policy poll cadence. [E] |
| `security.audit_flush_secs` | 2 | — | store request price | Background audit PUT batching. [E] |
| `mmli.segment.read_gap_budget_bytes` | 64 KiB | `ZEPPELIN_MMLI_SEGMENT_READ_GAP_BUDGET_BYTES` | store | 256 KiB was the SciFact knee but proved corpus-specific; ≥1 MiB strictly worse everywhere measured (doc comment, `src/config.rs`). [M] |
| `mmli.segment.read_max_concurrency` | 16 | `ZEPPELIN_MMLI_SEGMENT_READ_MAX_CONCURRENCY` | NIC, OS buffers | Measured knee 8→16 (SciFact fetch p50 64.3→46.8 ms); 32/64 noise; 128 regressed (p95 tail + macOS mbuf shed). [M] |
| `mmli.segment.read_max_request_bytes` | 8 MiB | `ZEPPELIN_MMLI_SEGMENT_READ_MAX_REQUEST_BYTES` | store | Never the binding constraint in any measured arm. [M] |
| `storage.s3_region` / `storage.s3_endpoint` | unset | `AWS_REGION` / `S3_ENDPOINT` | region | Keep node and bucket in the same region; cross-region adds RTT to every stage and per-GB egress cost. [E] |

**The measured rerank-gap table [M]** (dbpedia100k, np16, 8 workers, loopback
MinIO ~410 MB/s; doc comment at `src/config.rs:164-202`):

| gap | GETs/q | MB/q | QPS | ~$/M queries (S3 Standard) |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB | 19.5 | 49.5 | 8.3 | $7.80 ← default, cost-optimized |
| 512 KiB | 30.6 | 41.4 | 9.8 | $12.24 |
| 256 KiB | 50.4 | 34.1 | 11.6 | $20.16 |
| 128 KiB | 79.9 | 28.6 | 13.4 | $31.96 ← loopback throughput knee |
| 64 KiB | 127.5 | 25.2 | 8.8 | $51.00 ← past the knee; never use |

Portability caveat (from the same comment): real S3 request cost pushes the
optimum **up** (2 MiB is a reasonable real-S3 starting point); very-low-latency
stores push it back down. Re-run the sweep on target hardware for a final pin.

## 6. Data-shape-sensitive

- **nlist** — `IndexingConfig::effective_num_centroids(n)` =
  `ceil(n / target_rows_per_cluster)` clamped to
  [`indexing.default_num_centroids`, `indexing.max_num_centroids`], then
  `min(n)` (`src/config.rs:2674-2695`). Defaults: 3,000-row target, 256 floor,
  4096 cap. `indexing.default_num_centroids` is a **floor**, not the nlist —
  the name misleads. [M — pinned by the recall gate]
- **nprobe** — `effective_default_nprobe` = `ceil(3/16 × nlist)`, floor
  `indexing.default_nprobe` (32), cap `indexing.max_nprobe` (`src/config.rs:2703-2736`).
  Pinned expectations: nlist 256→48, 334→63, 667→126, 4096→256
  (`src/config.rs:1818-1824`). Warning [M]: nprobe 16 caps coarse recall at
  0.85–0.93 on wiki_dpr_e5 at every config and scale (`src/index/CLAUDE.md`) —
  do not tune nprobe down for cost without re-running the recall gate.
- **Quantization** — `indexing.quantization` default TwoBit (~16×; flipped
  2026-07-29 after the G5 gate). Stored bytes/row: f32 = 4d; SQ8 = d;
  2-bit = pad256(d)/4 + 8; 1-bit = pad256(d)/8 + 8; PQ = `indexing.pq_m`.
  [M]: 1-bit retains only ~95% recall on e5-768 (insufficient); 2-bit ≥99.3%
  at nprobe 32. The closed forms are pinned against the encoders by
  `sizing_row_bytes_match_the_production_encoder` (`src/index/ivf_flat/sketch.rs`).
- **FTS** — `indexing.fts_index` only when text search is needed; a segment
  without the global FTS index falls back to full scans (check
  `SegmentRef::has_global_fts` before profiling BM25 latency —
  `src/fts/CLAUDE.md`).
- **Filters** — `indexing.bitmap_index` (default true) pays off only with
  attribute filters; it costs sidecar artifacts per segment.
- **Dimensions bound** — `server.max_dimensions` (65,536 default) is a
  namespace-create bound, not an allocation. [E]
- **Branching** — `branching.enabled` (default false) needs BOTH the flag and
  a verified signed-license entitlement (`src/namespace/CLAUDE.md`). Cost
  trap: the first compaction of a foreign-backed branch is a full-corpus
  operation — budget GETs/bytes/CPU/RAM for it (`src/compaction/CLAUDE.md`).
  `branching.max_children_per_namespace` (256, hard max 4096) and
  `branching.max_depth` (16, hard max 64) bound the graph.
- **`[wal]` is intentionally empty** — group commit is unconditional; the old
  `batch_manifest_*` knobs were removed and now fail as unknown fields
  (`src/config.rs:2193`).

## 7. Safety couplings (validation will stop your boot)

**The GC floor** (`checked_gc_horizon_floor_secs`, `src/config.rs:4024-4030`,
enforced at `:3894-3923`):

```
gc.horizon_secs >= ceil(cache.namespace_registry_ttl_ms / 1000)
                 + ceil(cache.manifest_cache_ttl_ms / 1000)
                 + server.request_timeout_secs
                 + gc.compaction_upload_window_secs
                 + gc.skew_slop_secs
```

At defaults: `5 + 1 + 30 + 300 + 5 = 341s` against `gc.horizon_secs = 900`.
Tuning any input up (e.g. `server.request_timeout_secs` for a slow store, or
manifest TTLs for cheaper reads) shrinks the margin and can make a previously
valid config fail boot. Never set `gc.allow_unsafe_short_horizon = true` in
production; it exists for tests and WARNs loudly.

Other couplings:

- Hydration's job timeout is wired to `server.request_timeout_secs` —
  re-check that relationship whenever either changes (`src/cache/CLAUDE.md`);
  it keeps hydration readers from outliving the governed-deletion grace window.
- `compaction.lease_duration_secs` (300) must exceed the longest real
  compaction cycle, which scales with corpus size and
  `compaction.max_wal_bytes_before_compact`.
- `gc.compaction_upload_window_secs` (300) must cover the slowest
  compaction upload on the deployed store — scale with corpus and store
  throughput.
- `mmli.segment.read_gap_budget_bytes` must be strictly less than
  `mmli.segment.read_max_request_bytes` (validated).
- `server.default_top_k` ≤ `server.max_top_k`; `indexing.default_nprobe` ≤
  `indexing.max_nprobe` (validated).

## 8. Env-only knobs (never in the TOML)

| Env var | Default | Effect |
| --- | --- | --- |
| `ZEPPELIN_MAX_CLUSTERS_PER_OBJECT` | 3 | **The major GETs/query dial nobody documents.** Compaction-time grouping of logical clusters into physical objects (`src/index/ivf_flat/build.rs:213-215`); changes GETs at a given nprobe. Absent from every example file. The sizing model mirrors the 3 (`grouped_cluster_coverage`). [E] |
| `ZEPPELIN_COMPACTION_WORKERS` | `(cpus/4).max(1)` | Applied; see §2. |
| `ZEPPELIN_QUERY_WORKERS` / `ZEPPELIN_RAYON_THREADS` | `max(4,cpus*2)` / `cpus` | **Inert** (§2). |
| `ZEPPELIN_CONFIG` | — | Config path itself. |
| `RUST_LOG` | `logging.level` | Standard tracing filter; overrides the file. |
| `ZEPPELIN_CLUSTER_GROUP_STATS`, `ZEPPELIN_SKETCH_SCAN_STATS`, `ZEPPELIN_SQ_BYTE_STATS` | unset | Presence-only stderr diagnostics. |

## 9. Hardcoded (not tunable without a code change)

| Constant | Value | Where | Why it matters |
| --- | --- | --- | --- |
| S3 client retries / retry timeout / backoff | 2 / 2s / 100–500ms ×2.0 | `src/storage/store.rs:509-517` | Slow cross-region stores hit the 2s retry ceiling fast. [E] |
| S3 `pool_max_idle_per_host` | 64 | `src/storage/store.rs:523` | The real concurrency ceiling behind `server.max_concurrent_queries`. Comment cites a 28% sustained-throughput degradation (Run-007) behind the sizing. [M] |
| S3 request timeout / connect / pool idle | 30s / 2s / 90s | `src/storage/store.rs:524-526` | [E] |
| ReadPlan gap / max request / concurrency | 64 KiB / 8 MiB / 8 | `src/storage/read_plan.rs:20-24` | Standalone read-plan defaults (MMLI overrides concurrency to 16 via config). [E] |
| GC read batch concurrency | 32 | `src/compaction/gc.rs:149` | [E] |
| Clone copy concurrency | 16 | `src/server/handlers/namespace.rs:184` | [E] |
| Timestamp-history GET concurrency | 16 | `src/server/handlers/as_of.rs:75` | [E] |
| Sketch adaptive policy (11 consts) | — | `src/index/ivf_flat/search.rs:130-150` | Deliberately not promoted to config: recall-coupled. [M] |

## 10. Dead knobs, traps, and example-file gaps

- **Dead:** `compaction.max_pending_deletes` is deliberately unused —
  pending deletes are uncapped by design; capping them would leak S3 objects
  (`src/wal/CLAUDE.md`). GC owns deletes-then-prune. Never emit or tune it.
- **Stale example:** `zeppelin.toml.example:95` says `default_nprobe = 16`;
  the code default is **32** (`src/config.rs:2937-2939`).
- **Example gaps:** `zeppelin.toml.example` omits ~30 real fields, including
  the headline `cache.memory_cache_max_mb`, `cache.manifest_cache_ttl_ms`
  (appears only inside a comment), `indexing.quantization`,
  `indexing.target_rows_per_cluster`, `indexing.max_num_centroids`,
  `indexing.default_probe_fraction`, `server.max_query_batch_size`, all four
  principal rate-limit fields, `compaction.max_old_segments`,
  `mmli.text_matrix_dtype`, and the **entire `[mmli.segment]` table**
  (13 fields). `.env.example` similarly omits `ZEPPELIN_MEMORY_CACHE_MAX_MB`,
  `ZEPPELIN_MAX_CLUSTERS_PER_OBJECT`, and the CPU-budget vars.
- **Trap:** `indexing.default_num_centroids` is a floor, not the nlist (§6).
- **Trap:** `mmli.visual_matrix_dtype = "int8_g32"` is hard-rejected
  (99.25% < 99.5% fidelity bar, `src/config.rs:3724-3729`); only text may
  drop to int8.
- **Trap:** the `Local` storage backend is not a deployment target — CAS
  returns `NotImplemented` (`src/storage/CLAUDE.md`); three lib tests fail
  without MinIO for exactly this reason (`tests/CLAUDE.md`).

## 11. What the sizing advisor automates

`zeppelin_advisor` (see `src/sizing/`) consumes this document's rules: it
derives nlist/nprobe through the canonical `IndexingConfig` seams (§6), prices
GETs/query through the calibrated Tier 2 model (GT-A ≤10%, GT-B ≤20% QPS
residual), sets the §2–§5 knobs from the chosen instance's vCPU/RAM/NVMe/NIC,
auto-satisfies the §7 GC floor, and validates every emitted file through
`Config` parse + validate before writing. Numbers it cannot ground in a
measurement it labels as engineered estimates.
