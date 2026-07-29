# Upgrade infra to 2-bit — SQ8-assumption census

Census run 2026-07-29 alongside the `default_quantization()` flip
(`src/config.rs:2261` → `TwoBit`, uncommitted at census time). Three parallel
read-only sweeps: `tests/`, `src/`, and the ops surface (perf contracts, API
schema, docs, scripts, benches, fuzz/loom). Status column reflects work done
in the same session; see the G2 sweep log
`tasks/July10Quant/results/g2-sweep-twobit-flip.log`.

## 1. Functional gaps found in `src/` (loud failures under the new default)

| Site | Problem | Status |
| --- | --- | --- |
| `src/index/quantization/rq.rs` `RqClusterCodes::encode` | Required rows/centroid at exactly the padded code dim (256-multiple); any `dim` not a multiple of 256 (e.g. test fixtures at dim 4/16) failed two-bit compaction with `Rq("centroid dimension mismatch: expected 256, got 4")`. Invisible pre-flip because every two-bit test used dim 256/768. | **FIXED** — zero-pads shorter inputs (same semantics as sketch build `sketch.rs:1415` and query path `search.rs:2519`); regression test `cluster_codes_zero_pad_sub_block_dimensions` added. |
| `src/wal/manifest.rs` `prepare_zero_copy_fork` | Built the fork target manifest with fragments, active segment, routing nodes — but not `coarse_payload_encodings`. Untagged = Sq8, so a two-bit foreign segment read through a branch view died with `"two-bit segment … is missing its two-bit coarse payload tag"` (500 on strong queries; 2 branching tests red). | **FIXED** — fork now carries the active segment's encoding tag. |
| `src/retrieval_scope.rs:307` | Scoped (mandatory-filter) flat search hardcoded `CoarsePayloadEncoding::Sq8`; a two-bit scoped artifact would hit the same loud tag error. | **FIXED** — derives via new `CoarsePayloadEncoding::for_quantization(index.quantization)`. |
| `src/index/ivf_flat/search.rs:1171` | Public `search_ivf_flat` wrapper hardcoded Sq8 (used by the `VectorIndex::search` trait impl). | **FIXED** — same `for_quantization` derivation. |
| hierarchical + two_bit | Rejected only at compaction time (`hierarchical/build.rs:749`, `hierarchical/search.rs:668`), not at config time; with two-bit the default, `hierarchical: true` passed namespace validation and then failed every compaction. | **FIXED** — `NamespaceIndexConfig::validate` (`src/namespace/manager.rs:246`) now rejects `hierarchical && quantization == two_bit` up front. |
| `src/index/ivf_flat/build.rs:5083-5100` (`load_ivf_flat`, legacy probing loader) | Probes PQ codebook → SQ calibration → SQ sidecar → falls through to `None`. No two-bit detection: a probed two-bit segment is mislabeled `None` and takes `scan_clusters_flat` (works, but skips the two-bit coarse scan and the handle lies about its quantization). Production query paths use manifest-aware loaders; reachable via `IvfFlatIndex::load`. | **OPEN** — needs a two-bit probe (row-layout/encoding detection). Not hit by the sweep. |
| `src/bin/recall_eval.rs:733-738, 2330-2335` | Hard-requires Scalar for the resolved config and the compacted segment (`verify_compacted_sq8_segment`). Refuses to run against the now-default two-bit path without `ZEPPELIN_QUANTIZATION=scalar`. | **OPEN** — decision needed: teach it two-bit or pin it to scalar explicitly. Note the pinned gate `tests/ivf_recall_gate.rs` does not exercise payloads, so rule-11 coverage of the two-bit *encoding* at production shape is still thin (see §5). |

## 2. Observability gap

- `src/index/ivf_flat/search.rs:1283-1284` — `SqSearchByteStats::new_if_enabled(matches!(index.quantization, Scalar))`; `scan_clusters_rq` takes no stats parameter. With `ZEPLIN_SQ_BYTE_STATS` set, per-query GET/byte diagnostics go dark under the default. **OPEN** (no RQ byte-stats instrumentation exists).

## 3. Test-suite assumptions (tests/ census)

Fixed in this session, verified by targeted runs + the sweep:

- `tests/two_bit_compaction_tests.rs:119` — `default_quantization_stays_scalar` inverted to `default_quantization_is_two_bit`.
- `src/compaction/mod.rs` test fixtures — `segment_for_config` attaches a rotation-seed sketch for two-bit configs; foreign-segment fixture keys the sketch under the source prefix; `segment_layout_match_uses_scale_aware_centroid_count` uses the helper.
- `tests/bitmap_tests.rs:33` — `bitmap_test_config_hierarchical` pins `quantization: Scalar` (two-bit requires flat IVF).
- `tests/hierarchical_tests.rs` — four config literals (`:24` helper, `:90`, `:453`, `:506`) pin `quantization: Scalar`; two already did (`:302`, `:363`).
- `tests/filter_underfill_tests.rs:251` — `test_hierarchical_sq8_filtered_query_fills_top_k` pins `quantization: Scalar` (server config derives the namespace default; the new config-time validation 400s hierarchical+two_bit at namespace create).
- `tests/compaction_tests.rs:636` `test_compact_with_existing_segment` — build + compactor pinned to Scalar (hand-built segment has no sketch seed).
- `tests/get_count_bench.rs:77` — `indexing_config()` pins Scalar; those tests pin the SQ8 GET profile by name.
- `tests/rq_scan_tests.rs:68` — fixture compactor pins Scalar; two-bit coverage comes from explicit RQ rewrite, as before.
- `contract/fixtures/v0.3.0/create_namespace.resp.json` — recorded default `"scalar"` → `"two_bit"` (what fixture regen produces for a create with no explicit quantization). `get_namespace.resp.json` deliberately **stays `"scalar"`**: the contract flow PATCHes `index_config.quantization` to scalar explicitly before the GET, so the recorded post-patch state is correct as-is.

## Pre-existing failures verified NOT flip-caused

Each reproduced identically on clean `93cb6c1` with all flip changes stashed:

- `tests/compaction_tests.rs`: `test_background_compaction_records_missing_active_manifest_failure`,
  `test_background_compaction_accepts_missing_manifest_while_deleting`,
  `test_background_discovery_resets_manifest_cache_across_remote_recreate_with_same_time`
  — all `Elapsed(())` timeouts; the first two also failed the 6acca95 sweep
  (g2-triage Group D) and were not touched by the 93cb6c1 repair commit.
  Pass in isolation; fail in-binary on this host with and without the flip.
- `perf_contract::contract::tests::security_budget_checker_rejects_p50_overrun`
  (`tests/perf_contract/contract.rs:1981`) — budget-checker unit test, fails
  on clean main. Unrelated to quantization.

Verified safe (explicitly Scalar-scoped, do not touch): all of
`tests/incremental_compaction_tests.rs`, `tests/two_bit_measurement_tests.rs`,
`tests/coalescing_gap_tests.rs`, `tests/perf_contract/` (see §4),
`tests/hierarchical_tests.rs:302`, `tests/branch_fork_tests.rs:731`,
`tests/artifact_origin_tests.rs:286`, `tests/attrs_laziness_tests.rs`,
`tests/adversarial/generator.rs:218-224`, `tests/common/` helpers (clean).

Watch items (parity-asserting, default-config servers; two-bit rerank is
exact-f32 so parity should hold): `tests/warm_parity_tests.rs:55`,
`tests/topk_parity_tests.rs:44`, `tests/batch_query_tests.rs:130`.

## 4. Perf contracts — structural finding

The suite **double-pins SQ8**: `tests/perf_contract/scenario.rs:1924`
(`scenario_config` hard-pins `QuantizationType::Scalar`) and
`tests/perf_contract/scenarios.rs:71-73` (maps every contract's `ns_config` —
present or absent — to `"scalar"` in the namespace create body). Consequences:

- G4 bands will **not** move from the flip. No rebaseline is owed.
- The perf suite no longer covers the default path at all. **OPEN decision**:
  (a) keep it pinned deliberately and fix the 16 stale "default SQ8 path"
  `why` notes (`cold_query_strong.toml:92,107`, `cold_query_sketch_adc.toml:94,108`,
  `gc_cycle.toml:87,103`, `hydration.toml:89,104`,
  `compaction_incremental.toml:91,105,131,135,145`, `compaction_cycle.toml:144`,
  `as_of_query.toml:91,96,107`, `delete_single.toml:139`,
  `upsert_single.toml:138`, `upsert_batch.toml:139`); or
  (b) add a two-bit contract variant — requires extending the
  `Quantization` enum in `tests/perf_contract/contract.rs:50-54`
  (`deny_unknown_fields`, only `Sq8` admitted) and capturing new bands.
- Ideal/what-if models are SQ8-only by explicit pin (`ideal/variant_query.rs:184,196,390,502`,
  `ideal/variant_compaction.rs:64,267,361,426`). Informational.

## 5. Recall-gate coverage note

`tests/ivf_recall_gate.rs:495` feeds `IndexingConfig::default()` but scores
exact f32 through `partition_vectors` — it does not read quantized payloads,
so it is blind to the flip. The 2026-07-29 gate run (green, bit-identical to
pinned) discharges rule 11 for the partition policy only. Two-bit *encoding*
recall at production shape rests on D5.1 (`results/storage-2bit.md`), which is
on record as near-vacuous (nprobe = all clusters). Combined with
`recall_eval` refusing the default path (§1), a production-shape two-bit
recall measurement is the honest remaining gap.

## 6. Stale docs / schema (mechanical, mostly still open)

- `api/zeppelin-api.yaml:2683-2686` — `QuantizationType` enum is
  `[none, scalar, product]`, **missing `two_bit`**; the schema cannot
  represent the new default. **OPEN** (add `two_bit`; also referenced at
  `:2696, 2718, 3373-3377`).
- `src/config.rs:1920` — field rustdoc still says "Default: Scalar (SQ8) for
  4x compression". **OPEN.**
- `src/config.rs:2412-2413` — `impl Default` doc says "Scalar quantization and
  bitmap indexes are enabled". **OPEN.**
- `src/compaction/CLAUDE.md:40-42` — "**Trap: SQ8 is the default
  quantization.**" Directly contradicted by the flip; actively misleading.
  **OPEN** (highest-priority doc fix).
- `src/index/CLAUDE.md:35` — already updated in this session.
- Historical/plan files left as record (per AGENTS.md, `tasks/` is plans and
  logs): `adversarial-perf-runner/full_plan.md:296`,
  `tasks/exploration_notes.md:146`, `tasks/todo.md:2662`,
  `tasks/execution_order.md:748,786`,
  `tasks/July10Quant/phase4/04-config-compaction.md:29`,
  `tasks/July10Quant/results/compact-bench/main.rs:57`.

## 7. Clean

- `demo/monitoring` — no quantization references.
- `scripts/` — no SQ8 assumptions (`perf-contract*.sh`,
  `overnight-adversarial.sh`, `build_wikidpr2m.py`, `test.sh`).
- `loom-tests/` — no quantization references.
- `benches/core_benchmarks.rs:76-119` — benches the SQ8 codec, which still
  exists; no baseline keyed to the default. Informational only.
- `fuzz/fuzz_targets/fuzz_quantization.rs:5-6` — fuzzes SQ8/PQ decoders only;
  no two-bit target (coverage gap, not breakage).
- Root `AGENTS.md` / `CLAUDE.md`, `tests/CLAUDE.md`, `README.md:21` — no
  SQ8-default claims.
