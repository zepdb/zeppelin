# src/index — IVF-Flat, hierarchical, quantization

Trait-based (`VectorIndex`); IVF-Flat is the v1 production implementation.
This module never decides visibility — compaction publishes, query selects.

## The recall gate is the quality authority

`tests/ivf_recall_gate.rs` is the pinned authority for retrieval quality.
**Any change to flat partitioning or omitted-probe policy must run it against
both `wikidpr1m` and `wikidpr2m`:**

```bash
cargo test --release --test ivf_recall_gate -- --ignored --nocapture
```

Offline evaluators must call the production `partition_vectors` seam. Do not
carry an independent IVF implementation in a benchmark or evaluator — a
divergent copy silently invalidates every comparison it produces.

Current pinned numbers: recall@100 of 0.9688 (1M) / 0.9814 (2M).

## Flat IVF defaults are scale-aware and no-spill

- `nlist` resolves from a **3,000-row target**, clamped to 256–4096.
- An omitted `nprobe` resolves to **3/16 of the active flat segment's
  clusters**, with a runtime floor of 32.
- Scan budget is 0.20.

**Every logical row has exactly one stored location.** Do not add duplicate-row
spill or query-side dedup without a new measured plan — this was deliberately
re-pinned, not left unconsidered.

## Quantization

- Two-bit is the default (`default_quantization()` → `TwoBit`, flipped
  2026-07-29 after the G5 gate ran green on both pinned datasets); SQ8
  remains fully supported as a live configuration.
- Product quantization and f16 storage are available.
- RaBitQ / ZSK1: 1-bit retains only ~95% on e5-768 which is **insufficient**;
  2-bit reaches ≥99.3% and passes at nprobe 32. Do not re-propose 1-bit for
  768-dim embeddings without new evidence.

Known measured ceiling: nprobe 16 has a coarse recall ceiling of 0.85–0.93 on
`wiki_dpr_e5` at **all** configs and scales. That is a property of the data,
not a bug — it was investigated and no geometry error was found (≤0.005).
Mini-batch occupancy degenerates at high k.

## The resident sketch does not replace coarse payloads — measured

`ZBP5` (Phase 4 slices 9.1/9.2) makes a row's exact-vector offset pure
arithmetic from the manifest, which makes it *possible* to select the rerank
frontier from resident sketch row scores and skip cluster coarse reads
entirely. That was built and measured as slice 9.3 and **rejected**; the code
was removed. Numbers in `tasks/July10Quant/results/fixed-stride-f32.md`,
implementation recoverable from `212b689`.

- **Winner dispersion is near-total.** At production probe/frontier ratios a
  40-row frontier over 3,750 probed rows still touched 9 of the 10 grouped
  objects the coarse path touched: requests fell only 20 → 18. The bypass pays
  off only if winners concentrate into few objects, and they do not.
- **The two-bit sketch is a materially weaker selector than SQ8 coarse codes.**
  Same frontier, same probe set, same exact-f32 rerank: recall@10 1.00 → 0.80,
  recall@100 1.00 → 0.94.

Do not re-propose this on better row addressing. The constraint is selection
quality; a wider sketch is the only thing that would move it, and that is its
own slice with its own recall gate.

**What 9.1/9.2 still buy:** one fewer GET per grouped object touched per
query — the `ZBP4` directory read the manifest row layout replaced. Pinned by
`tests/get_count_bench.rs` (cluster GETs 6 → 4 at two objects); about 33% of
query GETs at production probe ratios. That stands without 9.3.

## Hierarchical search

Root beam search **must partition leaf vs internal children**. Hybrid root
nodes have mixed children, so the root-to-descent transition has to separate
them. Collapsing that distinction reintroduces a known bug.

## Distance

SIMD distance kernels live in `distance.rs`. `topk` owns bounded result
selection and score ordering shared by both implementations.

## See also

- `../compaction/CLAUDE.md` — who invokes the builders
- `tasks/` — recall investigations and quantization plans
