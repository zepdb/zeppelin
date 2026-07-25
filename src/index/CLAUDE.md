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

- SQ8 is the default (`default_quantization()` → `Scalar`).
- Product quantization and f16 storage are available.
- RaBitQ / ZSK1: 1-bit retains only ~95% on e5-768 which is **insufficient**;
  2-bit reaches ≥99.3% and passes at nprobe 32. Do not re-propose 1-bit for
  768-dim embeddings without new evidence.

Known measured ceiling: nprobe 16 has a coarse recall ceiling of 0.85–0.93 on
`wiki_dpr_e5` at **all** configs and scales. That is a property of the data,
not a bug — it was investigated and no geometry error was found (≤0.005).
Mini-batch occupancy degenerates at high k.

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
