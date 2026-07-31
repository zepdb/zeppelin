# MMLI-2 Phase 7 — Exact Overlay Query

## Wire example

```json
{
  "sources": [
    {
      "type": "late_interaction",
      "text": "find the section about storage ownership",
      "top_k": 100,
      "semantic_wait_ms": 5000
    }
  ],
  "consistency": "strong",
  "top_k": 10,
  "explain": true
}
```

The response carries `semantic_coverage: "complete"` for a fully covered
snapshot. Late-interaction source metadata reports
`score_direction: "higher_is_better"` plus the profile, epoch, FDE generation,
manifest generation, and consistency actually used. Eventual reads return
`semantic_coverage: "partial"` when pending or failed live versions are
suppressed.

Late-interaction may be combined with BM25 or ANN through RRF. Weighted fusion
with a late-interaction source fails with
`LATE_INTERACTION_WEIGHTED_FUSION_UNSUPPORTED`.

## Measured lag error

The zero-budget strong-consistency fixture observed generation `5`, coverage
through source sequence `1`, one pending record, and no failed records:

```json
{
  "code": "SEMANTIC_INDEX_LAG",
  "error": "semantic index lag at requested generation 5: covered through sequence 1, 1 pending, 0 failed",
  "status": 503,
  "retryable": true,
  "requested_generation": 5,
  "covered_sequence": 1,
  "pending_records": 1,
  "failed_records": 0
}
```

## Dev-encoder measurement

The independently rerun MinIO parity fixture exhaustively searched `200`
records with `top_k=40` in `11.434 ms`. This is informational and is not a
performance contract.

## Validation

- Phase 7 MinIO query suite: `13` passed, `0` failed.
- Dense/BM25 fusion neutrality suite: `14` passed, `0` failed.
- Late-interaction OpenAPI contract: `1` passed, `0` failed.
- Library suite: `722` passed, `0` failed, `1` ignored real-model fixture.
- Clippy with all targets and warnings denied: PASS.
- Rustfmt and `git diff --check`: PASS.
