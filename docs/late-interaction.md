# Late-interaction retrieval

Zeppelin supports multi-vector late-interaction retrieval (MaxSim over
token-level embedding matrices) for namespaces created with the
`late_interaction_fde` index kind and an active embedding profile. Text is
enriched asynchronously into fixed-dimensional encodings for coarse
candidate generation, then reranked with exact MaxSim against the stored
token matrices — the "truth wave".

## Querying

Send query text through the retrieval algebra as a `late_interaction`
source:

```bash
curl -s http://localhost:8080/v1/namespaces/<ns>/query \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [{
      "type": "late_interaction",
      "text": "find the section about storage ownership",
      "top_k": 100,
      "semantic_wait_ms": 5000
    }],
    "consistency": "strong",
    "top_k": 10
  }' | jq
```

## Consistency and coverage semantics

Strong queries wait for every visible live record to have an exact semantic
overlay. If the `semantic_wait_ms` budget expires, Zeppelin returns
`SEMANTIC_INDEX_LAG` with `requested_generation`, `covered_sequence`,
`pending_records`, and `failed_records`. Eventual queries score only covered
live versions and suppress newer pending versions and tombstones. Their
response reports `"semantic_coverage": "complete"` when every live version
was covered and `"semantic_coverage": "partial"` when any live version was
omitted.

Late-interaction sources can participate in RRF fusion with dense and BM25
sources; weighted fusion is rejected because raw MaxSim is not calibrated
against dense or BM25 scores.

## Performance

Measured warm p50 latencies on loopback MinIO (Apple M3 Max, release
build), f16 token matrices, `candidate_k = 1000`:

| Corpus | Truth wave | End-to-end |
| --- | ---: | ---: |
| SciFact (5,183 docs) | ~42 ms | — |
| 50k-unit heavy-tail corpus (1,109 queries) | 59 ms | 102 ms |

The 2026-08 optimization ladder cut the 50k-corpus truth wave from 417 ms
to 59 ms (7x) via streamed range reads, f16 decode acceleration, and
parallel scoped scoring. The end-to-end figure uses the combined
streaming+scoring pipeline, which is bench-gated
(`MMLI_FLAT_BENCH_PIPELINE=1`); production defaults capture the read
concurrency and scoring-parallelism levers. The truth wave scales with
corpus size (round-trip- then K-bound), so smaller corpora are faster.
Per-phase attribution and run JSONs live in
[`tasks/LateLatency/results/`](../tasks/LateLatency/results/).

Segment read-plan knobs (`[mmli.segment]` in `zeppelin.toml`) are measured
operating points — see the comments in
[`zeppelin.toml.example`](../zeppelin.toml.example) before changing them.
