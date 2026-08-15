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

Set `"explain": "plan"` or `"explain": "full"` to inspect the executed
late-interaction source. Explain output records the higher-is-better score
direction, active profile and encoder epoch, FDE generation, selected manifest
generation, and actual consistency. Full mode also preserves per-result source
and fusion provenance after final truncation.

## Performance

Measured full-run latencies on loopback MinIO (Apple M3 Max, release build,
1,109 queries) are workload-specific operating points, not cloud-storage
latency claims:

| Profile | Matrix dtype | Candidate K | Gap | Recall@10 | p50 |
| --- | --- | ---: | ---: | ---: | ---: |
| General/default | f16 | 1,000 | 64 KiB | 0.980072 | 72 ms |
| Speed-biased | INT8-G32 | 1,000 | 512 KiB | 0.974662 | 48 ms |
| Quality-preserving | INT8-G32 | 1,200 | 768 KiB | 0.980703 | 53 ms |

The larger gap budgets are qualified only for the measured high-density shape.
On a sparse 50,000-unit replay, a 256-KiB gap removed only 7.4% of GETs while
adding 83% planned bytes, so the general default remains 64 KiB. See the
[tracked result table](evidence/late-interaction-performance.md) for wave
timings, request counts, byte amplification, and incremental-compaction
equivalence evidence.

Segment read-plan knobs (`[mmli.segment]` in `zeppelin.toml`) are measured
operating points — see the comments in
[`zeppelin.toml.example`](../zeppelin.toml.example) before changing them.
