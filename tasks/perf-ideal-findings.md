# Ideal S3 performance findings

## Scope and evidence

This report analyzes one complete clean-revision pass of the dedicated
performance catalog. No adversarial runner code or artifacts participated.

- revision: `44af5aa429dec2d4f99e763baf4941518571e271`
- exhaustive artifact:
  `target/perf-contract/ideal-run-1783903074-871194000-86992`
- command:

      TEST_BACKEND=minio ZEPPELIN_PERF_IDEAL_SECONDS=1 \
        cargo test --release --test perf_contract_tests \
        ideal_analysis -- --ignored --nocapture

- result: 114/114 scenarios, 0 failures, 1 complete cycle, 5,426 ms
- artifact size: 2.4 MB
- repository state recorded by the run: clean
- ranking: serial GET depth, then GET count, then GET bytes, then scenario ID

The chain is the longest observable interval-ordered GET path. Overlapping
sibling GETs count toward totals but do not increase depth. The observer does
not possess semantic parent-span IDs and cannot see SDK/backend HTTP retries or
individual pages hidden beneath one recursive-LIST adapter invocation. Those
remain instrumentation gaps rather than inferred traffic.

The six leading suspicious paths were then sampled for 11 complete cycles:

`target/perf-contract/ideal-run-1783903103-499079000-87178`

Every path retained exactly one normalized cost vector. Calls, bytes, and depth
were identical in all 11 samples.

## Serial GET ranking

| Rank | Scenario | Depth | GETs | GET bytes | Verdict |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | `gc_cycle` | 60 | 60 | 34,734 | REDUCIBLE, contract-blocked |
| 2 | `compaction.fenced_incremental` | 12 | 12 | 7,665 | EXPLAINED with suspects |
| 3 | `compaction.flat_pq_incremental` | 10 | 10 | 28,884 | EXPLAINED with suspects |
| 4 | `compaction.direct_incremental` | 9 | 9 | 8,027 | EXPLAINED with suspects |
| 5 | `clone.timestamp` | 9 | 9 | 4,092 | REDUCIBLE |
| 6 | `background.tick_compaction_success` | 9 | 9 | 2,551 | EXPLAINED |
| 7 | `gc.orphan_sweep` | 9 | 9 | 835 | REDUCIBLE, contract-blocked |
| 8 | `gc.orphan_mark` | 9 | 9 | 558 | REDUCIBLE, contract-blocked |
| 9 | `query.hierarchical_pq_deep_filtered_bitmap` | 7 | 24 | 24,548 | REDUCIBLE |
| 10 | `query.as_of_timestamp` | 7 | 7 | 3,572 | same history-scan suspect |
| 11 | `compaction.layout_rewrite_no_wal` | 7 | 7 | 3,433 | EXPLAINED with suspects |
| 12 | `clone.snapshot` | 7 | 7 | 3,106 | EXPLAINED |

## Total GET bytes and calls

The highest-byte paths are data-plane queries, not metadata loops:

| Scenario | Depth | GETs | GET bytes | Dominant transferred work |
| --- | ---: | ---: | ---: | --- |
| `hybrid_query` | 5 | 10 | 2,051,956 | 1,879,459 cluster; 172,497 FTS |
| `filtered_query` | 4 | 14 | 1,235,383 | 1,188,784 cluster; 46,599 attrs |
| `filtered_query_bitmap` | 4 | 14 | 1,232,247 | 1,188,784 cluster; 43,463 bitmap |
| `paginate` | 6 | 10 | 860,238 | eight cluster reads |
| `cold_query_strong` | 6 | 10 | 730,219 | 429,303 cluster; 297,613 bootstrap |
| `cold_query_sketch_adc` | 6 | 14 | 537,604 | 299,694 bootstrap; 233,694 cluster |

By call count, the leading paths are `gc_cycle` (60), hierarchical existing
segment compaction (37), hierarchical PQ query (24), hierarchical FTS/PQ
compaction (20 each), and filtered flat PQ query (17). High counts are not
automatically high serial depth: the analyzer preserves parallel siblings.

## Finding 1: GC retained history is read three times

Classification: **REDUCIBLE**, but blocked by the frozen exact contract.

The 60-link chain contains the same 18 immutable
`<generation>.msgpack` objects three times:

1. retention pruning reads them in
   `Manifest::prune_history_with_retention_at`
   (`src/wal/manifest.rs:1696`);
2. GC immediately reconstructs the retained reachable-key set through
   `retained_manifest_history_reachable_keys`
   (`src/compaction/gc.rs:1333`);
3. the sweep phase freshly reconstructs the set before deletion
   (`src/compaction/gc.rs:1478`).

The prune function already returns the decoded retained manifests in
`ManifestHistoryPruneResult.retained_manifests`
(`src/wal/manifest.rs:590`). Reusing that result for step 2 is safe under the
single-writer-per-namespace rule. The fresh step-3 read must remain: it is the
pre-delete S3 authority check and detects newly published history/staging.

Correctness lower bound for this fixture:

- two complete retained-history snapshots: 36 generation GETs;
- three distinct authoritative `manifest.json` GETs;
- two distinct `lease.json` checks;
- one candidate-ledger GET.

That is 42 serial GETs. Reusing the prune result would remove exactly 18 GETs,
9,139 GET bytes, and one history LIST:

- depth: 60 -> 42;
- GET bytes: 34,734 -> 25,595;
- observed summed chain time in the representative sample: approximately
  18,419 us -> 12,950 us.

The change cannot currently be accepted. The frozen
`tests/perf_contract/contracts/gc_cycle.toml` requires the old exact depth,
GET, history-class, LIST, and byte values, while this task forbids rebaselining
the 18 contracts. The finding remains proven but unimplemented unless that
policy receives a narrow exception.

## Finding 2: timestamp history objects are unnecessarily serial

Classification: **REDUCIBLE** and eligible for a focused optimization.

`clone.timestamp` performs one live-manifest authority GET followed by four
immutable history GETs in a sequential timestamp scan. A fifth history GET
later validates the selected generation while pinning the clone:

- handler dispatch: `src/server/handlers/namespace.rs:1176`;
- timestamp resolution: `src/server/handlers/as_of.rs:254`;
- history LIST and per-object reads: `src/server/handlers/as_of.rs:256`;
- immutable history object read: `src/wal/manifest.rs:1560`.
- clone pin validation: `src/wal/manifest.rs:2089`.

The live manifest read is required because it caps stray history objects ahead
of the authoritative live generation. The history LIST and every retained
generation at or below that cap are also required: `updated_at` is deliberately
not assumed monotonic under clock skew. Existing restore and point-in-time
tests protect that selection rule.

After LIST, however, the four timestamp-scan history objects are independent
immutable keys. Their GETs have no data dependency and can be read concurrently
with a fixed bound. The later pin-validation GET is a distinct required
authority check and must remain serial. Errors must still fail loud, including
a listed object disappearing before its GET. Results must be deterministically
selected by generation after all reads.

Expected focused result with the same five-history fixture:

- total GET calls: unchanged at 9;
- GET bytes: unchanged at 4,092;
- history correctness: unchanged;
- serial GET depth: 9 -> 6;
- representative summed-chain estimate: 3,368 us -> approximately 2,348 us.

The same resolver serves `query.as_of_timestamp`, so its history stage should
receive the same reduction (depth 7 -> 4 in the complete pass). Generation and
snapshot selectors do not use this scan.

## Finding 3: incremental compaction repeats keys for distinct authority roles

Classification: **EXPLAINED**, with smaller ordering suspects.

The apparent repeated manifest, metadata, and lease keys do not represent
simple duplicate reads:

- the first manifest is the compaction input snapshot
  (`src/compaction/mod.rs:1093`);
- the final manifest is reread for fencing, concurrent-WAL preservation, and
  the conditional publication ETag (`src/compaction/mod.rs:1837`);
- lease acquire, post-PUT capture, and release each establish different
  ownership facts (`src/wal/lease.rs:276`, `src/wal/lease.rs:462`);
- background `should_compact` is advisory, so the compactor still needs a
  fresh authoritative manifest;
- the final metadata GET records health while preserving concurrent config or
  deletion changes.

Required artifact reads include WAL fragments, membership, centroids, coarse
sketch, touched clusters/attrs, and the existing PQ codebook where applicable.
They explain the call and byte totals.

Three serial-placement suspects remain, but none is yet a proven first change:

1. touched cluster and attrs objects have known keys and can potentially share
   a parallel stage;
2. centroids and coarse sketch may be prefetched together, although sketch
   decoding depends on centroids;
3. the existing PQ codebook is required but may be fetched earlier.

These should be isolated only after the timestamp-history optimization. Do not
remove either manifest read or weaken lease/metadata authority to save calls.

## Finding 4: orphan GC and deep hierarchical query have avoidable serialization

Classification: **REDUCIBLE** for both; orphan-GC changes are blocked by the
frozen `gc_cycle` contract.

Both `gc.orphan_mark` and `gc.orphan_sweep` contain nine serial GETs. Their
repeated keys straddle distinct prune, pending-drain, mark, and fresh-sweep
authority phases; the sweep case additionally reads the existing candidate
ledger. Like the final 18-history reread in `gc_cycle`, the last manifest,
lease, and history reads are required immediately before deletion. The tiny
byte totals (558 and 835 bytes) confirm this is round-trip authority work, not
over-fetch.

The independent inputs *within* a phase are currently serialized. Namespace
LIST, candidate-ledger GET, mark manifest GET, and staging lease GET at
`src/compaction/gc.rs:1371` can overlap. The sweep manifest and staging reads at
`src/compaction/gc.rs:1448` can also overlap while keeping the final history
read last. That projects both orphan scenarios from depth 9 to 6 without
changing calls or bytes. It would also change the frozen `gc_cycle` depth, so
the current exact-contract rule blocks implementation.

`query.hierarchical_pq_deep_filtered_bitmap` has 24 GETs but depth seven. Its
chain is live manifest -> tree metadata -> two hierarchical levels -> PQ
codebook -> attrs -> cluster. The remaining calls are parallel siblings at the
same tree/search stages. Each chain object has a distinct role and no repeated
immutable key was found. The fixed 16,396-byte PQ codebook currently waits
until after traversal (`src/index/hierarchical/search.rs:1113`) even though its
key and need are known from the segment's quantization metadata. Prefetching it
beside traversal and awaiting it only at PQ leaf scanning projects depth 7 to 6
with unchanged calls/bytes. This is eligible after the timestamp optimization.

Do not remove the four attrs reads: bitmap is only a prefilter, while exact
attributes remain the final filter authority (`hierarchical/search.rs:517` and
`:644`).

## Finding 5: hybrid lexical mapping fetches full exact-vector objects

Classification: **REDUCIBLE**, but blocked by the frozen exact contract.

`hybrid_query` transfers 2,051,956 GET bytes. Four full cluster-object GETs
account for 1,450,432 bytes (70.7% of the scenario). The BM25 path at
`src/query.rs:3160` loads complete packed cluster objects but retains only
`cluster.ids` to translate lexical positions to IDs. Those IDs are also present
in the compact SQ sections read by `src/index/quantization/sq.rs:700`.

For this fixture, reading only SQ sections would transfer approximately 331,840
bytes for the same four object arities, avoiding roughly 1,118,592 bytes (54.5%
of the scenario) without changing the number of objects. This is concrete
unnecessary payload, not a required authority reread.

Like GC, the path is a frozen guard: `hybrid_query.toml` pins its current exact
class and total bytes. Implementing the reduction would intentionally fail the
unchanged-contract gate, so it remains documented and unimplemented under the
current no-rebaseline rule.

The other highest-byte paths are not yet call-removal candidates:

- filtered query rerank ranges carry at least 888,184 bytes beyond the maximum
  exact vector payload needed for 40 rerank candidates, but this comes from the
  deliberate 1 MiB range-coalescing policy. It is **SUSPECT**, not proven net
  beneficial, until a gap sweep compares request count, bytes, and latency;
- the bitmap filtered path has the same rerank amplification. Its bitmap
  sidecars are **EXPLAINED** and save only 3,136 bytes versus attrs in this
  fixture;
- pagination deliberately re-executes the stateless, non-manifest-pinned query
  on page two. The repeated scan is **EXPLAINED**; its 139,759-byte rerank range
  remains the same coalescing suspect;
- cold query metadata, manifest, and bootstrap form the required authoritative
  cold-start prefix. Their rerank ranges are over-wide under a bytes-only lower
  bound, but changing them has the same request/byte tradeoff;
- sketch-ADC's bootstrap, headers, SQ sections, and selected grouped reads are
  **MINIMAL/EXPLAINED** for the current layout. Only its final coalesced rerank
  range remains **SUSPECT**.

## Optimization order

1. Add a RED perf regression for timestamp history sibling overlap while
   preserving exact GET count, bytes, and clock-skew selection.
2. Make the smallest bounded-concurrency change in the timestamp resolver.
3. Rerun focused correctness tests, both timestamp ideal scenarios, the full
   catalog, and all 18 frozen contracts.
4. Leave the larger GC call elimination and hybrid byte reduction documented
   but blocked by frozen exact contracts.
5. If timestamp validation is clean, add a separate RED test for hierarchical
   PQ codebook/traversal overlap before considering grouped cluster/attrs or
   rerank gap sensitivity.
