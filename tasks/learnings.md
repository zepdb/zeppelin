# Zeppelin — Learnings

Distilled from `~/Documents/code/zep-temp/claude_files/claude_readme/learnings.md`
(the full 900-line bug narratives live there). This file is the LIVE version: append
new learnings as tasks complete. Rule first, context second, keep entries short.

## Serialization

1. **Never use bincode (or any non-self-describing format) with `#[serde(untagged)]` or
   `#[serde(skip_serializing_if)]`.** Check ALL nested types in the serialization tree,
   not just the top-level struct. (Bug 1)
2. **WAL fragments + Manifest use MessagePack** with version byte 0x01 prefix,
   auto-detect legacy JSON (starts with `{`). MessagePack is self-describing (safe with
   untagged) and 2-5x faster than JSON. (Finding 28)
3. **MessagePack encodes structs as arrays: new fields must be TRAILING +
   `#[serde(default)]`.** Write an explicit backward-compat decode test (old-shape bytes,
   both MsgPack and JSON) for every manifest/fragment schema change. (Task 1 precedent:
   `FragmentRef.size_bytes`)
4. **Never compute checksums from non-deterministic serialization.** HashMap iteration
   order isn't stable across round-trips — canonicalize via BTreeMap before hashing. (Bug 4)
5. **`#[serde(untagged)]` can lose f64 precision through JSON round-trips** (buffered
   deserialization drops LSBs at 15+ significant digits). (Bug 20)
6. **All `from_bytes()`/deserialization paths must be panic-free.** External data can be
   corrupt/truncated; every `try_into()`/slice op uses `?`, never `.unwrap()`. (Bug 45)

## S3 / storage / CAS

7. **S3 builder needs `.with_conditional_put(S3ConditionalPut::ETagMatch)`** — without it
   `put_opts` with `PutMode::Update` returns `NotImplemented` and CAS is silently broken. (Bug 8)
8. **Never check-then-act on shared state without CAS.** `exists()` → `put()` is a TOCTOU
   race (concurrent namespace create silently overwrites — Bug 36). Fencing check + CAS
   two-layer defense: neither alone suffices (TLA+ Bug 9).
9. **Multi-step deletion must be atomic or tombstone-first.** Delete-A-then-B lets a
   concurrent writer observe the gap and recreate A → zombie namespace (Bug 37).
10. **Lease release must be best-effort** (holder may have been taken over — never block,
    Bug 10) and **must not delete the lease object** (fencing token monotonicity lives in
    it — mark expired instead, Bug 11).
11. **Fixed-count CAS retry loops starve under sustained write load** (Bug 39). Use
    backoff+jitter / adaptive retries. (Compactor has backoff; writer path doesn't yet —
    todo Task 5.)
12. **Write-through caches must be version-aware** — a delayed write-through can overwrite
    an invalidation with stale data (Bug 38).
13. **`Ulid::new()` is NOT monotonic within a millisecond.** For watermarks use `max()`,
    never `last()`; for strict ordering use `ulid::Generator` or a sequence number. (Bug 40)
14. **When metadata already exists in the manifest, pass it through — don't re-derive from
    S3.** (`SegmentRef.vector_count`/`quantization`/`hierarchical`; Bug 23 cost ~19 GETs/query.)
15. **When adding `#[serde(default)]` fields to SegmentRef, explicitly set them in
    compaction** — a defaulted `hierarchical: false` silently mis-dispatched the query path
    to IVF-Flat on hierarchical segments → empty results. (Run-014 Bugs 1-3)

## Axum / HTTP

16. **Axum 0.7 uses `:param`, 0.8 uses `{param}`.** Parameterized routes 404 + static
    routes work ⇒ suspect syntax mismatch. (Bug 2)
17. **S3 keys and URL path segments have different rules** — `/` is legal in keys, fatal
    in `:param`. Separate helpers: `key()` for S3, `api_ns()` for URLs. (Bug 3)
18. **Body limits need BOTH `DefaultBodyLimit::max()` AND `RequestBodyLimitLayer`** —
    axum's Json extractor enforces its own 2MB default independently of tower-http. (Bug 12)
19. **Never `.unwrap()` in HTTP handlers** — return proper error responses. (Bug 24)
20. **Filter serde: tag is `"op"`, And-variant field is `"filters"` (not `"conditions"`).** (Bug 14)

## Query / index correctness

21. **Every new query path must apply ALL cross-cutting concerns to BOTH WAL and segment
    sides**: filters, deletes, consistency. BM25 shipped missing WAL post-filter (Bug 17)
    and missing tombstone exclusion in merge (Bug 19).
22. **Deletes are invisible in WAL results but must still exclude segment results.**
    Merge tracks: (a) WAL docs override segment, (b) WAL-deleted excluded from segment,
    (c) rest pass through. (Bug 19)
23. **Prefix search needs handling at every layer** — `last_as_prefix` tokenization alone
    isn't enough; matching must expand prefixes in both WAL and segment paths. (Bug 18)
24. **Mixed-child tree nodes: classify each child individually, never by the parent's
    `is_leaf` flag**, and never discard intermediate results in multi-level traversal
    (Bug 15). The root-to-descent transition needs the same partition logic as the
    descent loop — hybrid roots broke 6/8 production namespaces (Bug 41).
25. **Degenerate numerical inputs (all-zero distances, empty clusters, NaN) deserve
    errors, not panics.** (Bugs 46, 47)

## Config

26. **Use enums, not strings, for finite-value config fields** — fail at parse time. (Bug 25)
27. **Single source of env-override truth**: hardcoded defaults in `default_*()`, env
    reads ONLY in `apply_env_overrides()`. (Bug 26)
28. **Every config field must be wired to an implementation** — dead config creates false
    confidence (`max_concurrent_queries` was a no-op, Bug 27).

## Performance (validated by benchmarks)

29. **Hot-path serialization: MessagePack over JSON** (WAL deser was 30-63% of query CPU).
30. **Cache derived data keyed by immutable-artifact ID** — fragment ULID is a perfect
    cache key for tokenization results (WalFtsCache, Finding 29).
31. **Flat QPS across concurrency ⇒ suspect connection pool limits** (tune
    `pool_max_idle_per_host`, Finding 30).
32. **CPU-heavy background work gets its own tokio runtime** — compaction stole query CPU,
    read p99 > write p99 (Finding 31).
33. **Mini-batch k-means above ~10k vectors** — 3-5x speedup, negligible quality loss (Finding 32).
34. **Check preconditions before work** — skip the whole WAL scan when
    `uncompacted_fragments()` is empty (Finding 35).

## Testing

35. **`random_vectors()` is fixed-seed: identical IDs AND identical VALUES across calls.**
    Overlapping IDs break dedup tests (Bug 5); identical values cause distance ties and
    flaky ordering assertions (Task 3 run). Use unique ID prefixes AND distinct data pools
    (or hash the prefix into the seed, Bug 13).
36. **Keep `TempDir` alive for the lifetime of anything using its path.** (Bug 6)
37. **Unique temp filenames (`{file}.{uuid}.tmp`) for atomic writes under concurrency.** (Bug 7)
38. **Test against the real backend early** — local backends masked the CAS
    NotImplemented failure for weeks. (Bug 8)
39. **Property-test reference implementations must match the real data model's op
    ordering** (separate deletes/upserts fields ≠ interleaved ops, Bug 22); constrain
    generated keys to be distinct when testing order-independence (Bug 21).
40. **The `stop-words` crate is too aggressive** (1,298 words incl. "computer", "help") —
    use the hardcoded Lucene 36-word list. (Bug 16)

## Unwrap discipline

41. **The further an `.unwrap()` is from its safety guard, the more dangerous it is.**
    Guards and unwraps drift apart under refactoring. Convert Option→Result at point of
    use with `ok_or_else`; `#![deny(clippy::unwrap_used)]` kills the class. (Bugs 42-44,
    cross-cutting theme)

---

# New learnings (2026-07-02 onward — append as tasks complete)

## From the UX-hardening audit + Task runs

42. **Count-only thresholds never converge for quiet workloads.** Any "do X after N
    events" trigger needs a time-based companion or idle workloads stall forever
    (compaction: count + age-from-ULID + bytes; Task 1). Corollary: `count >= threshold`
    with threshold ≤ 0 fires on EMPTY inputs — guard the zero case explicitly.
43. **Fragment age comes free from the ULID timestamp** — `fragment_age_secs()` in
    `src/compaction/mod.rs`; don't add timestamp fields that ULIDs already encode. (Task 1)
44. **Index metadata (centroids/tree_meta) must ride the same cache as cluster data.**
    A raw `store.get()` on the query path = permanent per-query S3 RTT. Pattern:
    `get_or_fetch` + `pin_scoped(namespace, key)` (one pinned key per scope; pinning the
    new segment's key atomically unpins the old) + post-compaction background warm
    (`warm_segment_index_meta` in `src/compaction/background.rs`). Warming is the ONLY
    tolerated-failure path; queries stay fail-loud. (Task 3)
45. **GET-count assertions beat latency assertions in integration tests.** The counting
    ObjectStore decorator at `tests/common/counting.rs` wraps the harness backend and
    counts GETs per key pattern — deterministic where wall-clock is flaky. Reuse it for
    any "this path must not touch S3" invariant. (Task 3)
46. **The JSON→inf smuggling path is f64→f32 NARROWING, not overflow.** serde_json
    rejects literals beyond f64 range (`1e999` → "number out of range", 400). The real
    hole: literals inside f64 range but outside f32 range (`1e39`) silently narrow to
    `f32::INFINITY` with a 200. Validate `is_finite()` post-deserialization on every f32
    field; NaN is unreachable via JSON but can exist in pre-validation durable data —
    guard compaction/k-means separately. (Task 10)
47. **Compaction's non-finite skip is the ONE sanctioned degradation** — crashing
    compaction forever on one bad historical vector bricks the namespace. Skip with
    ERROR log (vector_id, dimension, value) + `zeppelin_non_finite_vectors_skipped_total`
    metric; such vectors stay Strong-visible via WAL until the compaction lands, then
    drop. (Task 10)
48. **Long lease-holding work needs heartbeat renewal + a pre-CAS "still held" check —
    fencing alone misses the theft window.** Confirmed live (Task 2A RED): a thief that
    holds the lease at token+1 but hasn't bumped the MANIFEST token yet lets the victim
    sail through the fencing check and zombie-commit. Pattern: spawned renew loop at
    ≤ lease_duration/3 through the EXISTING lease protocol (never a second mechanism),
    flipping an `Arc<AtomicBool>` on `LeaseExpired` or on transient errors persisting
    past last-confirmed expiry; check the flag before EVERY CAS attempt. Aborted work
    becomes orphans for GC, never partial commits. Entry point:
    `compact_namespace_under_lease()`; direct `compact()` calls get NO heartbeat. (Task 2A)
49. **One resolution helper for every per-artifact key when artifacts can be shared across
    generations.** Incremental compaction (Task 2B) carries untouched cluster objects
    forward under an OLDER segment's keys. The instant a per-cluster key can belong to a
    segment other than the one referencing it, EVERY key builder (search: flat/SQ/PQ/
    bitmap/attrs/prefetch; BM25; compaction's own segment reader) must route through a
    single `cluster_owner(i)` resolver — a missed site 404s live queries. Add the owner
    map as a trailing `#[serde(default)] Vec<String>` (empty ⇒ self-owned legacy), set it
    EXPLICITLY at every construction site (rule 15), and mirror it onto the in-memory
    index handle. (Task 2B)
50. **Segment-global derived artifacts must be REUSED (copied), not recomputed, on the
    incremental path.** SQ calibration / PQ codebook are computed over the whole segment;
    carried clusters' codes were encoded against the OLD calibration, and search reads the
    calibration under the NEW segment id. Recomputing silently corrupts every carried
    cluster's approximate distances. Copy old→new and re-encode only rewritten clusters.
    (Task 2B — SQ recompute was a latent bug the carry-over path would have hit; PQ already
    reused its codebook, so make them symmetric.)
51. **When an object can be shared across generations, "delete everything under the old
    prefix" becomes data loss.** Incremental carry-over means some old-segment objects are
    still referenced by the new segment. Deletion set = `list_prefix(old)` MINUS the exact
    per-cluster keys carried clusters still reference. Under-deletion leaks (GC's job, the
    SAFE direction); over-deletion destroys live data. Corollary for GC/prune (Task 19): an
    object is live if ANY live segment's `cluster_owners` references it — pruning a segment
    ref no longer implies its objects are dead. (Task 2B)
52. **Gate a clever fast path OFF where it can't yet be correct, don't half-implement it.**
    Task 2B disables per-cluster carry-over when FTS is configured (the FTS pass rebuilds a
    per-segment global index over ALL clusters, which a carried cluster's stale sidecar
    would break) — centroid reuse still applies. Correctness over cleverness; leave a clear
    TODO rather than ship a subtly-wrong carry-over. (Task 2B)
53. **Group commit removes a per-namespace-mutex write ceiling without weakening CAS.**
    The write ceiling wasn't the CAS — it was doing fragment PUT + manifest GET + CAS all
    INSIDE the per-namespace mutex, serializing ~3 S3 RTTs per append. Fix: PUT the
    fragment outside the critical section (parallel-safe), then elect ONE leader (via the
    commit mutex) to drain a pending queue and fold many appends' refs into a SINGLE
    manifest CAS. Waiters a prior leader already committed read their result from a oneshot
    (fast path) and still lead a round for stragglers so nobody is stranded. Only fold
    same-fencing-token appends into one CAS — mixing tokens is unsafe (under single-writer
    -per-namespace all prod appends are token-less, so this is the common path). Fencing +
    CAS semantics are untouched; the lease/TLA/invariant suites are the regression guard. (Task 5)
54. **A durable-object write that precedes a CAS must clean up on CAS failure — but ONLY
    its own exact key.** The writer PUT the fragment before the manifest CAS; a failed CAS
    orphaned it forever. Best-effort delete the fragment's exact `wal/<ulid>.wal` key on
    every terminal path (fail-loud otherwise). NEVER delete by prefix and never touch a
    segment/cluster object — after incremental compaction (Task 2B) a cluster object under
    an old segment's prefix may be LIVE (carried by reference). Under-deletion → Task 19 GC
    (safe); over-deletion → data loss. (Task 5, rules 51 + 54 are the same principle on
    two code paths.)
55. **Deleting redundant machinery is part of the task.** Once group commit lived in
    `WalWriter`, the opt-in `BatchWalWriter` was dead weight carrying its own bugs (cross-
    namespace HOL blocking, typed-error collapse). Delete it and its config knobs rather
    than leaving two write paths — dead config breeds false confidence (rule / CLAUDE.md
    #28). Keep the config STRUCT (empty, reserved) so old TOML still parses when there's no
    `deny_unknown_fields`. (Task 5)
56. **A leader-based commit loop must be driven by the LEADER's own completion, never the
    queue front.** Group commit deadlocked because the leader elected its batch from
    `pending[0]`'s fencing token; when the holder's own token differed it deferred its own
    ref, then awaited its own oneshot while holding the lock — no future leader could ever
    acquire it (Task 5 follow-up bug, bf44f34). Rule: the lock holder LOOPS committing
    rounds until ITS OWN result resolves, then releases — holding the lock across rounds so
    deferred work is always picked up by the same leader. Never assume "a subsequent leader
    will handle the deferred remainder" while the current leader still holds the lock — it
    won't, because it's blocked. Reproduce such races with a FAST backend (in-memory CAS)
    and many racers with distinct inputs; a real-S3 test hides them behind network latency
    (MinIO ran 3000 iters clean; in-memory hung at ~137). (Task 5)
57. **Client error codes must be an EXHAUSTIVE match, not a wildcard.** `error_code()`
    maps every `ZeppelinError` variant to a stable string with no `_ =>` arm, so a new
    variant fails to compile until it's assigned a code — the type system enforces the
    "no uncoded errors ship" invariant (a test additionally pins SCREAMING_SNAKE format).
    Same discipline as SegmentRef field-explicitness (rule 15). (Task 11)
58. **Split "not found": a namespace miss is a 404, an internal object miss is a 500.**
    A raw S3 `NotFound{key}` below the namespace layer (segment/cluster/fragment the
    manifest references but S3 can't return) is a data-integrity failure → 500
    INTERNAL_DATA_MISSING, NOT a client 404. Real namespace misses already translate to
    NamespaceNotFound upstream (NamespaceManager::get), so reclassifying the raw variant is
    safe. Never put the S3 key in the client body — `client_message()` returns generic text,
    the full Display goes to logs keyed by request_id. (Task 11 I2/I3)
59. **Normalize middleware/layer errors with an OUTERMOST map-response layer.** Tower layers
    (TimeoutLayer 408, RequestBodyLimitLayer 413) and axum's fallback emit bare/plain bodies
    that never hit a handler, so a per-handler error type can't envelope them. A final
    `normalize_error_responses` layer that rewrites owned error statuses whose body isn't
    already `application/json`, plus a `.fallback()` for unmatched routes, unifies them.
    Detect "already enveloped" via content-type, not by parsing the body. (Task 11 I4)
60. **request_id into error bodies without threading it everywhere: `tokio::task_local`.**
    The request_id middleware runs downstream inside `REQUEST_ID.scope(...)`; the error
    renderer reads it via `try_with` (returns None outside a scope, so the lightweight query
    route that skips the middleware just omits the field). Avoids adding a param to every
    handler + error path. (Task 11)
61. **Prefer a custom raw-bytes rejection over framework extractors for consistent errors.**
    axum's `Json<T>` extractor rejects malformed bodies with a 422 plain-text body that
    bypasses the error envelope. Switching upsert/delete to `bytes::Bytes` + `serde_json`
    (mapping parse errors to `Validation`) makes malformed bodies a 400 with the canonical
    envelope, matching the query handler. Update tests that pinned the old 422. (Task 11 I5)
62. **Post-filter AFTER truncation silently under-fills top_k on quantized paths.**
    IVF SQ8/PQ coarse scans rank by APPROXIMATE distance, truncate to fetch_k*4,
    THEN apply the attribute post-filter — so a selective filter whose matches
    rank low by approx distance loses them all before filtering. Fix: evaluate
    the (non-bitmap) filter DURING the coarse scan, before truncation (fetch
    attrs in the coarse prefetch when filter.is_some()). Keep the bitmap
    prefilter authoritative (I2) and skip attr fetch entirely when unfiltered
    (I3). This bug lives in EVERY quantized scan — ivf_flat AND hierarchical,
    SQ + PQ + flat-fallback branches; grep for `truncate(rerank_count)` to find
    them all. (Task 6)
63. **Perf/recall-shaped bugs need ADVERSARIAL fixtures, not random data.** A
    uniform-random dataset returns full top_k even against the buggy truncate-
    then-filter code, because the nearest-by-approx-distance window happens to
    contain enough matches — so a random-data test is a false green on broken
    code. Reproduce deterministically: place filter-MATCHING vectors FAR from
    the query behind a mass (≫ rerank_count) of NON-matching decoys NEAR it, so
    ranking pushes all matches past the truncation window (pre-fix → 0 results).
    Use euclidean (parallel uniform-value vectors tie under cosine), probe all
    clusters (no IVF recall confound), bitmap off (force the post-filter path).
    (Task 6)
64. **Tolerate a missing object only after re-reading the source of truth to
    confirm it's genuinely gone.** A WAL fragment NotFound could be a benign GC
    race OR real data loss — indistinguishable at the read site. Don't assume;
    VERIFY: on NotFound, re-read the manifest and skip ONLY if the fresh manifest
    no longer references that id; a still-referenced NotFound is an error. Gate
    the re-read behind the NotFound path so the normal path pays nothing, meter
    the tolerated skip. Everything else that a query CONSUMES fails loud (`?`);
    keep best-effort work (speculative prefetch) fire-and-forget so it never
    fails the query — the distinction is "does the result depend on this read?".
    (Task 9)
66. **The real cold-query GET cost is `3 + nprobe + 2R`, not "2 roundtrips".**
    (3 = conditional manifest + centroids + SQ calibration; nprobe = one SQ
    sidecar GET per probed cluster; R = distinct clusters among the top `4×top_k`
    rerank candidates, contributing a cluster GET + an attrs GET each.) Three
    structural wastes drive it: (a) attrs are fetched on the rerank/flat paths
    even when the query has NO filter (`load_attrs` ignores its `_filter` arg,
    search.rs:640) — but attrs also enrich `SearchResult.attributes`, so the fix
    is LAZY attrs (fetch after scoring, only for final-top-k clusters), not
    deletion; (b) the SQ8 sidecar (`sq_cluster_i.bin`) is a SEPARATE object from
    the cluster blob (`cluster_i.bin`), so coarse GETs the sidecar and rerank
    re-GETs the full cluster — co-locate them in one object so the coarse fetch
    warms the cache for rerank; (c) SQ calibration is its own GET — embed it in
    the centroids blob to kill a GET and a sequential phase. Warm = 1 GET; the
    multiplier is a COLD/eviction/post-compaction cost, so it needs a COLD-shape
    gate (warm bars are near-vacuous). Restate the thesis honestly: "2–3
    sequential round-trip *latencies*, N parallel GETs in the fan-out phase,"
    never "2 GETs". (Phase B baseline analysis)
67. **A perf loop with a single-dataset recall gate will cheat.** At the default
    nprobe=16/256-centroids, recall@10 is 0.9996 on one dataset shape (D1) but
    0.726/0.880 on others (D2/D3) — pure IVF probe truncation (nprobe=all → 1.0
    exactly, so scoring is fine). A latency/GET loop graded only on D1 can prune
    probes to "win" while silently destroying D2/D3 recall. Make the multi-dataset
    recall bar a HARD PRECONDITION in every executor verification prompt (D1 ≥0.95
    absolute AND D2/D3 ≥ frozen-baseline−ε), BEFORE any loop starts — the gate is
    the fix; the recall shortfall itself is a descent target (adaptive nprobe),
    not a pre-loop fix. (Phase B baseline analysis)
68. **A toy benchmark fixture silently excludes cost paths and pins worst cases.**
    H1's 4-vector×4-dim fixture (a) forced R=nprobe (overstating the GET multiplier
    — "~50 @ nprobe=16" is worst-case, ~25–35 typical), (b) used nprobe=all so the
    speculative prefetch (search.rs:109-128) NEVER FIRED — and that prefetch fetches
    the WRONG artifact class for the SQ8 default (full-precision cluster, not the sq
    sidecar the coarse phase reads) AND is an uncounted async GET that races
    `counter.reset()`, so a production-shape GET benchmark would be nondeterministic.
    Always add a PRODUCTION-SHAPE profile (real dims, nprobe < num_clusters) before
    freezing a cost baseline, and record R explicitly. (Phase B baseline analysis)
65. **A sandboxed executor can't self-verify network/integration tests — the
    orchestrator must close that loop.** Codex's workspace sandbox blocked MinIO
    (127.0.0.1:9000 PermissionDenied); it correctly refused to fall back to the
    memory backend and shipped code with lib/clippy green but RED/GREEN
    UNCONFIRMED. The two-loop model absorbs this: executor writes + self-checks
    what it can, orchestrator runs the real-backend RED→GREEN, a separate
    verifier adversarially probes the untested edges. Never merge an executor's
    integration-behavior claim without an out-of-sandbox run. (Task 9)
69. **A cache WRITE must never fail the operation that already has the data.**
    C.0-WAL routed WAL fragment reads through `DiskCache::get_or_fetch`, whose
    `put()` failure propagates via `?` — so a query that had ALREADY fetched the
    bytes from S3 would 500 if the cache write failed. Caching is an optimization:
    on a MISS, fetch (consumed read — propagate its failure) then populate the
    cache BEST-EFFORT (log on failure, return the bytes anyway). Applies to any
    read-through cache on a hot path (disk full / torn-down dir / perms must
    degrade to uncached-but-served, not error). ALSO: the bug was masked until a
    NEW code path used the cache — `start_test_server()` dropped its cache TempDir
    immediately (learnings rule 6 again), harmless until C.0-WAL made the query
    path write there. Two lessons: (a) best-effort cache writes; (b) the
    per-suite green in a worktree is NOT the merge gate — run the FULL
    `TEST_BACKEND=minio cargo test` on the merged tree, because a change can break
    a suite it never names (here: api_tests, via a shared harness flaw). (C.0-WAL)
70. **A high-nprobe recall sentinel must not apply the low-nprobe cluster budget.**
    np128 is only a boundary check if it scans the full probed cluster set; reducing
    128 probes to the same small sketch-selected cluster subset turns the sentinel
    into another approximate-recall benchmark. Keep low-nprobe budgets for quality
    measurement, but make sentinel nprobe values bypass cluster reduction so any
    recall loss points to scoring/rerank boundaries, not intentional cluster drops.
    (Cycle 3 sketch boundary repair)
71. **Do not encode recall sentinels as exact CLI-value branches.**
    A literal `if nprobe >= 128 { full scan }` is benchmark-coupled even if the
    sentinel result is correct. Make the cluster budget a smooth monotonic
    function of `effective_nprobe` so low-nprobe GET limits, mid-nprobe expansion,
    and high-nprobe no-op behavior all fall out of one policy. Pin the important
    points with tests (`np16` budget, monotonicity, np128 full probed set) instead
    of a production special case. (Cycle 4 sketch budget repair)
72. **For resident PQ sketches, code-byte parity is not quality parity.** Cycle 5's
    64x256 8-bit sketch kept a 64 B/vector code payload but underperformed the 4-bit
    96x16 Cycle 4 sketch at the 7-cluster budget (`np16 recall@10 0.9530`,
    `recall@100 0.9036`). Fewer subquantizers can lose more locality than larger
    sub-codebooks recover, and fixed codebook bytes matter when reporting full resident
    artifact size.
73. **A buddy cluster is not automatically a missing nprobe cluster.** Cycle 7's
    paired objects covered exactly 14 logical clusters at np16 within 10 GETs and
    improved recall@100 (`0.9032 -> 0.9307`), but still missed the 16-cluster
    baseline (`0.9469`). Pairing from centroid/corpus geometry helps coverage, but
    the free buddy clusters do not reliably match the remaining baseline probe
    clusters needed for top-100 recall.
74. **Bigger grouped IVF objects trade recall against bytes very quickly.** Cycle 8
    capped centroid-density groups restored adaptive object selection and reached
    np16 recall@10 with fewer GETs, but max=3 already consumed 96.8 MB/query and
    recall@100 only moved to `0.9318`. At 1M, where clusters are much larger,
    larger physical objects need range reads, size-aware selection, or finer
    physical granularity; object grouping alone does not close the top-100 gap.
75. **Raw sketch mass is not a safe replacement for nearest-row distance.** Cycle 9
    counted each cluster's rows in the query-local global top-100 sketch ADC
    scores. Mass-primary object selection hurt np16 recall@10 badly
    (`0.9485`) even with the top-2 distance tie-break, while distance-gated mass
    extras only raised recall@100 to `0.9338`. Keep a distance-ranked core and
    use mass, if at all, only inside a distance/byte guard until the object
    layout becomes finer or size-aware.
76. **A calibrated lower bound must match the sketch error sign and scale.** Cycle
    11's serving test used `sketch_score - slack < kth_exact`, so the safe corpus
    residual is the sketch over-estimate, `sketch_score - exact_score`. Screening
    the opposite residual from the prompt (`exact_score - sketch_score`) skipped
    every second-wave object at np16 and collapsed recall (`r@10 0.9170`,
    `r@100 0.8538`). Before trusting a calibrated bound, verify whether the
    sketch usually over-estimates or under-estimates the exact metric and screen
    the unsafe sign explicitly.
77. **Candidate-vector range reads can win bytes while losing the GET fence.** Cycle
    2 F1's v4 SQ-first layout cut np16 bytes from 88.4 MB/q to 49.5 MB/q and kept
    recall parity, but exact rerank via `get_ranges` fanned out to 19.5 GETs/q.
    Coarser one-range-per-object rerank brought GETs below 9.5 only by either
    broadening bytes above 53 MB/q or shrinking object coverage enough to fail
    recall. Future F1 work needs a physical layout or request strategy that
    reduces f32 rerank requests without expanding candidate spans.
78. **Do not assume manual range coalescing beats the store's physical plan.**
    Cycle 3 explicitly coalesced f32 rerank ranges with a 1 MiB gap threshold
    before calling `get_ranges`, then sliced the same logical candidate vectors
    back out. Recall and bytes stayed identical, but GETs stayed at 19.5/q:
    the object-store path was already producing the same physical spans at that
    scale. Once remaining gaps are >= 1 MiB, removing ~10 more GETs costs at
    least ~10 MB/query and violates a 53 MB/q fence. Before spending another
    cycle here, measure the gap histogram or change the physical layout.
79. **Weak runtime-worker CPU samples are not enough to explain the 410 MB/s wall.**
    Cycle 4 moved `scan_clusters_flat` full-cluster decode and exact scan into
    `spawn_blocking` after prefetch. Fences stayed green, but np16 QPS only moved
    from 4.63 to 4.70 and aggregate throughput from 409 MB/s to 416 MB/s. Treat
    this as a failed CPU-offload explanation; next F3 work should measure the
    transport path directly before adding more blocking-task reshuffles.
80. **Background tasks spawned from `&self` need owned shared state, not borrowed
    cache handles.** Tokio tasks require `'static` captures, so async cache eviction
    cannot borrow `DiskCache` directly from `put()`. Put the maps/counters/guards the
    worker needs behind `Arc`s, spawn with those owned handles, and keep the public
    cache API unchanged. A current-thread test can prove eviction is not inline by
    asserting the over-capacity size before yielding to the spawned task. (Task 15)
81. **Active segment metadata tests must follow the manifest artifact, not a legacy
    filename.** Current IVF segments can load centroids/sketch from `bootstrap.bin`;
    legacy segments load `centroids.bin` plus sidecars. Query-path cache tests should
    derive the metadata key from `SegmentRef.bootstrap` with a centroids fallback, or
    they assert the wrong S3 object and miss fail-loud sabotage on bootstrap-backed
    segments. (Task 15)
82. **BM25 global-index hits still need manifest-aware cluster id resolution.** Global
    FTS returns `(cluster_idx, position)` pairs, but current compacted segments may store
    vectors in grouped `cluster_group_*.bin` objects instead of legacy `cluster_{i}.bin`
    files. Any BM25 path that resolves positions to ids must route through
    `SegmentRef.cluster_objects` when present, with `cluster_owner(i)` as the legacy
    fallback; otherwise compacted BM25 queries can return no ids even though global FTS
    found matches. (Task 26)
83. **Top-level namespace discovery tests must clean their own stale top-level fixtures.**
    `NamespaceManager::list(None)` sees every top-level `meta.json` in the shared MinIO
    bucket, not just the current harness prefix. Tests that intentionally use top-level
    names so a background loop can discover them must remove stale names with their exact
    suffix before starting the loop; otherwise an interrupted run can leave old fixtures
    that the next loop tries to process before the current namespace. (Task 26)
84. **`cluster_objects` can reference legacy singleton cluster files, not only grouped objects.**
    FTS incremental rebuilds disable carry-over and rewrite every cluster, but still
    populate `SegmentRef.cluster_objects` with `cluster_{i}.bin` singleton references.
    Query code that treats non-empty `cluster_objects` as exclusively grouped
    `cluster_group_*.bin` objects will skip all ids on the BM25 global path. Detect
    grouped object headers, and decode legacy singleton references with
    `deserialize_cluster` when the manifest entry names exactly one cluster. (Task 26)
85. **Do not bound per-field FTS postings before `rank_by` aggregation.** Segment/global
    FTS helpers return all per-field matches so multi-field `rank_by` expressions can
    combine scores correctly. Apply bounded top-k only after the final document score is
    known; for helper methods without a semantic `top_k`, route through the shared
    comparator with `k = len` or keep a full total-order sort. (Task 27)
86. **Process-global decoded artifact caches can hide S3 GETs from counting-store
    guards.** If a test intentionally measures object-store GETs without a disk cache,
    do not satisfy the read from a process-global decoded cache; otherwise repeated
    queries bypass the instrumented store and the frozen GET-count profile becomes
    order-dependent. (Task 27)
87. **Grouped cluster fetch planning needs both logical and physical cluster sets.**
    Query scoring should visit only the selected logical clusters, but grouped object
    decoding must validate against every physical cluster stored in the object. Expanding
    an `nprobe` selection to all clusters in a grouped object changes result candidates
    and frozen S3 GET-count expectations. (Task 27)
88. **Decoded bootstrap reuse must respect the caller's cache contract.** A process-wide
    decoded bootstrap cache is correct when an explicit `DiskCache` is supplied, but
    cacheless query paths still need to fetch the source-of-truth S3 bootstrap so
    GET-count tests and cache-disabled behavior remain honest. (Task 27)
89. **ANN tie-breaks can affect physical read cost, not just presentation order.** Using
    id tie-breaks during intermediate candidate cut-down can spread equal-distance
    candidates across different clusters and change the object read plan. Current
    bounded top-k selection is explicitly unstable under equal comparator values, so
    deterministic behavior must come from the comparator itself; there is no hidden
    scan-order exception. (Task 27)
90. **Shared MinIO cruft can invalidate full-suite signal.** `NamespaceManager::list(None)`
    uses a non-delimited recursive list over the whole bucket, so hundreds of stale
    top-level prefixes make every background compaction tick walk unrelated fragments,
    segments, and clusters. Sweep the shared test bucket before claiming a monolithic
    MinIO regression signal, or make namespace discovery prefix/delimiter scoped.
    (WR3 Phase 1 validation)
91. **Validation gates must follow manifest artifact layout, not legacy filenames.**
    `recall_eval` still verifies SQ8 by checking `sq_cluster_0.bin` or singleton
    `cluster_0.bin`, but current full compactions can store SQ sections in manifest
    `cluster_objects` such as `cluster_group_*.bin`. Gate code should validate through
    `SegmentRef.cluster_objects` before treating missing legacy keys as a recall-path
    failure. (WR3 Phase 1 validation)
92. **Background compaction discovery can stay S3-authoritative without recursive lists.**
    Per-tick compaction discovery should use delimiter namespace listing, not registry-only
    state and not recursive `list_prefix("")`. This preserves cross-node discovery while
    avoiding a walk through every WAL, segment, cluster, and attrs object in the bucket.
    (Task 12 partial)
93. **Namespace discovery must use delimiter semantics.** A recursive search for keys
    ending in `/meta.json` can mistake nested artifacts such as
    `segments/.../meta.json` for real namespaces. Delimiter listing immediate child
    prefixes keeps namespace discovery O(namespace count) and prevents nested metadata
    from becoming a synthetic namespace. (Task 12 partial)
94. **Post-compaction segment assertions need strong reads when compaction is out-of-band.**
    API tests that call a `Compactor` directly can leave the server's manifest cache
    with a still-fresh pre-compaction snapshot. Use strong consistency when the test is
    pinning segment-path behavior immediately after direct compaction; eventual reads
    are allowed to see the cached snapshot. (Task 20)
95. **Grouped-object recall depends on preserving manifest object membership.**
    Helpers that coalesce physical cluster-object GETs must keep each object's full
    `ClusterDataObjectRef.clusters` list. If they rewrite that list to only requested
    clusters, later expansion cannot scan sibling clusters that are free once the
    grouped object is fetched. (WR3 Phase 1 review fixup)
96. **Frozen GET-count benches need byte counters for ranged-read tradeoffs.**
    A change can reduce operation count by fetching full grouped objects, while
    increasing transported bytes enough to hurt the real bottleneck. When grouped
    SQ paths change, record both `ArtifactClass::Cluster` ops and bytes before
    accepting a new frozen profile. (WR3 Phase 1 review fixup)
97. **Background worker state flags need unwind-safe reset and retry pacing.**
    A boolean "worker running" guard must be reset by a drop guard or equivalent,
    not only at the happy-path end of an async task. Persistent filesystem errors
    also need bounded retries/backoff; otherwise the worker can alternate
    `false -> true` immediately and spin at full duty cycle. (Task 15)
98. **Strong WAL override sets must be built before filter/top-k/scoring cuts.**
    Segment suppression is a freshness rule, not a result-list side effect. If a
    WAL update filters out, scores zero, or ranks outside the bounded keeper, the
    stale compacted segment version still must not be admitted. Carry a separate
    live-WAL-id set from the dedup pass and merge against that set, not against
    returned WAL results. (Task 28)
99. **Concurrent MinIO compaction suites can hide order-dependent cache assertions.**
    `test_compaction_warms_new_segment_centroids` can fail in the full parallel
    `TEST_BACKEND=minio cargo test` run while passing alone and with
    `--test-threads=1`. Treat a monolithic MinIO failure at this assertion as an
    order/concurrency signal to isolate before attributing it to unrelated query
    changes. (Task 28 validation)
100. **Freshness-property oracles should not fork platform-sensitive distance math.**
    Keep the replay/materialization logic independent, but use the public distance
    kernel for score calculation when the behavior under test is freshness. Separate
    scalar score loops can differ by tiny SIMD reduction roundoff and create false
    failures unrelated to WAL ordering, delete, filter, or override semantics.
    (Task 28 review follow-up)
101. **Namespace-name prefixes are not directory prefixes.** Test namespace names
    such as `test-<uuid>-warm` are top-level S3 path components, not keys under
    `test-<uuid>/`. Some object_store backends do not return delimiter
    `common_prefixes` for a partial component prefix, so `NamespaceManager::list`
    must apply namespace-name prefix filtering before loading metadata rather
    than assuming the storage delimiter call can express that partial component
    portably. (Task 28 review follow-up)
102. **Baseline full-suite blockers before attributing them to the active task.**
    The Task 12 full MinIO run exposed failing incremental-compaction carry-over
    tests, but a clean worktree at pre-change HEAD `0f7ae7b` reproduced the same
    missing carried seed cluster object. When a monolithic regression sweep fails
    outside the touched lifecycle surface, prove whether it is pre-existing before
    folding an unrelated repair into the current task. (Task 12 validation)
103. **DashMap entry counters must distinguish first insert from update.**
    `entry().or_insert_with(...)` followed by shared increment logic can count the
    first observation twice. Use explicit `Entry::Vacant` / `Entry::Occupied` branches
    for heat windows and similar counters so RED tests catch the initial state.
    (new-cache Task 3)
104. **Library-level benchmarks bypass server-side background workers.**
    A benchmark that calls `execute_query` directly will not exercise HTTP-layer
    notification hooks or server-owned workers such as `SegmentHydrator`; pass the
    same dependencies explicitly or use an API benchmark before claiming warmed-server
    behavior. (new-cache Task 4)
105. **Process-global metrics are unsafe for per-test deltas in parallel integration tests.**
    Prometheus counters can be incremented by sibling tests in the same binary, so
    assertions should use per-test cache/store state or isolate execution instead of
    comparing global metric before/after values for labels shared across tests.
    (new-cache Task 5)
106. **Incremental compaction must keep `cluster_objects` byte format homogeneous.**
    If an old active segment uses manifest `cluster_objects`, rewritten clusters must
    be serialized as grouped cluster-data objects too; advertising raw `cluster_i.bin`
    bytes through `ClusterDataObjectRef` makes later loaders parse raw cluster bytes as
    grouped data. (new-cache Task 8)
107. **Fault-injection tests must delete the manifest-advertised cluster object.**
    Current full compactions may store cluster data under `cluster_group_*.bin`, so
    tests that construct legacy `cluster_{i}.bin` keys can delete a non-consumed path
    and falsely prove a query should have failed. Resolve the physical key through
    `SegmentRef.cluster_objects` first. (new-cache Task 8)
108. **Closed loopback S3 endpoints need a pre-object_store reachability check.**
    A dead MinIO endpoint can stall below an async timeout because DNS/connect work
    happens inside the object_store client path. For explicit loopback endpoints,
    check whether the port is actually listening before constructing/probing the
    object_store client; then use the real list/head probe for reachable endpoints.
    (Task 18)
109. **GC key parsers must fail closed when reachability is not exact.**
    Hierarchical `node_*.bin` objects are discovered through `tree_meta.json`, not
    directly represented in the manifest reachability union today. A storage GC
    parser must treat such shapes as unknown unless the exact reachable set is
    extended first; under-deletion is a leak, but over-deletion is data loss.
    (Task 19D)
110. **Retained-history reachability has a destructive-sweep phase boundary.**
     A GC cycle can reuse the post-prune retained-history union for
     pending-delete drain and mark, but sweep must re-read retained history
     before deleting. A retained generation can appear between mark and sweep;
     treating mark-time history as authoritative can delete a PITR-protected
     object. Counting `{namespace}/manifests/` GETs should catch accidental
     excess reads, not forbid the required sweep revalidation.
     (gc-history-reachability-cost, tla-storage-gc-safety)
111. **Write manifest history before flipping the live pointer.** The live
     manifest object is the commit record; if it advances before the immutable
     generation snapshot is durable, a history PUT failure creates a permanent
     PITR gap and makes retries conflict on stale ETags. Under the single-writer
     lease, a prewritten snapshot whose CAS later conflicts is a conservative
     orphan, not a data-loss path. (manifest-history-write-atomicity)
112. **A pre-CAS history snapshot can be an overwriteable orphan.** If
     `history[N]` is written but the live pointer PUT/CAS fails transiently,
     live still references `N-1`. A later retry at `N` may serialize different
     bytes, so treat the existing mismatched `history[N]` as overwriteable only
     when `live.version() < N`; if live reached `N`, preserve immutability and
     surface the conflict/error. (manifest-history-pointer-failure-wedge)
113. **Pending-delete drain age must come from the artifact key ULID, not the
     manifest update clock.** Busy namespaces rewrite `manifest.updated_at` on
     every compaction/GC pass, so using it as a retention lower bound can
     permanently block pending-delete pruning. WAL keys and `seg_<ulid>`
     segment artifact keys already carry the artifact creation time; unknown
     shapes still fail closed. (gc-pending-delete-horizon-clock)
114. **Manual long-running triggers should acquire synchronously, then spawn.**
     Keep cheap no-op checks inline; for real work, acquire the single-writer
     lease in the HTTP handler so `LeaseHeld` returns `409`, then move only the
     post-acquire heartbeat/work/release body into the background task and return
     the manifest generation clients should poll past. (compaction-trigger-sync-blocking)
115. **Use `localhost`, not numeric loopback, for Docker-published MinIO endpoints
     when exercising startup probes.** Docker Desktop can publish a port through
     an IPv6 wildcard listener; the engine's explicit numeric-loopback preflight
     may treat `127.0.0.1:<port>` as closed because a bind probe succeeds, while
     `localhost:<port>` uses the normal TCP connect path and reaches the same
     MinIO service. (client-e2e-conformance)
116. **Retained manifest history includes `pending_deletes` as reachability
     roots.** A history snapshot written while a key is queued in
     `pending_deletes` will protect that key from the pending-delete drain just
     like a segment or WAL ref. Tests for "unrelated pins do not retain dead
     pending deletes" must ensure no retained history generation itself carries
     the pending key, otherwise GC is correct to preserve it. (pitr-retention-gc)
117. **PITR reads must cap history by the live manifest generation.** The
     history object is written before the live pointer CAS, so `history[N]` can
     exist while live is still `N-1`. `as_of` generation/timestamp/clone
     resolution must treat `history[N]` as invisible until the live manifest
     reaches `N`; otherwise clients can observe an uncommitted, overwriteable
     orphan snapshot. (tla-pitr-history-retention)
118. **Fable features turn GC from single-head reachability into root-set
     reachability.** Branch heads, `branch_pending` markers, shadow manifests
     and staging, overlay mounts, batch-pinned manifest ETags, epoch retire
     pins, published images, and governed-forgetting lineage can all protect
     keys outside the current namespace head. Before any long-lived fable root
     ships, GC needs a concrete root registry/enumerator; until then, TLA+
     should model them as abstract `futureRoots` and prove sweep checks that
     set before deleting. (fable_options, tla-storage-gc-safety)
119. **Quiescence must await background maintenance, not sleep.** A shutdown
     signal plus a fixed delay can leave a background compaction mid-CAS while
     final adversarial checks run in-process compaction and compare HTTP status
     to direct S3 reads. Retain the background task handle and await it before
     final quiescence; invalidate manifest cache after any test-only direct
     maintenance path that bypasses HTTP handlers. (adversarial-quiescence-race)
120. **Bound integration-test linker artifacts on constrained CI runners.**
     Rust 1.97's `rust-lld` can terminate with `SIGBUS` while linking dozens of
     full-debuginfo test binaries; both MinIO integration and instrumented
     coverage runs hit the same crash across repeated runs even with Cargo and
     LLD restricted to one worker. Keep `-j 1` and `--threads=1`, and disable
     test-profile debuginfo in those CI jobs to bound mmap/backing-file usage
     without changing assertions or coverage instrumentation.
     (ci-integration-linker-resources)
121. **High-dimensional squared-L2 spill ratios can duplicate almost every
     row.** On normalized e5-768 data, the declared top-2 threshold
     `d2 <= 1.2 * d1` spilled about 90% of logical rows even after scaled
     mini-batches repaired occupancy. Sweep storage inflation before wiring a
     spill threshold; values that sound selective in low dimensions can
     violate storage and scan gates immediately. (FixIVFFlat Phase 0)
122. **A split-largest balance cap needs a convergence result, not only a
     round limit.** The Phase 0 dbpedia100k run split one overfull cluster in
     every declared round but still finished above the 4x-mean occupancy cap.
     Report the final ratio and treat exhausted rounds as non-convergence;
     never equate "repair ran" with "balance target holds."
     (FixIVFFlat Phase 0)
123. **Compare sketch recall at the same probe frontier.** A high-nprobe
     sentinel changes both sketch pruning and coarse IVF coverage, so its
     recall delta from the default cannot be attributed to the sketch alone.
     First prove whether the adaptive budget prunes the default probe set; if
     it is a structural no-op, record the wider-frontier sentinel as a cost and
     recall trade-off rather than silently tuning sketch constants.
     (FixIVFFlat Phase D)
124. **Exact MessagePack byte comparisons must guard integer-width
     boundaries.** Resetting a clone generation from 128 or higher to zero
     changes the encoded integer width, so a cold-clone contract that compares
     exact manifest bytes must reject that setup explicitly before publication.
     Use the domain reset method instead of rewriting private state through a
     JSON round-trip. (perf-contract cold clone)
125. **Resolve generated reports inside invocation-scoped artifact roots.** A
     global newest-report search can copy a later advisory tier or stale output
     from an earlier run. Give each entrypoint a unique root, require exactly
     one report there, and retain the gating tier before later tiers execute.
     (perf-contract driver)
126. **Ignore only storage-backed performance runner entrypoints.** Pure,
     fixture-backed, tempdir, and InMemory module selftests should run in the
     default integration-test command so their deterministic invariants stay
     covered without requiring MinIO. (perf-contract selftests)
127. **Large decoded-artifact registries must not outlive their owning cache.**
     A process-wide map of strong `Arc` values defeats DiskCache eviction and
     retains every resident-sketch generation indefinitely. Store `Weak`
     references for cross-cache reuse, validate the current manifest ref after
     upgrade, and let the bounded cache remain the allocation owner.
     (ZSK1 v4 resident sketch)
128. **Separate format incompatibility from artifact corruption before
     rebuilding.** A valid v3 sketch can explicitly request a one-time v4
     rebuild, but a referenced missing object, malformed v4 header, or
     manifest/object mismatch must propagate as a typed error. Collapsing both
     states into one "stitch unavailable" path silently repairs corruption and
     violates fail-loud storage semantics. (ZSK1 v4 compaction)
129. **Resident sketch storage is paid twice while bootstrap embeds it.** New
     v4 rows cost 200 bytes at 768 dimensions, and Zeppelin currently writes
     the sketch both standalone and inside `bootstrap.bin`: about 400 MB per
     1M rows before cluster data. Monitor MinIO and target growth on reruns,
     release the old sketch before building the bootstrap copy, and do not use
     `cargo clean` as incidental benchmark cleanup. (ZSK1 v4 operations)
130. **A simple MinIO upload smoke does not validate Docker Desktop's gVisor
     path for compaction.** A 512 MiB multipart upload/delete can pass, then the
     real compactor can fail on `cluster_group_1.bin` after Docker's data
     multiplexer goes offline and `virtio_net` reports TX watchdog timeouts.
     Reproduce with both control and candidate before blaming a format change,
     preserve the exact namespace, and verify cleanup at `KeyCount: 0`.
     (ZSK1 v4 D2 measurement)

52. **1M local benches exhaust macOS's ~1GB kernel mbuf pool.** Compaction's
    unbounded `join_all` WAL read (src/wal/reader.rs:284) puts ~3GB in
    flight (1000 x 3MB GETs); macOS (`kern.ipc.nmbclusters=262144`) denies
    socket-buffer memory and sheds connections with a clean FIN — the
    client sees `error decoding response body` (hyper UnexpectedEof /
    IncompleteBody), and MinIO's trace shows only 200 OKs. Loopback TCP
    bypasses classic netstat counters; the tells are `netstat -m`
    "requests for memory denied" deltas and a concurrency-matrix repro
    (tasks/July10Quant/results/compact-bench/diag-mbuf/). The same
    pressure killed Docker's gVisor transport. Mitigation for bench
    windows: `sudo sysctl -w net.inet.tcp.autorcvbufmax=262144
    net.inet.tcp.autosndbufmax=262144` (defaults 4194304), restore after.
    nginx buffering only halves the damage; MinIO `requests_max` hard-429s
    instead of queueing on 2025 releases. (ZSK1 v4 D2 measurement)

53. **object_store's retry window makes burst failures terminal.** With
    `RetryConfig { max_retries: 2, retry_timeout: 2s }` (src/storage/
    store.rs), any request that has been queued >2s behind a large
    concurrent burst gets zero retries when its connection breaks — a
    transient transport hiccup becomes a hard compaction failure. Keep in
    mind before blaming new formats: reproduce with the v3 control first.
    (ZSK1 v4 D2 measurement)

54. **Full-precision storage does not guarantee an exact ANN oracle when the
    query planner prunes clusters.** A `quantization = none` namespace at
    nprobe 16 still uses the resident-sketch adaptive cap, so both scripted
    queries and final adversarial quiescence must use Membership semantics.
    Classifying solely from stored-vector quantization creates false I1
    violations after an otherwise-correct ADC run. (ZSK1 v4 runner coverage)

55. **A non-conflict manifest CAS error does not prove the CAS failed.** The
    object store can commit the live pointer and lose only its acknowledgement.
    Re-read the authoritative manifest before cleanup: recover success when all
    batched refs are present, delete only when none are present, and preserve
    every object on a failed, missing, or partial reread so reachability GC can
    decide safely. (Antithesis post-commit manifest ambiguity)

56. **Spawned tasks do not prove overlap on Tokio's current-thread runtime.**
    Optimized in-memory object-store futures can remain ready through a whole
    append, letting each spawned task finish before the next is polled. Tests
    whose invariant requires a populated concurrent queue must use a
    multi-thread runtime (or an explicit seam-local rendezvous) instead of
    treating `tokio::spawn` alone as concurrency. (write-path group commit)

57. **Validate immutable WAL bytes before any consumer or cache trusts them.**
    Structurally valid bit flips can survive MessagePack decoding while
    changing tombstones, IDs, or vectors. Every query, fetch, FTS, and
    compaction read must verify the fragment checksum and key/payload ULID;
    evict corrupt cached bytes so a later request can recover from repaired S3,
    but never retry as a fallback in the failing request. (Antithesis content)

58. **A successful manifest PUT is not authoritative publication proof.**
    A misdirected store can acknowledge a write under the wrong key, and valid
    bytes from another namespace can otherwise decode cleanly. Bind new
    manifest bytes to their namespace and read back the exact live bytes before
    acknowledging publication while retaining legacy decode compatibility.
    (Antithesis content)

59. **A logical hold starts when the store call claims it, not when the runner
    predicts it.** Background work can win a one-shot selector before the
    foreground task is polled. Record the claimed logical op atomically, notify
    waiters without lost wakeups, and compute release from that exact window;
    otherwise joins can shift or deadlock. (Antithesis scheduling)

60. **A pending store hold must exclude every same-namespace foreground op.**
    Read-shaped operations such as namespace status can block behind held
    namespace state just as mutations can, consuming the HTTP timeout before
    logical time reaches release. Keep other namespaces moving, and exclude a
    pending event while checking for a second overlapping hold. (Antithesis
    scheduling)

61. **Resource-exhaustion tests must distinguish load shedding from wrong
    answers.** With a one-query semaphore, concurrent cache-fill probes may
    return 503. Accept only the complete canonical `CONCURRENCY_LIMIT` envelope
    (including retryability, status, and request ID); every malformed or other
    error remains a loud failure. (Antithesis operational faults)

62. **Active namespace metadata makes the live manifest mandatory.** A manifest
    `NotFound` must not become `Manifest::default`, and a read below a locally
    known successful publication generation must fail instead of poisoning the
    cache. Keep the optional empty-manifest path only for deletion status; never
    serve the cached generation as a fallback after remote inconsistency.
    (Antithesis semantic manifest faults)

63. **A repaired replay may outlive a historical quiet-period failure.** Keep
    workload, deferred-drain, target-node, and hold metadata exact. If and only
    if `failure.json` identifies the terminal recorded boundary as quiescence,
    require the old full trace as an exact normalized prefix and permit only
    contiguous, hold-free quiescence records after it. Clean artifacts still
    require whole-trace equality. (Antithesis replay compatibility)

64. **Namespace activation must come after the first live manifest commit.**
    Reserve `meta.json` in an explicit `creating` state, publish the bootstrap
    manifest, then CAS metadata to `active`. Restart recovery may complete only
    a durable `creating` reservation; a missing manifest beneath already-active
    metadata remains a loud integrity failure. (Antithesis seed 1052)

65. **Measurement counters must separate workload traffic from validation
    traffic.** Snapshot monotonic class counters immediately before the quiet
    period and derive the validation/maintenance census with checked
    subtraction from the final snapshot. Do not reset a live counter or reroute
    quiescence oracles through a different store merely to clean up a report;
    either can change concurrency or fault semantics. (Adversarial perf census)

66. **Batch HTTP success does not imply every query executed.** The batch route
    returns HTTP 200 while individual entries can carry `ok: false`. Perf
    scenarios must validate every entry and result shape; an outer-status-only
    assertion can report a convincing zero-I/O “success” for invalid requests.
    (Dedicated perf analyzer)

67. **ObjectStore observation is not transport-attempt observation.** A wrapper
    beneath `ZeppelinStore` sees adapter invocations and streamed payload bytes,
    but backend retries and recursive-LIST pages remain inside the concrete
    client. Reports must name that scope instead of claiming raw S3 HTTP
    attempts. (Dedicated perf analyzer)

68. **Detached workers need an owned completion seam before isolation.** Queue
    acceptance plus cache/metric polling cannot prove that one hydration or
    background operation owns every measured span or propagates its failure.
    Keep such paths as explicit instrumentation gaps until production exposes
    an awaited one-shot operation or completion handle. (Dedicated perf analyzer)

69. **Parallel critical-path ties need a semantic tie-break.** Completion order
    can select different equal-depth siblings across identical runs. Normalize
    run-specific keys and choose equal-depth interval paths by stable request,
    class, key, bytes, and outcome rather than latency or scheduler order.
    (Dedicated perf analyzer)

70. **A disposable cross-cycle S3 memo needs both observation validation and
    transactional publication.** Reuse an immutable body only after a fresh
    LIST reports the exact same nonempty ETag for the exact key; missing ETags
    remain uncacheable. Populate the next memo only from the final successful
    authority refresh, and restore the previous memo after any partial cycle or
    caught storage failure. (GC performance)

71. **Object-store test controls should compare parsed paths, not raw prefix
    strings.** `object_store::path::Path` normalizes a trailing slash, so a
    decorator comparing `Path::as_ref()` with an unparsed `"prefix/"` can fail
    to inject the intended metadata condition while the test appears to run.
    Parse the expected path once and compare typed `Path` values. (GC perf tests)

72. **Bounded async read fan-out must own its futures and defer error selection.**
    Iterator futures that borrow keys or namespaces can make a containing
    background-loop future fail `Send` only when it reaches `tokio::spawn`.
    Move cloned gateway handles, keys, observations, and cache entries into
    `BoxFuture<'static, _>` values; collect every bounded result in logical
    order, then return the first error in the former sequential priority. For
    retention scans, decode the entire batch before issuing any DELETE.
    (GC performance)

73. **A consolidated namespace LIST needs protocol-specific freshness after
    partitioning.** Reusing one recursive inventory safely removes redundant
    prefix LISTs only when every destructive root body is paired with the
    inventory ETag. Lease-scoped staging is special: after reading the lease,
    probe its exact token key when the earlier inventory did not contain it, or
    a staging publication between LIST and lease GET can be missed. When two
    destructive protocols share a fixed two-LIST budget, alternate a due phase
    on the next cycle after one mutates storage so continuous pending-delete
    churn cannot starve candidate mark/sweep work. (GC performance)

74. **Perf fixtures must use timestamps with fixed serialized precision.**
    RFC 3339 serialization trims trailing fractional-second zeroes, so a
    `Utc::now()` manifest can occasionally be three bytes shorter even when
    its S3 call shape is identical. Exact byte-cost determinism therefore
    requires fixed fixture timestamps (or an explicit normalization rule),
    not repeated runs chosen by chance. (Dedicated perf analyzer)

75. **Replacement safety cannot depend only on re-marking a candidate ledger.**
    If an immutable key changes identity and the refreshed ledger PUT fails,
    the old candidate age remains authoritative after restart. Require the
    S3-LISTed object's own `last_modified` to cross the GC horizon before any
    candidate DELETE, in addition to warm inventory identity checks. This
    supplies a durable fail-closed lower bound even when process memory and a
    corrective ledger write are lost. (GC performance)

76. **Write elision must preserve representation and durability state.** Two
    candidate ledgers can decode to the same vector while one is missing,
    empty-bodied, legacy, or versioned with a noncurrent schema. Skip a GC mark
    PUT only for the current canonical encoding with equal ordered contents;
    otherwise migration or a new mark still needs a durable write. Before a
    destructive sweep, also confirm that a fresh inventory still observes the
    exact ledger identity that made the mark durable. (GC performance)

77. **Memoizing a domain read must preserve its error vocabulary and legacy
    version behavior.** `ManifestCache::get_strong_required` exposes a missing
    object as storage `NotFound`, while compaction-trigger health previously
    recorded `ManifestNotFound`; map that boundary narrowly instead of leaking
    a changed domain error. Published generation-zero manifests are valid but
    are deliberately full-fetched by required strong reads, so bodyless warm
    conditional claims apply only after generation tracking begins. (Compaction
    trigger performance)

78. **Compaction fixtures must publish the same authoritative namespace tuple
    as production.** A raw live manifest plus WAL is not a valid active
    namespace: the lease-protected compaction transaction also requires
    `meta.json`. Tests that omit metadata can accidentally pin a production
    fallback and then fail far from their shared setup when that fallback is
    correctly removed. Seed active `NamespaceMetadata` with the fixture's real
    dimensions and configuration before invoking compaction. (Compaction
    trigger performance)

79. **GET body faults become eligible only after a body-bearing response.** A
    conditional GET can complete as `NotModified`, fail a precondition, or
    return another bodyless error. Reserving a one-shot corruption, stale-read,
    truncated-stream, or post-success crash before the backend answers is safe
    only when bodyless/error/cancelled owners release it deterministically for
    the next eligible GET. Keep the reservation through stream transformation,
    and preserve the old zero-I/O failure when a fault prerequisite such as
    stale body history is absent. (Adversarial compaction-trigger coverage)

80. **Authoritative test metadata must match the operation's distance metric.**
    A shared raw-namespace helper that silently writes Euclidean metadata makes
    cosine fixtures internally inconsistent: S3 claims one namespace while the
    query parameters exercise another. Require dimensions and distance metric
    explicitly at every helper call so fixture state has one source of truth.
    (Compaction trigger performance)

81. **Do not share one Cargo target directory across dirty worktrees when
    validating commit boundaries.** Cargo can reuse a same-named integration
    test binary built from the other worktree, making the output contain tests
    that do not exist in the checkout under validation. Use each worktree's own
    target directory, or clean and verify the executable provenance before
    treating results as evidence. (Performance commit isolation)

82. **Version object user metadata together with bodies in storage fault
    histories.** A stale or wrong-object response assembled from historical
    bytes and current headers is not a coherent old object version. Current
    adversarial schedules exclude `meta.json`, but if metadata objects become
    eligible, retain and restore `GetResult.attributes` alongside each body so
    namespace incarnation identity is faulted consistently. (Namespace
    incarnation cache invalidation)

83. **CAS-first domain errors can change an adversarial adapter without
    weakening its oracle.** When production moves conflict classification
    inside the operation, a raw `ManifestConflict` may become a domain-level
    `LeaseExpired`. Prove the existing self-test RED, adapt only the error
    bridge, and keep the stronger follow-up oracle that verifies the exact
    successor holder. (Lease-renew performance)

84. **A stale manifest memo collides at immutable history before live CAS.**
    Conditional publication writes history first. If another writer already
    published that generation, reconciliation reads the history body and live
    manifest before returning `ManifestConflict`; the retry then reads the live
    manifest again for its ETag. Pin the real slow path as three total GETs, two
    of them against `manifest.json`, instead of assuming a direct one-GET CAS
    conflict. (Writer group-commit performance)

85. **A PUT-result ETag memo inherits S3's atomic-success assumption.** If a
    provider-abuse fault persists truncated bytes while returning success and
    their ETag, the next same-writer CAS can safely replace that exact corrupt
    object from the fully published history candidate. Reads must fail loudly
    before repair, and the repair must preserve every acknowledged fragment;
    do not misclassify the bounded repair as supported-v1 corruption masking.
    (Writer group-commit adversarial coverage)

86. **Compaction cache reuse needs an explicit read-only policy.** Immutable WAL
    bytes selected by the authoritative manifest may safely satisfy compaction
    from the tiered cache, but a miss is about to become dead data and must not
    evict query-hot entries by populating the cache. Model bypass, read-through,
    and read-only as distinct enum variants; verify cold, partial, and warm GET
    counts plus identical segment descriptors and cluster checksums. (Compaction
    fragment-cache performance)

87. **A decoded WAL memo must stay query-scoped and visibility-blind.** Consult
    it only after an authoritative manifest selects exact immutable ULIDs, and
    return shared `Arc<WalFragment>` values so hits do not deep-clone vectors.
    Keep compaction on its explicit byte-cache policy, scope lifecycle eviction
    by namespace, and prove zero-capacity plus whole-cache clearing merely
    re-decode authoritative bytes without changing results. (Decoded WAL CPU
    performance)

88. **Frozen byte-cache contracts must reset any decoded tier above those
    bytes.** Invalidating an immutable FTS object in `DiskCache` does not make a
    measured repeat cold when a decoded `Arc` for the same key survives. Mirror
    an existing explicit repeat-invalidation boundary in disposable decoded
    caches; otherwise priming and earlier repeats hide contracted GETs even
    though production behavior is correct. Keep true warm reuse in a separate
    ideal-analysis scenario instead of rebaselining the frozen contract.
    (Decoded FTS CPU performance)

89. **A 43-character API-key secret must be canonical base64url, not merely
    base64url-shaped.** A string can use only URL-safe characters and have the
    right length while still carrying non-zero unused trailing bits; strict
    `URL_SAFE_NO_PAD` decoding rejects it before digest comparison. Generate
    test secrets from exactly 32 bytes, then hash the emitted 43-character
    encoding used on the wire. (Phase 1 contract security fixtures)

90. **Attach endpoint security to registered methods, not a router's method
    fallback.** Axum's `MethodRouter::route_layer` protects actual handlers and
    implicit HEAD dispatch while leaving unsupported methods to the canonical
    405 machinery. A router-wide auth layer can turn framework 405 responses
    into misleading 401/500 errors before dispatch. (Phase 1 central route
    authorization)

91. **Generated test credentials are server-lifecycle state.** A fresh random
    bearer per ordinary test server prevents a repository-wide secret, but
    adversarial crash restarts and sibling nodes must deliberately reuse the
    original bearer and rebuild the matching digest. Rotating implicitly on a
    restart tests authentication failure instead of the intended storage fault.
    (Phase 1 harness security)

92. **Offline binaries must choose security posture explicitly.** Removing an
    implicit production `Config::load(None)` default can break evaluators that
    only need indexing settings. Give those binaries a validated explicit
    `open_unsafe` config rather than synthesizing credentials or weakening the
    server's fail-closed configuration contract. (Phase 1 fail-closed boot)

93. **Authorize body-derived namespace targets after extraction and before
    storage I/O.** A route-level `System` resource can prove the action but
    cannot enforce a namespace named only in a create or clone body. Refine the
    decision through the same central kernel once the target is decoded, then
    reject it before reading or writing authoritative state. (Phase 1 namespace
    scoping)

94. **Migrate durable artifacts at their decoder boundary, not through a
    production config default.** Making a new fail-closed configuration field
    required can strand historical replay files. Inject the explicit legacy
    test actor only while decoding a missing field in replay artifacts; keep a
    present-but-invalid value loud and leave production boot strict. (Phase 1
    adversarial replay compatibility)

95. **Root control prefixes need explicit discovery and cleanup ownership.**
    `_audit/` does not live beneath a TestHarness namespace, so namespace
    discovery must reject the reserved root and test servers must stamp a
    unique node ID that cleanup can target without deleting another test's
    evidence. (Phase 2 audit isolation)

96. **Cross the durable audit barrier before launching irreversible cleanup.**
    An asynchronous namespace DELETE may persist its `deleting` tombstone
    before audit storage fails, but it must not start background object removal
    until the evidence barrier succeeds. This preserves a retryable state while
    returning the explicit `audit_unavailable` result. (Phase 2 must-audit)

97. **Frozen domain performance contracts and control-plane audit traffic need
    separate accounting.** Keep raw `_audit/` key counts so the security budget
    is asserted exactly, while excluding those objects from namespace artifact
    classes and depth chains. Never widen a domain tolerance to hide a required
    control-plane write. (Phase 2 performance accounting)

98. **Place request timeouts inside response-side security auditing.** A Tower
    timeout outside authorization drops the entire inner future, so an operation
    can persist its first mutation and then lose the only code that emits its
    audit outcome. Authenticate and authorize outside the endpoint timeout so a
    408 still unwinds through audit finalization. (Phase 2 audit cancellation)

99. **Validate client identifiers before projecting them into typed audit
    parameters.** A bounded list count does not bound the size or character set
    of each string. Apply the domain's existing ID validator before WAL work or
    audit capture so attacker-controlled payload text cannot become evidence.
    (Phase 2 audit redaction)

100. **Stop audit producers and drain their writer before deleting test
     evidence.** A one-shot prefix cleanup races a periodic flush when a
     detached test server outlives its harness. Give the harness explicit HTTP
     and audit-task ownership; graceful cleanup stops requests, drains buffered
     records, and only then removes scoped objects. (Phase 2 test isolation)

101. **Server errors do not relinquish background-service ownership.** Once
     startup returns an audit runtime and thread handles, a later bind or serve
     error is only the primary result; it must not bypass audit drain or
     background shutdown. Attempt every owned cleanup, log secondary failures,
     then return errors in causal priority. (Phase 2 process shutdown)

102. **Test evidence scope is lifecycle identity, not domain configuration.**
     A helper's optional namespace prefix can be absent even though its audit
     objects still belong to a concrete harness. Stamp audit node IDs from the
     harness cleanup identity independently so a graceful drain cannot create
     root objects that no owner can match. (Phase 2 audit isolation)

103. **Explicit test cleanup is a gate, not a best-effort destructor.** Collect
     server, domain-prefix, audit-LIST, and per-object DELETE failures while
     still attempting every cleanup step, then fail the test with all errors.
     Reserve warning-only behavior for `Drop`, where async failure cannot be
     returned to the caller. (Phase 2 test lifecycle)

104. **Teardown error priority must not short-circuit audit drain.** Signal all
     tasks, await compaction and HTTP results without panicking, then shut down
     audit and report every failure in causal order. An earlier join failure is
     not permission to abort queued security evidence. (Phase 2 test shutdown)

105. **`#[instrument]` records every unskipped handler argument.** A
     `HeaderMap` accepted for content negotiation also contains
     `Authorization`; leaving it out of `skip(...)` writes bearer material into
     every event in that span. Skip the complete header map and prove redaction
     by capturing a real authenticated HTTP request's tracing output. (Phase 2
     secret hygiene)

106. **SDK secret hygiene follows the final Authorization header, including
     overrides.** A canonical server envelope that never echoes credentials is
     a tautological redaction test. Inject an echoed credential, derive the
     value from the actual outgoing request, and cover runtime representations:
     TypeScript `private` is compile-time only, while `#private` prevents JSON
     and inspector exposure. (Phase 9/C1 client security)

107. **Timestamp an authoritative refresh observation when it completes, not
     when its cache install eventually wins the lock.** An unchanged conditional
     head read can finish, then wait while a remote revoke CAS becomes visible;
     stamping the later install time extends stale-open authorization beyond the
     modeled freshness bound. Carry the captured monotonic instant through the
     install and apply it only while the observed head identity still matches.
     (Phase 3 policy freshness)

108. **A scheduled revocation is not an effective revocation.** Rotation overlap
     persists a predecessor in `Revoked` state with a future `revokes_at`, so a
     state-only repeat-revoke check prevents emergency invalidation of a
     compromised credential. Let an explicit revoke shorten a future deadline
     to now; reject only revocations whose deadline is already effective.
     (Phase 3 key rotation)

109. **A test harness must scope its authoritative security store, not only its
     domain keys and audit identity.** Once authentication loads the exact
     `_security/` root from S3, passing the raw shared MinIO store makes otherwise
     isolated harnesses race on one policy head. A losing harness can return a
     newly generated admin credential that its server will never accept, and its
     policy objects escape normal prefix cleanup. Wrap only the security runtime
     in a harness-prefixed store; keep the domain store unchanged to avoid
     double-prefixing namespace artifacts. (Phase 3 test isolation)

110. **Immediate revocation must not be represented by a wall-clock deadline.**
     A timestamp captured by the writer remains in the future on a skewed reader,
     extending a compromised credential beyond the refresh contract. Keep
     scheduled rotation overlap as `Some(deadline)` and encode an effective
     explicit or zero-overlap revoke as `None`; authentication and authorization
     can then reject it independently of local wall-clock skew. This supersedes
     the "shorten to now" repair in learning 108. (Phase 3 key revocation)

111. **Authoritative mutation retries must sample trusted time after every fresh
     base load.** Reusing request-start time lets a credential whose rotation
     overlap expired authorize a later CAS retry against a newer snapshot. The
     policy cache must own the clock and reauthorize each attempt with a new
     sample before building the candidate. (Phase 3 policy mutation)

112. **Freshness begins at the authoritative observation, never at decode,
     compile, or cache install.** Initial loads, changed refreshes, bootstrap,
     and successful publication can all spend the entire freshness budget after
     the head read or CAS completes. Carry that monotonic instant through the
     verified snapshot and stamp every cache install from it so delayed work
     fails closed instead of minting a new interval. (Phase 3 policy freshness)

113. **Audit the decision that actually governed the last attempted work.** A
     security-admin handler can enter under cached policy V1, reauthorize a CAS
     mutation against authoritative V2, then deny or fail while building or
     retrying. Returning only the domain error leaves audit attached to the
     stale outer V1 allow. Carry the exact V2 deny or latest V2 allow alongside
     every post-decision error, including bounded conflicts and retry-load
     failures, and replace the request-local audit decision before rendering the
     response. (Phase 3 policy administration)

114. **Authentication result, policy version, and freshness are one atomic
     observation.** Reading cache freshness before authentication and reading
     the version again after a failure can combine three different snapshots,
     misclassify stale policy as a credential failure, and audit the wrong
     version. Return all three values from the single cached snapshot evaluated
     by the credential adapter and prioritize its stale-deny result. (Phase 3
     authentication)

115. **A must-audit route includes admission rejections before authorization.**
     Outer IP and authenticated-principal rate limits can return 429 before an
     `AuditRequest` exists. Preserve their DDoS ordering, but explicitly enqueue
     a buffered, decision-less `RATE_LIMITED` record with the mapped security
     action, principal when known, source IP, and observed policy version.
     (Phase 3 security administration)

116. **A body-derived namespace target needs an explicit action-only precheck.**
     Treating `Resource::System` as a placeholder works only while authorization
     ignores scope; a policy-backed namespace grant correctly rejects that
     global resource before the handler can evaluate the real body target. Use
     action-only authorization solely to prove the route capability, then keep
     the handler's exact namespace authorization as the mandatory scope check.
     (Phase 3 namespace RBAC)

117. **Held-operation conflicts include global admission resources, not only
     namespace state.** With one query permit, an in-flight held query makes an
     unrelated exact-error query observe the canonical 503 before its validation
     seam. Track whether the hold owns query admission and defer only probes
     whose oracle requires an exact downstream error; keep ordinary queries
     runnable so load-shedding coverage remains real. (Phase 3 adversarial
     scheduling)

118. **Composite fault schedules must leave authoritative startup reads
     possible.** A secondary node correctly fails closed when its required S3
     security-policy load begins inside a global read partition. Relocate the
     generated node-start boundary past that partition while preserving the
     window duration; never bypass the fault proxy, reuse another node's cache,
     or add a policy fallback to make the harness pass. (Phase 3 adversarial
     scheduling)

119. **A guarded write must bind namespace lifetime and manifest authority in
     the same durable CAS chain.** Process-local namespace metadata cannot fence
     a delete/recreate race. Read the authoritative metadata object with a
     nonempty ETag, CAS-migrate legacy incarnation metadata without changing its
     body, then require the live manifest to carry that exact UUID and ETag
     before publishing. Guarded appends must bypass group-commit coalescing and
     stale manifest memos. (Phase 4 write constraints)

120. **Legacy identity migration is an online recovery state machine, not a
     default.** Ordinary reads remain side-effect free. A guarded writer may
     repair unbound metadata/manifest state with bounded CAS, adopt an already
     bound manifest, and resume the exact empty generation-2 create crash; an
     Active namespace with no manifest is corruption and must fail 500 before
     mutation. (Phase 4 namespace incarnation)

121. **Authenticate a cursor before interpreting any field inside it.** The
     server-only HMAC key must never derive from client credentials or serialize
     through config/debug output. Bind policy version, complete
     result-affecting query shape, consistency, `as_of`, and page marker, verify
     the MAC first, then report stale-policy or shape errors. A batch containing
     only forged cursors must stop before namespace storage I/O. (Phase 4
     cursors)

122. **A field mask must close inference surfaces, not merely redact response
     JSON.** Reject caller filters, BM25 sources/rerank, facets, and grouping that
     name denied fields; otherwise membership, ordering, or counts reveal the
     hidden value. Constrained debug/explain output must also suppress physical
     scan counters and predicates. (Phase 4 field masks)

123. **Security stamping must not erase the absent-versus-empty attribute
     distinction.** Calling `get_or_insert_with(HashMap::new)` for every upsert
     silently turns an unconstrained missing attribute object into `{}` and can
     break exact fetch oracles after compaction. Materialize a map only when a
     server stamp or preserved protected value actually needs storage; evaluate
     a mandatory filter against a borrowed empty map without mutating the row.
     (Phase 4 write constraints)

124. **All-invalid batch authorization must return before shared namespace
     setup.** Per-entry cursor validation is not enough if the handler performs
     an unconditional metadata or manifest read afterward. When every entry is
     already a deterministic error, assemble the positional envelopes directly
     and prove zero namespace GETs with a counting store. (Phase 4 batch query)

125. **A raw derived artifact needs a policy-wide non-widening proof, not only
     authorization of its creator.** Before publishing a namespace clone,
     compare source and target Query and VectorFetch visibility for every
     compiled policy principal, including keyless principals and global or
     pre-provisioned target grants. Target-denied is safe; target-only access,
     dropped mandatory-filter conjuncts, or a weaker field-mask deny set must
     fail before target creation. (Phase 4 namespace clone)

126. **Post-activation rollback cannot safely delete a clone target.** The
     bootstrap target is independently writable while immutable source objects
     are copied, so another request can receive a successful write response
     before the clone fails. A later read-then-delete cleanup has a TOCTOU gap
     and can erase that acknowledged write. Invalidate disposable cache state,
     retain and report the target, and require explicit administrative cleanup.
     (Phase 4 namespace clone)

127. **A privileged exception and its constraints must share the write-action
     seam.** `AttributeAdmin` can bypass a `VectorUpsert` forbid-set even when it
     comes from a separate grant, but constraints carried only by that admin
     grant are not part of the upsert grant aggregation. Reject constrained
     `AttributeAdmin` grants unless the same grant also includes `VectorUpsert`;
     never silently discard security constraints from a privileged grant.
     (Phase 4 write constraints)

128. **Negative query predicates need a fail-closed write-scope evaluator.**
     Query semantics intentionally let a missing field satisfy `not_eq` and
     `not_in`, but reusing that evaluator at an upsert boundary lets `{}` or an
     unrelated attribute map create an apparently scoped row. Propagate missing
     leaves as unknown through AND, OR, and NOT and accept a write only on a
     definite match; do not change established read-filter semantics.
     (Phase 4 write constraints)

129. **Policy scope defines BM25 statistics; caller filters only narrow
     candidates.** Collapsing both predicates before building a scoped lexical
     corpus changes IDF and document-length score bits whenever a caller adds a
     filter. Materialize and score the complete policy-visible corpus, then
     apply the effective policy-and-caller predicate to scored candidates.
     (Phase 4 BM25 isolation)

130. **An active manifest pointer without its descriptor is corruption, not an
     empty segment.** Resolve the pointer through one fail-loud helper shared by
     ANN and BM25 paths. Treating an unresolved ID as `None` silently returns
     WAL-only or empty results from malformed authoritative state.
     (Phase 4 query isolation)

131. **A durable-audit obligation must itself control response settlement.**
     An action or privilege switch duplicated beside `Obligation::DurableAudit`
     will eventually miss a newly obligated path. Retain audit request state and
     cross the synchronous settlement barrier based on the decision obligation;
     route classifications only decide which obligations the kernel emits.
     (Phase 4 audit constraints)

132. **Row-scoped create and update need different identity capabilities.** A
     caller-chosen ID lets a constrained upsert distinguish an absent row from
     a hidden collision even when reads hide both. Under a mandatory filter,
     make explicit IDs update-only and give creates opaque server-owned IDs;
     preserve one manifest-snapshot guard and CAS across the whole batch.
     (Phase 4 write constraints)

133. **A security-scope index must own both its corpus and its lifecycle.**
     Filtering hits from a shared ANN index still leaks hidden rows through
     training and cluster selection, while rebuilding a policy corpus after
     every decoded-cache clear makes security correctness a cold-path scan.
     Create-publish immutable ANN and stable BM25 scope artifacts beneath the
     source segment prefix, bind descriptors to the source/filter/config key,
     and validate every referenced key stays inside that prefix. Keep mutable
     WAL-frontier BM25 variants only in the bounded decoded cache so per-write
     manifests cannot create an unbounded trail of durable derived objects.
     (Phase 4 retrieval isolation)

134. **A derived-artifact lifecycle prefix needs periodic-GC ownership, not
     only compaction-time listing.** A query can publish an immutable scope
     artifact after compaction snapshots the old segment prefix. Teach GC an
     allowlisted nested-key grammar, protect every known artifact while its
     parent segment is live or PITR-retained, and age-gate it as an orphan after
     the parent leaves all roots. Unknown nesting must still fail closed and be
     retained. (Phase 4 scoped retrieval GC)

135. **Moving only the final index builder off Tokio leaves an unbounded CPU
     prelude on the request worker.** Whole-segment decode assembly, row moves
     or clones, policy filtering, deterministic sorting, WAL merge, and artifact
     serialization all belong behind the blocking-worker boundary. Keep the
     async side to object-store I/O and lightweight orchestration, and propagate
     worker failure through a module-specific typed error. (Phase 4 scoped
     retrieval execution)

136. **A foreground store hold and a process crash are independent fault
     boundaries.** A request parked by `HoldCall` can legitimately be the task
     that observes a concurrent process-crash notification. Join it at the
     recorded logical boundary, apply its ambiguity once, then run the ordinary
     restart and recovery probe; never assert that the overlap is impossible.
     Exact replay must also delay an already-completed result to its recorded
     join boundary when cache or crash scheduling wins the hold-wait race, not
     execute the operation a second time. (Phase 4 adversarial soak)

137. **A post-activation clone failure leaves a reserved bootstrap target by
     design.** Once target creation succeeds, deleting it on copy failure can
     erase a concurrent acknowledged write. Fault tests should verify that the
     source survives and the retained target exposes only its safe bootstrap
     state, not assert that the target manifest is absent. (Phase 4 namespace
     clone adversarial regression)

138. **Security fault choreography needs fixed logical indexes across seeded
     setup variation.** A variable namespace count moves grant publication and
     can make a nominal four-boundary profile miss a fault entirely or collide
     two faults on one request. Pad only with harmless admin reads before the
     security script, then pin remove, restore, clock, and refresh events to
     invariant logical operations. (Phase 5 adversarial security)

139. **A cached policy-head fault needs an explicit refresh attempt to be
     observable.** Authorized requests normally use the bounded-stale in-memory
     policy and may perform no S3 GET during a short deterministic smoke. At a
     pinned workload barrier, force one refresh through a test-only kernel seam,
     require the scheduled head GET to fail, and prove the cached authorization
     path remains correct. (Phase 5 adversarial security)

140. **Chaos-mode compaction coverage belongs to quiescence, not sanitized
     foreground maintenance.** Chaos deliberately rewrites manual compaction
     and GC operations so storage faults do not drown foreground invariants. A
     security-profile smoke must require at least one explicit quiet-period
     compaction per seed while retaining the existing 20-compaction floor for
     mixed profiles. (Phase 5 adversarial security)

141. **A policy oracle must model grant state, not only expected status
     constants.** Apply definite grant mutations to a per-actor grant map,
     retain old/new outcomes for ambiguous publication within the bounded
     window, and close that window only after an authoritative refresh. Record
     every successful must-audit request ID at the same model transition so the
     durable audit oracle covers mutations as well as its explicit barrier.
     (Phase 5 adversarial security)

142. **Actor metadata is not actor execution.** Once every recorded operation
     carries an actor, the HTTP client-selection seam and the policy model must
     both consume it: select that actor's current credential for ordinary
     routes and derive the expected decision from the exact action and scope.
     Leaving either side on implicit admin makes authorization failures and
     false passes indistinguishable. (Phase 5 adversarial security)

143. **An audit-request predicate is broader than the durable-audit success
     contract.** Vector upserts create an audit request so constraint denials
     and AttributeAdmin writes can be recorded, but an ordinary successful
     upsert emits no durable record. An audit-evidence oracle must mirror the
     final durable boundary: always-audited actions plus explicit
     `DurableAudit` obligations, including AttributeAdmin. (Phase 5
     adversarial security)

144. **Final policy state cannot resolve inverse ambiguous mutations by
     itself.** A remove followed by an add can leave the same snapshot whether
     both applied or neither did. Classify each mutation with its deterministic
     request ID, typed security-policy audit action/resource, and old/new
     policy-version lineage against the authoritative head; emit that evidence
     per op and retain every pending mutation if validation fails. (Phase 5
     adversarial security)

145. **Compound routes need compound policy-model requirements.** A clone is
     not authorized by `NamespaceClone` alone: the same actor also needs source
     `NamespaceRead` and target `NamespaceCreate`, and every decision consumed
     by the raw copy must be unconstrained. Represent all route requirements at
     the model seam so a partial grant cannot turn a conforming 403 into an
     oracle false positive. (Phase 5 adversarial security)

146. **Bounded staleness is a decision transition, not an HTTP status pair.**
     Authorized creates, deletes, snapshots, compactions, and clones can return
     different 2xx statuses. Store complete old/new grant states and evaluate
     the operation under each; keep credential revocation as a separate
     authenticated-to-unauthorized transition. (Phase 5 adversarial security)

147. **Overlapping absorption windows compose as reachable whole-policy
     branches.** A definite publication advances every current branch; an
     ambiguous publication retains both its not-applied and applied branches;
     an overlapping credential revocation independently adds an unauthenticated
     outcome. Never take a Cartesian product of per-principal states because it
     invents combinations that never shared one authoritative policy version.
     (Phase 5 adversarial security)

148. **Clone no-widening is policy-wide, not only an acting-principal check.**
     The clone caller's three control grants can all pass while another
     principal gains target-only read or write authority over the raw copy.
     Mirror the production derived-action scope proof across every principal,
     including filter conjuncts, stamps, forbidden fields, and
     `AttributeAdmin` bypass. (Phase 5 adversarial security)

149. **A compound observation is not necessarily a compound authorization
     requirement.** `ExportProbe` reports success when either vector fetch or
     snapshot read reaches a non-forbidden surface, so its model must accept
     either grant. Keep all-of semantics only for routes, such as clone, whose
     production handler actually requires every capability. (Phase 5
     adversarial security)

150. **Policy-oracle action vocabulary must be typed and fail loud.** Raw
     string comparisons turn misspelled or newly added actions into silent
     denials and can hide omitted actors behind empty grant defaults. Parse
     modeled actions into the production `Action` type and require every actor
     lookup to exist in the configured principal vocabulary. (Phase 5
     adversarial security)

151. **A serialized security staleness bound is executable policy, not report
     metadata.** Window constructors must read the configured logical-op bound
     and use checked arithmetic. Hard-coded constants drift from replayed
     configuration, while saturation silently makes an overflowed window
     effectively permanent. (Phase 5 adversarial security)

152. **Security control-plane ops still authorize the declared actor.** Key
     mutations and grant publication require typed `SecurityAdminWrite`;
     security reads and audit barriers require typed `SecurityAdminRead`.
     Unconditionally predicting success hides non-admin coverage and
     mismodels legitimate 403 responses. (Phase 5 adversarial security)

153. **Grant binding identity uses canonical typed action sets.** Production
     sorts and validates selected actions before add/remove binding comparison,
     so an oracle that compares raw string-vector order can retain a grant the
     server removed. Parse every action eagerly and compare the resulting sets.
     (Phase 5 adversarial security)

154. **`ZEPPELIN_ADVERSARIAL_SEEDS` without a comma is a count, not a seed
     value.** Setting it to `0` requests zero seeds and fails before the runner
     executes an operation. Use `1` for one emitted seed (`0`), or a comma list
     when naming explicit seed values. Treat this startup rejection as a soak
     preflight/configuration failure, not an executed soak. (Phase 5
     adversarial security)
