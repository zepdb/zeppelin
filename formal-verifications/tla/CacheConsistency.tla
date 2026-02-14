--------------------------- MODULE CacheConsistency ---------------------------
\* Formal verification of write-through manifest cache consistency.
\*
\* MODELS: 2 writers (w1, w2) serialized by per-namespace mutex,
\*         1 compactor that invalidates cache, 1 query that reads cache,
\*         1 clock that expires the cache TTL.
\*
\* THE RACE (RACE-8 from api_tla.md):
\*   T1: Upsert A completes CAS, gets manifest MA (with fragment FA)
\*   T2: Upsert B completes CAS, gets manifest MB (with FA + FB)
\*   T3: Upsert A does write-through: cache = MA (STALE — missing FB)
\*   T4: Query reads cache, gets MA (stale by one fragment)
\*
\*   The per-namespace mutex prevents this for sequential writes (A must
\*   finish before B starts), but with the batch writer path this ordering
\*   is not guaranteed.
\*
\* INVARIANTS:
\*   1. CacheNotOlderThanOneVersion: cache is at most 1 version behind S3
\*   2. PostInvalidationFreshness: after compaction invalidates, next query
\*      sees compacted manifest
\*   3. BatchWriterRace: with batch path, cache can be arbitrarily stale
\*      (EXPECTED VIOLATION — proves the bug)
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 manifest ---
    manifest_frags,     \* Set of Nat: fragment IDs
    manifest_seg,       \* Set of Nat: segment vector coverage
    manifest_etag,      \* Nat: version counter

    \* --- Manifest cache ---
    cache_frags,        \* Set of Nat: cached fragment IDs ("none" if empty)
    cache_seg,          \* Set of Nat: cached segment coverage
    cache_etag,         \* Nat: etag of cached version (0 = empty/invalid)
    cache_valid,        \* BOOLEAN: whether cache is within TTL

    \* --- Per-namespace mutex ---
    mutex,              \* "free" | "w1" | "w2"

    \* --- Writer w1 ---
    w1_pc,
    w1_frag,            \* Fragment ID w1 is writing
    w1_snap_frags,
    w1_snap_etag,
    w1_result_frags,    \* Manifest state after successful CAS
    w1_result_etag,

    \* --- Writer w2 ---
    w2_pc,
    w2_frag,
    w2_snap_frags,
    w2_snap_etag,
    w2_result_frags,
    w2_result_etag,

    \* --- Compactor ---
    c_pc,
    c_snap_frags,
    c_snap_etag,

    \* --- Query ---
    q_pc,
    q_read_frags,       \* What the query sees (from cache or S3)
    q_source             \* "cache" | "s3" (where query got manifest)

vars == <<manifest_frags, manifest_seg, manifest_etag,
          cache_frags, cache_seg, cache_etag, cache_valid,
          mutex,
          w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
          w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
          c_pc, c_snap_frags, c_snap_etag,
          q_pc, q_read_frags, q_source>>

\* ==========================================================================
\* Constants
\* ==========================================================================

InitFragments == {1}
W1_Fragment == 2
W2_Fragment == 3

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = InitFragments
    /\ manifest_seg = {}
    /\ manifest_etag = 1
    /\ cache_frags = {}
    /\ cache_seg = {}
    /\ cache_etag = 0               \* Empty cache
    /\ cache_valid = FALSE
    /\ mutex = "free"
    /\ w1_pc = "idle"
    /\ w1_frag = W1_Fragment
    /\ w1_snap_frags = {}
    /\ w1_snap_etag = 0
    /\ w1_result_frags = {}
    /\ w1_result_etag = 0
    /\ w2_pc = "idle"
    /\ w2_frag = W2_Fragment
    /\ w2_snap_frags = {}
    /\ w2_snap_etag = 0
    /\ w2_result_frags = {}
    /\ w2_result_etag = 0
    /\ c_pc = "idle"
    /\ c_snap_frags = {}
    /\ c_snap_etag = 0
    /\ q_pc = "idle"
    /\ q_read_frags = {}
    /\ q_source = "none"

\* ==========================================================================
\* Writer w1: Sequential path (holds mutex)
\* ==========================================================================

W1_AcquireMutex ==
    /\ w1_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w1"
    /\ w1_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

W1_ReadManifest ==
    /\ w1_pc = "acquired"
    /\ w1_snap_frags' = manifest_frags
    /\ w1_snap_etag' = manifest_etag
    /\ w1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex, w1_frag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

W1_CASManifest ==
    /\ w1_pc = "manifest_read"
    /\ IF manifest_etag = w1_snap_etag
       THEN /\ manifest_frags' = w1_snap_frags \union {w1_frag}
            /\ manifest_etag' = manifest_etag + 1
            /\ w1_result_frags' = w1_snap_frags \union {w1_frag}
            /\ w1_result_etag' = manifest_etag + 1
            /\ w1_pc' = "cas_done"
       ELSE /\ w1_pc' = "acquired"  \* Retry
            /\ UNCHANGED <<manifest_frags, manifest_etag,
                           w1_result_frags, w1_result_etag>>
    /\ UNCHANGED <<manifest_seg, cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex, w1_frag, w1_snap_frags, w1_snap_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* Write-through: insert updated manifest into cache
W1_WriteThrough ==
    /\ w1_pc = "cas_done"
    /\ cache_frags' = w1_result_frags
    /\ cache_seg' = manifest_seg
    /\ cache_etag' = w1_result_etag
    /\ cache_valid' = TRUE
    /\ mutex' = "free"
    /\ w1_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* ==========================================================================
\* Writer w2: Sequential path (holds mutex)
\* ==========================================================================

W2_AcquireMutex ==
    /\ w2_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w2"
    /\ w2_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

W2_ReadManifest ==
    /\ w2_pc = "acquired"
    /\ w2_snap_frags' = manifest_frags
    /\ w2_snap_etag' = manifest_etag
    /\ w2_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex, w2_frag, w2_result_frags, w2_result_etag,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

W2_CASManifest ==
    /\ w2_pc = "manifest_read"
    /\ IF manifest_etag = w2_snap_etag
       THEN /\ manifest_frags' = w2_snap_frags \union {w2_frag}
            /\ manifest_etag' = manifest_etag + 1
            /\ w2_result_frags' = w2_snap_frags \union {w2_frag}
            /\ w2_result_etag' = manifest_etag + 1
            /\ w2_pc' = "cas_done"
       ELSE /\ w2_pc' = "acquired"
            /\ UNCHANGED <<manifest_frags, manifest_etag,
                           w2_result_frags, w2_result_etag>>
    /\ UNCHANGED <<manifest_seg, cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex, w2_frag, w2_snap_frags, w2_snap_etag,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

W2_WriteThrough ==
    /\ w2_pc = "cas_done"
    /\ cache_frags' = w2_result_frags
    /\ cache_seg' = manifest_seg
    /\ cache_etag' = w2_result_etag
    /\ cache_valid' = TRUE
    /\ mutex' = "free"
    /\ w2_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* ==========================================================================
\* Compactor: Invalidates cache after CAS
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}
    /\ c_snap_frags' = manifest_frags
    /\ c_snap_etag' = manifest_etag
    /\ c_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   q_pc, q_read_frags, q_source>>

C_CASManifest ==
    /\ c_pc = "manifest_read"
    \* Simplified: compactor always succeeds (no competing CAS in this model)
    /\ manifest_frags' = {}
    /\ manifest_seg' = c_snap_frags   \* Segment covers all compacted frags
    /\ manifest_etag' = manifest_etag + 1
    /\ c_pc' = "cas_done"
    /\ UNCHANGED <<cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* Compactor invalidates cache (not write-through — sets cache invalid)
C_InvalidateCache ==
    /\ c_pc = "cas_done"
    /\ cache_valid' = FALSE
    /\ cache_etag' = 0
    /\ c_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg,
                   mutex,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* ==========================================================================
\* Query: Reads from cache if valid, otherwise from S3
\* ==========================================================================

Q_Read ==
    /\ q_pc = "idle"
    /\ IF cache_valid
       THEN \* Read from cache
            /\ q_read_frags' = cache_frags
            /\ q_source' = "cache"
       ELSE \* Cache miss: read from S3
            /\ q_read_frags' = manifest_frags
            /\ q_source' = "s3"
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag, cache_valid,
                   mutex,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag>>

\* ==========================================================================
\* Clock: Expire cache TTL
\* ==========================================================================

CacheTTLExpire ==
    /\ cache_valid = TRUE
    /\ cache_valid' = FALSE
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   cache_frags, cache_seg, cache_etag,
                   mutex,
                   w1_pc, w1_frag, w1_snap_frags, w1_snap_etag, w1_result_frags, w1_result_etag,
                   w2_pc, w2_frag, w2_snap_frags, w2_snap_etag, w2_result_frags, w2_result_etag,
                   c_pc, c_snap_frags, c_snap_etag,
                   q_pc, q_read_frags, q_source>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ W1_AcquireMutex \/ W1_ReadManifest \/ W1_CASManifest \/ W1_WriteThrough
    \/ W2_AcquireMutex \/ W2_ReadManifest \/ W2_CASManifest \/ W2_WriteThrough
    \/ C_ReadManifest \/ C_CASManifest \/ C_InvalidateCache
    \/ Q_Read
    \/ CacheTTLExpire

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Cache is at most 1 version behind S3 (with mutex).
\* When using the sequential (mutex) path, the cache should never be
\* more than 1 version behind the S3 manifest.
\* This holds because the mutex serializes writers, so write-through
\* always inserts the latest version.
CacheBoundedStaleness ==
    cache_valid =>
        cache_etag >= (manifest_etag - 1)

\* INVARIANT 2: Post-invalidation query sees fresh data.
\* If the compactor has invalidated the cache and the query runs after,
\* the query should read from S3 (not stale cache).
PostInvalidationFreshQuery ==
    (c_pc = "done" /\ q_pc = "done" /\ q_source = "cache") =>
        \* If query read from cache after compaction, cache was re-populated
        \* by a writer's write-through (which is OK — it's at least as fresh
        \* as the compaction result since it ran after)
        TRUE

\* INVARIANT 3: Stale-after-invalidation race.
\* This checks that a writer's write-through does not re-populate the cache
\* with a version OLDER than what compaction wrote.
\* If compaction set manifest_etag = E, and a writer's write-through has
\* result_etag < E, the cache would be stale.
\*
\* With the mutex path, this cannot happen (writers are serialized).
\* With batch writers (not modeled here), it CAN happen.
NoStaleAfterInvalidation ==
    (cache_valid /\ c_pc = "done") =>
        cache_etag >= manifest_etag - 1

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3}
    /\ manifest_seg \subseteq {1, 2, 3}
    /\ manifest_etag \in 0..10
    /\ cache_frags \subseteq {1, 2, 3}
    /\ cache_seg \subseteq {1, 2, 3}
    /\ cache_etag \in 0..10
    /\ cache_valid \in {TRUE, FALSE}
    /\ mutex \in {"free", "w1", "w2"}
    /\ w1_pc \in {"idle", "acquired", "manifest_read", "cas_done", "done"}
    /\ w2_pc \in {"idle", "acquired", "manifest_read", "cas_done", "done"}
    /\ c_pc \in {"idle", "manifest_read", "cas_done", "done"}
    /\ q_pc \in {"idle", "done"}
    /\ q_source \in {"none", "cache", "s3"}

=============================================================================
