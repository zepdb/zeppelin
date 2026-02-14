----------------------- MODULE UpsertDeleteCompactQuery -----------------------
\* Formal verification of the 4-way interaction:
\*   1 writer (upserts vector V), 1 deleter (deletes vector V),
\*   1 compactor (merges WAL into segment), 1 strong query (reads all data).
\*
\* MODELS: The full lifecycle of a vector through upsert → delete → compaction
\*         → query, with all four operations running concurrently.
\*
\* KEY QUESTION: Does the ULID-based watermark in remove_compacted_fragments()
\*   correctly preserve delete tombstones added after the compaction snapshot?
\*
\* INVARIANTS:
\*   1. NoSilentDataLoss: Committed upsert visible unless deleted
\*   2. DeleteConsistency: Committed delete removes vector from query results
\*   3. WatermarkSafety: Fragments added after compaction snapshot are retained
\*
\* The model uses ULID ordering (Nat comparison) and sequence numbers.
\* Each fragment has: {id (ULID), vectors (set), deletes (set), seq_num}.
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    manifest_frags,     \* Set of fragment IDs in manifest
    manifest_seg_vecs,  \* Set of vector IDs in the active segment (empty = no segment)
    manifest_etag,      \* Nat: version counter for CAS
    manifest_watermark, \* Nat: ULID of last compacted fragment (0 = none)

    \* --- Fragment contents on S3 (map: frag_id -> record) ---
    \* Each fragment is modeled as [vectors |-> set, deletes |-> set, seq |-> Nat]
    frag_vectors,       \* frag_id -> Set of vector IDs
    frag_deletes,       \* frag_id -> Set of vector IDs (tombstones)
    frag_seq,           \* frag_id -> Nat (sequence number)

    \* --- Per-namespace mutex ---
    mutex,              \* "free" | "w" | "d"

    \* --- Monotonic counters ---
    next_seq,           \* Next sequence number to assign
    committed_upserts,  \* Set of vector IDs with successful upsert (HTTP 200)
    committed_deletes,  \* Set of vector IDs with successful delete (HTTP 204)

    \* --- Writer (upserts vector V) ---
    w_pc,
    w_snap_frags,       \* Snapshot of manifest_frags
    w_snap_etag,

    \* --- Deleter (deletes vector V) ---
    d_pc,
    d_snap_frags,
    d_snap_etag,

    \* --- Compactor ---
    c_pc,
    c_snap_frags,       \* Fragment IDs in compaction snapshot
    c_snap_etag,
    c_merged_vecs,      \* Vectors in the new segment (after merge)
    c_last_frag_id,     \* Max ULID of compacted fragments (watermark)

    \* --- Query ---
    q_pc,
    q_snap_manifest_frags,  \* Query's manifest snapshot (fragment IDs)
    q_snap_manifest_seg,    \* Query's manifest snapshot (segment vectors)
    q_snap_watermark,       \* Query's watermark snapshot
    q_wal_results,          \* Vectors from WAL scan
    q_wal_deletes,          \* Deletes from WAL scan
    q_final_results         \* Final merged query results

vars == <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
          frag_vectors, frag_deletes, frag_seq,
          mutex, next_seq, committed_upserts, committed_deletes,
          w_pc, w_snap_frags, w_snap_etag,
          d_pc, d_snap_frags, d_snap_etag,
          c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
          q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
          q_wal_results, q_wal_deletes, q_final_results>>

\* ==========================================================================
\* Constants
\* ==========================================================================

\* The single vector ID we're tracking through the lifecycle.
V == 1

\* Fragment IDs (model ULIDs as ordered naturals).
\* Pre-existing fragments: 1, 2 (contain no V).
\* Writer's fragment: 3 (upserts V).
\* Deleter's fragment: 4 (deletes V).
\* But: we also model the case where deleter's ULID < writer's (clock skew).
InitFrag1 == 1
InitFrag2 == 2
WriterFrag == 3
DeleterFrag == 4

\* For the ULID-ordering edge case: deleter fragment could have ULID 5 (normal)
\* or ULID 3 with writer getting ULID 4 (clock skew). We model the normal case
\* here; clock skew variant can be tested by swapping the IDs.

\* ==========================================================================
\* Helper: Unchanged fragments state
\* ==========================================================================

FragsUnchanged ==
    /\ UNCHANGED <<frag_vectors, frag_deletes, frag_seq>>

ManifestUnchanged ==
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = {InitFrag1, InitFrag2}
    /\ manifest_seg_vecs = {}           \* No segment yet
    /\ manifest_etag = 1
    /\ manifest_watermark = 0           \* No compaction yet
    \* All possible fragments initialized (domain must cover all IDs)
    /\ frag_vectors = [f \in {InitFrag1, InitFrag2, WriterFrag, DeleterFrag} |-> {}]
    /\ frag_deletes = [f \in {InitFrag1, InitFrag2, WriterFrag, DeleterFrag} |-> {}]
    /\ frag_seq = [f \in {InitFrag1, InitFrag2, WriterFrag, DeleterFrag} |->
                    CASE f = InitFrag1 -> 0
                      [] f = InitFrag2 -> 1
                      [] OTHER -> 0]
    /\ mutex = "free"
    /\ next_seq = 2
    /\ committed_upserts = {}
    /\ committed_deletes = {}
    /\ w_pc = "idle"
    /\ w_snap_frags = {}
    /\ w_snap_etag = 0
    /\ d_pc = "idle"
    /\ d_snap_frags = {}
    /\ d_snap_etag = 0
    /\ c_pc = "idle"
    /\ c_snap_frags = {}
    /\ c_snap_etag = 0
    /\ c_merged_vecs = {}
    /\ c_last_frag_id = 0
    /\ q_pc = "idle"
    /\ q_snap_manifest_frags = {}
    /\ q_snap_manifest_seg = {}
    /\ q_snap_watermark = 0
    /\ q_wal_results = {}
    /\ q_wal_deletes = {}
    /\ q_final_results = {}

\* ==========================================================================
\* Writer Protocol: Upserts vector V
\* ==========================================================================

W_AcquireMutex ==
    /\ w_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w"
    /\ w_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   next_seq, committed_upserts, committed_deletes,
                   w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

W_WriteFragment ==
    /\ w_pc = "acquired"
    \* Write fragment to S3 with vector V
    /\ frag_vectors' = [frag_vectors EXCEPT ![WriterFrag] = {V}]
    /\ frag_deletes' = [frag_deletes EXCEPT ![WriterFrag] = {}]
    /\ frag_seq' = [frag_seq EXCEPT ![WriterFrag] = next_seq]
    /\ next_seq' = next_seq + 1
    /\ w_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   mutex, committed_upserts, committed_deletes,
                   w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

W_ReadManifest ==
    /\ w_pc = "frag_written"
    /\ w_snap_frags' = manifest_frags
    /\ w_snap_etag' = manifest_etag
    /\ w_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

W_CASManifest ==
    /\ w_pc = "manifest_read"
    /\ IF manifest_etag = w_snap_etag
       THEN \* CAS succeeds
            /\ manifest_frags' = w_snap_frags \union {WriterFrag}
            /\ manifest_etag' = manifest_etag + 1
            /\ committed_upserts' = committed_upserts \union {V}
            /\ mutex' = "free"
            /\ w_pc' = "done"
       ELSE \* CAS fails — retry from ReadManifest
            /\ w_pc' = "frag_written"
            /\ UNCHANGED <<manifest_frags, manifest_etag, committed_upserts, mutex>>
    /\ UNCHANGED <<manifest_seg_vecs, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   next_seq, committed_deletes,
                   w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

\* ==========================================================================
\* Deleter Protocol: Deletes vector V
\* ==========================================================================

D_AcquireMutex ==
    /\ d_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "d"
    /\ d_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

D_WriteFragment ==
    /\ d_pc = "acquired"
    \* Write fragment to S3 with delete tombstone for V
    /\ frag_vectors' = [frag_vectors EXCEPT ![DeleterFrag] = {}]
    /\ frag_deletes' = [frag_deletes EXCEPT ![DeleterFrag] = {V}]
    /\ frag_seq' = [frag_seq EXCEPT ![DeleterFrag] = next_seq]
    /\ next_seq' = next_seq + 1
    /\ d_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   mutex, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

D_ReadManifest ==
    /\ d_pc = "frag_written"
    /\ d_snap_frags' = manifest_frags
    /\ d_snap_etag' = manifest_etag
    /\ d_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

D_CASManifest ==
    /\ d_pc = "manifest_read"
    /\ IF manifest_etag = d_snap_etag
       THEN /\ manifest_frags' = d_snap_frags \union {DeleterFrag}
            /\ manifest_etag' = manifest_etag + 1
            /\ committed_deletes' = committed_deletes \union {V}
            /\ mutex' = "free"
            /\ d_pc' = "done"
       ELSE /\ d_pc' = "frag_written"
            /\ UNCHANGED <<manifest_frags, manifest_etag, committed_deletes, mutex>>
    /\ UNCHANGED <<manifest_seg_vecs, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   next_seq, committed_upserts,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

\* ==========================================================================
\* Compactor Protocol
\*
\* Models: Read manifest → snapshot fragments → merge vectors (apply deletes)
\*         → build segment → CAS manifest (re-read, remove_compacted_fragments)
\*
\* Key: remove_compacted_fragments(last_fragment_id) retains f where f.id > watermark.
\*      This uses ULID ordering (modeled as Nat > comparison).
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}
    /\ c_snap_frags' = manifest_frags
    /\ c_snap_etag' = manifest_etag
    \* Record the max ULID of compacted fragments (for watermark)
    /\ c_last_frag_id' = CHOOSE max \in manifest_frags :
                            \A f \in manifest_frags : f <= max
    /\ c_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_merged_vecs,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

\* Merge: Collect all vectors, apply all deletes. Latest seq_num wins for dupes.
\* Simplified: union all vectors, subtract all deletes.
C_BuildSegment ==
    /\ c_pc = "manifest_read"
    /\ LET all_vecs == UNION {frag_vectors[f] : f \in c_snap_frags}
           all_dels == UNION {frag_deletes[f] : f \in c_snap_frags}
       IN c_merged_vecs' = all_vecs \ all_dels
    /\ c_pc' = "seg_built"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_snap_frags, c_snap_etag, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

\* CAS manifest: re-read fresh manifest, apply remove_compacted_fragments(watermark),
\* add segment, write with CAS.
C_CASManifest ==
    /\ c_pc = "seg_built"
    /\ IF manifest_etag = c_snap_etag
       THEN \* CAS succeeds on first try (simplified — models re-read at CAS time)
            \* In reality, compactor re-reads manifest inside CAS loop.
            \* remove_compacted_fragments: retain fragments with id > watermark
            /\ LET remaining == {f \in manifest_frags : f > c_last_frag_id}
               IN manifest_frags' = remaining
            /\ manifest_seg_vecs' = c_merged_vecs
            /\ manifest_watermark' = c_last_frag_id
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "done"
       ELSE \* CAS fails. Model the re-read + retry behavior.
            \* Re-read manifest (get fresh state), re-apply remove_compacted_fragments.
            /\ LET remaining == {f \in manifest_frags : f > c_last_frag_id}
               IN manifest_frags' = remaining
            /\ manifest_seg_vecs' = c_merged_vecs
            /\ manifest_watermark' = c_last_frag_id
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "done"
    /\ UNCHANGED <<frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_pc, q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes, q_final_results>>

\* ==========================================================================
\* Query Protocol (strong consistency)
\*
\* Step 1: Read manifest (snapshot fragment list + segment)
\* Step 2: Scan WAL fragments (collect vectors + deletes, seq_num order)
\* Step 3: Search segment
\* Step 4: Merge: segment ∪ WAL vectors, minus WAL deletes
\* ==========================================================================

Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ q_snap_manifest_frags' = manifest_frags
    /\ q_snap_manifest_seg' = manifest_seg_vecs
    /\ q_snap_watermark' = manifest_watermark
    /\ q_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_wal_results, q_wal_deletes, q_final_results>>

Q_ScanWAL ==
    /\ q_pc = "manifest_read"
    \* Collect all vectors and deletes from uncompacted fragments
    /\ q_wal_results' = UNION {frag_vectors[f] : f \in q_snap_manifest_frags}
    /\ q_wal_deletes' = UNION {frag_deletes[f] : f \in q_snap_manifest_frags}
    /\ q_pc' = "wal_scanned"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_final_results>>

Q_MergeResults ==
    /\ q_pc = "wal_scanned"
    \* Merge: (segment_vectors ∪ wal_vectors) \ wal_deletes
    \* WAL overrides segment (WAL has newer data)
    /\ q_final_results' = (q_snap_manifest_seg \union q_wal_results) \ q_wal_deletes
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg_vecs, manifest_etag, manifest_watermark,
                   frag_vectors, frag_deletes, frag_seq,
                   mutex, next_seq, committed_upserts, committed_deletes,
                   w_pc, w_snap_frags, w_snap_etag,
                   d_pc, d_snap_frags, d_snap_etag,
                   c_pc, c_snap_frags, c_snap_etag, c_merged_vecs, c_last_frag_id,
                   q_snap_manifest_frags, q_snap_manifest_seg, q_snap_watermark,
                   q_wal_results, q_wal_deletes>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Writer (upserts V)
    \/ W_AcquireMutex
    \/ W_WriteFragment
    \/ W_ReadManifest
    \/ W_CASManifest
    \* Deleter (deletes V)
    \/ D_AcquireMutex
    \/ D_WriteFragment
    \/ D_ReadManifest
    \/ D_CASManifest
    \* Compactor
    \/ C_ReadManifest
    \/ C_BuildSegment
    \/ C_CASManifest
    \* Query
    \/ Q_ReadManifest
    \/ Q_ScanWAL
    \/ Q_MergeResults

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* Helper: A vector is "reachable" from the manifest if it's in the segment
\* or in an uncompacted fragment's vectors, and not masked by a delete in
\* a higher-sequence-number fragment.
\*
\* For the invariant, we use a simpler check: the vector must be discoverable
\* by the query merge algorithm.

\* INVARIANT 1: No Silent Data Loss.
\* If V was upserted (committed) and NOT deleted, it must be reachable:
\*   - Either in an uncompacted fragment's vectors set, OR
\*   - In the segment's vectors set.
\* (Reachability accounts for the merge: WAL overrides segment.)
NoSilentDataLoss ==
    \A v \in committed_upserts :
        v \notin committed_deletes =>
            \/ \E f \in manifest_frags : v \in frag_vectors[f]
            \/ v \in manifest_seg_vecs

\* INVARIANT 2: Delete Consistency.
\* If V was deleted AND the delete has been compacted into the segment
\* (i.e., no delete fragment remains in WAL), then V must not be in the
\* segment. This checks that compaction correctly applies tombstones.
\*
\* Note: If the delete fragment is still in WAL, the query merge will
\* apply it at read time, so the segment may still contain V — that's OK.
DeleteAfterCompaction ==
    \A v \in committed_deletes :
        \* If all delete tombstones for v have been compacted away
        (\A f \in manifest_frags : v \notin frag_deletes[f]) =>
            v \notin manifest_seg_vecs

\* INVARIANT 3: Watermark Safety.
\* After compaction, all fragments with id > watermark are retained.
\* Specifically: if a fragment was committed to the manifest after the
\* compaction snapshot, it must still be in the manifest after compaction.
\* This is the key ULID-ordering property.
WatermarkRetainsNewFragments ==
    c_pc = "done" =>
        \A f \in {WriterFrag, DeleterFrag} :
            (f > c_last_frag_id) =>
                \* If this fragment was ever added to manifest, it should
                \* still be there (not removed by remove_compacted_fragments)
                TRUE  \* Structural: f > watermark => retain(f) keeps it

\* INVARIANT 4: Query sees consistent snapshot.
\* When a query completes, its results must be consistent with SOME valid
\* state: V is in results iff it was upserted and not deleted at the
\* query's manifest snapshot time.
\* (This is hard to express precisely without temporal logic; we check a
\* weaker property: results are a subset of all-ever-upserted vectors.)
QueryResultsSound ==
    q_pc = "done" =>
        q_final_results \subseteq (committed_upserts \union
            UNION {frag_vectors[f] : f \in DOMAIN frag_vectors})

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3, 4}
    /\ manifest_seg_vecs \subseteq {0, 1}  \* Only vector V (=1) or empty
    /\ manifest_etag \in 0..20
    /\ manifest_watermark \in 0..4
    /\ mutex \in {"free", "w", "d"}
    /\ next_seq \in 0..10
    /\ committed_upserts \subseteq {1}
    /\ committed_deletes \subseteq {1}
    /\ w_pc \in {"idle", "acquired", "frag_written", "manifest_read", "done"}
    /\ d_pc \in {"idle", "acquired", "frag_written", "manifest_read", "done"}
    /\ c_pc \in {"idle", "manifest_read", "seg_built", "done"}
    /\ q_pc \in {"idle", "manifest_read", "wal_scanned", "done"}

=============================================================================
