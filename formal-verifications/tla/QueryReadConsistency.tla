------------------------ MODULE QueryReadConsistency -------------------------
\* Formal verification of query consistency during compaction.
\*
\* MODELS: StrongQuery + Compactor + Writer
\* FINDS:  The stale-manifest-404 race where a query reads a manifest
\*         snapshot, then compaction deletes the referenced fragments
\*         before the query can read them.
\*
\* ==========================================================================
\* THE BUG:
\*   1. Query reads manifest: sees fragments [F1, F2, F3]
\*   2. Compaction runs: compacts F1-F3 into segment S1
\*   3. Compaction updates manifest: fragments=[], active_segment=S1
\*   4. Compaction deletes old .wal files: F1, F2, F3 removed from S3
\*   5. Query tries to read F1 from S3 — gets 404!
\*
\*   Code references:
\*     - src/query.rs:33   — reads manifest
\*     - src/query.rs:110  — reads fragment data via wal_reader
\*     - src/compaction/mod.rs:201-207 — deletes old fragment files
\*
\* CLIENT INVARIANT:
\*   A strong query that starts after an upsert returns 200 must either
\*   return the vector or complete without error. It must never fail with
\*   an S3 404 on a fragment the manifest told it to read.
\*
\* EXPECTED RESULT:
\*   TLC finds a counterexample where the query's fragment read hits a
\*   deleted .wal file (404).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    manifest_frags,     \* Set of Nat: fragment IDs in the manifest on S3
    manifest_seg,       \* Set of Nat: fragment IDs covered by active segment
    s3_frag_data,       \* Set of Nat: fragment .wal files that exist on S3

    \* --- Concurrency control ---
    mutex,              \* "free" | "w1" (writer mutex)

    \* --- Process tracking ---
    committed,          \* Set of Nat: committed fragment IDs

    \* --- Writer w1 local state ---
    w1_pc,
    w1_snap,

    \* --- Compactor local state ---
    c_pc,
    c_snap,
    c_seg_contents,

    \* --- Query local state ---
    q_pc,               \* Program counter for the query process
    q_snap_frags,       \* Query's snapshot of manifest fragments
    q_snap_seg,         \* Query's snapshot of active segment
    q_frag_read_ok      \* TRUE if all fragment reads succeeded (no 404)

vars == <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
          w1_pc, w1_snap, c_pc, c_snap, c_seg_contents,
          q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

\* ==========================================================================
\* Constants
\* ==========================================================================

InitFragments == {1, 2, 3}      \* 3 initial committed fragments
W1_Fragment == 4                 \* Writer creates fragment 4

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = InitFragments
    /\ manifest_seg = {}
    /\ s3_frag_data = InitFragments
    /\ mutex = "free"
    /\ committed = InitFragments
    /\ w1_pc = "idle"
    /\ w1_snap = {}
    /\ c_pc = "idle"
    /\ c_snap = {}
    /\ c_seg_contents = {}
    /\ q_pc = "idle"
    /\ q_snap_frags = {}
    /\ q_snap_seg = {}
    /\ q_frag_read_ok = TRUE

\* ==========================================================================
\* Writer Protocol
\* ==========================================================================

W1_AcquireMutex ==
    /\ w1_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w1"
    /\ w1_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, committed,
                   w1_snap, c_pc, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

W1_WriteFragmentToS3 ==
    /\ w1_pc = "acquired"
    /\ s3_frag_data' = s3_frag_data \union {W1_Fragment}
    /\ w1_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg, mutex, committed,
                   w1_snap, c_pc, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

W1_ReadManifest ==
    /\ w1_pc = "frag_written"
    /\ w1_snap' = manifest_frags
    /\ w1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   c_pc, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

W1_WriteManifestAndRelease ==
    /\ w1_pc = "manifest_read"
    /\ manifest_frags' = w1_snap \union {W1_Fragment}
    /\ mutex' = "free"
    /\ committed' = committed \union {W1_Fragment}
    /\ w1_pc' = "done"
    /\ UNCHANGED <<manifest_seg, s3_frag_data,
                   w1_snap, c_pc, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

\* ==========================================================================
\* Compactor Protocol (no mutex)
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}
    /\ c_snap' = manifest_frags
    /\ c_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

C_BuildSegment ==
    /\ c_pc = "manifest_read"
    /\ c_seg_contents' = c_snap
    /\ c_pc' = "seg_built"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_snap,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

C_WriteManifest ==
    /\ c_pc = "seg_built"
    /\ manifest_frags' = {}
    /\ manifest_seg' = c_seg_contents
    /\ c_pc' = "manifest_written"
    /\ UNCHANGED <<s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

\* THIS IS THE CRITICAL STEP for this spec:
\* The compactor deletes old .wal files from S3.
\* If a query already captured a manifest snapshot referencing these
\* fragments, the query will get 404 when it tries to read them.
C_DeleteOldFragments ==
    /\ c_pc = "manifest_written"
    /\ s3_frag_data' = s3_frag_data \ c_snap
    /\ c_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, mutex, committed,
                   w1_pc, w1_snap, c_snap, c_seg_contents,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>

\* ==========================================================================
\* Strong Query Protocol (models execute_query from src/query.rs)
\*
\* Step 1: Read manifest (capture fragment refs + active segment)
\* Step 2: Read fragment data from S3 (WAL scan for Strong consistency)
\* Step 3: Read segment (if active_segment exists)
\* Step 4: Merge results and return
\* ==========================================================================

Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ q_snap_frags' = manifest_frags           \* Snapshot manifest fragments
    /\ q_snap_seg' = manifest_seg               \* Snapshot active segment
    /\ q_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_pc, c_snap, c_seg_contents, q_frag_read_ok>>

\* The query reads all fragments referenced in its manifest snapshot.
\* If any fragment has been deleted from S3, the read fails (404).
Q_ReadFragments ==
    /\ q_pc = "manifest_read"
    /\ IF q_snap_frags \subseteq s3_frag_data
       THEN q_frag_read_ok' = TRUE              \* All fragments readable
       ELSE q_frag_read_ok' = FALSE             \* At least one fragment 404!
    /\ q_pc' = "frags_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_pc, c_snap, c_seg_contents,
                   q_snap_frags, q_snap_seg>>

Q_Return ==
    /\ q_pc = "frags_read"
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_pc, c_snap, c_seg_contents,
                   q_snap_frags, q_snap_seg, q_frag_read_ok>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ W1_AcquireMutex \/ W1_WriteFragmentToS3 \/ W1_ReadManifest
    \/ W1_WriteManifestAndRelease
    \/ C_ReadManifest \/ C_BuildSegment \/ C_WriteManifest \/ C_DeleteOldFragments
    \/ Q_ReadManifest \/ Q_ReadFragments \/ Q_Return

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* PRIMARY INVARIANT — EXPECTED TO FAIL.
\* A query should never encounter a 404 on fragments from its manifest.
\* If q_frag_read_ok becomes FALSE, the query hit a deleted fragment.
QueryNeverHitsOrphan ==
    q_frag_read_ok = TRUE

\* ALTERNATIVE: Check at the point where the query reads fragments.
\* When the query is about to read fragments, they should exist on S3.
QueryFragmentsExistWhenNeeded ==
    q_pc = "manifest_read" => q_snap_frags \subseteq s3_frag_data

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3, 4}
    /\ manifest_seg \subseteq {1, 2, 3, 4}
    /\ s3_frag_data \subseteq {1, 2, 3, 4}
    /\ mutex \in {"free", "w1"}
    /\ committed \subseteq {1, 2, 3, 4}
    /\ w1_pc \in {"idle", "acquired", "frag_written", "manifest_read", "done"}
    /\ c_pc \in {"idle", "manifest_read", "seg_built", "manifest_written", "done"}
    /\ q_pc \in {"idle", "manifest_read", "frags_read", "done"}

\* ==========================================================================
\* Candidate Fix: Deferred Fragment Deletion
\*
\* Instead of deleting fragments immediately after compaction, keep them
\* around for a grace period (or use reference counting). This ensures
\* in-flight queries can still read their snapshot.
\*
\* C_DeleteOldFragmentsDeferred ==
\*     /\ c_pc = "manifest_written"
\*     \* Don't delete fragments — leave them on S3 for in-flight queries.
\*     \* A separate garbage collector can delete them after a grace period.
\*     /\ c_pc' = "done"
\*     /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex,
\*                    committed, w1_pc, w1_snap, c_snap, c_seg_contents,
\*                    q_pc, q_snap_frags, q_snap_seg, q_frag_read_ok>>
\* ==========================================================================

=============================================================================
