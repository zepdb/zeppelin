--------------------------- MODULE CompactionSafety ---------------------------
\* Formal verification of Zeppelin's compaction protocol.
\*
\* MODELS: Writer (WalWriter::append) + Compactor (Compactor::compact)
\* FINDS:  The append-during-compaction data loss bug.
\*
\* ==========================================================================
\* THE BUG:
\*   The compactor reads the manifest, does expensive work (building an
\*   IVF-Flat segment), then writes an updated manifest. It does NOT hold
\*   the namespace mutex during this window. A concurrent WalWriter::append
\*   can acquire the mutex, append a new fragment, and update the manifest.
\*   When the compactor then writes its (stale) manifest, it OVERWRITES the
\*   writer's update — the newly appended fragment reference is silently lost.
\*
\*   Code references:
\*     - src/compaction/mod.rs: reads manifest at line 80, writes at line 199
\*     - src/wal/writer.rs: holds per-namespace mutex only during append
\*
\* CLIENT INVARIANT:
\*   If upsert returns HTTP 200, the vector is visible to all subsequent
\*   strong queries — forever (until explicitly deleted or compacted into
\*   a segment that includes it).
\*
\* EXPECTED RESULT:
\*   TLC finds a counterexample trace where a committed fragment is lost
\*   from the manifest after compaction overwrites the writer's update.
\* ==========================================================================

EXTENDS Naturals, FiniteSets, Sequences

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state (the "ground truth") ---
    manifest_frags,     \* Set of Nat: fragment IDs in the manifest on S3
    manifest_seg,       \* Set of Nat: fragment IDs covered by the active segment
                        \*   (empty set = no segment; non-empty = segment exists)
    s3_frag_data,       \* Set of Nat: fragment IDs whose .wal files exist on S3

    \* --- Concurrency control ---
    mutex,              \* "free" | "w1" | "w2" (per-namespace mutex from WalWriter)

    \* --- Durability tracking ---
    committed,          \* Set of Nat: fragment IDs whose append returned HTTP 200

    \* --- Writer w1 local state ---
    w1_pc,              \* Program counter: phase of the append protocol
    w1_snap,            \* w1's local snapshot of manifest_frags

    \* --- Writer w2 local state ---
    w2_pc,
    w2_snap,

    \* --- Compactor local state ---
    c_pc,               \* Program counter: phase of the compact protocol
    c_snap,             \* Compactor's snapshot of manifest_frags (read at start)
    c_seg_contents      \* Set of fragment IDs the new segment was built from

vars == <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
          w1_pc, w1_snap, w2_pc, w2_snap, c_pc, c_snap, c_seg_contents>>

\* ==========================================================================
\* Constants (hardcoded for finite model checking)
\* ==========================================================================

\* Initial fragments already committed and in the manifest.
InitFragments == {1, 2}

\* Fragment IDs that writers will create.
W1_Fragment == 3    \* Writer w1 creates fragment 3
W2_Fragment == 4    \* Writer w2 creates fragment 4

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = InitFragments
    /\ manifest_seg = {}                    \* No segment yet
    /\ s3_frag_data = InitFragments         \* Fragment data exists on S3
    /\ mutex = "free"
    /\ committed = InitFragments            \* Initial fragments are committed
    /\ w1_pc = "idle"
    /\ w1_snap = {}
    /\ w2_pc = "idle"
    /\ w2_snap = {}
    /\ c_pc = "idle"
    /\ c_snap = {}
    /\ c_seg_contents = {}

\* ==========================================================================
\* Writer w1 Protocol (models WalWriter::append from src/wal/writer.rs)
\*
\* Step 1: Acquire per-namespace mutex          (writer.rs:47)
\* Step 2: Write fragment data to S3            (writer.rs:58)
\* Step 3: Read manifest from S3                (writer.rs:68)
\* Step 4: Add fragment ref, write manifest     (writer.rs:72-78)
\* Step 5: Release mutex, commit                (writer.rs:47 — guard drop)
\* ==========================================================================

W1_AcquireMutex ==
    /\ w1_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w1"
    /\ w1_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, committed,
                   w1_snap, w2_pc, w2_snap, c_pc, c_snap, c_seg_contents>>

W1_WriteFragmentToS3 ==
    /\ w1_pc = "acquired"
    /\ s3_frag_data' = s3_frag_data \union {W1_Fragment}
    /\ w1_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg, mutex, committed,
                   w1_snap, w2_pc, w2_snap, c_pc, c_snap, c_seg_contents>>

W1_ReadManifest ==
    /\ w1_pc = "frag_written"
    /\ w1_snap' = manifest_frags               \* Snapshot current S3 manifest
    /\ w1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w2_pc, w2_snap, c_pc, c_snap, c_seg_contents>>

W1_WriteManifestAndRelease ==
    /\ w1_pc = "manifest_read"
    /\ manifest_frags' = w1_snap \union {W1_Fragment}  \* Add our fragment
    /\ mutex' = "free"                                  \* Release mutex
    /\ committed' = committed \union {W1_Fragment}      \* HTTP 200 returned
    /\ w1_pc' = "done"
    /\ UNCHANGED <<manifest_seg, s3_frag_data,
                   w1_snap, w2_pc, w2_snap, c_pc, c_snap, c_seg_contents>>

\* ==========================================================================
\* Writer w2 Protocol (symmetric to w1)
\* ==========================================================================

W2_AcquireMutex ==
    /\ w2_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w2"
    /\ w2_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, committed,
                   w1_pc, w1_snap, w2_snap, c_pc, c_snap, c_seg_contents>>

W2_WriteFragmentToS3 ==
    /\ w2_pc = "acquired"
    /\ s3_frag_data' = s3_frag_data \union {W2_Fragment}
    /\ w2_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg, mutex, committed,
                   w1_pc, w1_snap, w2_snap, c_pc, c_snap, c_seg_contents>>

W2_ReadManifest ==
    /\ w2_pc = "frag_written"
    /\ w2_snap' = manifest_frags
    /\ w2_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, c_pc, c_snap, c_seg_contents>>

W2_WriteManifestAndRelease ==
    /\ w2_pc = "manifest_read"
    /\ manifest_frags' = w2_snap \union {W2_Fragment}
    /\ mutex' = "free"
    /\ committed' = committed \union {W2_Fragment}
    /\ w2_pc' = "done"
    /\ UNCHANGED <<manifest_seg, s3_frag_data,
                   w1_pc, w1_snap, w2_snap, c_pc, c_snap, c_seg_contents>>

\* ==========================================================================
\* Compactor Protocol (models Compactor::compact from src/compaction/mod.rs)
\*
\* CRITICAL: The compactor does NOT hold the namespace mutex.
\*
\* Step 1: Read manifest from S3                (mod.rs:80)
\* Step 2: Build segment from snapshot fragments (mod.rs:101-188)
\*         (This is the expensive step — seconds to minutes in production.
\*          Other processes can modify the manifest during this window.)
\* Step 3: Write updated manifest to S3          (mod.rs:191-199)
\*         (The compactor writes its LOCAL modified manifest, not the
\*          current S3 manifest. This is a blind PUT, not a CAS.)
\* Step 4: Delete old fragment .wal files        (mod.rs:201-207)
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}                     \* Only compact if fragments exist
    /\ c_snap' = manifest_frags                 \* Snapshot (NO MUTEX HELD)
    /\ c_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, w2_pc, w2_snap, c_seg_contents>>

C_BuildSegment ==
    /\ c_pc = "manifest_read"
    /\ c_seg_contents' = c_snap                 \* Segment covers snapshot frags
    /\ c_pc' = "seg_built"
    /\ UNCHANGED <<manifest_frags, manifest_seg, s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, w2_pc, w2_snap, c_snap>>

\* THIS IS THE BUG: The compactor writes its stale local manifest to S3,
\* overwriting any concurrent updates from writers.
\*
\* In the code (mod.rs:191-199):
\*   manifest.add_segment(SegmentRef { ... });
\*   manifest.remove_compacted_fragments(last_fragment_id);
\*   manifest.write(&self.store, namespace).await?;
\*
\* The `manifest` variable here is the compactor's LOCAL copy read at line 80.
\* It does NOT contain any fragments added by writers since line 80.
\* The write() call REPLACES the entire manifest on S3.
C_WriteManifest ==
    /\ c_pc = "seg_built"
    \* The compactor's local manifest has:
    \*   - fragments: empty (all snapshot frags removed by remove_compacted_fragments)
    \*   - active_segment: the new segment
    \* This REPLACES whatever is currently on S3 — any fragments added
    \* by concurrent writers since C_ReadManifest are SILENTLY LOST.
    /\ manifest_frags' = {}
    /\ manifest_seg' = c_seg_contents
    /\ c_pc' = "manifest_written"
    /\ UNCHANGED <<s3_frag_data, mutex, committed,
                   w1_pc, w1_snap, w2_pc, w2_snap, c_snap, c_seg_contents>>

C_DeleteOldFragments ==
    /\ c_pc = "manifest_written"
    /\ s3_frag_data' = s3_frag_data \ c_snap   \* Delete compacted .wal files
    /\ c_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, mutex, committed,
                   w1_pc, w1_snap, w2_pc, w2_snap, c_snap, c_seg_contents>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Writer w1
    \/ W1_AcquireMutex
    \/ W1_WriteFragmentToS3
    \/ W1_ReadManifest
    \/ W1_WriteManifestAndRelease
    \* Writer w2
    \/ W2_AcquireMutex
    \/ W2_WriteFragmentToS3
    \/ W2_ReadManifest
    \/ W2_WriteManifestAndRelease
    \* Compactor
    \/ C_ReadManifest
    \/ C_BuildSegment
    \/ C_WriteManifest
    \/ C_DeleteOldFragments

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* A fragment is "reachable" if the manifest points to it — either directly
\* as an uncompacted fragment, or indirectly via a segment that was built
\* from it.
FragmentReachable(f) ==
    \/ f \in manifest_frags                     \* Listed in manifest fragments
    \/ f \in manifest_seg                       \* Covered by the active segment

\* CRITICAL INVARIANT: Every committed fragment must be reachable via the
\* manifest. If this is violated, data has been silently lost.
\*
\* Expected: TLC finds a counterexample. The trace will show:
\*   1. Compactor reads manifest (snapshot: {1, 2})
\*   2. Writer w1 appends fragment 3, updates manifest to {1, 2, 3}
\*   3. Writer w1 commits (fragment 3 got HTTP 200)
\*   4. Compactor writes stale manifest (fragments: {}, segment: {1, 2})
\*   5. Fragment 3 is committed but NOT reachable — VIOLATION
NoSilentDataLoss ==
    \A f \in committed : FragmentReachable(f)

\* SECONDARY INVARIANT: Fragment data on S3 should be a superset of what
\* the manifest references. (Verifies we don't delete files still needed.)
ManifestRefsExistOnS3 ==
    manifest_frags \subseteq s3_frag_data

\* TYPE INVARIANT: Basic type checking for state variables.
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3, 4}
    /\ manifest_seg \subseteq {1, 2, 3, 4}
    /\ s3_frag_data \subseteq {1, 2, 3, 4}
    /\ mutex \in {"free", "w1", "w2"}
    /\ committed \subseteq {1, 2, 3, 4}
    /\ w1_pc \in {"idle", "acquired", "frag_written", "manifest_read", "done"}
    /\ w2_pc \in {"idle", "acquired", "frag_written", "manifest_read", "done"}
    /\ c_pc \in {"idle", "manifest_read", "seg_built", "manifest_written", "done"}

\* ==========================================================================
\* Candidate Fix: CAS Manifest Write
\*
\* To verify that a CAS-based fix resolves the bug, replace C_WriteManifest
\* with C_WriteManifestCAS below. The compactor reads the current manifest
\* before writing and aborts if it has changed since the snapshot.
\*
\* C_WriteManifestCAS ==
\*     /\ c_pc = "seg_built"
\*     /\ IF manifest_frags = c_snap     \* CAS: check manifest unchanged
\*        THEN /\ manifest_frags' = {}
\*             /\ manifest_seg' = c_seg_contents
\*             /\ c_pc' = "manifest_written"
\*        ELSE /\ c_pc' = "idle"         \* Abort and retry
\*             /\ UNCHANGED <<manifest_frags, manifest_seg>>
\*     /\ UNCHANGED <<s3_frag_data, mutex, committed,
\*                    w1_pc, w1_snap, w2_pc, w2_snap, c_snap, c_seg_contents>>
\*
\* Candidate Fix: Re-read and Merge
\*
\* The compactor re-reads the manifest before writing and merges any new
\* fragments that appeared since the snapshot.
\*
\* C_WriteManifestMerge ==
\*     /\ c_pc = "seg_built"
\*     \* Re-read manifest and keep any fragments NOT in our snapshot
\*     /\ LET new_frags == manifest_frags \ c_snap
\*        IN /\ manifest_frags' = new_frags
\*           /\ manifest_seg' = c_seg_contents
\*     /\ c_pc' = "manifest_written"
\*     /\ UNCHANGED <<s3_frag_data, mutex, committed,
\*                    w1_pc, w1_snap, w2_pc, w2_snap, c_snap, c_seg_contents>>
\* ==========================================================================

=============================================================================
