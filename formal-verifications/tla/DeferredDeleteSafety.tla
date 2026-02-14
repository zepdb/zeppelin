------------------------- MODULE DeferredDeleteSafety -------------------------
\* Formal verification of deferred deletion safety across compaction cycles.
\*
\* MODELS: 2 sequential compaction cycles (C1, C2) + 1 long-running query
\*         + 1 writer (adds fragments between cycles).
\*
\* THE MECHANISM:
\*   When compaction C1 merges fragments [F1, F2] into segment S1:
\*     1. C1 does NOT delete F1.wal, F2.wal from S3 immediately.
\*     2. C1 puts [F1.wal, F2.wal] into manifest.pending_deletes.
\*     3. C2 starts: step 0 deletes everything in pending_deletes from S3.
\*     4. C2 then builds segment S2, sets new pending_deletes.
\*
\*   This gives queries that started before C1 finished a full cycle to
\*   complete before their fragment files are deleted.
\*
\* EXPECTED FINDINGS:
\*   - One-cycle queries are safe (fragments exist throughout query).
\*   - Two-cycle queries may hit 404 (EXPECTED bounded-staleness trade-off).
\*   - Segment S1 remains available until C2's GC in the NEXT cycle.
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 state ---
    manifest_frags,         \* Set of Nat: fragment IDs in manifest
    manifest_seg,           \* Nat: active segment ID (0 = none)
    manifest_seg_data,      \* Set of Nat: vector IDs in active segment
    manifest_pending_del,   \* Set of String: S3 keys pending deletion
    manifest_etag,          \* Nat: version counter

    s3_fragments,           \* Set of Nat: fragment .wal files on S3
    s3_segments,            \* Set of Nat: segment IDs on S3

    \* --- Writer (adds fragment between C1 and C2) ---
    wr_pc,

    \* --- Compaction cycle 1 ---
    c1_pc,
    c1_snap_frags,          \* C1's snapshot of fragments
    c1_seg_id,              \* Segment ID built by C1

    \* --- Compaction cycle 2 ---
    c2_pc,
    c2_snap_frags,          \* C2's snapshot of fragments
    c2_seg_id,              \* Segment ID built by C2

    \* --- Long-running query ---
    q_pc,
    q_snap_frags,           \* Query's manifest snapshot: fragment IDs
    q_snap_seg,             \* Query's manifest snapshot: segment ID
    q_frag_reads_ok,        \* Set of Nat: fragments successfully read
    q_seg_read_ok,          \* BOOLEAN: segment read succeeded
    q_frag_404              \* Set of Nat: fragments that returned 404

vars == <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
          manifest_etag, s3_fragments, s3_segments,
          wr_pc,
          c1_pc, c1_snap_frags, c1_seg_id,
          c2_pc, c2_snap_frags, c2_seg_id,
          q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Initial fragments
F1 == 1
F2 == 2
\* Fragment added by writer between C1 and C2
F3 == 3
\* Segment IDs
S1 == 10     \* Built by C1
S2 == 11     \* Built by C2

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = {F1, F2}
    /\ manifest_seg = 0                \* No segment yet
    /\ manifest_seg_data = {}
    /\ manifest_pending_del = {}       \* No pending deletes
    /\ manifest_etag = 1
    /\ s3_fragments = {F1, F2}
    /\ s3_segments = {}
    /\ wr_pc = "idle"
    /\ c1_pc = "idle"
    /\ c1_snap_frags = {}
    /\ c1_seg_id = S1
    /\ c2_pc = "idle"
    /\ c2_snap_frags = {}
    /\ c2_seg_id = S2
    /\ q_pc = "idle"
    /\ q_snap_frags = {}
    /\ q_snap_seg = 0
    /\ q_frag_reads_ok = {}
    /\ q_seg_read_ok = FALSE
    /\ q_frag_404 = {}

\* ==========================================================================
\* Compaction Cycle 1: Compact F1, F2 into S1
\* ==========================================================================

\* Step 0: GC — delete pending_deletes from S3 (first cycle has none)
C1_GC ==
    /\ c1_pc = "idle"
    /\ s3_fragments' = s3_fragments \ manifest_pending_del
    /\ s3_segments' = s3_segments    \* Would also delete old segment keys
    /\ c1_pc' = "gc_done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag,
                   wr_pc,
                   c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* Step 1: Read manifest, snapshot fragments
C1_ReadManifest ==
    /\ c1_pc = "gc_done"
    /\ manifest_frags /= {}
    /\ c1_snap_frags' = manifest_frags
    /\ c1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments, s3_segments,
                   wr_pc,
                   c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* Step 2: Build segment S1, upload to S3
C1_BuildAndUpload ==
    /\ c1_pc = "manifest_read"
    /\ s3_segments' = s3_segments \union {S1}
    /\ c1_pc' = "seg_uploaded"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments,
                   wr_pc,
                   c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* Step 3: CAS manifest — set segment, remove compacted frags, set pending_deletes
C1_CASManifest ==
    /\ c1_pc = "seg_uploaded"
    \* Remove compacted fragments, add segment, set pending_deletes
    /\ LET remaining == manifest_frags \ c1_snap_frags
       IN manifest_frags' = remaining
    /\ manifest_seg' = S1
    /\ manifest_seg_data' = {F1, F2}   \* Segment contains data from F1, F2
    /\ manifest_pending_del' = c1_snap_frags   \* Defer deletion of old frags
    /\ manifest_etag' = manifest_etag + 1
    /\ c1_pc' = "done"
    /\ UNCHANGED <<s3_fragments, s3_segments,
                   wr_pc,
                   c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* ==========================================================================
\* Writer: Adds fragment F3 (between C1 and C2)
\* ==========================================================================

Wr_WriteAndCommit ==
    /\ wr_pc = "idle"
    /\ c1_pc = "done"                  \* Only after C1 finishes
    /\ s3_fragments' = s3_fragments \union {F3}
    /\ manifest_frags' = manifest_frags \union {F3}
    /\ manifest_etag' = manifest_etag + 1
    /\ wr_pc' = "done"
    /\ UNCHANGED <<manifest_seg, manifest_seg_data, manifest_pending_del,
                   s3_segments,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* ==========================================================================
\* Compaction Cycle 2: Compact remaining frags + S1 into S2
\* ==========================================================================

\* Step 0: GC — delete pending_deletes from S3
\* THIS IS THE DANGEROUS STEP: deletes F1.wal, F2.wal from S3
C2_GC ==
    /\ c2_pc = "idle"
    /\ c1_pc = "done"                  \* C2 starts after C1
    /\ s3_fragments' = s3_fragments \ manifest_pending_del
    /\ c2_pc' = "gc_done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

C2_ReadManifest ==
    /\ c2_pc = "gc_done"
    /\ manifest_frags /= {}
    /\ c2_snap_frags' = manifest_frags
    /\ c2_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

C2_BuildAndUpload ==
    /\ c2_pc = "manifest_read"
    /\ s3_segments' = s3_segments \union {S2}
    /\ c2_pc' = "seg_uploaded"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

C2_CASManifest ==
    /\ c2_pc = "seg_uploaded"
    /\ LET remaining == manifest_frags \ c2_snap_frags
       IN manifest_frags' = remaining
    /\ manifest_seg' = S2
    /\ manifest_seg_data' = {F1, F2, F3}   \* S2 merges S1 + F3
    \* Pending: old segment S1 keys + compacted fragment F3
    /\ manifest_pending_del' = c2_snap_frags \union {S1}
    /\ manifest_etag' = manifest_etag + 1
    /\ c2_pc' = "done"
    /\ UNCHANGED <<s3_fragments, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_snap_frags, c2_seg_id,
                   q_pc, q_snap_frags, q_snap_seg, q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* ==========================================================================
\* Query: Long-running, can span both compaction cycles
\*
\* The query reads the manifest ONCE, then reads fragments and segments.
\* If the query started before C1 and reads fragments after C2's GC,
\* the fragments may be gone (404).
\* ==========================================================================

Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ q_snap_frags' = manifest_frags
    /\ q_snap_seg' = manifest_seg
    /\ q_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_frag_reads_ok, q_seg_read_ok, q_frag_404>>

\* Read each fragment from S3. Some may be 404 if GC ran.
Q_ReadFragments ==
    /\ q_pc = "manifest_read"
    /\ q_frag_reads_ok' = q_snap_frags \intersect s3_fragments
    /\ q_frag_404' = q_snap_frags \ s3_fragments
    /\ q_pc' = "frags_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_snap_frags, q_snap_seg, q_seg_read_ok>>

\* Read segment from S3 (if snapshot has one).
Q_ReadSegment ==
    /\ q_pc = "frags_read"
    /\ q_seg_read_ok' = (q_snap_seg = 0 \/ q_snap_seg \in s3_segments)
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_seg_data, manifest_pending_del,
                   manifest_etag, s3_fragments, s3_segments,
                   wr_pc,
                   c1_pc, c1_snap_frags, c1_seg_id,
                   c2_pc, c2_snap_frags, c2_seg_id,
                   q_snap_frags, q_snap_seg, q_frag_reads_ok, q_frag_404>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Compaction C1
    \/ C1_GC
    \/ C1_ReadManifest
    \/ C1_BuildAndUpload
    \/ C1_CASManifest
    \* Writer
    \/ Wr_WriteAndCommit
    \* Compaction C2
    \/ C2_GC
    \/ C2_ReadManifest
    \/ C2_BuildAndUpload
    \/ C2_CASManifest
    \* Query
    \/ Q_ReadManifest
    \/ Q_ReadFragments
    \/ Q_ReadSegment

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: One-Cycle Safety.
\* A query that reads the manifest AFTER C1 starts but BEFORE C2's GC
\* should never hit 404 on any fragment. Deferred deletion guarantees this.
\*
\* More precisely: if C2's GC has NOT run yet, all fragments in any
\* manifest snapshot should still exist on S3.
OneCycleSafety ==
    c2_pc \in {"idle"} =>
        \* All manifest fragments exist on S3
        manifest_frags \subseteq s3_fragments

\* INVARIANT 2: Manifest references are valid on S3.
\* At any time, all fragment IDs in the manifest should exist on S3.
\* This CAN be violated after C2's GC if manifest is stale — but the
\* manifest is updated atomically, so if the manifest says frags exist,
\* they should exist.
ManifestFragsExistOnS3 ==
    manifest_frags \subseteq s3_fragments

\* INVARIANT 3: Active segment exists on S3.
\* The active segment referenced by the manifest must exist on S3.
ActiveSegmentOnS3 ==
    manifest_seg /= 0 => manifest_seg \in s3_segments

\* INVARIANT 4: Two-Cycle Query May 404 (EXPECTED VIOLATION).
\* A query that started before C1 and reads fragments after C2's GC
\* may find its fragment files deleted. TLC should find a trace where
\* q_frag_404 is non-empty — this is the known bounded-staleness trade-off.
\*
\* INTENTIONALLY VIOLATED — use this to generate the counterexample trace.
QueryNever404 ==
    q_pc = "done" => q_frag_404 = {}

\* INVARIANT 5: Segment remains until next GC cycle.
\* After C1 finishes, segment S1 should remain on S3 until C2's GC
\* in the NEXT compaction cycle (which would delete S1 if it's in
\* pending_deletes).
SegmentSurvivesOneCycle ==
    (c1_pc = "done" /\ c2_pc \in {"idle", "gc_done", "manifest_read"}) =>
        S1 \in s3_segments

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3}
    /\ manifest_seg \in {0, 10, 11}
    /\ manifest_pending_del \subseteq {1, 2, 3, 10}
    /\ manifest_etag \in 0..10
    /\ s3_fragments \subseteq {1, 2, 3}
    /\ s3_segments \subseteq {10, 11}
    /\ wr_pc \in {"idle", "done"}
    /\ c1_pc \in {"idle", "gc_done", "manifest_read", "seg_uploaded", "done"}
    /\ c2_pc \in {"idle", "gc_done", "manifest_read", "seg_uploaded", "done"}
    /\ q_pc \in {"idle", "manifest_read", "frags_read", "done"}

=============================================================================
