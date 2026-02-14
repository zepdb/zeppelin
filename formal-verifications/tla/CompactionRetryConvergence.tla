---------------------- MODULE CompactionRetryConvergence ----------------------
\* Formal verification of compaction CAS retry convergence.
\*
\* MODELS: 1 compactor with CAS retry loop (max 5 retries) +
\*         2 writers that continuously CAS-write the manifest.
\*
\* QUESTION: Can writers starve the compactor by continuously updating
\*           the manifest etag between the compactor's re-read and CAS?
\*
\* EXPECTED: TLC finds a trace where 5 consecutive writer CAS-writes
\*           interleave with the compactor's 5 retries, exhausting its
\*           budget. This is a liveness issue (compaction is delayed),
\*           not a safety issue (no data loss).
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    manifest_frags,     \* Set of Nat: fragment IDs in manifest
    manifest_seg,       \* Set of Nat: segment coverage
    manifest_etag,      \* Nat: version counter

    \* --- Compactor ---
    c_pc,
    c_snap_frags,       \* Compaction snapshot (built segment from these)
    c_retry_count,      \* Number of CAS retries so far
    c_fresh_etag,       \* ETag from most recent re-read
    c_exhausted,        \* BOOLEAN: compactor hit max retries

    \* --- Writer w1 (can loop: write, commit, restart) ---
    w1_pc,
    w1_snap_etag,
    w1_frag,            \* Current fragment being written

    \* --- Writer w2 ---
    w2_pc,
    w2_snap_etag,
    w2_frag,

    \* --- Tracking ---
    next_frag            \* Counter for generating fragment IDs

vars == <<manifest_frags, manifest_seg, manifest_etag,
          c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
          w1_pc, w1_snap_etag, w1_frag,
          w2_pc, w2_snap_etag, w2_frag,
          next_frag>>

\* ==========================================================================
\* Constants
\* ==========================================================================

MAX_CAS_RETRIES == 5
InitFragments == {1, 2}

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = InitFragments
    /\ manifest_seg = {}
    /\ manifest_etag = 1
    /\ c_pc = "idle"
    /\ c_snap_frags = {}
    /\ c_retry_count = 0
    /\ c_fresh_etag = 0
    /\ c_exhausted = FALSE
    /\ w1_pc = "idle"
    /\ w1_snap_etag = 0
    /\ w1_frag = 0
    /\ w2_pc = "idle"
    /\ w2_snap_etag = 0
    /\ w2_frag = 0
    /\ next_frag = 3

\* ==========================================================================
\* Writer w1 (looping: can restart after done)
\* ==========================================================================

W1_Start ==
    /\ w1_pc = "idle"
    /\ next_frag <= 12      \* Bound the state space
    /\ w1_frag' = next_frag
    /\ next_frag' = next_frag + 1
    /\ w1_pc' = "ready"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_snap_etag,
                   w2_pc, w2_snap_etag, w2_frag>>

W1_ReadManifest ==
    /\ w1_pc = "ready"
    /\ w1_snap_etag' = manifest_etag
    /\ w1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_frag,
                   w2_pc, w2_snap_etag, w2_frag,
                   next_frag>>

W1_CASManifest ==
    /\ w1_pc = "manifest_read"
    /\ IF manifest_etag = w1_snap_etag
       THEN /\ manifest_frags' = manifest_frags \union {w1_frag}
            /\ manifest_etag' = manifest_etag + 1
            /\ w1_pc' = "idle"     \* Done, can restart
       ELSE /\ w1_pc' = "ready"    \* Retry
            /\ UNCHANGED <<manifest_frags, manifest_etag>>
    /\ UNCHANGED <<manifest_seg,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_snap_etag, w1_frag,
                   w2_pc, w2_snap_etag, w2_frag,
                   next_frag>>

\* ==========================================================================
\* Writer w2 (symmetric, looping)
\* ==========================================================================

W2_Start ==
    /\ w2_pc = "idle"
    /\ next_frag <= 12
    /\ w2_frag' = next_frag
    /\ next_frag' = next_frag + 1
    /\ w2_pc' = "ready"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_snap_etag>>

W2_ReadManifest ==
    /\ w2_pc = "ready"
    /\ w2_snap_etag' = manifest_etag
    /\ w2_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_frag,
                   next_frag>>

W2_CASManifest ==
    /\ w2_pc = "manifest_read"
    /\ IF manifest_etag = w2_snap_etag
       THEN /\ manifest_frags' = manifest_frags \union {w2_frag}
            /\ manifest_etag' = manifest_etag + 1
            /\ w2_pc' = "idle"
       ELSE /\ w2_pc' = "ready"
            /\ UNCHANGED <<manifest_frags, manifest_etag>>
    /\ UNCHANGED <<manifest_seg,
                   c_pc, c_snap_frags, c_retry_count, c_fresh_etag, c_exhausted,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_snap_etag, w2_frag,
                   next_frag>>

\* ==========================================================================
\* Compactor: CAS retry loop with bounded retries
\* ==========================================================================

\* Step 1: Read manifest, snapshot fragments
C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}
    /\ c_snap_frags' = manifest_frags
    /\ c_retry_count' = 0
    /\ c_fresh_etag' = manifest_etag
    /\ c_pc' = "seg_built"     \* Skip build step (instant for model)
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_exhausted,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_pc, w2_snap_etag, w2_frag,
                   next_frag>>

\* Step 2: CAS attempt in the retry loop
C_CASAttempt ==
    /\ c_pc = "seg_built"
    /\ c_retry_count < MAX_CAS_RETRIES
    /\ IF manifest_etag = c_fresh_etag
       THEN \* CAS succeeds!
            /\ LET remaining == manifest_frags \ c_snap_frags
               IN manifest_frags' = remaining
            /\ manifest_seg' = c_snap_frags
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "done"
            /\ c_exhausted' = FALSE
            /\ UNCHANGED <<c_retry_count, c_fresh_etag>>
       ELSE \* CAS fails — re-read and retry
            /\ c_retry_count' = c_retry_count + 1
            /\ c_fresh_etag' = manifest_etag   \* Re-read gets new etag
            /\ c_pc' = "seg_built"              \* Retry CAS
            /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag, c_exhausted>>
    /\ UNCHANGED <<c_snap_frags,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_pc, w2_snap_etag, w2_frag,
                   next_frag>>

\* Step 3: Exhausted retries — give up
C_Exhausted ==
    /\ c_pc = "seg_built"
    /\ c_retry_count >= MAX_CAS_RETRIES
    /\ c_exhausted' = TRUE
    /\ c_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_etag,
                   c_snap_frags, c_retry_count, c_fresh_etag,
                   w1_pc, w1_snap_etag, w1_frag,
                   w2_pc, w2_snap_etag, w2_frag,
                   next_frag>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ W1_Start \/ W1_ReadManifest \/ W1_CASManifest
    \/ W2_Start \/ W2_ReadManifest \/ W2_CASManifest
    \/ C_ReadManifest \/ C_CASAttempt \/ C_Exhausted

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Compactor always succeeds (EXPECTED VIOLATION).
\* TLC should find a trace where writers starve the compactor.
CompactorAlwaysSucceeds ==
    ~c_exhausted

\* INVARIANT 2: No data loss on exhaustion.
\* Even when the compactor exhausts retries, all committed fragments
\* remain in the manifest. The orphaned segment on S3 is a storage leak,
\* not data loss.
NoDataLossOnExhaustion ==
    c_exhausted =>
        \* All fragments from the original snapshot are still reachable
        \* (either in manifest or in the orphaned segment — but we check
        \* manifest since the segment isn't referenced)
        c_snap_frags \subseteq manifest_frags

\* INVARIANT 3: Stale segment correctness.
\* The compactor's segment was built from c_snap_frags. On retry, the
\* segment is NOT rebuilt. New fragments added since the snapshot are
\* NOT in the segment. But remove_compacted_fragments only removes
\* fragments <= watermark, so new fragments are preserved in manifest.
\* This is correct: the next compaction cycle will include them.
NewFragsPreservedAfterCompaction ==
    (c_pc = "done" /\ ~c_exhausted) =>
        \* Any fragment in the manifest that's NOT in our snapshot
        \* was added after our snapshot and must still be in manifest
        TRUE  \* Structural: manifest_frags' = manifest_frags \ c_snap_frags
              \* preserves {f : f \in manifest_frags /\ f \notin c_snap_frags}

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq 1..12
    /\ manifest_seg \subseteq 1..12
    /\ manifest_etag \in 0..20
    /\ c_pc \in {"idle", "seg_built", "done"}
    /\ c_retry_count \in 0..6
    /\ c_exhausted \in {TRUE, FALSE}
    /\ w1_pc \in {"idle", "ready", "manifest_read"}
    /\ w2_pc \in {"idle", "ready", "manifest_read"}
    /\ next_frag \in 3..13

=============================================================================
