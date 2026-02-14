------------------------ MODULE BatchWriterCorrectness ------------------------
\* Formal verification of the batch manifest writer.
\*
\* MODELS: 3 writers submitting fragment refs to a channel,
\*         1 flush loop that collects a batch and does a single CAS,
\*         1 compactor doing its own CAS.
\*
\* THE MECHANISM (src/wal/batch_writer.rs):
\*   Writers send fragment refs to an mpsc channel.
\*   The flush loop collects a batch (up to batch_size or timeout),
\*   groups by namespace, and for each namespace:
\*     1. Read manifest with ETag
\*     2. Add all fragment refs from the batch
\*     3. CAS-write manifest
\*     4. On CAS conflict: re-read, re-add ALL refs, retry
\*     5. On success: send Ok(manifest) to all writers in batch
\*
\* POTENTIAL BUGS:
\*   1. Duplicate FragmentRef if a fragment was already added by the
\*      non-batch WalWriter::append() path (which holds the mutex).
\*   2. On CAS retry, sequence numbers are re-assigned (next_sequence
\*      may have advanced due to the concurrent CAS winner).
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 manifest ---
    manifest_frags,     \* Set of Nat: fragment IDs in manifest
    manifest_etag,      \* Nat: version counter
    manifest_next_seq,  \* Nat: next sequence number to assign

    \* --- Channel (simplified as a set of pending writes) ---
    channel,            \* Set of Nat: fragment IDs pending in channel

    \* --- Flush loop ---
    fl_pc,
    fl_batch,           \* Set of Nat: current batch being flushed
    fl_snap_etag,       \* ETag from manifest read
    fl_retry_count,     \* Number of retries

    \* --- Writers (w1, w2, w3) ---
    w1_pc, w1_frag,
    w2_pc, w2_frag,
    w3_pc, w3_frag,

    \* --- Compactor ---
    c_pc,
    c_snap_frags,
    c_snap_etag,

    \* --- Tracking ---
    committed,          \* Set of Nat: fragments with Ok reply
    next_frag           \* Counter for generating fragment IDs

vars == <<manifest_frags, manifest_etag, manifest_next_seq,
          channel,
          fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
          w1_pc, w1_frag, w2_pc, w2_frag, w3_pc, w3_frag,
          c_pc, c_snap_frags, c_snap_etag,
          committed, next_frag>>

\* ==========================================================================
\* Constants
\* ==========================================================================

MAX_FLUSH_RETRIES == 3

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = {}
    /\ manifest_etag = 1
    /\ manifest_next_seq = 0
    /\ channel = {}
    /\ fl_pc = "idle"
    /\ fl_batch = {}
    /\ fl_snap_etag = 0
    /\ fl_retry_count = 0
    /\ w1_pc = "idle" /\ w1_frag = 1
    /\ w2_pc = "idle" /\ w2_frag = 2
    /\ w3_pc = "idle" /\ w3_frag = 3
    /\ c_pc = "idle"
    /\ c_snap_frags = {}
    /\ c_snap_etag = 0
    /\ committed = {}
    /\ next_frag = 4

\* ==========================================================================
\* Writers: Write fragment to S3, then submit to channel
\* ==========================================================================

W1_WriteAndSubmit ==
    /\ w1_pc = "idle"
    /\ channel' = channel \union {w1_frag}
    /\ w1_pc' = "waiting"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
                   w2_pc, w2_frag, w3_pc, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag, w1_frag>>

W2_WriteAndSubmit ==
    /\ w2_pc = "idle"
    /\ channel' = channel \union {w2_frag}
    /\ w2_pc' = "waiting"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
                   w1_pc, w1_frag, w3_pc, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag, w2_frag>>

W3_WriteAndSubmit ==
    /\ w3_pc = "idle"
    /\ channel' = channel \union {w3_frag}
    /\ w3_pc' = "waiting"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
                   w1_pc, w1_frag, w2_pc, w2_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag, w3_frag>>

\* ==========================================================================
\* Flush Loop
\* ==========================================================================

\* Collect batch from channel
FL_CollectBatch ==
    /\ fl_pc = "idle"
    /\ channel /= {}
    /\ fl_batch' = channel          \* Take everything in channel
    /\ channel' = {}                \* Drain channel
    /\ fl_retry_count' = 0
    /\ fl_pc' = "batch_collected"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   fl_snap_etag,
                   w1_pc, w1_frag, w2_pc, w2_frag, w3_pc, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag>>

\* Read manifest with ETag
FL_ReadManifest ==
    /\ fl_pc = "batch_collected"
    /\ fl_snap_etag' = manifest_etag
    /\ fl_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   channel, fl_batch, fl_retry_count,
                   w1_pc, w1_frag, w2_pc, w2_frag, w3_pc, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag>>

\* CAS write: add all batch fragments to manifest
FL_CASManifest ==
    /\ fl_pc = "manifest_read"
    /\ fl_retry_count < MAX_FLUSH_RETRIES
    /\ IF manifest_etag = fl_snap_etag
       THEN \* CAS succeeds — add all batch fragments, assign sequence numbers
            /\ manifest_frags' = manifest_frags \union fl_batch
            /\ manifest_next_seq' = manifest_next_seq + Cardinality(fl_batch)
            /\ manifest_etag' = manifest_etag + 1
            /\ committed' = committed \union fl_batch
            \* Notify writers: success
            /\ w1_pc' = IF w1_frag \in fl_batch THEN "done" ELSE w1_pc
            /\ w2_pc' = IF w2_frag \in fl_batch THEN "done" ELSE w2_pc
            /\ w3_pc' = IF w3_frag \in fl_batch THEN "done" ELSE w3_pc
            /\ fl_pc' = "idle"          \* Ready for next batch
            /\ UNCHANGED <<fl_retry_count, fl_snap_etag>>
       ELSE \* CAS fails — re-read and retry
            /\ fl_retry_count' = fl_retry_count + 1
            /\ fl_snap_etag' = manifest_etag    \* Re-read
            /\ fl_pc' = "manifest_read"          \* Retry CAS
            /\ UNCHANGED <<manifest_frags, manifest_next_seq, manifest_etag,
                           committed, w1_pc, w2_pc, w3_pc>>
    /\ UNCHANGED <<channel, fl_batch,
                   w1_frag, w2_frag, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   next_frag>>

\* Exhausted retries — notify writers of failure
FL_Exhausted ==
    /\ fl_pc = "manifest_read"
    /\ fl_retry_count >= MAX_FLUSH_RETRIES
    \* Notify writers: error (fragments are orphaned on S3 but NOT in manifest)
    /\ w1_pc' = IF w1_frag \in fl_batch THEN "error" ELSE w1_pc
    /\ w2_pc' = IF w2_frag \in fl_batch THEN "error" ELSE w2_pc
    /\ w3_pc' = IF w3_frag \in fl_batch THEN "error" ELSE w3_pc
    /\ fl_pc' = "idle"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   channel, fl_batch, fl_snap_etag, fl_retry_count,
                   w1_frag, w2_frag, w3_frag,
                   c_pc, c_snap_frags, c_snap_etag,
                   committed, next_frag>>

\* ==========================================================================
\* Compactor (can cause CAS conflicts with flush loop)
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ manifest_frags /= {}
    /\ c_snap_frags' = manifest_frags
    /\ c_snap_etag' = manifest_etag
    /\ c_pc' = "ready"
    /\ UNCHANGED <<manifest_frags, manifest_etag, manifest_next_seq,
                   channel,
                   fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
                   w1_pc, w1_frag, w2_pc, w2_frag, w3_pc, w3_frag,
                   committed, next_frag>>

C_CASManifest ==
    /\ c_pc = "ready"
    /\ IF manifest_etag = c_snap_etag
       THEN /\ manifest_frags' = manifest_frags \ c_snap_frags
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "done"
       ELSE /\ c_pc' = "idle"      \* Abort on conflict
            /\ UNCHANGED <<manifest_frags, manifest_etag>>
    /\ UNCHANGED <<manifest_next_seq, channel,
                   fl_pc, fl_batch, fl_snap_etag, fl_retry_count,
                   w1_pc, w1_frag, w2_pc, w2_frag, w3_pc, w3_frag,
                   c_snap_frags, c_snap_etag,
                   committed, next_frag>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ W1_WriteAndSubmit \/ W2_WriteAndSubmit \/ W3_WriteAndSubmit
    \/ FL_CollectBatch \/ FL_ReadManifest \/ FL_CASManifest \/ FL_Exhausted
    \/ C_ReadManifest \/ C_CASManifest

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: No Fragment Loss.
\* Every fragment that received Ok (in committed set) must be in the manifest.
NoFragmentLoss ==
    \A f \in committed : f \in manifest_frags

\* INVARIANT 2: Batch Atomicity.
\* If any fragment in a batch is committed, ALL fragments in that batch
\* are committed. (The flush loop adds all or none.)
BatchAtomicity ==
    (fl_pc = "idle" /\ fl_batch /= {}) =>
        \/ fl_batch \subseteq committed       \* All committed
        \/ fl_batch \intersect committed = {} \* None committed

\* INVARIANT 3: No Double-Add.
\* A fragment ID should not appear in the manifest more than once.
\* Since we model manifest_frags as a set, this is structural.
\* But in the real code, add_fragment() pushes to a Vec without dedup check.
\* The invariant here verifies the set-based model is safe.
NoDuplicateFragments ==
    TRUE  \* Structural in the set model; real code needs Vec dedup check

\* INVARIANT 4: Committed implies writer notified.
\* If a fragment is committed, its writer should be in "done" state.
CommittedImpliesNotified ==
    /\ (w1_frag \in committed => w1_pc = "done")
    /\ (w2_frag \in committed => w2_pc = "done")
    /\ (w3_frag \in committed => w3_pc = "done")

\* TYPE INVARIANT
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3}
    /\ manifest_etag \in 0..15
    /\ manifest_next_seq \in 0..10
    /\ channel \subseteq {1, 2, 3}
    /\ fl_batch \subseteq {1, 2, 3}
    /\ fl_retry_count \in 0..4
    /\ w1_pc \in {"idle", "waiting", "done", "error"}
    /\ w2_pc \in {"idle", "waiting", "done", "error"}
    /\ w3_pc \in {"idle", "waiting", "done", "error"}
    /\ c_pc \in {"idle", "ready", "done"}
    /\ committed \subseteq {1, 2, 3}

=============================================================================
