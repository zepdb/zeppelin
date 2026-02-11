-------------------------- MODULE NamespaceDeletion --------------------------
\* Formal verification of namespace deletion during active operations.
\*
\* MODELS: Deleter + Writer + Query
\* FINDS:  Non-atomic prefix deletion allows concurrent operations to
\*         observe partially deleted state — queries return partial results,
\*         writers create orphaned fragments.
\*
\* ==========================================================================
\* THE BUG:
\*   delete_namespace calls store.delete_prefix() which lists keys then
\*   deletes them one-by-one. This is NOT atomic. During the deletion
\*   window:
\*     - A query can read the manifest (still exists) but then fail to
\*       read fragments (already deleted), or succeed partially.
\*     - A writer can create a new fragment after some keys are deleted
\*       but before the manifest is deleted, creating orphaned data.
\*
\*   Code references:
\*     - src/namespace/manager.rs:159-180 — non-atomic prefix deletion
\*     - store.delete_prefix() lists keys then deletes one by one
\*
\* CLIENT INVARIANT:
\*   After namespace delete returns 200, all subsequent operations return
\*   "namespace not found." In-flight operations either complete fully or
\*   fail with a defined error — never return partial/corrupt results.
\*
\* EXPECTED RESULT:
\*   TLC finds a counterexample where a query observes partial state
\*   during namespace deletion.
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 state (individual keys, not atomic) ---
    s3_meta,            \* TRUE if meta.json exists on S3
    s3_manifest,        \* TRUE if manifest.json exists on S3
    s3_frag1,           \* TRUE if fragment 1 .wal file exists on S3
    s3_frag2,           \* TRUE if fragment 2 .wal file exists on S3

    \* --- Deleter local state ---
    d_pc,               \* Deleter program counter
    d_keys_to_delete,   \* Set of keys remaining to delete

    \* --- Writer local state ---
    w_pc,
    w_mutex,            \* "free" | "w"

    \* --- Query local state ---
    q_pc,
    q_saw_manifest,     \* TRUE if query successfully read the manifest
    q_saw_frag1,        \* TRUE/FALSE/unread for each fragment read
    q_saw_frag2,
    q_partial           \* TRUE if query observed inconsistent state

vars == <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
          d_pc, d_keys_to_delete,
          w_pc, w_mutex,
          q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* ==========================================================================
\* Initial State: namespace exists with 2 fragments
\* ==========================================================================

Init ==
    /\ s3_meta = TRUE
    /\ s3_manifest = TRUE
    /\ s3_frag1 = TRUE
    /\ s3_frag2 = TRUE
    /\ d_pc = "idle"
    /\ d_keys_to_delete = {}
    /\ w_pc = "idle"
    /\ w_mutex = "free"
    /\ q_pc = "idle"
    /\ q_saw_manifest = FALSE
    /\ q_saw_frag1 = FALSE
    /\ q_saw_frag2 = FALSE
    /\ q_partial = FALSE

\* ==========================================================================
\* Deleter Protocol (models NamespaceManager::delete)
\*
\* Step 1: Check existence (read meta.json)
\* Step 2: List all keys under namespace prefix
\* Step 3-N: Delete keys one at a time (NOT atomic!)
\* Step N+1: Remove from registry
\* ==========================================================================

\* Step 1: Verify namespace exists
D_CheckExists ==
    /\ d_pc = "idle"
    /\ s3_meta = TRUE                          \* Namespace exists
    /\ d_pc' = "listing"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_keys_to_delete,
                   w_pc, w_mutex,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* Step 2: List keys (gets current snapshot of keys)
D_ListKeys ==
    /\ d_pc = "listing"
    \* Collects all existing keys into the deletion set
    /\ d_keys_to_delete' = {k \in {"meta", "manifest", "frag1", "frag2"} :
                              CASE k = "meta" -> s3_meta
                                [] k = "manifest" -> s3_manifest
                                [] k = "frag1" -> s3_frag1
                                [] k = "frag2" -> s3_frag2}
    /\ d_pc' = "deleting"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   w_pc, w_mutex,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* Step 3: Delete one key at a time (non-atomic!)
\* The deleter picks any key from d_keys_to_delete and deletes it.
D_DeleteOneKey ==
    /\ d_pc = "deleting"
    /\ d_keys_to_delete /= {}
    /\ \E k \in d_keys_to_delete :
        /\ d_keys_to_delete' = d_keys_to_delete \ {k}
        /\ CASE k = "meta" -> s3_meta' = FALSE /\ UNCHANGED <<s3_manifest, s3_frag1, s3_frag2>>
             [] k = "manifest" -> s3_manifest' = FALSE /\ UNCHANGED <<s3_meta, s3_frag1, s3_frag2>>
             [] k = "frag1" -> s3_frag1' = FALSE /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag2>>
             [] k = "frag2" -> s3_frag2' = FALSE /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1>>
    /\ UNCHANGED <<d_pc, w_pc, w_mutex,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* Step 4: All keys deleted, finish
D_Finish ==
    /\ d_pc = "deleting"
    /\ d_keys_to_delete = {}
    /\ d_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_keys_to_delete,
                   w_pc, w_mutex,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* ==========================================================================
\* Writer Protocol (simplified — just checks manifest exists then writes)
\* ==========================================================================

W_AcquireMutex ==
    /\ w_pc = "idle"
    /\ w_mutex = "free"
    /\ w_mutex' = "w"
    /\ w_pc' = "acquired"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* Writer reads manifest — may fail if deleted
W_ReadManifest ==
    /\ w_pc = "acquired"
    /\ IF s3_manifest = TRUE
       THEN w_pc' = "manifest_read"             \* Proceed normally
       ELSE w_pc' = "error"                      \* Manifest gone — error
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete, w_mutex,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

W_WriteFragment ==
    /\ w_pc = "manifest_read"
    \* Writer creates a new fragment (writes to S3 even though namespace
    \* may be partially deleted — writer checked manifest, not meta)
    /\ w_pc' = "done"
    /\ w_mutex' = "free"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

W_Error ==
    /\ w_pc = "error"
    /\ w_pc' = "done"
    /\ w_mutex' = "free"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete,
                   q_pc, q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* ==========================================================================
\* Query Protocol (reads manifest, then reads each fragment)
\* ==========================================================================

Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ IF s3_manifest = TRUE
       THEN /\ q_saw_manifest' = TRUE
            /\ q_pc' = "reading_frag1"
       ELSE /\ q_saw_manifest' = FALSE
            /\ q_pc' = "error"                  \* No manifest — namespace gone
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete, w_pc, w_mutex,
                   q_saw_frag1, q_saw_frag2, q_partial>>

Q_ReadFrag1 ==
    /\ q_pc = "reading_frag1"
    /\ q_saw_frag1' = s3_frag1                 \* TRUE if exists, FALSE if 404
    /\ q_pc' = "reading_frag2"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete, w_pc, w_mutex,
                   q_saw_manifest, q_saw_frag2, q_partial>>

Q_ReadFrag2 ==
    /\ q_pc = "reading_frag2"
    /\ q_saw_frag2' = s3_frag2                 \* TRUE if exists, FALSE if 404
    \* Detect partial state: manifest existed but fragments are gone
    /\ q_partial' = (q_saw_manifest = TRUE /\
                     (~q_saw_frag1 \/ ~s3_frag2))
    /\ q_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete, w_pc, w_mutex,
                   q_saw_manifest, q_saw_frag1>>

Q_Error ==
    /\ q_pc = "error"
    /\ q_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_frag1, s3_frag2,
                   d_pc, d_keys_to_delete, w_pc, w_mutex,
                   q_saw_manifest, q_saw_frag1, q_saw_frag2, q_partial>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ D_CheckExists \/ D_ListKeys \/ D_DeleteOneKey \/ D_Finish
    \/ W_AcquireMutex \/ W_ReadManifest \/ W_WriteFragment \/ W_Error
    \/ Q_ReadManifest \/ Q_ReadFrag1 \/ Q_ReadFrag2 \/ Q_Error

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* PRIMARY INVARIANT — EXPECTED TO FAIL.
\* A query should never observe partial namespace state: if it saw the
\* manifest, it should be able to read all fragments referenced by it.
\* Violation indicates the non-atomic deletion exposed inconsistent state.
QueryNeverSeesPartialState ==
    q_partial = FALSE

\* SECONDARY: If manifest exists, all fragment data should exist too.
\* This can be violated during the deletion window.
NamespaceConsistency ==
    s3_manifest = TRUE => (s3_frag1 = TRUE /\ s3_frag2 = TRUE)

\* TYPE INVARIANT
TypeOK ==
    /\ s3_meta \in {TRUE, FALSE}
    /\ s3_manifest \in {TRUE, FALSE}
    /\ s3_frag1 \in {TRUE, FALSE}
    /\ s3_frag2 \in {TRUE, FALSE}
    /\ d_pc \in {"idle", "listing", "deleting", "done"}
    /\ w_pc \in {"idle", "acquired", "manifest_read", "error", "done"}
    /\ q_pc \in {"idle", "reading_frag1", "reading_frag2", "error", "done"}

\* ==========================================================================
\* Candidate Fix: Tombstone-Based Deletion
\*
\* Instead of deleting keys one by one, write a tombstone marker first,
\* then delete. All operations check for the tombstone before proceeding.
\*
\* This ensures that once deletion starts, all concurrent operations see
\* "namespace not found" immediately, even before keys are physically
\* removed from S3.
\* ==========================================================================

=============================================================================
