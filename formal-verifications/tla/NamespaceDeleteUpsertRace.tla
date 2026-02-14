----------------------- MODULE NamespaceDeleteUpsertRace -----------------------
\* Formal verification of the namespace delete + upsert race (RACE-6).
\*
\* MODELS: 1 namespace deleter + 1 writer mid-upsert.
\*
\* THE BUG:
\*   1. Writer validates namespace exists (meta.json in registry) ✓
\*   2. Deleter starts: deletes manifest.json, then meta.json, then all objects
\*   3. Writer writes fragment to S3 (succeeds — S3 creates the key)
\*   4. Writer reads manifest from S3 → NotFound
\*   5. Writer creates default manifest (ManifestVersion(None))
\*   6. Writer does unconditional PUT of manifest (first write, no ETag)
\*   7. Fragment is now orphaned: manifest exists but namespace is deleted
\*
\*   Even worse: If deleter's delete_prefix runs AFTER the writer's PUT,
\*   it deletes the writer's newly created manifest AND fragment.
\*   The writer already returned 200 → silent data loss.
\*
\* EXPECTED: TLC finds a trace showing zombie namespace creation and/or
\*           data loss after the writer committed.
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 objects ---
    s3_meta,            \* BOOLEAN: meta.json exists
    s3_manifest,        \* "none" | "original" | "writer_created"
    s3_fragments,       \* Set of Nat: fragment files on S3

    \* --- In-memory registry ---
    ns_registry,        \* BOOLEAN: namespace is in the in-memory DashMap

    \* --- Writer ---
    w_pc,
    w_ns_validated,     \* BOOLEAN: writer has validated namespace exists
    w_frag_written,     \* BOOLEAN: fragment written to S3
    w_committed,        \* BOOLEAN: writer returned HTTP 200

    \* --- Deleter ---
    d_pc,

    \* --- Tracking ---
    zombie,             \* BOOLEAN: manifest exists but meta does not
    data_loss           \* BOOLEAN: committed fragment was deleted

vars == <<s3_meta, s3_manifest, s3_fragments, ns_registry,
          w_pc, w_ns_validated, w_frag_written, w_committed,
          d_pc, zombie, data_loss>>

\* ==========================================================================
\* Constants
\* ==========================================================================

WriterFrag == 1

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ s3_meta = TRUE               \* Namespace exists
    /\ s3_manifest = "original"     \* Has a manifest
    /\ s3_fragments = {}            \* No fragments yet
    /\ ns_registry = TRUE           \* In the in-memory registry
    /\ w_pc = "idle"
    /\ w_ns_validated = FALSE
    /\ w_frag_written = FALSE
    /\ w_committed = FALSE
    /\ d_pc = "idle"
    /\ zombie = FALSE
    /\ data_loss = FALSE

\* ==========================================================================
\* Writer Protocol (upsert)
\*
\* The writer has already passed the HTTP handler's namespace validation.
\* It proceeds with the WAL append protocol.
\* ==========================================================================

\* Step 1: Validate namespace (check registry/meta.json)
W_ValidateNamespace ==
    /\ w_pc = "idle"
    /\ ns_registry = TRUE       \* Namespace exists in registry
    /\ w_ns_validated' = TRUE
    /\ w_pc' = "validated"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_fragments, ns_registry,
                   w_frag_written, w_committed,
                   d_pc, zombie, data_loss>>

\* Step 2: Write fragment to S3 (unconditional PUT, ULID-unique key)
W_WriteFragment ==
    /\ w_pc = "validated"
    /\ s3_fragments' = s3_fragments \union {WriterFrag}
    /\ w_frag_written' = TRUE
    /\ w_pc' = "frag_written"
    /\ UNCHANGED <<s3_meta, s3_manifest, ns_registry,
                   w_ns_validated, w_committed,
                   d_pc, zombie, data_loss>>

\* Step 3: Read manifest from S3
\* Case A: manifest still exists → normal CAS path
W_ReadManifestExists ==
    /\ w_pc = "frag_written"
    /\ s3_manifest /= "none"
    /\ w_pc' = "manifest_read"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_fragments, ns_registry,
                   w_ns_validated, w_frag_written, w_committed,
                   d_pc, zombie, data_loss>>

\* Case B: manifest was deleted → writer creates a default manifest
W_ReadManifestNotFound ==
    /\ w_pc = "frag_written"
    /\ s3_manifest = "none"
    /\ w_pc' = "manifest_not_found"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_fragments, ns_registry,
                   w_ns_validated, w_frag_written, w_committed,
                   d_pc, zombie, data_loss>>

\* Step 4a: Normal path — CAS write manifest (add fragment ref)
W_CASManifest ==
    /\ w_pc = "manifest_read"
    /\ s3_manifest /= "none"    \* Can only CAS if manifest exists
    \* CAS succeeds (simplified — manifest hasn't changed)
    /\ w_committed' = TRUE       \* HTTP 200 returned to client
    /\ w_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_fragments, ns_registry,
                   w_ns_validated, w_frag_written,
                   d_pc, zombie, data_loss>>

\* Step 4b: NotFound path — unconditional PUT creates new manifest
\* THIS IS THE BUG: writer resurrects the manifest after deleter removed it
W_CreateManifest ==
    /\ w_pc = "manifest_not_found"
    /\ s3_manifest' = "writer_created"   \* Zombie manifest!
    /\ w_committed' = TRUE                \* HTTP 200 returned
    /\ w_pc' = "done"
    \* Check zombie condition: manifest exists but meta does not
    /\ zombie' = ~s3_meta
    /\ UNCHANGED <<s3_meta, s3_fragments, ns_registry,
                   w_ns_validated, w_frag_written,
                   d_pc, data_loss>>

\* ==========================================================================
\* Deleter Protocol (namespace delete)
\*
\* Non-atomic multi-step delete:
\*   1. Delete manifest.json
\*   2. Delete meta.json
\*   3. Delete all objects under prefix (fragments, segments, etc.)
\*   4. Remove from in-memory registry
\* ==========================================================================

D_DeleteManifest ==
    /\ d_pc = "idle"
    /\ s3_manifest' = "none"
    /\ d_pc' = "manifest_deleted"
    /\ UNCHANGED <<s3_meta, s3_fragments, ns_registry,
                   w_pc, w_ns_validated, w_frag_written, w_committed,
                   zombie, data_loss>>

D_DeleteMeta ==
    /\ d_pc = "manifest_deleted"
    /\ s3_meta' = FALSE
    /\ d_pc' = "meta_deleted"
    /\ UNCHANGED <<s3_manifest, s3_fragments, ns_registry,
                   w_pc, w_ns_validated, w_frag_written, w_committed,
                   zombie, data_loss>>

\* Delete all objects under namespace prefix
\* This includes any fragments the writer may have created!
D_DeletePrefix ==
    /\ d_pc = "meta_deleted"
    /\ s3_fragments' = {}               \* Delete ALL fragment files
    /\ IF s3_manifest /= "none"
       THEN s3_manifest' = "none"       \* Also delete resurrected manifest
       ELSE s3_manifest' = "none"
    \* Check for data loss: writer committed but fragment is now gone
    /\ data_loss' = (w_committed /\ WriterFrag \in s3_fragments)
    /\ d_pc' = "prefix_deleted"
    /\ UNCHANGED <<s3_meta, ns_registry,
                   w_pc, w_ns_validated, w_frag_written, w_committed,
                   zombie>>

D_RemoveFromRegistry ==
    /\ d_pc = "prefix_deleted"
    /\ ns_registry' = FALSE
    /\ d_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest, s3_fragments,
                   w_pc, w_ns_validated, w_frag_written, w_committed,
                   zombie, data_loss>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Writer
    \/ W_ValidateNamespace
    \/ W_WriteFragment
    \/ W_ReadManifestExists
    \/ W_ReadManifestNotFound
    \/ W_CASManifest
    \/ W_CreateManifest
    \* Deleter
    \/ D_DeleteManifest
    \/ D_DeleteMeta
    \/ D_DeletePrefix
    \/ D_RemoveFromRegistry

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: No Zombie Namespace (EXPECTED VIOLATION).
\* After any state transition, there should be no state where manifest.json
\* exists but meta.json does not. TLC should find the trace:
\*   D_DeleteManifest → D_DeleteMeta → W_ReadManifestNotFound → W_CreateManifest
\* where the writer resurrects the manifest after the deleter removed meta.
NoZombieNamespace ==
    ~zombie

\* INVARIANT 2: No Data Loss After Commit (EXPECTED VIOLATION).
\* If the writer committed (HTTP 200), its fragment should not be deleted.
\* TLC should find the trace where D_DeletePrefix removes the fragment
\* after the writer already returned success.
NoDataLossAfterCommit ==
    ~data_loss

\* INVARIANT 3: After delete completes, no manifest exists.
\* Once the deleter finishes all steps, there should be no manifest on S3.
NoManifestAfterDelete ==
    d_pc = "done" => s3_manifest = "none"

\* INVARIANT 4: Registry consistency.
\* If the namespace is not in the registry, meta.json should not exist.
RegistryConsistency ==
    (~ns_registry) => (~s3_meta)

\* TYPE INVARIANT
TypeOK ==
    /\ s3_meta \in {TRUE, FALSE}
    /\ s3_manifest \in {"none", "original", "writer_created"}
    /\ s3_fragments \subseteq {1}
    /\ ns_registry \in {TRUE, FALSE}
    /\ w_pc \in {"idle", "validated", "frag_written", "manifest_read",
                  "manifest_not_found", "done"}
    /\ d_pc \in {"idle", "manifest_deleted", "meta_deleted",
                  "prefix_deleted", "done"}
    /\ w_ns_validated \in {TRUE, FALSE}
    /\ w_frag_written \in {TRUE, FALSE}
    /\ w_committed \in {TRUE, FALSE}
    /\ zombie \in {TRUE, FALSE}
    /\ data_loss \in {TRUE, FALSE}

=============================================================================
