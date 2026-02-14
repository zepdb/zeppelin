---------------------- MODULE ConcurrentNamespaceCreate -----------------------
\* Formal verification of concurrent namespace creation race (RACE-5).
\*
\* MODELS: 2 clients concurrently creating the same namespace name.
\*
\* THE BUG:
\*   The check-then-create pattern in create_with_fts() is non-atomic:
\*   1. Client A: HEAD meta.json → not exists
\*   2. Client B: HEAD meta.json → not exists
\*   3. Client A: PUT meta.json (unconditional) → succeeds
\*   4. Client B: PUT meta.json (unconditional) → OVERWRITES A's
\*   5. Both return 201 Created
\*
\*   If they specified different dimensions or distance_metrics, the
\*   second silently overwrites the first's configuration.
\*
\* EXPECTED: TLC finds a trace where both creates succeed (2x 201).
\*
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 state ---
    s3_meta,            \* "none" | "a" | "b" (who wrote it last)
    s3_manifest,        \* "none" | "a" | "b"

    \* --- In-memory registry ---
    ns_registry,        \* Set of String: registered namespace entries

    \* --- Client A ---
    a_pc,
    a_checked_exists,   \* Result of existence check
    a_returned_201,     \* BOOLEAN: returned success

    \* --- Client B ---
    b_pc,
    b_checked_exists,
    b_returned_201,

    \* --- Tracking ---
    total_creates        \* Number of successful 201 responses

vars == <<s3_meta, s3_manifest, ns_registry,
          a_pc, a_checked_exists, a_returned_201,
          b_pc, b_checked_exists, b_returned_201,
          total_creates>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ s3_meta = "none"
    /\ s3_manifest = "none"
    /\ ns_registry = {}
    /\ a_pc = "idle"
    /\ a_checked_exists = FALSE
    /\ a_returned_201 = FALSE
    /\ b_pc = "idle"
    /\ b_checked_exists = FALSE
    /\ b_returned_201 = FALSE
    /\ total_creates = 0

\* ==========================================================================
\* Client A: Create namespace
\* ==========================================================================

\* Step 1: Check if namespace exists (HEAD meta.json)
A_CheckExists ==
    /\ a_pc = "idle"
    /\ a_checked_exists' = (s3_meta /= "none")
    /\ a_pc' = "checked"
    /\ UNCHANGED <<s3_meta, s3_manifest, ns_registry,
                   a_returned_201,
                   b_pc, b_checked_exists, b_returned_201,
                   total_creates>>

\* Step 2: If not exists, create meta.json (unconditional PUT)
A_CreateMeta ==
    /\ a_pc = "checked"
    /\ a_checked_exists = FALSE     \* Namespace didn't exist when we checked
    /\ s3_meta' = "a"
    /\ a_pc' = "meta_created"
    /\ UNCHANGED <<s3_manifest, ns_registry,
                   a_checked_exists, a_returned_201,
                   b_pc, b_checked_exists, b_returned_201,
                   total_creates>>

\* Step 2b: If exists, return 409 Conflict
A_Conflict ==
    /\ a_pc = "checked"
    /\ a_checked_exists = TRUE
    /\ a_pc' = "done_conflict"
    /\ UNCHANGED <<s3_meta, s3_manifest, ns_registry,
                   a_checked_exists, a_returned_201,
                   b_pc, b_checked_exists, b_returned_201,
                   total_creates>>

\* Step 3: Create manifest.json (unconditional PUT)
A_CreateManifest ==
    /\ a_pc = "meta_created"
    /\ s3_manifest' = "a"
    /\ a_pc' = "manifest_created"
    /\ UNCHANGED <<s3_meta, ns_registry,
                   a_checked_exists, a_returned_201,
                   b_pc, b_checked_exists, b_returned_201,
                   total_creates>>

\* Step 4: Insert into registry, return 201
A_Return201 ==
    /\ a_pc = "manifest_created"
    /\ ns_registry' = ns_registry \union {"ns"}
    /\ a_returned_201' = TRUE
    /\ total_creates' = total_creates + 1
    /\ a_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest,
                   a_checked_exists,
                   b_pc, b_checked_exists, b_returned_201>>

\* ==========================================================================
\* Client B: Create namespace (symmetric)
\* ==========================================================================

B_CheckExists ==
    /\ b_pc = "idle"
    /\ b_checked_exists' = (s3_meta /= "none")
    /\ b_pc' = "checked"
    /\ UNCHANGED <<s3_meta, s3_manifest, ns_registry,
                   a_pc, a_checked_exists, a_returned_201,
                   b_returned_201,
                   total_creates>>

B_CreateMeta ==
    /\ b_pc = "checked"
    /\ b_checked_exists = FALSE
    /\ s3_meta' = "b"
    /\ b_pc' = "meta_created"
    /\ UNCHANGED <<s3_manifest, ns_registry,
                   a_pc, a_checked_exists, a_returned_201,
                   b_checked_exists, b_returned_201,
                   total_creates>>

B_Conflict ==
    /\ b_pc = "checked"
    /\ b_checked_exists = TRUE
    /\ b_pc' = "done_conflict"
    /\ UNCHANGED <<s3_meta, s3_manifest, ns_registry,
                   a_pc, a_checked_exists, a_returned_201,
                   b_checked_exists, b_returned_201,
                   total_creates>>

B_CreateManifest ==
    /\ b_pc = "meta_created"
    /\ s3_manifest' = "b"
    /\ b_pc' = "manifest_created"
    /\ UNCHANGED <<s3_meta, ns_registry,
                   a_pc, a_checked_exists, a_returned_201,
                   b_checked_exists, b_returned_201,
                   total_creates>>

B_Return201 ==
    /\ b_pc = "manifest_created"
    /\ ns_registry' = ns_registry \union {"ns"}
    /\ b_returned_201' = TRUE
    /\ total_creates' = total_creates + 1
    /\ b_pc' = "done"
    /\ UNCHANGED <<s3_meta, s3_manifest,
                   a_pc, a_checked_exists, a_returned_201,
                   b_checked_exists>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ A_CheckExists \/ A_CreateMeta \/ A_Conflict
    \/ A_CreateManifest \/ A_Return201
    \/ B_CheckExists \/ B_CreateMeta \/ B_Conflict
    \/ B_CreateManifest \/ B_Return201

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: At-Most-One Create Succeeds (EXPECTED VIOLATION).
\* For a given namespace name, at most one create should return 201.
\* TLC should find a trace where both clients return 201.
AtMostOneCreate ==
    total_creates <= 1

\* INVARIANT 2: No Silent Config Overwrite.
\* If both clients succeed, one's config silently overwrites the other's.
\* We detect this by checking if the meta.json author matches the manifest
\* author when both are done.
ConfigConsistency ==
    (a_pc \in {"done", "done_conflict"} /\ b_pc \in {"done", "done_conflict"}) =>
        (s3_meta = s3_manifest \/ s3_meta = "none")

\* INVARIANT 3: Last Writer Wins.
\* S3 meta and manifest should be from the same client (or none).
\* This can be violated if A writes meta, B writes meta (overwrites),
\* then A writes manifest. Now meta="b" but manifest="a".
MetaManifestConsistency ==
    (s3_meta /= "none" /\ s3_manifest /= "none") =>
        s3_meta = s3_manifest

\* TYPE INVARIANT
TypeOK ==
    /\ s3_meta \in {"none", "a", "b"}
    /\ s3_manifest \in {"none", "a", "b"}
    /\ ns_registry \subseteq {"ns"}
    /\ a_pc \in {"idle", "checked", "meta_created", "manifest_created",
                  "done", "done_conflict"}
    /\ b_pc \in {"idle", "checked", "meta_created", "manifest_created",
                  "done", "done_conflict"}
    /\ a_checked_exists \in {TRUE, FALSE}
    /\ b_checked_exists \in {TRUE, FALSE}
    /\ a_returned_201 \in {TRUE, FALSE}
    /\ b_returned_201 \in {TRUE, FALSE}
    /\ total_creates \in 0..2

=============================================================================
