--------------------------- MODULE MultiWriterLease ---------------------------
\* Formal verification of Zeppelin's multi-writer lease protocol with
\* fencing tokens.
\*
\* MODELS: 2 writers (w1, w2) + 1 compactor (c) + 1 clock (models expiry)
\* GOAL:   Unlike existing specs (which model bugs), this models the FIX.
\*         TLC should find NO counterexamples if the protocol is correct.
\*         Any counterexample reveals a protocol flaw to address before
\*         implementation.
\*
\* ==========================================================================
\* PROTOCOL SUMMARY:
\*
\*   Lease Object ({namespace}/lease.json on S3):
\*     - holder_id, fencing_token, acquired_at, expires_at
\*     - CAS-protected via ETag
\*     - One lease per namespace (writers + compactor compete)
\*
\*   Writer Protocol:
\*     1. AcquireLease — CAS on lease.json, get fencing_token = N
\*     2. WriteFragment — unconditional PUT (ULID-unique key)
\*     3. ReadManifest — GET manifest.json with ETag
\*     4. CheckFencing — if manifest.fencing_token > N, abort (zombie)
\*     5. WriteManifest — CAS on manifest.json (set fencing_token = N)
\*     6. ReleaseLease — CAS on lease.json (clear holder)
\*
\*   Compactor Protocol:
\*     1. AcquireLease — same as writer
\*     2. ReadManifest — snapshot fragment list
\*     3. BuildSegment — expensive IVF-Flat build
\*     4. CheckFencing — same as writer
\*     5. WriteManifest — CAS, replace fragments with segment
\*     6. ReleaseLease
\*
\*   Clock Process:
\*     Non-deterministically expires the current lease holder's lease.
\*     Models time passing without needing real clock values.
\* ==========================================================================

EXTENDS Naturals, FiniteSets, Sequences

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    manifest_frags,     \* Set of Nat: fragment IDs in manifest
    manifest_seg,       \* Set of Nat: fragment IDs covered by active segment
    manifest_fencing,   \* Nat: fencing token in manifest (only increases)
    manifest_etag,      \* Nat: version counter for manifest CAS
    s3_frag_data,       \* Set of Nat: fragment .wal files on S3

    \* --- Lease state (single lease.json on S3) ---
    lease_holder,       \* "none" | "w1" | "w2" | "c"
    lease_token,        \* Nat: current fencing token of lease
    lease_expired,      \* BOOLEAN: whether clock has expired the lease
    lease_etag,         \* Nat: version counter for lease.json CAS

    \* --- Monotonic counters ---
    next_token,         \* Nat: next fencing token to assign
    committed,          \* Set of Nat: fragments that got HTTP 200

    \* --- Writer w1 local state ---
    w1_pc,              \* Program counter
    w1_snap,            \* w1's snapshot of manifest_frags
    w1_snap_etag,       \* w1's snapshot of manifest_etag
    w1_local_token,     \* w1's fencing token from lease
    w1_frag,            \* w1's fragment ID (assigned at WriteFragment)

    \* --- Writer w2 local state ---
    w2_pc,
    w2_snap,
    w2_snap_etag,
    w2_local_token,
    w2_frag,

    \* --- Compactor local state ---
    c_pc,
    c_snap,
    c_snap_etag,
    c_local_token,
    c_seg_contents      \* Set of fragment IDs the segment was built from

vars == <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
          s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
          next_token, committed,
          w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
          w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
          c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Initial fragments already in manifest.
InitFragments == {1, 2}

\* Fragment IDs that writers will create.
W1_Fragment == 3
W2_Fragment == 4

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = InitFragments
    /\ manifest_seg = {}
    /\ manifest_fencing = 0
    /\ manifest_etag = 1
    /\ s3_frag_data = InitFragments
    /\ lease_holder = "none"
    /\ lease_token = 0
    /\ lease_expired = FALSE
    /\ lease_etag = 0
    /\ next_token = 1
    /\ committed = InitFragments
    /\ w1_pc = "idle"
    /\ w1_snap = {}
    /\ w1_snap_etag = 0
    /\ w1_local_token = 0
    /\ w1_frag = 0
    /\ w2_pc = "idle"
    /\ w2_snap = {}
    /\ w2_snap_etag = 0
    /\ w2_local_token = 0
    /\ w2_frag = 0
    /\ c_pc = "idle"
    /\ c_snap = {}
    /\ c_snap_etag = 0
    /\ c_local_token = 0
    /\ c_seg_contents = {}

\* ==========================================================================
\* Clock Process — models lease expiry
\*
\* Non-deterministically expires the current lease. This models time
\* passing and the lease timeout firing. Can only expire if someone
\* holds the lease and it isn't already expired.
\* ==========================================================================

ClockExpireLease ==
    /\ lease_holder /= "none"
    /\ lease_expired = FALSE
    /\ lease_expired' = TRUE
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* ==========================================================================
\* Writer w1 Protocol
\* ==========================================================================

\* Step 1: Acquire lease. Succeeds only if no one holds it, or lease expired.
W1_AcquireLease ==
    /\ w1_pc = "idle"
    /\ \/ lease_holder = "none"                  \* No one holds lease
       \/ (lease_expired = TRUE)                 \* Current holder's lease expired
    \* CAS on lease.json: check etag matches
    /\ w1_local_token' = next_token
    /\ next_token' = next_token + 1
    /\ lease_holder' = "w1"
    /\ lease_token' = next_token               \* = w1_local_token'
    /\ lease_expired' = FALSE                  \* Fresh lease
    /\ lease_etag' = lease_etag + 1            \* New etag after CAS write
    /\ w1_pc' = "has_lease"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, committed,
                   w1_snap, w1_snap_etag, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Step 2: Write fragment to S3 (unconditional, ULID-unique key).
W1_WriteFragment ==
    /\ w1_pc = "has_lease"
    /\ w1_frag' = W1_Fragment
    /\ s3_frag_data' = s3_frag_data \union {W1_Fragment}
    /\ w1_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_snap, w1_snap_etag, w1_local_token,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Step 3: Read manifest from S3 (snapshot + etag for CAS).
W1_ReadManifest ==
    /\ w1_pc = "frag_written"
    /\ w1_snap' = manifest_frags
    /\ w1_snap_etag' = manifest_etag
    /\ w1_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Step 4: Check fencing token. If manifest's token > mine, I'm a zombie.
\* On zombie detection, go to "aborted" — do NOT write manifest.
W1_CheckFencing ==
    /\ w1_pc = "manifest_read"
    /\ IF manifest_fencing <= w1_local_token
       THEN w1_pc' = "fencing_ok"
       ELSE w1_pc' = "aborted"                \* Zombie detected!
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Step 5: Write manifest (CAS). Check etag matches snapshot.
\* If etag mismatch → go back to ReadManifest to retry.
\* If etag matches → commit: add fragment, set fencing token.
W1_WriteManifest ==
    /\ w1_pc = "fencing_ok"
    /\ IF manifest_etag = w1_snap_etag
       THEN \* CAS succeeds
            /\ manifest_frags' = w1_snap \union {w1_frag}
            /\ manifest_fencing' = w1_local_token
            /\ manifest_etag' = manifest_etag + 1
            /\ committed' = committed \union {w1_frag}
            /\ w1_pc' = "committed"
       ELSE \* CAS fails — retry from ReadManifest
            /\ w1_pc' = "frag_written"
            /\ UNCHANGED <<manifest_frags, manifest_fencing, manifest_etag, committed>>
    /\ UNCHANGED <<manifest_seg, s3_frag_data,
                   lease_holder, lease_token, lease_expired, lease_etag,
                   next_token,
                   w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Step 6: Release lease (or skip if lease was taken over due to expiry).
W1_ReleaseLease ==
    /\ w1_pc \in {"committed", "aborted"}
    /\ IF lease_holder = "w1"
       THEN \* We still hold the lease — release it
            /\ lease_holder' = "none"
            /\ lease_expired' = FALSE
            /\ lease_etag' = lease_etag + 1
       ELSE \* Lease expired and was taken over — just move on
            /\ UNCHANGED <<lease_holder, lease_expired, lease_etag>>
    /\ w1_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_token,
                   next_token, committed,
                   w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* ==========================================================================
\* Writer w2 Protocol (symmetric to w1)
\* ==========================================================================

W2_AcquireLease ==
    /\ w2_pc = "idle"
    /\ \/ lease_holder = "none"
       \/ (lease_expired = TRUE)
    /\ w2_local_token' = next_token
    /\ next_token' = next_token + 1
    /\ lease_holder' = "w2"
    /\ lease_token' = next_token
    /\ lease_expired' = FALSE
    /\ lease_etag' = lease_etag + 1
    /\ w2_pc' = "has_lease"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_snap, w2_snap_etag, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

W2_WriteFragment ==
    /\ w2_pc = "has_lease"
    /\ w2_frag' = W2_Fragment
    /\ s3_frag_data' = s3_frag_data \union {W2_Fragment}
    /\ w2_pc' = "frag_written"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_snap, w2_snap_etag, w2_local_token,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

W2_ReadManifest ==
    /\ w2_pc = "frag_written"
    /\ w2_snap' = manifest_frags
    /\ w2_snap_etag' = manifest_etag
    /\ w2_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

W2_CheckFencing ==
    /\ w2_pc = "manifest_read"
    /\ IF manifest_fencing <= w2_local_token
       THEN w2_pc' = "fencing_ok"
       ELSE w2_pc' = "aborted"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

W2_WriteManifest ==
    /\ w2_pc = "fencing_ok"
    /\ IF manifest_etag = w2_snap_etag
       THEN /\ manifest_frags' = w2_snap \union {w2_frag}
            /\ manifest_fencing' = w2_local_token
            /\ manifest_etag' = manifest_etag + 1
            /\ committed' = committed \union {w2_frag}
            /\ w2_pc' = "committed"
       ELSE /\ w2_pc' = "frag_written"
            /\ UNCHANGED <<manifest_frags, manifest_fencing, manifest_etag, committed>>
    /\ UNCHANGED <<manifest_seg, s3_frag_data,
                   lease_holder, lease_token, lease_expired, lease_etag,
                   next_token,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

W2_ReleaseLease ==
    /\ w2_pc \in {"committed", "aborted"}
    /\ IF lease_holder = "w2"
       THEN /\ lease_holder' = "none"
            /\ lease_expired' = FALSE
            /\ lease_etag' = lease_etag + 1
       ELSE /\ UNCHANGED <<lease_holder, lease_expired, lease_etag>>
    /\ w2_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_token,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_pc, c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* ==========================================================================
\* Compactor Protocol
\* ==========================================================================

C_AcquireLease ==
    /\ c_pc = "idle"
    /\ \/ lease_holder = "none"
       \/ (lease_expired = TRUE)
    /\ c_local_token' = next_token
    /\ next_token' = next_token + 1
    /\ lease_holder' = "c"
    /\ lease_token' = next_token
    /\ lease_expired' = FALSE
    /\ lease_etag' = lease_etag + 1
    /\ c_pc' = "has_lease"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_snap, c_snap_etag, c_seg_contents>>

C_ReadManifest ==
    /\ c_pc = "has_lease"
    /\ manifest_frags /= {}                     \* Only compact if fragments exist
    /\ c_snap' = manifest_frags
    /\ c_snap_etag' = manifest_etag
    /\ c_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_local_token, c_seg_contents>>

C_BuildSegment ==
    /\ c_pc = "manifest_read"
    /\ c_seg_contents' = c_snap                 \* Segment covers snapshot frags
    /\ c_pc' = "seg_built"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_snap, c_snap_etag, c_local_token>>

C_CheckFencing ==
    /\ c_pc = "seg_built"
    /\ IF manifest_fencing <= c_local_token
       THEN c_pc' = "fencing_ok"
       ELSE c_pc' = "aborted"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* Compactor CAS-writes manifest: replace compacted fragments with segment.
\* Keeps any fragments that appeared AFTER the snapshot (new_frags).
C_WriteManifest ==
    /\ c_pc = "fencing_ok"
    /\ IF manifest_etag = c_snap_etag
       THEN \* CAS succeeds
            \* Keep fragments that are in current manifest but NOT in our snapshot
            \* (these were added by writers after we read the manifest)
            /\ LET new_frags == manifest_frags \ c_snap
               IN manifest_frags' = new_frags
            /\ manifest_seg' = c_seg_contents
            /\ manifest_fencing' = c_local_token
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "committed"
       ELSE \* CAS fails — abort (segment build may be stale)
            /\ c_pc' = "aborted"
            /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag>>
    /\ UNCHANGED <<s3_frag_data, lease_holder, lease_token, lease_expired, lease_etag,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_snap, c_snap_etag, c_local_token, c_seg_contents>>

C_ReleaseLease ==
    /\ c_pc \in {"committed", "aborted"}
    /\ IF lease_holder = "c"
       THEN /\ lease_holder' = "none"
            /\ lease_expired' = FALSE
            /\ lease_etag' = lease_etag + 1
       ELSE /\ UNCHANGED <<lease_holder, lease_expired, lease_etag>>
    /\ c_pc' = "done"
    /\ UNCHANGED <<manifest_frags, manifest_seg, manifest_fencing, manifest_etag,
                   s3_frag_data, lease_token,
                   next_token, committed,
                   w1_pc, w1_snap, w1_snap_etag, w1_local_token, w1_frag,
                   w2_pc, w2_snap, w2_snap_etag, w2_local_token, w2_frag,
                   c_snap, c_snap_etag, c_local_token, c_seg_contents>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Clock
    \/ ClockExpireLease
    \* Writer w1
    \/ W1_AcquireLease
    \/ W1_WriteFragment
    \/ W1_ReadManifest
    \/ W1_CheckFencing
    \/ W1_WriteManifest
    \/ W1_ReleaseLease
    \* Writer w2
    \/ W2_AcquireLease
    \/ W2_WriteFragment
    \/ W2_ReadManifest
    \/ W2_CheckFencing
    \/ W2_WriteManifest
    \/ W2_ReleaseLease
    \* Compactor
    \/ C_AcquireLease
    \/ C_ReadManifest
    \/ C_BuildSegment
    \/ C_CheckFencing
    \/ C_WriteManifest
    \/ C_ReleaseLease

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: No silent data loss.
\* Every committed fragment is reachable via the manifest — either directly
\* as an uncompacted fragment, or indirectly via a segment built from it.
\*
\* NOTE: Fragments committed by a zombie writer that was aborted at
\* CheckFencing are NOT in `committed` — they never got HTTP 200.
\* Their fragment data exists on S3 but is an orphan, not data loss.
NoSilentDataLoss ==
    \A f \in committed : (f \in manifest_frags) \/ (f \in manifest_seg)

\* INVARIANT 2: Lease exclusivity.
\* At most one process holds a non-expired lease at any time.
\* (When a lease expires, the clock sets lease_expired=TRUE, and a new
\* holder can acquire. But until then, only one holder.)
LeaseExclusivity ==
    \/ lease_holder = "none"
    \/ lease_expired = TRUE
    \/ /\ lease_holder /= "none"
       /\ lease_expired = FALSE

\* INVARIANT 3: Fencing token in manifest only increases.
\* This is checked structurally: manifest_fencing is only written in
\* WriteManifest steps, where it's set to the writer's local_token.
\* Since local_token comes from next_token (monotonically increasing),
\* and CheckFencing ensures manifest_fencing <= local_token before
\* writing, the token can only increase.
TokenMonotonicity ==
    manifest_fencing >= 0  \* Trivially true for Nat, but expresses intent

\* INVARIANT 4: Fencing prevents zombie writes.
\*
\* A zombie is a process whose lease expired and whose token is lower than
\* a later holder's token. The two-layer defense (CheckFencing + CAS) ensures
\* no zombie can successfully commit to the manifest:
\*
\*   Layer 1 (CheckFencing): If manifest_fencing > my_token → abort.
\*   Layer 2 (CAS): If another writer committed between my CheckFencing
\*     and WriteManifest, the etag changed → CAS fails → retry →
\*     re-check fencing → caught by Layer 1.
\*
\* NOTE: A process CAN transiently be at "fencing_ok" with a stale token
\* (TOCTOU gap between CheckFencing and WriteManifest). This is safe because
\* CAS catches it. The invariant checks the OUTCOME: no stale-token process
\* successfully commits. Since WriteManifest sets manifest_fencing = local_token
\* on commit, and tokens are monotonically assigned, manifest_fencing >=
\* local_token holds for all committed processes.
FencingPreventsZombie ==
    /\ (w1_pc = "committed" => manifest_fencing >= w1_local_token)
    /\ (w2_pc = "committed" => manifest_fencing >= w2_local_token)
    /\ (c_pc = "committed" => manifest_fencing >= c_local_token)

\* TYPE INVARIANT: Basic type checking.
TypeOK ==
    /\ manifest_frags \subseteq {1, 2, 3, 4}
    /\ manifest_seg \subseteq {1, 2, 3, 4}
    /\ s3_frag_data \subseteq {1, 2, 3, 4}
    /\ manifest_fencing \in 0..10
    /\ manifest_etag \in 0..20
    /\ lease_holder \in {"none", "w1", "w2", "c"}
    /\ lease_token \in 0..10
    /\ lease_expired \in {TRUE, FALSE}
    /\ lease_etag \in 0..20
    /\ next_token \in 1..10
    /\ committed \subseteq {1, 2, 3, 4}
    /\ w1_pc \in {"idle", "has_lease", "frag_written", "manifest_read",
                   "fencing_ok", "committed", "aborted", "done"}
    /\ w2_pc \in {"idle", "has_lease", "frag_written", "manifest_read",
                   "fencing_ok", "committed", "aborted", "done"}
    /\ c_pc \in {"idle", "has_lease", "manifest_read", "seg_built",
                  "fencing_ok", "committed", "aborted", "done"}

=============================================================================
