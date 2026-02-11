----------------------------- MODULE ULIDOrdering -----------------------------
\* Formal verification of "latest wins" semantics under clock skew.
\*
\* MODELS: Two writers with potentially skewed clocks + Strong query
\* FINDS:  Whether clock skew can cause the "wrong" value to win when
\*         two writers upsert the same vector ID.
\*
\* ==========================================================================
\* THE PROBLEM:
\*   Zeppelin uses ULIDs (Universally Unique Lexicographically Sortable
\*   Identifiers) to order operations. The merge/dedup logic processes
\*   fragments in ULID order — later ULIDs overwrite earlier ones.
\*
\*   ULIDs encode a millisecond timestamp in their high bits. If server
\*   clocks go backwards (NTP adjustment, VM migration, leap second),
\*   a later upsert could receive a ULID that sorts BEFORE an earlier one.
\*   The "wrong" value would then win during merge.
\*
\*   Code references:
\*     - src/compaction/mod.rs:107-120  — merge: processes in ULID order
\*     - src/query.rs:129-139           — WAL scan: processes in ULID order
\*     - Both use "latest ULID wins" for dedup
\*
\* CLIENT INVARIANT:
\*   For upserts U1 then U2 of the same vector ID (ordered by HTTP
\*   response time), a subsequent strong query returns U2's value.
\*
\* EXPECTED RESULT:
\*   - Under normal clocks (monotonic ULIDs): invariant HOLDS.
\*   - Under clock skew: TLC finds a counterexample where the query
\*     returns U1's value instead of U2's.
\* ==========================================================================

EXTENDS Naturals, FiniteSets, Sequences

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    manifest_frags,     \* Sequence of fragment records: <<[ulid, value]>>
    s3_frag_data,       \* Set of fragment records stored on S3

    \* --- Concurrency control ---
    mutex,              \* "free" | "w1" | "w2"

    \* --- Clock model ---
    \* Each writer has a local clock. ULIDs are derived from clock values.
    \* clock_w1, clock_w2: Nat (logical timestamps)
    clock_w1,
    clock_w2,
    clock_skew,         \* TRUE if clocks can go out of order

    \* --- Writer state ---
    w1_pc,
    w1_ulid,            \* ULID assigned to w1's fragment
    w1_value,           \* Value w1 is writing ("val_w1")

    w2_pc,
    w2_ulid,
    w2_value,

    \* --- Commit ordering ---
    \* Records the order in which HTTP 200 responses were returned.
    \* This is the "real" ordering that clients observe.
    commit_order,       \* Sequence of process names, e.g., <<"w1", "w2">>

    \* --- Query state ---
    q_pc,
    q_result            \* The value returned by the query ("val_w1" | "val_w2" | "none")

vars == <<manifest_frags, s3_frag_data, mutex, clock_w1, clock_w2, clock_skew,
          w1_pc, w1_ulid, w1_value, w2_pc, w2_ulid, w2_value,
          commit_order, q_pc, q_result>>

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Both writers upsert the SAME vector ID (this is the conflict case).
\* They differ only in their values and ULIDs.

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ manifest_frags = <<>>                    \* Empty manifest (no fragments)
    /\ s3_frag_data = {}
    /\ mutex = "free"
    /\ clock_w1 = 10                            \* w1 starts with clock at 10
    /\ clock_w2 = 10                            \* w2 starts with clock at 10
    /\ clock_skew = TRUE                        \* Enable clock skew model
                                                \* Set to FALSE to verify normal case
    /\ w1_pc = "idle"
    /\ w1_ulid = 0
    /\ w1_value = 1                             \* w1 writes value 1
    /\ w2_pc = "idle"
    /\ w2_ulid = 0
    /\ w2_value = 2                             \* w2 writes value 2
    /\ commit_order = <<>>
    /\ q_pc = "idle"
    /\ q_result = 0                             \* 0 = no result yet

\* ==========================================================================
\* Writer w1 Protocol
\* ==========================================================================

W1_AcquireMutex ==
    /\ w1_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w1"
    \* Assign ULID based on current clock
    /\ w1_ulid' = clock_w1
    /\ w1_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, s3_frag_data, clock_w1, clock_w2, clock_skew,
                   w1_value, w2_pc, w2_ulid, w2_value,
                   commit_order, q_pc, q_result>>

W1_WriteAndCommit ==
    /\ w1_pc = "acquired"
    \* Write fragment to S3 and update manifest atomically (under mutex)
    /\ LET frag == [ulid |-> w1_ulid, value |-> w1_value]
       IN /\ s3_frag_data' = s3_frag_data \union {frag}
          /\ manifest_frags' = Append(manifest_frags, frag)
    /\ mutex' = "free"
    /\ commit_order' = Append(commit_order, "w1")
    /\ w1_pc' = "done"
    \* Advance w1's clock (normal clock behavior)
    /\ clock_w1' = clock_w1 + 1
    /\ UNCHANGED <<clock_w2, clock_skew,
                   w1_ulid, w1_value, w2_pc, w2_ulid, w2_value,
                   q_pc, q_result>>

\* ==========================================================================
\* Writer w2 Protocol
\* ==========================================================================

W2_AcquireMutex ==
    /\ w2_pc = "idle"
    /\ mutex = "free"
    /\ mutex' = "w2"
    \* Under clock skew, w2's clock might be BEHIND w1's
    /\ IF clock_skew
       THEN \/ (w2_ulid' = clock_w2 /\ w2_pc' = "acquired")      \* Normal
            \/ (w2_ulid' = clock_w2 - 5 /\ w2_pc' = "acquired")  \* Skewed back!
       ELSE /\ w2_ulid' = clock_w2
            /\ w2_pc' = "acquired"
    /\ UNCHANGED <<manifest_frags, s3_frag_data, clock_w1, clock_w2, clock_skew,
                   w1_pc, w1_ulid, w1_value, w2_value,
                   commit_order, q_pc, q_result>>

W2_WriteAndCommit ==
    /\ w2_pc = "acquired"
    /\ LET frag == [ulid |-> w2_ulid, value |-> w2_value]
       IN /\ s3_frag_data' = s3_frag_data \union {frag}
          /\ manifest_frags' = Append(manifest_frags, frag)
    /\ mutex' = "free"
    /\ commit_order' = Append(commit_order, "w2")
    /\ w2_pc' = "done"
    /\ clock_w2' = clock_w2 + 1
    /\ UNCHANGED <<clock_w1, clock_skew,
                   w1_pc, w1_ulid, w1_value, w2_ulid, w2_value,
                   q_pc, q_result>>

\* ==========================================================================
\* Query Protocol (Strong consistency)
\*
\* Reads all fragments, sorts by ULID, last write wins (same as
\* the merge logic in compaction/mod.rs:107-120 and query.rs:129-139).
\* ==========================================================================

Q_Start ==
    /\ q_pc = "idle"
    \* Only query after both writers are done (to test the invariant)
    /\ w1_pc = "done"
    /\ w2_pc = "done"
    /\ q_pc' = "querying"
    /\ UNCHANGED <<manifest_frags, s3_frag_data, mutex, clock_w1, clock_w2,
                   clock_skew, w1_pc, w1_ulid, w1_value, w2_pc, w2_ulid,
                   w2_value, commit_order, q_result>>

\* The query processes fragments in ULID order (ascending).
\* For fragments with the same vector ID, the one with the higher ULID wins.
Q_Execute ==
    /\ q_pc = "querying"
    /\ Len(manifest_frags) > 0
    \* Find the fragment with the maximum ULID (latest wins)
    /\ LET max_ulid_frag ==
            CHOOSE f \in {manifest_frags[i] : i \in 1..Len(manifest_frags)} :
                \A g \in {manifest_frags[j] : j \in 1..Len(manifest_frags)} :
                    f.ulid >= g.ulid
       IN q_result' = max_ulid_frag.value
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_frags, s3_frag_data, mutex, clock_w1, clock_w2,
                   clock_skew, w1_pc, w1_ulid, w1_value, w2_pc, w2_ulid,
                   w2_value, commit_order>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ W1_AcquireMutex \/ W1_WriteAndCommit
    \/ W2_AcquireMutex \/ W2_WriteAndCommit
    \/ Q_Start \/ Q_Execute

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* The last committer's value should be the query result.
\* "Last committer" is determined by commit_order (HTTP response ordering).
LastCommitterWins ==
    (q_pc = "done" /\ Len(commit_order) = 2) =>
        LET last_committer == commit_order[2]
        IN CASE last_committer = "w1" -> q_result = w1_value
             [] last_committer = "w2" -> q_result = w2_value

\* Under normal clocks (clock_skew = FALSE), this should HOLD.
\* Under clock skew (clock_skew = TRUE), this may FAIL — TLC will find
\* a trace where w2 commits after w1 but gets a lower ULID, so the
\* query returns w1's value instead of w2's.

\* TYPE INVARIANT
TypeOK ==
    /\ mutex \in {"free", "w1", "w2"}
    /\ w1_pc \in {"idle", "acquired", "done"}
    /\ w2_pc \in {"idle", "acquired", "done"}
    /\ q_pc \in {"idle", "querying", "done"}
    /\ w1_value = 1
    /\ w2_value = 2

=============================================================================
