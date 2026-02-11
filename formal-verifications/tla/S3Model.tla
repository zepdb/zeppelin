------------------------------- MODULE S3Model --------------------------------
\* Models AWS S3 as a linearizable key-value store.
\*
\* S3 provides read-after-write consistency for PUT and DELETE operations
\* (as of December 2020). Each S3 operation is modeled as a single atomic
\* step — faithful to the actual S3 consistency model.
\*
\* This module defines reusable operators for the protocol specs.
\* Each importing spec defines its own state variables representing S3 objects
\* (fragments, manifest, segments) and uses these operators to manipulate them.
\*
\* Usage: INSTANCE S3Model in each protocol spec, or inline the operators.
\*
\* Reference: https://aws.amazon.com/s3/consistency/
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* --------------------------------------------------------------------------
\* Operators for modeling S3 as a set-based store
\* (used when S3 keys map to presence/absence, e.g., fragment data files)
\* --------------------------------------------------------------------------

\* Add an object to S3 (PUT). Idempotent.
\* @param store  A set of keys currently in S3.
\* @param key    The key to write.
\* @return       Updated set with the key added.
S3PutKey(store, key) == store \union {key}

\* Remove an object from S3 (DELETE). Idempotent.
\* @param store  A set of keys currently in S3.
\* @param key    The key to remove.
\* @return       Updated set with the key removed.
S3DeleteKey(store, key) == store \ {key}

\* Remove multiple objects from S3 (batch DELETE).
\* @param store  A set of keys currently in S3.
\* @param keys   The set of keys to remove.
\* @return       Updated set with all specified keys removed.
S3DeleteKeys(store, keys) == store \ keys

\* Check if an object exists in S3.
\* @param store  A set of keys currently in S3.
\* @param key    The key to check.
\* @return       TRUE if the key is present.
S3KeyExists(store, key) == key \in store

\* --------------------------------------------------------------------------
\* Operators for modeling S3 as a value store
\* (used when we need to track the content of S3 objects, e.g., manifest)
\* --------------------------------------------------------------------------

\* S3 manifest is modeled as a record that gets atomically replaced on PUT.
\* The manifest structure is defined per-spec since each spec tracks
\* different aspects of the manifest.
\*
\* The critical property: S3 PUT replaces the entire object atomically.
\* There is no compare-and-swap (CAS) — a blind PUT always succeeds,
\* even if the object was modified since the last GET.
\*
\* This is the root cause of the bugs we verify:
\*   1. Process A: GET manifest
\*   2. Process B: GET manifest, modify, PUT manifest
\*   3. Process A: modify (stale copy), PUT manifest
\*   → Process B's update is silently lost.

\* --------------------------------------------------------------------------
\* S3 Consistency Axioms (for documentation; not checked by TLC)
\* --------------------------------------------------------------------------
\*
\* Axiom 1 (Read-After-Write): A GET issued after a PUT completes returns
\*   the value written by that PUT (or a later PUT). There is no eventual
\*   consistency window.
\*
\* Axiom 2 (Atomicity): Each GET/PUT/DELETE operates on the full object
\*   atomically. There are no partial reads or partial writes.
\*
\* Axiom 3 (No CAS): PUT is unconditional. S3 does not offer
\*   compare-and-swap or conditional writes (as of 2024). The only way
\*   to achieve CAS-like behavior is via S3 Object Lock or external
\*   coordination (e.g., DynamoDB).
\*
\* Axiom 4 (List Consistency): LIST is eventually consistent with respect
\*   to recent PUTs and DELETEs. A recently written key may not appear
\*   in LIST results immediately. (We model LIST as immediately consistent
\*   in these specs to keep the state space small; the real bugs are in
\*   the read-modify-write protocols, not in LIST timing.)

=============================================================================
