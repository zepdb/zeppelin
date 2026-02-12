---------------------- MODULE InvertedIndexBuildSafety ----------------------
\* Formal verification that compaction building an inverted index
\* produces correct posting lists while queries read the old segment.
\*
\* MODELS: Compactor (builds inverted index) + Query (reads old segment)
\* VERIFIES: Index covers all docs, posting lists are genuine, IDF stats
\*           are consistent, and compaction preserves all non-deleted data.
\*
\* ==========================================================================
\* SCENARIO:
\*   6 documents with text fields. D1-D3 are in the old segment.
\*   D4-D5 are in WAL. D6 is a tombstone (delete of D3).
\*   Compaction reads the WAL and old segment, builds a new inverted
\*   index (posting lists per term), and writes a new segment.
\*   A concurrent query can read the old segment during this process.
\*
\* Code references:
\*   - src/compaction/mod.rs: compact() merges WAL into segment
\*   - src/index/ivf_flat/build.rs: builds inverted index
\*   - src/query.rs: reads segment posting lists for BM25
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Document IDs
D1 == 1
D2 == 2
D3 == 3    \* Will be deleted
D4 == 4
D5 == 5
D6 == 6    \* Tombstone for D3

AllDocIds == {D1, D2, D3, D4, D5, D6}

\* Terms in the vocabulary
Terms == {"alpha", "beta", "gamma", "delta"}

\* Document-term mapping: which terms appear in which documents
\* This models the text content of each document.
DocTerms(d) ==
    CASE d = D1 -> {"alpha", "beta"}           \* D1 contains "alpha beta"
      [] d = D2 -> {"beta", "gamma"}           \* D2 contains "beta gamma"
      [] d = D3 -> {"alpha", "gamma", "delta"} \* D3 contains "alpha gamma delta" (will be deleted)
      [] d = D4 -> {"alpha", "delta"}          \* D4 contains "alpha delta"
      [] d = D5 -> {"beta", "delta"}           \* D5 contains "beta delta"
      [] d = D6 -> {}                          \* D6 is a tombstone, no terms

\* Initial state
OldSegDocs == {D1, D2, D3}     \* Documents in old segment
WalDocs == {D4, D5}            \* New documents in WAL
WalDeletes == {D3}             \* D3 is deleted (D6 is the tombstone)

\* Expected new segment: old_seg UNION wal_docs MINUS deletes
ExpectedNewSegDocs == (OldSegDocs \union WalDocs) \ WalDeletes

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- Old segment (immutable, readable by queries) ---
    old_seg,                \* Set of doc IDs in old segment
    old_posting,            \* Function: term -> set of doc IDs with that term

    \* --- WAL state ---
    wal_docs,               \* Set of doc IDs in WAL
    wal_deletes,            \* Set of doc IDs to delete

    \* --- Compactor state ---
    c_pc,                   \* Compactor program counter
    c_merged_docs,          \* Set of doc IDs in the merged result
    c_processed,            \* Set of doc IDs already indexed
    c_posting,              \* Function: term -> set of doc IDs (new posting list)
    c_doc_count,            \* Number of distinct docs indexed so far

    \* --- New segment (written by compactor) ---
    new_seg,                \* Set of doc IDs in new segment (empty until compaction done)
    new_posting,            \* Function: term -> set of doc IDs in new segment

    \* --- Query state ---
    q_pc,                   \* Query program counter
    q_reading_old           \* TRUE if query is reading old segment

vars == <<old_seg, old_posting, wal_docs, wal_deletes,
          c_pc, c_merged_docs, c_processed, c_posting, c_doc_count,
          new_seg, new_posting, q_pc, q_reading_old>>

\* ==========================================================================
\* Helper: compute correct posting list for a set of docs
\* ==========================================================================

PostingFor(docs, term) == {d \in docs : term \in DocTerms(d)}

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ old_seg = OldSegDocs
    /\ old_posting = [t \in Terms |-> PostingFor(OldSegDocs, t)]
    /\ wal_docs = WalDocs
    /\ wal_deletes = WalDeletes
    /\ c_pc = "idle"
    /\ c_merged_docs = {}
    /\ c_processed = {}
    /\ c_posting = [t \in Terms |-> {}]
    /\ c_doc_count = 0
    /\ new_seg = {}
    /\ new_posting = [t \in Terms |-> {}]
    /\ q_pc = "idle"
    /\ q_reading_old = FALSE

\* ==========================================================================
\* Compactor Protocol
\*
\* Step 1: Read manifest, compute merged doc set
\* Step 2: Index each document (one at a time)
\* Step 3: Finalize new segment
\* ==========================================================================

C_ReadAndMerge ==
    /\ c_pc = "idle"
    /\ c_merged_docs' = (old_seg \union wal_docs) \ wal_deletes
    /\ c_pc' = "indexing"
    /\ UNCHANGED <<old_seg, old_posting, wal_docs, wal_deletes,
                   c_processed, c_posting, c_doc_count,
                   new_seg, new_posting, q_pc, q_reading_old>>

\* Index one document at a time (models the loop in build)
C_IndexDoc ==
    /\ c_pc = "indexing"
    /\ c_processed /= c_merged_docs     \* Still docs to index
    /\ \E d \in c_merged_docs \ c_processed :
        /\ c_posting' = [t \in Terms |->
            IF t \in DocTerms(d)
            THEN c_posting[t] \union {d}
            ELSE c_posting[t]]
        /\ c_doc_count' = c_doc_count + 1
        /\ c_processed' = c_processed \union {d}
    /\ UNCHANGED <<old_seg, old_posting, wal_docs, wal_deletes,
                   c_pc, c_merged_docs,
                   new_seg, new_posting, q_pc, q_reading_old>>

\* All docs indexed, write new segment
C_Finalize ==
    /\ c_pc = "indexing"
    /\ c_processed = c_merged_docs      \* All docs indexed
    /\ new_seg' = c_merged_docs
    /\ new_posting' = c_posting
    /\ c_pc' = "done"
    /\ UNCHANGED <<old_seg, old_posting, wal_docs, wal_deletes,
                   c_merged_docs, c_processed, c_posting, c_doc_count,
                   q_pc, q_reading_old>>

\* ==========================================================================
\* Query Protocol (reads old segment while compaction runs)
\* ==========================================================================

Q_StartRead ==
    /\ q_pc = "idle"
    /\ q_reading_old' = TRUE
    /\ q_pc' = "reading"
    /\ UNCHANGED <<old_seg, old_posting, wal_docs, wal_deletes,
                   c_pc, c_merged_docs, c_processed, c_posting, c_doc_count,
                   new_seg, new_posting>>

Q_FinishRead ==
    /\ q_pc = "reading"
    \* Query can read old segment at any time — old segment is immutable
    /\ q_pc' = "done"
    /\ UNCHANGED <<old_seg, old_posting, wal_docs, wal_deletes,
                   c_pc, c_merged_docs, c_processed, c_posting, c_doc_count,
                   new_seg, new_posting, q_reading_old>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ C_ReadAndMerge
    \/ C_IndexDoc
    \/ C_Finalize
    \/ Q_StartRead
    \/ Q_FinishRead

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Every doc in the new segment has a posting entry for each
\* of its terms. No document is missed in the inverted index.
IndexCoversAllDocuments ==
    c_pc = "done" =>
        \A d \in new_seg :
            \A t \in DocTerms(d) :
                d \in new_posting[t]

\* INVARIANT 2: Every (term, doc) pair in a posting list is genuine.
\* No false positives: if doc d is in posting[t], then t really appears in d.
PostingListCorrectness ==
    c_pc = "done" =>
        \A t \in Terms :
            \A d \in new_posting[t] :
                t \in DocTerms(d)

\* INVARIANT 3: The doc_count tracked during indexing matches the actual
\* number of distinct documents in the new segment.
IDFStatsConsistent ==
    c_pc = "done" =>
        c_doc_count = Cardinality(new_seg)

\* INVARIANT 4: Compaction does not lose data.
\* new_segment == old_segment UNION wal_docs MINUS wal_deletes
CompactionDoesNotLoseData ==
    c_pc = "done" =>
        new_seg = ExpectedNewSegDocs

\* INVARIANT 5: Old segment remains readable during compaction.
\* The old_posting list is correct for the old segment at all times.
OldSegmentStable ==
    \A t \in Terms :
        old_posting[t] = PostingFor(OldSegDocs, t)

\* TYPE INVARIANT
TypeOK ==
    /\ old_seg \subseteq AllDocIds
    /\ wal_docs \subseteq AllDocIds
    /\ wal_deletes \subseteq AllDocIds
    /\ c_pc \in {"idle", "indexing", "done"}
    /\ c_merged_docs \subseteq AllDocIds
    /\ c_processed \subseteq AllDocIds
    /\ c_doc_count \in Nat
    /\ new_seg \subseteq AllDocIds
    /\ q_pc \in {"idle", "reading", "done"}
    /\ q_reading_old \in BOOLEAN

=============================================================================
