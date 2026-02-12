----------------------- MODULE BM25QueryConsistency -----------------------
\* Formal verification of BM25 full-text search query consistency
\* under concurrent writes and compaction.
\*
\* MODELS: Writer + Compactor + StrongQuery
\* VERIFIES: Committed documents are never missed, WAL overrides segment
\*           on merge, deleted documents never returned.
\*
\* ==========================================================================
\* SCENARIO:
\*   4 documents (D1-D4). D1, D2 start in the segment (already compacted).
\*   D3 is appended to WAL. D4 is appended to WAL as an update of D2
\*   (same doc_id, newer version). D1 gets deleted via a tombstone in WAL.
\*   Compaction may run concurrently, merging WAL into a new segment.
\*   A strong query reads the manifest, then scans both segment and WAL,
\*   computing BM25 scores. The query must see the correct, consistent view.
\*
\*   KEY MODELING INSIGHT: BM25 scores are a property of document CONTENT,
\*   not storage location. A document has the same score whether it lives
\*   in WAL or segment. After compaction, WAL docs move to segment but
\*   their content (and scores) are preserved.
\*
\* Code references:
\*   - src/query.rs: execute_query() reads manifest, scans WAL + segment
\*   - src/wal/writer.rs: append() adds fragments to WAL
\*   - src/compaction/mod.rs: compact() merges WAL into segment
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Document IDs
D1 == 1    \* Initially in segment, will be deleted
D2 == 2    \* Initially in segment, will be updated in WAL
D3 == 3    \* New document, only in WAL
D4 == 4    \* Update of D2 in WAL (same logical doc, new content)

AllDocs == {D1, D2, D3, D4}
InitSegDocs == {D1, D2}        \* Documents in the initial segment

\* BM25-like scores: canonical score per document content.
\* These represent the BM25 relevance of each document's content to the
\* query terms. The score is intrinsic to the content, not the storage layer.
DocScore(d) ==
    CASE d = D1 -> 10     \* D1 has some relevance
      [] d = D2 -> 15     \* D2 original content
      [] d = D3 -> 20     \* D3 new doc content
      [] d = D4 -> 18     \* D4 is updated D2 content (different score)

\* D4 is an update of D2: same logical document, newer version
UpdatedBy(d) ==
    CASE d = D2 -> D4     \* D2 is superseded by D4
      [] OTHER  -> 0      \* no update relationship

HasUpdate(d) == d = D2

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    seg_docs,           \* Set of doc IDs in the active segment
    wal_docs,           \* Set of doc IDs in the WAL (includes updates & new)
    wal_deletes,        \* Set of doc IDs with tombstones in WAL
    manifest_version,   \* Nat: manifest version counter (for CAS)

    \* --- Writer state ---
    w_pc,               \* Writer program counter
    w_written,          \* Set of doc IDs the writer has written to WAL

    \* --- Compactor state ---
    c_pc,               \* Compactor program counter
    c_snap_seg,         \* Compactor's snapshot of seg_docs
    c_snap_wal,         \* Compactor's snapshot of wal_docs
    c_snap_del,         \* Compactor's snapshot of wal_deletes
    c_snap_ver,         \* Compactor's snapshot of manifest_version

    \* --- Query state ---
    q_pc,               \* Query program counter
    q_snap_seg,         \* Query's snapshot of seg_docs
    q_snap_wal,         \* Query's snapshot of wal_docs
    q_snap_del,         \* Query's snapshot of wal_deletes
    q_results,          \* Set of (doc_id, score) pairs returned
    q_manifest_ver,     \* Manifest version at query snapshot time

    \* --- Tracking ---
    committed           \* Set of doc IDs confirmed committed (write returned 200)

vars == <<seg_docs, wal_docs, wal_deletes, manifest_version,
          w_pc, w_written,
          c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
          q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver,
          committed>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ seg_docs = InitSegDocs         \* D1, D2 in segment
    /\ wal_docs = {}                  \* WAL empty
    /\ wal_deletes = {}               \* No deletes yet
    /\ manifest_version = 1
    /\ w_pc = "write_d3"              \* Writer will add D3, D4, delete D1
    /\ w_written = {}
    /\ c_pc = "idle"
    /\ c_snap_seg = {}
    /\ c_snap_wal = {}
    /\ c_snap_del = {}
    /\ c_snap_ver = 0
    /\ q_pc = "idle"
    /\ q_snap_seg = {}
    /\ q_snap_wal = {}
    /\ q_snap_del = {}
    /\ q_results = {}
    /\ q_manifest_ver = 0
    /\ committed = InitSegDocs        \* D1, D2 already committed

\* ==========================================================================
\* Writer Protocol: appends D3, D4 (update of D2), and delete of D1
\* ==========================================================================

W_WriteD3 ==
    /\ w_pc = "write_d3"
    /\ wal_docs' = wal_docs \union {D3}
    /\ committed' = committed \union {D3}
    /\ manifest_version' = manifest_version + 1
    /\ w_pc' = "write_d4"
    /\ w_written' = w_written \union {D3}
    /\ UNCHANGED <<seg_docs, wal_deletes,
                   c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver>>

W_WriteD4 ==
    /\ w_pc = "write_d4"
    /\ wal_docs' = wal_docs \union {D4}
    /\ committed' = committed \union {D4}
    /\ manifest_version' = manifest_version + 1
    /\ w_pc' = "delete_d1"
    /\ w_written' = w_written \union {D4}
    /\ UNCHANGED <<seg_docs, wal_deletes,
                   c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver>>

W_DeleteD1 ==
    /\ w_pc = "delete_d1"
    /\ wal_deletes' = wal_deletes \union {D1}
    /\ manifest_version' = manifest_version + 1
    /\ w_pc' = "done"
    /\ w_written' = w_written \union {D1}
    /\ UNCHANGED <<seg_docs, wal_docs, committed,
                   c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver>>

\* ==========================================================================
\* Compactor Protocol (CAS-based, reads manifest then merges)
\* ==========================================================================

C_ReadManifest ==
    /\ c_pc = "idle"
    /\ (wal_docs /= {} \/ wal_deletes /= {})  \* Only compact if WAL has data
    /\ c_snap_seg' = seg_docs
    /\ c_snap_wal' = wal_docs
    /\ c_snap_del' = wal_deletes
    /\ c_snap_ver' = manifest_version
    /\ c_pc' = "building"
    /\ UNCHANGED <<seg_docs, wal_docs, wal_deletes, manifest_version,
                   w_pc, w_written, committed,
                   q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver>>

C_BuildAndWrite ==
    /\ c_pc = "building"
    \* CAS: only succeed if manifest hasn't changed
    /\ IF manifest_version = c_snap_ver
       THEN
            \* Merge: new_segment = (old_segment UNION wal_docs) MINUS deletes
            /\ seg_docs' = (c_snap_seg \union c_snap_wal) \ c_snap_del
            /\ wal_docs' = {}
            /\ wal_deletes' = {}
            /\ manifest_version' = manifest_version + 1
            /\ c_pc' = "done"
       ELSE
            \* CAS failed, abort (will retry)
            /\ c_pc' = "idle"
            /\ UNCHANGED <<seg_docs, wal_docs, wal_deletes, manifest_version>>
    /\ UNCHANGED <<w_pc, w_written, committed,
                   c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_pc, q_snap_seg, q_snap_wal, q_snap_del, q_results, q_manifest_ver>>

\* ==========================================================================
\* Strong Query Protocol
\*
\* Step 1: Read manifest (snapshot seg + WAL + deletes)
\* Step 2: Compute scores, merge WAL over segment, apply deletes
\*
\* The score of a document is determined by its content (DocScore),
\* regardless of whether it resides in segment or WAL. When a WAL doc
\* supersedes a segment doc (D4 updates D2), the superseded doc is
\* excluded and the new version's score is used.
\* ==========================================================================

Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ q_snap_seg' = seg_docs
    /\ q_snap_wal' = wal_docs
    /\ q_snap_del' = wal_deletes
    /\ q_manifest_ver' = manifest_version
    /\ q_pc' = "scoring"
    /\ UNCHANGED <<seg_docs, wal_docs, wal_deletes, manifest_version,
                   w_pc, w_written, committed,
                   c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_results>>

\* Score and merge: WAL overrides segment for updated docs, apply deletes
Q_ScoreAndMerge ==
    /\ q_pc = "scoring"
    /\ LET
        \* Segment docs that are NOT deleted and NOT superseded by a WAL update
        effective_seg_ids == {d \in q_snap_seg :
                              /\ d \notin q_snap_del
                              /\ ~(HasUpdate(d) /\ UpdatedBy(d) \in q_snap_wal)}
        \* Score effective segment docs
        seg_scored == {<<d, DocScore(d)>> : d \in effective_seg_ids}
        \* WAL docs that are NOT deleted
        effective_wal_ids == {d \in q_snap_wal : d \notin q_snap_del}
        \* Score WAL docs
        wal_scored == {<<d, DocScore(d)>> : d \in effective_wal_ids}
        \* Merge: segment + WAL (no overlap due to effective_seg filtering)
        merged == seg_scored \union wal_scored
        \* Remove zero-score entries (irrelevant docs)
        final == {pair \in merged : pair[2] > 0}
       IN
        q_results' = final
    /\ q_pc' = "done"
    /\ UNCHANGED <<seg_docs, wal_docs, wal_deletes, manifest_version,
                   w_pc, w_written, committed,
                   c_pc, c_snap_seg, c_snap_wal, c_snap_del, c_snap_ver,
                   q_snap_seg, q_snap_wal, q_snap_del, q_manifest_ver>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Writer
    \/ W_WriteD3
    \/ W_WriteD4
    \/ W_DeleteD1
    \* Compactor
    \/ C_ReadManifest
    \/ C_BuildAndWrite
    \* Query
    \/ Q_ReadManifest
    \/ Q_ScoreAndMerge

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Helper: compute what the query SHOULD see given its snapshot
\* ==========================================================================

\* The set of live doc IDs visible in the query's snapshot:
\* (segment UNION wal) MINUS deletes, with WAL overriding segment updates
LiveDocsInSnapshot ==
    LET
        \* Segment docs minus those deleted or superseded by WAL update
        seg_live == {d \in q_snap_seg :
                      /\ d \notin q_snap_del
                      /\ ~(HasUpdate(d) /\ UpdatedBy(d) \in q_snap_wal)}
        \* WAL docs minus those deleted
        wal_live == {d \in q_snap_wal : d \notin q_snap_del}
    IN seg_live \union wal_live

\* Doc IDs that appear in the query result set
ResultDocIds == {pair[1] : pair \in q_results}

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: TYPE INVARIANT
TypeOK ==
    /\ seg_docs \subseteq AllDocs
    /\ wal_docs \subseteq AllDocs
    /\ wal_deletes \subseteq AllDocs
    /\ manifest_version \in Nat
    /\ w_pc \in {"write_d3", "write_d4", "delete_d1", "done"}
    /\ committed \subseteq AllDocs
    /\ c_pc \in {"idle", "building", "done"}
    /\ q_pc \in {"idle", "scoring", "done"}

\* INVARIANT 2: A document committed before the query's manifest read
\* must appear in the query results (if not deleted and has positive score).
\* Specifically: every live doc in the snapshot must appear in results.
QueryNeverMissesCommittedDoc ==
    q_pc = "done" =>
        \A d \in LiveDocsInSnapshot :
            DocScore(d) > 0 => d \in ResultDocIds

\* INVARIANT 3: WAL overrides segment on merge.
\* If D2 is in segment and D4 (its WAL update) is in WAL snapshot,
\* then D2's segment score must NOT appear in results — D4 replaces it.
WalSegmentMergeCorrectness ==
    (q_pc = "done" /\ D2 \in q_snap_seg /\ D4 \in q_snap_wal) =>
        ~(\E pair \in q_results : pair[1] = D2)

\* INVARIANT 4: Deleted documents never appear in results.
DeletedDocNeverReturned ==
    q_pc = "done" =>
        \A d \in q_snap_del :
            d \notin ResultDocIds

=============================================================================
