---------------------- MODULE HierarchicalBuildAtomicity ----------------------
\* Formal verification that queries never observe a partially-built
\* hierarchical tree during compaction.
\*
\* The compactor writes multiple S3 objects for a hierarchical index:
\*   1. Leaf clusters (IVF-Flat format vectors + attributes)
\*   2. Tree nodes (centroids + child pointers)
\*   3. tree_meta.json (root node ID, num_levels, etc.)
\*   4. Manifest CAS (atomically switch segment reference)
\*
\* A query auto-detects hierarchical indexing by probing tree_meta.json
\* for the segment_id from its manifest snapshot. The key insight:
\* the segment_id comes EXCLUSIVELY from the manifest, and the manifest
\* is only updated AFTER all tree objects are written.
\*
\* PROCESSES: Compactor (4-step build + CAS) + Query (5-step read path)
\*
\* Code references:
\*   - src/index/hierarchical/build.rs: bottom-up tree build
\*   - src/query.rs:211-224: tree_meta.json probe for hierarchical detection
\*   - src/compaction/mod.rs:290-314: manifest CAS after build
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* Constants (hardcoded for finite model checking)
\* ==========================================================================

\* Expected tree objects for the new segment.
NewTreeNodes    == {"tn1", "tn2"}
NewLeafClusters == {"lc1", "lc2", "lc3"}

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* --- S3 shared state ---
    manifest_seg_id,   \* "old" | "new": which segment the manifest points to
    manifest_etag,     \* Nat: version counter for manifest CAS

    \* --- Old segment objects (always complete) ---
    \* old_tree_meta, old_tree_nodes, old_leaf_clusters are implicitly TRUE.
    \* The old segment was fully built before this spec begins.

    \* --- New segment objects (written by compactor) ---
    new_leaf_clusters, \* Set: leaf cluster objects written so far
    new_tree_nodes,    \* Set: tree node objects written so far
    new_tree_meta,     \* BOOLEAN: tree_meta.json written

    \* --- Compactor local state ---
    c_pc,              \* Program counter
    c_snap_etag,       \* Manifest etag snapshot for CAS

    \* --- Query local state ---
    q_pc,              \* Program counter
    q_seg_id,          \* "none" | "old" | "new": segment from manifest snapshot
    q_detected_hier,   \* BOOLEAN: did tree_meta.json probe succeed?
    q_nodes_ok,        \* BOOLEAN: did tree node reads succeed?
    q_clusters_ok      \* BOOLEAN: did cluster reads succeed?

vars == <<manifest_seg_id, manifest_etag,
          new_leaf_clusters, new_tree_nodes, new_tree_meta,
          c_pc, c_snap_etag,
          q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    \* Manifest points to old (complete) segment.
    /\ manifest_seg_id = "old"
    /\ manifest_etag = 1

    \* New segment objects not yet written.
    /\ new_leaf_clusters = {}
    /\ new_tree_nodes = {}
    /\ new_tree_meta = FALSE

    \* Compactor starts idle.
    /\ c_pc = "idle"
    /\ c_snap_etag = 0

    \* Query starts idle.
    /\ q_pc = "idle"
    /\ q_seg_id = "none"
    /\ q_detected_hier = FALSE
    /\ q_nodes_ok = FALSE
    /\ q_clusters_ok = FALSE

\* ==========================================================================
\* Compactor Protocol
\*
\* Models compaction building a hierarchical index:
\*   1. Read manifest (snapshot etag for CAS)
\*   2. Write leaf clusters to S3
\*   3. Write tree nodes to S3
\*   4. Write tree_meta.json to S3
\*   5. CAS manifest to point to new segment
\*
\* Order is critical: clusters before nodes before meta before CAS.
\* This ensures any observer that finds tree_meta.json for a segment
\* is guaranteed all tree data already exists on S3.
\* ==========================================================================

\* Step 1: Read manifest, snapshot etag for later CAS.
C_ReadManifest ==
    /\ c_pc = "idle"
    /\ c_snap_etag' = manifest_etag
    /\ c_pc' = "snap_taken"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, new_tree_meta,
                   q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* Step 2: Write all leaf clusters to S3.
\* In reality these are written one-by-one during recursive build,
\* but all complete before any tree nodes are written.
C_WriteClusters ==
    /\ c_pc = "snap_taken"
    /\ new_leaf_clusters' = NewLeafClusters
    /\ c_pc' = "clusters_written"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_tree_nodes, new_tree_meta, c_snap_etag,
                   q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* Step 3: Write all tree nodes to S3.
\* Internal nodes reference leaf clusters by ID — clusters must exist first.
C_WriteNodes ==
    /\ c_pc = "clusters_written"
    /\ new_tree_nodes' = NewTreeNodes
    /\ c_pc' = "nodes_written"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_meta, c_snap_etag,
                   q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* Step 4: Write tree_meta.json — the hierarchical detection sentinel.
\* This is the LAST data object written before manifest CAS.
\* A query that finds tree_meta.json is guaranteed all tree data exists.
C_WriteMeta ==
    /\ c_pc = "nodes_written"
    /\ new_tree_meta' = TRUE
    /\ c_pc' = "meta_written"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, c_snap_etag,
                   q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* Step 5: CAS manifest to point to new segment.
\* Checks etag matches snapshot (no concurrent modification).
\* After this, queries reading the manifest will get the new segment ID.
C_CasManifest ==
    /\ c_pc = "meta_written"
    /\ IF manifest_etag = c_snap_etag
       THEN \* CAS succeeds — atomically update manifest
            /\ manifest_seg_id' = "new"
            /\ manifest_etag' = manifest_etag + 1
            /\ c_pc' = "done"
       ELSE \* CAS fails — another writer modified manifest (abort)
            /\ c_pc' = "aborted"
            /\ UNCHANGED <<manifest_seg_id, manifest_etag>>
    /\ UNCHANGED <<new_leaf_clusters, new_tree_nodes, new_tree_meta, c_snap_etag,
                   q_pc, q_seg_id, q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* ==========================================================================
\* Query Protocol
\*
\* Models a query that auto-detects hierarchical indexing:
\*   1. Read manifest (get segment ID)
\*   2. Probe tree_meta.json for that segment
\*   3. If hierarchical: read tree nodes
\*   4. If hierarchical: read leaf clusters
\*   5. Done
\*
\* The query uses its manifest SNAPSHOT segment ID for all probes.
\* It never sees a segment ID that wasn't committed via manifest CAS.
\* ==========================================================================

\* Step 1: Read manifest, snapshot the current segment ID.
Q_ReadManifest ==
    /\ q_pc = "idle"
    /\ q_seg_id' = manifest_seg_id
    /\ q_pc' = "manifest_read"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, new_tree_meta,
                   c_pc, c_snap_etag,
                   q_detected_hier, q_nodes_ok, q_clusters_ok>>

\* Step 2: Probe tree_meta.json for the segment from our manifest snapshot.
\* - "old" segment: tree_meta always exists (old segment is complete)
\* - "new" segment: depends on whether compactor has written tree_meta
Q_ProbeTreeMeta ==
    /\ q_pc = "manifest_read"
    /\ IF q_seg_id = "old"
       THEN q_detected_hier' = TRUE           \* Old segment is complete
       ELSE q_detected_hier' = new_tree_meta  \* New segment: check S3
    /\ q_pc' = "meta_probed"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, new_tree_meta,
                   c_pc, c_snap_etag,
                   q_seg_id, q_nodes_ok, q_clusters_ok>>

\* Step 3: Read tree nodes (if hierarchical) or skip to done (if flat).
Q_ReadNodes ==
    /\ q_pc = "meta_probed"
    /\ IF q_detected_hier
       THEN \* Hierarchical path: attempt to read tree nodes from S3.
            /\ IF q_seg_id = "old"
               THEN q_nodes_ok' = TRUE                          \* Old is complete
               ELSE q_nodes_ok' = (new_tree_nodes = NewTreeNodes) \* Check all exist
            /\ q_pc' = "nodes_read"
       ELSE \* Non-hierarchical: skip to done (flat index path).
            /\ q_nodes_ok' = FALSE
            /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, new_tree_meta,
                   c_pc, c_snap_etag,
                   q_seg_id, q_detected_hier, q_clusters_ok>>

\* Step 4: Read leaf clusters (hierarchical path only).
Q_ReadClusters ==
    /\ q_pc = "nodes_read"
    /\ IF q_seg_id = "old"
       THEN q_clusters_ok' = TRUE                                \* Old is complete
       ELSE q_clusters_ok' = (new_leaf_clusters = NewLeafClusters) \* Check all exist
    /\ q_pc' = "done"
    /\ UNCHANGED <<manifest_seg_id, manifest_etag,
                   new_leaf_clusters, new_tree_nodes, new_tree_meta,
                   c_pc, c_snap_etag,
                   q_seg_id, q_detected_hier, q_nodes_ok>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \* Compactor
    \/ C_ReadManifest
    \/ C_WriteClusters
    \/ C_WriteNodes
    \/ C_WriteMeta
    \/ C_CasManifest
    \* Query
    \/ Q_ReadManifest
    \/ Q_ProbeTreeMeta
    \/ Q_ReadNodes
    \/ Q_ReadClusters

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants (all EXPECTED TO HOLD)
\* ==========================================================================

\* INVARIANT 1: If a query detected hierarchical indexing and completed,
\* it successfully read all tree nodes AND all leaf clusters.
\* This is the primary client-facing safety property.
QuerySeesCompleteTree ==
    (q_pc = "done" /\ q_detected_hier = TRUE)
        => (q_nodes_ok = TRUE /\ q_clusters_ok = TRUE)

\* INVARIANT 2: If tree_meta.json has been written, ALL tree nodes
\* and ALL leaf clusters have also been written.
\* This is the write-order invariant that makes the protocol safe:
\* tree_meta is the "seal" — its existence guarantees completeness.
NoPartialTreeVisible ==
    new_tree_meta = TRUE
        => (new_tree_nodes = NewTreeNodes /\ new_leaf_clusters = NewLeafClusters)

\* INVARIANT 3: Type checking.
TypeOK ==
    /\ manifest_seg_id \in {"old", "new"}
    /\ manifest_etag \in 1..5
    /\ new_leaf_clusters \subseteq NewLeafClusters
    /\ new_tree_nodes \subseteq NewTreeNodes
    /\ new_tree_meta \in {TRUE, FALSE}
    /\ c_pc \in {"idle", "snap_taken", "clusters_written", "nodes_written",
                  "meta_written", "done", "aborted"}
    /\ c_snap_etag \in 0..5
    /\ q_pc \in {"idle", "manifest_read", "meta_probed", "nodes_read", "done"}
    /\ q_seg_id \in {"none", "old", "new"}
    /\ q_detected_hier \in {TRUE, FALSE}
    /\ q_nodes_ok \in {TRUE, FALSE}
    /\ q_clusters_ok \in {TRUE, FALSE}

=============================================================================
