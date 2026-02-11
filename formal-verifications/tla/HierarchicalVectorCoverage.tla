---------------------- MODULE HierarchicalVectorCoverage ----------------------
\* Formal verification that recursive k-means partitioning in
\* hierarchical index building never drops or duplicates vectors.
\*
\* CLIENT INVARIANT:
\*   After compaction, every upserted vector is in exactly one leaf
\*   cluster. The partition is exhaustive (no vectors lost) and
\*   disjoint (no vectors duplicated).
\*
\* MODEL:
\*   Single sequential process (no concurrency). Work-queue approach:
\*   pending_groups holds sets still to partition, leaf_clusters holds
\*   completed leaves. Non-deterministic partition over-approximates
\*   k-means nearest-centroid assignment.
\*
\* Code references:
\*   - src/index/hierarchical/build.rs: build_subtree() recursive partitioning
\*   - K-means assigns each vector to nearest centroid, then recurses
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* Constants (hardcoded for finite model checking)
\* ==========================================================================

\* 4 vectors to partition. Models the input vector set.
AllVectors == {1, 2, 3, 4}

\* Max vectors per leaf cluster (build.rs: leaf_size parameter).
LeafSize == 2

\* Branching factor = 2 (binary k-means split at each level).
\* Implicit in the Partition step which splits into exactly 2 groups.

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    pending_groups,    \* Set of Sets: groups still to be partitioned
    leaf_clusters,     \* Set of Sets: completed leaf clusters
    current,           \* Set: the group currently being processed ({} if none)
    pc                 \* Program counter

vars == <<pending_groups, leaf_clusters, current, pc>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ pending_groups = {AllVectors}    \* Start with all vectors in one group
    /\ leaf_clusters = {}
    /\ current = {}
    /\ pc = "pick"

\* ==========================================================================
\* State Transitions
\* ==========================================================================

\* Pick a group from the pending queue to process next.
\* Models build_subtree() popping the next group from the work queue.
Pick ==
    /\ pc = "pick"
    /\ pending_groups /= {}
    /\ \E g \in pending_groups :
        /\ current' = g
        /\ pending_groups' = pending_groups \ {g}
    /\ pc' = "check_size"
    /\ UNCHANGED leaf_clusters

\* Check if the current group fits in a leaf cluster.
\* Models the base case check: vectors.len() <= leaf_size.
CheckSize ==
    /\ pc = "check_size"
    /\ IF Cardinality(current) <= LeafSize
       THEN pc' = "make_leaf"
       ELSE pc' = "partition"
    /\ UNCHANGED <<pending_groups, leaf_clusters, current>>

\* Current group is small enough — promote to leaf cluster.
\* Models build_subtree() writing a leaf cluster to S3.
MakeLeaf ==
    /\ pc = "make_leaf"
    /\ leaf_clusters' = leaf_clusters \union {current}
    /\ current' = {}
    /\ IF pending_groups = {}
       THEN pc' = "done"
       ELSE pc' = "pick"
    /\ UNCHANGED pending_groups

\* Current group is too large — partition into 2 non-empty subgroups.
\* Uses existential quantification over all valid binary splits.
\* This over-approximates k-means: real k-means picks ONE specific
\* split (nearest centroid), but we check ALL possible splits.
\* If invariants hold for all splits, they hold for the real one.
Partition ==
    /\ pc = "partition"
    /\ \E A \in (SUBSET current \ {{}, current}) :
        LET B == current \ A IN
        pending_groups' = pending_groups \union {A, B}
    /\ current' = {}
    /\ pc' = "pick"
    /\ UNCHANGED leaf_clusters

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ Pick
    \/ CheckSize
    \/ MakeLeaf
    \/ Partition

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants (all EXPECTED TO HOLD)
\* ==========================================================================

\* INVARIANT 1: Every vector ends up in some leaf cluster.
\* No vector is lost during recursive partitioning.
AllVectorsCovered ==
    pc = "done" => UNION leaf_clusters = AllVectors

\* INVARIANT 2: No vector appears in more than one leaf cluster.
\* Each vector is assigned to exactly one partition.
NoDuplicates ==
    pc = "done" =>
        \A c1, c2 \in leaf_clusters :
            c1 /= c2 => c1 \cap c2 = {}

\* INVARIANT 3: Conservation — at every step, the union of all groups
\* (pending, in-progress, and completed) equals AllVectors.
\* This is the loop invariant that guarantees coverage.
InProgressCoverage ==
    (UNION leaf_clusters) \union (UNION pending_groups) \union current = AllVectors

\* INVARIANT 4: Type checking.
TypeOK ==
    /\ pending_groups \subseteq SUBSET AllVectors
    /\ leaf_clusters \subseteq SUBSET AllVectors
    /\ current \subseteq AllVectors
    /\ pc \in {"pick", "check_size", "make_leaf", "partition", "done"}

=============================================================================
