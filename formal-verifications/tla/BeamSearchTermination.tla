------------------------ MODULE BeamSearchTermination ------------------------
\* Formal verification that beam search over a hierarchical ANN index
\* always terminates and reaches leaf nodes.
\*
\* Tests BOTH balanced and unbalanced (mixed-depth) trees:
\*
\*   Balanced (7 nodes):       Unbalanced (5 nodes):
\*       root                      root
\*      /    \                    /    \
\*    n1      n2               n1     L3  <- leaf at level 1!
\*   / \     / \              / \
\*  L1  L2  L3  L4           L1  L2
\*
\* The unbalanced tree exercises the mixed-depth code path in
\* search.rs:124-232 where some children are leaves and others
\* are internal nodes at the same expansion step.
\*
\* CLIENT INVARIANT:
\*   Beam search always terminates and finds at least one leaf cluster.
\*
\* Code references:
\*   - src/index/hierarchical/search.rs: beam search loop (lines 45-233)
\*   - Mixed-depth handling at line 222 (TODO comment in code)
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets

\* ==========================================================================
\* Constants (hardcoded for finite model checking)
\* ==========================================================================

BeamWidth == 2        \* Max candidates kept per level
MaxSteps  == 5        \* Safety bound: exceeding this flags non-termination

\* All node IDs used across both tree variants.
AllNodeIds == {"root", "n1", "n2", "L1", "L2", "L3", "L4"}

\* ==========================================================================
\* Tree Definitions
\* ==========================================================================

\* --- Balanced tree (7 nodes, 3 levels) ---

BalancedLevel(node) ==
    CASE node = "root" -> 2
    []   node = "n1"   -> 1
    []   node = "n2"   -> 1
    []   node = "L1"   -> 0
    []   node = "L2"   -> 0
    []   node = "L3"   -> 0
    []   node = "L4"   -> 0

BalancedIsLeaf(node) ==
    CASE node = "root" -> FALSE
    []   node = "n1"   -> FALSE
    []   node = "n2"   -> FALSE
    []   node = "L1"   -> TRUE
    []   node = "L2"   -> TRUE
    []   node = "L3"   -> TRUE
    []   node = "L4"   -> TRUE

BalancedChildren(node) ==
    CASE node = "root" -> {"n1", "n2"}
    []   node = "n1"   -> {"L1", "L2"}
    []   node = "n2"   -> {"L3", "L4"}
    []   node = "L1"   -> {}
    []   node = "L2"   -> {}
    []   node = "L3"   -> {}
    []   node = "L4"   -> {}

\* --- Unbalanced tree (5 nodes, mixed-depth: L3 is a leaf child of root) ---

UnbalancedLevel(node) ==
    CASE node = "root" -> 2
    []   node = "n1"   -> 1
    []   node = "L1"   -> 0
    []   node = "L2"   -> 0
    []   node = "L3"   -> 0

UnbalancedIsLeaf(node) ==
    CASE node = "root" -> FALSE
    []   node = "n1"   -> FALSE
    []   node = "L1"   -> TRUE
    []   node = "L2"   -> TRUE
    []   node = "L3"   -> TRUE

UnbalancedChildren(node) ==
    CASE node = "root" -> {"n1", "L3"}
    []   node = "n1"   -> {"L1", "L2"}
    []   node = "L1"   -> {}
    []   node = "L2"   -> {}
    []   node = "L3"   -> {}

\* ==========================================================================
\* Tree Dispatch Helpers
\* Dispatch to the correct tree based on tree_id (chosen at Init).
\* ==========================================================================

Level(tid, node) ==
    IF tid = "balanced" THEN BalancedLevel(node) ELSE UnbalancedLevel(node)

IsLeaf(tid, node) ==
    IF tid = "balanced" THEN BalancedIsLeaf(node) ELSE UnbalancedIsLeaf(node)

Children(tid, node) ==
    IF tid = "balanced" THEN BalancedChildren(node) ELSE UnbalancedChildren(node)

\* Max level among a non-empty set of nodes.
MaxLevelOf(tid, nodes) ==
    CHOOSE m \in {Level(tid, n) : n \in nodes} :
        \A n \in nodes : Level(tid, n) <= m

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    tree_id,               \* "balanced" | "unbalanced" — chosen at init
    current_beam,          \* Set of node IDs: internal nodes to expand next
    visited,               \* Set of node IDs: already expanded
    leaf_clusters_found,   \* Set of leaf node IDs found during search
    step_count,            \* Nat: number of expand-select rounds completed
    max_level,             \* Nat: max level of nodes in current beam
    prev_max_level,        \* Nat: max level from previous beam (monotonicity)
    pc                     \* Program counter

vars == <<tree_id, current_beam, visited, leaf_clusters_found,
          step_count, max_level, prev_max_level, pc>>

\* ==========================================================================
\* Initial State
\* Non-deterministic choice of tree structure. TLC verifies BOTH.
\* ==========================================================================

Init ==
    /\ tree_id \in {"balanced", "unbalanced"}
    /\ current_beam = {"root"}
    /\ visited = {}
    /\ leaf_clusters_found = {}
    /\ step_count = 0
    /\ max_level = 2          \* Root is at level 2 in both trees
    /\ prev_max_level = 2
    /\ pc = "expand"

\* ==========================================================================
\* State Transitions
\* ==========================================================================

\* Expand current beam: collect all children, non-deterministically select
\* up to BeamWidth candidates. Leaves go to leaf_clusters_found, internal
\* nodes become the next beam. Over-approximates distance-based ranking.
\*
\* Models search.rs:127-232 beam search loop body.
ExpandAndSelect ==
    /\ pc = "expand"
    /\ step_count < MaxSteps
    /\ LET all_children == UNION {Children(tree_id, n) : n \in current_beam}
       IN
       /\ all_children /= {}        \* Internal nodes always have children
       /\ \E selected \in SUBSET all_children :
           /\ selected /= {}
           /\ Cardinality(selected) <= BeamWidth
           /\ LET sel_leaves   == {s \in selected : IsLeaf(tree_id, s)}
                  sel_internal == {s \in selected : ~IsLeaf(tree_id, s)}
                  new_max == IF sel_internal /= {}
                             THEN MaxLevelOf(tree_id, sel_internal)
                             ELSE 0
              IN
              /\ visited' = visited \union current_beam
              /\ leaf_clusters_found' = leaf_clusters_found \union sel_leaves
              /\ step_count' = step_count + 1
              /\ prev_max_level' = max_level
              /\ IF sel_internal = {}
                 THEN \* All selected are leaves — search complete
                      /\ current_beam' = {}
                      /\ max_level' = 0
                      /\ pc' = "done"
                 ELSE \* Continue descent with internal nodes
                      /\ current_beam' = sel_internal
                      /\ max_level' = new_max
                      /\ pc' = "expand"
    /\ UNCHANGED tree_id

\* Safety valve: if step_count hits MaxSteps, flag non-termination.
\* This should NEVER be reachable if the search terminates correctly.
Timeout ==
    /\ pc = "expand"
    /\ step_count >= MaxSteps
    /\ pc' = "terminated"
    /\ UNCHANGED <<tree_id, current_beam, visited, leaf_clusters_found,
                   step_count, max_level, prev_max_level>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ ExpandAndSelect
    \/ Timeout

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants (all EXPECTED TO HOLD)
\* ==========================================================================

\* INVARIANT 1: Search always terminates within MaxSteps.
\* If violated, the beam search loop is unbounded.
SearchTerminates ==
    pc /= "terminated"

\* INVARIANT 2: No node is expanded (visited) more than once.
\* The beam never revisits nodes already processed.
NoRevisiting ==
    current_beam \cap visited = {}

\* INVARIANT 3: The max level of the beam never increases.
\* Ensures monotonic descent through the tree toward leaves.
\* Children always have strictly lower levels than their parents,
\* so the beam's max level must decrease at each expansion step.
LevelMonotonicity ==
    max_level <= prev_max_level

\* INVARIANT 4: Search always finds at least one leaf cluster.
\* A completed search that found no leaves would be useless.
LeafReachability ==
    pc = "done" => leaf_clusters_found /= {}

\* INVARIANT 5: Type checking.
TypeOK ==
    /\ tree_id \in {"balanced", "unbalanced"}
    /\ current_beam \subseteq AllNodeIds
    /\ visited \subseteq AllNodeIds
    /\ leaf_clusters_found \subseteq AllNodeIds
    /\ step_count \in 0..MaxSteps
    /\ max_level \in 0..2
    /\ prev_max_level \in 0..2
    /\ pc \in {"expand", "done", "terminated"}

=============================================================================
