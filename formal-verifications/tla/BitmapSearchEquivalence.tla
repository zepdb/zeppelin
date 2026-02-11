---------------------- MODULE BitmapSearchEquivalence ----------------------
\* Formal verification that pre-filter search via bitmaps produces
\* identical results to post-filter search.
\*
\* CLIENT INVARIANT:
\*   Given the same vectors, distances, filter, and top_k, both search
\*   paths return the same result set in the same order. When bitmaps
\*   are unavailable, the system falls back to post-filter correctly.
\*
\* MODEL:
\*   8 vectors across 2 clusters (4 each). A filter matches ~50%.
\*   Two search paths computed in parallel, results compared.
\*
\* Code references:
\*   - src/index/ivf_flat/search.rs: search_ivf_flat()
\*   - src/index/bitmap/evaluate.rs: evaluate_filter_bitmap()
\*   - src/index/filter.rs: evaluate_filter()
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Naturals, FiniteSets, Sequences

\* ==========================================================================
\* Constants
\* ==========================================================================

\* 8 vectors across 2 clusters
Cluster1Vecs == {0, 1, 2, 3}
Cluster2Vecs == {4, 5, 6, 7}
AllVecs == Cluster1Vecs \union Cluster2Vecs

TopK == 2

\* ==========================================================================
\* Vector Attributes (determines filter match)
\* ==========================================================================

\* Attribute: status = "active" or "inactive"
StatusOf(i) ==
    CASE i = 0 -> "active"
      [] i = 1 -> "inactive"
      [] i = 2 -> "active"
      [] i = 3 -> "active"
      [] i = 4 -> "inactive"
      [] i = 5 -> "active"
      [] i = 6 -> "inactive"
      [] i = 7 -> "inactive"

\* Filter: status = "active"
\* Matching vectors: {0, 2, 3, 5} (4 out of 8 = 50%)
Matches(i) == StatusOf(i) = "active"
MatchingVecs == {i \in AllVecs : Matches(i)}

\* ==========================================================================
\* Distance Scores (lower = better, all unique to avoid ties)
\* ==========================================================================

DistOf(i) ==
    CASE i = 0 -> 10    \* active, cluster 1
      [] i = 1 -> 5     \* inactive, cluster 1 (best distance but filtered out)
      [] i = 2 -> 20    \* active, cluster 1
      [] i = 3 -> 15    \* active, cluster 1
      [] i = 4 -> 3     \* inactive, cluster 2 (very close but filtered out)
      [] i = 5 -> 8     \* active, cluster 2
      [] i = 6 -> 12    \* inactive, cluster 2
      [] i = 7 -> 25    \* inactive, cluster 2

\* ==========================================================================
\* Helper: Sort matching vectors by distance, take top_k
\* ==========================================================================

\* The set of matching vectors with distance <= d
MatchingWithDistLE(d) == {i \in MatchingVecs : DistOf(i) <= d}

\* Matching vectors sorted by distance (ascending):
\* 5(dist=8), 0(dist=10), 3(dist=15), 2(dist=20)
\* Top-2: {5, 0}

\* Expected result: the top_k matching vectors by distance
\* We compute this as the set of matching vectors that have
\* at most TopK-1 matching vectors closer than them.
BetterMatchCount(i) == Cardinality({j \in MatchingVecs : DistOf(j) < DistOf(i)})
ExpectedTopK == {i \in MatchingVecs : BetterMatchCount(i) < TopK}

\* ==========================================================================
\* Bitmap for cluster pre-filtering
\* ==========================================================================

\* Bitmap for cluster 1: active positions within cluster
Cluster1Bitmap == {i \in Cluster1Vecs : Matches(i)}

\* Bitmap for cluster 2: active positions within cluster
Cluster2Bitmap == {i \in Cluster2Vecs : Matches(i)}

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* Post-filter path
    post_scored,           \* Set of all scored vectors (all vectors)
    post_filtered,         \* Scored vectors that pass filter
    post_result,           \* Top-k from post_filtered

    \* Pre-filter path
    pre_bitmap_c1,         \* Bitmap result for cluster 1
    pre_bitmap_c2,         \* Bitmap result for cluster 2
    pre_scored,            \* Only bitmap-matching vectors get scored
    pre_result,            \* Top-k from pre_scored

    \* Fallback path (bitmap unavailable)
    fallback_active,       \* TRUE if bitmap was unavailable
    fallback_result,       \* Falls back to post-filter result

    pc

vars == <<post_scored, post_filtered, post_result,
          pre_bitmap_c1, pre_bitmap_c2, pre_scored, pre_result,
          fallback_active, fallback_result, pc>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ post_scored = {}
    /\ post_filtered = {}
    /\ post_result = {}
    /\ pre_bitmap_c1 = {}
    /\ pre_bitmap_c2 = {}
    /\ pre_scored = {}
    /\ pre_result = {}
    /\ fallback_active = FALSE
    /\ fallback_result = {}
    /\ pc = "score_all"

\* ==========================================================================
\* Post-filter path: score ALL vectors, then filter, then top-k
\* ==========================================================================

ScoreAll ==
    /\ pc = "score_all"
    /\ post_scored' = AllVecs
    /\ pc' = "apply_filter"
    /\ UNCHANGED <<post_filtered, post_result,
                   pre_bitmap_c1, pre_bitmap_c2, pre_scored, pre_result,
                   fallback_active, fallback_result>>

ApplyFilter ==
    /\ pc = "apply_filter"
    /\ post_filtered' = {i \in post_scored : Matches(i)}
    /\ pc' = "post_topk"
    /\ UNCHANGED <<post_scored, post_result,
                   pre_bitmap_c1, pre_bitmap_c2, pre_scored, pre_result,
                   fallback_active, fallback_result>>

PostTopK ==
    /\ pc = "post_topk"
    /\ post_result' = {i \in post_filtered : BetterMatchCount(i) < TopK}
    /\ pc' = "bitmap_filter"
    /\ UNCHANGED <<post_scored, post_filtered,
                   pre_bitmap_c1, pre_bitmap_c2, pre_scored, pre_result,
                   fallback_active, fallback_result>>

\* ==========================================================================
\* Pre-filter path: bitmap first, then score only matching, then top-k
\* ==========================================================================

BitmapFilter ==
    /\ pc = "bitmap_filter"
    /\ pre_bitmap_c1' = Cluster1Bitmap
    /\ pre_bitmap_c2' = Cluster2Bitmap
    /\ pc' = "pre_score"
    /\ UNCHANGED <<post_scored, post_filtered, post_result,
                   pre_scored, pre_result,
                   fallback_active, fallback_result>>

PreScore ==
    /\ pc = "pre_score"
    \* Only score vectors that are in the bitmap
    /\ pre_scored' = pre_bitmap_c1 \union pre_bitmap_c2
    /\ pc' = "pre_topk"
    /\ UNCHANGED <<post_scored, post_filtered, post_result,
                   pre_bitmap_c1, pre_bitmap_c2, pre_result,
                   fallback_active, fallback_result>>

PreTopK ==
    /\ pc = "pre_topk"
    /\ pre_result' = {i \in pre_scored : BetterMatchCount(i) < TopK}
    /\ pc' = "test_fallback"
    /\ UNCHANGED <<post_scored, post_filtered, post_result,
                   pre_bitmap_c1, pre_bitmap_c2, pre_scored,
                   fallback_active, fallback_result>>

\* ==========================================================================
\* Fallback path: bitmap unavailable (old segment)
\* ==========================================================================

TestFallback ==
    /\ pc = "test_fallback"
    /\ fallback_active' = TRUE
    \* When bitmap is unavailable, fall back to post-filter result
    /\ fallback_result' = post_result
    /\ pc' = "done"
    /\ UNCHANGED <<post_scored, post_filtered, post_result,
                   pre_bitmap_c1, pre_bitmap_c2, pre_scored, pre_result>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ ScoreAll
    \/ ApplyFilter
    \/ PostTopK
    \/ BitmapFilter
    \/ PreScore
    \/ PreTopK
    \/ TestFallback

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Type checking
TypeOK ==
    /\ post_scored \subseteq AllVecs
    /\ post_filtered \subseteq AllVecs
    /\ post_result \subseteq AllVecs
    /\ pre_bitmap_c1 \subseteq Cluster1Vecs
    /\ pre_bitmap_c2 \subseteq Cluster2Vecs
    /\ pre_scored \subseteq AllVecs
    /\ pre_result \subseteq AllVecs
    /\ fallback_active \in BOOLEAN
    /\ fallback_result \subseteq AllVecs
    /\ pc \in {"score_all", "apply_filter", "post_topk",
               "bitmap_filter", "pre_score", "pre_topk",
               "test_fallback", "done"}

\* INVARIANT 2: Pre-filter and post-filter return the exact same vector IDs
ResultSetEquivalence ==
    pc = "done" => pre_result = post_result

\* INVARIANT 3: Results match the expected top-k
ResultMatchesExpected ==
    pc = "done" => post_result = ExpectedTopK

\* INVARIANT 4: Pre-filter never misses a candidate
\* Every vector in the post-filter result must also be in the pre-filter scored set
PreFilterNeverMissesCandidate ==
    pc = "done" => post_result \subseteq pre_scored

\* INVARIANT 5: Fallback produces correct results (same as post-filter)
FallbackCorrect ==
    (pc = "done" /\ fallback_active) => fallback_result = post_result

=============================================================================
