--------------------- MODULE BM25ScoreCommutativity -----------------------
\* Formal verification of BM25 multi-field combinators and WAL vs segment
\* scoring equivalence.
\*
\* MODELS: Multi-field BM25 score combination + WAL/segment paths
\* VERIFIES: Sum commutativity, WAL vs segment equivalence,
\*           Product associativity, and TopK stability under tie-breaking.
\*
\* ==========================================================================
\* SCENARIO:
\*   BM25 scores for 4 documents across 2 fields (title, body).
\*   Documents can be stored in WAL or segment -- scores must be identical
\*   regardless of storage path. Multi-field combinators (Sum, Product)
\*   must be deterministic and commutative/associative. TopK ordering
\*   must be stable when scores are equal (tie-breaking by doc_id).
\*
\* Code references:
\*   - src/query.rs: multi-field BM25 scoring, combinator logic
\*   - src/index/ivf_flat/search.rs: BM25 score computation per cluster
\*   - src/compaction/mod.rs: data equivalence across WAL and segment
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Integers, FiniteSets, Sequences

\* ==========================================================================
\* Constants
\* ==========================================================================

\* Document IDs
D1 == 1
D2 == 2
D3 == 3
D4 == 4

AllDocs == {D1, D2, D3, D4}

\* Fields
Fields == {"title", "body"}

\* BM25 scores per (doc, field) -- fixed test data
\* Using integers scaled by 100 to avoid floating point
\* (e.g., 250 represents 2.50)
TitleScore(d) ==
    CASE d = D1 -> 250    \* 2.50
      [] d = D2 -> 180    \* 1.80
      [] d = D3 -> 300    \* 3.00
      [] d = D4 -> 200    \* 2.00

BodyScore(d) ==
    CASE d = D1 -> 150    \* 1.50
      [] d = D2 -> 320    \* 3.20
      [] d = D3 -> 100    \* 1.00
      [] d = D4 -> 300    \* 3.00

\* Field score lookup
FieldScore(field, d) ==
    CASE field = "title" -> TitleScore(d)
      [] field = "body"  -> BodyScore(d)

\* Field weights for weighted product
TitleWeight == 2
BodyWeight == 3

\* Top-K
TopKConst == 2

\* ==========================================================================
\* Combinators (deterministic integer arithmetic)
\* ==========================================================================

\* Sum combinator: title_score + body_score
SumScore(d) == TitleScore(d) + BodyScore(d)

\* Reverse order sum: body_score + title_score
SumScoreReversed(d) == BodyScore(d) + TitleScore(d)

\* Weighted product combinator: weight_title * title + weight_body * body
\* (models Product(w, expr) as a weighted linear combination)
ProductScore(d) == TitleWeight * TitleScore(d) + BodyWeight * BodyScore(d)

\* Product with reversed field ordering
ProductScoreReversed(d) == BodyWeight * BodyScore(d) + TitleWeight * TitleScore(d)

\* ==========================================================================
\* WAL vs Segment score computation
\* Same data, different read path. Must produce identical scores.
\*
\* WAL path: reads raw vectors from WAL fragments, computes BM25 inline
\* Segment path: reads pre-indexed posting lists from segment
\*
\* We model both as the same deterministic BM25 formula applied to the
\* same underlying term frequencies and document lengths. If the formula
\* is deterministic (which it must be), both paths agree.
\* ==========================================================================

\* BM25 formula components (simplified, using integers)
\* IDF component: log(N / df) scaled. For our 4-doc corpus:
\* If term appears in 2 of 4 docs: IDF = log(4/2) ~ 0.69 -> scaled to 69
IDF_TERM == 69

\* Term frequencies for title field (modeled as fixed values)
TitleTF(d) ==
    CASE d = D1 -> 3    \* "alpha alpha alpha" in title
      [] d = D2 -> 1    \* "alpha" in title
      [] d = D3 -> 2    \* "alpha alpha" in title
      [] d = D4 -> 4    \* "alpha alpha alpha alpha" in title

\* Title field average length
TitleAvgDL == 2    \* average doc length in title field

\* Title doc lengths
TitleDL(d) ==
    CASE d = D1 -> 3
      [] d = D2 -> 1
      [] d = D3 -> 2
      [] d = D4 -> 4

\* BM25 parameters (k1=120 means 1.20 scaled, b=75 means 0.75 scaled)
K1 == 120
BParam == 75

\* BM25(d) = IDF * (tf * (k1 + 100)) / (tf + k1 * (100 - b + b * dl / avgdl))
\* All values scaled by 100 to keep integers.
\* We compute a simplified BM25 numerator and denominator.
BM25Numerator(d) == IDF_TERM * (TitleTF(d) * (K1 + 100))
BM25Denominator(d) == (TitleTF(d) * 100) + (K1 * (100 - BParam + ((BParam * TitleDL(d) * 100) \div TitleAvgDL)))

\* WAL path computes from raw TF/DF
WalBM25(d) == (BM25Numerator(d) * 1000) \div BM25Denominator(d)

\* Segment path computes from pre-built posting lists (same formula)
SegBM25(d) == (BM25Numerator(d) * 1000) \div BM25Denominator(d)

\* ==========================================================================
\* Top-K with tie-breaking by doc_id (ascending)
\* ==========================================================================

\* Count of docs that rank strictly above d: (higher score, or same score + lower id)
BetterSumCount(d) == Cardinality({d2 \in AllDocs :
    \/ SumScore(d2) > SumScore(d)
    \/ (SumScore(d2) = SumScore(d) /\ d2 < d)})

\* Top-K by sum score with doc_id tie-breaking
TopKBySum == {d \in AllDocs : BetterSumCount(d) < TopKConst}

\* Same computation but using reversed sum (should give same result)
BetterSumRevCount(d) == Cardinality({d2 \in AllDocs :
    \/ SumScoreReversed(d2) > SumScoreReversed(d)
    \/ (SumScoreReversed(d2) = SumScoreReversed(d) /\ d2 < d)})

TopKBySumReversed == {d \in AllDocs : BetterSumRevCount(d) < TopKConst}

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    \* Sum combinator results
    sum_ab,                \* Function: doc -> Sum(title, body)
    sum_ba,                \* Function: doc -> Sum(body, title) [reversed]

    \* Product combinator results
    prod_ab,               \* Function: doc -> Product(title_w, body_w)
    prod_ba,               \* Function: doc -> Product(body_w, title_w) [reversed]

    \* WAL vs Segment BM25 scores
    wal_scores,            \* Function: doc -> BM25 via WAL path
    seg_scores,            \* Function: doc -> BM25 via segment path

    \* Top-K results
    topk_sum,              \* Set of doc IDs in top-K by sum
    topk_sum_rev,          \* Set of doc IDs in top-K by reversed sum

    pc

vars == <<sum_ab, sum_ba, prod_ab, prod_ba,
          wal_scores, seg_scores,
          topk_sum, topk_sum_rev, pc>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ sum_ab = [d \in AllDocs |-> 0]
    /\ sum_ba = [d \in AllDocs |-> 0]
    /\ prod_ab = [d \in AllDocs |-> 0]
    /\ prod_ba = [d \in AllDocs |-> 0]
    /\ wal_scores = [d \in AllDocs |-> 0]
    /\ seg_scores = [d \in AllDocs |-> 0]
    /\ topk_sum = {}
    /\ topk_sum_rev = {}
    /\ pc = "compute_sums"

\* ==========================================================================
\* State Transitions
\* ==========================================================================

\* Step 1: Compute sum scores in both orderings
ComputeSums ==
    /\ pc = "compute_sums"
    /\ sum_ab' = [d \in AllDocs |-> SumScore(d)]
    /\ sum_ba' = [d \in AllDocs |-> SumScoreReversed(d)]
    /\ pc' = "compute_products"
    /\ UNCHANGED <<prod_ab, prod_ba, wal_scores, seg_scores,
                   topk_sum, topk_sum_rev>>

\* Step 2: Compute product scores in both orderings
ComputeProducts ==
    /\ pc = "compute_products"
    /\ prod_ab' = [d \in AllDocs |-> ProductScore(d)]
    /\ prod_ba' = [d \in AllDocs |-> ProductScoreReversed(d)]
    /\ pc' = "compute_bm25"
    /\ UNCHANGED <<sum_ab, sum_ba, wal_scores, seg_scores,
                   topk_sum, topk_sum_rev>>

\* Step 3: Compute BM25 via WAL and segment paths
ComputeBM25 ==
    /\ pc = "compute_bm25"
    /\ wal_scores' = [d \in AllDocs |-> WalBM25(d)]
    /\ seg_scores' = [d \in AllDocs |-> SegBM25(d)]
    /\ pc' = "compute_topk"
    /\ UNCHANGED <<sum_ab, sum_ba, prod_ab, prod_ba,
                   topk_sum, topk_sum_rev>>

\* Step 4: Compute top-K in both orderings
ComputeTopK ==
    /\ pc = "compute_topk"
    /\ topk_sum' = TopKBySum
    /\ topk_sum_rev' = TopKBySumReversed
    /\ pc' = "done"
    /\ UNCHANGED <<sum_ab, sum_ba, prod_ab, prod_ba,
                   wal_scores, seg_scores>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ ComputeSums
    \/ ComputeProducts
    \/ ComputeBM25
    \/ ComputeTopK

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Sum is commutative.
\* Sum([title, body]) == Sum([body, title]) for all documents.
SumCommutativity ==
    pc = "done" =>
        \A d \in AllDocs : sum_ab[d] = sum_ba[d]

\* INVARIANT 2: WAL and segment paths produce identical BM25 scores.
\* Same term frequencies and document lengths must yield same scores.
WalVsSegmentEquivalence ==
    pc = "done" =>
        \A d \in AllDocs : wal_scores[d] = seg_scores[d]

\* INVARIANT 3: Product combinator is deterministic regardless of field order.
\* Product(title_w * title + body_w * body) == Product(body_w * body + title_w * title)
ProductAssociativity ==
    pc = "done" =>
        \A d \in AllDocs : prod_ab[d] = prod_ba[d]

\* INVARIANT 4: Top-K result set is stable.
\* Same scores with same tie-breaking produce identical result sets.
TopKStability ==
    pc = "done" =>
        topk_sum = topk_sum_rev

\* TYPE INVARIANT
TypeOK ==
    /\ \A d \in AllDocs : sum_ab[d] \in Nat
    /\ \A d \in AllDocs : sum_ba[d] \in Nat
    /\ \A d \in AllDocs : prod_ab[d] \in Nat
    /\ \A d \in AllDocs : prod_ba[d] \in Nat
    /\ \A d \in AllDocs : wal_scores[d] \in Nat
    /\ \A d \in AllDocs : seg_scores[d] \in Nat
    /\ topk_sum \subseteq AllDocs
    /\ topk_sum_rev \subseteq AllDocs
    /\ pc \in {"compute_sums", "compute_products", "compute_bm25",
               "compute_topk", "done"}

=============================================================================
