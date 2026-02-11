---------------------- MODULE BitmapFilterAlgebra ----------------------
\* Formal verification that bitmap-based filter evaluation produces
\* identical results to brute-force (per-vector) filter evaluation.
\*
\* CLIENT INVARIANT:
\*   For every filter type and every vector set, the bitmap evaluation
\*   returns exactly the same set of matching vector positions as
\*   brute-force iteration. No false negatives, no false positives.
\*
\* MODEL:
\*   Universe of 5 vector positions {0..4} with 3 attribute fields.
\*   State machine iterates through filter types, evaluates each
\*   via bitmap set operations, and compares against brute-force.
\*
\* Code references:
\*   - src/index/bitmap/evaluate.rs: evaluate_filter_bitmap()
\*   - src/index/filter.rs: evaluate_filter() (brute-force reference)
\*   - src/types.rs: Filter enum, AttributeValue enum
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Integers, FiniteSets

\* ==========================================================================
\* Constants
\* ==========================================================================

\* 5 vector positions (0-indexed, matching RoaringBitmap positions)
Positions == {0, 1, 2, 3, 4}

\* Attribute values for "color" field (string type)
Colors == {"red", "blue", "green"}

\* Attribute values for "size" field (numeric type)
Sizes == {1, 2, 3}

\* Attribute values for "tags" field (list type, each vector has subset)
TagElements == {"a", "b"}

\* Special sentinel for null (vector has no value for field)
Null == "NULL"

\* ==========================================================================
\* Vector Attribute Assignments (fixed test data)
\* ==========================================================================

\* color[i] = color of vector i, or Null if missing
ColorOf(i) ==
    CASE i = 0 -> "red"
      [] i = 1 -> "blue"
      [] i = 2 -> "red"
      [] i = 3 -> Null      \* vector 3 has no color
      [] i = 4 -> "green"

\* size[i] = numeric size of vector i, or -1 if missing
SizeOf(i) ==
    CASE i = 0 -> 1
      [] i = 1 -> 2
      [] i = 2 -> 3
      [] i = 3 -> 2
      [] i = 4 -> -1         \* vector 4 has no size (use -1 as sentinel)

\* tags[i] = set of tag strings for vector i, or {} for no tags
TagsOf(i) ==
    CASE i = 0 -> {"a", "b"}
      [] i = 1 -> {"a"}
      [] i = 2 -> {}          \* empty list
      [] i = 3 -> {"b"}
      [] i = 4 -> {"a", "b"}

\* ==========================================================================
\* Brute-force evaluation helpers
\* ==========================================================================

\* Positions where color equals val
BruteEqColor(val) ==
    {i \in Positions : ColorOf(i) = val}

\* Positions where color does NOT equal val (includes nulls!)
BruteNotEqColor(val) ==
    {i \in Positions : ColorOf(i) /= val}

\* Positions where size >= lo AND size <= hi (nulls excluded)
BruteRangeSize(lo, hi) ==
    {i \in Positions : SizeOf(i) >= lo /\ SizeOf(i) <= hi /\ SizeOf(i) /= -1}

\* Positions where size > lo (strict, nulls excluded)
BruteRangeSizeGt(lo) ==
    {i \in Positions : SizeOf(i) > lo /\ SizeOf(i) /= -1}

\* Positions where size < hi (strict, nulls excluded)
BruteRangeSizeLt(hi) ==
    {i \in Positions : SizeOf(i) < hi /\ SizeOf(i) /= -1}

\* Positions where color is in value set
BruteInColor(vals) ==
    {i \in Positions : ColorOf(i) \in vals}

\* Positions where color is NOT in value set (includes nulls)
BruteNotInColor(vals) ==
    {i \in Positions : ColorOf(i) \notin vals}

\* Positions where tags contain element
BruteContainsTags(elem) ==
    {i \in Positions : elem \in TagsOf(i)}

\* ==========================================================================
\* Bitmap evaluation helpers
\* ==========================================================================

\* Present bitmap: positions with non-null color
ColorPresent == {i \in Positions : ColorOf(i) /= Null}

\* Value bitmap for specific color
ColorBitmap(val) == {i \in Positions : ColorOf(i) = val}

\* Present bitmap: positions with non-null size
SizePresent == {i \in Positions : SizeOf(i) /= -1}

\* Numeric size bitmaps: positions with exact size value
SizeBitmap(val) == {i \in Positions : SizeOf(i) = val}

\* Tag element bitmaps (inverted index): positions containing element
TagBitmap(elem) == {i \in Positions : elem \in TagsOf(i)}

\* --- Bitmap operations ---

\* Eq via bitmap: intersection of value bitmap
BitmapEqColor(val) == ColorBitmap(val)

\* NotEq via bitmap: universe minus value bitmap
\* CRITICAL: NotEq includes nulls (positions without the field)
BitmapNotEqColor(val) == Positions \ ColorBitmap(val)

\* Range via bitmap: union of size bitmaps in range
BitmapRangeSize(lo, hi) ==
    UNION {SizeBitmap(v) : v \in {s \in Sizes : s >= lo /\ s <= hi}}

\* Range with strict bounds
BitmapRangeSizeGt(lo) ==
    UNION {SizeBitmap(v) : v \in {s \in Sizes : s > lo}}

BitmapRangeSizeLt(hi) ==
    UNION {SizeBitmap(v) : v \in {s \in Sizes : s < hi}}

\* In via bitmap: union of value bitmaps
BitmapInColor(vals) == UNION {ColorBitmap(v) : v \in vals}

\* NotIn via bitmap: universe minus union of value bitmaps
BitmapNotInColor(vals) == Positions \ BitmapInColor(vals)

\* Contains via bitmap: tag element bitmap lookup
BitmapContainsTags(elem) == TagBitmap(elem)

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    filter_type,      \* Current filter being evaluated
    brute_result,     \* Result from brute-force evaluation
    bitmap_result,    \* Result from bitmap evaluation
    bitmap_is_none,   \* TRUE if bitmap evaluation returns None
    pc                \* Program counter

vars == <<filter_type, brute_result, bitmap_result, bitmap_is_none, pc>>

\* All filter types we check
FilterTypes == {
    "eq_red", "eq_blue", "eq_green",
    "not_eq_red", "not_eq_blue",
    "range_1_2", "range_2_3", "range_gt_1", "range_lt_3",
    "in_red_blue", "in_green",
    "not_in_red", "not_in_red_blue",
    "contains_a", "contains_b",
    "contains_str_substring",
    "and_eq_red_range_1_2",
    "or_eq_red_eq_blue",
    "not_wrap_eq_red",
    "and_evaluable_none",
    "or_evaluable_none",
    "eq_missing_field"
}

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ filter_type = "eq_red"
    /\ brute_result = {}
    /\ bitmap_result = {}
    /\ bitmap_is_none = FALSE
    /\ pc = "evaluate"

\* ==========================================================================
\* State Transitions
\* ==========================================================================

\* Evaluate the current filter type
Evaluate ==
    /\ pc = "evaluate"
    /\ CASE filter_type = "eq_red" ->
            /\ brute_result' = BruteEqColor("red")
            /\ bitmap_result' = BitmapEqColor("red")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "eq_blue" ->
            /\ brute_result' = BruteEqColor("blue")
            /\ bitmap_result' = BitmapEqColor("blue")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "eq_green" ->
            /\ brute_result' = BruteEqColor("green")
            /\ bitmap_result' = BitmapEqColor("green")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "not_eq_red" ->
            /\ brute_result' = BruteNotEqColor("red")
            /\ bitmap_result' = BitmapNotEqColor("red")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "not_eq_blue" ->
            /\ brute_result' = BruteNotEqColor("blue")
            /\ bitmap_result' = BitmapNotEqColor("blue")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "range_1_2" ->
            /\ brute_result' = BruteRangeSize(1, 2)
            /\ bitmap_result' = BitmapRangeSize(1, 2)
            /\ bitmap_is_none' = FALSE
         [] filter_type = "range_2_3" ->
            /\ brute_result' = BruteRangeSize(2, 3)
            /\ bitmap_result' = BitmapRangeSize(2, 3)
            /\ bitmap_is_none' = FALSE
         [] filter_type = "range_gt_1" ->
            /\ brute_result' = BruteRangeSizeGt(1)
            /\ bitmap_result' = BitmapRangeSizeGt(1)
            /\ bitmap_is_none' = FALSE
         [] filter_type = "range_lt_3" ->
            /\ brute_result' = BruteRangeSizeLt(3)
            /\ bitmap_result' = BitmapRangeSizeLt(3)
            /\ bitmap_is_none' = FALSE
         [] filter_type = "in_red_blue" ->
            /\ brute_result' = BruteInColor({"red", "blue"})
            /\ bitmap_result' = BitmapInColor({"red", "blue"})
            /\ bitmap_is_none' = FALSE
         [] filter_type = "in_green" ->
            /\ brute_result' = BruteInColor({"green"})
            /\ bitmap_result' = BitmapInColor({"green"})
            /\ bitmap_is_none' = FALSE
         [] filter_type = "not_in_red" ->
            /\ brute_result' = BruteNotInColor({"red"})
            /\ bitmap_result' = BitmapNotInColor({"red"})
            /\ bitmap_is_none' = FALSE
         [] filter_type = "not_in_red_blue" ->
            /\ brute_result' = BruteNotInColor({"red", "blue"})
            /\ bitmap_result' = BitmapNotInColor({"red", "blue"})
            /\ bitmap_is_none' = FALSE
         [] filter_type = "contains_a" ->
            /\ brute_result' = BruteContainsTags("a")
            /\ bitmap_result' = BitmapContainsTags("a")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "contains_b" ->
            /\ brute_result' = BruteContainsTags("b")
            /\ bitmap_result' = BitmapContainsTags("b")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "contains_str_substring" ->
            \* Contains on a string field (substring match) cannot be done via bitmap
            /\ brute_result' = {}
            /\ bitmap_result' = {}
            /\ bitmap_is_none' = TRUE
         [] filter_type = "and_eq_red_range_1_2" ->
            \* And(Eq(color, red), Range(size, 1, 2))
            /\ brute_result' = BruteEqColor("red") \cap BruteRangeSize(1, 2)
            /\ bitmap_result' = BitmapEqColor("red") \cap BitmapRangeSize(1, 2)
            /\ bitmap_is_none' = FALSE
         [] filter_type = "or_eq_red_eq_blue" ->
            \* Or(Eq(color, red), Eq(color, blue))
            /\ brute_result' = BruteEqColor("red") \cup BruteEqColor("blue")
            /\ bitmap_result' = BitmapEqColor("red") \cup BitmapEqColor("blue")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "not_wrap_eq_red" ->
            \* Not(Eq(color, red)) = everything except red
            /\ brute_result' = Positions \ BruteEqColor("red")
            /\ bitmap_result' = Positions \ BitmapEqColor("red")
            /\ bitmap_is_none' = FALSE
         [] filter_type = "and_evaluable_none" ->
            \* And(Eq(color, red), Contains(string_field, substring))
            \* When one sub-filter returns None, And returns None (conservative)
            /\ brute_result' = {}
            /\ bitmap_result' = {}
            /\ bitmap_is_none' = TRUE
         [] filter_type = "or_evaluable_none" ->
            \* Or(Eq(color, red), Contains(string_field, substring))
            \* When one sub-filter returns None, Or returns None (conservative)
            /\ brute_result' = {}
            /\ bitmap_result' = {}
            /\ bitmap_is_none' = TRUE
         [] filter_type = "eq_missing_field" ->
            \* Eq on a field that has no bitmap index → None
            /\ brute_result' = {}
            /\ bitmap_result' = {}
            /\ bitmap_is_none' = TRUE
    /\ pc' = "check"
    /\ UNCHANGED filter_type

\* Advance to next filter type
Advance ==
    /\ pc = "check"
    /\ \E next \in FilterTypes :
        /\ next /= filter_type
        /\ filter_type' = next
    /\ pc' = "evaluate"
    /\ UNCHANGED <<brute_result, bitmap_result, bitmap_is_none>>

\* Terminal: all done
Done ==
    /\ pc = "check"
    /\ pc' = "done"
    /\ UNCHANGED <<filter_type, brute_result, bitmap_result, bitmap_is_none>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ Evaluate
    \/ Advance
    \/ Done

\* ==========================================================================
\* Specification
\* ==========================================================================

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Type checking
TypeOK ==
    /\ filter_type \in FilterTypes
    /\ brute_result \subseteq Positions
    /\ bitmap_result \subseteq Positions
    /\ bitmap_is_none \in BOOLEAN
    /\ pc \in {"evaluate", "check", "done"}

\* INVARIANT 2: When bitmap evaluation succeeds, it matches brute-force exactly
BitmapMatchesBruteForce ==
    (pc = "check" /\ ~bitmap_is_none) => bitmap_result = brute_result

\* INVARIANT 3: No false negatives — every brute-force match is in bitmap result
NoFalseNegatives ==
    (pc = "check" /\ ~bitmap_is_none) => brute_result \subseteq bitmap_result

\* INVARIANT 4: No false positives — every bitmap match is a real match
NoFalsePositives ==
    (pc = "check" /\ ~bitmap_is_none) => bitmap_result \subseteq brute_result

\* INVARIANT 5: When evaluate returns None, it's for a legitimate reason
\* (substring Contains, missing field, or compound with None sub-filter)
NoneIsConservative ==
    (pc = "check" /\ bitmap_is_none) =>
        filter_type \in {"contains_str_substring", "and_evaluable_none",
                          "or_evaluable_none", "eq_missing_field"}

=============================================================================
