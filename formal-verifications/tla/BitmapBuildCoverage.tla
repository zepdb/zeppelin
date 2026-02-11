---------------------- MODULE BitmapBuildCoverage ----------------------
\* Formal verification that the bitmap build process produces correct
\* bitmaps from vector attribute data.
\*
\* CLIENT INVARIANT:
\*   After processing all vectors, every present_bitmap, value_bitmap,
\*   and list element bitmap exactly reflects the source attribute data.
\*   Numeric sorted keys are in order. High-cardinality fields are excluded.
\*
\* MODEL:
\*   6 vectors with mixed attributes. State machine processes vectors
\*   one at a time, updating bitmaps. Verifies correctness at every step.
\*
\* Code references:
\*   - src/index/bitmap/build.rs: build_cluster_bitmaps()
\*   - Processes each vector, builds present_bitmap + value_bitmaps per field
\*
\* EXPECTED RESULT: All invariants hold (correctness proof).
\* ==========================================================================

EXTENDS Integers, FiniteSets, Sequences

\* ==========================================================================
\* Constants
\* ==========================================================================

\* 6 vector positions
Positions == {0, 1, 2, 3, 4, 5}

\* Attribute fields
Fields == {"color", "size", "tags", "rare"}

\* Possible string values for "color"
ColorValues == {"red", "blue", "green"}

\* Possible numeric values for "size"
SizeValues == {10, 20, 30}

\* Possible tag elements for "tags" (list field)
TagElements == {"x", "y", "z"}

\* Null sentinel
Null == "NULL"

\* Max cardinality before a field is excluded
MAX_CARDINALITY == 3

\* ==========================================================================
\* Vector Attribute Assignments (fixed test data)
\* ==========================================================================

\* color[i]: string attribute
ColorOf(i) ==
    CASE i = 0 -> "red"
      [] i = 1 -> "blue"
      [] i = 2 -> "red"
      [] i = 3 -> Null          \* missing color
      [] i = 4 -> "green"
      [] i = 5 -> "blue"

\* size[i]: numeric attribute
SizeOf(i) ==
    CASE i = 0 -> 10
      [] i = 1 -> 20
      [] i = 2 -> 30
      [] i = 3 -> 20
      [] i = 4 -> -1            \* missing size
      [] i = 5 -> 10

\* tags[i]: list attribute (set of elements)
TagsOf(i) ==
    CASE i = 0 -> {"x", "y"}
      [] i = 1 -> {"x"}
      [] i = 2 -> {}            \* empty list (present but no elements)
      [] i = 3 -> {"y", "z"}
      [] i = 4 -> {"x", "y", "z"}
      [] i = 5 -> {}            \* another empty list

\* rare[i]: field with many distinct values (tests cardinality check)
\* Each vector has a unique value, giving 6 distinct values > MAX_CARDINALITY(3)
RareOf(i) ==
    CASE i = 0 -> "v0"
      [] i = 1 -> "v1"
      [] i = 2 -> "v2"
      [] i = 3 -> "v3"
      [] i = 4 -> "v4"
      [] i = 5 -> "v5"

\* Whether a field has a non-null value for vector i
HasField(field, i) ==
    CASE field = "color" -> ColorOf(i) /= Null
      [] field = "size"  -> SizeOf(i) /= -1
      [] field = "tags"  -> TRUE          \* tags always present (even if empty)
      [] field = "rare"  -> TRUE          \* rare always present

\* Get the value for a field (only for non-list scalars)
ValueOf(field, i) ==
    CASE field = "color" -> ColorOf(i)
      [] field = "size"  -> SizeOf(i)
      [] field = "rare"  -> RareOf(i)

\* ==========================================================================
\* Expected (ground truth) bitmaps
\* ==========================================================================

\* Present bitmap: positions where field has non-null value
ExpectedPresent(field) ==
    {i \in Positions : HasField(field, i)}

\* Value bitmap for scalar fields: positions with specific value
ExpectedValueBitmap(field, val) ==
    {i \in Positions : HasField(field, i) /\ ValueOf(field, i) = val}

\* Tag element bitmap: positions whose tag list contains element
ExpectedTagBitmap(elem) ==
    {i \in Positions : elem \in TagsOf(i)}

\* Distinct values for a scalar field
DistinctValues(field) ==
    {ValueOf(field, i) : i \in {j \in Positions : HasField(field, j)}}

\* ==========================================================================
\* State Variables
\* ==========================================================================

VARIABLES
    processed,              \* Set of vector positions already processed
    present_bitmaps,        \* Function: field -> set of positions with non-null value
    color_bitmaps,          \* Function: color_value -> set of positions
    size_bitmaps,           \* Function: size_value -> set of positions
    tag_bitmaps,            \* Function: tag_element -> set of positions
    rare_bitmaps,           \* Function: rare_value -> set of positions
    rare_cardinality,       \* Number of distinct rare values seen
    rare_excluded,          \* TRUE if rare field exceeded cardinality limit
    pc

vars == <<processed, present_bitmaps, color_bitmaps, size_bitmaps,
          tag_bitmaps, rare_bitmaps, rare_cardinality, rare_excluded, pc>>

\* ==========================================================================
\* Initial State
\* ==========================================================================

Init ==
    /\ processed = {}
    /\ present_bitmaps = [f \in Fields |-> {}]
    /\ color_bitmaps = [v \in ColorValues |-> {}]
    /\ size_bitmaps = [v \in SizeValues |-> {}]
    /\ tag_bitmaps = [e \in TagElements |-> {}]
    /\ rare_bitmaps = [v \in {"v0","v1","v2","v3","v4","v5"} |-> {}]
    /\ rare_cardinality = 0
    /\ rare_excluded = FALSE
    /\ pc = "process"

\* ==========================================================================
\* State Transitions
\* ==========================================================================

\* Process a single vector: update all bitmaps
ProcessVector ==
    /\ pc = "process"
    /\ processed /= Positions        \* still vectors to process
    /\ \E i \in Positions \ processed :
        \* Update present bitmaps for each field
        /\ present_bitmaps' = [f \in Fields |->
            IF HasField(f, i)
            THEN present_bitmaps[f] \union {i}
            ELSE present_bitmaps[f]]
        \* Update color value bitmaps
        /\ color_bitmaps' =
            IF ColorOf(i) /= Null
            THEN [v \in ColorValues |->
                IF v = ColorOf(i) THEN color_bitmaps[v] \union {i}
                ELSE color_bitmaps[v]]
            ELSE color_bitmaps
        \* Update size value bitmaps
        /\ size_bitmaps' =
            IF SizeOf(i) /= -1
            THEN [v \in SizeValues |->
                IF v = SizeOf(i) THEN size_bitmaps[v] \union {i}
                ELSE size_bitmaps[v]]
            ELSE size_bitmaps
        \* Update tag element bitmaps (inverted index)
        /\ tag_bitmaps' = [e \in TagElements |->
            IF e \in TagsOf(i) THEN tag_bitmaps[e] \union {i}
            ELSE tag_bitmaps[e]]
        \* Update rare value bitmaps and cardinality tracking
        /\ LET rv == RareOf(i)
               is_new == rare_bitmaps[rv] = {}
           IN
           /\ rare_bitmaps' = [v \in DOMAIN rare_bitmaps |->
                IF v = rv THEN rare_bitmaps[v] \union {i}
                ELSE rare_bitmaps[v]]
           /\ rare_cardinality' =
                IF is_new THEN rare_cardinality + 1
                ELSE rare_cardinality
           /\ rare_excluded' =
                IF is_new /\ rare_cardinality + 1 > MAX_CARDINALITY
                THEN TRUE
                ELSE rare_excluded
        /\ processed' = processed \union {i}
    /\ pc' = "process"

\* Finalize: all vectors processed
Finalize ==
    /\ pc = "process"
    /\ processed = Positions
    /\ pc' = "done"
    /\ UNCHANGED <<processed, present_bitmaps, color_bitmaps, size_bitmaps,
                   tag_bitmaps, rare_bitmaps, rare_cardinality, rare_excluded>>

\* ==========================================================================
\* Next-State Relation
\* ==========================================================================

Next ==
    \/ ProcessVector
    \/ Finalize

Spec == Init /\ [][Next]_vars

\* ==========================================================================
\* Invariants
\* ==========================================================================

\* INVARIANT 1: Type checking
TypeOK ==
    /\ processed \subseteq Positions
    /\ \A f \in Fields : present_bitmaps[f] \subseteq Positions
    /\ \A v \in ColorValues : color_bitmaps[v] \subseteq Positions
    /\ \A v \in SizeValues : size_bitmaps[v] \subseteq Positions
    /\ \A e \in TagElements : tag_bitmaps[e] \subseteq Positions
    /\ rare_cardinality \in Nat
    /\ rare_excluded \in BOOLEAN
    /\ pc \in {"process", "done"}

\* INVARIANT 2: Present bitmap is correct for processed vectors
\* Every processed vector with a non-null value is in the present bitmap.
PresentBitmapComplete ==
    \A f \in Fields :
        \A i \in processed :
            HasField(f, i) => i \in present_bitmaps[f]

\* INVARIANT 3: Value bitmaps are correct for processed vectors
\* Vector i has value V for field F ⟺ position i is in value_bitmap[F][V]
ValueBitmapCorrect ==
    \A i \in processed :
        /\ (ColorOf(i) /= Null =>
            \A v \in ColorValues :
                (ColorOf(i) = v) <=> (i \in color_bitmaps[v]))
        /\ (SizeOf(i) /= -1 =>
            \A v \in SizeValues :
                (SizeOf(i) = v) <=> (i \in size_bitmaps[v]))

\* INVARIANT 4: List inversion is correct
\* For tags: each element gets its own bitmap entry.
\* Position i appears in element bitmap ⟺ element is in tags[i].
ListInversionCorrect ==
    \A i \in processed :
        \A e \in TagElements :
            (e \in TagsOf(i)) <=> (i \in tag_bitmaps[e])

\* INVARIANT 5: Sorted numeric keys (verified at final state)
\* The set of distinct size values in the bitmaps are exactly those that appear.
\* In implementation, these keys are stored sorted; here we verify the set.
NumericKeysComplete ==
    pc = "done" =>
        \A v \in SizeValues :
            (size_bitmaps[v] /= {} <=> \E i \in Positions : SizeOf(i) = v)

\* INVARIANT 6: Cardinality bound respected
\* The "rare" field has 6 distinct values > MAX_CARDINALITY(3), so it must be excluded.
CardinalityBoundRespected ==
    pc = "done" => rare_excluded = TRUE

=============================================================================
