---------------------- MODULE IndexAtomicity ----------------------
EXTENDS Integers, FiniteSets, TLC

CONSTANTS 
    SegmentComponents \* {"Vectors", "FTS", "Centroids"}

VARIABLES 
    s3_objects,      \* Set of objects
    manifest_segment, \* Boolean: does manifest point to the segment?
    query_result     \* "Success", "Fail", "Stale"

Vars == <<s3_objects, manifest_segment, query_result>>

Init ==
    /\ s3_objects = {}
    /\ manifest_segment = FALSE
    /\ query_result = "Stale"

\* Action: Uploader writes a component
Upload(c) ==
    /\ ~(c \in s3_objects)
    /\ s3_objects' = s3_objects \cup {c}
    /\ UNCHANGED <<manifest_segment, query_result>>

\* Action: Uploader commits manifest (prematurely?)
\* In reality, code should "Upload All -> Then Manifest".
\* But what if "Upload All" is partial (network error) and we proceed?
CommitManifest ==
    /\ manifest_segment = FALSE
    /\ manifest_segment' = TRUE
    /\ UNCHANGED <<s3_objects, query_result>>

\* Action: Query attempts to read segment
Query ==
    /\ manifest_segment = TRUE
    /\ IF SegmentComponents \subseteq s3_objects 
       THEN query_result' = "Success"
       ELSE query_result' = "Fail" \* Availability loss
    /\ UNCHANGED <<s3_objects, manifest_segment>>

Next ==
    \/ \E c \in SegmentComponents : Upload(c)
    \/ CommitManifest
    \/ Query

\* Invariant: We never want "Fail". 
\* This implies Manifest should ONLY be committed if SegmentComponents \subseteq s3_objects.
Availability ==
    query_result /= "Fail"

Spec == Init /\ [][Next]_Vars

=============================================================================
