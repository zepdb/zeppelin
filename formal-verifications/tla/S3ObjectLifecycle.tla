---------------------- MODULE S3ObjectLifecycle ----------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS
    Fragments

VARIABLES
    s3_objects,      \* Set of objects existing in S3
    manifest_refs,   \* Set of fragments referenced by the S3 manifest
    reader_state,    \* State of the reader: "Idle", "ReadingFragments", "PartialRead", "Success"
    reader_refs      \* Snapshot of manifest refs taken when the reader starts

Vars == <<s3_objects, manifest_refs, reader_state, reader_refs>>

Init ==
    /\ s3_objects = Fragments
    /\ manifest_refs = Fragments
    /\ reader_state = "Idle"
    /\ reader_refs = {}

\* Compactor deletes a fragment that is no longer in the manifest (conceptually)
\* But here we model the race: Compactor updates manifest (removing ref) THEN deletes object.
CompactDelete(f) ==
    /\ f \in s3_objects
    /\ s3_objects' = s3_objects \ {f}
    /\ manifest_refs' = manifest_refs \ {f} \* Atomic switch in manifest
    /\ UNCHANGED <<reader_state, reader_refs>>

\* Reader Step 1: Read Manifest — snapshot the refs into reader_refs
ReadManifest ==
    /\ reader_state = "Idle"
    /\ reader_state' = "ReadingFragments"
    /\ reader_refs' = manifest_refs
    /\ UNCHANGED <<s3_objects, manifest_refs>>

\* Reader Step 2: Read Fragments
\* The reader iterates through the refs it grabbed in Step 1.
\* If an object is missing, the code currently logs warning and SKIPS it.
\* Result: Reader returns a subset of data.
ReadFragments ==
    /\ reader_state = "ReadingFragments"
    /\ LET missing == {f \in reader_refs : ~(f \in s3_objects)} IN
       IF missing /= {}
       THEN
            \* BUG: We successfully return partial data!
            reader_state' = "PartialRead"
       ELSE
            reader_state' = "Success"
    /\ UNCHANGED <<s3_objects, manifest_refs, reader_refs>>

Next ==
    \/ \E f \in Fragments : CompactDelete(f)
    \/ ReadManifest
    \/ ReadFragments

\* Invariant: We should never return a PartialRead.
\* If objects are missing, we should either Error or Retry, but the code "skips".
NoPartialReads ==
    reader_state /= "PartialRead"

Spec == Init /\ [][Next]_Vars

=============================================================================
