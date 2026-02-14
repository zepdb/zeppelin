---------------------- MODULE ResourceBound ----------------------
EXTENDS Integers, FiniteSets, TLC

CONSTANTS 
    MaxNamespaces,
    MaxLocks

VARIABLES 
    active_namespaces, \* Set of active namespace IDs
    lock_map           \* Set of namespace IDs that have an entry in the lock map

Vars == <<active_namespaces, lock_map>>

Init ==
    /\ active_namespaces = {}
    /\ lock_map = {}

\* User creates a namespace
CreateNS(n) ==
    /\ Cardinality(active_namespaces) < MaxNamespaces
    /\ active_namespaces' = active_namespaces \cup {n}
    /\ UNCHANGED lock_map

\* User writes to namespace (creates lock entry)
Write(n) ==
    /\ n \in active_namespaces
    /\ lock_map' = lock_map \cup {n}
    /\ UNCHANGED active_namespaces

\* User deletes a namespace
DeleteNS(n) ==
    /\ n \in active_namespaces
    /\ active_namespaces' = active_namespaces \ {n}
    \* Crucially: The code DOES NOT remove the lock from the map
    /\ UNCHANGED lock_map

Next ==
    \/ \E n \in 1..(MaxNamespaces*2) : CreateNS(n)
    \/ \E n \in 1..(MaxNamespaces*2) : Write(n)
    \/ \E n \in 1..(MaxNamespaces*2) : DeleteNS(n)

\* Invariant: Lock map size should be proportional to active namespaces.
\* If we delete everything, lock map should be empty (or small).
\* Bug: Lock map grows monotonically.
BoundedLocks ==
    Cardinality(lock_map) <= Cardinality(active_namespaces) + 5


Spec == Init /\ [][Next]_Vars

=============================================================================
