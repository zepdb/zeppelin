------------------------ MODULE NamespaceBranchingGc -------------------------
\* Branch-root retention, two-pass source GC, target-local pending deletes, and
\* the durable reader-grace protocol.
\*
\* Protocol:
\* 1. Reserve a fresh child under a visible parent and pin the exact live
\*    predecessor generation before publishing child visibility.
\* 2. Source history pruning retains current branch pins and named snapshots.
\* 3. GC marks only unreachable objects, records the complete root observation,
\*    revalidates that observation, ages the mark, and checks current reachability
\*    again before sweeping.
\* 4. Target pending-delete GC accepts and deletes only target-origin keys.
\* 5. A read admitted while the branch is visible has a bounded remaining
\*    lifetime. Dropping visibility persists a release deadline; root removal is
\*    gated by time, not by consulting the in-flight-reader map.
\* 6. Source deletion is permitted only after all child roots are gone. Nested
\*    children therefore block deletion of their direct parent.
\*
\* Rust seam map:
\* - ReserveBranch, CreateBranchRoot, PublishBranchVisibility:
\*   future src/branching/NamespaceGraph fork and src/wal/manifest.rs root CAS.
\* - AdvanceSource: src/wal/manifest.rs history PUT + live CAS abstraction; the
\*   lifecycle model checks those remote writes separately.
\* - PruneHistory, MarkUnreachable, RevalidateRoots, SweepSourceObject:
\*   src/wal/manifest.rs retention and src/compaction/gc.rs mark/revalidate/sweep.
\* - StartTargetRead, FinishTargetRead: src/server/handlers/query.rs admission.
\* - DropBranchVisibility, RemoveBranchRoot, AdvancePastReaderGrace,
\*   DeleteSourceNamespace: NamespaceGraph delete/maintenance.
\* - MaterializeBranch: src/compaction/mod.rs target-owned full rewrite.
\* - AppendTargetPendingDelete, DrainTargetPendingDelete:
\*   src/compaction/mod.rs and src/compaction/gc.rs pending-delete drain.
\* - BuggyIgnoreBranchPin, BuggyDeleteForeignPendingKey: negative-only actions.

EXTENDS Naturals, FiniteSets

CONSTANTS
    Incarnations,
    ScenarioModes,
    Generations,
    ObjectKeys,
    MaxGeneration,
    ReaderGrace,
    AllowPublishWithoutRoot,
    AllowRootRemovalBeforeReaderGrace,
    AllowIgnoreBranchPin,
    AllowDeleteForeignPendingKey

NoParent == "NoParent"

AllowedEdge(parent, child) ==
    \/ /\ parent = "root"
       /\ child = "branch"
    \/ /\ parent = "root"
       /\ child = "child"
    \/ /\ parent = "branch"
       /\ child = "child"

LocalObject(incarnation, generation) ==
    CASE incarnation = "root" -> IF generation = 0 THEN "r0"
                                 ELSE IF generation = 1 THEN "r1" ELSE "r2"
      [] incarnation = "branch" -> IF generation = 0 THEN "b0"
                                   ELSE IF generation = 1 THEN "b1" ELSE "b2"
      [] OTHER -> IF generation = 0 THEN "c0"
                  ELSE IF generation = 1 THEN "c1" ELSE "c2"

AllowedEdgeForScenario(mode, parent, child) ==
    CASE mode = "nested" ->
            (parent = "root" /\ child = "branch") \/
            (parent = "branch" /\ child = "child")
      [] mode = "siblings" ->
            parent = "root" /\ child \in {"branch", "child"}
      [] OTHER -> parent = "root" /\ child = "branch"

ObjectOrigin(object) ==
    IF object \in {"r0", "r1", "r2"} THEN "root"
    ELSE IF object \in {"b0", "b1", "b2"} THEN "branch"
    ELSE "child"

VARIABLES gc, now

vars == <<gc, now>>

HasParent(child) == gc.parentByChild[child] # NoParent
Parent(child) == gc.parentByChild[child]

AllExisting == UNION {gc.objectsByOrigin[n] : n \in Incarnations}

HistoryClosure(incarnation, generations) ==
    UNION {gc.generationRefsByIncarnation[incarnation][g] : g \in generations}

VisibleRefs ==
    UNION {IF gc.namespaceVisible[n] THEN gc.liveRefsByIncarnation[n] ELSE {} :
           n \in Incarnations}

RetainedHistoryRefs ==
    UNION {gc.historyRefsByIncarnation[n] : n \in Incarnations}

NamedPinRefs ==
    UNION {HistoryClosure(n, gc.namedSnapshotPins[n]) : n \in Incarnations}

BranchPinRefs ==
    UNION {
        UNION {
            gc.generationRefsByIncarnation[parent][gc.rootGenerationByChild[child]] :
            child \in gc.branchRootPinsByParent[parent]
        } : parent \in Incarnations
    }

PendingRefs == UNION {gc.pendingDeletesByIncarnation[n] : n \in Incarnations}

GraceRefs ==
    UNION {IF gc.droppedByChild[n] /\
              now < gc.rootReleaseNotBeforeByChild[n]
           THEN gc.liveRefsByIncarnation[n] ELSE {} : n \in Incarnations}

Reachable == VisibleRefs \cup RetainedHistoryRefs \cup NamedPinRefs
             \cup BranchPinRefs \cup PendingRefs \cup GraceRefs

RootObservationStable ==
    /\ gc.rootsObservedAtMark = gc.branchRootPinsByParent
    /\ gc.markObservationVersion = gc.gcInventoryVersion

Init ==
    \E mode \in ScenarioModes :
    /\ gc = [
        scenario |-> mode,
        liveGenerationByIncarnation |-> [n \in Incarnations |-> 0],
        retainedGenerationsByIncarnation |-> [n \in Incarnations |-> {}],
        namedSnapshotPins |-> [n \in Incarnations |-> {}],
        branchRootPinsByParent |-> [n \in Incarnations |-> {}],
        parentByChild |-> [n \in Incarnations |-> NoParent],
        rootGenerationByChild |-> [n \in Incarnations |-> 0],
        rootPublicationHeadByChild |-> [n \in Incarnations |-> 0],
        rootEverPublishedByChild |-> [n \in Incarnations |-> FALSE],
        rootRemovedByChild |-> [n \in Incarnations |-> FALSE],
        generationRefsByIncarnation |->
            [n \in Incarnations |->
                [g \in Generations |->
                    IF n = "root" /\ g = 0 THEN {"r0"} ELSE {}]],
        objectsByOrigin |->
            [n \in Incarnations |-> IF n = "root" THEN {"r0"} ELSE {}],
        namespaceVisible |-> [n \in Incarnations |-> n = "root"],
        sourceDeletedByIncarnation |-> [n \in Incarnations |-> FALSE],
        liveRefsByIncarnation |->
            [n \in Incarnations |-> IF n = "root" THEN {"r0"} ELSE {}],
        historyRefsByIncarnation |-> [n \in Incarnations |-> {}],
        pendingDeletesByIncarnation |-> [n \in Incarnations |-> {}],
        pendingCreatedByIncarnation |-> [n \in Incarnations |-> FALSE],
        deletedByTarget |-> [n \in Incarnations |-> {}],
        sweptObjects |-> {},
        markSet |-> {},
        markAge |-> [o \in ObjectKeys |-> 0],
        rootsObservedAtMark |-> [n \in Incarnations |-> {}],
        markObservationVersion |-> 0,
        markValidatedVersion |-> 0,
        markValidated |-> FALSE,
        unsafeSweep |-> FALSE,
        gcInventoryVersion |-> 0,
        requestInFlight |-> [n \in Incarnations |-> 0],
        readRefsByChild |-> [n \in Incarnations |-> {}],
        readStartedByChild |-> [n \in Incarnations |-> FALSE],
        visibilityRemovedAtByChild |-> [n \in Incarnations |-> 0],
        rootReleaseNotBeforeByChild |-> [n \in Incarnations |-> 0],
        droppedByChild |-> [n \in Incarnations |-> FALSE],
        materializedByIncarnation |-> [n \in Incarnations |-> FALSE],
        advancedByIncarnation |-> [n \in Incarnations |-> FALSE],
        branchReservedByChild |-> [n \in Incarnations |-> FALSE]
        ]
    /\ now = 0

ReserveBranch ==
    \E parent, child \in Incarnations :
        /\ AllowedEdgeForScenario(gc.scenario, parent, child)
        /\ gc.namespaceVisible[parent]
        /\ ~gc.sourceDeletedByIncarnation[parent]
        /\ ~gc.branchReservedByChild[child]
        /\ ~gc.namespaceVisible[child]
        /\ ~gc.droppedByChild[child]
        /\ gc' = [gc EXCEPT
            !.parentByChild[child] = parent,
            !.rootGenerationByChild[child] = gc.liveGenerationByIncarnation[parent],
            !.branchReservedByChild[child] = TRUE]
        /\ UNCHANGED now

\* Root creation is head-only. If the provisional reservation generation is no
\* longer live, this action is disabled and the lifecycle orchestrator must
\* rebuild against the fresh head rather than root retained history.
CreateBranchRoot ==
    \E child \in Incarnations :
        /\ gc.branchReservedByChild[child]
        /\ HasParent(child)
        /\ LET parent == Parent(child)
               generation == gc.rootGenerationByChild[child]
           IN /\ gc.namespaceVisible[parent]
              /\ ~gc.sourceDeletedByIncarnation[parent]
              /\ child \notin gc.branchRootPinsByParent[parent]
              /\ generation = gc.liveGenerationByIncarnation[parent]
              /\ gc.gcInventoryVersion < 10
              /\ gc' = [gc EXCEPT
                  !.branchRootPinsByParent[parent] = @ \cup {child},
                  !.retainedGenerationsByIncarnation[parent] = @ \cup {generation},
                  !.historyRefsByIncarnation[parent] =
                      HistoryClosure(parent,
                          gc.retainedGenerationsByIncarnation[parent] \cup {generation}),
                  !.rootPublicationHeadByChild[child] =
                      gc.liveGenerationByIncarnation[parent],
                  !.rootEverPublishedByChild[child] = TRUE,
                  !.gcInventoryVersion = @ + 1,
                  !.markValidated = FALSE]
        /\ UNCHANGED now

PublishBranchVisibility ==
    \E child \in Incarnations :
        /\ gc.branchReservedByChild[child]
        /\ HasParent(child)
        /\ ~gc.namespaceVisible[child]
        /\ ~gc.droppedByChild[child]
        /\ LET parent == Parent(child)
           IN /\ (child \in gc.branchRootPinsByParent[parent] \/
                   AllowPublishWithoutRoot)
              /\ gc' = [gc EXCEPT
                  !.namespaceVisible[child] = TRUE,
                  !.liveGenerationByIncarnation[child] = 0,
                  !.liveRefsByIncarnation[child] =
                      gc.generationRefsByIncarnation[parent]
                        [gc.rootGenerationByChild[child]],
                  !.generationRefsByIncarnation[child][0] =
                      gc.generationRefsByIncarnation[parent]
                        [gc.rootGenerationByChild[child]]]
        /\ UNCHANGED now

AdvanceSource ==
    \E incarnation \in Incarnations :
        /\ gc.scenario = "gc"
        /\ incarnation = "root"
        /\ gc.namespaceVisible[incarnation]
        /\ ~gc.sourceDeletedByIncarnation[incarnation]
        /\ ~gc.advancedByIncarnation[incarnation]
        /\ gc.liveGenerationByIncarnation[incarnation] < MaxGeneration
        /\ LET old == gc.liveGenerationByIncarnation[incarnation]
               new == old + 1
               object == LocalObject(incarnation, new)
               newRefs == gc.liveRefsByIncarnation[incarnation] \cup {object}
           IN /\ object \notin PendingRefs
              /\ object \notin gc.sweptObjects
              /\ object \notin UNION {gc.deletedByTarget[n] : n \in Incarnations}
              /\ gc' = [gc EXCEPT
                !.retainedGenerationsByIncarnation[incarnation] = @ \cup {old},
                !.historyRefsByIncarnation[incarnation] = @ \cup
                    gc.generationRefsByIncarnation[incarnation][old],
                !.liveGenerationByIncarnation[incarnation] = new,
                !.generationRefsByIncarnation[incarnation][new] = newRefs,
                !.liveRefsByIncarnation[incarnation] = newRefs,
                !.objectsByOrigin[incarnation] = @ \cup {object},
                !.advancedByIncarnation[incarnation] = TRUE]
        /\ UNCHANGED now

PruneHistory ==
    /\ gc.scenario \in {"gc", "reader"}
    /\ \E incarnation \in Incarnations :
      \E generation \in gc.retainedGenerationsByIncarnation[incarnation] :
        /\ generation # gc.liveGenerationByIncarnation[incarnation]
        /\ generation \notin gc.namedSnapshotPins[incarnation]
        /\ \A child \in gc.branchRootPinsByParent[incarnation] :
            gc.rootGenerationByChild[child] # generation
        /\ LET retained ==
               gc.retainedGenerationsByIncarnation[incarnation] \ {generation}
           IN gc' = [gc EXCEPT
                !.retainedGenerationsByIncarnation[incarnation] = retained,
                !.historyRefsByIncarnation[incarnation] =
                    HistoryClosure(incarnation, retained)]
        /\ UNCHANGED now

MarkUnreachable ==
    /\ gc.scenario \in {"gc", "reader"}
    /\ \E object \in AllExisting \ Reachable :
        /\ gc.markSet = {}
        /\ gc' = [gc EXCEPT
            !.markSet = {object},
            !.markAge[object] = 0,
            !.rootsObservedAtMark = gc.branchRootPinsByParent,
            !.markObservationVersion = gc.gcInventoryVersion,
            !.markValidated = FALSE]
        /\ UNCHANGED now

RevalidateRoots ==
    /\ gc.scenario \in {"gc", "reader"}
    /\ gc.markSet # {}
    /\ RootObservationStable
    /\ gc' = [gc EXCEPT
        !.markValidated = TRUE,
        !.markValidatedVersion = gc.gcInventoryVersion]
    /\ UNCHANGED now

SweepSourceObject ==
    /\ gc.scenario \in {"gc", "reader"}
    /\ \E object \in gc.markSet :
        /\ gc.markValidated
        /\ gc.markValidatedVersion = gc.gcInventoryVersion
        /\ RootObservationStable
        /\ gc.markAge[object] >= 1
        /\ object \notin Reachable
        /\ LET origin == ObjectOrigin(object)
           IN gc' = [gc EXCEPT
                !.objectsByOrigin[origin] = @ \ {object},
                !.sweptObjects = @ \cup {object},
                !.markSet = @ \ {object},
                !.markValidated = FALSE,
                !.unsafeSweep = @ \/ ~RootObservationStable]
        /\ UNCHANGED now

StartTargetRead ==
    /\ gc.scenario = "reader"
    /\ \E child \in Incarnations :
        /\ HasParent(child)
        /\ gc.namespaceVisible[child]
        /\ ~gc.readStartedByChild[child]
        /\ gc.requestInFlight[child] = 0
        /\ gc' = [gc EXCEPT
            !.requestInFlight[child] = ReaderGrace,
            !.readRefsByChild[child] = gc.liveRefsByIncarnation[child],
            !.readStartedByChild[child] = TRUE]
        /\ UNCHANGED now

FinishTargetRead ==
    /\ gc.scenario = "reader"
    /\ \E child \in Incarnations :
        /\ gc.requestInFlight[child] > 0
        /\ gc' = [gc EXCEPT !.requestInFlight[child] = 0]
        /\ UNCHANGED now

MaterializeBranch ==
    /\ gc.scenario = "gc"
    /\ \E child \in Incarnations :
        /\ child = "branch"
        /\ HasParent(child)
        /\ gc.namespaceVisible[child]
        /\ ~gc.materializedByIncarnation[child]
        /\ gc.liveGenerationByIncarnation[child] = 0
        /\ LET object == LocalObject(child, 1)
               oldRefs == gc.liveRefsByIncarnation[child]
           IN gc' = [gc EXCEPT
                !.retainedGenerationsByIncarnation[child] = @ \cup {0},
                !.historyRefsByIncarnation[child] = @ \cup oldRefs,
                !.liveGenerationByIncarnation[child] = 1,
                !.generationRefsByIncarnation[child][1] = {object},
                !.liveRefsByIncarnation[child] = {object},
                !.objectsByOrigin[child] = @ \cup {object},
                !.materializedByIncarnation[child] = TRUE]
        /\ UNCHANGED now

AppendTargetPendingDelete ==
    /\ gc.scenario = "gc"
    /\ \E child \in Incarnations :
        /\ child = "branch"
        /\ HasParent(child)
        /\ gc.namespaceVisible[child]
        /\ ~gc.pendingCreatedByIncarnation[child]
        /\ LET object == LocalObject(child, 2)
           IN /\ object \notin Reachable
              /\ object \notin gc.liveRefsByIncarnation[child]
              /\ object \notin gc.historyRefsByIncarnation[child]
              /\ gc' = [gc EXCEPT
                !.objectsByOrigin[child] = @ \cup {object},
                !.pendingDeletesByIncarnation[child] = @ \cup {object},
                !.pendingCreatedByIncarnation[child] = TRUE]
        /\ UNCHANGED now

DrainTargetPendingDelete ==
    /\ gc.scenario = "gc"
    /\ \E child \in Incarnations :
      \E object \in gc.pendingDeletesByIncarnation[child] :
        /\ ObjectOrigin(object) = child
        /\ gc' = [gc EXCEPT
            !.pendingDeletesByIncarnation[child] = @ \ {object},
            !.objectsByOrigin[child] = @ \ {object},
            !.deletedByTarget[child] = @ \cup {object}]
        /\ UNCHANGED now

DropBranchVisibility ==
    /\ gc.scenario \in {"reader", "siblings"}
    /\ \E child \in Incarnations :
        /\ child = "branch"
        /\ HasParent(child)
        /\ gc.namespaceVisible[child]
        /\ gc.branchRootPinsByParent[child] = {}
        /\ now <= 3 - ReaderGrace
        /\ gc' = [gc EXCEPT
            !.namespaceVisible[child] = FALSE,
            !.droppedByChild[child] = TRUE,
            !.visibilityRemovedAtByChild[child] = now + 1,
            !.rootReleaseNotBeforeByChild[child] = now + ReaderGrace]
        /\ UNCHANGED now

RemoveBranchRoot ==
    /\ gc.scenario \in {"reader", "siblings"}
    /\ \E child \in Incarnations :
        /\ HasParent(child)
        /\ gc.droppedByChild[child]
        /\ ~gc.namespaceVisible[child]
        /\ LET parent == Parent(child)
           IN /\ child \in gc.branchRootPinsByParent[parent]
              /\ (now >= gc.rootReleaseNotBeforeByChild[child] \/
                  AllowRootRemovalBeforeReaderGrace)
              /\ gc.gcInventoryVersion < 10
              /\ gc' = [gc EXCEPT
                  !.branchRootPinsByParent[parent] = @ \ {child},
                  !.rootRemovedByChild[child] = TRUE,
                  !.gcInventoryVersion = @ + 1,
                  !.markValidated = FALSE]
        /\ UNCHANGED now

DeleteSourceNamespace ==
    /\ gc.scenario = "reader"
    /\ \E incarnation \in Incarnations :
        /\ ~HasParent(incarnation)
        /\ gc.namespaceVisible[incarnation]
        /\ gc.branchRootPinsByParent[incarnation] = {}
        /\ gc' = [gc EXCEPT
            !.namespaceVisible[incarnation] = FALSE,
            !.sourceDeletedByIncarnation[incarnation] = TRUE]
        /\ UNCHANGED now

AdvancePastReaderGrace ==
    /\ gc.scenario \in {"reader", "gc", "siblings"}
    /\ now < 3
    /\ \/ \E child \in Incarnations :
            gc.rootReleaseNotBeforeByChild[child] > now
       \/ \E object \in gc.markSet : gc.markAge[object] < 1
    /\ now' = now + 1
    /\ gc' = [gc EXCEPT
        !.requestInFlight =
            [child \in Incarnations |->
                IF gc.requestInFlight[child] = 0
                THEN 0 ELSE gc.requestInFlight[child] - 1],
        !.markAge =
            [object \in ObjectKeys |->
                IF object \in gc.markSet
                THEN gc.markAge[object] + 1 ELSE gc.markAge[object]]]

BuggyIgnoreBranchPin ==
    /\ AllowIgnoreBranchPin
    /\ \E parent \in Incarnations :
      \E child \in gc.branchRootPinsByParent[parent] :
        LET generation == gc.rootGenerationByChild[child]
            retained == gc.retainedGenerationsByIncarnation[parent] \ {generation}
        IN gc' = [gc EXCEPT
            !.retainedGenerationsByIncarnation[parent] = retained,
            !.historyRefsByIncarnation[parent] = HistoryClosure(parent, retained)]
    /\ UNCHANGED now

BuggyDeleteForeignPendingKey ==
    /\ AllowDeleteForeignPendingKey
    /\ \E child \in Incarnations :
      \E object \in gc.liveRefsByIncarnation[child] :
        /\ HasParent(child)
        /\ gc.namespaceVisible[child]
        /\ ObjectOrigin(object) # child
        /\ LET origin == ObjectOrigin(object)
           IN gc' = [gc EXCEPT
                !.objectsByOrigin[origin] = @ \ {object},
                !.deletedByTarget[child] = @ \cup {object}]
    /\ UNCHANGED now

Stutter == UNCHANGED vars

Next ==
    \/ ReserveBranch
    \/ CreateBranchRoot
    \/ PublishBranchVisibility
    \/ AdvanceSource
    \/ PruneHistory
    \/ MarkUnreachable
    \/ RevalidateRoots
    \/ SweepSourceObject
    \/ StartTargetRead
    \/ FinishTargetRead
    \/ MaterializeBranch
    \/ AppendTargetPendingDelete
    \/ DrainTargetPendingDelete
    \/ DropBranchVisibility
    \/ RemoveBranchRoot
    \/ DeleteSourceNamespace
    \/ AdvancePastReaderGrace
    \/ BuggyIgnoreBranchPin
    \/ BuggyDeleteForeignPendingKey
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Incarnations = {"root", "branch", "child"}
    /\ ScenarioModes = {"nested", "siblings", "reader", "gc"}
    /\ gc.scenario \in ScenarioModes
    /\ Generations = 0..MaxGeneration
    /\ ObjectKeys = {"r0", "r1", "b1", "b2"}
    /\ ReaderGrace = 2
    /\ gc.liveGenerationByIncarnation \in [Incarnations -> Generations]
    /\ gc.retainedGenerationsByIncarnation \in [Incarnations -> SUBSET Generations]
    /\ gc.namedSnapshotPins \in [Incarnations -> SUBSET Generations]
    /\ gc.branchRootPinsByParent \in [Incarnations -> SUBSET Incarnations]
    /\ gc.parentByChild \in [Incarnations -> Incarnations \cup {NoParent}]
    /\ gc.rootGenerationByChild \in [Incarnations -> Generations]
    /\ gc.rootPublicationHeadByChild \in [Incarnations -> Generations]
    /\ gc.rootEverPublishedByChild \in [Incarnations -> BOOLEAN]
    /\ gc.rootRemovedByChild \in [Incarnations -> BOOLEAN]
    /\ gc.generationRefsByIncarnation \in
        [Incarnations -> [Generations -> SUBSET ObjectKeys]]
    /\ gc.objectsByOrigin \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.namespaceVisible \in [Incarnations -> BOOLEAN]
    /\ gc.sourceDeletedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ gc.liveRefsByIncarnation \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.historyRefsByIncarnation \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.pendingDeletesByIncarnation \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.pendingCreatedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ gc.deletedByTarget \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.sweptObjects \subseteq ObjectKeys
    /\ gc.markSet \subseteq ObjectKeys
    /\ gc.markAge \in [ObjectKeys -> 0..3]
    /\ gc.rootsObservedAtMark \in [Incarnations -> SUBSET Incarnations]
    /\ gc.markObservationVersion \in 0..10
    /\ gc.markValidatedVersion \in 0..10
    /\ gc.markValidated \in BOOLEAN
    /\ gc.unsafeSweep \in BOOLEAN
    /\ gc.gcInventoryVersion \in 0..10
    /\ gc.requestInFlight \in [Incarnations -> 0..ReaderGrace]
    /\ gc.readRefsByChild \in [Incarnations -> SUBSET ObjectKeys]
    /\ gc.readStartedByChild \in [Incarnations -> BOOLEAN]
    /\ gc.visibilityRemovedAtByChild \in [Incarnations -> 0..3]
    /\ gc.rootReleaseNotBeforeByChild \in [Incarnations -> 0..3]
    /\ gc.droppedByChild \in [Incarnations -> BOOLEAN]
    /\ gc.materializedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ gc.advancedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ gc.branchReservedByChild \in [Incarnations -> BOOLEAN]
    /\ now \in 0..3

BranchPinnedGenerationRetained ==
    \A parent \in Incarnations :
      \A child \in gc.branchRootPinsByParent[parent] :
        gc.rootGenerationByChild[child] \in
            gc.retainedGenerationsByIncarnation[parent]

RootCreatedFromLiveHead ==
    \A child \in Incarnations :
        gc.rootEverPublishedByChild[child] =>
            gc.rootGenerationByChild[child] = gc.rootPublicationHeadByChild[child]

VisibleBranchRefsExist ==
    \A child \in Incarnations :
        (HasParent(child) /\ gc.namespaceVisible[child]) =>
            /\ child \in gc.branchRootPinsByParent[Parent(child)]
            /\ gc.liveRefsByIncarnation[child] \subseteq AllExisting

TargetGcDeletesOnlyTargetOwnedKeys ==
    \A child \in Incarnations :
      \A object \in gc.deletedByTarget[child] : ObjectOrigin(object) = child

ForeignRefsNeverEnterTargetPendingDeletes ==
    \A child \in Incarnations :
      \A object \in gc.pendingDeletesByIncarnation[child] :
        ObjectOrigin(object) = child

TwoPassSweepRequiresStableRootObservation == ~gc.unsafeSweep

InFlightReadProtectedByHorizon ==
    \A child \in Incarnations :
        gc.requestInFlight[child] > 0 =>
            gc.readRefsByChild[child] \subseteq AllExisting

RootRetainedThroughReaderGrace ==
    \A child \in Incarnations :
        (gc.droppedByChild[child] /\
         now < gc.rootReleaseNotBeforeByChild[child]) =>
            child \in gc.branchRootPinsByParent[Parent(child)]

DroppedBranchCannotResurrect ==
    \A child \in Incarnations :
        gc.droppedByChild[child] => ~gc.namespaceVisible[child]

RemovingOneRootDoesNotReleaseAnotherBranch ==
    \A child \in Incarnations :
        (gc.namespaceVisible[child] /\ HasParent(child)) =>
            child \in gc.branchRootPinsByParent[Parent(child)]

====
