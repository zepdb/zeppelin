------------------------- MODULE NamespaceBranching --------------------------
\* Namespace branching lifecycle, manifest publication, deletion, and recovery.
\*
\* Protocol:
\* 1. Reserve a fresh target incarnation in Creating.
\* 2. Acquire the source writer lease and select either the current head or an
\*    already-retained historical generation, including its exact digest.
\* 3. Write the pre-CAS current source head to immutable history.
\* 4. CAS the source live manifest to its next generation and add the root. The
\*    root pins the selected identity; the history write preserves the distinct
\*    pre-CAS current head even when the selected generation is historical.
\* 5. Write target generation-one history, publish target live generation one,
\*    install the policy guard, and activate only after every subsystem is safe.
\* 6. Source deletion fences only a root-free head.
\* 7. Target deletion publishes evidence, removes visibility, records a durable
\*    reader deadline, waits through it, removes the exact root, and then deletes
\*    only target-owned state.
\* 8. Crashes preserve durable state. Recovery resumes by identity; it never
\*    guesses whether a root or manifest was published.
\*
\* Rust seam map (future branching names denote the phase-05 deep module):
\* - ReserveTarget, RetryFork, CancelPreparedTarget, RecoverCreatingTarget:
\*   src/branching/NamespaceGraph + src/namespace/manager.rs metadata CAS.
\* - AcquireSourceLease, UpdateIndexConfig, SourceWriterWriteHistory,
\*   SourceWriterCAS, WriteSourceHeadHistory, PublishSourceRootCAS:
\*   src/wal/lease.rs and src/wal/manifest.rs history/write_conditional.
\* - WriteTargetHistoryOne, PublishTargetManifest:
\*   src/wal/manifest.rs create-only history/live publication.
\* - AcquirePolicyLease, PolicyWriterCAS, RevalidateActivationPolicy,
\*   InstallPolicyActivationGuardCAS, ExpirePolicyLease,
\*   ResolveExpiredActivationGuard:
\*   src/security/policy.rs fenced policy-head and activation-guard CAS.
\* - InstallBranchSafeSubsystems, ActivateTarget:
\*   phase-08 server adapter over NamespaceGraph activation.
\* - MaterializeBranch: src/compaction/mod.rs full target-owned rewrite.
\* - PrepareDeleteIntent, FenceSourceDeleteCAS, FenceTargetDelete,
\*   PublishDestructionEvidence, StrongPreservationCheck,
\*   PublishVisibilityRemovedMarker, RemoveTargetVisibility,
\*   RemoveParentRootCAS, DeleteTargetObjects, DeleteTargetMetadata,
\*   RecoverDeletingTarget: NamespaceGraph delete + namespace manager + GC.
\* - PreservationLockCAS: src/security/preservation.rs authoritative lock CAS.
\* - LoseForkResponse and Crash: response loss/process failure boundaries.

EXTENDS Naturals, Integers, FiniteSets

CONSTANTS
    Namespaces,
    Incarnations,
    BranchIds,
    ScenarioModes,
    SourceGenerations,
    ConfigValues,
    PolicyVersions,
    MaxGeneration,
    MaxPolicyVersion,
    MaxRoots,
    MaxDepth,
    AllowPublishWithoutRoot,
    AllowDeleteWithRoots,
    AllowRootRemovalBeforeVisibilityGone,
    AllowRootRemovalBeforeReaderGrace,
    AllowActivateBeforeSubsystems,
    AllowVisibilityRemovalWithoutEvidence,
    AllowActivationWithoutPolicyGuard,
    AllowPolicyWritePastActivationGuard,
    AllowGuardRemovalBeforeNonceRevocation,
    AllowDestructionWithStalePreservation

NoParent == "NoParent"
NoBranch == "NoBranch"
NoOwner == "NoOwner"
NoConfig == "NoConfig"
NoVersion == -1

NamespaceStates == {"Absent", "Creating", "Active", "Deleting", "Deleted"}
IntentStates == {"None", "Reserved", "Rooted", "ManifestPublished", "Active"}
DeletionStates ==
    {"None", "Prepared", "Fenced", "Evidence", "VisibilityRemoved",
     "RootRemoved", "ObjectsDeleted", "MetadataDeleted"}
ResultStates == {"None", "Success", "Error"}
AllSubsystems ==
    {"physical-read", "write-compaction", "graph-delete", "authorization",
     "preservation", "audit"}

\* The cfg deliberately binds a concrete three-node chain. Maps remain keyed by
\* incarnation, while the restricted edge relation keeps TLC finite.
AllowedEdge(parent, child) ==
    \/ /\ parent = "root"
       /\ child = "branch"
    \/ /\ parent = "branch"
       /\ child = "child"

BranchIdFor(child) == IF child = "branch" THEN "b1" ELSE "b2"
Digest(generation) == generation

AllowedEdgeForScenario(mode, parent, child) ==
    IF mode = "nested"
    THEN AllowedEdge(parent, child)
    ELSE parent = "root" /\ child = "branch"

VARIABLES graph, policy, preservation, now, crashed

vars == <<graph, policy, preservation, now, crashed>>

HasParent(child) == graph.parentByChild[child] # NoParent
Parent(child) == graph.parentByChild[child]

RootMatches(child) ==
    /\ HasParent(child)
    /\ child \in graph.rootsByParent[Parent(child)]
    /\ graph.rootGenerationByChild[child] = graph.baseGenerationByChild[child]
    /\ graph.rootDigestByChild[child] = graph.baseDigestByChild[child]

DestructionAuthorized(incarnation) ==
    /\ preservation.proofVersionByIncarnation[incarnation] = preservation.version
    /\ ~preservation.lockedByIncarnation[incarnation]

DepthOK(child) ==
    IF ~HasParent(child)
    THEN TRUE
    ELSE LET parent == Parent(child)
         IN /\ parent # child
            /\ IF ~HasParent(parent)
                  THEN TRUE
                  ELSE LET grandparent == Parent(parent)
                       IN /\ grandparent # child
                          /\ grandparent # parent
                          /\ ~HasParent(grandparent)

Init ==
    \E mode \in ScenarioModes :
    /\ graph = [
        scenario |-> mode,
        generationByIncarnation |-> [n \in Incarnations |-> 0],
        historyByIncarnation |-> [n \in Incarnations |-> {}],
        rootsByParent |-> [n \in Incarnations |-> {}],
        parentByChild |-> [n \in Incarnations |-> NoParent],
        branchIdByChild |-> [n \in Incarnations |-> NoBranch],
        rootGenerationByChild |-> [n \in Incarnations |-> 0],
        rootDigestByChild |-> [n \in Incarnations |-> Digest(0)],
        rootPreCasHeadByChild |-> [n \in Incarnations |-> 0],
        rootPreCasDigestByChild |-> [n \in Incarnations |-> Digest(0)],
        rootPublicationGenerationByChild |-> [n \in Incarnations |-> 0],
        rootWasPublishedByChild |-> [n \in Incarnations |-> FALSE],
        rootRemovedByChild |-> [n \in Incarnations |-> FALSE],
        fenceByIncarnation |-> [n \in Incarnations |-> FALSE],
        leaseTokenByIncarnation |-> [n \in Incarnations |-> 0],
        sourceLeaseOwnerByParent |-> [n \in Incarnations |-> NoOwner],
        etagByIncarnation |-> [n \in Incarnations |-> 0],
        rootHistoryPreparedByChild |-> [n \in Incarnations |-> FALSE],
        rootPreCasEtagByChild |-> [n \in Incarnations |-> 0],
        writerHistoryPreparedByIncarnation |-> [n \in Incarnations |-> FALSE],
        writerPreCasHeadByIncarnation |-> [n \in Incarnations |-> 0],
        writerPreCasEtagByIncarnation |-> [n \in Incarnations |-> 0],
        writerAdvancedByIncarnation |-> [n \in Incarnations |-> FALSE],
        configUpdatedByIncarnation |-> [n \in Incarnations |-> FALSE],
        stateByIncarnation |-> [n \in Incarnations |-> IF n = "root" THEN "Active" ELSE "Absent"],
        intentByIncarnation |-> [n \in Incarnations |-> IF n = "root" THEN "Active" ELSE "None"],
        dataPlaneConfigByIncarnation |-> [n \in Incarnations |-> "cfg0"],
        reservedConfigByChild |-> [n \in Incarnations |-> NoConfig],
        frozenConfigDigestByChild |-> [n \in Incarnations |-> NoConfig],
        rootConfigDigestByChild |-> [n \in Incarnations |-> NoConfig],
        activationConfigByChild |-> [n \in Incarnations |-> NoConfig],
        manifestVisibleByIncarnation |-> [n \in Incarnations |-> n = "root"],
        targetHistoryWrittenByChild |-> [n \in Incarnations |-> FALSE],
        baseGenerationByChild |-> [n \in Incarnations |-> 0],
        baseDigestByChild |-> [n \in Incarnations |-> Digest(0)],
        foreignRefsByIncarnation |-> [n \in Incarnations |-> {}],
        localRefsByIncarnation |-> [n \in Incarnations |-> n = "root"],
        materializedByIncarnation |-> [n \in Incarnations |-> FALSE],
        forkViewDigestByChild |-> [n \in Incarnations |-> Digest(0)],
        visibilityRemovedAtByChild |-> [n \in Incarnations |-> 0],
        rootReleaseNotBeforeByChild |-> [n \in Incarnations |-> 0],
        visibilityRemovedWithEvidenceByChild |-> [n \in Incarnations |-> TRUE],
        deletionIntentByIncarnation |-> [n \in Incarnations |-> "None"],
        branchSafeSubsystems |-> {},
        forkResultByTarget |-> [n \in Incarnations |-> "None"],
        deleteResultByIncarnation |-> [n \in Incarnations |-> "None"],
        responseLostByTarget |-> [n \in Incarnations |-> FALSE],
        everActiveByIncarnation |-> [n \in Incarnations |-> n = "root"],
        cancelledByIncarnation |-> [n \in Incarnations |-> FALSE],
        crashUsed |-> FALSE
        ]
    /\ policy = [
        version |-> 0,
        leaseOwner |-> NoOwner,
        activationNonceByChild |-> [n \in Incarnations |-> 0],
        guardNonceByChild |-> [n \in Incarnations |-> 0],
        guardVersionByChild |-> [n \in Incarnations |-> NoVersion],
        activationPolicyVersionByChild |-> [n \in Incarnations |-> NoVersion],
        activationGuardUsedByChild |-> [n \in Incarnations |-> FALSE],
        guardRemovalSafeByChild |-> [n \in Incarnations |-> TRUE],
        guardRecoveryUsedByChild |-> [n \in Incarnations |-> FALSE]
        ]
    /\ preservation = [
        version |-> 0,
        lockedByIncarnation |-> [n \in Incarnations |-> FALSE],
        proofVersionByIncarnation |-> [n \in Incarnations |-> NoVersion],
        destructiveBoundaryFreshByIncarnation |-> [n \in Incarnations |-> TRUE]
        ]
    /\ now = 0
    /\ crashed = FALSE

\* NamespaceGraph::fork metadata reservation.
ReserveTarget ==
    \E parent, child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario # "preservation"
        /\ AllowedEdgeForScenario(graph.scenario, parent, child)
        /\ graph.stateByIncarnation[parent] = "Active"
        /\ graph.stateByIncarnation[child] = "Absent"
        /\ Cardinality(graph.rootsByParent[parent]) < MaxRoots
        /\ graph' = [graph EXCEPT
            !.stateByIncarnation[child] = "Creating",
            !.intentByIncarnation[child] = "Reserved",
            !.parentByChild[child] = parent,
            !.branchIdByChild[child] = BranchIdFor(child),
            !.reservedConfigByChild[child] = graph.dataPlaneConfigByIncarnation[parent],
            !.baseGenerationByChild[child] = graph.generationByIncarnation[parent],
            !.baseDigestByChild[child] = Digest(graph.generationByIncarnation[parent])]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Lease acquisition is also the authoritative selected-generation/config read.
AcquireSourceLease ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "Reserved"
        /\ HasParent(child)
        /\ LET parent == Parent(child)
               candidates == graph.historyByIncarnation[parent] \cup
                             {graph.generationByIncarnation[parent]}
           IN \E selected \in candidates :
                /\ graph.stateByIncarnation[parent] = "Active"
                /\ ~graph.fenceByIncarnation[parent]
                /\ graph.sourceLeaseOwnerByParent[parent] = NoOwner
                /\ graph.leaseTokenByIncarnation[parent] < 10
                /\ graph' = [graph EXCEPT
                    !.sourceLeaseOwnerByParent[parent] = child,
                    !.leaseTokenByIncarnation[parent] = @ + 1,
                    !.baseGenerationByChild[child] = selected,
                    !.baseDigestByChild[child] = Digest(selected),
                    !.reservedConfigByChild[child] = graph.dataPlaneConfigByIncarnation[parent]]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Source PATCH takes the same lease domain atomically and then releases it.
UpdateIndexConfig ==
    \E parent \in Incarnations, config \in ConfigValues :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ parent = "root"
        /\ graph.stateByIncarnation[parent] = "Active"
        /\ ~graph.fenceByIncarnation[parent]
        /\ graph.sourceLeaseOwnerByParent[parent] = NoOwner
        /\ ~graph.configUpdatedByIncarnation[parent]
        /\ config # graph.dataPlaneConfigByIncarnation[parent]
        /\ graph.leaseTokenByIncarnation[parent] < 10
        /\ graph' = [graph EXCEPT
            !.dataPlaneConfigByIncarnation[parent] = config,
            !.configUpdatedByIncarnation[parent] = TRUE,
            !.leaseTokenByIncarnation[parent] = @ + 1]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Ordinary manifest writers also split immutable history from live CAS.
SourceWriterWriteHistory ==
    \E parent \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ parent = "root"
        /\ graph.stateByIncarnation[parent] = "Active"
        /\ ~graph.fenceByIncarnation[parent]
        /\ graph.sourceLeaseOwnerByParent[parent] = NoOwner
        /\ ~graph.writerAdvancedByIncarnation[parent]
        /\ ~graph.writerHistoryPreparedByIncarnation[parent]
        /\ graph.generationByIncarnation[parent] < MaxGeneration
        /\ graph' = [graph EXCEPT
            !.historyByIncarnation[parent] = @ \cup {graph.generationByIncarnation[parent]},
            !.writerHistoryPreparedByIncarnation[parent] = TRUE,
            !.writerPreCasHeadByIncarnation[parent] = graph.generationByIncarnation[parent],
            !.writerPreCasEtagByIncarnation[parent] = graph.etagByIncarnation[parent]]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

SourceWriterCAS ==
    \E parent \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ parent = "root"
        /\ graph.writerHistoryPreparedByIncarnation[parent]
        /\ graph.sourceLeaseOwnerByParent[parent] = NoOwner
        /\ graph.generationByIncarnation[parent] = graph.writerPreCasHeadByIncarnation[parent]
        /\ graph.etagByIncarnation[parent] = graph.writerPreCasEtagByIncarnation[parent]
        /\ graph.generationByIncarnation[parent] < MaxGeneration
        /\ graph' = [graph EXCEPT
            !.generationByIncarnation[parent] = @ + 1,
            !.etagByIncarnation[parent] = @ + 1,
            !.writerHistoryPreparedByIncarnation[parent] = FALSE,
            !.writerAdvancedByIncarnation[parent] = TRUE]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Separate S3 PUT of the pre-CAS current head; no live root exists yet.
WriteSourceHeadHistory ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "Reserved"
        /\ HasParent(child)
        /\ LET parent == Parent(child)
           IN /\ graph.sourceLeaseOwnerByParent[parent] = child
              /\ ~graph.rootHistoryPreparedByChild[child]
              /\ graph.generationByIncarnation[parent] < MaxGeneration
              /\ graph' = [graph EXCEPT
                  !.historyByIncarnation[parent] = @ \cup {graph.generationByIncarnation[parent]},
                  !.rootHistoryPreparedByChild[child] = TRUE,
                  !.rootPreCasHeadByChild[child] = graph.generationByIncarnation[parent],
                  !.rootPreCasDigestByChild[child] = Digest(graph.generationByIncarnation[parent]),
                  !.rootPreCasEtagByChild[child] = graph.etagByIncarnation[parent]]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Live manifest CAS: selected root identity and pre-CAS head identity are distinct.
PublishSourceRootCAS ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "Reserved"
        /\ graph.rootHistoryPreparedByChild[child]
        /\ HasParent(child)
        /\ LET parent == Parent(child)
               selected == graph.baseGenerationByChild[child]
               head == graph.rootPreCasHeadByChild[child]
           IN /\ graph.sourceLeaseOwnerByParent[parent] = child
              /\ graph.stateByIncarnation[parent] = "Active"
              /\ ~graph.fenceByIncarnation[parent]
              /\ selected \in graph.historyByIncarnation[parent]
              /\ graph.generationByIncarnation[parent] = head
              /\ graph.etagByIncarnation[parent] = graph.rootPreCasEtagByChild[child]
              /\ head < MaxGeneration
              /\ child \notin graph.rootsByParent[parent]
              /\ Cardinality(graph.rootsByParent[parent]) < MaxRoots
              /\ graph' = [graph EXCEPT
                  !.generationByIncarnation[parent] = @ + 1,
                  !.etagByIncarnation[parent] = @ + 1,
                  !.rootsByParent[parent] = @ \cup {child},
                  !.rootGenerationByChild[child] = selected,
                  !.rootDigestByChild[child] = Digest(selected),
                  !.rootPublicationGenerationByChild[child] = head + 1,
                  !.rootWasPublishedByChild[child] = TRUE,
                  !.rootConfigDigestByChild[child] = graph.dataPlaneConfigByIncarnation[parent],
                  !.frozenConfigDigestByChild[child] = graph.dataPlaneConfigByIncarnation[parent],
                  !.intentByIncarnation[child] = "Rooted",
                  !.sourceLeaseOwnerByParent[parent] = NoOwner]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

\* Target generation-one history PUT precedes target live-manifest create.
WriteTargetHistoryOne ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ \/ graph.intentByIncarnation[child] = "Rooted"
           \/ /\ AllowPublishWithoutRoot
              /\ graph.intentByIncarnation[child] = "Reserved"
        /\ ~graph.targetHistoryWrittenByChild[child]
        /\ graph' = [graph EXCEPT
            !.historyByIncarnation[child] = @ \cup {1},
            !.targetHistoryWrittenByChild[child] = TRUE]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

PublishTargetManifest ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.targetHistoryWrittenByChild[child]
        /\ ~graph.manifestVisibleByIncarnation[child]
        /\ \/ /\ graph.intentByIncarnation[child] = "Rooted"
              /\ RootMatches(child)
           \/ /\ AllowPublishWithoutRoot
              /\ graph.intentByIncarnation[child] = "Reserved"
        /\ LET parent == Parent(child)
           IN graph' = [graph EXCEPT
                !.generationByIncarnation[child] = 1,
                !.manifestVisibleByIncarnation[child] = TRUE,
                !.intentByIncarnation[child] = "ManifestPublished",
                !.dataPlaneConfigByIncarnation[child] = graph.reservedConfigByChild[child],
                !.foreignRefsByIncarnation[child] = {parent} \cup graph.foreignRefsByIncarnation[parent],
                !.forkViewDigestByChild[child] = graph.baseDigestByChild[child]]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

InstallBranchSafeSubsystems ==
    /\ ~crashed
    /\ graph.branchSafeSubsystems # AllSubsystems
    /\ \E child \in Incarnations :
        graph.stateByIncarnation[child] = "Creating"
    /\ graph' = [graph EXCEPT !.branchSafeSubsystems = AllSubsystems]
    /\ UNCHANGED <<policy, preservation, now, crashed>>

AcquirePolicyLease ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "ManifestPublished"
        /\ policy.leaseOwner = NoOwner
        /\ policy.guardNonceByChild[child] = 0
        /\ policy.activationNonceByChild[child] < 4
        /\ policy' = [policy EXCEPT
            !.leaseOwner = child,
            !.activationNonceByChild[child] = @ + 1,
            !.activationPolicyVersionByChild[child] = NoVersion]
        /\ UNCHANGED <<graph, preservation, now, crashed>>

PolicyWriterCAS ==
    /\ ~crashed
    /\ graph.scenario = "policy"
    /\ policy.version < MaxPolicyVersion
    /\ policy.leaseOwner = NoOwner
    /\ \E child \in Incarnations :
        graph.stateByIncarnation[child] = "Creating" /\
        graph.intentByIncarnation[child] = "ManifestPublished"
    /\ \/ (\A child \in Incarnations : policy.guardNonceByChild[child] = 0)
       \/ AllowPolicyWritePastActivationGuard
    /\ policy' = [policy EXCEPT !.version = @ + 1]
    /\ UNCHANGED <<graph, preservation, now, crashed>>

RevalidateActivationPolicy ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ policy.leaseOwner = child
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "ManifestPublished"
        /\ policy' = [policy EXCEPT
            !.activationPolicyVersionByChild[child] = policy.version]
        /\ UNCHANGED <<graph, preservation, now, crashed>>

InstallPolicyActivationGuardCAS ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ policy.leaseOwner = child
        /\ policy.activationPolicyVersionByChild[child] = policy.version
        /\ policy.activationNonceByChild[child] > 0
        /\ policy.guardNonceByChild[child] = 0
        /\ policy' = [policy EXCEPT
            !.guardNonceByChild[child] = policy.activationNonceByChild[child],
            !.guardVersionByChild[child] = policy.version]
        /\ UNCHANGED <<graph, preservation, now, crashed>>

\* Lease expiry leaves the durable guard in place. A policy writer must still
\* respect that guard after it can acquire the now-free lease.
ExpirePolicyLease ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "policy"
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ policy.guardNonceByChild[child] > 0
        /\ policy.leaseOwner = child
        /\ policy' = [policy EXCEPT !.leaseOwner = NoOwner]
        /\ UNCHANGED <<graph, preservation, now, crashed>>

ActivateTarget ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ graph.intentByIncarnation[child] = "ManifestPublished"
        /\ graph.manifestVisibleByIncarnation[child]
        /\ graph.targetHistoryWrittenByChild[child]
        /\ RootMatches(child)
        /\ (graph.branchSafeSubsystems = AllSubsystems \/ AllowActivateBeforeSubsystems)
        /\ policy.leaseOwner = child
        /\ policy.activationPolicyVersionByChild[child] = policy.version
        /\ \/ /\ policy.guardNonceByChild[child] = policy.activationNonceByChild[child]
              /\ policy.guardVersionByChild[child] = policy.version
           \/ AllowActivationWithoutPolicyGuard
        /\ graph' = [graph EXCEPT
            !.stateByIncarnation[child] = "Active",
            !.intentByIncarnation[child] = "Active",
            !.forkResultByTarget[child] = "Success",
            !.activationConfigByChild[child] = graph.dataPlaneConfigByIncarnation[child],
            !.everActiveByIncarnation[child] = TRUE]
        /\ policy' = [policy EXCEPT
            !.activationGuardUsedByChild[child] =
                (policy.guardNonceByChild[child] = policy.activationNonceByChild[child] /\
                 policy.guardVersionByChild[child] = policy.version)]
        /\ UNCHANGED <<preservation, now, crashed>>

\* Active recovery removes the guard after observing matching durable evidence;
\* creating recovery revokes the exact nonce first. The negative toggle exposes
\* the stale-activator bug as an explicit state transition.
ResolveExpiredActivationGuard ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ policy.guardNonceByChild[child] > 0
        /\ IF graph.stateByIncarnation[child] = "Active"
              THEN /\ policy' = [policy EXCEPT
                      !.guardNonceByChild[child] = 0,
                      !.guardVersionByChild[child] = NoVersion,
                      !.leaseOwner = NoOwner]
                   /\ UNCHANGED graph
              ELSE /\ graph.stateByIncarnation[child] = "Creating"
                   /\ graph.scenario = "policy"
                   /\ policy.leaseOwner = NoOwner
                   /\ ~policy.guardRecoveryUsedByChild[child]
                   /\ policy' = [policy EXCEPT
                      !.guardNonceByChild[child] = 0,
                      !.guardVersionByChild[child] = NoVersion,
                      !.leaseOwner = NoOwner,
                      !.activationNonceByChild[child] =
                          IF AllowGuardRemovalBeforeNonceRevocation THEN @ ELSE @ + 1,
                      !.guardRemovalSafeByChild[child] =
                          IF AllowGuardRemovalBeforeNonceRevocation THEN FALSE ELSE @,
                      !.guardRecoveryUsedByChild[child] = TRUE]
                   /\ UNCHANGED graph
        /\ UNCHANGED <<preservation, now, crashed>>

MaterializeBranch ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ child = "branch"
        /\ graph.stateByIncarnation[child] = "Active"
        /\ HasParent(child)
        /\ ~graph.materializedByIncarnation[child]
        /\ graph' = [graph EXCEPT
            !.materializedByIncarnation[child] = TRUE,
            !.localRefsByIncarnation[child] = TRUE]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

LoseForkResponse ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ graph.forkResultByTarget[child] = "Success"
        /\ ~graph.responseLostByTarget[child]
        /\ graph' = [graph EXCEPT
            !.forkResultByTarget[child] = "None",
            !.responseLostByTarget[child] = TRUE]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

RetryFork ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "fork"
        /\ graph.stateByIncarnation[child] = "Active"
        /\ graph.responseLostByTarget[child]
        /\ RootMatches(child)
        /\ graph' = [graph EXCEPT
            !.forkResultByTarget[child] = "Success",
            !.responseLostByTarget[child] = FALSE]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

PrepareDeleteIntent ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario \in {"delete", "preservation", "cancel"}
        /\ IF graph.scenario = "preservation"
              THEN incarnation = "root"
              ELSE TRUE
        /\ graph.deletionIntentByIncarnation[incarnation] = "None"
        /\ graph.stateByIncarnation[incarnation] \in {"Creating", "Active"}
        /\ graph' = [graph EXCEPT
            !.deletionIntentByIncarnation[incarnation] = "Prepared"]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

StrongPreservationCheck ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.deletionIntentByIncarnation[incarnation] # "None"
        /\ ~preservation.lockedByIncarnation[incarnation]
        /\ preservation' = [preservation EXCEPT
            !.proofVersionByIncarnation[incarnation] = preservation.version]
        /\ UNCHANGED <<graph, policy, now, crashed>>

PreservationLockCAS ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "preservation"
        /\ preservation.version < MaxPolicyVersion
        /\ ~preservation.lockedByIncarnation[incarnation]
        /\ graph.deletionIntentByIncarnation[incarnation] # "None"
        /\ preservation' = [preservation EXCEPT
            !.version = @ + 1,
            !.lockedByIncarnation[incarnation] = TRUE]
        /\ UNCHANGED <<graph, policy, now, crashed>>

FenceSourceDeleteCAS ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario \in {"delete", "preservation", "cancel"}
        /\ ~HasParent(incarnation)
        /\ graph.stateByIncarnation[incarnation] = "Active"
        /\ graph.deletionIntentByIncarnation[incarnation] = "Prepared"
        /\ graph.sourceLeaseOwnerByParent[incarnation] = NoOwner
        /\ (graph.rootsByParent[incarnation] = {} \/ AllowDeleteWithRoots)
        /\ (DestructionAuthorized(incarnation) \/ AllowDestructionWithStalePreservation)
        /\ graph' = [graph EXCEPT
            !.fenceByIncarnation[incarnation] = TRUE,
            !.stateByIncarnation[incarnation] = "Deleting",
            !.deletionIntentByIncarnation[incarnation] = "Fenced"]
        /\ preservation' = [preservation EXCEPT
            !.destructiveBoundaryFreshByIncarnation[incarnation] =
                @ /\ DestructionAuthorized(incarnation)]
        /\ UNCHANGED <<policy, now, crashed>>

FenceTargetDelete ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ HasParent(child)
        /\ graph.stateByIncarnation[child] = "Active"
        /\ graph.deletionIntentByIncarnation[child] = "Prepared"
        /\ graph.rootsByParent[child] = {}
        /\ (DestructionAuthorized(child) \/ AllowDestructionWithStalePreservation)
        /\ graph' = [graph EXCEPT
            !.fenceByIncarnation[child] = TRUE,
            !.stateByIncarnation[child] = "Deleting",
            !.deletionIntentByIncarnation[child] = "Fenced"]
        /\ preservation' = [preservation EXCEPT
            !.destructiveBoundaryFreshByIncarnation[child] =
                @ /\ DestructionAuthorized(child)]
        /\ UNCHANGED <<policy, now, crashed>>

PublishDestructionEvidence ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario \in {"delete", "cancel"}
        /\ \/ graph.deletionIntentByIncarnation[incarnation] = "Fenced"
           \/ /\ graph.stateByIncarnation[incarnation] = "Creating"
              /\ graph.deletionIntentByIncarnation[incarnation] = "Prepared"
        /\ graph' = [graph EXCEPT
            !.deletionIntentByIncarnation[incarnation] = "Evidence"]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

RemoveTargetVisibility ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ graph.stateByIncarnation[incarnation] = "Deleting"
        /\ graph.manifestVisibleByIncarnation[incarnation]
        /\ \/ graph.deletionIntentByIncarnation[incarnation] = "Evidence"
           \/ AllowVisibilityRemovalWithoutEvidence
        /\ graph' = [graph EXCEPT
            !.manifestVisibleByIncarnation[incarnation] = FALSE,
            !.visibilityRemovedWithEvidenceByChild[incarnation] =
                (graph.deletionIntentByIncarnation[incarnation] = "Evidence")]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

PublishVisibilityRemovedMarker ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ HasParent(child)
        /\ graph.stateByIncarnation[child] = "Deleting"
        /\ graph.deletionIntentByIncarnation[child] = "Evidence"
        /\ (~graph.manifestVisibleByIncarnation[child] \/
            AllowRootRemovalBeforeVisibilityGone)
        /\ now < 2
        /\ graph' = [graph EXCEPT
            !.visibilityRemovedAtByChild[child] = now + 1,
            !.rootReleaseNotBeforeByChild[child] = now + 2,
            !.deletionIntentByIncarnation[child] = "VisibilityRemoved"]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

AdvancePastReaderGrace ==
    /\ ~crashed
    /\ graph.scenario = "delete"
    /\ now < 3
    /\ \E child \in Incarnations :
        graph.rootReleaseNotBeforeByChild[child] > now
    /\ now' = now + 1
    /\ UNCHANGED <<graph, policy, preservation, crashed>>

RemoveParentRootCAS ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ HasParent(child)
        /\ graph.stateByIncarnation[child] = "Deleting"
        /\ graph.deletionIntentByIncarnation[child] = "VisibilityRemoved"
        /\ LET parent == Parent(child)
           IN /\ child \in graph.rootsByParent[parent]
              /\ (~graph.manifestVisibleByIncarnation[child] \/
                  AllowRootRemovalBeforeVisibilityGone)
              /\ (now >= graph.rootReleaseNotBeforeByChild[child] \/
                  AllowRootRemovalBeforeReaderGrace)
              /\ (DestructionAuthorized(child) \/ AllowDestructionWithStalePreservation)
              /\ graph' = [graph EXCEPT
                  !.rootsByParent[parent] = @ \ {child},
                  !.rootRemovedByChild[child] = TRUE,
                  !.deletionIntentByIncarnation[child] = "RootRemoved"]
        /\ preservation' = [preservation EXCEPT
            !.destructiveBoundaryFreshByIncarnation[child] =
                @ /\ DestructionAuthorized(child)]
        /\ UNCHANGED <<policy, now, crashed>>

DeleteTargetObjects ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ graph.stateByIncarnation[incarnation] = "Deleting"
        /\ \/ graph.deletionIntentByIncarnation[incarnation] = "RootRemoved"
           \/ /\ ~HasParent(incarnation)
              /\ graph.deletionIntentByIncarnation[incarnation] = "Evidence"
        /\ (DestructionAuthorized(incarnation) \/ AllowDestructionWithStalePreservation)
        /\ graph' = [graph EXCEPT
            !.localRefsByIncarnation[incarnation] = FALSE,
            !.deletionIntentByIncarnation[incarnation] = "ObjectsDeleted"]
        /\ preservation' = [preservation EXCEPT
            !.destructiveBoundaryFreshByIncarnation[incarnation] =
                @ /\ DestructionAuthorized(incarnation)]
        /\ UNCHANGED <<policy, now, crashed>>

DeleteTargetMetadata ==
    \E incarnation \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "delete"
        /\ graph.stateByIncarnation[incarnation] = "Deleting"
        /\ graph.deletionIntentByIncarnation[incarnation] = "ObjectsDeleted"
        /\ (DestructionAuthorized(incarnation) \/ AllowDestructionWithStalePreservation)
        /\ graph' = [graph EXCEPT
            !.stateByIncarnation[incarnation] = "Deleted",
            !.deletionIntentByIncarnation[incarnation] = "MetadataDeleted",
            !.deleteResultByIncarnation[incarnation] = "Success"]
        /\ preservation' = [preservation EXCEPT
            !.destructiveBoundaryFreshByIncarnation[incarnation] =
                @ /\ DestructionAuthorized(incarnation)]
        /\ UNCHANGED <<policy, now, crashed>>

CancelPreparedTarget ==
    \E child \in Incarnations :
        /\ ~crashed
        /\ graph.scenario = "cancel"
        /\ graph.stateByIncarnation[child] = "Creating"
        /\ ~graph.everActiveByIncarnation[child]
        /\ graph.deletionIntentByIncarnation[child] = "Evidence"
        /\ ~graph.manifestVisibleByIncarnation[child]
        /\ HasParent(child)
        /\ LET parent == Parent(child)
           IN /\ graph' = [graph EXCEPT
                  !.rootsByParent[parent] = @ \ {child},
                  !.rootRemovedByChild[child] = graph.rootWasPublishedByChild[child],
                  !.stateByIncarnation[child] = "Deleted",
                  !.intentByIncarnation[child] = "None",
                  !.cancelledByIncarnation[child] = TRUE,
                  !.forkResultByTarget[child] = "Error",
                  !.sourceLeaseOwnerByParent[parent] =
                      IF @ = child THEN NoOwner ELSE @]
        /\ UNCHANGED <<policy, preservation, now, crashed>>

Crash ==
    /\ ~crashed
    /\ graph.scenario \in {"fork", "delete", "cancel"}
    /\ ~graph.crashUsed
    /\ \E incarnation \in Incarnations :
        graph.stateByIncarnation[incarnation] \in {"Creating", "Deleting"}
    /\ graph' = [graph EXCEPT !.crashUsed = TRUE]
    /\ crashed' = TRUE
    /\ UNCHANGED <<policy, preservation, now>>

RecoverCreatingTarget ==
    /\ crashed
    /\ \E child \in Incarnations : graph.stateByIncarnation[child] = "Creating"
    /\ crashed' = FALSE
    /\ UNCHANGED <<graph, policy, preservation, now>>

RecoverDeletingTarget ==
    /\ crashed
    /\ \E incarnation \in Incarnations :
        graph.stateByIncarnation[incarnation] = "Deleting"
    /\ crashed' = FALSE
    /\ UNCHANGED <<graph, policy, preservation, now>>

Stutter == UNCHANGED vars

Next ==
    \/ ReserveTarget
    \/ AcquireSourceLease
    \/ UpdateIndexConfig
    \/ SourceWriterWriteHistory
    \/ SourceWriterCAS
    \/ WriteSourceHeadHistory
    \/ PublishSourceRootCAS
    \/ WriteTargetHistoryOne
    \/ PublishTargetManifest
    \/ InstallBranchSafeSubsystems
    \/ AcquirePolicyLease
    \/ PolicyWriterCAS
    \/ RevalidateActivationPolicy
    \/ InstallPolicyActivationGuardCAS
    \/ ExpirePolicyLease
    \/ ActivateTarget
    \/ ResolveExpiredActivationGuard
    \/ MaterializeBranch
    \/ LoseForkResponse
    \/ RetryFork
    \/ PrepareDeleteIntent
    \/ StrongPreservationCheck
    \/ PreservationLockCAS
    \/ FenceSourceDeleteCAS
    \/ FenceTargetDelete
    \/ PublishDestructionEvidence
    \/ RemoveTargetVisibility
    \/ PublishVisibilityRemovedMarker
    \/ AdvancePastReaderGrace
    \/ RemoveParentRootCAS
    \/ DeleteTargetObjects
    \/ DeleteTargetMetadata
    \/ CancelPreparedTarget
    \/ Crash
    \/ RecoverCreatingTarget
    \/ RecoverDeletingTarget
    \/ Stutter

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ Namespaces = Incarnations
    /\ Incarnations = {"root", "branch", "child"}
    /\ BranchIds = {"b1", "b2"}
    /\ ScenarioModes = {"fork", "nested", "delete", "policy", "preservation", "cancel"}
    /\ graph.scenario \in ScenarioModes
    /\ SourceGenerations = 0..MaxGeneration
    /\ ConfigValues = {"cfg0", "cfg1"}
    /\ PolicyVersions = 0..MaxPolicyVersion
    /\ MaxDepth = 2
    /\ graph.generationByIncarnation \in [Incarnations -> SourceGenerations]
    /\ graph.historyByIncarnation \in [Incarnations -> SUBSET SourceGenerations]
    /\ graph.rootsByParent \in [Incarnations -> SUBSET Incarnations]
    /\ graph.parentByChild \in [Incarnations -> Incarnations \cup {NoParent}]
    /\ graph.branchIdByChild \in [Incarnations -> BranchIds \cup {NoBranch}]
    /\ graph.rootGenerationByChild \in [Incarnations -> SourceGenerations]
    /\ graph.rootDigestByChild \in [Incarnations -> SourceGenerations]
    /\ graph.rootPreCasHeadByChild \in [Incarnations -> SourceGenerations]
    /\ graph.rootPreCasDigestByChild \in [Incarnations -> SourceGenerations]
    /\ graph.rootPublicationGenerationByChild \in [Incarnations -> SourceGenerations]
    /\ graph.rootWasPublishedByChild \in [Incarnations -> BOOLEAN]
    /\ graph.rootRemovedByChild \in [Incarnations -> BOOLEAN]
    /\ graph.fenceByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.leaseTokenByIncarnation \in [Incarnations -> 0..10]
    /\ graph.sourceLeaseOwnerByParent \in [Incarnations -> Incarnations \cup {NoOwner}]
    /\ graph.etagByIncarnation \in [Incarnations -> 0..10]
    /\ graph.rootHistoryPreparedByChild \in [Incarnations -> BOOLEAN]
    /\ graph.rootPreCasEtagByChild \in [Incarnations -> 0..10]
    /\ graph.writerHistoryPreparedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.writerPreCasHeadByIncarnation \in [Incarnations -> SourceGenerations]
    /\ graph.writerPreCasEtagByIncarnation \in [Incarnations -> 0..10]
    /\ graph.writerAdvancedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.configUpdatedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.stateByIncarnation \in [Incarnations -> NamespaceStates]
    /\ graph.intentByIncarnation \in [Incarnations -> IntentStates]
    /\ graph.dataPlaneConfigByIncarnation \in [Incarnations -> ConfigValues]
    /\ graph.reservedConfigByChild \in [Incarnations -> ConfigValues \cup {NoConfig}]
    /\ graph.frozenConfigDigestByChild \in [Incarnations -> ConfigValues \cup {NoConfig}]
    /\ graph.rootConfigDigestByChild \in [Incarnations -> ConfigValues \cup {NoConfig}]
    /\ graph.activationConfigByChild \in [Incarnations -> ConfigValues \cup {NoConfig}]
    /\ graph.manifestVisibleByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.targetHistoryWrittenByChild \in [Incarnations -> BOOLEAN]
    /\ graph.baseGenerationByChild \in [Incarnations -> SourceGenerations]
    /\ graph.baseDigestByChild \in [Incarnations -> SourceGenerations]
    /\ graph.foreignRefsByIncarnation \in [Incarnations -> SUBSET Incarnations]
    /\ graph.localRefsByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.materializedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.forkViewDigestByChild \in [Incarnations -> SourceGenerations]
    /\ graph.visibilityRemovedAtByChild \in [Incarnations -> 0..3]
    /\ graph.rootReleaseNotBeforeByChild \in [Incarnations -> 0..3]
    /\ graph.visibilityRemovedWithEvidenceByChild \in [Incarnations -> BOOLEAN]
    /\ graph.deletionIntentByIncarnation \in [Incarnations -> DeletionStates]
    /\ graph.branchSafeSubsystems \subseteq AllSubsystems
    /\ graph.forkResultByTarget \in [Incarnations -> ResultStates]
    /\ graph.deleteResultByIncarnation \in [Incarnations -> ResultStates]
    /\ graph.responseLostByTarget \in [Incarnations -> BOOLEAN]
    /\ graph.everActiveByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.cancelledByIncarnation \in [Incarnations -> BOOLEAN]
    /\ graph.crashUsed \in BOOLEAN
    /\ policy.version \in PolicyVersions
    /\ policy.leaseOwner \in Incarnations \cup {NoOwner}
    /\ policy.activationNonceByChild \in [Incarnations -> 0..4]
    /\ policy.guardNonceByChild \in [Incarnations -> 0..4]
    /\ policy.guardVersionByChild \in [Incarnations -> PolicyVersions \cup {NoVersion}]
    /\ policy.activationPolicyVersionByChild \in [Incarnations -> PolicyVersions \cup {NoVersion}]
    /\ policy.activationGuardUsedByChild \in [Incarnations -> BOOLEAN]
    /\ policy.guardRemovalSafeByChild \in [Incarnations -> BOOLEAN]
    /\ policy.guardRecoveryUsedByChild \in [Incarnations -> BOOLEAN]
    /\ preservation.version \in PolicyVersions
    /\ preservation.lockedByIncarnation \in [Incarnations -> BOOLEAN]
    /\ preservation.proofVersionByIncarnation \in [Incarnations -> PolicyVersions \cup {NoVersion}]
    /\ preservation.destructiveBoundaryFreshByIncarnation \in [Incarnations -> BOOLEAN]
    /\ now \in 0..3
    /\ crashed \in BOOLEAN

ActiveBranchHasMatchingRoot ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) => RootMatches(child)

RootPinsExactPredecessorGeneration ==
    \A child \in Incarnations :
        graph.rootWasPublishedByChild[child] =>
            LET parent == Parent(child)
            IN /\ graph.rootGenerationByChild[child] \in graph.historyByIncarnation[parent]
               /\ graph.rootDigestByChild[child] = Digest(graph.rootGenerationByChild[child])
               /\ graph.rootPreCasHeadByChild[child] \in graph.historyByIncarnation[parent]
               /\ graph.rootPreCasDigestByChild[child] = Digest(graph.rootPreCasHeadByChild[child])
               /\ graph.rootPublicationGenerationByChild[child] =
                  graph.rootPreCasHeadByChild[child] + 1

ActiveBranchBaseIdentityMatchesRoot ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            /\ graph.baseGenerationByChild[child] = graph.rootGenerationByChild[child]
            /\ graph.baseDigestByChild[child] = graph.rootDigestByChild[child]

SourceFenceExcludesRoots ==
    \A parent \in Incarnations :
        graph.fenceByIncarnation[parent] => graph.rootsByParent[parent] = {}

TargetActivationRequiresVisibleManifest ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            /\ graph.manifestVisibleByIncarnation[child]
            /\ graph.targetHistoryWrittenByChild[child]

VisibleManifestRequiresRoot ==
    \A child \in Incarnations :
        (graph.manifestVisibleByIncarnation[child] /\ HasParent(child)) => RootMatches(child)

ActivationRequiresBranchSafeSubsystems ==
    (\E child \in Incarnations :
        graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            graph.branchSafeSubsystems = AllSubsystems

TargetConfigMatchesSourceSnapshot ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            graph.activationConfigByChild[child] = graph.frozenConfigDigestByChild[child]

ConfigFreezeLinearizesWithIndexConfigUpdate ==
    \A child \in Incarnations :
        graph.rootWasPublishedByChild[child] =>
            graph.frozenConfigDigestByChild[child] = graph.rootConfigDigestByChild[child]

ActivationUsesOneFencedPolicyHead ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            /\ policy.activationGuardUsedByChild[child]
            /\ policy.activationPolicyVersionByChild[child] \in PolicyVersions

PolicyMutationCannotPassActivationGuard ==
    \A child \in Incarnations :
        policy.guardNonceByChild[child] > 0 =>
            policy.guardVersionByChild[child] = policy.version

GuardRemovalRevokesStaleActivationOrObservesActive ==
    \A child \in Incarnations : policy.guardRemovalSafeByChild[child]

RootRemovalRequiresTargetVisibilityGone ==
    \A child \in Incarnations :
        graph.rootRemovedByChild[child] /\ graph.everActiveByIncarnation[child] =>
            ~graph.manifestVisibleByIncarnation[child]

RootRemovalRequiresReaderGrace ==
    \A child \in Incarnations :
        graph.rootRemovedByChild[child] /\ graph.everActiveByIncarnation[child] =>
            /\ graph.rootReleaseNotBeforeByChild[child] > 0
            /\ now >= graph.rootReleaseNotBeforeByChild[child]

VisibilityRemovalRequiresDestructionEvidence ==
    \A child \in Incarnations : graph.visibilityRemovedWithEvidenceByChild[child]

EachDestructiveBoundaryUsesFreshPreservationHead ==
    \A incarnation \in Incarnations :
        preservation.destructiveBoundaryFreshByIncarnation[incarnation]

SuccessfulForkIsIdempotent ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            \/ graph.forkResultByTarget[child] = "Success"
            \/ graph.responseLostByTarget[child]

SourceWritesDoNotChangeTargetBase == ActiveBranchBaseIdentityMatchesRoot

TargetWritesDoNotChangeSource ==
    \A child \in Incarnations :
        HasParent(child) => child \notin graph.foreignRefsByIncarnation[Parent(child)]

FreshIncarnationPreventsCycles ==
    \A child \in Incarnations : HasParent(child) => Parent(child) # child

ParentRelationIsAcyclicAndDepthBounded ==
    /\ MaxDepth = 2
    /\ \A child \in Incarnations : DepthOK(child)

NestedOriginClosureRemainsRooted ==
    \A child \in Incarnations :
        (graph.stateByIncarnation[child] = "Active" /\ HasParent(child)) =>
            LET parent == Parent(child)
            IN /\ {parent} \cup graph.foreignRefsByIncarnation[parent]
                   \subseteq graph.foreignRefsByIncarnation[child]
               /\ RootMatches(child)
               /\ (HasParent(parent) => RootMatches(parent))

MaterializationKeepsHistoricalForeignRefsRooted ==
    \A child \in Incarnations :
        (graph.materializedByIncarnation[child] /\
         graph.manifestVisibleByIncarnation[child]) =>
            /\ graph.foreignRefsByIncarnation[child] # {}
            /\ RootMatches(child)

FailedForkIsNeverReportedSuccess ==
    \A child \in Incarnations :
        graph.cancelledByIncarnation[child] => graph.forkResultByTarget[child] = "Error"

ActivationAndPreparedCancelAreMutuallyExclusive ==
    \A child \in Incarnations :
        graph.everActiveByIncarnation[child] => ~graph.cancelledByIncarnation[child]

PreparedCancelCannotLeaveOrphanRoot ==
    \A child \in Incarnations :
        graph.cancelledByIncarnation[child] =>
            /\ HasParent(child)
            /\ child \notin graph.rootsByParent[Parent(child)]

ReservedNoRootCanCancelAfterParentDeletion ==
    \A child \in Incarnations :
        (graph.cancelledByIncarnation[child] /\ ~graph.rootWasPublishedByChild[child]) =>
            /\ graph.stateByIncarnation[child] = "Deleted"
            /\ graph.forkResultByTarget[child] = "Error"

====
