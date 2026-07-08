---- MODULE RestoreCloneSafety ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    SourceKeys,
    TargetKeys,
    AllowBuggyPublish,
    AllowCopyFailure

VARIABLES
    sourceHistoryRetained,
    resolved,
    sourceObjects,
    sourceManifestKeys,
    targetMeta,
    targetObjects,
    targetManifestVisible,
    targetManifestKeys,
    manifestRewritten,
    copyFailed,
    sourceDeleted,
    sourceGcDeleted,
    cloneResult,
    publishFailed,
    targetFencingReset,
    targetPendingCleared,
    targetVersionReset,
    targetGeneration

vars ==
    << sourceHistoryRetained, resolved, sourceObjects, sourceManifestKeys,
       targetMeta, targetObjects, targetManifestVisible, targetManifestKeys,
       manifestRewritten, copyFailed, sourceDeleted, sourceGcDeleted,
       cloneResult, publishFailed, targetFencingReset, targetPendingCleared,
       targetVersionReset, targetGeneration >>

AllKeys ==
    SourceKeys \cup TargetKeys

\* Keep the model small and concrete. The cfg binds these exact source keys.
TargetOf(s) ==
    CASE s = "s1" -> "t1"
      [] s = "s2" -> "t2"
      [] s = "s3" -> "t3"
      [] OTHER -> "unknown-target"

RewrittenTargetKeys ==
    {TargetOf(s) : s \in sourceManifestKeys}

Init ==
    /\ sourceHistoryRetained = TRUE
    /\ resolved = FALSE
    /\ sourceObjects = SourceKeys
    /\ sourceManifestKeys = SourceKeys
    /\ targetMeta = "absent"
    /\ targetObjects = {}
    /\ targetManifestVisible = FALSE
    /\ targetManifestKeys = {}
    /\ manifestRewritten = FALSE
    /\ copyFailed = {}
    /\ sourceDeleted = FALSE
    /\ sourceGcDeleted = {}
    /\ cloneResult = "none"
    /\ publishFailed = FALSE
    /\ targetFencingReset = FALSE
    /\ targetPendingCleared = FALSE
    /\ targetVersionReset = FALSE
    /\ targetGeneration = 0

PruneSourceHistoryBeforeResolve ==
    /\ ~resolved
    /\ cloneResult = "none"
    /\ sourceHistoryRetained' = FALSE
    /\ UNCHANGED << resolved, sourceObjects, sourceManifestKeys,
                    targetMeta, targetObjects, targetManifestVisible,
                    targetManifestKeys, manifestRewritten, copyFailed,
                    sourceDeleted, sourceGcDeleted, cloneResult,
                    publishFailed, targetFencingReset, targetPendingCleared,
                    targetVersionReset, targetGeneration >>

ResolveSourceHistory ==
    /\ cloneResult = "none"
    /\ sourceHistoryRetained
    /\ resolved' = TRUE
    /\ UNCHANGED << sourceHistoryRetained, sourceObjects, sourceManifestKeys,
                    targetMeta, targetObjects, targetManifestVisible,
                    targetManifestKeys, manifestRewritten, copyFailed,
                    sourceDeleted, sourceGcDeleted, cloneResult,
                    publishFailed, targetFencingReset, targetPendingCleared,
                    targetVersionReset, targetGeneration >>

ResolveSourceHistoryMissing ==
    /\ cloneResult = "none"
    /\ ~sourceHistoryRetained
    /\ cloneResult' = "error"
    /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                    sourceManifestKeys, targetMeta, targetObjects,
                    targetManifestVisible, targetManifestKeys,
                    manifestRewritten, copyFailed, sourceDeleted,
                    sourceGcDeleted, publishFailed, targetFencingReset,
                    targetPendingCleared, targetVersionReset,
                    targetGeneration >>

CreateTargetNamespace ==
    /\ resolved
    /\ cloneResult = "none"
    /\ targetMeta = "absent"
    /\ targetMeta' = "created"
    /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                    sourceManifestKeys, targetObjects, targetManifestVisible,
                    targetManifestKeys, manifestRewritten, copyFailed,
                    sourceDeleted, sourceGcDeleted, cloneResult,
                    publishFailed, targetFencingReset, targetPendingCleared,
                    targetVersionReset, targetGeneration >>

RewriteManifestToTarget ==
    /\ resolved
    /\ cloneResult = "none"
    /\ targetMeta = "created"
    /\ ~manifestRewritten
    /\ manifestRewritten' = TRUE
    /\ targetManifestKeys' = RewrittenTargetKeys
    /\ targetFencingReset' = TRUE
    /\ targetPendingCleared' = TRUE
    /\ targetVersionReset' = TRUE
    /\ UNCHANGED << sourceHistoryRetained, sourceObjects, sourceManifestKeys,
                    targetMeta, targetObjects, targetManifestVisible,
                    copyFailed, sourceDeleted, sourceGcDeleted, cloneResult,
                    publishFailed, resolved, targetGeneration >>

CopyOneObject ==
    \E s \in sourceManifestKeys :
        /\ resolved
        /\ cloneResult = "none"
        /\ targetMeta = "created"
        /\ s \in sourceObjects
        /\ TargetOf(s) \notin targetObjects
        /\ targetObjects' = targetObjects \cup {TargetOf(s)}
        /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                        sourceManifestKeys, targetMeta,
                        targetManifestVisible, targetManifestKeys,
                        manifestRewritten, copyFailed, sourceDeleted,
                        sourceGcDeleted, cloneResult, publishFailed,
                        targetFencingReset, targetPendingCleared, targetVersionReset,
                        targetGeneration >>

CopyOneObjectFails ==
    /\ AllowCopyFailure
    /\ \E s \in sourceManifestKeys :
        /\ resolved
        /\ cloneResult = "none"
        /\ targetMeta = "created"
        /\ TargetOf(s) \notin targetObjects
        /\ copyFailed' = copyFailed \cup {s}
        /\ cloneResult' = "error"
        /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                        sourceManifestKeys, targetMeta, targetObjects,
                        targetManifestVisible, targetManifestKeys,
                        manifestRewritten, sourceDeleted, sourceGcDeleted,
                        publishFailed, targetFencingReset, targetPendingCleared,
                        targetVersionReset, targetGeneration >>

PublishTargetManifest ==
    /\ resolved
    /\ cloneResult = "none"
    /\ targetMeta = "created"
    /\ manifestRewritten
    /\ targetManifestKeys \subseteq targetObjects
    /\ copyFailed = {}
    /\ ~targetManifestVisible
    /\ targetManifestVisible' = TRUE
    /\ cloneResult' = "success"
    /\ targetGeneration' = 1
    /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                    sourceManifestKeys, targetMeta, targetObjects,
                    targetManifestKeys, manifestRewritten, copyFailed,
                    sourceDeleted, sourceGcDeleted, publishFailed, targetFencingReset,
                    targetPendingCleared, targetVersionReset >>

PublishTargetManifestFails ==
    /\ resolved
    /\ cloneResult = "none"
    /\ targetMeta = "created"
    /\ manifestRewritten
    /\ targetManifestKeys \subseteq targetObjects
    /\ copyFailed = {}
    /\ ~targetManifestVisible
    /\ publishFailed' = TRUE
    /\ cloneResult' = "error"
    /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                    sourceManifestKeys, targetMeta, targetObjects,
                    targetManifestVisible, targetManifestKeys,
                    manifestRewritten, copyFailed, sourceDeleted,
                    sourceGcDeleted, targetFencingReset,
                    targetPendingCleared, targetVersionReset,
                    targetGeneration >>

BuggyPublishBeforeCopy ==
    /\ AllowBuggyPublish
    /\ resolved
    /\ cloneResult = "none"
    /\ targetMeta = "created"
    /\ ~targetManifestVisible
    /\ targetManifestVisible' = TRUE
    /\ targetManifestKeys' = RewrittenTargetKeys
    /\ manifestRewritten' = TRUE
    /\ targetFencingReset' = TRUE
    /\ targetPendingCleared' = TRUE
    /\ targetVersionReset' = TRUE
    /\ targetGeneration' = 1
    /\ cloneResult' = "success"
    /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                    sourceManifestKeys, targetMeta, targetObjects,
                    copyFailed, sourceDeleted, sourceGcDeleted, publishFailed >>

SourceDeleteOrGc ==
    \E s \in sourceObjects :
        /\ sourceObjects' = sourceObjects \ {s}
        /\ sourceGcDeleted' = sourceGcDeleted \cup {s}
        /\ sourceDeleted' = (sourceObjects \ {s} = {})
        /\ UNCHANGED << sourceHistoryRetained, resolved, sourceManifestKeys,
                        targetMeta, targetObjects, targetManifestVisible,
                        targetManifestKeys, manifestRewritten, copyFailed,
                        cloneResult, publishFailed, targetFencingReset,
                        targetPendingCleared, targetVersionReset,
                        targetGeneration >>

TargetWriteAfterClone ==
    \E t \in TargetKeys :
        /\ cloneResult = "success"
        /\ targetObjects' = targetObjects \cup {t}
        /\ UNCHANGED << sourceHistoryRetained, resolved, sourceObjects,
                        sourceManifestKeys, targetMeta, targetManifestVisible,
                        targetManifestKeys, manifestRewritten, copyFailed,
                        sourceDeleted, sourceGcDeleted, cloneResult,
                        publishFailed, targetFencingReset, targetPendingCleared,
                        targetVersionReset, targetGeneration >>

TerminalStutter ==
    /\ cloneResult # "none"
    /\ UNCHANGED vars

Next ==
    \/ PruneSourceHistoryBeforeResolve
    \/ ResolveSourceHistory
    \/ ResolveSourceHistoryMissing
    \/ CreateTargetNamespace
    \/ RewriteManifestToTarget
    \/ CopyOneObject
    \/ CopyOneObjectFails
    \/ PublishTargetManifest
    \/ PublishTargetManifestFails
    \/ BuggyPublishBeforeCopy
    \/ SourceDeleteOrGc
    \/ TargetWriteAfterClone
    \/ TerminalStutter

Spec ==
    Init /\ [][Next]_vars

VisibleTargetRefsExist ==
    targetManifestVisible => targetManifestKeys \subseteq targetObjects

TargetRefsAreTargetOwned ==
    targetManifestVisible => targetManifestKeys \subseteq TargetKeys

SourceDeletionCannotBreakSuccessfulClone ==
    cloneResult = "success" => targetManifestKeys \subseteq targetObjects

FailedCloneNotReportedSuccess ==
    (copyFailed # {} \/ publishFailed) => cloneResult # "success"

SuccessRequiresVisibleTarget ==
    cloneResult = "success" => targetManifestVisible

CloneResetsSourceState ==
    targetManifestVisible =>
        /\ targetFencingReset
        /\ targetPendingCleared
        /\ targetVersionReset
        /\ targetGeneration = 1

TypeOK ==
    /\ sourceHistoryRetained \in BOOLEAN
    /\ resolved \in BOOLEAN
    /\ sourceObjects \subseteq SourceKeys
    /\ sourceManifestKeys \subseteq SourceKeys
    /\ targetMeta \in {"absent", "created"}
    /\ targetObjects \subseteq TargetKeys
    /\ targetManifestVisible \in BOOLEAN
    /\ targetManifestKeys \subseteq AllKeys \cup {"unknown-target"}
    /\ manifestRewritten \in BOOLEAN
    /\ copyFailed \subseteq SourceKeys
    /\ sourceDeleted \in BOOLEAN
    /\ sourceGcDeleted \subseteq SourceKeys
    /\ cloneResult \in {"none", "success", "error"}
    /\ publishFailed \in BOOLEAN
    /\ targetFencingReset \in BOOLEAN
    /\ targetPendingCleared \in BOOLEAN
    /\ targetVersionReset \in BOOLEAN
    /\ targetGeneration \in 0..1
    /\ SourceKeys = {"s1", "s2", "s3"}
    /\ TargetKeys = {"t1", "t2", "t3"}

====
