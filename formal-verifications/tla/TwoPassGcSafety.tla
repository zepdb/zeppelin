---- MODULE TwoPassGcSafety ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,
    Horizon,
    MaxTime,
    InitialLive,
    InitialHistory,
    InitialPendingDeletes,
    InitialHistoryPendingDeletes,
    AllowBuggySweepWithoutRevalidate,
    AllowBuggyStaleHistorySweep,
    AllowBuggyHistoryPendingDeleteFilter

VARIABLES
    s3,
    live,
    history,
    \* Abstracts root sets that are not current production heads but are called
    \* out by the fable plans: branch heads, branch_pending markers, shadow
    \* manifests/staging, overlay mounts, batch-pinned manifests, epoch pins,
    \* published images, and governed-forgetting lineage roots.
    futureRoots,
    pendingDeletes,
    \* Keys a retained generation still pins through its own pending_deletes.
    \* These are the *uncertain* keys of that generation: recorded for deletion
    \* but not confirmed gone. Nothing else roots them - that is what makes
    \* them the exact shape aaf7b86 swept by mistake.
    historyPendingDeletes,
    staged,
    stagingActive,
    candidates,
    listedThisCycle,
    \* Retained-history reachability captured at mark time. Sweep must not use
    \* this stale snapshot for destructive deletes.
    historyAtMark,
    now,
    deleteFailed,
    deleted,
    pendingPruneBad

vars ==
    << s3, live, history, futureRoots, pendingDeletes, historyPendingDeletes,
       staged, stagingActive,
       candidates, listedThisCycle, historyAtMark, now, deleteFailed,
       deleted, pendingPruneBad >>

NoCandidate == MaxTime + 1

ActiveStaged ==
    IF stagingActive THEN staged ELSE {}

\* What GC *computes* for a retained generation's pending_deletes.
\*
\* This is the seam aaf7b86 broke. retained_manifest_history_reachable_keys
\* and load_history_observation_owned both cleared pending_deletes off a
\* retained historical manifest before expanding its reachable set, so keys
\* pinned only that way vanished from GC's view while still being needed.
\* With the flag FALSE this equals historyPendingDeletes and the model
\* describes the fixed code.
HistoryPinnedAsComputed ==
    IF AllowBuggyHistoryPendingDeleteFilter THEN {} ELSE historyPendingDeletes

\* Ground truth: what must never be deleted. Deliberately NOT filtered - a
\* retained generation's pending_deletes pin the object regardless of whether
\* GC's expansion happens to see them. The gap between this and
\* HistoryPinnedAsComputed is exactly the production defect.
ClientReachable ==
    live \cup history \cup ActiveStaged \cup futureRoots \cup historyPendingDeletes

\* Production mark/sweep reachability also includes manifest pending_deletes.
\* Pending-delete drain is the only path allowed to physically remove them.
GcReachable ==
    live \cup history \cup ActiveStaged \cup futureRoots \cup pendingDeletes
        \cup HistoryPinnedAsComputed

MarkReachable ==
    live \cup historyAtMark \cup ActiveStaged \cup futureRoots \cup pendingDeletes
        \cup HistoryPinnedAsComputed

Candidate(k) ==
    candidates[k] # NoCandidate

CandidateEligible(k) ==
    /\ Candidate(k)
    /\ now >= candidates[k] + Horizon

Init ==
    /\ s3 = InitialLive \cup InitialHistory \cup InitialPendingDeletes
             \cup InitialHistoryPendingDeletes
    /\ live = InitialLive
    /\ history = InitialHistory
    /\ futureRoots = {}
    /\ pendingDeletes = InitialPendingDeletes
    /\ historyPendingDeletes = InitialHistoryPendingDeletes
    /\ staged = {}
    /\ stagingActive = FALSE
    /\ candidates = [k \in Keys |-> NoCandidate]
    /\ listedThisCycle = {}
    /\ historyAtMark = InitialHistory
    /\ now = 0
    /\ deleteFailed = {}
    /\ deleted = {}
    /\ pendingPruneBad = FALSE

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                    staged, stagingActive, candidates, listedThisCycle,
                    historyAtMark,
                    deleteFailed, deleted, pendingPruneBad >>

StartCycle ==
    /\ listedThisCycle' = s3
    /\ historyAtMark' = history
    /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                    staged, stagingActive, candidates, now,
                    deleteFailed, deleted, pendingPruneBad >>

MarkCandidates ==
    LET reachable == MarkReachable
        nextCandidates ==
            [k \in Keys |->
                IF k \in reachable
                THEN NoCandidate
                ELSE IF k \in listedThisCycle /\ candidates[k] = NoCandidate
                     THEN now
                     ELSE candidates[k]]
    IN
    /\ candidates' = nextCandidates
    /\ deleteFailed' =
        {k \in deleteFailed :
            k \in pendingDeletes \/ nextCandidates[k] # NoCandidate}
    /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                    staged, stagingActive, listedThisCycle, historyAtMark,
                    now, deleted, pendingPruneBad >>

MakeLive ==
    \E k \in s3 \ (pendingDeletes \cup ActiveStaged) :
        /\ live' = live \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, history, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

DropLive ==
    \E k \in live :
        /\ live' = live \ {k}
        /\ UNCHANGED << historyPendingDeletes, s3, history, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

MoveLiveToPendingDelete ==
    \E k \in live :
        /\ live' = live \ {k}
        /\ pendingDeletes' = pendingDeletes \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, history, futureRoots, staged, stagingActive,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

AddHistoryRoot ==
    \E k \in live :
        /\ history' = history \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

PruneHistoryRoots ==
    \E kept \in SUBSET history :
        /\ history' = kept
        /\ UNCHANGED << historyPendingDeletes, s3, live, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

AddFutureRoot ==
    \E k \in s3 \ pendingDeletes :
        /\ futureRoots' = futureRoots \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

DropFutureRoot ==
    \E k \in futureRoots :
        /\ futureRoots' = futureRoots \ {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

StageNewUpload ==
    \E k \in Keys \ (s3 \cup deleted) :
        /\ s3' = s3 \cup {k}
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << historyPendingDeletes, live, history, futureRoots, pendingDeletes,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

StageExistingObject ==
    \E k \in s3 \ (pendingDeletes \cup live) :
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

CommitStagedUploads ==
    /\ stagingActive
    /\ live' = live \cup staged
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << historyPendingDeletes, s3, history, futureRoots, pendingDeletes,
                    candidates, listedThisCycle, historyAtMark, now,
                    deleteFailed, deleted, pendingPruneBad >>

ExpireStaging ==
    /\ stagingActive
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                    candidates, listedThisCycle, historyAtMark, now,
                    deleteFailed, deleted, pendingPruneBad >>

DrainPendingDeleteSuccess ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin history
        /\ k \notin futureRoots
        \* Production's drain skips any key retained history still reaches
        \* (`retained_history.contains(key)` -> retained, continue). It reads
        \* the *computed* pin set, which is why clearing pending_deletes out
        \* of that expansion let the drain delete protected objects.
        /\ k \notin HistoryPinnedAsComputed
        /\ now >= Horizon
        /\ s3' = s3 \ {k}
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << historyPendingDeletes, live, history, futureRoots, staged, stagingActive,
                        listedThisCycle, historyAtMark, now, pendingPruneBad >>

DrainPendingDeleteAbsent ==
    \E k \in pendingDeletes :
        /\ k \notin s3
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ deleteFailed' = deleteFailed \ {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleted, pendingPruneBad >>

DrainPendingDeleteFailure ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin history
        /\ k \notin futureRoots
        /\ k \notin HistoryPinnedAsComputed
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                        staged, stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleted, pendingPruneBad >>

SweepCandidate ==
    \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ k \notin GcReachable
        /\ s3' = s3 \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << historyPendingDeletes, live, history, futureRoots, pendingDeletes, staged,
                        stagingActive, listedThisCycle, historyAtMark, now,
                        pendingPruneBad >>

SweepCandidateDeleteFailure ==
    \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ k \notin GcReachable
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << historyPendingDeletes, s3, live, history, futureRoots, pendingDeletes,
                        staged, stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleted, pendingPruneBad >>

BuggySweepWithoutRevalidate ==
    /\ AllowBuggySweepWithoutRevalidate
    /\ \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ s3' = s3 \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << historyPendingDeletes, live, history, futureRoots, pendingDeletes, staged,
                        stagingActive, listedThisCycle, historyAtMark, now,
                        pendingPruneBad >>

BuggySweepWithStaleHistory ==
    /\ AllowBuggyStaleHistorySweep
    /\ \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ k \notin (live \cup historyAtMark \cup ActiveStaged \cup futureRoots \cup pendingDeletes)
        /\ s3' = s3 \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << historyPendingDeletes, live, history, futureRoots, pendingDeletes, staged,
                        stagingActive, listedThisCycle, historyAtMark, now,
                        pendingPruneBad >>

\* A generation retires an object into its own pending_deletes. Nothing else
\* roots it from here on, so only the history pin keeps it alive.
QueueHistoryPendingDelete ==
    \E k \in s3 :
        /\ k \notin live
        /\ k \notin history
        /\ k \notin futureRoots
        /\ k \notin pendingDeletes
        /\ k \notin historyPendingDeletes
        /\ historyPendingDeletes' = historyPendingDeletes \cup {k}
        /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                        staged, stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

\* The pinning generation falls out of the retention window, so the pin goes
\* with it and the object becomes collectable on the ordinary path.
PruneHistoryPendingDelete ==
    \E k \in historyPendingDeletes :
        /\ historyPendingDeletes' = historyPendingDeletes \ {k}
        /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                        staged, stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

Next ==
    \/ AdvanceTime
    \/ StartCycle
    \/ MarkCandidates
    \/ MakeLive
    \/ DropLive
    \/ MoveLiveToPendingDelete
    \/ AddHistoryRoot
    \/ PruneHistoryRoots
    \/ QueueHistoryPendingDelete
    \/ PruneHistoryPendingDelete
    \/ AddFutureRoot
    \/ DropFutureRoot
    \/ StageNewUpload
    \/ StageExistingObject
    \/ CommitStagedUploads
    \/ ExpireStaging
    \/ DrainPendingDeleteSuccess
    \/ DrainPendingDeleteAbsent
    \/ DrainPendingDeleteFailure
    \/ SweepCandidate
    \/ SweepCandidateDeleteFailure
    \/ BuggySweepWithoutRevalidate
    \/ BuggySweepWithStaleHistory

Spec ==
    Init /\ [][Next]_vars

NoReachableKeyDeleted ==
    deleted \cap ClientReachable = {}

CandidateStateIsAdvisory ==
    \A k \in Keys :
        (Candidate(k) /\ k \in GcReachable) => k \notin deleted

PendingDeletesPrunedOnlyAfterAbsence ==
    pendingPruneBad = FALSE

ActiveStagingPinsUploads ==
    stagingActive => deleted \cap staged = {}

FutureRootsProtected ==
    deleted \cap futureRoots = {}

PendingDeletesNotClientReachable ==
    pendingDeletes \cap (live \cup ActiveStaged) = {}

FailedDeletesRemainRetriable ==
    \A k \in deleteFailed :
        /\ k \in s3
        /\ k \in pendingDeletes \/ Candidate(k)

TypeOK ==
    /\ s3 \subseteq Keys
    /\ live \subseteq Keys
    /\ history \subseteq Keys
    /\ futureRoots \subseteq Keys
    /\ pendingDeletes \subseteq Keys
    /\ historyPendingDeletes \subseteq Keys
    /\ staged \subseteq Keys
    /\ stagingActive \in BOOLEAN
    /\ candidates \in [Keys -> 0..NoCandidate]
    /\ listedThisCycle \subseteq Keys
    /\ historyAtMark \subseteq Keys
    /\ now \in 0..MaxTime
    /\ deleteFailed \subseteq Keys
    /\ deleted \subseteq Keys
    /\ pendingPruneBad \in BOOLEAN
    /\ InitialLive \subseteq Keys
    /\ InitialHistory \subseteq Keys
    /\ InitialPendingDeletes \subseteq Keys
    /\ InitialPendingDeletes \cap InitialLive = {}
    /\ InitialPendingDeletes \cap InitialHistory = {}
    /\ InitialHistoryPendingDeletes \subseteq Keys
    /\ InitialHistoryPendingDeletes \cap InitialLive = {}
    /\ InitialHistoryPendingDeletes \cap InitialHistory = {}
    /\ InitialHistoryPendingDeletes \cap InitialPendingDeletes = {}

====
