---- MODULE TwoPassGcSafety ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,
    Horizon,
    MaxTime,
    InitialLive,
    InitialHistory,
    InitialPendingDeletes,
    AllowBuggySweepWithoutRevalidate,
    AllowBuggyStaleHistorySweep

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
    << s3, live, history, futureRoots, pendingDeletes, staged, stagingActive,
       candidates, listedThisCycle, historyAtMark, now, deleteFailed,
       deleted, pendingPruneBad >>

NoCandidate == MaxTime + 1

ActiveStaged ==
    IF stagingActive THEN staged ELSE {}

ClientReachable ==
    live \cup history \cup ActiveStaged \cup futureRoots

\* Production mark/sweep reachability also includes manifest pending_deletes.
\* Pending-delete drain is the only path allowed to physically remove them.
GcReachable ==
    ClientReachable \cup pendingDeletes

MarkReachable ==
    live \cup historyAtMark \cup ActiveStaged \cup futureRoots \cup pendingDeletes

Candidate(k) ==
    candidates[k] # NoCandidate

CandidateEligible(k) ==
    /\ Candidate(k)
    /\ now >= candidates[k] + Horizon

Init ==
    /\ s3 = InitialLive \cup InitialHistory \cup InitialPendingDeletes
    /\ live = InitialLive
    /\ history = InitialHistory
    /\ futureRoots = {}
    /\ pendingDeletes = InitialPendingDeletes
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
    /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                    staged, stagingActive, candidates, listedThisCycle,
                    historyAtMark,
                    deleteFailed, deleted, pendingPruneBad >>

StartCycle ==
    /\ listedThisCycle' = s3
    /\ historyAtMark' = history
    /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
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
    /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                    staged, stagingActive, listedThisCycle, historyAtMark,
                    now, deleted, pendingPruneBad >>

MakeLive ==
    \E k \in s3 \ (pendingDeletes \cup ActiveStaged) :
        /\ live' = live \cup {k}
        /\ UNCHANGED << s3, history, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

DropLive ==
    \E k \in live :
        /\ live' = live \ {k}
        /\ UNCHANGED << s3, history, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

MoveLiveToPendingDelete ==
    \E k \in live :
        /\ live' = live \ {k}
        /\ pendingDeletes' = pendingDeletes \cup {k}
        /\ UNCHANGED << s3, history, futureRoots, staged, stagingActive,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

AddHistoryRoot ==
    \E k \in live :
        /\ history' = history \cup {k}
        /\ UNCHANGED << s3, live, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

PruneHistoryRoots ==
    \E kept \in SUBSET history :
        /\ history' = kept
        /\ UNCHANGED << s3, live, futureRoots, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

AddFutureRoot ==
    \E k \in s3 \ pendingDeletes :
        /\ futureRoots' = futureRoots \cup {k}
        /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

DropFutureRoot ==
    \E k \in futureRoots :
        /\ futureRoots' = futureRoots \ {k}
        /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleteFailed, deleted,
                        pendingPruneBad >>

StageNewUpload ==
    \E k \in Keys \ (s3 \cup deleted) :
        /\ s3' = s3 \cup {k}
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << live, history, futureRoots, pendingDeletes,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

StageExistingObject ==
    \E k \in s3 \ (pendingDeletes \cup live) :
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                        candidates, listedThisCycle, historyAtMark, now,
                        deleteFailed, deleted, pendingPruneBad >>

CommitStagedUploads ==
    /\ stagingActive
    /\ live' = live \cup staged
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << s3, history, futureRoots, pendingDeletes,
                    candidates, listedThisCycle, historyAtMark, now,
                    deleteFailed, deleted, pendingPruneBad >>

ExpireStaging ==
    /\ stagingActive
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
                    candidates, listedThisCycle, historyAtMark, now,
                    deleteFailed, deleted, pendingPruneBad >>

DrainPendingDeleteSuccess ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin history
        /\ k \notin futureRoots
        /\ now >= Horizon
        /\ s3' = s3 \ {k}
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << live, history, futureRoots, staged, stagingActive,
                        listedThisCycle, historyAtMark, now, pendingPruneBad >>

DrainPendingDeleteAbsent ==
    \E k \in pendingDeletes :
        /\ k \notin s3
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ deleteFailed' = deleteFailed \ {k}
        /\ UNCHANGED << s3, live, history, futureRoots, staged,
                        stagingActive, candidates, listedThisCycle,
                        historyAtMark, now, deleted, pendingPruneBad >>

DrainPendingDeleteFailure ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin history
        /\ k \notin futureRoots
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
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
        /\ UNCHANGED << live, history, futureRoots, pendingDeletes, staged,
                        stagingActive, listedThisCycle, historyAtMark, now,
                        pendingPruneBad >>

SweepCandidateDeleteFailure ==
    \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ k \notin GcReachable
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << s3, live, history, futureRoots, pendingDeletes,
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
        /\ UNCHANGED << live, history, futureRoots, pendingDeletes, staged,
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
        /\ UNCHANGED << live, history, futureRoots, pendingDeletes, staged,
                        stagingActive, listedThisCycle, historyAtMark, now,
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

====
