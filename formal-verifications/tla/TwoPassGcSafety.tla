---- MODULE TwoPassGcSafety ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,
    Horizon,
    MaxTime,
    InitialLive,
    InitialHistory,
    InitialPendingDeletes,
    AllowBuggySweepWithoutRevalidate

VARIABLES
    s3,
    live,
    history,
    pendingDeletes,
    staged,
    stagingActive,
    candidates,
    listedThisCycle,
    now,
    deleteFailed,
    deleted,
    pendingPruneBad

vars ==
    << s3, live, history, pendingDeletes, staged, stagingActive,
       candidates, listedThisCycle, now, deleteFailed, deleted,
       pendingPruneBad >>

NoCandidate == MaxTime + 1

ActiveStaged ==
    IF stagingActive THEN staged ELSE {}

ClientReachable ==
    live \cup history \cup ActiveStaged

\* Production mark/sweep reachability also includes manifest pending_deletes.
\* Pending-delete drain is the only path allowed to physically remove them.
GcReachable ==
    ClientReachable \cup pendingDeletes

Candidate(k) ==
    candidates[k] # NoCandidate

CandidateEligible(k) ==
    /\ Candidate(k)
    /\ now >= candidates[k] + Horizon

Init ==
    /\ s3 = InitialLive \cup InitialHistory \cup InitialPendingDeletes
    /\ live = InitialLive
    /\ history = InitialHistory
    /\ pendingDeletes = InitialPendingDeletes
    /\ staged = {}
    /\ stagingActive = FALSE
    /\ candidates = [k \in Keys |-> NoCandidate]
    /\ listedThisCycle = {}
    /\ now = 0
    /\ deleteFailed = {}
    /\ deleted = {}
    /\ pendingPruneBad = FALSE

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                    stagingActive, candidates, listedThisCycle,
                    deleteFailed, deleted, pendingPruneBad >>

StartCycle ==
    /\ listedThisCycle' = s3
    /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                    stagingActive, candidates, now, deleteFailed,
                    deleted, pendingPruneBad >>

MarkCandidates ==
    LET reachable == GcReachable
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
    /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                    stagingActive, listedThisCycle, now, deleted,
                    pendingPruneBad >>

MakeLive ==
    \E k \in s3 :
        /\ live' = live \cup {k}
        /\ UNCHANGED << s3, history, pendingDeletes, staged, stagingActive,
                        candidates, listedThisCycle, now, deleteFailed,
                        deleted, pendingPruneBad >>

DropLive ==
    \E k \in live :
        /\ live' = live \ {k}
        /\ UNCHANGED << s3, history, pendingDeletes, staged, stagingActive,
                        candidates, listedThisCycle, now, deleteFailed,
                        deleted, pendingPruneBad >>

AddHistoryRoot ==
    \E k \in live :
        /\ history' = history \cup {k}
        /\ UNCHANGED << s3, live, pendingDeletes, staged, stagingActive,
                        candidates, listedThisCycle, now, deleteFailed,
                        deleted, pendingPruneBad >>

PruneHistoryRoots ==
    \E kept \in SUBSET history :
        /\ history' = kept
        /\ UNCHANGED << s3, live, pendingDeletes, staged, stagingActive,
                        candidates, listedThisCycle, now, deleteFailed,
                        deleted, pendingPruneBad >>

StageNewUpload ==
    \E k \in Keys \ (s3 \cup deleted) :
        /\ s3' = s3 \cup {k}
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << live, history, pendingDeletes, candidates,
                        listedThisCycle, now, deleteFailed, deleted,
                        pendingPruneBad >>

StageExistingObject ==
    \E k \in s3 :
        /\ staged' = staged \cup {k}
        /\ stagingActive' = TRUE
        /\ UNCHANGED << s3, live, history, pendingDeletes, candidates,
                        listedThisCycle, now, deleteFailed, deleted,
                        pendingPruneBad >>

CommitStagedUploads ==
    /\ stagingActive
    /\ live' = live \cup staged
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << s3, history, pendingDeletes, candidates,
                    listedThisCycle, now, deleteFailed, deleted,
                    pendingPruneBad >>

ExpireStaging ==
    /\ stagingActive
    /\ staged' = {}
    /\ stagingActive' = FALSE
    /\ UNCHANGED << s3, live, history, pendingDeletes, candidates,
                    listedThisCycle, now, deleteFailed, deleted,
                    pendingPruneBad >>

DrainPendingDeleteSuccess ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin ClientReachable
        /\ now >= Horizon
        /\ s3' = s3 \ {k}
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ candidates' = [candidates EXCEPT ![k] = NoCandidate]
        /\ deleteFailed' = deleteFailed \ {k}
        /\ deleted' = deleted \cup {k}
        /\ UNCHANGED << live, history, staged, stagingActive,
                        listedThisCycle, now, pendingPruneBad >>

DrainPendingDeleteAbsent ==
    \E k \in pendingDeletes :
        /\ k \notin s3
        /\ pendingDeletes' = pendingDeletes \ {k}
        /\ deleteFailed' = deleteFailed \ {k}
        /\ UNCHANGED << s3, live, history, staged, stagingActive,
                        candidates, listedThisCycle, now, deleted,
                        pendingPruneBad >>

DrainPendingDeleteFailure ==
    \E k \in pendingDeletes :
        /\ k \in s3
        /\ k \notin ClientReachable
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle, now,
                        deleted, pendingPruneBad >>

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
        /\ UNCHANGED << live, history, pendingDeletes, staged,
                        stagingActive, listedThisCycle, now,
                        pendingPruneBad >>

SweepCandidateDeleteFailure ==
    \E k \in Keys :
        /\ CandidateEligible(k)
        /\ k \in listedThisCycle
        /\ k \in s3
        /\ k \notin GcReachable
        /\ deleteFailed' = deleteFailed \cup {k}
        /\ UNCHANGED << s3, live, history, pendingDeletes, staged,
                        stagingActive, candidates, listedThisCycle, now,
                        deleted, pendingPruneBad >>

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
        /\ UNCHANGED << live, history, pendingDeletes, staged,
                        stagingActive, listedThisCycle, now,
                        pendingPruneBad >>

Next ==
    \/ AdvanceTime
    \/ StartCycle
    \/ MarkCandidates
    \/ MakeLive
    \/ DropLive
    \/ AddHistoryRoot
    \/ PruneHistoryRoots
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

FailedDeletesRemainRetriable ==
    \A k \in deleteFailed :
        /\ k \in s3
        /\ k \in pendingDeletes \/ Candidate(k)

TypeOK ==
    /\ s3 \subseteq Keys
    /\ live \subseteq Keys
    /\ history \subseteq Keys
    /\ pendingDeletes \subseteq Keys
    /\ staged \subseteq Keys
    /\ stagingActive \in BOOLEAN
    /\ candidates \in [Keys -> 0..NoCandidate]
    /\ listedThisCycle \subseteq Keys
    /\ now \in 0..MaxTime
    /\ deleteFailed \subseteq Keys
    /\ deleted \subseteq Keys
    /\ pendingPruneBad \in BOOLEAN
    /\ InitialLive \subseteq Keys
    /\ InitialHistory \subseteq Keys
    /\ InitialPendingDeletes \subseteq Keys

====
