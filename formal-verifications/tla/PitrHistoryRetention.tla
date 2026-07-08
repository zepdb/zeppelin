---- MODULE PitrHistoryRetention ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Generations,
    Artifacts,
    SnapshotNames,
    KeepCount,
    PitrRetention,
    MaxTime,
    AllowBuggyCommit

VARIABLES
    live_gen,
    committed,
    history,
    history_body,
    s3_artifacts,
    pins,
    commit_time,
    now,
    resolved,
    as_of_error,
    gc_deleted,
    pending_gen,
    pending_refs,
    pending_history_written

vars ==
    << live_gen, committed, history, history_body, s3_artifacts,
       pins, commit_time, now, resolved, as_of_error, gc_deleted,
       pending_gen, pending_refs, pending_history_written >>

Zero == 0

GenArtifacts(g) == {g}

NextGeneration ==
    Cardinality(committed) + 1

NewerCommitted(g) ==
    {h \in committed : h > g}

Pinned(g) ==
    \E name \in SnapshotNames : pins[name] = g

KeepByCount(g) ==
    Cardinality(NewerCommitted(g)) < KeepCount

KeepByTime(g) ==
    /\ PitrRetention > 0
    /\ now >= commit_time[g]
    /\ now - commit_time[g] <= PitrRetention

Retained(g) ==
    KeepByCount(g) \/ KeepByTime(g) \/ Pinned(g)

ReachableArtifacts ==
    UNION { history_body[g] : g \in history }

InFlightArtifacts ==
    IF pending_gen = Zero THEN {} ELSE pending_refs

GcProtectedArtifacts ==
    ReachableArtifacts \cup InFlightArtifacts

CandidatesAtOrBefore(t) ==
    {g \in history \cap committed : commit_time[g] <= t}

LatestAtOrBefore(t) ==
    CHOOSE g \in CandidatesAtOrBefore(t) :
        \A h \in CandidatesAtOrBefore(t) : h <= g

Init ==
    /\ live_gen = Zero
    /\ committed = {}
    /\ history = {}
    /\ history_body = [g \in Generations |-> {}]
    /\ s3_artifacts = {}
    /\ pins = [name \in SnapshotNames |-> Zero]
    /\ commit_time = [g \in Generations |-> Zero]
    /\ now = Zero
    /\ resolved = Zero
    /\ as_of_error = FALSE
    /\ gc_deleted = {}
    /\ pending_gen = Zero
    /\ pending_refs = {}
    /\ pending_history_written = FALSE

StartCommitArtifacts ==
    /\ Cardinality(committed) < Cardinality(Generations)
    /\ pending_gen = Zero
    /\ LET g == NextGeneration
           refs == GenArtifacts(g)
       IN
       /\ g \in Generations
       /\ s3_artifacts' = s3_artifacts \cup refs
       /\ pending_gen' = g
       /\ pending_refs' = refs
       /\ pending_history_written' = FALSE
       /\ commit_time' = [commit_time EXCEPT ![g] = now]
       /\ UNCHANGED << live_gen, committed, history, history_body,
                       pins, resolved, as_of_error, gc_deleted, now >>

WriteHistorySnapshot ==
    /\ pending_gen # Zero
    /\ ~pending_history_written
    /\ history' = history \cup {pending_gen}
    /\ history_body' = [history_body EXCEPT ![pending_gen] = pending_refs]
    /\ pending_history_written' = TRUE
    /\ UNCHANGED << live_gen, committed, s3_artifacts, pins,
                    commit_time, now, resolved, as_of_error, gc_deleted,
                    pending_gen, pending_refs >>

PublishLivePointer ==
    /\ pending_gen # Zero
    /\ pending_history_written
    /\ pending_gen \in history
    /\ committed' = committed \cup {pending_gen}
    /\ live_gen' = pending_gen
    /\ pending_gen' = Zero
    /\ pending_refs' = {}
    /\ pending_history_written' = FALSE
    /\ UNCHANGED << history, history_body, s3_artifacts, pins,
                    commit_time, now, resolved, as_of_error, gc_deleted >>

AbortBeforeHistoryWrite ==
    /\ pending_gen # Zero
    /\ ~pending_history_written
    /\ pending_gen' = Zero
    /\ pending_refs' = {}
    /\ pending_history_written' = FALSE
    /\ UNCHANGED << live_gen, committed, history, history_body,
                    s3_artifacts, pins, commit_time, now, resolved,
                    as_of_error, gc_deleted >>

AbortAfterHistoryWrite ==
    /\ pending_gen # Zero
    /\ pending_history_written
    /\ pending_gen' = Zero
    /\ pending_refs' = {}
    /\ pending_history_written' = FALSE
    /\ UNCHANGED << live_gen, committed, history, history_body,
                    s3_artifacts, pins, commit_time, now, resolved,
                    as_of_error, gc_deleted >>

BuggyCommitPointerWithoutHistory ==
    /\ AllowBuggyCommit
    /\ pending_gen # Zero
    /\ ~pending_history_written
    /\ committed' = committed \cup {pending_gen}
    /\ live_gen' = pending_gen
    /\ pending_gen' = Zero
    /\ pending_refs' = {}
    /\ pending_history_written' = FALSE
    /\ UNCHANGED << history, history_body, s3_artifacts, pins,
                    commit_time, now, resolved, as_of_error, gc_deleted >>

CreateSnapshotPin ==
    \E name \in SnapshotNames :
        /\ pins[name] = Zero
        /\ live_gen # Zero
        /\ pins' = [pins EXCEPT ![name] = live_gen]
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, commit_time, now, resolved,
                        as_of_error, gc_deleted, pending_gen, pending_refs,
                        pending_history_written >>

DeleteSnapshotPin ==
    \E name \in SnapshotNames :
        /\ pins[name] # Zero
        /\ pins' = [pins EXCEPT ![name] = Zero]
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, commit_time, now, resolved,
                        as_of_error, gc_deleted, pending_gen, pending_refs,
                        pending_history_written >>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED << live_gen, committed, history, history_body,
                    s3_artifacts, pins, commit_time, resolved,
                    as_of_error, gc_deleted, pending_gen, pending_refs,
                    pending_history_written >>

PruneHistory ==
    /\ history' = {g \in history : Retained(g)}
    /\ resolved' = Zero
    /\ as_of_error' = FALSE
    /\ UNCHANGED << live_gen, committed, history_body, s3_artifacts,
                    pins, commit_time, now, gc_deleted, pending_gen,
                    pending_refs, pending_history_written >>

ResolveAsOfGeneration ==
    \E target \in Generations :
        /\ IF target \in history /\ target \in committed
           THEN
               /\ resolved' = target
               /\ as_of_error' = FALSE
           ELSE
               /\ resolved' = Zero
               /\ as_of_error' = TRUE
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, pins, commit_time, now, gc_deleted,
                        pending_gen, pending_refs, pending_history_written >>

ResolveAsOfTimestamp ==
    \E target_time \in 0..MaxTime :
        /\ IF CandidatesAtOrBefore(target_time) # {}
           THEN
               /\ resolved' = LatestAtOrBefore(target_time)
               /\ as_of_error' = FALSE
           ELSE
               /\ resolved' = Zero
               /\ as_of_error' = TRUE
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, pins, commit_time, now, gc_deleted,
                        pending_gen, pending_refs, pending_history_written >>

ResolveAsOfSnapshot ==
    \E name \in SnapshotNames :
        /\ IF pins[name] # Zero /\ pins[name] \in history
           THEN
               /\ resolved' = pins[name]
               /\ as_of_error' = FALSE
           ELSE
               /\ resolved' = Zero
               /\ as_of_error' = TRUE
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, pins, commit_time, now, gc_deleted,
                        pending_gen, pending_refs, pending_history_written >>

GcSweep ==
    \E dead \in s3_artifacts \ GcProtectedArtifacts :
        /\ s3_artifacts' = s3_artifacts \ {dead}
        /\ gc_deleted' = gc_deleted \cup {dead}
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        pins, commit_time, now, resolved, as_of_error,
                        pending_gen, pending_refs, pending_history_written >>

Next ==
    \/ StartCommitArtifacts
    \/ WriteHistorySnapshot
    \/ PublishLivePointer
    \/ AbortBeforeHistoryWrite
    \/ AbortAfterHistoryWrite
    \/ BuggyCommitPointerWithoutHistory
    \/ CreateSnapshotPin
    \/ DeleteSnapshotPin
    \/ AdvanceTime
    \/ PruneHistory
    \/ ResolveAsOfGeneration
    \/ ResolveAsOfTimestamp
    \/ ResolveAsOfSnapshot
    \/ GcSweep

Spec ==
    Init /\ [][Next]_vars

LiveHasHistory ==
    live_gen = Zero \/ live_gen \in history

PinnedGenerationRetained ==
    \A name \in SnapshotNames :
        pins[name] = Zero \/ pins[name] \in history

AsOfNeverFallsBackToHead ==
    /\ as_of_error => resolved = Zero
    /\ resolved # Zero => resolved \in history

AsOfReturnsCommittedGeneration ==
    resolved = Zero \/ resolved \in committed

GcPreservesRetainedArtifacts ==
    ReachableArtifacts \subseteq s3_artifacts

GcPreservesInFlightArtifacts ==
    InFlightArtifacts \subseteq s3_artifacts

PrunedGenerationUnavailable ==
    resolved # Zero => resolved \in history

TypeOK ==
    /\ live_gen \in {Zero} \cup Generations
    /\ committed \subseteq Generations
    /\ history \subseteq Generations
    /\ live_gen = Zero \/ live_gen \in committed
    /\ history_body \in [Generations -> SUBSET Artifacts]
    /\ \A g \in Generations : history_body[g] = {} \/ history_body[g] = GenArtifacts(g)
    /\ s3_artifacts \subseteq Artifacts
    /\ pins \in [SnapshotNames -> ({Zero} \cup Generations)]
    /\ commit_time \in [Generations -> 0..MaxTime]
    /\ now \in 0..MaxTime
    /\ resolved \in {Zero} \cup Generations
    /\ as_of_error \in BOOLEAN
    /\ gc_deleted \subseteq Artifacts
    /\ pending_gen \in {Zero} \cup Generations
    /\ pending_refs \subseteq Artifacts
    /\ pending_history_written \in BOOLEAN
    /\ pending_gen = Zero => pending_refs = {}

====
