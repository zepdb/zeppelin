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
    gc_deleted

vars ==
    << live_gen, committed, history, history_body, s3_artifacts,
       pins, commit_time, now, resolved, as_of_error, gc_deleted >>

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

CandidatesAtOrBefore(t) ==
    {g \in history : commit_time[g] <= t}

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

Commit ==
    /\ Cardinality(committed) < Cardinality(Generations)
    /\ LET g == NextGeneration
           refs == GenArtifacts(g)
       IN
       /\ g \in Generations
       /\ committed' = committed \cup {g}
       /\ history' = history \cup {g}
       /\ history_body' = [history_body EXCEPT ![g] = refs]
       /\ s3_artifacts' = s3_artifacts \cup refs
       /\ commit_time' = [commit_time EXCEPT ![g] = now]
       /\ live_gen' = g
       /\ UNCHANGED << pins, resolved, as_of_error, gc_deleted, now >>

BuggyCommitPointerWithoutHistory ==
    /\ AllowBuggyCommit
    /\ Cardinality(committed) < Cardinality(Generations)
    /\ LET g == NextGeneration
           refs == GenArtifacts(g)
       IN
       /\ g \in Generations
       /\ committed' = committed \cup {g}
       /\ history' = history
       /\ history_body' = history_body
       /\ s3_artifacts' = s3_artifacts \cup refs
       /\ commit_time' = [commit_time EXCEPT ![g] = now]
       /\ live_gen' = g
       /\ UNCHANGED << pins, resolved, as_of_error, gc_deleted, now >>

CreateSnapshotPin ==
    \E name \in SnapshotNames, g \in history :
        /\ pins[name] = Zero
        /\ pins' = [pins EXCEPT ![name] = g]
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, commit_time, now, resolved,
                        as_of_error, gc_deleted >>

DeleteSnapshotPin ==
    \E name \in SnapshotNames :
        /\ pins[name] # Zero
        /\ pins' = [pins EXCEPT ![name] = Zero]
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, commit_time, now, resolved,
                        as_of_error, gc_deleted >>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED << live_gen, committed, history, history_body,
                    s3_artifacts, pins, commit_time, resolved,
                    as_of_error, gc_deleted >>

PruneHistory ==
    /\ history' = {g \in history : Retained(g)}
    /\ resolved' = Zero
    /\ as_of_error' = FALSE
    /\ UNCHANGED << live_gen, committed, history_body, s3_artifacts,
                    pins, commit_time, now, gc_deleted >>

ResolveAsOfGeneration ==
    \E target \in Generations :
        /\ IF target \in history
           THEN
               /\ resolved' = target
               /\ as_of_error' = FALSE
           ELSE
               /\ resolved' = Zero
               /\ as_of_error' = TRUE
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        s3_artifacts, pins, commit_time, now, gc_deleted >>

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
                        s3_artifacts, pins, commit_time, now, gc_deleted >>

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
                        s3_artifacts, pins, commit_time, now, gc_deleted >>

GcSweep ==
    \E dead \in s3_artifacts \ ReachableArtifacts :
        /\ s3_artifacts' = s3_artifacts \ {dead}
        /\ gc_deleted' = gc_deleted \cup {dead}
        /\ UNCHANGED << live_gen, committed, history, history_body,
                        pins, commit_time, now, resolved, as_of_error >>

Next ==
    \/ Commit
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

GcPreservesRetainedArtifacts ==
    ReachableArtifacts \subseteq s3_artifacts

PrunedGenerationUnavailable ==
    resolved # Zero => resolved \in history

TypeOK ==
    /\ live_gen \in {Zero} \cup Generations
    /\ committed \subseteq Generations
    /\ history \subseteq committed
    /\ history_body \in [Generations -> SUBSET Artifacts]
    /\ \A g \in Generations : history_body[g] = {} \/ history_body[g] = GenArtifacts(g)
    /\ s3_artifacts \subseteq Artifacts
    /\ pins \in [SnapshotNames -> ({Zero} \cup Generations)]
    /\ commit_time \in [Generations -> 0..MaxTime]
    /\ now \in 0..MaxTime
    /\ resolved \in {Zero} \cup Generations
    /\ as_of_error \in BOOLEAN
    /\ gc_deleted \subseteq Artifacts

====
