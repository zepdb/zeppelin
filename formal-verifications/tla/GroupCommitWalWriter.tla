---- MODULE GroupCommitWalWriter ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Writers,
    Fragments,
    Tokens,
    MaxEtag,
    AllowBuggyMixedTokenDeadlock

VARIABLES
    namespaceState,
    manifestExists,
    manifestFrags,
    s3Frags,
    committed,
    failed,
    queue,
    leader,
    leaderStalled,
    reply,
    pc,
    manifestEtag,
    manifestFencing,
    lastBatch,
    lastBatchResult,
    deletedOnce,
    zombieCommitted

vars ==
    << namespaceState, manifestExists, manifestFrags, s3Frags, committed,
       failed, queue, leader, leaderStalled, reply, pc, manifestEtag,
       manifestFencing, lastBatch, lastBatchResult, deletedOnce,
       zombieCommitted >>

FragOf(w) ==
    CASE w = "w1" -> "f1"
      [] w = "w2" -> "f2"
      [] w = "w3" -> "f3"
      [] OTHER -> "unknown-fragment"

TokenOf(w) ==
    CASE w = "w1" -> 1
      [] w = "w2" -> 2
      [] w = "w3" -> 1
      [] OTHER -> 0

BatchFor(t) ==
    {w \in queue : TokenOf(w) = t}

FragsOf(ws) ==
    {FragOf(w) : w \in ws}

AllDone ==
    /\ queue = {}
    /\ leader = "none"
    /\ \A w \in Writers : pc[w] = "done"

Init ==
    /\ namespaceState = "live"
    /\ manifestExists = TRUE
    /\ manifestFrags = {}
    /\ s3Frags = {}
    /\ committed = {}
    /\ failed = {}
    /\ queue = {}
    /\ leader = "none"
    /\ leaderStalled = FALSE
    /\ reply = [w \in Writers |-> "none"]
    /\ pc = [w \in Writers |-> "idle"]
    /\ manifestEtag = 0
    /\ manifestFencing = 0
    /\ lastBatch = {}
    /\ lastBatchResult = "none"
    /\ deletedOnce = FALSE
    /\ zombieCommitted = FALSE

UploadFragment ==
    \E w \in Writers :
        /\ pc[w] = "idle"
        /\ s3Frags' = s3Frags \cup {FragOf(w)}
        /\ pc' = [pc EXCEPT ![w] = "uploaded"]
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        committed, failed, queue, leader, leaderStalled,
                        reply, manifestEtag, manifestFencing, lastBatch,
                        lastBatchResult, deletedOnce, zombieCommitted >>

EnqueueAppend ==
    \E w \in Writers :
        /\ pc[w] = "uploaded"
        /\ queue' = queue \cup {w}
        /\ pc' = [pc EXCEPT ![w] = "queued"]
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        s3Frags, committed, failed, leader, leaderStalled,
                        reply, manifestEtag, manifestFencing, lastBatch,
                        lastBatchResult, deletedOnce, zombieCommitted >>

ElectLeader ==
    /\ leader = "none"
    /\ queue # {}
    /\ \E w \in queue :
        /\ leader' = w
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        s3Frags, committed, failed, queue, leaderStalled,
                        reply, pc, manifestEtag, manifestFencing,
                        lastBatch, lastBatchResult, deletedOnce,
                        zombieCommitted >>

CommitCompatibleBatch ==
    /\ leader # "none"
    /\ ~leaderStalled
    /\ namespaceState = "live"
    /\ manifestExists
    /\ manifestEtag < MaxEtag
    /\ \E t \in Tokens :
        LET batch == BatchFor(t)
            frags == FragsOf(batch)
        IN
        /\ batch # {}
        /\ manifestFencing <= t
        /\ manifestFrags' = manifestFrags \cup frags
        /\ committed' = committed \cup frags
        /\ queue' = queue \ batch
        /\ reply' = [w \in Writers |->
                        IF w \in batch THEN "ok" ELSE reply[w]]
        /\ pc' = [w \in Writers |->
                    IF w \in batch THEN "done" ELSE pc[w]]
        /\ manifestFencing' = t
        /\ manifestEtag' = manifestEtag + 1
        /\ lastBatch' = frags
        /\ lastBatchResult' = "ok"
        /\ leader' = IF leader \in batch THEN "none" ELSE leader
        /\ UNCHANGED << namespaceState, manifestExists, s3Frags, failed,
                        leaderStalled, deletedOnce, zombieCommitted >>

FailBatchMissingManifest ==
    /\ leader # "none"
    /\ ~leaderStalled
    /\ \/ namespaceState = "deleted"
       \/ ~manifestExists
    /\ \E t \in Tokens :
        LET batch == BatchFor(t)
            frags == FragsOf(batch)
        IN
        /\ batch # {}
        /\ s3Frags' = s3Frags \ frags
        /\ failed' = failed \cup frags
        /\ queue' = queue \ batch
        /\ reply' = [w \in Writers |->
                        IF w \in batch THEN "err" ELSE reply[w]]
        /\ pc' = [w \in Writers |->
                    IF w \in batch THEN "done" ELSE pc[w]]
        /\ lastBatch' = frags
        /\ lastBatchResult' = "err"
        /\ leader' = IF leader \in batch THEN "none" ELSE leader
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        committed, leaderStalled, manifestEtag,
                        manifestFencing, deletedOnce, zombieCommitted >>

FailBatchStaleFence ==
    /\ leader # "none"
    /\ ~leaderStalled
    /\ namespaceState = "live"
    /\ manifestExists
    /\ \E t \in Tokens :
        LET batch == BatchFor(t)
            frags == FragsOf(batch)
        IN
        /\ batch # {}
        /\ manifestFencing > t
        /\ s3Frags' = s3Frags \ frags
        /\ failed' = failed \cup frags
        /\ queue' = queue \ batch
        /\ reply' = [w \in Writers |->
                        IF w \in batch THEN "err" ELSE reply[w]]
        /\ pc' = [w \in Writers |->
                    IF w \in batch THEN "done" ELSE pc[w]]
        /\ lastBatch' = frags
        /\ lastBatchResult' = "err"
        /\ leader' = IF leader \in batch THEN "none" ELSE leader
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        committed, leaderStalled, manifestEtag,
                        manifestFencing, deletedOnce, zombieCommitted >>

ExternalManifestAdvance ==
    /\ leader # "none"
    /\ ~leaderStalled
    /\ namespaceState = "live"
    /\ manifestExists
    /\ manifestEtag < MaxEtag
    /\ \E t \in Tokens :
        /\ t > manifestFencing
        /\ manifestFencing' = t
        /\ manifestEtag' = manifestEtag + 1
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        s3Frags, committed, failed, queue, leader,
                        leaderStalled, reply, pc, lastBatch,
                        lastBatchResult, deletedOnce, zombieCommitted >>

DeleteNamespace ==
    /\ namespaceState = "live"
    /\ namespaceState' = "deleted"
    /\ manifestExists' = FALSE
    /\ s3Frags' = {}
    /\ deletedOnce' = TRUE
    /\ UNCHANGED << manifestFrags, committed, failed, queue, leader,
                    leaderStalled, reply, pc, manifestEtag,
                    manifestFencing, lastBatch, lastBatchResult,
                    zombieCommitted >>

BuggyMixedTokenDeadlock ==
    /\ AllowBuggyMixedTokenDeadlock
    /\ leader # "none"
    /\ ~leaderStalled
    /\ \E w \in queue :
        /\ TokenOf(w) # TokenOf(leader)
        /\ leaderStalled' = TRUE
        /\ UNCHANGED << namespaceState, manifestExists, manifestFrags,
                        s3Frags, committed, failed, queue, leader,
                        reply, pc, manifestEtag, manifestFencing,
                        lastBatch, lastBatchResult, deletedOnce,
                        zombieCommitted >>

TerminalStutter ==
    /\ AllDone
    /\ UNCHANGED vars

Next ==
    \/ UploadFragment
    \/ EnqueueAppend
    \/ ElectLeader
    \/ CommitCompatibleBatch
    \/ FailBatchMissingManifest
    \/ FailBatchStaleFence
    \/ ExternalManifestAdvance
    \/ DeleteNamespace
    \/ BuggyMixedTokenDeadlock
    \/ TerminalStutter

Spec ==
    Init /\ [][Next]_vars

NoCommittedFragmentLost ==
    committed \subseteq manifestFrags

FailedAppendLeavesNoOrphan ==
    \A f \in failed : f \notin s3Frags \/ f \in manifestFrags

DeletedNamespaceNotResurrected ==
    deletedOnce => namespaceState = "deleted" /\ ~manifestExists

FencingPreventsZombieCommit ==
    zombieCommitted = FALSE

GroupCommitAtomicPerBatch ==
    lastBatch = {}
    \/ /\ lastBatchResult = "ok"
       /\ lastBatch \subseteq committed
       /\ lastBatch \subseteq manifestFrags
    \/ /\ lastBatchResult = "err"
       /\ lastBatch \subseteq failed
       /\ lastBatch \cap committed = {}

NoMixedTokenLeaderDeadlock ==
    leaderStalled = FALSE

TypeOK ==
    /\ namespaceState \in {"live", "deleted"}
    /\ manifestExists \in BOOLEAN
    /\ manifestFrags \subseteq Fragments
    /\ s3Frags \subseteq Fragments
    /\ committed \subseteq Fragments
    /\ failed \subseteq Fragments
    /\ queue \subseteq Writers
    /\ leader \in Writers \cup {"none"}
    /\ leaderStalled \in BOOLEAN
    /\ reply \in [Writers -> {"none", "ok", "err"}]
    /\ pc \in [Writers -> {"idle", "uploaded", "queued", "done"}]
    /\ manifestEtag \in 0..MaxEtag
    /\ manifestFencing \in Tokens
    /\ lastBatch \subseteq Fragments
    /\ lastBatchResult \in {"none", "ok", "err"}
    /\ deletedOnce \in BOOLEAN
    /\ zombieCommitted \in BOOLEAN
    /\ Writers = {"w1", "w2", "w3"}
    /\ Fragments = {"f1", "f2", "f3"}
    /\ Tokens = {0, 1, 2}

====
