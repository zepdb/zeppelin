---- MODULE IncrementalArtifactClosure ----

EXTENDS Naturals, FiniteSets

CONSTANTS
    Segments,
    Clusters,
    AllowBuggyPrefixGc

VARIABLES
    liveSegments,
    historySegments,
    owner,
    dataKey,
    explicitKeys,
    s3,
    pendingDeletes,
    gcDeleted,
    oldDropped,
    touchedClusters

vars ==
    << liveSegments, historySegments, owner, dataKey, explicitKeys,
       s3, pendingDeletes, gcDeleted, oldDropped, touchedClusters >>

KeyUniverse ==
    {"s1_centroids", "s1_sketch", "s1_bootstrap", "s1_membership",
     "s1_group", "s1_attrs0", "s1_attrs1",
     "s2_centroids", "s2_sketch", "s2_bootstrap", "s2_membership",
     "s2_group0", "s2_group1", "s2_attrs0", "s2_attrs1"}

MetaKeys(seg) ==
    CASE seg = "s1" ->
            {"s1_centroids", "s1_sketch", "s1_bootstrap", "s1_membership"}
      [] seg = "s2" ->
            {"s2_centroids", "s2_sketch", "s2_bootstrap", "s2_membership"}
      [] OTHER -> {}

AttrKey(seg, c) ==
    CASE seg = "s1" /\ c = 0 -> "s1_attrs0"
      [] seg = "s1" /\ c = 1 -> "s1_attrs1"
      [] seg = "s2" /\ c = 0 -> "s2_attrs0"
      [] seg = "s2" /\ c = 1 -> "s2_attrs1"
      [] OTHER -> "unknown"

NewGroupKey(c) ==
    CASE c = 0 -> "s2_group0"
      [] c = 1 -> "s2_group1"
      [] OTHER -> "unknown"

SegmentPrefix(k) ==
    CASE k \in {"s1_centroids", "s1_sketch", "s1_bootstrap",
                "s1_membership", "s1_group", "s1_attrs0", "s1_attrs1"}
            -> "s1"
      [] k \in {"s2_centroids", "s2_sketch", "s2_bootstrap",
                "s2_membership", "s2_group0", "s2_group1",
                "s2_attrs0", "s2_attrs1"}
            -> "s2"
      [] OTHER -> "unknown"

SegmentClusterKeys(seg) ==
    UNION {{dataKey[seg][c], AttrKey(owner[seg][c], c)} : c \in Clusters}

SegmentReachable(seg) ==
    explicitKeys[seg] \cup SegmentClusterKeys(seg)

ManifestReachable ==
    UNION {SegmentReachable(seg) : seg \in liveSegments \cup historySegments}
        \cup pendingDeletes

InitialOwner ==
    [seg \in Segments |->
        [c \in Clusters |-> seg]]

InitialDataKey ==
    [seg \in Segments |->
        [c \in Clusters |->
            IF seg = "s1" THEN "s1_group" ELSE NewGroupKey(c)]]

InitialExplicitKeys ==
    [seg \in Segments |-> MetaKeys(seg)]

Init ==
    /\ liveSegments = {"s1"}
    /\ historySegments = {}
    /\ owner = InitialOwner
    /\ dataKey = InitialDataKey
    /\ explicitKeys = InitialExplicitKeys
    /\ s3 = SegmentReachable("s1")
    /\ pendingDeletes = {}
    /\ gcDeleted = {}
    /\ oldDropped = FALSE
    /\ touchedClusters = {}

IncrementalCompactTouchCluster ==
    /\ "s1" \in liveSegments
    /\ "s2" \notin liveSegments
    /\ LET newOwner ==
               [owner EXCEPT !["s2"] =
                   [c \in Clusters |->
                       IF c = 0 THEN "s2" ELSE owner["s1"][c]]]
           newDataKey ==
               [dataKey EXCEPT !["s2"] =
                   [c \in Clusters |->
                       IF c = 0 THEN NewGroupKey(0) ELSE dataKey["s1"][c]]]
           newExplicitKeys ==
               [explicitKeys EXCEPT !["s2"] = MetaKeys("s2")]
           s2Reachable ==
               newExplicitKeys["s2"]
                   \cup UNION {{newDataKey["s2"][c],
                                AttrKey(newOwner["s2"][c], c)}
                               : c \in Clusters}
       IN
       /\ owner' = newOwner
       /\ dataKey' = newDataKey
       /\ explicitKeys' = newExplicitKeys
       /\ s3' = s3 \cup s2Reachable
       /\ liveSegments' = liveSegments \cup {"s2"}
       /\ touchedClusters' = {0}
    /\ UNCHANGED << historySegments, pendingDeletes, gcDeleted, oldDropped >>

DropOldSegmentRef ==
    /\ "s2" \in liveSegments
    /\ "s1" \in liveSegments
    /\ liveSegments' = liveSegments \ {"s1"}
    /\ oldDropped' = TRUE
    /\ UNCHANGED << historySegments, owner, dataKey, explicitKeys, s3,
                    pendingDeletes, gcDeleted, touchedClusters >>

RetainHistory ==
    /\ "s1" \notin historySegments
    /\ gcDeleted = {}
    /\ historySegments' = historySegments \cup {"s1"}
    /\ UNCHANGED << liveSegments, owner, dataKey, explicitKeys, s3,
                    pendingDeletes, gcDeleted, oldDropped, touchedClusters >>

PruneHistory ==
    /\ historySegments # {}
    /\ historySegments' = {}
    /\ UNCHANGED << liveSegments, owner, dataKey, explicitKeys, s3,
                    pendingDeletes, gcDeleted, oldDropped, touchedClusters >>

GcExactReachability ==
    \E k \in s3 \ ManifestReachable :
        /\ s3' = s3 \ {k}
        /\ gcDeleted' = gcDeleted \cup {k}
        /\ UNCHANGED << liveSegments, historySegments, owner, dataKey,
                        explicitKeys, pendingDeletes, oldDropped,
                        touchedClusters >>

BuggyPrefixGc ==
    /\ AllowBuggyPrefixGc
    /\ oldDropped
    /\ "s1" \notin liveSegments
    /\ \E k \in s3 :
        /\ SegmentPrefix(k) = "s1"
        /\ s3' = s3 \ {k}
        /\ gcDeleted' = gcDeleted \cup {k}
        /\ UNCHANGED << liveSegments, historySegments, owner, dataKey,
                        explicitKeys, pendingDeletes, oldDropped,
                        touchedClusters >>

Stutter ==
    UNCHANGED vars

Next ==
    \/ IncrementalCompactTouchCluster
    \/ DropOldSegmentRef
    \/ RetainHistory
    \/ PruneHistory
    \/ GcExactReachability
    \/ BuggyPrefixGc
    \/ Stutter

Spec ==
    Init /\ [][Next]_vars

ManifestReachableArtifactsExist ==
    ManifestReachable \subseteq s3

CarriedObjectsSurviveOldSegmentDrop ==
    oldDropped /\ "s2" \in liveSegments =>
        \A c \in Clusters :
            owner["s2"][c] = "s1" =>
                /\ dataKey["s2"][c] \in s3
                /\ AttrKey("s1", c) \in s3

GcDoesNotUseSegmentPrefixAsTruth ==
    gcDeleted \cap ManifestReachable = {}

RetainedHistoryPinsOldClosure ==
    "s1" \in historySegments => SegmentReachable("s1") \subseteq s3

TouchedClusterRewritten ==
    "s2" \in liveSegments =>
        /\ 0 \in touchedClusters
        /\ owner["s2"][0] = "s2"
        /\ dataKey["s2"][0] = "s2_group0"

TypeOK ==
    /\ liveSegments \subseteq Segments
    /\ historySegments \subseteq Segments
    /\ owner \in [Segments -> [Clusters -> Segments]]
    /\ dataKey \in [Segments -> [Clusters -> KeyUniverse]]
    /\ explicitKeys \in [Segments -> SUBSET KeyUniverse]
    /\ s3 \subseteq KeyUniverse
    /\ pendingDeletes \subseteq KeyUniverse
    /\ gcDeleted \subseteq KeyUniverse
    /\ oldDropped \in BOOLEAN
    /\ touchedClusters \subseteq Clusters
    /\ Segments = {"s1", "s2"}
    /\ Clusters = {0, 1}

====
