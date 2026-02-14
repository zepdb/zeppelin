---------------------- MODULE ManifestCacheConsistency ----------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS 
    Namespaces,
    MaxVersion

VARIABLES 
    s3_manifest_version,  \* The version of the manifest in S3 (monotonic)
    cache_version,        \* The version stored in the local cache (-1 if empty)
    last_invalidated_ts,  \* Timestamp of last invalidation
    wall_clock            \* Global wall clock

Vars == <<s3_manifest_version, cache_version, last_invalidated_ts, wall_clock>>

Init ==
    /\ s3_manifest_version = [n \in Namespaces |-> 1]
    /\ cache_version = [n \in Namespaces |-> 0] \* 0 means empty/miss
    /\ last_invalidated_ts = [n \in Namespaces |-> 0]
    /\ wall_clock = 1

\* Action: A write happens. Updates S3.
Write(n) ==
    /\ s3_manifest_version[n] < MaxVersion
    /\ s3_manifest_version' = [s3_manifest_version EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<cache_version, last_invalidated_ts, wall_clock>>

\* Action: Background Compaction finishes.
\* 1. Updates S3 (new segment).
\* 2. Invalidates Cache.
Compact(n) ==
    /\ s3_manifest_version[n] < MaxVersion
    /\ s3_manifest_version' = [s3_manifest_version EXCEPT ![n] = @ + 1]
    /\ last_invalidated_ts' = [last_invalidated_ts EXCEPT ![n] = wall_clock]
    /\ cache_version' = [cache_version EXCEPT ![n] = 0] \* Evict
    /\ wall_clock' = wall_clock + 1

\* Action: Write-Through Cache Insert.
\* This simulates the `upsert_vectors` handler inserting the manifest it just wrote.
\* PROBLEM: It uses a local timestamp check.
WriteThrough(n, version) ==
    /\ version = s3_manifest_version[n] \* We are inserting what is currently in S3 (or what we think is)
    \* The logic in Rust: if manifest.updated_at <= last_invalidated, return.
    \* We simulate "updated_at" as the current wall_clock when the write happened.
    \* But there's a race: The write happened at `wall_clock`, but `Compact` might have bumped `last_invalidated_ts`.
    /\ IF wall_clock > last_invalidated_ts[n] 
       THEN cache_version' = [cache_version EXCEPT ![n] = version]
       ELSE UNCHANGED cache_version
    /\ UNCHANGED <<s3_manifest_version, last_invalidated_ts, wall_clock>>

\* Action: Reader fetches from S3 if cache miss
ReadMiss(n) ==
    /\ cache_version[n] = 0
    /\ cache_version' = [cache_version EXCEPT ![n] = s3_manifest_version[n]]
    /\ UNCHANGED <<s3_manifest_version, last_invalidated_ts, wall_clock>>

\* Check: Cache should never hold a version older than what was known at invalidation time?
\* Actually, the invariant is: The cache should typically track S3. 
\* The specific bug is: Can we overwrite a generic invalidation with an OLDER write-through?
\* Let's say:
\* T1: Write(v2)
\* T2: Compact(v3) -> Invalidate(ts=2)
\* T3: WriteThrough(v2) -> check: write_time(1) <= invalid_time(2)?
\* If we don't model the timestamps precisely, we miss the bug.

\* Let's model the specific race:
\* Writer writes v2.
\* Compactor writes v3, invalidates cache.
\* Writer (slow) tries to cache v2.
\* If Writer thinks "now" is later than invalidation, it overwrites v3-cleared cache with v2.

Next ==
    \/ \E n \in Namespaces : Write(n)
    \/ \E n \in Namespaces : Compact(n)
    \/ \E n \in Namespaces : WriteThrough(n, s3_manifest_version[n]) \* Simplified: inserting current S3
    \/ \E n \in Namespaces : ReadMiss(n)
    \/ wall_clock' = wall_clock + 1 /\ UNCHANGED <<s3_manifest_version, cache_version, last_invalidated_ts>>

\* Invariant: Cache should not be stale relative to S3 IF it claims to be fresh.
\* Specifically, if the cache has a value, it shouldn't be drastically older than S3 if invalidation happened.
\* A strong invariant: cache_version[n] <= s3_manifest_version[n] (Always true physically)
\* But also: If cache_version[n] > 0, it should not be "invalidated".
\* Actually, the bug is "Lost Update" or "Stale Read".
\* If S3 has v3 (compacted), and Cache has v2 (uncompacted), and we just read v2 from Cache...
\* That is acceptable for "Eventual Consistency".
\* BUT, the code tries to prevent "stale write-through".
\* Bug condition: Can we successfully Insert v2 into Cache AFTER Compact has set S3=v3 and Invalidated?
\* If yes, we have "reverted" the cache state from "Clean/Empty" (ready for v3) to "Dirty/v2".
CacheReversion ==
    \A n \in Namespaces :
        (s3_manifest_version[n] > cache_version[n]) => (cache_version[n] = 0 \/ cache_version[n] = s3_manifest_version[n] - 1)
        \* Allow being 1 version behind (normal propagation delay).
        \* Fail if we are 2 versions behind? Or if we re-inserted old data?
        

        
\* StaleCacheCondition: A state where S3 is ahead of Cache, BUT Cache has an old value (not empty).
        
\* This implies we are serving stale reads potentially forever (or until TTL).
        
StaleCacheCondition ==
        
    \E n \in Namespaces : s3_manifest_version[n] > cache_version[n] /\ cache_version[n] > 0
        

        
\* The Invariant we want to hold: The cache should NEVER be in a Stale condition.
        
CacheIsFresh == ~StaleCacheCondition
        

        
Spec == Init /\ [][Next]_Vars
        

        
=============================================================================
        

