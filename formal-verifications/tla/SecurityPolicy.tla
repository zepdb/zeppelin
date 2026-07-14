--------------------------- MODULE SecurityPolicy ---------------------------
\* Formal model of Zeppelin's S3-authoritative security-policy head and the
\* bounded-staleness authorization cache.
\*
\* The model deliberately separates a node's publisher memo from the S3 head.
\* Two nodes may therefore prepare from the same head.  Only the node whose
\* memo still matches the head ETag can make its immutable snapshot visible.
\* A losing node must ObserveHead before retrying.
\*
\* RefreshCache models the successful, linearizable conditional head GET plus
\* snapshot verification.  Missing refresh transitions model read failures:
\* the cache remains usable only until RevocationBound elapses, after which
\* Authorize fails closed.

EXTENDS Naturals

CONSTANTS
    Nodes,
    Versions,
    MaxTime,
    RevocationBound,
    AllowNonCasPublish,
    AllowStaleAuthorization

NoVersion == 0
DecisionValues == {"None", "Allow", "Deny"}

VARIABLES
    \* S3-authoritative immutable snapshots and CAS-updated head.
    head_version,
    head_etag,
    head_versions_seen,
    policy_written,
    policy_allows,

    \* Per-node memo used as the expected version and ETag for policy CAS.
    publisher_version,
    publisher_etag,

    \* Per-node disposable policy cache and monotonic freshness timestamp.
    cache_version,
    cache_confirmed_at,
    now,

    \* First CAS-visible revocation in this bounded model.
    revocation_visible,
    revocation_version,
    revocation_visible_at,

    \* Most recent authorization result issued by each node.
    last_decision,
    last_auth_version,
    last_auth_at

vars ==
    << head_version, head_etag, head_versions_seen, policy_written,
       policy_allows, publisher_version, publisher_etag, cache_version,
       cache_confirmed_at, now, revocation_visible, revocation_version,
       revocation_visible_at, last_decision, last_auth_version,
       last_auth_at >>

InitialVersion == 1

Init ==
    /\ InitialVersion \in Versions
    /\ NoVersion \notin Versions
    /\ head_version = InitialVersion
    /\ head_etag = 1
    /\ head_versions_seen = {InitialVersion}
    /\ policy_written = {InitialVersion}
    /\ policy_allows = [v \in Versions |-> TRUE]
    /\ publisher_version = [n \in Nodes |-> InitialVersion]
    /\ publisher_etag = [n \in Nodes |-> 1]
    /\ cache_version = [n \in Nodes |-> InitialVersion]
    /\ cache_confirmed_at = [n \in Nodes |-> 0]
    /\ now = 0
    /\ revocation_visible = FALSE
    /\ revocation_version = NoVersion
    /\ revocation_visible_at = 0
    /\ last_decision = [n \in Nodes |-> "None"]
    /\ last_auth_version = [n \in Nodes |-> InitialVersion]
    /\ last_auth_at = [n \in Nodes |-> 0]

CasMemoMatches(n) ==
    /\ publisher_version[n] = head_version
    /\ publisher_etag[n] = head_etag

CandidateVersion(n) == publisher_version[n] + 1

CacheFresh(n) ==
    now - cache_confirmed_at[n] <= RevocationBound

\* A CAS loser reads the authoritative head before retrying.  This action is
\* also how a second writer observes a successful concurrent publication.
ObserveHead(n) ==
    /\ n \in Nodes
    /\ \/ publisher_version[n] # head_version
       \/ publisher_etag[n] # head_etag
    /\ publisher_version' =
        [publisher_version EXCEPT ![n] = head_version]
    /\ publisher_etag' = [publisher_etag EXCEPT ![n] = head_etag]
    /\ UNCHANGED
        << head_version, head_etag, head_versions_seen, policy_written,
           policy_allows, cache_version, cache_confirmed_at, now,
           revocation_visible, revocation_version, revocation_visible_at,
           last_decision, last_auth_version, last_auth_at >>

\* PublishPolicy writes an immutable complete snapshot, then conditionally
\* updates _security/heads/policy.json with the publisher's cached ETag.
\* AllowNonCasPublish exists only for the negative TLC config.
PublishPolicy(n) ==
    /\ n \in Nodes
    /\ CandidateVersion(n) \in Versions
    /\ IF AllowNonCasPublish THEN TRUE ELSE CasMemoMatches(n)
    /\ LET candidate == CandidateVersion(n)
       IN
       /\ policy_written' = policy_written \cup {candidate}
       /\ policy_allows' =
            IF candidate \in policy_written
            THEN policy_allows
            ELSE [policy_allows EXCEPT
                    ![candidate] = policy_allows[head_version]]
       /\ head_version' = candidate
       /\ head_etag' = head_etag + 1
       /\ head_versions_seen' = head_versions_seen \cup {candidate}
       /\ publisher_version' =
            [publisher_version EXCEPT ![n] = candidate]
       /\ publisher_etag' =
            [publisher_etag EXCEPT ![n] = head_etag + 1]
       \* The mutating node installs its successful publication immediately.
       /\ cache_version' = [cache_version EXCEPT ![n] = candidate]
       /\ cache_confirmed_at' =
            [cache_confirmed_at EXCEPT ![n] = now]
    /\ UNCHANGED
        << now, revocation_visible, revocation_version,
           revocation_visible_at, last_decision, last_auth_version,
           last_auth_at >>

\* Revoke is a specialized policy publication: it writes a complete snapshot
\* with the credential disabled, CAS-publishes the head, and writes through on
\* the mutating node.  Later ordinary publications copy the disabled state.
Revoke(n) ==
    /\ n \in Nodes
    /\ ~revocation_visible
    /\ policy_allows[head_version]
    /\ CandidateVersion(n) \in Versions
    /\ IF AllowNonCasPublish THEN TRUE ELSE CasMemoMatches(n)
    /\ LET candidate == CandidateVersion(n)
       IN
       /\ policy_written' = policy_written \cup {candidate}
       /\ policy_allows' =
            IF candidate \in policy_written
            THEN policy_allows
            ELSE [policy_allows EXCEPT ![candidate] = FALSE]
       /\ head_version' = candidate
       /\ head_etag' = head_etag + 1
       /\ head_versions_seen' = head_versions_seen \cup {candidate}
       /\ publisher_version' =
            [publisher_version EXCEPT ![n] = candidate]
       /\ publisher_etag' =
            [publisher_etag EXCEPT ![n] = head_etag + 1]
       /\ cache_version' = [cache_version EXCEPT ![n] = candidate]
       /\ cache_confirmed_at' =
            [cache_confirmed_at EXCEPT ![n] = now]
       /\ revocation_visible' = TRUE
       /\ revocation_version' = candidate
       /\ revocation_visible_at' = now
    /\ UNCHANGED << now, last_decision, last_auth_version, last_auth_at >>

\* A successful conditional head GET either confirms the cached ETag or loads
\* and checksum-verifies the new immutable snapshot.  Both cases renew the
\* monotonic freshness timestamp.
RefreshCache(n) ==
    /\ n \in Nodes
    /\ \/ cache_version[n] # head_version
       \/ cache_confirmed_at[n] # now
    /\ cache_version' = [cache_version EXCEPT ![n] = head_version]
    /\ cache_confirmed_at' = [cache_confirmed_at EXCEPT ![n] = now]
    /\ UNCHANGED
        << head_version, head_etag, head_versions_seen, policy_written,
           policy_allows, publisher_version, publisher_etag, now,
           revocation_visible, revocation_version, revocation_visible_at,
           last_decision, last_auth_version, last_auth_at >>

\* Authorization performs no S3 operation.  It may issue Allow only when the
\* cached snapshot grants access and the last confirmation is within the
\* fail-closed bound.  AllowStaleAuthorization is negative-model-only.
Authorize(n) ==
    /\ n \in Nodes
    /\ last_decision' =
        [last_decision EXCEPT
            ![n] = IF policy_allows[cache_version[n]]
                       /\ (CacheFresh(n) \/ AllowStaleAuthorization)
                    THEN "Allow"
                    ELSE "Deny"]
    /\ last_auth_version' =
        [last_auth_version EXCEPT ![n] = cache_version[n]]
    /\ last_auth_at' = [last_auth_at EXCEPT ![n] = now]
    /\ UNCHANGED
        << head_version, head_etag, head_versions_seen, policy_written,
           policy_allows, publisher_version, publisher_etag, cache_version,
           cache_confirmed_at, now, revocation_visible, revocation_version,
           revocation_visible_at >>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED
        << head_version, head_etag, head_versions_seen, policy_written,
           policy_allows, publisher_version, publisher_etag, cache_version,
           cache_confirmed_at, revocation_visible, revocation_version,
           revocation_visible_at, last_decision, last_auth_version,
           last_auth_at >>

Next ==
    \/ \E n \in Nodes : ObserveHead(n)
    \/ \E n \in Nodes : PublishPolicy(n)
    \/ \E n \in Nodes : RefreshCache(n)
    \/ \E n \in Nodes : Authorize(n)
    \/ \E n \in Nodes : Revoke(n)
    \/ AdvanceTime

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ head_version \in Versions
    /\ head_etag \in Nat
    /\ head_versions_seen \subseteq Versions
    /\ policy_written \subseteq Versions
    /\ head_version \in policy_written
    /\ policy_allows \in [Versions -> BOOLEAN]
    /\ publisher_version \in [Nodes -> Versions]
    /\ publisher_etag \in [Nodes -> Nat]
    /\ cache_version \in [Nodes -> Versions]
    /\ cache_confirmed_at \in [Nodes -> 0..MaxTime]
    /\ now \in 0..MaxTime
    /\ revocation_visible \in BOOLEAN
    /\ revocation_version \in Versions \cup {NoVersion}
    /\ revocation_visible_at \in 0..MaxTime
    /\ last_decision \in [Nodes -> DecisionValues]
    /\ last_auth_version \in [Nodes -> Versions]
    /\ last_auth_at \in [Nodes -> 0..MaxTime]

\* Every version ever made CAS-visible remains less than or equal to the
\* current head.  A stale unconditional head overwrite violates this.
CasHeadVersionNeverRegresses ==
    \A version \in head_versions_seen : version <= head_version

\* Once a revocation is CAS-visible, an authorization may use an older cached
\* snapshot only through the explicit bound.  At times strictly beyond that
\* bound, an Allow from a pre-revocation snapshot violates the contract.
NoAllowPastVisibleRevocationBound ==
    \A n \in Nodes :
        /\ last_decision[n] = "Allow"
        /\ revocation_visible
        /\ last_auth_version[n] < revocation_version
        => last_auth_at[n] <= revocation_visible_at + RevocationBound

=============================================================================
