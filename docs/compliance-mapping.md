# Compliance control mapping

This document maps Zeppelin mechanisms to commonly referenced control
families. It is implementation guidance, not a certification, legal opinion,
or claim that a deployment is compliant. Operators remain responsible for
configuration, identity lifecycle, infrastructure controls, evidence review,
and applicable-law decisions.

| Reference control | Zeppelin mechanism | Phase | Deployment responsibility |
|---|---|---:|---|
| SOC 2 CC6.1-CC6.3 logical access | Authenticated principals, S3-authoritative RBAC policy, fail-closed route authorization, constrained grants | 1, 3, 4 | Define roles, approve grants, rotate credentials, and review access |
| SOC 2 CC7.2 monitoring | Structured authorization events, durable S3 audit batches, per-node/day hash chains, and signed terminal anchors | 2, 10 | Retain, export, verify, alert on, and review audit evidence |
| HIPAA 45 CFR 164.312(a)(2)(i) unique user identification | Stable `PrincipalId` values and separately issued credentials | 3 | Issue individual identities; do not share human credentials |
| HIPAA 45 CFR 164.312(b) audit controls | Redaction-safe durable audit records with decision and request identity | 2 | Configure retention and operational review appropriate to the environment |
| GDPR Article 17 erasure requests | Governed namespace destruction record written before manifest removal | 8 | Validate the request, scope, lawful basis, downstream copies, and retention exceptions |
| GDPR preservation or retention obligations interacting with Article 17 | Generic active preservation locks block namespace, snapshot, vector, compaction, and GC destruction | 8 | Decide legal precedence. Zeppelin encodes no jurisdiction-specific hierarchy |
| GDPR Article 32 security of processing | TLS and object-store encryption deployment controls, hardened authentication and authorization | 1-4 | Configure TLS, SSE/CMEK, network isolation, backups, monitoring, and incident response |

## Preservation semantics

Preservation is deliberately generic. `reason_kind` is a bounded operational
category, while `reason_text` records the operator's rationale; neither field
changes enforcement semantics. Active locks are selected by one CAS-headed S3
record. A node may use the cached selection only inside its bounded freshness
interval. If refresh fails past that bound, destructive operations fail closed.

Namespace-filter locks currently use conservative overlap: a delete in the same
namespace is blocked unless disjointness is structurally proved. The current
implementation proves no Filter-AST disjointness and therefore favors
over-retention.

Locked GC and compaction use whole-operation deferral. GC exits before listing,
marking, pruning, or deleting the locked namespace. Compaction returns an
explicit no-op before reading or publishing a manifest. This preserves every
source fragment, tombstone, history root, and immutable artifact and avoids a
second partial-retention algorithm. Unlocked namespaces continue through the
existing maintenance paths. Each deferral writes immutable evidence below
`_audit/preservation/` before returning; failure to persist that evidence still
leaves maintenance fail-closed.

`PreservationRelease` always requires a distinct authorized `zpk1` approver.
The requirement is attached by the server and cannot be removed from a policy
grant. Delegated tokens cannot approve a release.
Every successful preservation-head CAS also points at an immutable transition
record. Those records form a backward-linked authoritative path, so a losing
CAS attempt's orphan create or release record cannot be mistaken for committed
lock state. The service installs the CAS-known state immediately; a fallible
post-commit reread cannot downgrade a committed mutation or its audit outcome.

## Destruction evidence

Namespace deletion first CAS-publishes a `deleting` tombstone that binds one
deterministic `_audit/destruction/` record key. With new writes rejected, the
record captures the exact manifest version plus observed object and byte
counts, actor, optional approver, decision identity, and timestamp. Only after
that immutable record is durable may the live manifest be removed. If the
record write fails, deletion returns `audit_unavailable`; the tombstone and
manifest remain intact so a retry can reuse the bound key. If manifest removal
fails afterward, the same tombstone/evidence pair resumes without emitting a
second destruction record. Every physical cleanup pass rechecks fresh active
locks and retains the tombstone when a lock was activated mid-protocol.

Phase 10 links ordinary audit records within each node/day stream, reserves a
create-only terminal slot against late writers, and signs a terminal anchor at
day rollover or graceful shutdown. This detects mutation,
removal, and reordering when the verifier retains a trusted checkpoint. Bucket
immutability, retention, access policy, and external anchor custody remain
deployment controls; a hash chain alone cannot prevent replacement of both the
stream and its anchor by an actor with unrestricted bucket authority.

Structural retrieval receipts additionally bind the exact canonical query and
result digests, production traversal evidence for every ANN or BM25 source, the
historical policy-owned filter hash, a manifest-rooted Merkle inventory, and a
separately rooted receipt-signed inventory for lazy policy-scope ANN/BM25
artifacts. A signed, persisted binding version selects the stable field-by-field
query-routing projection, preventing schema evolution from reinterpreting old
receipt digests. Privileged verification may resolve the named immutable policy
generation; unprivileged verification explicitly reports that check as
unchecked. These receipts provide tamper evidence, not a claim of deterministic
replay, semantic completeness, or exact recall.
