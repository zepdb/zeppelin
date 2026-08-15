# Compliance control mapping

This document maps Zeppelin mechanisms to commonly referenced control
families. It is implementation guidance, not a certification, legal opinion,
or claim that a deployment is compliant. Operators remain responsible for
configuration, identity lifecycle, infrastructure controls, evidence review,
and applicable-law decisions.

| Reference control | Zeppelin mechanism | Deployment responsibility |
|---|---|---|
| SOC 2 CC6.1-CC6.3 logical access | Authenticated principals, object-store-authoritative RBAC policy, fail-closed route authorization, constrained grants | Define roles, approve grants, rotate credentials, and review access |
| SOC 2 CC7.2 monitoring | Structured authorization events and, when `security.audit_s3 = true`, durable audit batches, per-node/day hash chains, and signed terminal anchors | Retain, export, verify, alert on, and review audit evidence |
| [HIPAA 45 CFR 164.312(a)(2)(i) unique user identification](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164/subpart-C/section-164.312) | Stable `PrincipalId` values and separately issued credentials | Issue individual identities; do not share human credentials |
| [HIPAA 45 CFR 164.312(b) audit controls](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164/subpart-C/section-164.312) | Redaction-safe durable audit records with decision and request identity | Configure durable audit, retention, and operational review appropriate to the environment |
| [GDPR Article 17 erasure requests](https://eur-lex.europa.eu/eli/reg/2016/679/oj?locale=EN) | Governed namespace destruction evidence written before manifest removal | Validate the request, scope, lawful basis, downstream copies, and retention exceptions |
| GDPR preservation or retention obligations interacting with Article 17 | Generic active preservation locks block namespace, snapshot, vector, compaction, and GC destruction | Decide legal precedence. Zeppelin encodes no jurisdiction-specific hierarchy |
| [GDPR Article 32 security of processing](https://eur-lex.europa.eu/eli/reg/2016/679/oj?locale=EN) | TLS and object-store encryption deployment controls, hardened authentication and authorization | Configure TLS, encryption, network isolation, backups, monitoring, and incident response |

## Preservation semantics

Preservation is deliberately generic. `reason_kind` is a bounded operational
category, while `reason_text` records the operator's rationale; neither field
changes enforcement semantics. Active locks are selected by one CAS-headed
object-storage record. A node may use the cached selection only inside its
bounded freshness interval. If refresh fails past that bound, destructive
operations fail closed.

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

Namespace deletion first CAS-installs a durable `deletion_intent` in metadata,
binding a deterministic `_audit/destruction/` record key. Under the namespace
writer lease, Zeppelin then fences the exact live-manifest generation, records
that generation in the intent, and writes immutable evidence containing the
observed object and byte counts, actor, optional approver, decision identity,
and timestamp. Only after that evidence is durable does Zeppelin mark the
metadata `deleting` and remove the live manifest. If the evidence write fails,
deletion returns `audit_unavailable`; the intent and fenced live manifest remain
for a resumable retry using the same bound key. If manifest removal fails
afterward, the same intent/evidence pair resumes without emitting a second
destruction record. Every physical cleanup pass rechecks fresh active locks and
retains the tombstone when a lock was activated mid-protocol.

Durable audit links ordinary records within each node/day stream, reserves a
create-only terminal slot against late writers, and signs a terminal anchor at
day rollover or graceful shutdown. This detects mutation,
removal, and reordering when the verifier retains a trusted checkpoint. Bucket
immutability, retention, access policy, and external anchor custody remain
deployment controls; a hash chain alone cannot prevent replacement of both the
stream and its anchor by an actor with unrestricted bucket authority.
