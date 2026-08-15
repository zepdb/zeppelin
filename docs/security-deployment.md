# Security deployment guide

Zeppelin supplies authentication, authorization, structured audit evidence,
and fail-loud storage semantics. A secure deployment also depends on the
network, identity, object-storage, key-management, host, monitoring, retention,
and incident-response controls around the process. Code alone does not make a
deployment SOC 2, HIPAA, GDPR, or otherwise compliant. The organization that
operates Zeppelin must select, implement, test, and document the controls that
apply to its risks and obligations.

## Transport and network exposure

- Require TLS 1.2 or newer either at Zeppelin's trusted ingress or at a trusted
  load balancer or reverse proxy immediately in front of it. Redirecting plain
  HTTP is not equivalent to refusing it; block plaintext traffic at the network
  boundary.
- Bind Zeppelin only to the interface needed by that ingress. Prefer a private
  address, security group, firewall, or equivalent allowlist instead of exposing
  the application listener directly to the Internet.
- Configure `server.trusted_proxies` with only the exact proxy CIDRs that are
  allowed to supply forwarding headers. Zeppelin ignores `X-Forwarded-For` from
  other peers; overly broad trusted ranges let clients influence source-IP
  attribution and rate limiting.
- Restrict `/metrics` to the monitoring network even though enforced mode also
  requires the `MetricsRead` action. Metric names and operating values can
  disclose workload shape.
- Compile the `/debug/pprof/cpu` endpoint only where profiling is required. It
  shares the application listener, requires `MetricsRead`, consumes process
  resources while sampling, and can reveal internal function names. Apply the
  same private-network controls as `/metrics` and remove the feature from normal
  production builds when it is not needed.
- Keep `/readyz` protected unless a load balancer cannot present a credential
  and the explicit `security.readyz_public` exception has been risk-reviewed.
  `/healthz` is intentionally the minimal public liveness route.

## Security and audit configuration

Every configuration must choose `security.mode` explicitly. Production should
use `enforced`; `open_unsafe` is for isolated development and emits an
unsafe-mode signal. Bootstrap authority (`security.rbac = false`) requires at
least one unexpired API key. On the first `security.rbac = true` boot, when no
policy head exists in object storage, at least one unexpired bootstrap key is
also required. After that policy head exists, object storage is authoritative
and bootstrap configuration is ignored.

```toml
[security]
mode = "enforced"
rbac = true
audit_s3 = true
audit_flush_secs = 2
cursor_hmac_key_hex = "<64 random hexadecimal characters>"
# Required when delegation or durable audit is active.
token_signing_key_path = "<absolute-path-to-ed25519-seed>"
delegated_token_max_ttl_secs = 3600
```

Durable audit is opt-in even in enforced mode. A deployment that requires
durable evidence must set `audit_s3 = true`; leaving it false keeps tracing-only
audit. Every mode rejects a zero flush interval. Enforced startup also requires
a 256-bit cursor HMAC key; provision the same secret value on every stateless
node, retain it across restarts, and never derive it from a client API key.
Rotating it invalidates outstanding cursors. A successful `must_audit` response
means the configured audit barrier completed, but an `audit_unavailable`
response is intentionally ambiguous: the domain operation may already have
occurred. Reconcile authoritative object-storage state and the audit prefix
before retrying a destructive request.

Delegation-enabled nodes and nodes with durable `audit_s3` fail boot unless
`token_signing_key_path` names a
mode-`0600` file containing exactly 32 bytes encoded as 64 hexadecimal
characters. Treat that seed as a signing secret: provision it from the
deployment secret manager, do not place it in TOML or a container image, and
prefer a distinct seed per independently operated node. At boot Zeppelin
publishes the derived public key as an immutable
`_security/signers/<signer-id>.json` object. A create-only reservation under
`_security/signer-slots/00.json` through `31.json` is the object-store-
authoritative active inventory, so concurrent boots cannot oversubscribe the
fixed 32 combined node and rotation slots. Request-path verification uses only
the disposable cache derived from those reservations. Retire an old signer
before a deployment would exceed that budget; startup fails loudly when no
reservation is available.
Rotate by installing a new seed and restarting the node, which creates a new
signer ID. Retain the prior reservation and public-key object through the longer
of the maximum token lifetime and audit-evidence retention window. Deleting the
reservation removes the signer from authoritative trust: successful cache
refreshes stop accepting its tokens, and a cache that cannot refresh fails
closed after twice `security.policy_refresh_secs`. Audit verification also
requires the reservation. Delete it first when revoking a compromised seed. The
32-slot limit
therefore bounds active nodes, rotations, and promised verification windows;
do not promise indefinite verification with this v1 registry. External or
KMS-held signing keys are not supported by this seed-file contract, and
Zeppelin never falls back to an in-process generated key.

Each durable audit `node-id` is a signer ID plus an immutable stream epoch. The
object-store-authoritative mutable head at
`_security/audit-writers/<signer-id>.json` records the current epoch, its open
UTC day, and a CAS-renewed writer lease. Startup claims that lease before making
the writer available; a second live process with the same seed fails startup.
After a crash, wait for lease expiry and restart with the same seed: Zeppelin
resumes the head's last unanchored day and tail, including across midnight. A
sealed head rotates to a fresh epoch on the next boot, so multiple clean starts
on one day never try to replace an immutable anchor. Keep the node seed stable
through crash recovery; before rotation, gracefully stop and anchor the old
stream. A new seed intentionally creates a separate signer and stream lineage.
Lease-renewal failures fence further writes. Transient object-store renewal
errors are retried only while the locally held lease remains valid. Typed
authority, serialization, or immutable-integrity failures from the periodic
renewal timer terminate the actor immediately, even before local lease expiry;
lease loss or expiry marks `/readyz` unavailable and increments the audit
flush-failure metric.
Writer-head CAS loss or reconciliation divergence during day rollover is an
authority failure, not a transient storage error, and terminates the actor
immediately. An occupied terminal slot that is neither the expected seal nor a
valid chain continuation is likewise a fatal immutable-object conflict.
Malformed or chain-divergent immutable bytes already present in the target UTC
day are a fatal serialization/integrity failure; only genuine object-store
transport failures remain retryable before the day-head CAS.
If an expired writer's already-in-flight batch wins a deterministic chain slot
after takeover, the successor detects the divergent immutable bytes, fails its
durable request, terminates its audit actor, and marks `/readyz` unavailable.
After that successor lease expires, restart with the same seed to adopt the
object-store winner; callers must retry every durable request that received the
explicit failure.

The first constrained upsert or scoped delete against a legacy namespace whose
metadata predates lifetime identities repairs that identity with conditional
object-store writes. If `meta.json` lacks
the incarnation header, Zeppelin CAS-publishes the unchanged JSON body with an
identity adopted from an already-bound live manifest, or with a fresh identity
when the manifest is still unbound. It then CAS-publishes one data-identical
manifest successor carrying that same identity. Every step requires a nonempty
backend ETag; concurrent activity is reloaded and retried, a different
incarnation fails with a conflict, and an active namespace missing its manifest
is an integrity error. Quiesce old Zeppelin binaries during this one-time
upgrade so they cannot create or republish unbound state.

Branch-aware deletion also has a one-way mixed-version boundary. Before
downgrading to a binary that predates graph-owned deletion intents, keep the
current-version nodes running while they resume or cancel every namespace that
carries `deletion_intent` (whether `active`, `creating`, or `deleting`) and every
legacy `deleting` tombstone. Verify that none remain, then stop every current
node before starting any old binary. Do not run old and new binaries
concurrently while deletion or cancellation is active. Fenced and otherwise
advanced current intents deliberately contain fields rejected by the old strict
decoder; that fail-closed behavior prevents an old cleanup worker from deleting
branch-owned artifacts before the durable reader-grace period and parent-root
release.

A namespace clone copies raw immutable artifacts, so it cannot apply a row
filter or field mask while preserving the source representation. Zeppelin
therefore requires unconstrained clone, source-read, and target-create
decisions, then uses the current in-memory policy snapshot to prove that no
principal gains broader `Query` or `VectorFetch` visibility at the target.
This proof includes global grants, target grants created before the namespace,
and principals that do not yet have a key. Unsafe clones fail before target
creation. If copying fails after the target has become active, Zeppelin retains
the target and invalidates local manifest state instead of deleting it: a
concurrent writer may already have received a success response. Inspect the
reported target and delete it explicitly after reconciling its authoritative
object-storage state.

Treat JSONL objects under
`_audit/<yyyy-mm-dd>/<node-id>/<ulid>.jsonl` as security records. Export them to
the organization's monitoring or SIEM pipeline, alert on audit flush failures,
and test that records can be enumerated and parsed. Do not base authorization or
data recovery on process-local tracing or Prometheus counters; object storage
remains the durable evidence store.

Every record carries the canonical SHA-256 hash of the previous record and a
one-based `chain_position` in that node's UTC-day stream. Before the writer is
made available at boot, Zeppelin performs one LIST and, for a non-empty stream,
one GET of the last immutable object. If that object is a terminal seal and a
preceding batch exists, recovery performs one additional GET. The final
position and record hash are the complete recovery state. A legacy tail without
`chain_position` is rejected explicitly. Preserve that evidence and either
start the upgraded binary with a fresh node stream or perform an audited offline
migration; Zeppelin never guesses a count or falls back to replaying the whole
day during startup.

Graceful day rollover and process shutdown first reserve the create-only object
slot for `chain_position + 1` with a terminal-seal document, then create a
signed terminal anchor at `_audit/anchors/<yyyy-mm-dd>/<node-id>.json`. The seal
uses the same deterministic key a late record batch would need, so an expired
writer cannot land evidence after the anchor's committed tail. If Zeppelin
crashes after the seal but before the anchor, the next lease holder validates
the seal, completes the exact anchor, and rotates the stream before accepting
requests. Run
`zeppelin_audit_verify --config <path> --day <yyyy-mm-dd> --node <node-id>`
against retained evidence; a missing anchor, removed or reordered record,
mutated body, absent/invalid terminal seal, or invalid signature produces a
nonzero exit and the first divergence. Abrupt termination before the seal can
temporarily leave the current day without an anchor. After the object-store
writer lease expires, restarting with the same node seed resumes that stream;
its next
graceful day rollover or shutdown seals and anchors it. A failed recovery or a
seed rotation performed before recovery remains an evidence gap to investigate,
not a condition the verifier silently accepts. Object Lock or an externally
retained anchor is still required to make the chain resistant to an operator
who can replace the entire stream and its checkpoint.

Audit startup captures one application-clock instant for initial chain-day
selection and boot-record timestamps. The signer-scoped object-store lease
still expires against real wall time because it coordinates independent
processes; changing an application evidence clock cannot extend writer
authority.

## Object-storage public-access and transport controls

For Amazon S3 deployments, follow the provider's current guidance for
[Block Public Access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html),
[TLS-only access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html),
and [Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html).
For GCS, Azure Blob, or another supported substrate, apply equivalent
provider-native controls and validate them with the deployment identity.

- Enable all four S3 Block Public Access settings at the account and bucket
  levels where possible. Do not use public bucket policies or public ACLs for
  namespace data, `_security/`, or `_audit/`.
- Deny requests when `aws:SecureTransport` is `false`. Keep
  `storage.s3_allow_http = false`; plain HTTP is suitable only for an isolated
  local MinIO test environment.
- Enable bucket versioning. Versioning supports recovery from operator mistakes
  and policy-head history, but it is not by itself an immutability or retention
  control.
- Use a dedicated bucket and role for each environment. Prevent development or
  staging identities from reading or writing production prefixes.
- Log S3 data events appropriate to the deployment and alert on unexpected
  access to `_security/`, writes outside known Zeppelin prefixes, and every
  attempted delete under `_audit/`.

An HTTPS-only bucket-policy statement should explicitly deny every principal
over both bucket and object resources when transport is insecure. Apply and test
the policy with the deployment's infrastructure tooling; do not assume the SDK
endpoint scheme alone enforces the bucket boundary.

## Least-privilege identities

Run Zeppelin with a workload identity scoped to its bucket or container, rather
than an account-wide storage role. Namespace data prefixes need the read, write,
list, and delete operations used by normal lifecycle, compaction, snapshots,
and garbage collection. `_security/` needs read/write access and conditional
object writes for authoritative security state. That includes conditional
create/update on the mutable `_security/audit-writers/` heads; these lease/head
documents are not audit evidence and must remain outside evidence retention.
On S3, narrow bucket-level `ListBucket` permission with prefix conditions.

For `_audit/`, grant `PutObject` as the only write authority; Zeppelin sends
create-only conditional requests for JSONL batches, terminal slots, and signed
anchors and must never overwrite an existing key.
Grant no delete authority when the deployment requires write-once evidence. The
process may also need read/list access for collision verification, evidence
checks, and operations; those permissions do not require `DeleteObject`. Deny
`DeleteObject` and `DeleteObjectVersion` for `_audit/*` explicitly so a broader
allow statement cannot accidentally grant deletion. Do not grant an ordinary
server role permission to change bucket policy, versioning, Object Lock,
encryption, or KMS key policy.

Validate the final effective policy with both positive and negative probes:
normal namespace lifecycle must work, an audit create must work, replacing an
existing audit key must fail, and audit deletion must fail.

## Credentials and key management

- Prefer temporary credentials from the runtime's workload-identity mechanism,
  such as an instance, task, pod, or federated role. Avoid long-lived access-key
  IDs and secrets in TOML, environment snapshots, container images, shell
  history, or source control.
- Store Zeppelin API bearer secrets in a secret manager and distribute them over
  an authenticated channel. The config contains only their SHA-256 digests, but
  the original bearer value must still be rotated, access-controlled, and never
  logged.
- Configure default bucket encryption with SSE-KMS and a customer-managed KMS
  key when the deployment requires customer-managed encryption. Zeppelin relies
  on bucket configuration; it does not silently substitute an application-side
  encryption default.
- Scope KMS use to the Zeppelin role and S3 service path, separate key
  administrators from data users, enable key-use logging, and test decrypt after
  rotation. Include the required KMS permissions in the role without granting
  key-policy administration.

## Versioning, retention, and Object Lock

Regulated or high-assurance S3 deployments should evaluate Object Lock for
audit evidence. Object Lock must be enabled for the bucket and depends on
versioning; choose governance or compliance mode and a retention period through
the organization's records policy. Zeppelin does not choose that legal or
operational policy and does not automate WORM configuration.

If retention is intended specifically for `_audit/`, make sure the deployment
mechanism actually applies retention to every new audit object. Bucket-default
retention can affect objects outside `_audit/`; use infrastructure and bucket
layout that match the intended scope. Test retention with the server role and
with privileged operator roles, and control legal-hold and bypass-governance
permissions separately. Object Lock is not a substitute for access review,
export monitoring, restoration tests, or a documented deletion/retention
process.

## Local cache and host controls

The local cache is disposable and never authoritative, but it can contain
vectors, index pages, WAL fragments, and namespace-derived data. Place
`cache.dir` on a dedicated directory owned by the Zeppelin service account,
with no access for unrelated users (for example, mode `0700` on Unix-like
systems). Do not share it between tenants or processes.

Use host or volume encryption for the cache device when data-at-rest controls
require it. Protect swap, crash dumps, snapshots, backups, and host diagnostics
under the same policy. Run the service as a non-root identity, keep the host and
runtime patched, and securely discard cache volumes when a node is retired.
Deleting cache files does not delete authoritative object-storage data.

## Operational verification

Before exposing traffic, verify at least the following:

1. Plain HTTP and direct untrusted network access are rejected.
2. Enforced mode refuses missing credentials and unauthorized actions.
3. The workload role cannot make the bucket public, administer KMS, or delete
   `_audit/` objects.
4. An authorized destructive operation creates durable audit evidence, and an
   injected audit failure prevents a success response.
5. `zeppelin_auth_failures_total`, `zeppelin_authz_denials_total`,
   `zeppelin_audit_records_total`, and `zeppelin_audit_flush_failures_total`
   are scraped without principal IDs or other unbounded identity labels.
6. Audit export, version restoration, KMS rotation, credential rotation, and
   incident-response procedures work in a non-production exercise.

Re-run these checks after changes to ingress, IAM, bucket policy, KMS, retention,
the Zeppelin security configuration, or the deployment platform. Record the
results as deployment evidence; the presence of this guide or the software's
security features is not a certification.
