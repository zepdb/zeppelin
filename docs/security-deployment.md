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
use `enforced` with at least one unexpired bootstrap key; `open_unsafe` is for
isolated development and emits an unsafe-mode signal.

```toml
[security]
mode = "enforced"
audit_s3 = true
audit_flush_secs = 2
cursor_hmac_key_hex = "<64 random hexadecimal characters>"
```

Enforced mode refuses to start when `audit_s3 = false`, and every mode rejects a
zero flush interval. Enforced startup also requires a 256-bit cursor HMAC key;
provision the same secret value on every stateless node, retain it across
restarts, and never derive it from a client API key. Rotating it invalidates
outstanding cursors. Keep audit persistence enabled. A successful
`must_audit` response means the audit barrier completed, but an
`audit_unavailable` response is intentionally ambiguous: the domain operation
may already have occurred. Reconcile authoritative S3 state and the audit prefix
before retrying a destructive request.

The first constrained upsert or scoped delete against a pre-Phase-4 namespace
repairs its lifetime identity with conditional S3 writes. If `meta.json` lacks
the incarnation header, Zeppelin CAS-publishes the unchanged JSON body with an
identity adopted from an already-bound live manifest, or with a fresh identity
when the manifest is still unbound. It then CAS-publishes one data-identical
manifest successor carrying that same identity. Every step requires a nonempty
backend ETag; concurrent activity is reloaded and retried, a different
incarnation fails with a conflict, and an active namespace missing its manifest
is an integrity error. Quiesce old Zeppelin binaries during this one-time
upgrade so they cannot create or republish unbound state.

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
reported target and delete it explicitly after reconciling its authoritative S3
state.

Treat JSONL objects under
`_audit/<yyyy-mm-dd>/<node-id>/<ulid>.jsonl` as security records. Export them to
the organization's monitoring or SIEM pipeline, alert on audit flush failures,
and test that records can be enumerated and parsed. Do not base authorization or
data recovery on process-local tracing or Prometheus counters; S3 remains the
durable evidence store.

## S3 public-access and transport controls

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

Run Zeppelin with a workload identity scoped to its bucket, rather than an
account-wide S3 role. Namespace data prefixes need the read, write, list, and
delete operations used by normal lifecycle, compaction, snapshots, and garbage
collection. `_security/` needs read/write access and conditional object writes
for authoritative security state. Narrow bucket-level `ListBucket` permission
with prefix conditions.

For `_audit/`, grant `PutObject` as the only write authority; Zeppelin sends
create-only conditional requests and must never overwrite an existing key.
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

Regulated or high-assurance deployments should evaluate S3 Object Lock for
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
Deleting cache files does not delete authoritative S3 data.

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
