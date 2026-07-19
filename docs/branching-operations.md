# Namespace branching operations

Zeppelin branching creates writable copy-on-write forks of the source's exact
live head. It is **fork only**: Zeppelin does not implement branch merge,
rebase, diff, or conflict-resolution operations. Moving changes between
namespaces is an application-level export/import workflow.

## Enable branching explicitly

Branching is disabled by default. Production activation requires both of these
independent boot-time authorities:

1. Set the server admission switch in `zeppelin.toml`:

   ```toml
   [branching]
   enabled = true
   max_children_per_namespace = 256
   max_depth = 16
   ```

2. Configure `security.license_path` with a valid signed license whose
   `features` array contains `"branching"`.

The configuration switch alone only mounts the branch route. A request still
fails closed when the verified license lacks the branching entitlement. The
caller also needs `NamespaceFork` and `NamespaceRead` on the source plus
`NamespaceCreate` on the target; all three decisions must be unconstrained and
pass the source-to-target no-widening policy check. Restart every stateless node
with the same switch and licensed feature before sending branch traffic.

Create a live-head fork with:

```http
POST /v1/namespaces/{source}/branches
Content-Type: application/json

{"target":"fresh-target-name"}
```

Historical generation, timestamp, and snapshot selectors are intentionally not
accepted by this endpoint.

## Retention and deletion

A fork installs a durable direct-child root in its parent manifest. Deleting a
source or an intermediate parent is blocked while any live child root remains;
delete descendants from the leaves upward. Branch deletion removes the child's
live visibility first, persists a reader-safety deadline, and releases the
exact parent root only after that deadline. A deletion can therefore remain in
progress for the configured reader-safety horizon without being stalled.

If the operational requirement is an independent target that must not retain a
parent root, use the copy-clone endpoint instead:

```http
POST /v1/namespaces/{source}/clone
Content-Type: application/json

{"target":"independent-copy","as_of":"<live-generation>"}
```

Copy clone materializes target-owned immutable artifacts immediately. Its cost
scales with the selected reachable data and it requires `NamespaceClone` and
`NamespaceRead` on the source plus `NamespaceCreate` on the target.

## Materialization cost and limits

Fork creation publishes control metadata and does not copy the corpus. The
first manual or background compaction of a foreign-backed branch is different:
it reads the branch's complete logical view and writes a target-owned segment.
Budget that first materialization as a full-corpus operation for object-store
GETs, bytes read, index construction, target uploads, CPU, memory, and elapsed
time. Later compactions use the ordinary target-local incremental path.

`branching.max_children_per_namespace` limits direct children, not all
descendants. Its default is 256 and its hard maximum is 4,096.
`branching.max_depth` limits ancestry depth. Its default is 16 and its hard
maximum is 64. Admission fails loudly when either configured limit would be
exceeded; raising the values increases manifest control-state size and graph
maintenance work.

## Readiness and metrics

`GET /readyz` strongly scans namespace metadata and parent manifests. It becomes
not ready for an orphan root or for a branch lifecycle intent that has made no
durable progress for five minutes. A branch deletion waiting for its persisted
reader-safety `not_before` deadline is not considered stalled until five
minutes after that deadline. Protected readiness returns bounded aggregate
stalled-intent counts and at most 16 orphan-root repair identities; public
readiness never returns those identities.

The same successful scan refreshes these process-local Prometheus gauges:

- `zeppelin_branch_intents_stalled{state="creating"}`
- `zeppelin_branch_intents_stalled{state="deleting"}`
- `zeppelin_branch_roots`

The only metric label is the bounded `state` enum. Namespace, branch, principal,
and policy identities are deliberately absent. Alert on either stalled gauge
being nonzero and use the protected readiness repair response plus authoritative
S3 metadata to investigate. The root gauge is the total direct-child roots
observed by the latest successful scan; it is capacity and retention pressure,
not a monotonically increasing counter.
