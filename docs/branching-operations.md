# Namespace branching operations

Zeppelin branching creates writable copy-on-write forks of the source's exact
live head. It is **fork only**: Zeppelin does not implement branch merge,
rebase, diff, or conflict-resolution operations. Moving changes between
namespaces is an application-level export/import workflow.

> **Release status:** Branching is default-disabled. The repository's MinIO,
> soak, recall, and independent-review release gates have not been recorded as
> complete. Treat the feature as pre-release until the deployment owner accepts
> those gates.

## Enable branching explicitly

Branching is disabled by default. Enable it with the server admission switch
in `zeppelin.toml`:

```toml
[branching]
enabled = true
max_children_per_namespace = 256
max_depth = 16
```

The configuration switch mounts the branch route. The caller also needs
`NamespaceFork` and `NamespaceRead` on the source plus `NamespaceCreate` on
the target; all three decisions must be unconstrained and pass the
source-to-target no-widening policy check. Restart every stateless node with
the same switch before sending branch traffic.

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

Background maintenance strongly scans namespace metadata and parent manifests
once per compaction tick and publishes the result as a process-local snapshot.
The branch-graph portion of `GET /readyz` answers from that snapshot and adds no
object-store work to the route, so probe cost does not grow with namespace count
and a transient per-namespace read failure cannot evict an otherwise healthy
process. The route still performs its separate storage-reachability probe. A
failed graph scan retains the previous snapshot and retries on the next tick.

The branch-graph condition becomes not ready for an orphan root or for a branch
lifecycle intent that has made no durable progress for five minutes. A branch
deletion waiting for its persisted reader-safety `not_before` deadline is not
considered stalled until five minutes after that deadline. Protected readiness
returns bounded aggregate stalled-intent counts and at most 16 orphan-root
repair identities; public readiness never returns those identities.

`/readyz` also checks the durable-audit latch, the compaction-loop heartbeat,
and object-storage reachability. It returns 503 when the compaction supervisor's
exit guard reports dead or when no tick has completed for more than
`3 * compaction.interval_secs + 60` seconds. The heartbeat check is two atomic
loads and does not scan object storage.

Two consequences follow from the snapshot being the answer. A defect is visible
one maintenance tick after it appears rather than instantly; against the
five-minute stall threshold that lag is immaterial. And a process reports ready
until its first scan completes, because a scan that has not run has not observed
a defect. With `branching.enabled = false` no scan ever runs and branch
readiness is permanently inert.

The same successful scan refreshes these process-local Prometheus gauges:

- `zeppelin_branch_intents_stalled{state="creating"}`
- `zeppelin_branch_intents_stalled{state="deleting"}`
- `zeppelin_branch_roots`

The only metric label is the bounded `state` enum. Namespace, branch, principal,
and policy identities are deliberately absent. Alert on either stalled gauge
being nonzero and use the protected readiness repair response plus authoritative
object-storage metadata to investigate. The root gauge is the total direct-child
roots observed by the latest successful scan; it is capacity and retention
pressure, not a monotonically increasing counter.

The compaction supervisor separately publishes
`zeppelin_compaction_loop_last_tick_timestamp_seconds` and the 0/1 gauge
`zeppelin_compaction_loop_alive`.
