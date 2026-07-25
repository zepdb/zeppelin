# src/namespace — metadata, lifecycle, and the branch graph

Two entry points, and the distinction matters:

- `manager.rs` (`NamespaceManager`) — CRUD and persisted `meta.json`. Owns
  low-level, `pub(crate)` lifecycle primitives.
- `graph.rs` (`NamespaceGraph`) — the **governed** coordinator. Composes those
  primitives into the fork, activation, and deletion state machines.

## The single most important rule

**`NamespaceGraph::delete` is the sole entry point for namespace deletion —
for every namespace, branched or not.** The HTTP handler, request-scoped
cleanup, and the background sweep all funnel through it
(`server/handlers/namespace.rs:3018`, `server/mod.rs:spawn_namespace_delete_cleanup`,
`compaction/background.rs`).

This means **deletion-unification work is not gated behind `branching.enabled`**.
A change to the governed delete path affects ordinary single-namespace
deployments. Budget review effort accordingly; this is the widest blast radius
in the codebase.

Delete is a durable, resumable state machine, roughly:

```
intent (metadata CAS) -> lease -> fence manifest generation
  -> destruction evidence -> visibility removal marker + grace
  -> parent-root release + ack -> owned-key object cleanup -> meta.json last
```

Every boundary is crash-resumable via `resume_delete`. `meta.json` is deleted
**last** and acts as the tombstone. If you add a step, add it to the recovery
matrix in `tests/branch_deletion_tests.rs` too.

## Branching model

Fork-only. **There is no merge, rebase, diff, or promote**, and that is a
locked product decision, not a gap — see
`tasks/branching/deletion-unification-design.md` §13.

- Copy-on-write. A fresh branch copies **no artifacts**; its manifest's visible
  refs carry a physical origin pointing at the source namespace.
- The first compaction of a foreign-backed branch **fully materializes** it:
  reads the entire logical view, writes target-owned segments. Budget it as a
  full-corpus operation. Later compactions are ordinary incremental local ones.
- A source namespace with a live child root **cannot be deleted** (409). The
  documented alternative is copy-clone.
- Limits: `max_children_per_namespace` (default 256, hard max 4096),
  `max_depth` (default 16, hard max 64).

### Gating

Branching is **off by default** and needs *two* independent things:

1. `config.branching.enabled = true` — controls whether routes are registered
   at all (`server/mod.rs:2742`);
2. a valid `Feature::Branching` entitlement.

Both paths enforce both gates. `create_branch` and `list_branches` each check
`config.branching.enabled` in the handler, and `authorize_namespace_fork` /
`authorize_branch_list` each check the entitlement in `security/kernel.rs`.
Keep new branch routes on this shape.

> Corrected 2026-07-24. An earlier revision claimed `list_branches` and
> `authorize_branch_list` checked only the config flag. That was already false
> when written: the kernel check landed in `4f8583c`.

## `/readyz` reads a snapshot; it does not scan

`inspect_readiness` is O(namespaces) in strong reads: one
`list_common_prefixes("")`, a metadata *and* manifest read per namespace, plus
a metadata read per branch root. **It runs only on the background maintenance
pass**, which publishes its result into `BranchGraphReadinessSnapshot`.
`readiness_check` reads that snapshot and issues no object-store work of its
own. With `branching.enabled = false` the scan never runs at all.

Two consequences to keep in mind when changing this:

- **Absent snapshot means ready, not unhealthy.** A scan that never ran
  observed no defect, so withholding readiness on it would fail closed on no
  evidence.
- **Detection lags one maintenance tick** (~30s default) against a 300s stall
  threshold. That margin is what makes the trade sound; shrinking the threshold
  or lengthening the compaction interval erodes it.

`GovernedDeletionWorker::refresh_readiness` is public so tests can observe one
scan deterministically instead of sleeping on a tick — use it rather than
polling. `branching_tests::readiness_probes_never_scan_the_namespace_graph`
pins the zero-cost property.

> Corrected 2026-07-24. An earlier revision of this file said the scan ran on
> every probe, unbudgeted. That was true when written and was fixed in
> `4f8583c`.

## Backward compatibility

Every field added to `NamespaceMetadata` for branching is
`#[serde(default)]` (+ `skip_serializing_if` for options), so pre-branching
`meta.json` still deserializes. Keep it that way. `incarnation_id` is
`#[serde(skip)]` — it comes from S3 user metadata, not the JSON body.

`meta.json` is JSON, so `skip_serializing_if` is safe here. Do **not** copy
that pattern into `../wal/` types, which are MessagePack.

## Testing

`branching-test-support` is a non-default Cargo feature exposing
`branching/test_support.rs`. **No release artifact may contain it** — the
release build must use default features.

```bash
TEST_BACKEND=minio cargo test --features branching-test-support \
  --test branch_fork_tests --test branch_deletion_tests \
  --test branch_root_tests --test branching_tests \
  --test artifact_origin_tests -- --test-threads=1
```

MinIO is what actually exercises CAS; `TEST_BACKEND=memory` (the default) is
weaker evidence for anything concurrency- or CAS-shaped.

## See also

- `tasks/branching/` — the 10 phase plans + `deletion-unification-design.md`
  (10 slices) + `10-release-evidence.md` (status ledger)
- `../storage/CLAUDE.md` — owned-key classification used by cleanup
- `../wal/CLAUDE.md` — artifact origins and manifest binding versions
