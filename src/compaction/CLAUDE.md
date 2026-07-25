# src/compaction — WAL → segments, and GC

`mod.rs` builds segments; `gc.rs` reclaims retired objects; `background.rs`
runs the production loops. Uploading a segment does **not** publish it — only
the manifest CAS that installs its `SegmentRef` does.

## Two-pass mark-and-sweep GC

Deletion is deletes-then-prune, and the revalidation pass is not optional.
Manifest `pending_deletes` is deliberately uncapped (see `../wal/CLAUDE.md`);
capping it would leak objects.

**Every destructive path goes through `TargetOwnedDeletionKey::classify`**,
which delegates to `NamespaceObjectKey::classify` in `../storage/`. It fails
closed on any key that is not provably owned by the exact namespace and a known
family. Error text is
`"GC target {ns} cannot delete unowned key {key}: {inner}"`.

Do not add a `delete` call that bypasses this classifier. For branch targets,
this is the mechanism that guarantees a target's cleanup never issues a
DELETE against a source namespace's prefix.

## Branch materialization

The **first** compaction of a foreign-backed branch is not incremental: it
reads the branch's complete logical view through the artifact-origin resolver
and writes target-owned segments. Budget it as a full-corpus operation (GETs,
bytes, index build, uploads, CPU, memory, wall time). Subsequent compactions
take the ordinary local incremental path.

`background.rs` also drives `NamespaceGraph::maintain` (25s budget) for
governed-deletion recovery and activation-guard resolution. That worker runs
unconditionally, not only when branching is enabled, because ordinary
namespace deletes now depend on it.

## Incremental compaction and centroids

Centroids are reused when `new_from_wal / existing < retrain_imbalance_threshold`.

Trap: **SQ8 is the default quantization.** Tests that build compaction configs
with `..Default::default()` and are *not* testing quantization must explicitly
set `quantization: None`, or they exercise a different path than intended.

## CPU budget

Compaction is capped at `(cpus/4).max(1)` and runs on a dedicated runtime so it
cannot starve the query path. Raising this trades query latency for compaction
throughput; measure before changing.

## Local benchmark gotcha (macOS)

Compaction's ~1000-way parallel GET burst exhausts the macOS ~1 GB mbuf pool at
1M+ vectors. Connections shed and surface as `error decoding response body`.
Workaround for local 1M+ runs is to cap
`net.inet.tcp.auto{rcv,snd}bufmax=262144` (needs sudo) and restore afterward.
This is an environment limit, not a product bug — don't chase it in the code.

## See also

- `../wal/CLAUDE.md` — manifest CAS and pruning
- `../storage/CLAUDE.md` — the key-ownership classifier
- `../index/CLAUDE.md` — what the builders produce
