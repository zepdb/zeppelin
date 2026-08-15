# src/wal — fragments, manifest, leases

The manifest is the **visibility boundary**. Uploading a WAL fragment or a
segment object does not make it queryable; only a successful CAS that installs
its reference does. `manifest.rs` is ~9.5k lines and is the single most
load-bearing file in the repo — read its rustdoc before editing.

## Serialization rules (these have caused real bugs)

Manifests and WAL fragments use **MessagePack with a leading version byte**:
`[0x01][MessagePack payload]` (`MANIFEST_FORMAT_MSGPACK = 0x01`,
`manifest.rs:157`). Decoders auto-detect legacy JSON by sniffing a leading
`{`, so both formats must keep round-tripping.

Because the format is not fully self-describing:

- **Never put `#[serde(untagged)]` or `#[serde(skip_serializing_if)]` on any
  type reachable from `Manifest` or `WalFragment`.** Check nested types, not
  just the top-level struct. Types carrying those attributes must use a
  self-describing format (JSON/MessagePack-with-names/CBOR).
  Note the contrast: `NamespaceMetadata` in `../namespace/` *does* use
  `skip_serializing_if`, and that is fine because `meta.json` is JSON.
- **Never checksum non-deterministic serialization.** `HashMap` iteration
  order is not stable across round-trips. Canonicalize through `BTreeMap`
  first. This is why `branch_roots` is a `BTreeMap<BranchId, BranchRoot>`
  and not a `HashMap`.

`ManifestExecutionBindingV1`/`V2` are the checksum-input shapes. V2 added
`artifact_origins`; `branch_roots` belongs to the V3/V4 control projection. If
you add a manifest field that participates in identity, it needs a binding
version — do not silently extend V2 in place, or you invalidate every existing
checksum.

## Artifact origins (branching)

A manifest entry has a **logical** identity (which namespace owns the row) and
a **physical** origin (which namespace's prefix actually holds the bytes). A
copy-on-write branch has visible refs whose physical origin points at the
*source* namespace. That is what makes fork O(1) instead of a full copy.

- `artifact_origins: Vec<ArtifactOrigin>` is the origin table; entries
  reference it by index (`Option<u32>`), `None` meaning "local".
- `visible_refs_are_local()` answers "is this branch fully materialized?"
- Anything that turns a ref into an object key **must** go through the origin
  resolver. Reconstructing a key from the namespace name is the bug this whole
  subsystem exists to prevent.

## Writer contract

`append()` and `append_with_lease()` return `Result<(WalFragment, Manifest)>`
— destructure both. Returning the manifest is what enables write-through
manifest caching; do not discard it.

Group commit and the last-committed manifest/ETag memo are local to one
`WalWriter` instance/process (`writer.rs:404-417`). Attempt zero for an
unguarded batch may start from that memo (`writer.rs:1047-1056`); when a
version-reporting backend alternates one namespace's writes between nodes that
have both committed, the returning node presents a stale ETag, loses its first
CAS, clears the memo, and requests a 10–19 ms backoff before the fresh-read
retry (`writer.rs:147-150, 1135-1144`). This is the latency reason to keep a
namespace's v1 write path sticky to one process even though manifest CAS
prevents silent lost updates (`writer.rs:565-568`).

Lease release is **best-effort**. A process whose lease expired and was taken
over must handle release gracefully (Ok, or a non-fatal error). It must never
block or deadlock. Two-layer defense for distributed writes is deliberate:
fencing check **and** CAS. Fencing alone has a TOCTOU gap; CAS alone does not
detect a stale token. Do not remove either layer.

First lease acquisition is a create-only PUT, relying on the capability that
every substrate declares and the boot probe verifies. A create collision must
re-read the authoritative lease within the bounded five-attempt acquisition
loop; never fall back to an unconditional PUT.

## Manifest pruning

`prune()` caps **old segments only** (default 10). `pending_deletes` is
deliberately *not* capped — the `max_pending_deletes` parameter is unused.
Capping it would leak S3 objects. GC owns the deletes-then-prune ordering.
This looks like dead code and is not; leave it alone.

## See also

- `../storage/CLAUDE.md` — CAS mechanics and the `Local`-backend limitation
- `../compaction/CLAUDE.md` — who replaces fragment refs with segment refs
