# src/storage — object-store boundary

`ZeppelinStore` is the **only** place in the codebase allowed to touch
`object_store`. If you find yourself importing `object_store` above this
directory, you are in the wrong layer.

Module rustdoc in `mod.rs` explains the architecture. This file records the
traps that rustdoc does not.

## Backends and the CAS trap

`StorageBackend` has three variants. They do **not** have equal capabilities:

| Backend | ETag CAS (`put_if_match`) | Notes |
| --- | --- | --- |
| `S3` (also MinIO) | yes | requires `.with_conditional_put(S3ConditionalPut::ETagMatch)` |
| `Local` | **no** — returns `Storage(NotImplemented)` | `config.rs` marks it "development/testing only" |
| `InMemory` (tests only) | yes | what `TEST_BACKEND=memory` uses |

Two consequences that have already cost real debugging time:

1. **Never drop `S3ConditionalPut::ETagMatch` from the S3 builder**
   (`store.rs:445`). Without it `put_opts` with `PutMode::Update` returns
   `NotImplemented` and every CAS in the system silently stops working.
   `store.rs:389` and `:1444` carry warnings about this — leave them there.
2. **Any new code that requires CAS breaks the `Local` backend.** Code paths
   reachable at boot are especially dangerous, because a `Local`-backed
   process then cannot start at all. As of the branching work,
   `PolicyPublicationLease::release` does an ETag CAS on the boot path, which
   is why `cargo test --lib` currently has 3 failures on a machine without
   MinIO. Before adding CAS to a startup path, decide explicitly whether
   `Local` is still meant to boot.

`put()` borrows the key as `&str`. When writing many objects in parallel,
pre-serialize into `Vec<(String, Bytes)>` first, then fan out — otherwise the
borrows fight the futures.

## Namespace key ownership (`namespace_key.rs`)

Destructive work must never infer ownership from a loose string prefix.
`NamespaceObjectKey::classify` accepts only the exact `<namespace>/` prefix
plus a closed set of known families, and **fails closed** on anything unknown.

Families: `Metadata` (`meta.json`), `Manifest` (`manifest.json`), `Lease`,
`ManifestHistory` (`manifests/`), `Snapshot` (`snapshots/`), `Wal` (`wal/`),
`Segment` (`segments/`), `Staging` (`_staging/`), `Gc` (`_gc/`),
`BranchVisibilityRemoved` (`_lifecycle/`).

Rules encoded there, do not weaken them:

- Only `Wal` and `Segment` may appear in a manifest's `pending_deletes`
  (`allows_deferred_delete`). Control/history/snapshot/staging/GC/lifecycle
  objects have separate ownership protocols.
- `delete_namespace_objects_paged` always retains `meta.json` as the lifecycle
  tombstone, and **refuses to run** while a live `manifest.json` still exists.
- Adding a new object family means adding a variant here. A new key shape that
  is not classified will stop deletion rather than be deleted by default —
  that is the intended behavior, not a bug to route around.

This module is what makes "a branch target delete never issues a
foreign-prefix DELETE" true. Changes here need `tests/storage_gc_tests.rs`
and `tests/artifact_origin_tests.rs`.

## See also

- `../wal/CLAUDE.md` — who calls `put_if_match` and why
- `../namespace/CLAUDE.md` — governed deletion, which drives the paged delete
