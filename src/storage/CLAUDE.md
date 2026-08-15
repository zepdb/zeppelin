# src/storage — object-store boundary

`ZeppelinStore` is the **only** place in the codebase allowed to touch
`object_store`. If you find yourself importing `object_store` above this
directory, you are in the wrong layer. (Declared exceptions: `error.rs`
`#[from]` variants and the error-shape matches in `wal/lease.rs` /
`wal/writer.rs`; integration tests may use raw builders where a test's
purpose requires bypassing the seam, e.g. `tests/emulator_fidelity_probe.rs`.)

Module rustdoc in `mod.rs` explains the architecture. This file records the
traps that rustdoc does not.

## Substrates and the capability matrix (`capabilities.rs`)

Four `StorageBackend` variants construct (S3/MinIO, Gcs, Azure, Local), and
tests additionally wrap `InMemory`. **Nothing outside `from_config`'s
construction match may compare backend identity** — everything else asks
`store.capabilities()`:

| capability | S3/MinIO | Gcs | Azure | Local | InMemory |
| --- | --- | --- | --- | --- | --- |
| `conditional_put` | ETag | **BackendVersion** (generation) | ETag | **None** | ETag |
| `native_batch_delete` | yes | no | no | no | no |
| `delete_absent_is_ok` | yes | no | no | no | no |
| `user_metadata_identifier_names` | no | no | **yes** | no | no |

(`create_only_put`, `copy_if_not_exists`, `list_etag_comparable`, and
`user_metadata` are true everywhere except `user_metadata` on Local.)

The matrix is declared statically and **verified live at boot** under
`storage.fail_fast` by `verify_declared_capabilities` — a create/CAS/stale-
CAS/LIST/delete round-trip on the reserved `__zeppelin_probe__/` prefix
(invisible to namespace discovery, unclassifiable by GC). A deployment whose
substrate can't enforce a declared capability refuses to boot; the flagship
case is a MinIO without conditional-PUT support, where every CAS would
silently become an overwrite.

Traps that have already cost real debugging time:

1. **Never drop `S3ConditionalPut::ETagMatch` from the S3 builder** (the arm
   in `raw_backend_from_config`). Without it `put_opts` with
   `PutMode::Update` returns `NotImplemented` and every CAS in the system
   silently stops working. GCS and Azure conditional puts need no opt-in.
2. **Any new code that requires CAS breaks the `Local` backend.** Boot paths
   are the dangerous ones. Since the capability model, `Config::validate`
   and the startup pre-flight reject `security.rbac`/`security.audit_s3` on
   a CAS-less backend up front; the direct `cargo test --lib` lease tests
   (`security::policy_publication`) still go red without a CAS-capable
   backend because `PolicyPublicationLease::renew` is a conditional PUT.
3. **GCS carries its CAS token only on GET/PUT responses.** A LIST-derived
   `StorageVersion` has an ETag but no generation and must never reach
   `put_if_match*` (`ListedObject.version` rustdoc; audited across all 25
   production call sites in `tasks/multi-substrate/08-release-evidence.md`).
4. **Cross-observation ETag comparisons go through `canonical_etag`.** Azure
   returns unquoted ETags in LIST and quoted ones on GET/PUT (real-cloud
   behavior); raw byte equality silently works on S3 and fail-closes forever
   on Azure.
5. **Delete-of-absent is normalized to success at the seam** (S3 reports
   success natively; GCS/Azure/Local/InMemory report NotFound). GC's drain
   idempotency depends on the normalized contract. The raw substrate
   behavior is still verified by the boot probe.
6. **Azure metadata wire names may not contain hyphens.** The seam lowers
   logical hyphenated keys to underscores on identifier-name substrates and
   normalizes back on read; the mapping is bijective only because
   `ObjectUserMetadata::insert` restricts logical keys to lowercase
   alphanumerics + hyphen. Do not loosen that alphabet.

`put()` borrows the key as `&str`. When writing many objects in parallel,
pre-serialize into `Vec<(String, Bytes)>` first, then fan out — otherwise the
borrows fight the futures.

## Emulator test story

`TEST_BACKEND=gcs` runs against a **patched** fake-gcs-server (stock cannot
serve `object_store`'s XML-API writes; patch + build script in
`scripts/emulators/`, fidelity arbitrated by
`tests/emulator_fidelity_probe.rs`). `TEST_BACKEND=azurite` runs against
stock Azurite. No gate has run against real GCS/Azure — an explicit
operator decision recorded in `tasks/multi-substrate/00-overview.md`.

## Namespace key ownership (`namespace_key.rs`)

Destructive work must never infer ownership from a loose string prefix.
`NamespaceObjectKey::classify` accepts only the exact `<namespace>/` prefix
plus a closed set of known families, and **fails closed** on anything unknown.

Families (19): `Metadata` (`meta.json`), `Manifest` (`manifest.json`),
`Lease`, `ManifestHistory` (`manifests/`), `Snapshot` (`snapshots/`), `Wal`
(`wal/`), `InputWal`, `Source`, `Segment` (`segments/`), `LateSection`,
`MatrixFragment`, `FdeFragment`, `FdeTransform`, `Centering`, `Quarantine`,
`LateSegment`, `Staging` (`_staging/`), `Gc` (`_gc/`),
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

- `capabilities.rs` rustdoc — the full matrix and `canonical_etag`
- `../wal/CLAUDE.md` — who calls `put_if_match` and why
- `../namespace/CLAUDE.md` — governed deletion, which drives the paged delete
- `../../tests/CLAUDE.md` — `TEST_BACKEND` values and emulator setup
- `../../scripts/emulators/README.md` — pinned emulator versions and the
  fake-gcs-server patch
