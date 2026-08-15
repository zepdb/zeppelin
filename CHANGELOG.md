# Changelog

All notable changes to Zeppelin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Manifest envelope v2 adds a self-describing MessagePack header with the
  writing Zeppelin version, minimum reader version, and write timestamp while
  preserving the existing positional payload byte-for-byte. Readers always
  accept v2; writers remain on v1 by default behind
  `storage.manifest_envelope = 2` / `ZEPPELIN_MANIFEST_ENVELOPE=2`.
- IVF-Flat bootstrap v3 embeds an exact, bounded per-segment filter-cardinality
  summary for bitmap-indexed equality values. The bounds are configured by
  `indexing.filter_summary_max_values_per_field` (default 4096) and
  `indexing.filter_summary_max_bytes` (default 1 MiB).

### Changed

- Manifest rolling-upgrade sequence: first upgrade every node to this release,
  so every reader accepts envelopes v1 and v2; then set
  `storage.manifest_envelope = 2` on every writer. The default will flip to v2
  in the next minor release, and the compatibility knob will be removed one
  minor release later. Enabling v2 before the fleet upgrade makes older nodes
  refuse the unknown prefix explicitly.
- Retroactive compatibility warning: rmp-serde positional structs reject an
  N+1-field payload when decoded as the older N-field struct (`array had
  incorrect length, expected N`). Earlier manifest additions documented as
  "must remain trailing" therefore protected new readers of old manifests but
  were already silent pre-1.0 flag days for old readers of new manifests.
  Envelope v2 replaces that generic failure with an explicit minimum-reader
  diagnostic for future changes.
- A rolling downgrade across this commit cannot read segments compacted by the
  newer binary: older binaries reject the new v3 bootstrap objects. This is a
  pre-1.0 immutable-format compatibility break; v1/v2 objects remain readable
  by the newer binary.

## [0.2.0] - 2026-08-15

### Added

- **Google Cloud Storage and Azure Blob Storage backends**, selected by
  `[storage] backend = "gcs" | "azure"` beside the existing `s3` and
  `local`. New flat config fields with env overrides: `gcs_service_account_path`
  / `gcs_service_account_key` / `gcs_endpoint` (`GCS_SERVICE_ACCOUNT_PATH`,
  `GCS_SERVICE_ACCOUNT_KEY`, `GCS_ENDPOINT`) and `azure_account_name` /
  `azure_access_key` / `azure_endpoint` / `azure_use_emulator` /
  `azure_allow_http` (`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCESS_KEY`,
  `AZURE_ENDPOINT`, `AZURE_USE_EMULATOR`, `AZURE_ALLOW_HTTP`). GCS conditional
  writes key on the object generation, Azure on the ETag; both ride the same
  `StorageVersion` seam as S3.
- **Storage capability model** (`StorageCapabilities`): the engine asks what
  the configured substrate can do — conditional PUT and its token kind,
  native batch delete, delete-of-absent semantics, metadata naming rules —
  instead of comparing backend identity. Under `storage.fail_fast = true`
  every boot now verifies the declared capabilities live (create-only PUT,
  fresh and stale conditional PUT, LIST-vs-GET ETag identity, delete of an
  absent key) on a reserved `__zeppelin_probe__/` prefix and refuses to
  start if the substrate cannot enforce them — the flagship catch is an
  S3-compatible store without conditional-PUT support, where every
  compare-and-swap would otherwise silently become an overwrite.
- `zeppelin_advisor emit-config --cloud gcp|azure` now emits configs that
  pass validation and construct a store (previously they died at boot).
- Substrate parity test suite (`tests/substrate_contract_tests.rs`),
  emulator fidelity probes (`tests/emulator_fidelity_probe.rs`), and native
  emulator setup for fake-gcs-server and Azurite in `scripts/emulators/`.

### Changed

- Config validation is stricter about storage: fields from a non-selected
  backend family (for example `gcs_*` with `backend = "s3"`) are a hard
  error, `gcs_service_account_path` and `gcs_service_account_key` are
  mutually exclusive, and `backend = "azure"` requires an account name
  unless `azure_use_emulator = true`. `security.audit_s3` and `security.rbac`
  are rejected at config-parse time on a backend without conditional PUT
  (`local`) instead of failing mid-boot with a raw storage error.
- Deleting an absent object is success on every backend (S3 already reported
  success; GCS/Azure/local NotFound is normalized at the storage seam so
  garbage-collection drain idempotency holds identically everywhere).
- LIST-vs-GET ETag comparisons use a canonical (quote-stripped) form: Azure
  lists ETags unquoted and returns them quoted on GET, so byte comparison
  would have permanently disabled garbage collection there. S3 behavior is
  unchanged.
- Object user-metadata names are canonicalized per substrate at the seam:
  Azure requires C#-identifier names, so hyphenated logical keys are written
  with underscores there and normalized back on read; S3 and GCS keep the
  hyphenated wire form.
- Storage-gateway tracing strings say `object-store <op>` instead of
  `s3 <op>`, key constructors are `object_store_key()` instead of
  `s3_key()`, and endpoint-parse errors name object-store URLs. Metrics keep
  their `zeppelin_storage_*` names; `s3_*` config fields keep theirs.
- `ZeppelinStore::head()` returns the seam's `ListedObject` (with a
  `StorageVersion`) rather than a raw `object_store::ObjectMeta`.
- Removed licensing entirely: no feature is gated behind a signed license
  anymore. The RBAC policy authority is now selected by the new
  `security.rbac` config flag (default `false`, preserving the previous
  default boot), delegated tokens compose whenever
  `security.token_signing_key_path` is set, preservation composes on every
  RBAC boot, and durable audit is driven purely by `security.audit_s3`
  (default now `false`; it requires an S3-compatible backend and a signing
  key). Branching remains gated only by `branching.enabled`.
- Error codes `feature_not_licensed`, `license_expired`, and
  `license_limit_exceeded` are replaced by a single 403 `feature_disabled`;
  the post-expiry management freeze, the `max_principals` limit, the
  `zeppelin_license_expiry_seconds` metric, and the `zeppelin_license` CLI
  are gone.

### Removed

- The `zeppelin_license` CLI and every license-gated code path (see Changed).
- `ZeppelinStore::new_with_native_batch_delete`; use
  `new_with_capabilities` with the matching `StorageCapabilities` row.

### Known limitations

- Pre-1.0: no on-disk format stability guarantee between versions.
- **GCS and Azure gates ran against emulators only** — a patched
  fake-gcs-server (stock cannot serve `object_store`'s XML-API writes) and
  stock Azurite 3.36.0. No gate has run against real GCS or real Azure, and
  per-substrate performance is unmeasured. Treat both as implemented but not
  yet production-validated; S3 and S3-compatible stores remain the
  battle-tested substrates.
- Namespace branching is disabled by default and its release validation
  gates are not complete.
- Published performance numbers are loopback-MinIO measurements, not
  cloud-S3 latency claims.

## [0.1.0] - 2026-08-12

Initial release.

### Added

- S3-native architecture: object storage (S3, MinIO, S3-compatible) is the
  single source of truth; nodes are stateless and any node can serve any
  namespace.
- Write-ahead log of immutable fragments with ETag-CAS-committed manifests
  and lease-fenced background compaction into indexed segments.
- Scale-aware IVF-Flat vector indexing with hierarchical ANN.
- Quantization: 2-bit Extended-RaBitQ with exact f32 rerank (production
  default), SQ8, PQ, and f16 storage.
- BM25 full-text search with configurable tokenization, stemming, and
  multi-field `rank_by` expressions (opt-in segment indexes).
- Late-interaction (multi-vector MaxSim) retrieval with asynchronous
  semantic enrichment and RRF fusion.
- RoaringBitmap attribute pre-filters with `and`/`or`/`not`/`in`/range/
  `contains` composition.
- Per-query strong or eventual consistency selection.
- Licensed security suite: fail-closed authorization kernel, RBAC, durable
  audit log, delegation tokens, and preservation holds.
- Licensed, disabled-by-default namespace forks (copy-on-write branching;
  fork only, no merge).
- Sizing advisor (`zeppelin_advisor`) with an embedded cloud pricing catalog
  and validated config emission.
- HTTP API defined by an OpenAPI 3.1 spec (`api/zeppelin-api.yaml`,
  versioned independently of the crate).
- Docker image published to `ghcr.io/zepdb/zeppelin`.

### Known limitations

- Pre-1.0: no on-disk format stability guarantee between versions.
- Namespace branching is disabled by default and its release validation
  gates are not complete.
- GCS and Azure backends are planned, not implemented.
- Published performance numbers are loopback-MinIO measurements, not
  cloud-S3 latency claims.

[Unreleased]: https://github.com/zepdb/zeppelin/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/zepdb/zeppelin/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/zepdb/zeppelin/releases/tag/v0.1.0
