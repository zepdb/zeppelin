# Changelog

All notable changes to Zeppelin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

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
- GCS and Azure Blob backends are implemented behind `[storage] backend`
  with emulator-backed test gates (patched fake-gcs-server, Azurite); no
  gate has run against real GCS/Azure yet.
- Published performance numbers are loopback-MinIO measurements, not
  cloud-S3 latency claims.

[0.1.0]: https://github.com/zepdb/zeppelin/releases/tag/v0.1.0
