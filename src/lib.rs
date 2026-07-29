//! S3-native vector and full-text retrieval with stateless compute nodes.
//!
//! Zeppelin stores durable namespace metadata, write-ahead-log (WAL) fragments,
//! manifests, and immutable search segments in object storage. S3 or MinIO is
//! the source of truth; process memory and local disk are disposable
//! accelerators. A compute node can therefore restart or be replaced without
//! owning the only copy of user data.
//!
//! The authoritative [`wal::Manifest`] is the visibility boundary. Uploading an
//! immutable WAL fragment or segment is preparation, not publication. Readers
//! discover an artifact only after a successful manifest update. Writers use
//! fencing where applicable and object-store compare-and-swap (CAS) so stale
//! work cannot silently overwrite a newer view.
//!
//! ## Architecture
//!
//! ```text
//!                         HTTP clients
//!                              |
//!                              v
//!                 server: routing and validation
//!                              |
//!                +-------------+--------------+
//!                |                            |
//!                v                            v
//!       namespace + WAL writes          query orchestration
//!                |                     /         |         \
//!                |                    v          v          v
//!                |                 WAL tail   vector     full-text
//!                |                             index       index
//!                |                    \          |          /
//!                |                     +---------+---------+
//!                |                               |
//!                v                               v
//!       authoritative manifest             ranked results
//!                |                               ^
//!                v                               |
//!     compaction: WAL -> immutable segment ------+
//!                |
//!                v
//!       storage::ZeppelinStore -> S3 / MinIO
//!
//!       cache tiers may serve immutable bytes, but never choose visibility
//! ```
//!
//! [`server`] is the transport boundary. [`namespace`] owns namespace metadata
//! and lifecycle. [`wal`] turns accepted mutations into immutable fragments and
//! publishes their references. [`query`] searches the manifest-selected WAL and
//! active segment using [`index`] and [`fts`]. [`compaction`] converts the WAL
//! tail into a new immutable segment and publishes that replacement. All
//! higher-level storage access passes through [`storage::ZeppelinStore`];
//! [`cache`] can reduce remote reads but cannot override object-store state.
//!
//! ## Reading map
//!
//! 1. Start with [`types`] for vectors, filters, distance metrics, consistency,
//!    and index choices, then [`error`] for the fail-loud error vocabulary.
//! 2. Read [`config`] for boot-time contracts and [`runtime_config`] for the
//!    small set of query controls that can change while a process is running.
//! 3. Read [`storage`] for the only object-store gateway, then [`wal`] for
//!    immutable fragments, manifest authority, leases, reads, and publication.
//! 4. Read [`namespace`] for metadata lifecycle and validation boundaries.
//! 5. Read [`index`] for ANN segment formats and [`fts`] for lexical indexing
//!    and BM25 ranking.
//! 6. Read [`cache`] and [`query`] together to understand how one manifest
//!    snapshot becomes WAL and segment work without making cached state
//!    authoritative.
//! 7. Read [`compaction`] for WAL-to-segment publication and garbage collection.
//! 8. Finish with [`metrics`], [`server`], and [`startup`] for process assembly,
//!    middleware, background work, observability, and shutdown.
//!
//! ## Write and publication example
//!
//! ```text
//! client upserts vectors
//!          |
//!          v
//! validate namespace contract
//!          |
//!          v
//! upload immutable WAL fragment -------- upload fails -> request fails
//!          |
//!          | object exists but is not yet visible
//!          v
//! publish manifest with ETag CAS -------- CAS miss -> reload; do not overwrite
//!          |
//!          v
//! strong readers replay the fragment
//!          |
//!          v
//! compaction builds a new immutable segment
//!          |
//!          v
//! manifest CAS swaps WAL references for the segment reference
//! ```
//!
//! For example, an upsert may successfully upload fragment `wal-42` but lose a
//! manifest CAS race. That object remains unreferenced rather than becoming
//! partially visible. A later safe retry must rebase on the current manifest;
//! it must not treat object existence as proof that the write was published.
//!
//! ## Query example
//!
//! ```text
//! request + namespace metadata
//!             |
//!             v
//! choose one manifest snapshot
//!   Strong: verify current object-store state
//!   Eventual: bounded manifest-cache reuse is allowed
//!             |
//!       +-----+------------------+
//!       |                        |
//!       v                        v
//! active immutable segment   visible WAL fragments
//! ANN and/or BM25 search      latest writes and tombstones
//!       |                        |
//!       +------------+-----------+
//!                    v
//!       suppress stale/deleted IDs and rank top-k
//!                    |
//!                    v
//!       optional fusion, rerank, facets, grouping, or cursor page
//! ```
//!
//! A strong query includes committed WAL upserts that compaction has not yet
//! placed in a segment. An eventual query may skip those recent upserts to save
//! work, but still applies visible tombstones so an already-known delete is not
//! resurrected from an older segment. ANN scores are distances where smaller is
//! better; BM25 scores are relevance values where larger is better. Hybrid
//! orchestration must normalize or rank-fuse those different score spaces.
//!
//! ## System invariants
//!
//! - **Object storage is authoritative.** Memory and local disk may cache bytes
//!   selected by authoritative metadata; neither tier may invent visibility.
//! - **The manifest commits visibility.** An artifact is not readable merely
//!   because its object exists. A stale ETag must never replace a newer manifest.
//! - **Artifacts are immutable.** WAL fragments and segments are write-once.
//!   Replacement creates new objects and publishes new references.
//! - **Failures stay explicit.** Storage, decoding, validation, and index errors
//!   propagate as [`error::ZeppelinError`]; the engine does not substitute empty
//!   state or a quieter retrieval path.
//! - **Namespace writes have one active owner.** Lease fencing and manifest CAS,
//!   where used, provide complementary protection against stale or zombie work.
//! - **One query uses one visibility snapshot.** Source searches, filters,
//!   enrichment, and reranking must not silently mix manifest generations.
//! - **Async workers must remain responsive.** Remote I/O is asynchronous and
//!   CPU-heavy/background work uses the execution boundary chosen by its module
//!   rather than blocking Tokio request threads.
//! - **Types carry domain rules.** Enums, newtypes, and validated configuration
//!   distinguish score direction, consistency, artifact identity, and legal
//!   states instead of relying on unrelated strings and integers.
//!
//! ## Rust concepts used here
//!
//! The public modules below form a compiler-checked facade, similar to Java
//! packages or C headers, while keeping implementation details in narrower
//! submodules. [`index::VectorIndex`] is a trait boundary: it resembles a Java
//! interface or a C function-pointer table, with additional ownership and
//! `Send + Sync` guarantees checked by the compiler.
//!
//! Borrowed values such as `&[f32]` let query and index code inspect caller-owned
//! vectors without copying them. Unlike a Java reference or C pointer, a Rust
//! borrow is non-null and cannot outlive its owner. Owned values move between
//! phases; [`std::sync::Arc`] is used when async tasks need shared ownership.
//! Cloning an `Arc` increments a reference count rather than deep-copying the
//! service or cached object.
//!
//! [`Result`] and [`Option`] make
//! failure and absence explicit in signatures. The `?` operator propagates an
//! error while Rust's RAII cleanup drops owned values and guards on every return,
//! comparable to Java `finally` or carefully maintained C cleanup labels.
//! Tagged enums and exhaustive `match` expressions provide a checked analogue
//! to Java sealed types or C tagged unions. Async functions suspend without
//! blocking a thread, while module-specific synchronization types make shared
//! mutation and lock lifetime visible in the type system.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
// Test modules assert with expect/unwrap by design: a failed assertion
// should abort the test loudly. The denies above exist to keep that
// style out of production paths, where the house idiom is an explicit
// `unwrap_or_else(|_| panic!("..."))` carrying context.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![deny(missing_docs)]

/// Disposable memory/disk caches, manifest freshness caching, and segment hydration.
///
/// Begin with [`cache::DiskCache`] for immutable byte lookup, then
/// [`cache::manifest_cache`] for current-manifest TTL/verification rules and
/// [`cache::hydration`] for speculative warming. Cache hits optimize reads; they
/// never make an artifact visible or newer than its selected manifest.
pub mod cache;
/// Lease-protected WAL-to-segment compaction and reachability-aware garbage collection.
///
/// [`compaction::Compactor`] reads manifest-selected WAL state, builds immutable
/// vector/FTS artifacts, and publishes a rebased manifest with fencing and CAS.
/// Uploaded outputs remain invisible until publication succeeds. The
/// [`compaction::background`] and [`compaction::gc`] modules own scheduling and
/// conservative physical reclamation.
pub mod compaction;
/// Boot-time configuration, validation, defaults, environment loading, and CPU budgets.
///
/// [`config::Config`] is the root process configuration. Its validation rejects
/// unsafe or contradictory settings before services start; individual subsystem
/// structs preserve units and bounds close to the values they control.
pub mod config;
/// Typed failure vocabulary and the crate-wide result alias.
///
/// [`error::ZeppelinError`] classifies validation, storage, manifest, WAL,
/// namespace, index, and HTTP-facing failures. Callers match variants or use the
/// response classification helpers rather than parsing error strings.
pub mod error;
/// Tokenization, BM25 scoring, rank expressions, immutable inverted indexes, and WAL scans.
///
/// [`fts::FtsFieldConfig`] defines compatible build/query tokenization. Segment
/// indexes accelerate compacted text while the WAL path supplies strong-read
/// coverage for visible uncompacted updates. BM25 relevance is higher-is-better.
pub mod fts;
/// Decode seams for the `fuzz/` libFuzzer targets, gated behind the `fuzz`
/// feature so the crate-internal two-bit decoders stay internal otherwise.
#[cfg(feature = "fuzz")]
pub mod fuzz;
/// Approximate-nearest-neighbor indexes, distance kernels, quantization, and bitmap filters.
///
/// [`index::VectorIndex`] defines the build/search boundary implemented by
/// IVF-Flat and hierarchical indexes. Index builds write immutable segment
/// artifacts but do not publish them; compaction owns the manifest visibility
/// transition. ANN distance results are lower-is-better.
pub mod index;
/// Prometheus collectors and RAII helpers for request, storage, cache, and background work.
///
/// [`metrics::init`] registers collectors before serving. Metric handles are
/// process-local observations, never authority for data or manifest state.
pub mod metrics;
/// Namespace metadata, lifecycle, durable tombstones, and the disposable registry.
///
/// [`namespace::NamespaceManager`] is the boundary used by startup and HTTP
/// handlers. Namespace metadata is stored in object storage; its in-process
/// registry accelerates lookup but may be rebuilt.
pub mod namespace;
/// Manifest-scoped ANN/BM25 execution across WAL and the active immutable segment.
///
/// Start with [`query::QueryParams`] and [`query::execute_query`]. This module
/// enforces latest-write-wins suppression, filters candidates, merges WAL and
/// segment results, and reports storage/cache diagnostics. HTTP-level hybrid
/// fusion and presentation transforms are layered above it.
pub mod query;
/// Policy-scope-derived retrieval artifacts and their integrity errors.
///
/// Query execution owns the concrete artifact types; the public error type is
/// exposed so [`error::ZeppelinError`] can preserve the subsystem boundary.
pub mod retrieval_scope;
/// Atomically published snapshots of the query controls allowed to change at runtime.
///
/// [`runtime_config::RuntimeQueryConfig`] validates patches and gives each query
/// one immutable [`runtime_config::QueryKnobs`] snapshot, preventing a request
/// from mixing old and new tuning values mid-execution.
pub mod runtime_config;
/// Typed principals, exhaustive actions, credential adapters, and central authorization.
pub mod security;
/// Axum application state, routing, middleware, rate limiting, and HTTP handlers.
///
/// [`server::AppState`] shares initialized services with request futures and
/// [`server::build_router`] assembles the API/middleware stack. Handlers validate
/// wire contracts and delegate durable behavior to domain modules.
pub mod server;
/// Process configuration discovery, service construction, background tasks, and shutdown.
///
/// [`startup::build_app`] validates configuration, probes storage, constructs
/// managers/caches/writers, starts background compaction/hydration, and returns
/// the router plus owned shutdown handles.
pub mod startup;
/// The normalized object-store gateway used by every higher-level subsystem.
///
/// [`storage::ZeppelinStore`] wraps S3/MinIO and local test backends, including
/// ranged reads, immutable writes, conditional updates, listing, and bounded
/// cleanup. Code above this layer does not call `object_store` directly.
pub mod storage;
/// Explicit wall-clock source used by correctness-sensitive components.
///
/// [`time::Clock`] defaults to the system wall clock in production and accepts
/// an injected [`time::TimeSource`] during construction for deterministic fault
/// testing. Monotonic elapsed-time paths continue to use `std::time::Instant`.
pub mod time;
/// Shared vectors, attributes, filters, score metrics, consistency, and index choices.
///
/// Read [`types::VectorEntry`], [`types::Filter`],
/// [`types::DistanceMetric`], and [`types::ConsistencyLevel`] first when tracing
/// data across HTTP, WAL, compaction, and query layers.
pub mod types;
/// Immutable WAL fragments, authoritative manifests, leases, readers, and writers.
///
/// [`wal::WalWriter`] uploads fragments before CAS publication,
/// [`wal::WalReader`] replays only manifest-selected fragments, and
/// [`wal::Manifest`] defines all visible WAL/segment membership. [`wal::Lease`]
/// and fencing complement CAS for long-running namespace work.
pub mod wal;
