//! Zeppelin: S3-native vector search engine.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(missing_docs)]

/// Cache layer (disk + memory) for S3 data.
pub mod cache;
/// WAL-to-segment compaction engine.
pub mod compaction;
/// Configuration types for all subsystems.
pub mod config;
/// Error types and result aliases.
pub mod error;
/// Full-text search: tokenization, BM25 scoring, inverted indexes.
pub mod fts;
/// Vector indexing: IVF-Flat, hierarchical, quantization, bitmaps.
pub mod index;
/// Prometheus metrics definitions and helpers.
pub mod metrics;
/// Namespace CRUD and metadata management.
pub mod namespace;
/// Query execution: WAL scan, segment search, result merging.
pub mod query;
/// HTTP server (axum handlers and middleware).
pub mod server;
/// Startup and initialization routines.
pub mod startup;
/// S3 object storage abstraction.
pub mod storage;
/// Shared domain types (vectors, filters, distance metrics).
pub mod types;
/// Write-ahead log: fragments, manifest, leases, readers, writers.
pub mod wal;
