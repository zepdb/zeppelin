/// HTTP request handlers for all API endpoints.
pub mod handlers;
/// Custom middleware (request IDs, concurrency limits, metrics).
pub mod middleware;
/// Axum router construction and route definitions.
pub mod routes;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::compaction::Compactor;
use crate::config::Config;
use crate::fts::wal_cache::WalFtsCache;
use crate::namespace::NamespaceManager;
use crate::storage::ZeppelinStore;
use crate::wal::batch_writer::BatchWalWriter;
use crate::wal::{WalReader, WalWriter};

/// Shared application state injected into all handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// S3-backed object store for all persistence operations.
    pub store: ZeppelinStore,
    /// Manages namespace CRUD and metadata.
    pub namespace_manager: Arc<NamespaceManager>,
    /// Writes WAL fragments to S3.
    pub wal_writer: Arc<WalWriter>,
    /// Reads WAL fragments from S3.
    pub wal_reader: Arc<WalReader>,
    /// Global server and indexing configuration.
    pub config: Arc<Config>,
    /// Background WAL-to-segment compactor.
    pub compactor: Arc<Compactor>,
    /// LRU disk cache for segment data.
    pub cache: Arc<DiskCache>,
    /// In-memory manifest cache with TTL.
    pub manifest_cache: Arc<ManifestCache>,
    /// In-memory cache for WAL-level full-text search indexes.
    pub fts_cache: Arc<WalFtsCache>,
    /// Semaphore that caps concurrent in-flight queries.
    pub query_semaphore: Arc<Semaphore>,
    /// Optional batched WAL writer (enabled when batch_manifest_size > 1).
    pub batch_wal_writer: Option<Arc<BatchWalWriter>>,
    /// Per-IP token bucket state for rate limiting.
    /// Maps IP → (available tokens, last refill time).
    pub rate_limiters: Arc<DashMap<IpAddr, (u64, Instant)>>,
}
