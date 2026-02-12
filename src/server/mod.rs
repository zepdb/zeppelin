pub mod handlers;
pub mod middleware;
pub mod routes;

use std::sync::Arc;

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
    pub store: ZeppelinStore,
    pub namespace_manager: Arc<NamespaceManager>,
    pub wal_writer: Arc<WalWriter>,
    pub wal_reader: Arc<WalReader>,
    pub config: Arc<Config>,
    pub compactor: Arc<Compactor>,
    pub cache: Arc<DiskCache>,
    pub manifest_cache: Arc<ManifestCache>,
    pub fts_cache: Arc<WalFtsCache>,
    pub query_semaphore: Arc<Semaphore>,
    /// Optional batched WAL writer (enabled when batch_manifest_size > 1).
    pub batch_wal_writer: Option<Arc<BatchWalWriter>>,
}
