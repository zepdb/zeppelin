pub mod handlers;
pub mod routes;

use std::sync::Arc;

use crate::config::Config;
use crate::namespace::NamespaceManager;
use crate::storage::ZeppelinStore;
use crate::wal::{WalReader, WalWriter};

/// Shared application state injected into all handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    pub store: ZeppelinStore,
    pub namespace_manager: Arc<NamespaceManager>,
    pub wal_writer: Arc<WalWriter>,
    pub wal_reader: Arc<WalReader>,
    pub config: Arc<Config>,
}
