use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use zeppelin::config::Config;
use zeppelin::namespace::NamespaceManager;
use zeppelin::server::routes::build_router;
use zeppelin::server::AppState;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{WalReader, WalWriter};

#[tokio::main]
async fn main() {
    // Load .env
    let _ = dotenvy::dotenv();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    tracing::info!("zeppelin starting");

    // Load config
    let config = Config::load(None).expect("failed to load config");

    // Initialize storage
    let store =
        ZeppelinStore::from_config(&config.storage).expect("failed to initialize storage");

    // Initialize namespace manager and scan existing namespaces
    let namespace_manager = Arc::new(NamespaceManager::new(store.clone()));
    match namespace_manager.scan_and_register().await {
        Ok(count) => tracing::info!(count, "registered existing namespaces"),
        Err(e) => tracing::warn!(error = %e, "failed to scan namespaces on startup"),
    }

    // Initialize WAL writer and reader
    let wal_writer = Arc::new(WalWriter::new(store.clone()));
    let wal_reader = Arc::new(WalReader::new(store.clone()));

    // Build application state
    let state = AppState {
        store,
        namespace_manager,
        wal_writer,
        wal_reader,
        config: Arc::new(config.clone()),
    };

    // Build router
    let app = build_router(state);

    // Bind and serve
    let addr = format!("{}:{}", config.server.host, config.server.port);
    tracing::info!(addr = %addr, "listening");

    let listener = TcpListener::bind(&addr)
        .await
        .expect("failed to bind to address");

    axum::serve(listener, app)
        .await
        .expect("server error");
}
