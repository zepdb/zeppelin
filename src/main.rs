use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use zeppelin::cache::DiskCache;
use zeppelin::compaction::background::compaction_loop;
use zeppelin::compaction::Compactor;
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

    // Load config first (needed for logging setup)
    let config = Config::load(None).expect("failed to load config");

    // Initialize tracing from LoggingConfig
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    match config.logging.format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }
    }

    tracing::info!("zeppelin starting");

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

    // Initialize disk cache
    let cache = Arc::new(
        DiskCache::new(&config.cache).expect("failed to initialize disk cache"),
    );

    // Initialize compactor
    let compactor = Arc::new(Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        config.compaction.clone(),
        config.indexing.clone(),
    ));

    // Spawn background compaction loop
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let compactor = compactor.clone();
        let namespace_manager = namespace_manager.clone();
        tokio::spawn(async move {
            compaction_loop(compactor, namespace_manager, shutdown_rx).await;
        });
    }

    // Build application state
    let state = AppState {
        store,
        namespace_manager,
        wal_writer,
        wal_reader,
        config: Arc::new(config.clone()),
        compactor,
        cache,
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

    // Signal shutdown to background tasks
    let _ = shutdown_tx.send(true);
}
