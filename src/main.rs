//! Zeppelin server process entry point and top-level graceful-shutdown order.
//!
//! The binary keeps policy-light orchestration here and delegates dependency
//! construction to [`zeppelin::startup::build_app`]. It loads optional dotenv
//! values, resolves/loads configuration, installs logging, binds the Axum
//! listener, serves requests until SIGINT/SIGTERM, then signals and joins the
//! dedicated compaction thread.
//!
//! ```text
//! dotenv (best effort) -> Config::load -> init logging -> build_app
//!                                                    |
//!                                                    v
//!                                            bind TCP listener
//!                                                    |
//!                                                    v
//!                                    serve until SIGINT/SIGTERM
//!                                                    |
//!                                                    v
//!                                      graceful HTTP drain completes
//!                                                    |
//!                                                    v
//!                                      drain durable audit writer
//!                                                    |
//!                                                    v
//!                               signal + join compaction thread -> exit
//! ```
//!
//! The application router and store remain responsible for S3/manifest
//! authority; this file never reads local data as a substitute. Once startup
//! returns owned background handles, every bind/serve outcome is settled only
//! after audit drain and compaction shutdown have both been attempted.
//!
//! ## Rust concepts used here
//!
//! `#[tokio::main]` creates the primary async runtime. `tokio::select!` races
//! owned signal futures and cancels the losing wait. The shutdown future is
//! passed to Axum, which stops accepting new work and waits for in-flight
//! services before returning. `?` then propagates bind, serve, and shutdown
//! errors to Rust's runtime as a nonzero process exit. Audit drains before the
//! compaction thread is stopped so every already-accepted HTTP event settles.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpListener;

use zeppelin::config::Config;
use zeppelin::security::AuditRuntime;
use zeppelin::startup::{
    build_app, init_logging, resolve_config_path, shutdown_background_tasks, BackgroundTasks,
};

/// Drain owned process services while preserving the primary server result.
async fn settle_server_and_backgrounds(
    server_result: Result<(), Box<dyn std::error::Error>>,
    audit_runtime: AuditRuntime,
    background_tasks: BackgroundTasks,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let audit_result = audit_runtime.shutdown().await;
    let background_result = shutdown_background_tasks(background_tasks, shutdown_timeout).await;

    if let Err(server_error) = server_result {
        if let Err(audit_error) = &audit_result {
            tracing::error!(error = %audit_error, "audit drain also failed after server error");
        }
        if let Err(background_error) = &background_result {
            tracing::error!(
                error = %background_error,
                "background shutdown also failed after server error"
            );
        }
        return Err(server_error);
    }
    if let Err(audit_error) = audit_result {
        if let Err(background_error) = &background_result {
            tracing::error!(
                error = %background_error,
                "background shutdown also failed after audit drain error"
            );
        }
        return Err(audit_error.into());
    }
    background_result.map_err(Into::into)
}

/// Loads configuration, serves HTTP, and coordinates process shutdown.
///
/// # Returns
///
/// `Ok(())` after graceful HTTP and audit drains plus normal
/// compaction-thread shutdown.
///
/// # Errors
///
/// Propagates dotenv-independent configuration loading, application startup,
/// TCP bind, Axum serve, audit drain, or compaction shutdown errors. After
/// application construction succeeds, a bind/serve error remains the primary
/// result but does not bypass audit drain or compaction shutdown.
///
/// # Panics
///
/// On Unix, panics if installing the SIGTERM handler fails. Logging setup may
/// also panic if another global tracing subscriber was installed first.
///
/// # Side Effects
///
/// Reads `.env`, environment variables, and configuration files; initializes
/// global logging/metrics; probes storage; creates cache state; starts hydration
/// and compaction; binds a network socket; serves HTTP; and listens for process
/// signals.
///
/// # Examples
///
/// With defaults, the process loads `./zeppelin.toml` when present, binds the
/// configured host/port, and on Ctrl-C drains Axum before waiting up to the
/// configured shutdown timeout for compaction after flushing queued audit
/// records.
///
/// # Rust Notes for Java/C Engineers
///
/// The `async` blocks borrow or own exactly the state they need. `config` is
/// cloned once because [`build_app`] takes ownership while this function still
/// needs bind/shutdown settings. The compaction `JoinHandle` is moved into the
/// shutdown function, enforcing a single join owner at compile time. The audit
/// runtime is likewise moved into its one graceful-shutdown call.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env
    let _ = dotenvy::dotenv();

    // Load config (priority: ZEPPELIN_CONFIG env var > ./zeppelin.toml; no file fails closed)
    let config = Config::load(resolve_config_path().as_deref())?;

    // Initialize logging
    init_logging(&config);

    // Build application (router + background tasks)
    let (app, background_tasks, audit_runtime) = build_app(config.clone()).await?;

    // Bind and serve
    let addr = format!("{}:{}", config.server.host, config.server.port);
    tracing::info!(addr = %addr, "listening");

    let server_result: Result<(), Box<dyn std::error::Error>> = async {
        let listener = TcpListener::bind(&addr).await?;
        let shutdown_signal = async {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            let terminate = async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install signal handler")
                    .recv()
                    .await;
            };
            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();
            tokio::select! {
                _ = ctrl_c => tracing::info!("received SIGINT"),
                _ = terminate => tracing::info!("received SIGTERM"),
            }
        };

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal)
        .await?;
        Ok(())
    }
    .await;

    tracing::info!("server stopped, settling owned background tasks");
    settle_server_and_backgrounds(
        server_result,
        audit_runtime,
        background_tasks,
        Duration::from_secs(config.server.shutdown_timeout_secs),
    )
    .await?;
    tracing::info!("zeppelin shutdown complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use object_store::memory::InMemory;
    use tokio::sync::watch;

    use zeppelin::security::{AuditRecord, AuditRuntime};
    use zeppelin::startup::BackgroundTasks;
    use zeppelin::storage::ZeppelinStore;

    use super::settle_server_and_backgrounds;

    #[tokio::test]
    async fn primary_server_error_still_drains_audit_and_joins_backgrounds() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let (audit, audit_runtime) =
            AuditRuntime::start(store.clone(), "main-error-test", Duration::from_secs(60))
                .await
                .unwrap();
        audit
            .submit_buffered(AuditRecord::open_unsafe_boot(Utc::now(), audit.node_id()))
            .unwrap();

        let (shutdown_tx, mut observer_rx) = watch::channel(false);
        let license_observer = tokio::spawn(async move {
            let _ = observer_rx.changed().await;
        });
        let joined = Arc::new(AtomicBool::new(false));
        let joined_by_thread = Arc::clone(&joined);
        let background = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            joined_by_thread.store(true, Ordering::SeqCst);
        });
        let primary: Result<(), Box<dyn std::error::Error>> =
            Err(Box::new(std::io::Error::other("primary serve failure")));
        let backgrounds = BackgroundTasks::from_parts(shutdown_tx, background, license_observer);

        let result = settle_server_and_backgrounds(
            primary,
            audit_runtime,
            backgrounds,
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(result.unwrap_err().to_string(), "primary serve failure");
        assert!(joined.load(Ordering::SeqCst));
        assert_eq!(store.list_prefix("_audit/").await.unwrap().len(), 1);
    }
}
