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
//!                               signal + join compaction thread -> exit
//! ```
//!
//! The application router and store remain responsible for S3/manifest
//! authority; this file never reads local data as a substitute. An error before
//! the explicit shutdown call propagates from `main`; in that path the owned
//! compaction join handle is dropped rather than joined.
//!
//! ## Rust concepts used here
//!
//! `#[tokio::main]` creates the primary async runtime. `tokio::select!` races
//! owned signal futures and cancels the losing wait. The shutdown future is
//! passed to Axum, which stops accepting new work and waits for in-flight
//! services before returning. `?` then propagates bind, serve, and shutdown
//! errors to Rust's runtime as a nonzero process exit.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpListener;

use zeppelin::config::Config;
use zeppelin::startup::{build_app, init_logging, resolve_config_path, shutdown_background_tasks};

/// Loads configuration, serves HTTP, and coordinates process shutdown.
///
/// # Returns
///
/// `Ok(())` after graceful HTTP drain and normal compaction-thread shutdown.
///
/// # Errors
///
/// Propagates dotenv-independent configuration loading, application startup,
/// TCP bind, Axum serve, or compaction shutdown errors. Failure before the final
/// shutdown phase may leave the already-started compaction thread detached until
/// process exit.
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
/// configured shutdown timeout for compaction.
///
/// # Rust Notes for Java/C Engineers
///
/// The `async` blocks borrow or own exactly the state they need. `config` is
/// cloned once because [`build_app`] takes ownership while this function still
/// needs bind/shutdown settings. The compaction `JoinHandle` is moved into the
/// shutdown function, enforcing a single join owner at compile time.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env
    let _ = dotenvy::dotenv();

    // Load config (priority: ZEPPELIN_CONFIG env var > ./zeppelin.toml > defaults)
    let config = Config::load(resolve_config_path().as_deref())?;

    // Initialize logging
    init_logging(&config);

    // Build application (router + background tasks)
    let (app, shutdown_tx, compaction_handle) = build_app(config.clone()).await?;

    // Bind and serve
    let addr = format!("{}:{}", config.server.host, config.server.port);
    tracing::info!(addr = %addr, "listening");

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

    tracing::info!("server stopped, shutting down background tasks");
    shutdown_background_tasks(
        shutdown_tx,
        compaction_handle,
        Duration::from_secs(config.server.shutdown_timeout_secs),
    )
    .await?;
    tracing::info!("zeppelin shutdown complete");

    Ok(())
}
