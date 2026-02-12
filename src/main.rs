use std::time::Duration;

use tokio::net::TcpListener;

use zeppelin::config::Config;
use zeppelin::startup::{build_app, init_logging, resolve_config_path};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env
    let _ = dotenvy::dotenv();

    // Load config (priority: ZEPPELIN_CONFIG env var > ./zeppelin.toml > defaults)
    let config = Config::load(resolve_config_path().as_deref())?;

    // Initialize logging
    init_logging(&config);

    // Build application (router + background tasks)
    let (app, shutdown_tx) = build_app(config.clone()).await?;

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

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await?;

    tracing::info!("server stopped, shutting down background tasks");
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_secs(config.server.shutdown_timeout_secs)).await;
    tracing::info!("zeppelin shutdown complete");

    Ok(())
}
