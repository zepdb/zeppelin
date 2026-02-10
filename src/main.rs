use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Load .env
    let _ = dotenvy::dotenv();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    tracing::info!("zeppelin starting");

    // TODO: Phase 1 — config loading, store init, HTTP server
    eprintln!("zeppelin is not yet fully implemented. Run `cargo test` to exercise the storage layer.");
}
