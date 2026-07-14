use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use zeppelin::config::{Config, SecurityMode, StorageBackend};
use zeppelin::startup::{build_app, shutdown_background_tasks};

async fn closed_loopback_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn dead_s3_config(endpoint: String, cache_dir: &tempfile::TempDir) -> Config {
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    config.storage.backend = StorageBackend::S3;
    config.storage.bucket = "zeppelin-task18-dead-endpoint".to_string();
    config.storage.s3_region = Some("us-east-1".to_string());
    config.storage.s3_endpoint = Some(endpoint);
    config.storage.s3_access_key_id = Some("minioadmin".to_string());
    config.storage.s3_secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_allow_http = true;
    config.cache.dir = cache_dir.path().to_path_buf();
    config.compaction.interval_secs = 3600;
    config
}

fn local_config(storage_dir: &tempfile::TempDir, cache_dir: &tempfile::TempDir) -> Config {
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    config.storage.backend = StorageBackend::Local;
    config.storage.bucket = storage_dir
        .path()
        .join("objects")
        .to_string_lossy()
        .to_string();
    config.cache.dir = cache_dir.path().to_path_buf();
    config.compaction.interval_secs = 3600;
    config.server.shutdown_timeout_secs = 30;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_app_refuses_dead_s3_when_fail_fast_enabled() {
    let cache_dir = tempfile::TempDir::new().unwrap();
    let endpoint = closed_loopback_endpoint().await;
    let config = dead_s3_config(endpoint, &cache_dir);

    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), build_app(config))
        .await
        .expect("dead-endpoint startup probe should finish quickly");

    let message = match result {
        Ok((_router, shutdown_tx, compaction_handle, audit_runtime)) => {
            let _ = audit_runtime.shutdown().await;
            let _ =
                shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(1))
                    .await;
            panic!("build_app must refuse to boot on dead S3");
        }
        Err(error) => error.to_string(),
    };
    assert!(
        message.contains("storage health probe") || message.contains("S3"),
        "dead S3 boot error should be actionable, got: {message}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "dead S3 boot should fail fast, elapsed {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_app_allows_dead_s3_when_fail_fast_disabled() {
    let cache_dir = tempfile::TempDir::new().unwrap();
    let endpoint = closed_loopback_endpoint().await;
    let mut config = dead_s3_config(endpoint, &cache_dir);
    config.storage.fail_fast = false;

    let (router, shutdown_tx, compaction_handle, audit_runtime) =
        tokio::time::timeout(Duration::from_secs(5), build_app(config))
            .await
            .expect("opted-out startup should finish quickly")
            .expect("storage.fail_fast=false should allow degraded boot");
    drop(router);

    audit_runtime.shutdown().await.unwrap();
    shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_shutdown_joins_background_compaction_without_fixed_sleep() {
    let storage_dir = tempfile::TempDir::new().unwrap();
    let cache_dir = tempfile::TempDir::new().unwrap();
    let config = local_config(&storage_dir, &cache_dir);

    let (router, shutdown_tx, compaction_handle, audit_runtime) = build_app(config).await.unwrap();
    drop(router);

    let start = Instant::now();
    audit_runtime.shutdown().await.unwrap();
    shutdown_background_tasks(shutdown_tx, compaction_handle, Duration::from_secs(30))
        .await
        .unwrap();

    assert!(
        start.elapsed() < Duration::from_secs(2),
        "idle shutdown should not sleep for the full shutdown timeout, elapsed {:?}",
        start.elapsed()
    );
}
