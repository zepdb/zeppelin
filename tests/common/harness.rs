use uuid::Uuid;
use zeppelin::config::StorageConfig;
use zeppelin::storage::ZeppelinStore;

/// Test harness that connects to real S3 (or MinIO), isolates each test
/// under a random prefix, and cleans up on drop.
pub struct TestHarness {
    pub store: ZeppelinStore,
    pub prefix: String,
    bucket: String,
}

impl TestHarness {
    /// Create a new test harness. Reads config from environment variables.
    /// Each harness gets a unique random prefix for test isolation.
    pub async fn new() -> Self {
        // Load .env file if present (ignore errors — CI may use real env vars)
        let _ = dotenvy::dotenv();

        let backend = std::env::var("TEST_BACKEND").unwrap_or_else(|_| "s3".to_string());
        let bucket = std::env::var("TEST_S3_BUCKET")
            .unwrap_or_else(|_| "stormcrow-test".to_string());

        let config = match backend.as_str() {
            "s3" => StorageConfig {
                backend: "s3".to_string(),
                bucket: bucket.clone(),
                s3_region: std::env::var("AWS_REGION").ok(),
                s3_endpoint: std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty()),
                s3_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                s3_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                s3_allow_http: std::env::var("S3_ALLOW_HTTP")
                    .ok()
                    .map(|v| v == "true")
                    .unwrap_or(false),
                gcs_service_account_path: None,
                azure_account: None,
                azure_access_key: None,
            },
            "minio" => StorageConfig {
                backend: "s3".to_string(),
                bucket: bucket.clone(),
                s3_region: Some("us-east-1".to_string()),
                s3_endpoint: Some(
                    std::env::var("MINIO_ENDPOINT")
                        .unwrap_or_else(|_| "http://localhost:9000".to_string()),
                ),
                s3_access_key_id: Some(
                    std::env::var("MINIO_ACCESS_KEY")
                        .unwrap_or_else(|_| "minioadmin".to_string()),
                ),
                s3_secret_access_key: Some(
                    std::env::var("MINIO_SECRET_KEY")
                        .unwrap_or_else(|_| "minioadmin".to_string()),
                ),
                s3_allow_http: true,
                gcs_service_account_path: None,
                azure_account: None,
                azure_access_key: None,
            },
            other => panic!("unsupported TEST_BACKEND: {other}"),
        };

        let store = ZeppelinStore::from_config(&config)
            .expect("failed to create store from config");

        let prefix = format!("test-{}", Uuid::new_v4());

        Self {
            store,
            prefix,
            bucket,
        }
    }

    /// Get a namespaced key under this test's random prefix.
    /// e.g., `test-<uuid>/my-key` — keeps tests isolated.
    pub fn key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    /// Clean up all objects under this test's prefix.
    pub async fn cleanup(&self) {
        let prefix = format!("{}/", self.prefix);
        match self.store.delete_prefix(&prefix).await {
            Ok(count) => {
                if count > 0 {
                    eprintln!("[test harness] cleaned up {count} objects under {prefix}");
                }
            }
            Err(e) => {
                eprintln!("[test harness] warning: cleanup failed: {e}");
            }
        }
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        // We can't do async cleanup in Drop, so we spawn a blocking task.
        // Tests should call cleanup() explicitly in an async context.
        // This is a best-effort fallback.
        let store = self.store.clone();
        let prefix = format!("{}/", self.prefix);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = store.delete_prefix(&prefix).await;
            });
        });
    }
}
