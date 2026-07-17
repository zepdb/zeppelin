use std::sync::{Arc, Mutex};

use object_store::memory::InMemory;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;
use zeppelin::compaction::background::CompactionLifecycle;
use zeppelin::config::{StorageBackend, StorageConfig};
use zeppelin::security::{AuditRuntime, SecurityKernel};
use zeppelin::server::ServerTaskSupervisor;
use zeppelin::storage::ZeppelinStore;

/// Server-owned work that must stop before one harness deletes remote state.
pub(crate) struct TestServerRuntime {
    shutdown_http: watch::Sender<bool>,
    server_task: JoinHandle<()>,
    server_tasks: Arc<ServerTaskSupervisor>,
    compaction_lifecycle: CompactionLifecycle,
    audit_runtime: Option<AuditRuntime>,
    audit_signing_security: Option<Arc<SecurityKernel>>,
}

impl TestServerRuntime {
    pub(crate) fn new(
        shutdown_http: watch::Sender<bool>,
        server_task: JoinHandle<()>,
        server_tasks: Arc<ServerTaskSupervisor>,
        compaction_lifecycle: CompactionLifecycle,
        audit_runtime: AuditRuntime,
        audit_signing_security: Arc<SecurityKernel>,
    ) -> Self {
        Self {
            shutdown_http,
            server_task,
            server_tasks,
            compaction_lifecycle,
            audit_runtime: Some(audit_runtime),
            audit_signing_security: Some(audit_signing_security),
        }
    }

    async fn shutdown(mut self) -> Result<(), String> {
        let _ = self.shutdown_http.send(true);
        let server_result = self
            .server_task
            .await
            .map_err(|error| format!("test HTTP server failed: {error}"));
        let request_tasks_result = self
            .server_tasks
            .join()
            .await
            .map_err(|error| format!("test request-spawned task failed: {error}"));
        let heartbeat_result = self
            .compaction_lifecycle
            .close_and_abort_heartbeats()
            .await
            .map_err(|error| format!("test compaction heartbeat retirement failed: {error}"));
        let audit_result = match self.audit_runtime.take() {
            Some(runtime) => runtime
                .shutdown()
                .await
                .map_err(|error| format!("test audit runtime failed: {error}")),
            None => Ok(()),
        };
        // The store retains only a weak signer binding. Keep the kernel's
        // signer root alive until graceful audit shutdown has sealed its tail.
        drop(self.audit_signing_security.take());

        let errors = [
            server_result,
            request_tasks_result,
            heartbeat_result,
            audit_result,
        ]
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; secondary shutdown failure: "))
        }
    }

    fn abort(mut self) {
        self.server_task.abort();
        self.server_tasks.abort();
        // AuditRuntime::drop aborts the writer before fallback object cleanup.
        drop(self.audit_runtime.take());
        drop(self.audit_signing_security.take());
    }
}

/// Test harness that connects to in-memory storage, local storage, real S3, or MinIO,
/// isolates each test under a random prefix, and cleans up on drop.
pub struct TestHarness {
    pub store: ZeppelinStore,
    pub prefix: String,
    bucket: String,
    _temp_dir: Option<tempfile::TempDir>,
    test_servers: Mutex<Vec<TestServerRuntime>>,
}

impl TestHarness {
    /// Create a new test harness. Reads config from environment variables.
    /// Each harness gets a unique random prefix for test isolation.
    pub async fn new() -> Self {
        let backend = std::env::var("TEST_BACKEND").unwrap_or_else(|_| "memory".to_string());
        let temp_dir = if backend == "local" {
            Some(tempfile::tempdir().expect("failed to create local test storage dir"))
        } else {
            None
        };
        let bucket = match temp_dir.as_ref() {
            Some(dir) => dir.path().to_string_lossy().into_owned(),
            None => {
                std::env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "stormcrow-test".to_string())
            }
        };

        let store = match backend.as_str() {
            "memory" => ZeppelinStore::new(Arc::new(InMemory::new())),
            "local" => {
                let config = StorageConfig {
                    backend: StorageBackend::Local,
                    bucket: bucket.clone(),
                    s3_region: None,
                    s3_endpoint: None,
                    s3_access_key_id: None,
                    s3_secret_access_key: None,
                    s3_allow_http: false,
                    fail_fast: true,
                };
                ZeppelinStore::from_config(&config).expect("failed to create store from config")
            }
            "s3" => {
                let config = StorageConfig {
                    backend: StorageBackend::S3,
                    bucket: bucket.clone(),
                    s3_region: std::env::var("AWS_REGION").ok(),
                    s3_endpoint: std::env::var("S3_ENDPOINT").ok().filter(|s| !s.is_empty()),
                    s3_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                    s3_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                    s3_allow_http: std::env::var("S3_ALLOW_HTTP")
                        .ok()
                        .map(|v| v == "true")
                        .unwrap_or(false),
                    fail_fast: true,
                };
                ZeppelinStore::from_config(&config).expect("failed to create store from config")
            }
            "minio" => {
                let config = StorageConfig {
                    backend: StorageBackend::S3,
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
                    fail_fast: true,
                };
                ZeppelinStore::from_config(&config).expect("failed to create store from config")
            }
            other => panic!("unsupported TEST_BACKEND: {other}"),
        };

        let prefix = format!("test-{}", Uuid::new_v4());

        Self {
            store,
            prefix,
            bucket,
            _temp_dir: temp_dir,
            test_servers: Mutex::new(Vec::new()),
        }
    }

    /// Register a server whose HTTP and audit tasks share this harness's store.
    pub(crate) fn register_test_server(&self, runtime: TestServerRuntime) {
        self.test_servers
            .lock()
            .unwrap_or_else(|_| panic!("test server registry lock poisoned"))
            .push(runtime);
    }

    /// Get a namespaced key under this test's random prefix.
    /// e.g., `test-<uuid>/my-key` — keeps tests isolated.
    pub fn key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    /// Build a URL-safe namespace owned by this fixture's random identity.
    #[cfg(feature = "branching-test-support")]
    pub fn artifact_origin_namespace(&self, suffix: &str) -> String {
        format!("{}-{suffix}", self.prefix)
    }

    /// Construct an in-memory foreign-origin view through the feature adapter.
    #[cfg(feature = "branching-test-support")]
    pub async fn synthetic_foreign_origin_view(
        &self,
        source_namespace: &str,
        target_namespace: &str,
    ) -> zeppelin::error::Result<
        zeppelin::namespace::branching::test_support::SyntheticForeignOriginView,
    > {
        zeppelin::namespace::branching::test_support::SyntheticForeignOriginView::from_source(
            self.store.clone(),
            source_namespace,
            target_namespace,
        )
        .await
    }

    /// Remove one URL-safe namespace created by an artifact-origin fixture.
    #[cfg(feature = "branching-test-support")]
    pub async fn cleanup_artifact_origin_namespace(&self, namespace: &str) {
        let deleted = self
            .store
            .delete_prefix(&format!("{namespace}/"))
            .await
            .unwrap_or_else(|error| panic!("artifact-origin cleanup failed: {error}"));
        if deleted > 0 {
            eprintln!("[test harness] cleaned up {deleted} objects under {namespace}/");
        }
    }

    /// Clean up all objects under this test's prefix.
    pub async fn cleanup(&self) {
        let runtimes = {
            let mut registered = self
                .test_servers
                .lock()
                .unwrap_or_else(|_| panic!("test server registry lock poisoned"));
            std::mem::take(&mut *registered)
        };
        let mut cleanup_errors = Vec::new();
        for runtime in runtimes {
            if let Err(error) = runtime.shutdown().await {
                cleanup_errors.push(error);
            }
        }

        let prefix = format!("{}/", self.prefix);
        match self.store.delete_prefix(&prefix).await {
            Ok(count) => {
                if count > 0 {
                    eprintln!("[test harness] cleaned up {count} objects under {prefix}");
                }
            }
            Err(e) => {
                cleanup_errors.push(format!("domain prefix cleanup failed for {prefix}: {e}"));
            }
        }
        if let Err(error) = cleanup_audit_scope(&self.store, &self.prefix).await {
            cleanup_errors.push(error);
        }
        assert!(
            cleanup_errors.is_empty(),
            "test harness cleanup failed: {cleanup_errors:?}"
        );
    }
}

async fn cleanup_audit_scope(store: &ZeppelinStore, scope: &str) -> Result<usize, String> {
    let marker = format!("/test-node-{scope}-");
    let keys = store
        .list_prefix("_audit/")
        .await
        .map_err(|error| format!("audit prefix LIST failed for scope {scope}: {error}"))?;
    let mut deleted = 0usize;
    let mut delete_errors = Vec::new();
    for key in keys.into_iter().filter(|key| key.contains(&marker)) {
        match store.delete(&key).await {
            Ok(()) => deleted += 1,
            Err(error) => delete_errors.push(format!("{key}: {error}")),
        }
    }
    if deleted > 0 {
        eprintln!("[test harness] cleaned up {deleted} audit objects for {scope}");
    }
    if delete_errors.is_empty() {
        Ok(deleted)
    } else {
        Err(format!(
            "audit object cleanup failed for scope {scope}: {}",
            delete_errors.join("; ")
        ))
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let runtimes = self
            .test_servers
            .get_mut()
            .unwrap_or_else(|_| panic!("test server registry lock poisoned"));
        for runtime in std::mem::take(runtimes) {
            runtime.abort();
        }

        // We can't do async cleanup in Drop, so we spawn a blocking task.
        // Tests should call cleanup() explicitly in an async context.
        // This is a best-effort fallback.
        let store = self.store.clone();
        let prefix = format!("{}/", self.prefix);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                if let Err(error) = store.delete_prefix(&prefix).await {
                    eprintln!(
                        "[test harness] best-effort domain cleanup failed for {prefix}: {error}"
                    );
                }
                let scope = prefix.trim_end_matches('/');
                if let Err(error) = cleanup_audit_scope(&store, scope).await {
                    eprintln!("[test harness] best-effort {error}");
                }
            });
        });
    }
}
