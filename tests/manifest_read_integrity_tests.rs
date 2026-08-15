//! Active namespaces must never turn manifest read failures into empty success.

mod common;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::harness::TestHarness;
use common::server::{
    api_ns, cleanup_ns, create_ns_api_with, start_test_server, start_test_server_on_store,
};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::Manifest;

#[derive(Clone, Debug)]
struct StaleManifestHandle {
    next: Arc<Mutex<Option<(String, Bytes)>>>,
}

impl StaleManifestHandle {
    async fn arm(&self, key: String, bytes: Bytes) {
        *self.next.lock().await = Some((key, bytes));
    }
}

#[derive(Debug)]
struct StaleManifestOnceStore {
    inner: Arc<dyn ObjectStore>,
    next: Arc<Mutex<Option<(String, Bytes)>>>,
}

impl StaleManifestOnceStore {
    fn wrap(store: &ZeppelinStore) -> (ZeppelinStore, StaleManifestHandle) {
        let next = Arc::new(Mutex::new(None));
        let wrapped = Self {
            inner: store.inner(),
            next: Arc::clone(&next),
        };
        (
            ZeppelinStore::new(Arc::new(wrapped)),
            StaleManifestHandle { next },
        )
    }
}

impl fmt::Display for StaleManifestOnceStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "StaleManifestOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for StaleManifestOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let stale = {
            let mut next = self.next.lock().await;
            match next.as_ref() {
                Some((key, _)) if key == location.as_ref() => next.take().map(|(_, bytes)| bytes),
                _ => None,
            }
        };
        let result = self.inner.get_opts(location, options).await?;
        let Some(stale) = stale else {
            return Ok(result);
        };

        let mut meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let _current = result.bytes().await?;
        meta.size = stale.len();
        let len = stale.len();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(stale) }).boxed()),
            meta,
            range: 0..len,
            attributes,
        })
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn active_bounded_read_rejects_missing_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("required-missing");
    let cache = ManifestCache::new(Duration::ZERO);

    let error = cache
        .get_required(&harness.store, &namespace)
        .await
        .expect_err("active bounded reads must reject a missing live manifest");
    assert!(matches!(error, ZeppelinError::NotFound { .. }));

    harness.cleanup().await;
}

#[tokio::test]
async fn active_required_reads_accept_published_legacy_generation_zero_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("published-legacy-generation-zero");
    let legacy = serde_json::to_vec(&json!({
        "fragments": [],
        "segments": [],
        "compaction_watermark": null,
        "active_segment": null,
        "next_sequence": 0,
        "pending_deletes": [],
        "fencing_token": 0,
        "updated_at": chrono::Utc::now(),
    }))
    .unwrap();
    harness
        .store
        .put(&Manifest::object_store_key(&namespace), Bytes::from(legacy))
        .await
        .unwrap();

    let bounded = ManifestCache::new(Duration::from_secs(60))
        .get_required(&harness.store, &namespace)
        .await
        .expect("a published pre-generation manifest must remain readable");
    let strong = ManifestCache::new(Duration::from_secs(60))
        .get_strong_required(&harness.store, &namespace)
        .await
        .expect("a strong read must preserve pre-generation compatibility");
    assert_eq!(bounded.version(), 0);
    assert_eq!(strong.version(), 0);
    assert!(bounded.fragments.is_empty());
    assert!(strong.fragments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn active_strong_read_rejects_missing_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("strong-required-missing");
    let cache = ManifestCache::new(Duration::from_secs(60));

    let error = cache
        .get_strong_required(&harness.store, &namespace)
        .await
        .expect_err("active strong reads must reject a missing live manifest");
    assert!(matches!(error, ZeppelinError::NotFound { .. }));

    harness.cleanup().await;
}

#[tokio::test]
async fn lifecycle_bounded_read_accepts_missing_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("bounded-lifecycle-missing");
    let cache = ManifestCache::new(Duration::from_secs(60));

    let manifest = cache
        .get(&harness.store, &namespace)
        .await
        .expect("lifecycle reads must represent absence as empty");
    assert_eq!(manifest.version(), 0);
    assert!(manifest.fragments.is_empty());
    assert!(manifest.segments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn lifecycle_strong_read_accepts_missing_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("strong-lifecycle-missing");
    let cache = ManifestCache::new(Duration::from_secs(60));

    let manifest = cache
        .get_strong(&harness.store, &namespace)
        .await
        .expect("lifecycle reads must represent absence as empty");
    assert_eq!(manifest.version(), 0);
    assert!(manifest.fragments.is_empty());
    assert!(manifest.segments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn lifecycle_reads_accept_generation_zero_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("lifecycle-generation-zero");
    harness
        .store
        .put(
            &Manifest::object_store_key(&namespace),
            Manifest::new().to_bytes().unwrap(),
        )
        .await
        .unwrap();
    let cache = ManifestCache::new(Duration::from_secs(60));

    let bounded = cache
        .get(&harness.store, &namespace)
        .await
        .expect("bounded lifecycle reads must preserve generation-zero compatibility");
    let strong = cache
        .get_strong(&harness.store, &namespace)
        .await
        .expect("strong lifecycle reads must preserve generation-zero compatibility");
    assert_eq!(bounded.version(), 0);
    assert_eq!(strong.version(), 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn lifecycle_strong_read_accepts_deleted_manifest_after_warm_cache() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("deleting-warm-cache");
    common::seed_bound_manifest(&harness.store, &namespace).await;
    let cache = ManifestCache::new(Duration::from_secs(60));
    let warm = cache.get_strong(&harness.store, &namespace).await.unwrap();
    assert!(warm.version() > 0);

    harness
        .store
        .delete(&Manifest::object_store_key(&namespace))
        .await
        .unwrap();
    let deleting = cache
        .get_strong(&harness.store, &namespace)
        .await
        .expect("deletion lifecycle reads must represent absence as empty");
    assert_eq!(deleting.version(), 0);
    assert!(deleting.fragments.is_empty());
    assert!(deleting.segments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn active_namespace_missing_manifest_fails_fetch_loudly() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "present",
                "values": [1.0, 2.0]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), StatusCode::OK);

    harness
        .store
        .delete(&Manifest::object_store_key(&namespace))
        .await
        .unwrap();

    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["present"],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();

    assert!(
        status.is_server_error(),
        "missing manifest beneath active metadata must fail loud: {status} {body}"
    );
    assert_eq!(body["code"], "INTERNAL_DATA_MISSING");

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn active_namespace_missing_manifest_fails_pitr_boundary_loudly() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    harness
        .store
        .delete(&Manifest::object_store_key(&namespace))
        .await
        .unwrap();

    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/clone"))
        .json(&json!({
            "target": api_ns(&harness, "missing-live-pitr-target"),
            "as_of": "1"
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();

    assert!(
        status.is_server_error(),
        "missing active live manifest must be an integrity error: {status} {body}"
    );
    assert_eq!(body["code"], "INTERNAL_DATA_MISSING");

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn active_namespace_stale_manifest_body_fails_fetch_loudly() {
    let harness = TestHarness::new().await;
    let (store, stale_manifest) = StaleManifestOnceStore::wrap(&harness.store);
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, store, Some(harness.prefix.clone())).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    let manifest_key = Manifest::object_store_key(&namespace);
    let stale_bytes = harness.store.get(&manifest_key).await.unwrap();

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "present",
                "values": [1.0, 2.0]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), StatusCode::OK);
    let current_bytes = harness.store.get(&manifest_key).await.unwrap();
    assert_ne!(stale_bytes, current_bytes);
    assert_ne!(stale_bytes.len(), current_bytes.len());

    stale_manifest.arm(manifest_key, stale_bytes).await;
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({
            "ids": ["present"],
            "include_vector": true,
            "include_attributes": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();

    assert!(
        status.is_server_error(),
        "stale manifest body with current metadata must fail loud: {status} {body}"
    );

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}
