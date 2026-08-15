mod common;

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common::harness::TestHarness;
use common::server::{
    api_ns, cleanup_ns, client_with_bearer, create_ns_api_with, start_test_server,
    start_test_server_on_store,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use reqwest::{Response, StatusCode};
use serde_json::{json, Map, Value};
use tokio::sync::Notify;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::NamedSnapshot;
use zeppelin::wal::Manifest;

async fn expect_json(response: Response, expected: StatusCode, context: &str) -> Value {
    let actual = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{context} response body must be readable: {error}"));
    assert_eq!(
        actual,
        expected,
        "{context}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{context} response must be JSON: {error}"))
    }
}

async fn create_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, namespace: &str, id: &str, tenant: &str) {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": id,
                "values": [0.0, 0.0],
                "attributes": {"tenant_id": tenant, "ssn": "hidden"}
            }]
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("{id} upsert must complete: {error}"));
    expect_json(response, StatusCode::OK, &format!("{id} upsert")).await;
}

async fn create_principal(admin: &reqwest::Client, base_url: &str, label: &str) -> String {
    let principal_id = format!("service:clone-{label}-{}", uuid::Uuid::new_v4().simple());
    let response = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": format!("clone-{label}")
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("{label} principal creation must complete: {error}"));
    expect_json(
        response,
        StatusCode::CREATED,
        &format!("{label} principal creation"),
    )
    .await;
    principal_id
}

async fn issue_key(
    admin: &reqwest::Client,
    base_url: &str,
    label: &str,
    principal_id: &str,
) -> reqwest::Client {
    let response = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": format!("clone-{label}-primary")
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("{label} key issuance must complete: {error}"));
    let body = expect_json(
        response,
        StatusCode::CREATED,
        &format!("{label} key issuance"),
    )
    .await;
    client_with_bearer(
        body["api_key"]
            .as_str()
            .unwrap_or_else(|| panic!("{label} key issuance must return api_key")),
    )
}

async fn add_grant(
    admin: &reqwest::Client,
    base_url: &str,
    principal_id: &str,
    scope: Value,
    actions: &[&str],
    constraint_fields: Value,
) {
    let mut grant = Map::from_iter([
        ("principal_id".to_string(), json!(principal_id)),
        ("scope".to_string(), scope),
        (
            "actions".to_string(),
            json!({"kind": "selected", "actions": actions}),
        ),
    ]);
    for (field, value) in constraint_fields
        .as_object()
        .expect("grant constraint fields must be an object")
    {
        assert!(
            grant.insert(field.clone(), value.clone()).is_none(),
            "constraint field {field} must not replace grant identity"
        );
    }
    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&Value::Object(grant))
        .send()
        .await
        .expect("grant creation must complete");
    expect_json(response, StatusCode::CREATED, "grant creation").await;
}

async fn create_clone_controller(
    admin: &reqwest::Client,
    base_url: &str,
    source: &str,
    label: &str,
) -> reqwest::Client {
    let principal_id = create_principal(admin, base_url, label).await;
    let client = issue_key(admin, base_url, label, &principal_id).await;
    add_grant(
        admin,
        base_url,
        &principal_id,
        json!({"kind": "namespace", "namespace": source}),
        &["NamespaceClone", "NamespaceRead"],
        json!({}),
    )
    .await;
    add_grant(
        admin,
        base_url,
        &principal_id,
        json!({"kind": "global"}),
        &["NamespaceCreate"],
        json!({}),
    )
    .await;
    client
}

async fn generation(store: &ZeppelinStore, namespace: &str) -> u64 {
    Manifest::read(store, namespace)
        .await
        .expect("manifest read must succeed")
        .expect("manifest must exist")
        .version()
}

async fn clone_request(
    client: &reqwest::Client,
    base_url: &str,
    source: &str,
    target: &str,
    generation: u64,
) -> Response {
    client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": generation.to_string()
        }))
        .send()
        .await
        .expect("clone request must complete")
}

async fn fetch_one(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    id: &str,
) -> Response {
    client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors/get"))
        .json(&json!({"ids": [id]}))
        .send()
        .await
        .expect("vector fetch must complete")
}

#[tokio::test]
async fn clone_rejects_policy_wide_global_read_widening_before_target_creation() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, "source-acme", "acme").await;
    let source_generation = generation(&harness.store, &source).await;
    let controller = create_clone_controller(&admin, &base_url, &source, "policy-controller").await;

    // This principal deliberately has no API key. The proof must cover every
    // policy principal rather than only the clone caller or currently usable
    // credentials. The global grant pre-provisions target access, while the
    // source-specific grant narrows that same global access on the source.
    let observer = create_principal(&admin, &base_url, "keyless-observer").await;
    add_grant(
        &admin,
        &base_url,
        &observer,
        json!({"kind": "global"}),
        &["Query", "VectorFetch"],
        json!({}),
    )
    .await;
    add_grant(
        &admin,
        &base_url,
        &observer,
        json!({"kind": "namespace", "namespace": source}),
        &["Query", "VectorFetch"],
        json!({
            "mandatory_filter": {
                "op": "eq",
                "field": "tenant_id",
                "value": "acme"
            },
            "field_mask": {"deny": ["ssn"]}
        }),
    )
    .await;

    let target = api_ns(&harness, "policy-wide-widening-target");
    let body = expect_json(
        clone_request(&controller, &base_url, &source, &target, source_generation).await,
        StatusCode::FORBIDDEN,
        "policy-wide widening clone",
    )
    .await;
    assert_eq!(body["code"], "constraint_violation");
    for leaked in ["namespace", "generation", "target_generation", "mode"] {
        assert!(
            body.get(leaked).is_none(),
            "rejected clone response must not leak full-count field {leaked}"
        );
    }

    let target_lookup = admin
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .expect("target lookup must complete");
    expect_json(
        target_lookup,
        StatusCode::NOT_FOUND,
        "target rejected before creation",
    )
    .await;

    cleanup_ns(&harness.store, &source).await;
    harness.cleanup().await;
}

#[derive(Debug)]
struct BlockingCopyState {
    blocked: AtomicBool,
    released: AtomicBool,
    fired: AtomicBool,
    blocked_notify: Notify,
    release_notify: Notify,
}

#[derive(Clone, Debug)]
struct BlockingCopyControl {
    state: Arc<BlockingCopyState>,
}

impl BlockingCopyControl {
    async fn wait_until_blocked(&self) {
        loop {
            let notified = self.state.blocked_notify.notified();
            if self.state.blocked.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn release_failure(&self) {
        self.state.released.store(true, Ordering::SeqCst);
        self.state.release_notify.notify_waiters();
    }
}

#[derive(Debug)]
struct BlockingCopyFailureStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    state: Arc<BlockingCopyState>,
}

impl BlockingCopyFailureStore {
    fn wrap(
        store: &ZeppelinStore,
        needle: impl Into<String>,
    ) -> (ZeppelinStore, BlockingCopyControl) {
        let state = Arc::new(BlockingCopyState {
            blocked: AtomicBool::new(false),
            released: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            blocked_notify: Notify::new(),
            release_notify: Notify::new(),
        });
        let control = BlockingCopyControl {
            state: Arc::clone(&state),
        };
        let wrapped = Self {
            inner: store.inner(),
            needle: needle.into(),
            state,
        };
        (ZeppelinStore::new(Arc::new(wrapped)), control)
    }

    fn should_block(&self, from: &Path, to: &Path) -> bool {
        (from.as_ref().contains(&self.needle) || to.as_ref().contains(&self.needle))
            && self
                .state
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

impl fmt::Display for BlockingCopyFailureStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BlockingCopyFailureStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for BlockingCopyFailureStore {
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
        self.inner.get_opts(location, options).await
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
        if self.should_block(from, to) {
            self.state.blocked.store(true, Ordering::SeqCst);
            self.state.blocked_notify.notify_waiters();
            loop {
                let notified = self.state.release_notify.notified();
                if self.state.released.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
            return Err(object_store::Error::Generic {
                store: "blocking_copy_failure",
                source: Box::new(std::io::Error::other(format!(
                    "injected delayed copy failure from {from} to {to}"
                ))),
            });
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[derive(Debug)]
struct BlockingManifestCasStore {
    inner: Arc<dyn ObjectStore>,
    manifest_key: String,
    state: Arc<BlockingCopyState>,
}

impl BlockingManifestCasStore {
    fn wrap(store: &ZeppelinStore, manifest_key: String) -> (ZeppelinStore, BlockingCopyControl) {
        let state = Arc::new(BlockingCopyState {
            blocked: AtomicBool::new(false),
            released: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            blocked_notify: Notify::new(),
            release_notify: Notify::new(),
        });
        let control = BlockingCopyControl {
            state: Arc::clone(&state),
        };
        let wrapped = Self {
            inner: store.inner(),
            manifest_key,
            state,
        };
        (ZeppelinStore::new(Arc::new(wrapped)), control)
    }

    fn should_block(&self, location: &Path, options: &PutOptions) -> bool {
        location.as_ref() == self.manifest_key
            && matches!(&options.mode, PutMode::Update(_))
            && self
                .state
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

impl fmt::Display for BlockingManifestCasStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BlockingManifestCasStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for BlockingManifestCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.should_block(location, &options) {
            self.state.blocked.store(true, Ordering::SeqCst);
            self.state.blocked_notify.notify_waiters();
            loop {
                let notified = self.state.release_notify.notified();
                if self.state.released.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
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
        self.inner.get_opts(location, options).await
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

/// The clone's final conditional manifest write must lose to an acknowledged
/// target write, and failed-clone cleanup must preserve that target lifetime.
#[tokio::test]
async fn clone_publication_rejects_a_concurrently_changed_target() {
    let harness = TestHarness::new().await;
    let target = api_ns(&harness, "clone-cas-target");
    let (barrier_store, barrier) =
        BlockingManifestCasStore::wrap(&harness.store, Manifest::object_store_key(&target));
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, barrier_store, Some(harness.prefix.clone())).await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, "source-row", "source").await;
    let source_generation = generation(&harness.store, &source).await;

    let clone_client = admin.clone();
    let clone_base_url = base_url.clone();
    let clone_source = source.clone();
    let clone_target = target.clone();
    let clone_task = tokio::spawn(async move {
        clone_request(
            &clone_client,
            &clone_base_url,
            &clone_source,
            &clone_target,
            source_generation,
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        barrier.wait_until_blocked(),
    )
    .await
    .expect("clone publication must reach the target manifest CAS barrier");
    upsert(
        &admin,
        &base_url,
        &target,
        "concurrent-target-row",
        "target",
    )
    .await;
    barrier.release_failure();

    let response = clone_task.await.expect("clone task must not panic");
    let body = expect_json(response, StatusCode::CONFLICT, "stale clone publication").await;
    assert_eq!(body["code"], "CONFLICT_RETRY");
    let fetched = expect_json(
        fetch_one(&admin, &base_url, &target, "concurrent-target-row").await,
        StatusCode::OK,
        "concurrent target write after stale clone",
    )
    .await;
    assert_eq!(fetched["results"][0]["id"], "concurrent-target-row");

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn clone_copy_failure_never_deletes_an_acknowledged_concurrent_target_write() {
    let harness = TestHarness::new().await;
    let (faulted_store, copy_control) = BlockingCopyFailureStore::wrap(&harness.store, "/wal/");
    let (base_url, _cache, _cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, faulted_store, Some(harness.prefix.clone())).await;
    let admin = client_with_bearer(&admin_bearer);
    let source = create_namespace(&admin, &base_url).await;
    upsert(&admin, &base_url, &source, "source-row", "source").await;
    let source_generation = generation(&harness.store, &source).await;
    let target = api_ns(&harness, "concurrent-write-retained-target");

    let clone_client = admin.clone();
    let clone_base_url = base_url.clone();
    let clone_source = source.clone();
    let clone_target = target.clone();
    let clone_task = tokio::spawn(async move {
        clone_request(
            &clone_client,
            &clone_base_url,
            &clone_source,
            &clone_target,
            source_generation,
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        copy_control.wait_until_blocked(),
    )
    .await
    .expect("clone copy must reach the deterministic failure barrier");

    // The target is active while clone copying is in flight. This successful
    // response is the write that a later clone cleanup must never erase.
    upsert(
        &admin,
        &base_url,
        &target,
        "concurrent-target-row",
        "target",
    )
    .await;
    copy_control.release_failure();

    let clone_response = clone_task.await.expect("clone task must not panic");
    let clone_error = expect_json(
        clone_response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "delayed clone copy failure",
    )
    .await;
    assert_eq!(clone_error["code"], "STORAGE_ERROR");

    let target_lookup = admin
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .expect("retained target lookup must complete");
    let target_status = expect_json(
        target_lookup,
        StatusCode::OK,
        "target retained after clone failure",
    )
    .await;
    assert_eq!(target_status["vector_count"], 1);

    let fetched = expect_json(
        fetch_one(&admin, &base_url, &target, "concurrent-target-row").await,
        StatusCode::OK,
        "concurrent target write after clone failure",
    )
    .await;
    assert_eq!(fetched["results"][0]["id"], "concurrent-target-row");
    assert!(
        NamedSnapshot::list(&harness.store, &source)
            .await
            .expect("source snapshots must list")
            .iter()
            .all(|snapshot| !snapshot.name.starts_with("__clone_")),
        "clone failure must still release its temporary source pin"
    );

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
    harness.cleanup().await;
}
