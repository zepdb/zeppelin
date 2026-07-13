//! Safe HTTP/control-plane executors for ideal-analysis catalog cases.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::time::{Clock, TimeSource};
use zeppelin::wal::manifest::NamedSnapshot;

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{api_ns, start_test_server_full, FullTestServer};
use crate::perf_contract::depth::{depth_store, DepthTracker, OpSpan, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{
    CompactionCase, IdealCase, IdealOperation, NamespaceControlCase, OperationalCase,
    SnapshotCloneCase,
};

const SNAPSHOT_NAME: &str = "ideal-pin";

#[derive(Debug)]
struct FixedHttpTime(DateTime<Utc>);

impl TimeSource for FixedHttpTime {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

fn ideal_http_clock() -> Clock {
    let now = DateTime::from_timestamp(1_750_000_000, 987_654_321)
        .expect("fixed HTTP ideal timestamp must be representable");
    Clock::from_source(Arc::new(FixedHttpTime(now)))
}

/// Execute one supported HTTP/control-plane case against real TestHarness
/// storage. Unsupported catalog groups return `None` for another executor.
#[must_use]
pub(crate) fn supports(case: &IdealCase) -> bool {
    SupportedOperation::from_case(case).is_some()
}

pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    let operation = SupportedOperation::from_case(case)?;
    let harness = TestHarness::new().await;
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let config = ideal_http_config();
    let namespace = api_ns(&harness, "ideal-http");
    let client = Client::new();
    let clock = ideal_http_clock();
    let mut server = start_test_server_full(
        instrumented_store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;

    prepare_world(operation, &client, &server, &namespace).await;
    if operation == SupportedOperation::GetMetadataCold {
        server.shutdown().await;
        server = start_test_server_full(
            instrumented_store,
            Some(harness.prefix.clone()),
            config,
            false,
            Some(clock),
        )
        .await;
    }
    if operation == SupportedOperation::GetMetadataResident {
        assert_request(
            client.get(format!("{}/v1/namespaces/{namespace}", server.base_url)),
            StatusCode::OK,
            "prime resident namespace GET",
        )
        .await;
    }
    await_tracker_idle(&tracker).await;
    counter.reset();
    tracker.reset();

    measure(operation, &client, &server, &namespace).await;
    await_tracker_idle(&tracker).await;
    let repeat = snapshot_repeat(&counter, &tracker);
    let sample = IdealSample::from_repeat(case.id.as_str(), &repeat);
    verify_post_state(&harness, operation, &namespace).await;

    server.shutdown().await;
    cleanup_namespace(&harness, &namespace).await;
    harness.cleanup().await;
    Some(sample)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedOperation {
    HealthCheckStorageList,
    CreateFresh,
    CreateIdempotentExisting,
    GetMetadataCold,
    GetMetadataResident,
    PatchIndexConfig,
    CompactionStatus,
    CompactionHttpNoop,
    SnapshotCreate,
    SnapshotGet,
    SnapshotList,
    SnapshotDelete,
}

impl SupportedOperation {
    fn from_case(case: &IdealCase) -> Option<Self> {
        match case.operation {
            IdealOperation::Operational(OperationalCase::HealthCheckStorageList) => {
                Some(Self::HealthCheckStorageList)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::CreateFresh) => {
                Some(Self::CreateFresh)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::CreateIdempotentExisting) => {
                Some(Self::CreateIdempotentExisting)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::GetMetadataCold) => {
                Some(Self::GetMetadataCold)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::GetMetadataResident) => {
                Some(Self::GetMetadataResident)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::PatchIndexConfig) => {
                Some(Self::PatchIndexConfig)
            }
            IdealOperation::NamespaceControl(NamespaceControlCase::CompactionStatus) => {
                Some(Self::CompactionStatus)
            }
            IdealOperation::Compaction(CompactionCase::HttpNoop) => Some(Self::CompactionHttpNoop),
            IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotCreate) => {
                Some(Self::SnapshotCreate)
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotGet) => {
                Some(Self::SnapshotGet)
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotList) => {
                Some(Self::SnapshotList)
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotDelete) => {
                Some(Self::SnapshotDelete)
            }
            _ => None,
        }
    }

    fn needs_namespace(self) -> bool {
        !matches!(self, Self::HealthCheckStorageList | Self::CreateFresh)
    }

    fn needs_snapshot(self) -> bool {
        matches!(
            self,
            Self::SnapshotGet | Self::SnapshotList | Self::SnapshotDelete
        )
    }
}

fn ideal_http_config() -> Config {
    let mut config = Config::load(None).expect("failed to load ideal HTTP config");
    config.cache.namespace_registry_ttl_ms = 3_600_000;
    config.cache.manifest_cache_ttl_ms = 3_600_000;
    config.cache.hydration_enabled = false;
    config.compaction.max_wal_fragments_before_compact = usize::MAX;
    config
}

async fn prepare_world(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
) {
    if operation.needs_namespace() {
        create_namespace(client, server, namespace, StatusCode::CREATED).await;
    }
    if operation.needs_snapshot() {
        put_snapshot(client, server, namespace).await;
    }
}

async fn measure(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
) {
    match operation {
        SupportedOperation::HealthCheckStorageList => {
            let body = assert_request(
                client.get(format!("{}/readyz", server.base_url)),
                StatusCode::OK,
                "readiness check",
            )
            .await;
            assert_eq!(
                body,
                json!({ "status": "ready", "s3_connected": true }),
                "readiness response did not prove connected storage"
            );
        }
        SupportedOperation::CreateFresh => {
            create_namespace(client, server, namespace, StatusCode::CREATED).await;
        }
        SupportedOperation::CreateIdempotentExisting => {
            create_namespace(client, server, namespace, StatusCode::OK).await;
        }
        SupportedOperation::GetMetadataCold | SupportedOperation::GetMetadataResident => {
            let body = assert_request(
                client.get(format!("{}/v1/namespaces/{namespace}", server.base_url)),
                StatusCode::OK,
                "namespace GET",
            )
            .await;
            assert_empty_namespace_response(&body, namespace);
        }
        SupportedOperation::PatchIndexConfig => {
            let body = assert_request(
                client
                    .patch(format!(
                        "{}/v1/namespaces/{namespace}/index_config",
                        server.base_url
                    ))
                    .json(&json!({ "nlist": 2 })),
                StatusCode::ACCEPTED,
                "index config patch",
            )
            .await;
            assert_eq!(body["namespace"], namespace);
            assert_eq!(body["status"], "accepted");
            assert_eq!(body["index_config"]["nlist"].as_u64(), Some(2));
            assert_eq!(body["index_config"]["quantization"], "none");
            assert_eq!(body["index_config"]["hierarchical"], false);
            assert!(body["observe"]
                .as_str()
                .is_some_and(|value| value.contains(namespace)));
        }
        SupportedOperation::CompactionStatus => {
            let body = assert_request(
                client.get(format!(
                    "{}/v1/namespaces/{namespace}/compact/status",
                    server.base_url
                )),
                StatusCode::OK,
                "compaction status",
            )
            .await;
            assert_compaction_state(&body, namespace, None);
        }
        SupportedOperation::CompactionHttpNoop => {
            let body = assert_request(
                client.post(format!(
                    "{}/v1/namespaces/{namespace}/compact",
                    server.base_url
                )),
                StatusCode::OK,
                "manual compaction no-op",
            )
            .await;
            assert_compaction_state(&body, namespace, Some("noop"));
        }
        SupportedOperation::SnapshotCreate => {
            put_snapshot(client, server, namespace).await;
        }
        SupportedOperation::SnapshotGet => {
            let body = assert_request(
                client.get(snapshot_url(server, namespace)),
                StatusCode::OK,
                "snapshot GET",
            )
            .await;
            assert_snapshot_response(&body);
        }
        SupportedOperation::SnapshotList => {
            let body = assert_request(
                client.get(format!(
                    "{}/v1/namespaces/{namespace}/snapshots",
                    server.base_url
                )),
                StatusCode::OK,
                "snapshot LIST",
            )
            .await;
            let snapshots = body["snapshots"]
                .as_array()
                .expect("snapshot LIST response must contain an array");
            assert_eq!(snapshots.len(), 1);
            assert_snapshot_response(&snapshots[0]);
        }
        SupportedOperation::SnapshotDelete => {
            let body = assert_request(
                client.delete(snapshot_url(server, namespace)),
                StatusCode::NO_CONTENT,
                "snapshot DELETE",
            )
            .await;
            assert!(body.is_null());
        }
    }
}

async fn create_namespace(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    expected: StatusCode,
) {
    let body = assert_request(
        client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&json!({
                "name": namespace,
                "dimensions": 4,
                "distance_metric": "euclidean",
                "index_config": {
                    "nlist": 4,
                    "quantization": "none",
                    "hierarchical": false,
                    "fts_index": false,
                    "bitmap_index": false
                }
            })),
        expected,
        "namespace create",
    )
    .await;
    assert_empty_namespace_response(&body, namespace);
    assert_eq!(
        body["warning"],
        "Client-specified namespace names are idempotent for identical configuration."
    );
}

async fn put_snapshot(client: &Client, server: &FullTestServer, namespace: &str) {
    let body = assert_request(
        client.put(snapshot_url(server, namespace)),
        StatusCode::CREATED,
        "snapshot PUT",
    )
    .await;
    assert_snapshot_response(&body);
}

fn assert_empty_namespace_response(body: &Value, namespace: &str) {
    assert_eq!(body["name"], namespace);
    assert_eq!(body["dimensions"].as_u64(), Some(4));
    assert_eq!(body["distance_metric"], "euclidean");
    assert_eq!(body["vector_count"].as_u64(), Some(0));
    assert_eq!(body["uncompacted_fragments"].as_u64(), Some(0));
    assert_eq!(body["segment_count"].as_u64(), Some(0));
    assert_eq!(body["approximate_storage_bytes"].as_u64(), Some(0));
    assert!(body["quantization"].is_null());
    assert_eq!(body["index_kind"], "ivf_flat");
    assert_eq!(body["index_config"]["nlist"].as_u64(), Some(4));
    assert_eq!(body["index_config"]["quantization"], "none");
    assert_eq!(body["index_config"]["hierarchical"], false);
    assert_eq!(body["index_config"]["fts_index"], false);
    assert_eq!(body["index_config"]["bitmap_index"], false);
    assert_eq!(body["active_segment_vector_count"].as_u64(), Some(0));
    assert!(body["last_compaction_at"].is_null());
    assert_eq!(body["last_compaction_status"], "never");
    assert_eq!(body["consecutive_compaction_failures"].as_u64(), Some(0));
    assert_eq!(body["index_degraded"], false);
    assert_eq!(body["state"], "active");
    assert!(body["created_at"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert!(body["updated_at"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert!(body.get("full_text_search").is_none());
}

fn assert_compaction_state(body: &Value, namespace: &str, status: Option<&str>) {
    assert_eq!(body["namespace"], namespace);
    assert_eq!(body["manifest_generation"].as_u64(), Some(1));
    assert_eq!(body["uncompacted_fragments"].as_u64(), Some(0));
    assert_eq!(body["segment_count"].as_u64(), Some(0));
    assert!(body["active_segment"].is_null());
    assert_eq!(body["active_segment_vector_count"].as_u64(), Some(0));
    assert_eq!(body["ready"], true);
    match status {
        Some(expected) => assert_eq!(body["status"], expected),
        None => assert!(body.get("status").is_none()),
    }
}

fn assert_snapshot_response(body: &Value) {
    assert_eq!(body["name"], SNAPSHOT_NAME);
    assert_eq!(body["generation"].as_u64(), Some(1));
    assert!(body["created_at"]
        .as_str()
        .is_some_and(|value| chrono::DateTime::parse_from_rfc3339(value).is_ok()));
}

async fn verify_post_state(harness: &TestHarness, operation: SupportedOperation, namespace: &str) {
    match operation {
        SupportedOperation::CreateFresh | SupportedOperation::CreateIdempotentExisting => {
            let metadata = read_metadata(harness, namespace).await;
            assert_eq!(metadata.name, namespace);
            assert_eq!(metadata.dimensions, 4);
            assert_eq!(metadata.state, NamespaceState::Active);
            assert_eq!(
                metadata
                    .index_config
                    .as_ref()
                    .expect("created namespace omitted index config")
                    .nlist,
                4
            );
        }
        SupportedOperation::PatchIndexConfig => {
            let metadata = read_metadata(harness, namespace).await;
            assert_eq!(metadata.state, NamespaceState::Active);
            assert_eq!(
                metadata
                    .index_config
                    .as_ref()
                    .expect("patched namespace omitted index config")
                    .nlist,
                2
            );
        }
        SupportedOperation::SnapshotCreate => {
            let snapshot = NamedSnapshot::read(&harness.store, namespace, SNAPSHOT_NAME)
                .await
                .expect("created snapshot oracle failed")
                .expect("created snapshot disappeared");
            assert_eq!(snapshot.generation, 1);
        }
        SupportedOperation::SnapshotDelete => {
            assert!(
                NamedSnapshot::read(&harness.store, namespace, SNAPSHOT_NAME)
                    .await
                    .expect("deleted snapshot oracle failed")
                    .is_none()
            );
        }
        SupportedOperation::HealthCheckStorageList
        | SupportedOperation::GetMetadataCold
        | SupportedOperation::GetMetadataResident
        | SupportedOperation::CompactionStatus
        | SupportedOperation::CompactionHttpNoop
        | SupportedOperation::SnapshotGet
        | SupportedOperation::SnapshotList => {}
    }
}

async fn read_metadata(harness: &TestHarness, namespace: &str) -> NamespaceMetadata {
    NamespaceMetadata::from_bytes(
        &harness
            .store
            .get(&NamespaceMetadata::s3_key(namespace))
            .await
            .expect("namespace metadata oracle read failed"),
    )
    .expect("namespace metadata oracle decode failed")
}

fn snapshot_url(server: &FullTestServer, namespace: &str) -> String {
    format!(
        "{}/v1/namespaces/{namespace}/snapshots/{SNAPSHOT_NAME}",
        server.base_url
    )
}

async fn assert_request(
    request: reqwest::RequestBuilder,
    expected: StatusCode,
    label: &str,
) -> Value {
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("{label} request failed: {error}"));
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{label} response read failed: {error}"));
    assert_eq!(
        status,
        expected,
        "{label} returned an unexpected response: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{label} returned invalid JSON: {error}"))
    }
}

async fn cleanup_namespace(harness: &TestHarness, namespace: &str) {
    harness
        .store
        .delete_prefix(&format!("{namespace}/"))
        .await
        .unwrap_or_else(|error| panic!("failed to clean ideal HTTP namespace: {error}"));
}

async fn await_tracker_idle(tracker: &DepthTracker) {
    const MAX_YIELDS: usize = 4096;
    const REQUIRED_ZERO_STREAK: usize = 8;
    let mut zero_streak = 0;
    for _ in 0..MAX_YIELDS {
        tokio::task::yield_now().await;
        if tracker.active_operations() == 0 {
            zero_streak += 1;
            if zero_streak == REQUIRED_ZERO_STREAK {
                return;
            }
        } else {
            zero_streak = 0;
        }
    }
    panic!(
        "ideal HTTP measurement did not quiesce: active_operations={}",
        tracker.active_operations()
    );
}

fn snapshot_repeat(counter: &GetCounter, tracker: &DepthTracker) -> RepeatCounters {
    let cutoff_us = tracker.elapsed_us();
    let classes = counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect::<BTreeMap<_, _>>();
    let totals = classes
        .values()
        .copied()
        .fold(ClassStats::default(), add_stats);
    let spans = tracker.take_spans();
    let raw_get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], None);
    let raw_put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        None,
    );
    let get_path = raw_get_path.clone();
    let put_get_path = raw_put_get_path.clone();
    RepeatCounters {
        classes,
        totals,
        get_path,
        put_get_path,
        op_counts: operation_counts(&spans),
        spans,
        labeled: Vec::new(),
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
        raw_get_path,
        raw_put_get_path,
    }
}

fn add_stats(mut total: ClassStats, class: ClassStats) -> ClassStats {
    total.get_ops = total
        .get_ops
        .checked_add(class.get_ops)
        .expect("ideal HTTP GET operation total overflowed");
    total.get_bytes = total
        .get_bytes
        .checked_add(class.get_bytes)
        .expect("ideal HTTP GET byte total overflowed");
    total.put_ops = total
        .put_ops
        .checked_add(class.put_ops)
        .expect("ideal HTTP PUT operation total overflowed");
    total.put_bytes = total
        .put_bytes
        .checked_add(class.put_bytes)
        .expect("ideal HTTP PUT byte total overflowed");
    total
}

fn operation_counts(spans: &[OpSpan]) -> BTreeMap<String, u64> {
    [
        ("head", SpanKind::Head),
        ("list", SpanKind::List),
        ("copy", SpanKind::Copy),
        ("delete", SpanKind::Delete),
    ]
    .into_iter()
    .map(|(name, kind)| {
        (
            name.to_string(),
            u64::try_from(spans.iter().filter(|span| span.kind == kind).count())
                .expect("ideal HTTP operation count does not fit u64"),
        )
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn supported_ids_are_explicit_and_clone_selectors_remain_unowned() {
        let supported = catalog::all()
            .iter()
            .filter(|case| SupportedOperation::from_case(case).is_some())
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            vec![
                "operational.health_check_storage_list",
                "namespace.create_fresh",
                "namespace.create_idempotent_existing",
                "namespace.get_metadata_cold",
                "namespace.get_metadata_resident",
                "namespace.patch_index_config",
                "namespace.compaction_status",
                "snapshot.create",
                "snapshot.get",
                "snapshot.list",
                "snapshot.delete",
                "compaction.http_noop",
            ]
        );
        for id in [
            "clone.current",
            "clone.generation",
            "clone.timestamp",
            "clone.snapshot",
        ] {
            let case = catalog::all()
                .iter()
                .find(|case| case.id.as_str() == id)
                .expect("clone case missing from ideal catalog");
            assert!(SupportedOperation::from_case(case).is_none());
        }
    }

    #[test]
    fn snapshot_helpers_have_stable_paths() {
        assert_eq!(SNAPSHOT_NAME, "ideal-pin");
        let counts = operation_counts(&[]);
        assert_eq!(
            counts,
            BTreeMap::from([
                ("copy".to_string(), 0),
                ("delete".to_string(), 0),
                ("head".to_string(), 0),
                ("list".to_string(), 0),
            ])
        );
    }
}
