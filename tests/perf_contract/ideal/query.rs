//! Deterministic HTTP executors for vector, query, fetch, and clone cases.

use std::collections::BTreeMap;

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use zeppelin::compaction::gc::reachable_keys;
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::wal::Manifest;

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{api_ns, start_test_server_full, FullTestServer};
use crate::perf_contract::depth::{depth_store, DepthTracker, OpSpan, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{
    BatchQueryCase, FetchCase, IdealCase, IdealOperation, QueryCase, SnapshotCloneCase,
    VectorWriteCase,
};

const CLONE_SNAPSHOT: &str = "ideal-clone-pin";

/// Execute one supported data-plane HTTP case. Unsupported catalog groups
/// return `None` so the owning executor can try the case.
#[must_use]
pub(crate) fn supports(case: &IdealCase) -> bool {
    SupportedOperation::from_case(case).is_some()
}

pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    let operation = SupportedOperation::from_case(case)?;
    let harness = TestHarness::new().await;
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let server = start_test_server_full(
        instrumented_store,
        Some(harness.prefix.clone()),
        ideal_query_config(),
        false,
        None,
    )
    .await;
    let client = crate::common::server::client_with_bearer(&server.admin_bearer);
    let namespace = api_ns(&harness, "ideal-query");
    let clone_target = api_ns(&harness, "ideal-clone-target");

    let world = prepare_world(operation, &client, &server, &namespace).await;
    if operation == SupportedOperation::AnnEventualWalOnly {
        // This case owns the cold-manifest WAL-only branch. The setup upsert
        // writes through the production manifest cache, so make the intended
        // pre-state explicit before the measured interval begins.
        server.manifest_cache.invalidate(&namespace);
    }
    await_tracker_idle(&tracker).await;
    counter.reset();
    tracker.reset();

    measure(
        operation,
        &client,
        &server,
        &namespace,
        &clone_target,
        &world,
    )
    .await;
    await_tracker_idle(&tracker).await;
    let repeat = snapshot_repeat(&counter, &tracker);
    if operation.is_clone() {
        assert!(
            repeat.spans.iter().any(|span| span.kind == SpanKind::Copy),
            "artifact-bearing clone did not issue a physical COPY"
        );
    }
    let sample = IdealSample::from_repeat(case.id.as_str(), &repeat);
    if operation.is_batch() {
        assert!(
            sample.total_get_ops > 0,
            "successful batch query produced no physical object-store GETs"
        );
    }
    if operation.is_clone() {
        verify_clone_target(&harness, operation, &namespace, &clone_target, &world).await;
    }

    server.shutdown().await;
    cleanup_namespace(&harness, &namespace).await;
    cleanup_namespace(&harness, &clone_target).await;
    harness.cleanup().await;
    Some(sample)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedOperation {
    UpsertIntoEmpty,
    UpsertIntoCompacted,
    DeleteBatch,
    AnnStrongWalOnly,
    AnnEventualWalOnly,
    AnnStrongCompactedAndWal,
    AnnEventualCompactedAndWal,
    AnnIncludeAttributes,
    AnnVectorRerank,
    AnnMultiRangeRerank,
    AsOfTimestamp,
    AsOfSnapshot,
    BatchStrongCompacted,
    BatchEventualCompactedAndWal,
    FetchEventualCompacted,
    FetchStrongWalOnly,
    FetchStrongCompactedAndWal,
    FetchStrongMiss,
    FetchStrongWithAttributes,
    CloneCurrent,
    CloneGeneration,
    CloneTimestamp,
    CloneSnapshot,
}

impl SupportedOperation {
    fn from_case(case: &IdealCase) -> Option<Self> {
        Some(match case.operation {
            IdealOperation::VectorWrite(VectorWriteCase::UpsertIntoEmpty) => Self::UpsertIntoEmpty,
            IdealOperation::VectorWrite(VectorWriteCase::UpsertIntoCompacted) => {
                Self::UpsertIntoCompacted
            }
            IdealOperation::VectorWrite(VectorWriteCase::DeleteBatch) => Self::DeleteBatch,
            IdealOperation::Query(QueryCase::AnnStrongWalOnly) => Self::AnnStrongWalOnly,
            IdealOperation::Query(QueryCase::AnnEventualWalOnly) => Self::AnnEventualWalOnly,
            IdealOperation::Query(QueryCase::AnnStrongCompactedAndWal) => {
                Self::AnnStrongCompactedAndWal
            }
            IdealOperation::Query(QueryCase::AnnEventualCompactedAndWal) => {
                Self::AnnEventualCompactedAndWal
            }
            IdealOperation::Query(QueryCase::AnnIncludeAttributes) => Self::AnnIncludeAttributes,
            IdealOperation::Query(QueryCase::AnnVectorRerank) => Self::AnnVectorRerank,
            IdealOperation::Query(QueryCase::AnnMultiRangeRerank) => Self::AnnMultiRangeRerank,
            IdealOperation::Query(QueryCase::AsOfTimestamp) => Self::AsOfTimestamp,
            IdealOperation::Query(QueryCase::AsOfSnapshot) => Self::AsOfSnapshot,
            IdealOperation::BatchQuery(BatchQueryCase::StrongCompacted) => {
                Self::BatchStrongCompacted
            }
            IdealOperation::BatchQuery(BatchQueryCase::EventualCompactedAndWal) => {
                Self::BatchEventualCompactedAndWal
            }
            IdealOperation::Fetch(FetchCase::EventualCompacted) => Self::FetchEventualCompacted,
            IdealOperation::Fetch(FetchCase::StrongWalOnly) => Self::FetchStrongWalOnly,
            IdealOperation::Fetch(FetchCase::StrongCompactedAndWal) => {
                Self::FetchStrongCompactedAndWal
            }
            IdealOperation::Fetch(FetchCase::StrongMiss) => Self::FetchStrongMiss,
            IdealOperation::Fetch(FetchCase::StrongWithAttributes) => {
                Self::FetchStrongWithAttributes
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::CloneCurrent) => Self::CloneCurrent,
            IdealOperation::SnapshotClone(SnapshotCloneCase::CloneGeneration) => {
                Self::CloneGeneration
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::CloneTimestamp) => {
                Self::CloneTimestamp
            }
            IdealOperation::SnapshotClone(SnapshotCloneCase::CloneSnapshot) => Self::CloneSnapshot,
            _ => return None,
        })
    }

    fn is_wal_only(self) -> bool {
        matches!(
            self,
            Self::AnnStrongWalOnly | Self::AnnEventualWalOnly | Self::FetchStrongWalOnly
        )
    }

    fn needs_mixed_wal(self) -> bool {
        matches!(
            self,
            Self::AnnStrongCompactedAndWal
                | Self::AnnEventualCompactedAndWal
                | Self::BatchEventualCompactedAndWal
                | Self::FetchStrongCompactedAndWal
        )
    }

    fn needs_history(self) -> bool {
        matches!(
            self,
            Self::AsOfTimestamp
                | Self::AsOfSnapshot
                | Self::CloneCurrent
                | Self::CloneGeneration
                | Self::CloneTimestamp
                | Self::CloneSnapshot
        )
    }

    fn is_clone(self) -> bool {
        matches!(
            self,
            Self::CloneCurrent | Self::CloneGeneration | Self::CloneTimestamp | Self::CloneSnapshot
        )
    }

    fn is_batch(self) -> bool {
        matches!(
            self,
            Self::BatchStrongCompacted | Self::BatchEventualCompactedAndWal
        )
    }
}

#[derive(Debug, Default)]
struct PreparedWorld {
    historical_generation: Option<u64>,
    historical_timestamp: Option<String>,
    current_generation: Option<u64>,
}

fn ideal_query_config() -> Config {
    let mut config = Config::default();
    config.cache.namespace_registry_ttl_ms = 3_600_000;
    config.cache.manifest_cache_ttl_ms = 3_600_000;
    config.cache.hydration_enabled = false;
    config.compaction.max_wal_fragments_before_compact = usize::MAX;
    config.indexing = IndexingConfig {
        default_num_centroids: 2,
        default_nprobe: 2,
        max_nprobe: 8,
        quantization: QuantizationType::None,
        bitmap_index: false,
        fts_index: false,
        ..config.indexing.clone()
    };
    config
}

async fn prepare_world(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
) -> PreparedWorld {
    create_namespace(client, server, namespace).await;
    if operation == SupportedOperation::UpsertIntoEmpty {
        return PreparedWorld::default();
    }

    upsert(client, server, namespace, base_vectors()).await;
    if operation.is_wal_only() {
        return PreparedWorld::default();
    }

    let compacted = server
        .compactor
        .compact(namespace)
        .await
        .expect("ideal query setup compaction failed");
    assert!(
        compacted.segment_id.is_some(),
        "ideal query setup compaction produced no segment"
    );
    server.manifest_cache.invalidate(namespace);

    let historical = Manifest::read(&server.store, namespace)
        .await
        .expect("failed to read ideal historical manifest")
        .expect("ideal historical manifest missing");
    let mut world = PreparedWorld {
        historical_generation: Some(historical.version()),
        historical_timestamp: Some(historical.updated_at.to_rfc3339()),
        current_generation: Some(historical.version()),
    };

    if matches!(
        operation,
        SupportedOperation::AsOfSnapshot | SupportedOperation::CloneSnapshot
    ) {
        put_snapshot(client, server, namespace).await;
    }
    if operation.needs_mixed_wal() || operation.needs_history() {
        upsert(client, server, namespace, extra_vectors()).await;
        let current = Manifest::read(&server.store, namespace)
            .await
            .expect("failed to read ideal current manifest")
            .expect("ideal current manifest missing");
        world.current_generation = Some(current.version());
        assert!(
            current.version() > historical.version(),
            "ideal history setup did not advance the live manifest"
        );
    }
    world
}

async fn measure(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    clone_target: &str,
    world: &PreparedWorld,
) {
    match operation {
        SupportedOperation::UpsertIntoEmpty | SupportedOperation::UpsertIntoCompacted => {
            upsert(client, server, namespace, measured_vectors()).await;
        }
        SupportedOperation::DeleteBatch => {
            assert_json_request(
                client
                    .delete(format!(
                        "{}/v1/namespaces/{namespace}/vectors",
                        server.base_url
                    ))
                    .json(&json!({ "ids": ["base-0", "base-1", "base-2"] })),
                StatusCode::NO_CONTENT,
                "vector delete batch",
            )
            .await;
        }
        SupportedOperation::AnnStrongWalOnly => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnEventualWalOnly => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnStrongCompactedAndWal => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnEventualCompactedAndWal => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnIncludeAttributes => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnVectorRerank => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AnnMultiRangeRerank => {
            query(operation, client, server, namespace, None).await;
        }
        SupportedOperation::AsOfTimestamp => {
            query(
                operation,
                client,
                server,
                namespace,
                Some(
                    world
                        .historical_timestamp
                        .as_deref()
                        .expect("timestamp query missing historical selector"),
                ),
            )
            .await;
        }
        SupportedOperation::AsOfSnapshot => {
            query(
                operation,
                client,
                server,
                namespace,
                Some(&format!("snapshot:{CLONE_SNAPSHOT}")),
            )
            .await;
        }
        SupportedOperation::BatchStrongCompacted => {
            batch_query(operation, client, server, namespace).await;
        }
        SupportedOperation::BatchEventualCompactedAndWal => {
            batch_query(operation, client, server, namespace).await;
        }
        SupportedOperation::FetchEventualCompacted => {
            fetch(operation, client, server, namespace).await;
        }
        SupportedOperation::FetchStrongWalOnly => {
            fetch(operation, client, server, namespace).await;
        }
        SupportedOperation::FetchStrongCompactedAndWal => {
            fetch(operation, client, server, namespace).await;
        }
        SupportedOperation::FetchStrongMiss => {
            fetch(operation, client, server, namespace).await;
        }
        SupportedOperation::FetchStrongWithAttributes => {
            fetch(operation, client, server, namespace).await;
        }
        SupportedOperation::CloneCurrent
        | SupportedOperation::CloneGeneration
        | SupportedOperation::CloneTimestamp
        | SupportedOperation::CloneSnapshot => {
            let selector = match operation {
                SupportedOperation::CloneCurrent => world
                    .current_generation
                    .expect("current clone missing generation")
                    .to_string(),
                SupportedOperation::CloneGeneration => world
                    .historical_generation
                    .expect("generation clone missing generation")
                    .to_string(),
                SupportedOperation::CloneTimestamp => world
                    .historical_timestamp
                    .clone()
                    .expect("timestamp clone missing selector"),
                SupportedOperation::CloneSnapshot => format!("snapshot:{CLONE_SNAPSHOT}"),
                _ => unreachable!(),
            };
            let response = assert_json_request(
                client
                    .post(format!(
                        "{}/v1/namespaces/{namespace}/clone",
                        server.base_url
                    ))
                    .json(&json!({ "target": clone_target, "as_of": selector })),
                StatusCode::CREATED,
                "namespace clone",
            )
            .await;
            assert_clone_response(
                operation,
                &response,
                namespace,
                clone_target,
                clone_source_generation(operation, world),
            );
        }
    }
}

async fn create_namespace(client: &Client, server: &FullTestServer, namespace: &str) {
    assert_json_request(
        client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&json!({
                "name": namespace,
                "dimensions": 4,
                "distance_metric": "euclidean",
                "index_config": {
                    "nlist": 2,
                    "quantization": "none",
                    "hierarchical": false,
                    "fts_index": false,
                    "bitmap_index": false
                }
            })),
        StatusCode::CREATED,
        "ideal query namespace create",
    )
    .await;
}

async fn upsert(client: &Client, server: &FullTestServer, namespace: &str, vectors: Value) {
    assert_json_request(
        client
            .post(format!(
                "{}/v1/namespaces/{namespace}/vectors",
                server.base_url
            ))
            .json(&json!({ "vectors": vectors })),
        StatusCode::OK,
        "ideal query upsert",
    )
    .await;
}

async fn put_snapshot(client: &Client, server: &FullTestServer, namespace: &str) {
    assert_json_request(
        client.put(format!(
            "{}/v1/namespaces/{namespace}/snapshots/{CLONE_SNAPSHOT}",
            server.base_url
        )),
        StatusCode::CREATED,
        "ideal clone snapshot",
    )
    .await;
}

async fn query(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    as_of: Option<&str>,
) {
    let consistency = if matches!(
        operation,
        SupportedOperation::AnnEventualWalOnly | SupportedOperation::AnnEventualCompactedAndWal
    ) {
        "eventual"
    } else {
        "strong"
    };
    let include_attributes = operation == SupportedOperation::AnnIncludeAttributes;
    let rerank_candidates = match operation {
        SupportedOperation::AnnVectorRerank => Some(4),
        SupportedOperation::AnnMultiRangeRerank => Some(8),
        _ => None,
    };
    let body = rerank_candidates.map_or_else(
        || {
            json!({
                "vector": [0.0, 0.0, 0.0, 0.0],
                "top_k": 4,
                "nprobe": 2,
                "consistency": consistency,
                "include_attributes": include_attributes
            })
        },
        |candidate_k| {
            json!({
                "sources": [{
                    "type": "ann",
                    "vector": [0.0, 0.0, 0.0, 0.0],
                    "nprobe": 2
                }],
                "candidate_k": candidate_k,
                "top_k": 4,
                "consistency": consistency,
                "rerank": {
                    "type": "vector",
                    "vector": [7.0, 0.0, 0.0, 0.0]
                },
                "projection": { "include_attributes": include_attributes }
            })
        },
    );
    let mut body = body;
    if operation == SupportedOperation::AnnEventualWalOnly {
        body["debug"] = Value::Bool(true);
    }
    let request = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&body);
    let request = match as_of {
        Some(selector) => request.query(&[("as_of", selector)]),
        None => request,
    };
    let response = assert_json_request(request, StatusCode::OK, "ideal ANN query").await;
    assert_query_response(operation, &response);
}

async fn batch_query(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
) {
    let consistency = match operation {
        SupportedOperation::BatchStrongCompacted => "strong",
        SupportedOperation::BatchEventualCompactedAndWal => "eventual",
        _ => panic!("non-batch operation reached batch executor: {operation:?}"),
    };
    let response = assert_json_request(
        client
            .post(format!(
                "{}/v1/namespaces/{namespace}/query/batch",
                server.base_url
            ))
            .json(&json!({
                "queries": [
                    {
                        "vector": [0.0, 0.0, 0.0, 0.0],
                        "top_k": 3,
                        "nprobe": 2,
                        "consistency": consistency,
                        "include_attributes": false
                    },
                    {
                        "vector": [7.0, 0.0, 0.0, 0.0],
                        "top_k": 3,
                        "nprobe": 2,
                        "consistency": consistency,
                        "include_attributes": false
                    }
                ]
            })),
        StatusCode::OK,
        "ideal batch query",
    )
    .await;
    assert_batch_query_response(operation, &response);
}

async fn fetch(
    operation: SupportedOperation,
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
) {
    let (ids, consistency, include_attributes, expected_found, expected_missing): (
        &[&str],
        &str,
        bool,
        &[&str],
        &[&str],
    ) = match operation {
        SupportedOperation::FetchEventualCompacted => (
            &["base-0", "base-1"],
            "eventual",
            false,
            &["base-0", "base-1"],
            &[],
        ),
        SupportedOperation::FetchStrongWalOnly => (
            &["base-0", "base-1"],
            "strong",
            false,
            &["base-0", "base-1"],
            &[],
        ),
        SupportedOperation::FetchStrongCompactedAndWal => (
            &["base-0", "wal-extra"],
            "strong",
            false,
            &["base-0", "wal-extra"],
            &[],
        ),
        SupportedOperation::FetchStrongMiss => (&["missing"], "strong", false, &[], &["missing"]),
        SupportedOperation::FetchStrongWithAttributes => (
            &["base-0", "base-1"],
            "strong",
            true,
            &["base-0", "base-1"],
            &[],
        ),
        _ => panic!("non-fetch operation reached fetch executor: {operation:?}"),
    };
    let response = assert_json_request(
        client
            .post(format!(
                "{}/v1/namespaces/{namespace}/vectors/get",
                server.base_url
            ))
            .json(&json!({
                "ids": ids,
                "include_vector": true,
                "include_attributes": include_attributes,
                "attribute_fields": include_attributes.then_some(["tenant"]),
                "consistency": consistency
            })),
        StatusCode::OK,
        "ideal vector fetch",
    )
    .await;
    assert_fetch_response(
        operation,
        &response,
        expected_found,
        expected_missing,
        include_attributes,
    );
}

fn assert_query_response(operation: SupportedOperation, response: &Value) {
    let (expected_ids, expected_fragments, expected_segments): (&[&str], u64, u64) = match operation
    {
        SupportedOperation::AnnEventualWalOnly => (&[], 0, 0),
        SupportedOperation::AnnStrongWalOnly => (&["base-0", "base-1", "base-2", "base-3"], 1, 0),
        SupportedOperation::AnnStrongCompactedAndWal => {
            (&["base-0", "base-1", "base-2", "base-3"], 1, 1)
        }
        SupportedOperation::AnnVectorRerank => (&["base-3", "base-2", "base-1", "base-0"], 0, 1),
        SupportedOperation::AnnMultiRangeRerank => {
            (&["base-7", "base-6", "base-5", "base-4"], 0, 1)
        }
        SupportedOperation::AnnEventualCompactedAndWal
        | SupportedOperation::AnnIncludeAttributes
        | SupportedOperation::AsOfTimestamp
        | SupportedOperation::AsOfSnapshot => (&["base-0", "base-1", "base-2", "base-3"], 0, 1),
        _ => panic!("non-query operation reached query response assertion: {operation:?}"),
    };
    assert_eq!(
        response_ids(response, "ideal ANN query"),
        expected_ids,
        "unexpected ANN result IDs for {operation:?}: {response}"
    );
    assert_eq!(
        response["scanned_fragments"].as_u64(),
        Some(expected_fragments),
        "unexpected scanned_fragments for {operation:?}: {response}"
    );
    assert_eq!(
        response["scanned_segments"].as_u64(),
        Some(expected_segments),
        "unexpected scanned_segments for {operation:?}: {response}"
    );

    let results = response["results"]
        .as_array()
        .expect("ideal ANN query results must be an array");
    for result in results {
        if operation == SupportedOperation::AnnIncludeAttributes {
            let attributes = result["attributes"]
                .as_object()
                .expect("attribute query must return an attribute object");
            assert_eq!(attributes.len(), 2);
            assert!(attributes.contains_key("tenant"));
            assert!(attributes.contains_key("ordinal"));
        } else {
            assert!(
                result["attributes"].is_null(),
                "non-attribute query leaked attributes for {operation:?}: {result}"
            );
        }
    }

    if operation == SupportedOperation::AnnEventualWalOnly {
        assert_eq!(response["debug"]["consistency_effective"], "eventual");
        assert_eq!(
            response["debug"]["underfill_reason"],
            "eventual_skipped_wal"
        );
        assert_eq!(response["debug"]["fragments_scanned"].as_u64(), Some(0));
        assert_eq!(response["debug"]["segments_scanned"].as_u64(), Some(0));
    }
}

fn assert_batch_query_response(operation: SupportedOperation, response: &Value) {
    let entries = response["results"]
        .as_array()
        .expect("ideal batch response results must be an array");
    assert_eq!(entries.len(), 2, "ideal batch must return two entries");
    let expected_ids: [&[&str]; 2] = [
        &["base-0", "base-1", "base-2"],
        &["base-7", "base-6", "base-5"],
    ];
    for (index, (entry, expected)) in entries.iter().zip(expected_ids).enumerate() {
        assert_eq!(
            entry["ok"].as_bool(),
            Some(true),
            "ideal batch entry {index} failed for {operation:?}: {entry}"
        );
        assert!(
            entry.get("error").is_none(),
            "successful ideal batch entry {index} carried an error: {entry}"
        );
        assert!(
            entry["metadata"]["latency_ms"].is_u64(),
            "ideal batch entry {index} omitted latency metadata: {entry}"
        );
        let query_response = &entry["response"];
        assert_eq!(
            response_ids(query_response, "ideal batch query entry"),
            expected,
            "unexpected batch result IDs for {operation:?} entry {index}: {entry}"
        );
        assert_eq!(query_response["scanned_fragments"].as_u64(), Some(0));
        assert_eq!(query_response["scanned_segments"].as_u64(), Some(1));
        for result in query_response["results"]
            .as_array()
            .expect("ideal batch entry results must be an array")
        {
            assert!(result["attributes"].is_null());
        }
    }
}

fn assert_fetch_response(
    operation: SupportedOperation,
    response: &Value,
    expected_found: &[&str],
    expected_missing: &[&str],
    include_attributes: bool,
) {
    let results = response["results"]
        .as_array()
        .expect("ideal fetch results must be an array");
    let found = results
        .iter()
        .map(|record| {
            record["id"]
                .as_str()
                .expect("ideal fetch result ID must be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        found, expected_found,
        "unexpected found IDs for {operation:?}: {response}"
    );
    let missing = response["missing"]
        .as_array()
        .expect("ideal fetch missing must be an array")
        .iter()
        .map(|id| {
            id.as_str()
                .expect("ideal fetch missing ID must be a string")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        missing, expected_missing,
        "unexpected missing IDs for {operation:?}: {response}"
    );

    for record in results {
        let id = record["id"].as_str().expect("ideal fetch ID");
        let expected_values = match id {
            "base-0" => json!([0.0, 0.0, 0.0, 0.0]),
            "base-1" => json!([1.0, 0.0, 0.0, 0.0]),
            "wal-extra" => json!([9.0, 0.0, 0.0, 0.0]),
            _ => panic!("unexpected ideal fetch result ID: {id}"),
        };
        assert_eq!(record["values"], expected_values);
        if include_attributes {
            let expected_tenant = if id == "base-0" { "even" } else { "odd" };
            assert_eq!(record["attributes"], json!({ "tenant": expected_tenant }));
        } else {
            assert!(
                record.get("attributes").is_none(),
                "non-attribute fetch returned attributes for {operation:?}: {record}"
            );
        }
    }
}

fn response_ids<'a>(response: &'a Value, label: &str) -> Vec<&'a str> {
    response["results"]
        .as_array()
        .unwrap_or_else(|| panic!("{label} results must be an array: {response}"))
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} result ID must be a string: {result}"))
        })
        .collect()
}

fn clone_source_generation(operation: SupportedOperation, world: &PreparedWorld) -> u64 {
    match operation {
        SupportedOperation::CloneCurrent => world
            .current_generation
            .expect("current clone missing source generation"),
        SupportedOperation::CloneGeneration
        | SupportedOperation::CloneTimestamp
        | SupportedOperation::CloneSnapshot => world
            .historical_generation
            .expect("historical clone missing source generation"),
        _ => panic!("non-clone operation requested clone generation: {operation:?}"),
    }
}

fn clone_expected_state(operation: SupportedOperation) -> (u64, u64) {
    match operation {
        SupportedOperation::CloneCurrent => (9, 1),
        SupportedOperation::CloneGeneration
        | SupportedOperation::CloneTimestamp
        | SupportedOperation::CloneSnapshot => (8, 0),
        _ => panic!("non-clone operation requested clone state: {operation:?}"),
    }
}

fn assert_clone_response(
    operation: SupportedOperation,
    response: &Value,
    source: &str,
    target: &str,
    source_generation: u64,
) {
    let (vector_count, uncompacted_fragments) = clone_expected_state(operation);
    assert_eq!(response["source"], source);
    assert_eq!(response["target"], target);
    assert_eq!(response["generation"].as_u64(), Some(source_generation));
    assert_eq!(response["target_generation"].as_u64(), Some(2));
    assert_eq!(response["mode"], "copy");
    assert_eq!(response["namespace"]["name"], target);
    assert_eq!(response["namespace"]["dimensions"].as_u64(), Some(4));
    assert_eq!(
        response["namespace"]["vector_count"].as_u64(),
        Some(vector_count)
    );
    assert_eq!(
        response["namespace"]["uncompacted_fragments"].as_u64(),
        Some(uncompacted_fragments)
    );
    assert_eq!(response["namespace"]["segment_count"].as_u64(), Some(1));
    assert_eq!(
        response["namespace"]["active_segment_vector_count"].as_u64(),
        Some(8)
    );
    assert_eq!(response["namespace"]["state"], "active");
}

async fn verify_clone_target(
    harness: &TestHarness,
    operation: SupportedOperation,
    source: &str,
    target: &str,
    world: &PreparedWorld,
) {
    let expected_source_generation = clone_source_generation(operation, world);
    let (vector_count, uncompacted_fragments) = clone_expected_state(operation);
    let manifest = Manifest::read(&harness.store, target)
        .await
        .expect("clone target manifest read failed")
        .expect("clone target manifest missing");
    assert_eq!(manifest.version(), 2);
    assert_eq!(manifest.vector_count(), vector_count);
    assert_eq!(
        u64::try_from(manifest.uncompacted_fragments().len())
            .expect("clone fragment count does not fit u64"),
        uncompacted_fragments
    );
    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segment_vector_count(), 8);
    assert!(manifest.pending_deletes.is_empty());
    assert_eq!(manifest.fencing_token, 0);
    for key in reachable_keys(target, &manifest).expect("clone reachability must resolve") {
        assert!(
            key.starts_with(&format!("{target}/")),
            "clone target retained a source key: {key}"
        );
    }

    let source_manifest =
        Manifest::read_history(&harness.store, source, expected_source_generation)
            .await
            .expect("clone source history read failed")
            .expect("clone source history missing");
    assert_eq!(manifest.vector_count(), source_manifest.vector_count());
    assert_eq!(
        manifest.uncompacted_fragments().len(),
        source_manifest.uncompacted_fragments().len()
    );
    assert_eq!(manifest.segments.len(), source_manifest.segments.len());
    assert_eq!(
        manifest.segment_vector_count(),
        source_manifest.segment_vector_count()
    );

    let metadata = NamespaceMetadata::from_bytes(
        &harness
            .store
            .get(&NamespaceMetadata::s3_key(target))
            .await
            .expect("clone target metadata read failed"),
    )
    .expect("clone target metadata decode failed");
    assert_eq!(metadata.name, target);
    assert_eq!(metadata.dimensions, 4);
    assert_eq!(metadata.state, NamespaceState::Active);
}

fn base_vectors() -> Value {
    Value::Array(
        (0..8)
            .map(|index| {
                json!({
                    "id": format!("base-{index}"),
                    "values": [index as f32, 0.0, 0.0, 0.0],
                    "attributes": {
                        "tenant": if index % 2 == 0 { "even" } else { "odd" },
                        "ordinal": index
                    }
                })
            })
            .collect(),
    )
}

fn extra_vectors() -> Value {
    json!([{
        "id": "wal-extra",
        "values": [9.0, 0.0, 0.0, 0.0],
        "attributes": { "tenant": "wal", "ordinal": 9 }
    }])
}

fn measured_vectors() -> Value {
    json!([{
        "id": "measured-upsert",
        "values": [10.0, 0.0, 0.0, 0.0],
        "attributes": { "tenant": "measured", "ordinal": 10 }
    }])
}

async fn assert_json_request(
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
        .unwrap_or_else(|error| panic!("failed to clean ideal query namespace: {error}"));
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
        "ideal query measurement did not quiesce: active_operations={}",
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
    RepeatCounters {
        classes,
        totals,
        get_path: raw_get_path.clone(),
        put_get_path: raw_put_get_path.clone(),
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
        .expect("GET ops overflow");
    total.get_bytes = total
        .get_bytes
        .checked_add(class.get_bytes)
        .expect("GET bytes overflow");
    total.put_ops = total
        .put_ops
        .checked_add(class.put_ops)
        .expect("PUT ops overflow");
    total.put_bytes = total
        .put_bytes
        .checked_add(class.put_bytes)
        .expect("PUT bytes overflow");
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
                .expect("operation count does not fit u64"),
        )
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn executor_owns_every_non_frozen_data_plane_and_clone_case() {
        let supported = catalog::all()
            .iter()
            .filter(|case| SupportedOperation::from_case(case).is_some())
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(supported.len(), 23);
        for id in [
            "batch_query.strong_compacted",
            "query.as_of_timestamp",
            "query.as_of_snapshot",
            "fetch.strong_compacted_and_wal",
            "fetch.strong_with_attributes",
            "query.ann_vector_rerank",
            "clone.current",
            "clone.generation",
            "clone.timestamp",
            "clone.snapshot",
        ] {
            assert!(supported.contains(&id), "missing supported case {id}");
        }
    }

    #[tokio::test]
    #[ignore = "requires MinIO for timestamp history GET overlap measurement"]
    async fn timestamp_as_of_history_reads_overlap_without_changing_census() {
        crate::perf_contract::require_minio();
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "query.as_of_timestamp")
            .expect("ideal timestamp as-of query case missing from catalog");
        let sample = execute(case)
            .await
            .expect("ideal timestamp as-of query executor rejected its catalog case");

        assert_eq!(sample.total_get_ops, 7);
        assert_eq!(sample.total_get_bytes, 3_572);
        assert_eq!(sample.serial_get_chain.depth, 4);
        assert_eq!(
            sample
                .serial_get_chain
                .links
                .iter()
                .map(|link| link.key.as_str())
                .collect::<Vec<_>>(),
            vec![
                "manifest.json",
                "<generation>.msgpack",
                "bootstrap.bin",
                "cluster_group_<index>.bin",
            ]
        );

        let history = sample
            .physical_operations
            .iter()
            .filter(|operation| operation.verb == "get" && operation.key == "<generation>.msgpack")
            .collect::<Vec<_>>();
        assert_eq!(history.len(), 4);
        assert_eq!(
            history
                .iter()
                .map(|operation| operation.successful_bytes)
                .sum::<u64>(),
            1_782
        );
        let last_start = history
            .iter()
            .map(|operation| operation.start_seq)
            .max()
            .expect("timestamp history GET fixture must not be empty");
        let first_finish = history
            .iter()
            .map(|operation| operation.end_seq)
            .min()
            .expect("timestamp history GET fixture must not be empty");
        assert!(
            last_start < first_finish,
            "all timestamp history GETs must start before the first completes: {history:#?}"
        );
    }

    #[test]
    fn deterministic_vector_fixture_has_unique_ids_and_attributes() {
        let vectors = base_vectors();
        let rows = vectors
            .as_array()
            .expect("base vector fixture must be array");
        assert_eq!(rows.len(), 8);
        let ids = rows
            .iter()
            .map(|row| row["id"].as_str().expect("fixture ID"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), rows.len());
        assert!(rows.iter().all(|row| row["attributes"].is_object()));
    }

    #[test]
    #[should_panic(expected = "ideal batch entry 0 failed")]
    fn batch_response_gate_rejects_http_200_entry_errors() {
        assert_batch_query_response(
            SupportedOperation::BatchStrongCompacted,
            &json!({
                "results": [
                    {
                        "ok": false,
                        "error": {
                            "code": "VALIDATION_ERROR",
                            "status": 400
                        },
                        "metadata": { "latency_ms": 0 }
                    },
                    {
                        "ok": false,
                        "error": {
                            "code": "VALIDATION_ERROR",
                            "status": 400
                        },
                        "metadata": { "latency_ms": 0 }
                    }
                ]
            }),
        );
    }
}
