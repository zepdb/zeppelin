//! Frozen Phase 10 branching object-operation census.
//!
//! This is a dedicated catalog entry because the contract compares related
//! operations (root versus branch, tiny versus corpus-scale, first versus
//! subsequent compaction) rather than one isolated request. It reuses the same
//! counting and depth decorators as the ordinary performance-contract runner.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::time::Instant;

use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::{json, Value};
use zeppelin::compaction::gc::reachable_keys;
use zeppelin::config::Config;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::wal::Manifest;

use crate::common::counting::{perf_counting_store, ArtifactClass, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{api_ns, client_with_bearer, start_test_server_full, FullTestServer};

use super::depth::{depth_store, DepthTracker, OpSpan, PhysicalRequest, SpanKind};
use super::require_minio;

const CONTRACT_VERSION: u32 = 1;
const FORK_SAMPLES: usize = 8;
const TINY_LOGICAL_ROWS: usize = 16;
const CORPUS_LOGICAL_ROWS: usize = 1_000_000;
// Fork control-GET budget, justified by the per-key census taken at e6bfb57
// (35 GETs, tiny fixture; `assert_fork_contract` proves the count is
// independent of logical row count). Each group backs a distinct state
// transition or a deliberate phase-boundary re-verification:
//
//   <dst>/meta.json                         9  creating-intent probes, three
//     metadata-transition CAS reads, publication reads, activation loops
//   _security/heads/policy.json             5  authority check at every
//     activation step (head is mutable; never memoized)
//   <src>/manifest.json                     5  candidate build, fenced CAS
//     base, root-visibility proof, two independent verification phases
//   <src>/manifests/<generation>.msgpack    3  root-digest proofs inside
//     those verification phases
//   <dst>/manifest.json                     3  live==publication checks (x2
//     phases) plus the handler's response read
//   <src>/meta.json                         3  reservation, under-lease
//     rooting, guarded-write recheck
//   <src>/lease.json                        2  acquire create-vs-takeover
//     probe, release ownership check
//   <dst>/manifests/<generation>.msgpack    2  history==live checks
//   _security/leases/policy-publication.json 2  two separate lease epochs
//   _security/policies/<id>.json            1  immutable snapshot body,
//     fetched once per fork via PolicySnapshotMemo
//
// Getting below 35 requires retiring a deliberate duplicate verification
// (the second verify_prepared_target pass, -4) or the handler's terminal
// read (-1); those are design decisions, not redundancy.
const MAX_FORK_CONTROL_GETS: u64 = 35;
const MAX_FORK_CONTROL_PUTS: u64 = 32;
// Fork CAS-PUT budget, justified by the per-key census taken after the GET
// census landed (22-23 PutUpdate spans, tiny fixture; this assertion was
// latent because the GET assertion panicked first). Every CAS succeeded on
// the first attempt — no retry spinning — and each backs a distinct state
// transition or the fencing layer of a fenced write:
//
//   <dst>/meta.json                          4  Rooted, ManifestPublished,
//     ActivationPending, Active visibility transitions
//   _security/leases/policy-publication.json 10  two lease sessions (acquire
//     x2, release x2) plus one renew immediately before each fenced head
//     CAS or the visibility CAS (fencing + CAS: both layers required)
//   _security/heads/policy.json              4  claim (retain session),
//     claim (begin session), guard install, guard removal
//   <src>/lease.json                      3-4  ownership renew immediately
//     before each fenced source write; the first fork of a source creates
//     the lease (PutOverwrite, not counted), while every later fork takes
//     over the expired record its predecessor's release deliberately
//     preserved, one takeover CAS that keeps the namespace fencing token
//     monotonic across sequential writers
//   <src>/manifest.json                      1  branch-root insertion, the
//     fork's single data-plane mutation
//
// The contract forks one source 8 times, so the 23 takeover shape is the
// steady state for 7 of 8 samples and the budget prices it. Lowering this
// means changing the fencing/activation protocol (fewer guard
// revalidations, combined head CASes, delete-on-release), a
// security-posture decision, not redundancy removal.
const MAX_FORK_CAS_PUTS: u64 = 23;

/// Frozen scenario inventory. Adding or removing an entry is a contract change.
pub const BRANCHING_SCENARIOS: [&str; 7] = [
    "fork_tiny",
    "fork_corpus",
    "branch_query",
    "branch_query_shared_cache",
    "branch_wal_write",
    "branch_materialize_first",
    "branch_compact_subsequent",
];

#[derive(Debug, Clone, Serialize)]
struct BranchingContract {
    version: u32,
    scenarios: Vec<&'static str>,
    fork_samples: usize,
    tiny_logical_rows: usize,
    corpus_logical_rows: usize,
    fork_control_gets_max: u64,
    fork_control_puts_max: u64,
    fork_cas_puts_max: u64,
}

impl Default for BranchingContract {
    fn default() -> Self {
        Self {
            version: CONTRACT_VERSION,
            scenarios: BRANCHING_SCENARIOS.to_vec(),
            fork_samples: FORK_SAMPLES,
            tiny_logical_rows: TINY_LOGICAL_ROWS,
            corpus_logical_rows: CORPUS_LOGICAL_ROWS,
            fork_control_gets_max: MAX_FORK_CONTROL_GETS,
            fork_control_puts_max: MAX_FORK_CONTROL_PUTS,
            fork_cas_puts_max: MAX_FORK_CAS_PUTS,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct OperationSnapshot {
    wall_elapsed_us: u64,
    observed_get_ops: u64,
    observed_put_ops: u64,
    classes: BTreeMap<String, ClassStats>,
    spans: Vec<OpSpan>,
}

impl OperationSnapshot {
    fn count_kind(&self, kind: SpanKind) -> u64 {
        self.spans.iter().filter(|span| span.kind == kind).count() as u64
    }

    fn count_request(&self, request: PhysicalRequest) -> u64 {
        self.spans
            .iter()
            .filter(|span| span.request == request)
            .count() as u64
    }

    fn operation_signature(&self) -> BTreeMap<String, u64> {
        let mut signature = BTreeMap::new();
        for span in &self.spans {
            *signature
                .entry(format!("{:?}:{:?}", span.kind, span.request))
                .or_insert(0) += 1;
        }
        signature
    }

    fn data_get_keys(&self) -> Vec<String> {
        let mut keys = self
            .spans
            .iter()
            .filter(|span| span.kind == SpanKind::Get && is_data_artifact(span.class))
            .map(|span| span.key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    fn data_get_bytes(&self) -> u64 {
        self.spans
            .iter()
            .filter(|span| span.kind == SpanKind::Get && is_data_artifact(span.class))
            .map(|span| span.bytes)
            .sum()
    }

    fn data_put_bytes(&self) -> u64 {
        self.spans
            .iter()
            .filter(|span| span.kind == SpanKind::Put && is_data_artifact(span.class))
            .map(|span| span.bytes)
            .sum()
    }
}

#[derive(Debug, Clone, Serialize)]
struct MeasuredOperation {
    /// Product work between the explicit counter reset and quiescence boundary.
    product: OperationSnapshot,
    /// Post-product manifest/query oracle reads, captured after a second reset.
    oracle: OperationSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct ForkSample {
    target: String,
    product: OperationSnapshot,
    oracle: OperationSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyPercentiles {
    p50_us: u64,
    p90_us: u64,
    p99_us: u64,
}

#[derive(Debug, Serialize)]
struct ForkCensus {
    logical_rows: usize,
    latency: LatencyPercentiles,
    samples: Vec<ForkSample>,
}

#[derive(Debug, Serialize)]
struct QueryCensus {
    root: MeasuredOperation,
    branch: MeasuredOperation,
    shared_physical_cache: MeasuredOperation,
    physical_data_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WriteCensus {
    ordinary: MeasuredOperation,
    branch: MeasuredOperation,
}

#[derive(Debug, Serialize)]
struct MaterializationCensus {
    first: MeasuredOperation,
    subsequent: MeasuredOperation,
    first_full_corpus_get_bytes: u64,
    first_target_upload_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BranchingCensus {
    contract: BranchingContract,
    fork_tiny: ForkCensus,
    fork_corpus: ForkCensus,
    query: QueryCensus,
    write: WriteCensus,
    materialization: MaterializationCensus,
}

/// Run the frozen branching object-operation census and write its evidence.
pub async fn run_branching_census_entry() {
    require_minio();
    let contract = BranchingContract::default();
    let harness = TestHarness::new().await;
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    config.indexing.default_num_centroids = 2;
    config.indexing.default_nprobe = 2;
    config.indexing.max_nprobe = 8;
    config.indexing.quantization = QuantizationType::None;
    config.indexing.bitmap_index = false;
    config.indexing.fts_index = false;
    let server = start_test_server_full(
        instrumented_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);

    // Warm the shared authorization/config paths on a disposable source. The
    // measured tiny and corpus sources both begin with an empty child-root map.
    let warm_source = api_ns(&harness, "branch-perf-warm-source");
    create_compacted_source(&client, &server, &warm_source).await;
    let warm_target = api_ns(&harness, "branch-perf-warm-target");
    post_fork(&client, &server, &warm_source, &warm_target).await;

    let tiny_source = api_ns(&harness, "branch-perf-source-a");
    let corpus_source = api_ns(&harness, "branch-perf-source-b");
    create_compacted_source(&client, &server, &tiny_source).await;
    create_compacted_source(&client, &server, &corpus_source).await;
    set_logical_row_count(&server, &corpus_source, CORPUS_LOGICAL_ROWS).await;

    let tiny_targets = (0..FORK_SAMPLES)
        .map(|sample| api_ns(&harness, &format!("branch-perf-target-a-{sample:02}")))
        .collect::<Vec<_>>();
    let corpus_targets = (0..FORK_SAMPLES)
        .map(|sample| api_ns(&harness, &format!("branch-perf-target-b-{sample:02}")))
        .collect::<Vec<_>>();
    let fork_tiny = measure_forks(
        &client,
        &server,
        &tiny_source,
        &tiny_targets,
        TINY_LOGICAL_ROWS,
        &counter,
        &tracker,
    )
    .await;
    let fork_corpus = measure_forks(
        &client,
        &server,
        &corpus_source,
        &corpus_targets,
        CORPUS_LOGICAL_ROWS,
        &counter,
        &tracker,
    )
    .await;
    assert_fork_contract(&fork_tiny, &fork_corpus);

    let query = measure_query_contract(
        &client,
        &server,
        &tiny_source,
        &tiny_targets[0],
        &counter,
        &tracker,
    )
    .await;
    let write = measure_write_contract(
        &client,
        &server,
        &harness,
        &tiny_source,
        &tiny_targets[1],
        &counter,
        &tracker,
    )
    .await;
    let materialization = measure_materialization_contract(
        &client,
        &server,
        &tiny_source,
        &tiny_targets[2],
        &counter,
        &tracker,
    )
    .await;

    let census = BranchingCensus {
        contract,
        fork_tiny,
        fork_corpus,
        query,
        write,
        materialization,
    };
    let root = artifact_root();
    fs::create_dir_all(&root)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", root.display()));
    let json_path = root.join("branching-census.json");
    fs::write(
        &json_path,
        serde_json::to_vec_pretty(&census).expect("branching census must serialize"),
    )
    .unwrap_or_else(|error| panic!("failed to write {}: {error}", json_path.display()));
    let report_path = root.join("report.md");
    fs::write(
        &report_path,
        format!(
            "# Branching performance contract\n\n- scenarios failed: 0\n- census: `{}`\n- tiny fork p50/p90/p99 us: `{}/{}/{}`\n- corpus fork p50/p90/p99 us: `{}/{}/{}`\n- first materialization GET/upload bytes: `{}/{}`\n",
            json_path.display(),
            census.fork_tiny.latency.p50_us,
            census.fork_tiny.latency.p90_us,
            census.fork_tiny.latency.p99_us,
            census.fork_corpus.latency.p50_us,
            census.fork_corpus.latency.p90_us,
            census.fork_corpus.latency.p99_us,
            census.materialization.first_full_corpus_get_bytes,
            census.materialization.first_target_upload_bytes,
        ),
    )
    .unwrap_or_else(|error| panic!("failed to write {}: {error}", report_path.display()));
    println!("branching performance census: {}", json_path.display());

    server.shutdown().await;
    harness.cleanup().await;
}

async fn measure_forks(
    client: &Client,
    server: &FullTestServer,
    source: &str,
    targets: &[String],
    logical_rows: usize,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> ForkCensus {
    let mut samples = Vec::with_capacity(targets.len());
    for target in targets {
        let (_, product) = measured(counter, tracker, || {
            post_fork(client, server, source, target)
        })
        .await;
        let (_, oracle) = measured(counter, tracker, || async {
            let manifest = Manifest::read(&server.store, target)
                .await
                .expect("fork oracle manifest read must succeed")
                .expect("fork oracle target manifest must exist");
            assert_eq!(manifest.version(), 1);
            assert!(manifest.has_foreign_visible_artifacts().unwrap());
            assert_eq!(
                manifest
                    .segments
                    .iter()
                    .map(|segment| segment.vector_count)
                    .sum::<usize>(),
                logical_rows,
                "fork target must retain the source's frozen logical row count"
            );
        })
        .await;
        assert_fork_sample(source, target, &product);
        samples.push(ForkSample {
            target: target.clone(),
            product,
            oracle,
        });
    }
    let latency = percentiles(
        samples
            .iter()
            .map(|sample| sample.product.wall_elapsed_us)
            .collect(),
    );
    ForkCensus {
        logical_rows,
        latency,
        samples,
    }
}

fn assert_fork_sample(source: &str, target: &str, sample: &OperationSnapshot) {
    assert!(
        sample
            .spans
            .iter()
            .all(|span| !(span.kind == SpanKind::Get && is_data_artifact(span.class))),
        "fork must issue zero artifact GETs: {source} -> {target}"
    );
    assert_eq!(
        sample.count_kind(SpanKind::Copy),
        0,
        "fork must issue zero COPYs"
    );
    assert!(
        sample.spans.iter().all(|span| {
            !(span.kind == SpanKind::Put
                && span.key.starts_with(&format!("{target}/"))
                && is_data_artifact(span.class))
        }),
        "fork must upload no target WAL, segment, or cluster artifact"
    );
    assert!(
        sample.observed_get_ops <= MAX_FORK_CONTROL_GETS,
        "fork used {} control GETs, maximum is {MAX_FORK_CONTROL_GETS}",
        sample.observed_get_ops
    );
    assert!(sample.observed_put_ops <= MAX_FORK_CONTROL_PUTS);
    let cas_puts = sample.count_request(PhysicalRequest::PutUpdate);
    assert!(
        cas_puts <= MAX_FORK_CAS_PUTS,
        "fork used {cas_puts} CAS PUTs, maximum is {MAX_FORK_CAS_PUTS}"
    );
}

fn assert_fork_contract(tiny: &ForkCensus, corpus: &ForkCensus) {
    assert_eq!(tiny.logical_rows, TINY_LOGICAL_ROWS);
    assert_eq!(corpus.logical_rows, CORPUS_LOGICAL_ROWS);
    assert_eq!(tiny.samples.len(), FORK_SAMPLES);
    assert_eq!(corpus.samples.len(), FORK_SAMPLES);
    for (tiny, corpus) in tiny.samples.iter().zip(&corpus.samples) {
        assert_eq!(
            tiny.product.operation_signature(),
            corpus.product.operation_signature(),
            "fork control operation shape must be independent of logical row count"
        );
        assert_eq!(
            tiny.product.observed_get_ops,
            corpus.product.observed_get_ops
        );
        assert_eq!(
            tiny.product.observed_put_ops,
            corpus.product.observed_put_ops
        );
    }
}

async fn measure_query_contract(
    client: &Client,
    server: &FullTestServer,
    source: &str,
    branch: &str,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> QueryCensus {
    let source_manifest = Manifest::read(&server.store, source)
        .await
        .expect("source manifest setup read must succeed")
        .expect("source manifest setup must exist");
    let physical_keys = reachable_keys(source, &source_manifest)
        .expect("query census source keys must resolve")
        .into_iter()
        .collect::<Vec<_>>();

    prepare_cold_query(server, source, &source_manifest, &physical_keys).await;
    let (root_ids, root_product) =
        measured(counter, tracker, || query_ids(client, server, source)).await;
    let (_, root_oracle) = measured(counter, tracker, || async {
        Manifest::read(&server.store, source)
            .await
            .expect("root query oracle read must succeed")
            .expect("root query oracle manifest must exist");
    })
    .await;

    // Do not invalidate physical data after the root query. This first branch
    // request must reuse cache entries populated under the source's physical
    // keys, proving cache identity follows origin rather than logical branch.
    server.manifest_cache.invalidate(branch);
    let (shared_ids, shared_product) =
        measured(counter, tracker, || query_ids(client, server, branch)).await;
    let (_, shared_oracle) = measured(counter, tracker, || async {
        assert_eq!(shared_ids, root_ids);
    })
    .await;
    assert!(
        shared_product.data_get_keys().is_empty(),
        "branch query must safely reuse source-populated physical-key caches"
    );

    prepare_cold_query(server, branch, &source_manifest, &physical_keys).await;
    let (branch_ids, branch_product) =
        measured(counter, tracker, || query_ids(client, server, branch)).await;
    let (_, branch_oracle) = measured(counter, tracker, || async {
        Manifest::read(&server.store, branch)
            .await
            .expect("branch query oracle read must succeed")
            .expect("branch query oracle manifest must exist");
    })
    .await;
    assert_eq!(
        root_ids, branch_ids,
        "branch query must preserve source view"
    );
    assert_eq!(
        root_product.data_get_keys(),
        branch_product.data_get_keys(),
        "branch query must read the same physical artifact keys as the source"
    );
    assert!(
        branch_product
            .spans
            .iter()
            .all(|span| { !(span.kind == SpanKind::Get && span.key == Manifest::s3_key(source)) }),
        "branch query must not reread its ancestry manifest"
    );

    QueryCensus {
        physical_data_keys: root_product.data_get_keys(),
        root: MeasuredOperation {
            product: root_product,
            oracle: root_oracle,
        },
        branch: MeasuredOperation {
            product: branch_product,
            oracle: branch_oracle,
        },
        shared_physical_cache: MeasuredOperation {
            product: shared_product,
            oracle: shared_oracle,
        },
    }
}

async fn measure_write_contract(
    client: &Client,
    server: &FullTestServer,
    harness: &TestHarness,
    source: &str,
    branch: &str,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> WriteCensus {
    let ordinary = api_ns(harness, "branch-perf-ordinary-write");
    create_compacted_source(client, server, &ordinary).await;
    let ordinary_op = measured_upsert(
        client,
        server,
        &ordinary,
        "ordinary-write",
        counter,
        tracker,
    )
    .await;
    let branch_op = measured_upsert(client, server, branch, "branch-write", counter, tracker).await;
    assert_eq!(
        ordinary_op.product.operation_signature(),
        branch_op.product.operation_signature(),
        "branch WAL write must preserve the ordinary object-operation census"
    );
    assert_eq!(
        ordinary_op.product.observed_get_ops,
        branch_op.product.observed_get_ops
    );
    assert_eq!(
        ordinary_op.product.observed_put_ops,
        branch_op.product.observed_put_ops
    );
    assert_eq!(
        branch_op
            .product
            .spans
            .iter()
            .filter(|span| span.kind == SpanKind::Put && span.class == ArtifactClass::Wal)
            .count(),
        1
    );
    assert!(branch_op.product.spans.iter().all(|span| {
        !(span.kind == SpanKind::Put
            && span.class == ArtifactClass::Wal
            && span.key.starts_with(&format!("{source}/")))
    }));
    WriteCensus {
        ordinary: ordinary_op,
        branch: branch_op,
    }
}

async fn measured_upsert(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    id: &str,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> MeasuredOperation {
    server.manifest_cache.invalidate(namespace);
    server.reset_wal_writer_state(namespace);
    let (_, product) = measured(counter, tracker, || {
        upsert_one(client, server, namespace, id)
    })
    .await;
    let (_, oracle) = measured(counter, tracker, || async {
        let manifest = Manifest::read(&server.store, namespace)
            .await
            .expect("write oracle manifest read must succeed")
            .expect("write oracle manifest must exist");
        assert_eq!(manifest.uncompacted_fragments().len(), 1);
    })
    .await;
    MeasuredOperation { product, oracle }
}

async fn measure_materialization_contract(
    client: &Client,
    server: &FullTestServer,
    source: &str,
    branch: &str,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> MaterializationCensus {
    let (first_result, first_product) =
        measured(counter, tracker, || server.compactor.compact(branch)).await;
    assert!(first_result
        .expect("first branch materialization must succeed")
        .segment_id
        .is_some());
    let (first_manifest, first_oracle) = measured(counter, tracker, || async {
        Manifest::read(&server.store, branch)
            .await
            .expect("materialization oracle read must succeed")
            .expect("materialization oracle manifest must exist")
    })
    .await;
    assert!(!first_manifest.has_foreign_visible_artifacts().unwrap());
    let first_full_corpus_get_bytes = first_product
        .spans
        .iter()
        .filter(|span| {
            span.kind == SpanKind::Get
                && span.key.starts_with(&format!("{source}/"))
                && is_data_artifact(span.class)
        })
        .map(|span| span.bytes)
        .sum::<u64>();
    let first_target_upload_bytes = first_product
        .spans
        .iter()
        .filter(|span| {
            span.kind == SpanKind::Put
                && span.key.starts_with(&format!("{branch}/"))
                && is_data_artifact(span.class)
        })
        .map(|span| span.bytes)
        .sum::<u64>();
    assert!(first_full_corpus_get_bytes > 0);
    assert!(first_target_upload_bytes > 0);

    upsert_one(client, server, branch, "post-materialization-write").await;
    server.manifest_cache.invalidate(branch);
    let (second_result, subsequent_product) =
        measured(counter, tracker, || server.compactor.compact(branch)).await;
    assert!(second_result
        .expect("subsequent branch compaction must succeed")
        .segment_id
        .is_some());
    let (subsequent_manifest, subsequent_oracle) = measured(counter, tracker, || async {
        Manifest::read(&server.store, branch)
            .await
            .expect("subsequent compaction oracle read must succeed")
            .expect("subsequent compaction oracle manifest must exist")
    })
    .await;
    assert!(!subsequent_manifest.has_foreign_visible_artifacts().unwrap());
    assert!(subsequent_product.spans.iter().all(|span| {
        !(span.kind == SpanKind::Get
            && span.key.starts_with(&format!("{source}/"))
            && is_data_artifact(span.class))
    }));
    assert!(subsequent_product.data_get_bytes() > 0);
    assert!(subsequent_product.data_put_bytes() > 0);

    MaterializationCensus {
        first: MeasuredOperation {
            product: first_product,
            oracle: first_oracle,
        },
        subsequent: MeasuredOperation {
            product: subsequent_product,
            oracle: subsequent_oracle,
        },
        first_full_corpus_get_bytes,
        first_target_upload_bytes,
    }
}

async fn create_compacted_source(client: &Client, server: &FullTestServer, namespace: &str) {
    let response = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&json!({
            "name": namespace,
            "dimensions": 4,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 2,
                "quantization": "none",
                "hierarchical": false,
                "bitmap_index": false,
                "fts_index": false
            }
        }))
        .send()
        .await
        .expect("branch perf namespace create request must succeed");
    assert_eq!(response.status(), StatusCode::CREATED);
    let vectors = (0..TINY_LOGICAL_ROWS)
        .map(|index| {
            json!({
                "id": format!("branch-perf-row-{index:02}"),
                "values": [index as f32, 0.0, 0.0, 0.0]
            })
        })
        .collect::<Vec<_>>();
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .expect("branch perf source upsert must succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let result = server
        .compactor
        .compact(namespace)
        .await
        .expect("branch perf source compaction must succeed");
    assert!(result.segment_id.is_some());
    server.manifest_cache.invalidate(namespace);
}

async fn set_logical_row_count(server: &FullTestServer, namespace: &str, logical_rows: usize) {
    let mut manifest = Manifest::read(&server.store, namespace)
        .await
        .expect("corpus manifest read must succeed")
        .expect("corpus manifest must exist");
    let active = manifest
        .active_segment
        .clone()
        .expect("corpus manifest must have an active segment");
    let segment = manifest
        .segments
        .iter_mut()
        .find(|segment| segment.id == active)
        .expect("corpus active segment descriptor must exist");
    segment.vector_count = logical_rows;
    manifest
        .write(&server.store, namespace)
        .await
        .expect("corpus logical row-count publication must succeed");
    server.manifest_cache.invalidate(namespace);
}

async fn post_fork(client: &Client, server: &FullTestServer, source: &str, target: &str) -> Value {
    let response = client
        .post(format!(
            "{}/v1/namespaces/{source}/branches",
            server.base_url
        ))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("branch perf fork request must succeed");
    let status = response.status();
    let body: Value = response.json().await.expect("fork response must decode");
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn query_ids(client: &Client, server: &FullTestServer, namespace: &str) -> Vec<String> {
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 10,
            "nprobe": 2,
            "consistency": "strong",
            "include_attributes": false
        }))
        .send()
        .await
        .expect("branch perf query request must succeed");
    let status = response.status();
    let body: Value = response.json().await.expect("query response must decode");
    assert_eq!(status, StatusCode::OK, "{body}");
    body["results"]
        .as_array()
        .expect("query results must be an array")
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .expect("query result ID must be a string")
                .to_string()
        })
        .collect()
}

async fn upsert_one(client: &Client, server: &FullTestServer, namespace: &str, id: &str) {
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [{ "id": id, "values": [0.0, 1.0, 0.0, 0.0] }]
        }))
        .send()
        .await
        .expect("branch perf upsert request must succeed");
    assert_eq!(response.status(), StatusCode::OK);
}

/// Returns the query path to a genuinely cold state for `physical_keys`.
///
/// Immutable segment artifacts are **not** cached under their object key. They
/// are cached by physical incarnation, `artifact-origin/<incarnation>/<store
/// key>`, so that two branches sharing a source segment share one entry and
/// neither can reconstruct a key from its own name. Invalidating the raw store
/// key therefore matches nothing and silently leaves the artifact warm.
///
/// That is what this helper used to do. It went unnoticed because the census
/// has never been executed: the first measured query in a run is cold anyway,
/// having never populated the cache, so the no-op eviction only shows up on
/// the *second* cold preparation - which is precisely the branch-versus-source
/// comparison this census exists to make. `Manifest::segment_artifact_cache_key`
/// is the seam for exactly this, and its own rustdoc warns that a caller
/// carrying a divergent copy of the key format is what silently rots.
///
/// The derived key is computed for every (segment, key) pair rather than by
/// parsing segment ids out of object keys. Pairs that do not belong together
/// yield a key that is simply absent from the cache, so over-invalidating is
/// harmless and cannot miss the pair that does belong together.
async fn prepare_cold_query(
    server: &FullTestServer,
    namespace: &str,
    manifest: &Manifest,
    physical_keys: &[String],
) {
    server.manifest_cache.invalidate(namespace);
    server.clear_decoded_artifact_cache();
    for key in physical_keys {
        server
            .cache
            .invalidate(key)
            .await
            .unwrap_or_else(|error| panic!("failed to invalidate query census key {key}: {error}"));
        for segment in &manifest.segments {
            let Ok(cache_key) = manifest.segment_artifact_cache_key(segment, key) else {
                continue;
            };
            server
                .cache
                .invalidate(&cache_key)
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to invalidate query census cache key {cache_key}: {error}")
                });
        }
    }
}

async fn measured<T, F, Fut>(
    counter: &GetCounter,
    tracker: &DepthTracker,
    operation: F,
) -> (T, OperationSnapshot)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    await_idle(tracker).await;
    counter.reset();
    tracker.reset();
    let started = Instant::now();
    let result = operation().await;
    await_idle(tracker).await;
    let wall_elapsed_us = started.elapsed().as_micros() as u64;
    let classes = counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect();
    let snapshot = OperationSnapshot {
        wall_elapsed_us,
        observed_get_ops: counter.total_observed_gets(),
        observed_put_ops: counter.total_observed_puts(),
        classes,
        spans: tracker.take_spans(),
    };
    (result, snapshot)
}

async fn await_idle(tracker: &DepthTracker) {
    let mut zero_streak = 0;
    for _ in 0..4_096 {
        tokio::task::yield_now().await;
        if tracker.active_operations() == 0 {
            zero_streak += 1;
            if zero_streak == 8 {
                return;
            }
        } else {
            zero_streak = 0;
        }
    }
    panic!(
        "branch census did not reach object-store quiescence: {} active operations",
        tracker.active_operations()
    );
}

fn is_data_artifact(class: ArtifactClass) -> bool {
    !matches!(class, ArtifactClass::Manifest | ArtifactClass::Other)
}

fn percentiles(mut values: Vec<u64>) -> LatencyPercentiles {
    assert!(!values.is_empty());
    values.sort_unstable();
    let at = |percent: usize| {
        let rank = (values.len() * percent).div_ceil(100).saturating_sub(1);
        values[rank]
    };
    LatencyPercentiles {
        p50_us: at(50),
        p90_us: at(90),
        p99_us: at(99),
    }
}

fn artifact_root() -> PathBuf {
    std::env::var_os("ZEPPELIN_PERF_ARTIFACTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/perf-contract/branching-census"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_branching_catalog_covers_each_required_cost_boundary() {
        assert_eq!(BRANCHING_SCENARIOS.len(), 7);
        assert!(BRANCHING_SCENARIOS.contains(&"fork_tiny"));
        assert!(BRANCHING_SCENARIOS.contains(&"fork_corpus"));
        assert!(BRANCHING_SCENARIOS.contains(&"branch_query"));
        assert!(BRANCHING_SCENARIOS.contains(&"branch_wal_write"));
        assert!(BRANCHING_SCENARIOS.contains(&"branch_materialize_first"));
        assert!(BRANCHING_SCENARIOS.contains(&"branch_compact_subsequent"));
        assert_eq!(BranchingContract::default().corpus_logical_rows, 1_000_000);
    }

    #[test]
    fn percentile_contract_uses_nearest_rank() {
        let observed = percentiles(vec![8, 1, 7, 2, 6, 3, 5, 4]);
        assert_eq!(observed.p50_us, 4);
        assert_eq!(observed.p90_us, 8);
        assert_eq!(observed.p99_us, 8);
    }
}
