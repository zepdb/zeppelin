mod common;

use std::collections::{BTreeSet, HashMap};
use std::fs;

use common::artifact_fixtures::{
    hex_sha256, load_manifest, version_directories, version_root, write_current_corpus,
    VERSION_DIRECTORY,
};
use common::harness::TestHarness;
use ulid::Ulid;
use zeppelin::format::fixture::{
    encode_manifest_envelope_v2_fixture, reencode_checksum_input, validate_artifact,
    validate_manifest_for_namespace,
};
use zeppelin::format::FORMATS;
use zeppelin::fts::rank_by::RankBy;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::bitmap::evaluate::evaluate_filter_bitmap;
use zeppelin::index::bitmap::ClusterBitmapIndex;
use zeppelin::query::{execute_bm25_query, execute_query, QueryParams};
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::manifest::CoarsePayloadEncoding;
use zeppelin::wal::{Manifest, WalFragment, WalReader};

#[tokio::test]
async fn generate_unreleased_936994f() {
    if std::env::var_os("UPDATE_ARTIFACT_FIXTURES").is_none() {
        return;
    }
    let manifest = write_current_corpus().await;
    assert_eq!(manifest.corpus.version_directory, VERSION_DIRECTORY);
}

#[test]
fn every_version_directory_decodes_every_registry_family() {
    let directories = version_directories();
    assert!(
        !directories.is_empty(),
        "artifact corpus has no version directories"
    );
    let registry = FORMATS
        .iter()
        .map(|format| (format.name, format))
        .collect::<HashMap<_, _>>();

    for directory in directories {
        let manifest = load_manifest(&directory);
        assert_eq!(
            manifest.corpus.version_directory,
            directory.file_name().unwrap().to_string_lossy()
        );
        let declared_families = manifest
            .files
            .iter()
            .map(|file| file.family.as_str())
            .collect::<BTreeSet<_>>();
        let registry_families = FORMATS
            .iter()
            .map(|format| format.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            declared_families,
            registry_families,
            "{} must make a fixture decision for every FORMATS row",
            directory.display()
        );

        for file in &manifest.files {
            let format = registry
                .get(file.family.as_str())
                .unwrap_or_else(|| panic!("unknown fixture family {}", file.family));
            let path = directory.join(&file.path);
            assert!(path.starts_with(&directory));
            let bytes =
                fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            assert_eq!(bytes.len() as u64, file.size_bytes, "{}", path.display());
            assert_eq!(hex_sha256(&bytes), file.sha256, "{}", path.display());
            validate_artifact(&file.family, &bytes)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            if file.family == "manifest"
                && matches!(
                    file.path.as_str(),
                    "manifest.bin" | "manifest_envelope_v2.bin"
                )
            {
                validate_manifest_for_namespace(&bytes, &manifest.corpus.namespace)
                    .expect("current manifest namespace binding must validate");
            }
            if format.checksum_input && file.comparison == "bytes" {
                assert_eq!(
                    reencode_checksum_input(&file.family, &bytes)
                        .unwrap_or_else(|error| panic!("{}: {error}", path.display())),
                    bytes,
                    "checksum-input family {} must re-encode byte-identically",
                    file.family
                );
            }
        }
    }
}

#[test]
fn wal_fixture_replaces_checksum_stability_properties_with_real_types() {
    let manifest = load_manifest(&version_root());
    let wal = manifest
        .files
        .iter()
        .find(|file| file.family == "wal_fragment")
        .expect("WAL fixture must be declared");
    let bytes = fs::read(version_root().join(&wal.path)).expect("WAL fixture must read");
    let fragment = WalFragment::from_bytes(&bytes).expect("WAL fixture checksum must validate");
    fragment
        .validate_checksum()
        .expect("fixture checksum must remain valid");
    assert_eq!(fragment.to_bytes().expect("WAL must re-encode"), bytes);
    assert_eq!(
        WalFragment::from_bytes(&bytes)
            .expect("first round trip")
            .to_bytes()
            .expect("second encoding"),
        bytes,
        "double round trip must preserve exact current bytes"
    );

    let mut left_attrs = HashMap::new();
    left_attrs.insert("alpha".to_string(), AttributeValue::Integer(1));
    left_attrs.insert("beta".to_string(), AttributeValue::Bool(true));
    let mut right_attrs = HashMap::new();
    right_attrs.insert("beta".to_string(), AttributeValue::Bool(true));
    right_attrs.insert("alpha".to_string(), AttributeValue::Integer(1));
    let vector = |attributes| VectorEntry {
        id: "ordered-map".to_string(),
        values: vec![0.25, -0.5],
        attributes: Some(attributes),
    };
    let fixed_id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FB3").expect("fixed ULID");
    let mut left = WalFragment::try_new(vec![vector(left_attrs)], Vec::new()).expect("left WAL");
    let mut right = WalFragment::try_new(vec![vector(right_attrs)], Vec::new()).expect("right WAL");
    left.id = fixed_id;
    right.id = fixed_id;
    assert_eq!(left.checksum, right.checksum);
    assert_eq!(
        left.to_bytes().expect("left bytes"),
        right.to_bytes().expect("right bytes")
    );

    let mut tampered = fragment.clone();
    tampered.vectors[0].values[0] += 1.0;
    tampered
        .validate_checksum()
        .expect_err("payload mutation must invalidate checksum");
    let tampered_bytes = tampered.to_bytes().expect("tampered structure encodes");
    WalFragment::from_bytes(&tampered_bytes)
        .expect_err("checked decoder must reject a stale checksum");
}

#[test]
fn manifest_envelope_v2_min_reader_99_refusal_precedes_payload_decode() {
    let root = version_root();
    let v1 = fs::read(root.join("manifest.bin")).expect("v1 manifest fixture must read");
    let v2 =
        fs::read(root.join("manifest_envelope_v2.bin")).expect("v2 manifest fixture must read");
    let header_len = usize::from(u16::from_be_bytes([v2[1], v2[2]]));
    assert_eq!(
        &v2[3 + header_len..],
        &v1[1..],
        "golden v2 payload must be byte-identical to the golden v1 body"
    );
    Manifest::from_bytes(&v2).expect("golden envelope v2 must decode");

    let synthetic = encode_manifest_envelope_v2_fixture(
        b"\xc1deliberately invalid payload",
        "99.0.0",
        "99.0.0",
        1_700_000_000,
    )
    .expect("synthetic future-reader envelope must encode");
    let error = Manifest::from_bytes(&synthetic)
        .expect_err("reader below min_reader must refuse before decoding the payload");
    assert_eq!(
        error.to_string(),
        format!(
            "serialization error: manifest requires zeppelin >= 99.0.0; this binary is {}",
            env!("CARGO_PKG_VERSION")
        )
    );
}

#[test]
fn fixture_manifest_bitmap_and_late_state_semantics_are_pinned() {
    let root = version_root();
    let declaration = load_manifest(&root);
    let declared_keys = declaration
        .files
        .iter()
        .filter_map(|file| file.object_key.as_deref())
        .collect::<BTreeSet<_>>();
    let manifest_bytes = fs::read(root.join("manifest.bin")).expect("manifest fixture");
    let manifest = Manifest::from_bytes(&manifest_bytes).expect("manifest must decode");
    let fragment = manifest.fragments.first().expect("one WAL fragment");
    let fragment_key = WalFragment::object_store_key(&declaration.corpus.namespace, &fragment.id);
    assert!(declared_keys.contains(fragment_key.as_str()));
    let segment = manifest.segments.first().expect("one IVF segment");
    assert!(declared_keys.contains(segment.sketch.as_ref().expect("sketch").key.as_str()));
    assert!(declared_keys.contains(segment.bootstrap.as_ref().expect("bootstrap").key.as_str()));
    assert!(declared_keys.contains(
        segment
            .membership
            .as_ref()
            .expect("membership")
            .key
            .as_str()
    ));
    for object in &segment.cluster_objects {
        assert!(declared_keys.contains(object.key.as_str()));
    }

    let bitmap_bytes = fs::read(root.join("segment/bitmap_0.bin")).expect("bitmap fixture");
    let bitmap = ClusterBitmapIndex::from_bytes(&bitmap_bytes).expect("bitmap must decode");
    let matching = evaluate_filter_bitmap(
        &Filter::Eq {
            field: "tenant".to_string(),
            value: AttributeValue::String("blue".to_string()),
        },
        &bitmap,
    )
    .expect("tenant bitmap must answer exactly")
    .iter()
    .collect::<Vec<_>>();
    assert_eq!(matching, vec![0], "cluster-zero blue rows changed");

    let late_bytes = fs::read(root.join("late/state.bin")).expect("late state fixture");
    assert_eq!(
        zeppelin::wal::LateStateSection::from_bytes(&late_bytes)
            .expect("late state must decode")
            .to_bytes()
            .expect("late state must encode"),
        late_bytes
    );
}

fn remap_fixture_key(key: &str, namespace: &str) -> String {
    key.strip_prefix(zeppelin::format::fixture::FIXTURE_NAMESPACE)
        .map_or_else(|| key.to_string(), |suffix| format!("{namespace}{suffix}"))
}

async fn load_query_fixture(
    harness: &TestHarness,
    namespace: &str,
) -> (Vec<f32>, HashMap<String, FtsFieldConfig>) {
    let fixture_root = version_root();
    let declaration = load_manifest(&fixture_root);
    for file in &declaration.files {
        let Some(object_key) = file.object_key.as_deref() else {
            continue;
        };
        if file.family == "manifest" || file.family == "namespace_metadata" {
            continue;
        }
        let bytes = fs::read(fixture_root.join(&file.path)).expect("query artifact must read");
        harness
            .store
            .put(&remap_fixture_key(object_key, namespace), bytes.into())
            .await
            .expect("query artifact must upload");
    }

    let manifest_bytes = fs::read(fixture_root.join("manifest.bin")).expect("manifest fixture");
    let source = Manifest::from_bytes(&manifest_bytes).expect("fixture manifest must decode");
    let now = chrono::DateTime::parse_from_rfc3339(common::artifact_fixtures::FIXED_TIMESTAMP)
        .expect("fixed timestamp")
        .with_timezone(&chrono::Utc);
    let incarnation = uuid::Uuid::from_u128(0x11111111_2222_3333_4444_555555555555);
    let mut target = Manifest::new_at(now);
    target
        .bind_namespace_incarnation(incarnation)
        .expect("query fixture manifest must bind");
    for fragment in source.fragments {
        target.add_fragment_at(fragment, now);
    }
    for mut segment in source.segments {
        if let Some(sketch) = &mut segment.sketch {
            sketch.key = remap_fixture_key(&sketch.key, namespace);
        }
        if let Some(bootstrap) = &mut segment.bootstrap {
            bootstrap.key = remap_fixture_key(&bootstrap.key, namespace);
        }
        if let Some(membership) = &mut segment.membership {
            membership.key = remap_fixture_key(&membership.key, namespace);
        }
        for object in &mut segment.cluster_objects {
            object.key = remap_fixture_key(&object.key, namespace);
        }
        let segment_id = segment.id.clone();
        target.add_segment_with_limits_at(segment, 1_000, 10, now);
        target.set_coarse_payload_encoding(segment_id, CoarsePayloadEncoding::TwoBit);
    }
    target
        .write(&harness.store, namespace)
        .await
        .expect("query fixture manifest must publish");

    (
        common::artifact_fixtures::fixed_vectors()[0].values.clone(),
        HashMap::from([("body".to_string(), FtsFieldConfig::default())]),
    )
}

#[tokio::test]
async fn fixture_queries_through_production_ann_and_bm25_paths() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("golden-artifacts");
    let (ann_query, fts) = load_query_fixture(&harness, &namespace).await;
    let reader = WalReader::new(harness.store.clone());
    let ann = execute_query(QueryParams {
        store: &harness.store,
        wal_reader: &reader,
        namespace: &namespace,
        query: &ann_query,
        top_k: 3,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 4,
        rerank_coalesce_gap_bytes: 64 * 1024,
        cache: None,
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .expect("fixture ANN query must succeed");
    let ann_ids = ann
        .results
        .iter()
        .map(|result| result.id.as_str())
        .collect::<Vec<_>>();

    let bm25 = execute_bm25_query(
        &harness.store,
        &reader,
        &namespace,
        &RankBy::Bm25 {
            field: "body".to_string(),
            query: "zeppelin golden artifact".to_string(),
        },
        &fts,
        3,
        None,
        ConsistencyLevel::Strong,
        false,
        None,
        None,
        None,
        None,
        None,
        64,
        10_000,
        true,
    )
    .await
    .expect("fixture BM25 query must succeed");
    let bm25_ids = bm25
        .results
        .iter()
        .map(|result| result.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ann_ids, ["vec_0", "vec_5", "vec_6"]);
    assert_eq!(bm25_ids, ["vec_0", "vec_1", "vec_2"]);
    harness.cleanup().await;
}
