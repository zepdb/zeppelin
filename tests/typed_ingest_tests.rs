mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use chrono::Duration as ChronoDuration;
use reqwest::StatusCode;
use serde_json::{json, Value};
use zeppelin::compaction::gc::{reachable_keys_with_late_state, GcNamespaceIncarnation, GcRunner};
use zeppelin::config::{Config, GcConfig};
use zeppelin::embedding::{
    ArtifactChecksum, EncoderInputRef, ImageObjectRef, InputModality,
    LateInteractionNamespaceConfig, RetrievalUnitRecord, TextContentRef,
};
use zeppelin::namespace::branching::ArtifactOrigin;
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::types::{AttributeValue, DistanceMetric, IndexType, VectorEntry};
use zeppelin::wal::manifest::NamedSnapshot;
use zeppelin::wal::{LateStateSection, Manifest, SourceInventoryRef, WalReader, WalWriter};

use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, start_test_server_on_store_with_config, start_test_server_with_config,
};

fn require_minio() {
    TestHarness::require_cas_backend();
}

async fn create_late_namespace(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    modalities: &[&str],
    with_fts: bool,
) {
    let full_text_search = if with_fts {
        json!({
            "title": {
                "language": "english",
                "stemming": true,
                "remove_stopwords": true
            }
        })
    } else {
        json!({})
    };
    let response = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": namespace,
            "dimensions": 0,
            "index_type": "late_interaction_fde",
            "late_interaction": { "accepted_modalities": modalities },
            "full_text_search": full_text_search
        }))
        .send()
        .await
        .expect("late namespace create request must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("late namespace create response must decode");
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

async fn append_units(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    body: Value,
) -> Value {
    let response = client
        .post(format!(
            "{base_url}/v1/namespaces/{namespace}/retrieval-units"
        ))
        .json(&body)
        .send()
        .await
        .expect("retrieval-unit append request must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("retrieval-unit append response must decode");
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

async fn bm25_ids(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    text: &str,
) -> BTreeSet<String> {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "rank_by": ["title", "BM25", text],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("BM25 request must complete");
    let status = response.status();
    let body: Value = response.json().await.expect("BM25 response must decode");
    assert_eq!(status, StatusCode::OK, "{body}");
    body["results"]
        .as_array()
        .expect("BM25 response must contain results")
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .expect("BM25 result must contain an ID")
                .to_string()
        })
        .collect()
}

fn text_record(id: &str, text: &str) -> RetrievalUnitRecord {
    let input = EncoderInputRef::Text {
        content: TextContentRef::Inline(text.to_string()),
    };
    RetrievalUnitRecord {
        id: id.to_string(),
        content_hash: input.content_hash().expect("fixture text input must hash"),
        input,
        parent_id: None,
        unit_ordinal: None,
        attributes: None,
    }
}

fn image_record(
    namespace: &str,
    id: &str,
    bytes: Bytes,
) -> (RetrievalUnitRecord, SourceInventoryRef, Bytes) {
    let checksum = ArtifactChecksum::digest(&bytes);
    let size_bytes = u64::try_from(bytes.len()).expect("fixture image length must fit u64");
    let mut input = EncoderInputRef::Image {
        image: ImageObjectRef {
            key: String::new(),
            checksum,
            media_type: "image/jpeg".to_string(),
            encoded_size_bytes: size_bytes,
            width: 2,
            height: 2,
        },
    };
    let content_hash = input.content_hash().expect("fixture image input must hash");
    let key = SourceInventoryRef::object_store_key(namespace, content_hash);
    if let EncoderInputRef::Image { image } = &mut input {
        image.key = key.clone();
    }
    let source = SourceInventoryRef {
        key,
        checksum,
        size_bytes,
        media_type: "image/jpeg".to_string(),
        artifact_origin: None,
    };
    (
        RetrievalUnitRecord {
            id: id.to_string(),
            input,
            content_hash,
            parent_id: None,
            unit_ordinal: None,
            attributes: None,
        },
        source,
        bytes,
    )
}

async fn create_direct_late_namespace(store: &zeppelin::storage::ZeppelinStore, namespace: &str) {
    NamespaceManager::new(store.clone())
        .create_typed_with_fts_and_index_config(
            namespace,
            0,
            DistanceMetric::DotProduct,
            IndexType::LateInteractionFde,
            Some(LateInteractionNamespaceConfig {
                accepted_modalities: vec![
                    InputModality::Text,
                    InputModality::Image,
                    InputModality::ImageText,
                ],
            }),
            HashMap::new(),
            None,
        )
        .await
        .expect("direct late namespace create must succeed");
}

#[tokio::test]
async fn typed_lifecycle_restarts_with_checked_sources_and_fts_ordering() {
    require_minio();
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(None).await;
    let client = client_with_bearer(&admin_bearer);
    let namespace = harness.artifact_origin_namespace("typed-lifecycle");
    create_late_namespace(
        &client,
        &base_url,
        &namespace,
        &["text", "image", "image_text"],
        true,
    )
    .await;

    let ack = append_units(
        &client,
        &base_url,
        &namespace,
        json!({
            "upserts": [
                {
                    "id": "text",
                    "input": { "type": "text", "text": "retained text" },
                    "attributes": { "title": "oldterm zeppelin" }
                },
                {
                    "id": "image",
                    "input": {
                        "type": "image",
                        "image_base64": "AQIDBA==",
                        "media_type": "image/jpeg",
                        "width": 2,
                        "height": 2
                    }
                },
                {
                    "id": "mixed",
                    "input": {
                        "type": "image_text",
                        "image_base64": "CQgH",
                        "media_type": "image/png",
                        "width": 3,
                        "height": 1,
                        "text": "retained caption"
                    },
                    "attributes": { "title": "mixedterm caption" }
                }
            ]
        }),
    )
    .await;
    assert_eq!(ack["semantic_state"], json!("pending"));
    assert_eq!(ack["semantic_sequence"], json!(0));
    assert!(
        ack["manifest_generation"].as_u64().is_some(),
        "typed write acknowledgement must carry its committed manifest generation"
    );

    assert_eq!(
        bm25_ids(&client, &base_url, &namespace, "oldterm").await,
        BTreeSet::from(["text".to_string()])
    );
    assert_eq!(
        bm25_ids(&client, &base_url, &namespace, "mixedterm").await,
        BTreeSet::from(["mixed".to_string()])
    );
    assert!(
        bm25_ids(&client, &base_url, &namespace, "imageonlysecret")
            .await
            .is_empty(),
        "an image-only unit must not become lexically visible"
    );

    append_units(
        &client,
        &base_url,
        &namespace,
        json!({
            "upserts": [{
                "id": "text",
                "input": { "type": "text", "text": "updated retained text" },
                "attributes": { "title": "newterm zeppelin" }
            }]
        }),
    )
    .await;
    append_units(
        &client,
        &base_url,
        &namespace,
        json!({ "deletes": ["mixed"] }),
    )
    .await;

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("typed namespace must have a manifest");
    let reader = WalReader::new(harness.store.clone());
    let mut visible = BTreeMap::<String, RetrievalUnitRecord>::new();
    let mut sequence_numbers = Vec::new();
    for reference in manifest.uncompacted_input_fragments() {
        sequence_numbers.push(reference.sequence_number);
        let fragment = reader
            .read_input_fragment(&namespace, &reference.id)
            .await
            .expect("checked input-fragment read must succeed");
        for record in fragment.upserts {
            visible.insert(record.id.clone(), record);
        }
        for id in fragment.deletes {
            visible.remove(&id);
        }
    }
    assert_eq!(sequence_numbers, vec![1, 2, 3]);
    assert_eq!(
        visible.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from(["image".to_string(), "text".to_string()])
    );
    assert_eq!(
        visible["text"]
            .attributes
            .as_ref()
            .and_then(|attributes| attributes.get("title")),
        Some(&AttributeValue::String("newterm zeppelin".to_string()))
    );

    let section = manifest
        .load_late_state(&harness.store)
        .await
        .expect("late section read must succeed")
        .expect("image writes must publish a late section");
    assert_eq!(section.source_inventory.len(), 2);
    let metadata = NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .expect("namespace metadata read must succeed");
    let origin = ArtifactOrigin {
        namespace: NamespaceId::new(namespace.clone()).expect("namespace must be valid"),
        incarnation: metadata
            .incarnation_id
            .expect("new namespace must have an incarnation"),
    };
    let mut retained_images = BTreeSet::new();
    for source in &section.source_inventory {
        let bytes = section
            .read_source_checked(&harness.store, source, &origin)
            .await
            .expect("inventory source read must verify size and checksum");
        retained_images.insert(bytes.to_vec());
    }
    assert_eq!(
        retained_images,
        BTreeSet::from([vec![1, 2, 3, 4], vec![9, 8, 7]])
    );

    let (restarted_url, _cache, _cache_dir, _restarted_bearer) =
        start_test_server_on_store_with_config(
            &harness,
            harness.store.clone(),
            Some(harness.prefix.clone()),
            Config::default(),
        )
        .await;
    let restarted_client = client_with_bearer(&admin_bearer);
    assert_eq!(
        bm25_ids(&restarted_client, &restarted_url, &namespace, "newterm").await,
        BTreeSet::from(["text".to_string()])
    );
    assert!(
        bm25_ids(&restarted_client, &restarted_url, &namespace, "oldterm")
            .await
            .is_empty(),
        "an update must suppress the older fragment's lexical value after restart"
    );
    assert!(
        bm25_ids(&restarted_client, &restarted_url, &namespace, "mixedterm")
            .await
            .is_empty(),
        "a later tombstone must suppress the earlier typed record after restart"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn typed_sources_obey_gc_and_snapshot_roots() {
    require_minio();
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("typed-gc");
    create_direct_late_namespace(&harness.store, &namespace).await;
    let writer = WalWriter::new(harness.store.clone());

    let (old_record, old_source, old_bytes) =
        image_record(&namespace, "old-image", Bytes::from_static(b"old-image"));
    let (_, old_manifest) = writer
        .append_retrieval_units(
            &namespace,
            vec![old_record],
            Vec::new(),
            vec![(old_source.clone(), old_bytes)],
        )
        .await
        .expect("old image append must succeed");
    let old_section = old_manifest
        .late_state
        .clone()
        .expect("image append must publish a section");
    let old_section_body = old_manifest
        .load_late_state(&harness.store)
        .await
        .expect("old section read must succeed")
        .expect("old section must exist");
    assert!(
        reachable_keys_with_late_state(&namespace, &old_manifest, Some(&old_section_body))
            .expect("live reachability expansion must succeed")
            .contains(&old_source.key),
        "the live source must be in the exact reachable set"
    );
    NamedSnapshot::create(
        &harness.store,
        &namespace,
        "before-source-replacement",
        old_manifest.version(),
    )
    .await
    .expect("snapshot pin must succeed");

    let (_new_record, new_source, new_bytes) =
        image_record(&namespace, "new-image", Bytes::from_static(b"new-image"));
    harness
        .store
        .put(&new_source.key, new_bytes)
        .await
        .expect("new source upload must succeed");
    let replacement = LateStateSection {
        source_inventory: vec![new_source.clone()],
        artifact_origins: Vec::new(),
        ..LateStateSection::new()
    };
    let (mut current, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("manifest must exist");
    current
        .publish_with_late_state(&harness.store, &namespace, &version, &replacement)
        .await
        .expect("replacement section publication must succeed");
    let (mut current, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("manifest reread must succeed")
        .expect("manifest must exist");
    current
        .publish_with_late_state(&harness.store, &namespace, &version, &replacement)
        .await
        .expect("second root generation must publish");

    let orphan_key = SourceInventoryRef::object_store_key(
        &namespace,
        zeppelin::embedding::ContentHash::new([0xee; 32]),
    );
    harness
        .store
        .put(&orphan_key, Bytes::from_static(b"crash orphan"))
        .await
        .expect("orphan source upload must succeed");
    let pending_source_key = SourceInventoryRef::object_store_key(
        &namespace,
        zeppelin::embedding::ContentHash::new([0xdd; 32]),
    );
    harness
        .store
        .put(&pending_source_key, Bytes::from_static(b"deferred source"))
        .await
        .expect("deferred source upload must succeed");
    let (mut pending_manifest, pending_version) =
        Manifest::read_versioned(&harness.store, &namespace)
            .await
            .expect("pending source manifest read must succeed")
            .expect("pending source manifest must exist");
    pending_manifest
        .pending_deletes
        .push(pending_source_key.clone());
    pending_manifest
        .write_conditional(&harness.store, &namespace, &pending_version)
        .await
        .expect("pending source root update must succeed");
    let retained_history = zeppelin::compaction::gc::retained_manifest_history_reachable_keys(
        &harness.store,
        &namespace,
    )
    .await
    .expect("retained history expansion must succeed");
    assert!(
        !retained_history.contains(&pending_source_key),
        "the deferred source fixture must not be history-reachable"
    );

    let gc = GcConfig {
        horizon_secs: 1,
        compaction_upload_window_secs: 1,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: 1,
        pitr_retention_secs: 0,
    };
    let pending_source_modified = harness
        .store
        .list_prefix_meta(&format!("{namespace}/sources/"))
        .await
        .expect("source inventory LIST must succeed")
        .into_iter()
        .find(|object| object.key == pending_source_key)
        .expect("pending source must appear in LIST metadata")
        .last_modified;
    let now = pending_source_modified + ChronoDuration::seconds(5);
    let metadata = NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .expect("typed namespace metadata must load");
    let incarnation = GcNamespaceIncarnation::from_metadata(&metadata);
    let mut runner = GcRunner::new(harness.store.clone(), gc);
    let mut pending_deleted = 0;
    for offset in 0..4 {
        let report = runner
            .run_cycle_at(incarnation.clone(), now + ChronoDuration::seconds(offset))
            .await
            .expect("cold and warm GC passes must succeed");
        pending_deleted += report.pending_deletes_deleted;
    }

    assert!(
        harness
            .store
            .exists(&new_source.key)
            .await
            .expect("new source existence check must succeed"),
        "the live source must survive GC"
    );
    assert!(
        harness
            .store
            .exists(&old_source.key)
            .await
            .expect("old source existence check must succeed"),
        "the snapshot-pinned source must survive GC"
    );
    assert!(
        harness
            .store
            .exists(&old_section.key)
            .await
            .expect("old section existence check must succeed"),
        "the snapshot-pinned section must survive GC"
    );
    assert!(
        !harness
            .store
            .exists(&orphan_key)
            .await
            .expect("orphan existence check must succeed"),
        "a source uploaded without a manifest CAS must collect"
    );
    assert!(
        !harness
            .store
            .exists(&pending_source_key)
            .await
            .expect("pending source existence check must succeed"),
        "an aged source routed through pending_deletes must drain"
    );
    assert_eq!(
        pending_deleted, 1,
        "the pending source must be accepted exactly once"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn typed_branch_reads_inherited_input_zero_copy_and_blocks_release() {
    require_minio();
    let mut config = Config::default();
    config.branching.enabled = true;
    config.security.policy_refresh_secs = 3_600;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("typed-branch-source");
    let target = harness.artifact_origin_namespace("typed-branch-target");
    create_late_namespace(
        &client,
        &base_url,
        &source,
        &["text", "image", "image_text"],
        true,
    )
    .await;
    append_units(
        &client,
        &base_url,
        &source,
        json!({
            "upserts": [{
                "id": "inherited",
                "input": {
                    "type": "image_text",
                    "image_base64": "AQMFBw==",
                    "media_type": "image/webp",
                    "width": 2,
                    "height": 2,
                    "text": "inherited caption"
                },
                "attributes": { "title": "branchterm inherited" }
            }]
        }),
    )
    .await;

    let response = client
        .post(format!("{base_url}/v1/namespaces/{source}/branches"))
        .json(&json!({ "target": target }))
        .send()
        .await
        .expect("branch request must complete");
    let status = response.status();
    let body: Value = response.json().await.expect("branch response must decode");
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        bm25_ids(&client, &base_url, &target, "branchterm").await,
        BTreeSet::from(["inherited".to_string()]),
        "the branch must resolve and replay its inherited input fragment"
    );

    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist");
    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest read must succeed")
        .expect("target manifest must exist");
    assert_eq!(target_manifest.input_fragments.len(), 1);
    assert!(
        target_manifest.input_fragments[0].artifact_origin.is_some(),
        "the inherited input fragment must carry its physical origin"
    );
    assert!(
        target_manifest
            .late_state
            .as_ref()
            .expect("target must inherit the source section")
            .artifact_origin
            .is_some(),
        "the inherited late section must carry its physical origin"
    );
    let source_fragment_key = zeppelin::wal::EncoderInputWalFragment::object_store_key(
        &source,
        &source_manifest.input_fragments[0].id,
    );
    let target_fragment_key = zeppelin::wal::EncoderInputWalFragment::object_store_key(
        &target,
        &target_manifest.input_fragments[0].id,
    );
    assert!(harness
        .store
        .exists(&source_fragment_key)
        .await
        .expect("source fragment existence check must succeed"));
    assert!(
        !harness
            .store
            .exists(&target_fragment_key)
            .await
            .expect("target fragment existence check must succeed"),
        "branch activation must not copy the input fragment"
    );
    let target_section = target_manifest
        .load_late_state(&harness.store)
        .await
        .expect("target must resolve the inherited section")
        .expect("target must have a late section");
    assert!(
        target_section
            .source_inventory
            .iter()
            .all(|source_ref| source_ref.key.starts_with(&format!("{source}/"))),
        "section-resident sources must remain source-owned"
    );
    assert!(
        !target_manifest
            .visible_refs_are_local_with_late_state(Some(&target_section))
            .expect("locality projection must succeed"),
        "root release must remain blocked while typed inputs and sources are foreign"
    );

    append_units(
        &client,
        &base_url,
        &target,
        json!({
            "upserts": [{
                "id": "target-image",
                "input": {
                    "type": "image",
                    "image_base64": "AgQGCA==",
                    "media_type": "image/webp",
                    "width": 2,
                    "height": 2
                }
            }]
        }),
    )
    .await;
    let rebound_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest reread must succeed")
        .expect("target manifest must exist");
    assert!(
        rebound_manifest
            .late_state
            .as_ref()
            .expect("target image append must publish a section")
            .artifact_origin
            .is_none(),
        "the republished section root must be target-owned"
    );
    let rebound_section = rebound_manifest
        .load_late_state(&harness.store)
        .await
        .expect("rebased target section must validate")
        .expect("rebased target section must exist");
    let target_metadata = NamespaceManager::new(harness.store.clone())
        .get(&target)
        .await
        .expect("target metadata must load");
    let target_origin = ArtifactOrigin {
        namespace: NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        incarnation: target_metadata
            .incarnation_id
            .expect("target must have an incarnation"),
    };
    let inherited_source = rebound_section
        .source_inventory
        .iter()
        .find(|source_ref| source_ref.key.starts_with(&format!("{source}/")))
        .expect("rebased section must retain the inherited source");
    assert!(
        inherited_source.artifact_origin.is_some(),
        "an inherited source needs an explicit owner after a target-local republish"
    );
    let local_source = rebound_section
        .source_inventory
        .iter()
        .find(|source_ref| source_ref.key.starts_with(&format!("{target}/")))
        .expect("rebased section must include the target-local source");
    assert!(
        local_source.artifact_origin.is_none(),
        "a target-owned source must inherit the target section owner"
    );
    let mut source_bodies = BTreeSet::new();
    for source_ref in &rebound_section.source_inventory {
        source_bodies.insert(
            rebound_section
                .read_source_checked(&harness.store, source_ref, &target_origin)
                .await
                .expect("foreign and local sources must remain readable")
                .to_vec(),
        );
    }
    assert_eq!(
        source_bodies,
        BTreeSet::from([vec![1, 3, 5, 7], vec![2, 4, 6, 8]])
    );
    assert!(
        !rebound_manifest
            .visible_refs_are_local_with_late_state(Some(&rebound_section))
            .expect("rebased locality projection must succeed"),
        "foreign source ownership must still block materialization"
    );
    let status_response = client
        .get(format!("{base_url}/v1/namespaces/{target}"))
        .send()
        .await
        .expect("branch status request must complete");
    let status_code = status_response.status();
    let status_body: Value = status_response
        .json()
        .await
        .expect("branch status response must decode");
    assert_eq!(status_code, StatusCode::OK, "{status_body}");
    assert_eq!(
        status_body["branch"]["materialized"],
        json!(false),
        "a target-local section root must not hide its inherited foreign source"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn full_clone_rebinds_typed_sources_and_input_refs_to_target() {
    require_minio();
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(None).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("typed-clone-source");
    let target = harness.artifact_origin_namespace("typed-clone-target");
    create_late_namespace(
        &client,
        &base_url,
        &source,
        &["text", "image", "image_text"],
        true,
    )
    .await;
    append_units(
        &client,
        &base_url,
        &source,
        json!({
            "upserts": [{
                "id": "cloned-image-text",
                "input": {
                    "type": "image_text",
                    "image_base64": "CgsMDQ==",
                    "media_type": "image/png",
                    "width": 2,
                    "height": 2,
                    "text": "retained clone caption"
                },
                "attributes": { "title": "cloneterm retained" }
            }]
        }),
    )
    .await;

    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist");
    let response = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": source_manifest.version().to_string()
        }))
        .send()
        .await
        .expect("typed clone request must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("typed clone response must decode");
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["namespace"]["index_kind"], "late_interaction_fde");
    assert_eq!(
        body["namespace"]["late_interaction"]["accepted_modalities"],
        json!(["text", "image", "image_text"])
    );

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest read must succeed")
        .expect("target manifest must exist");
    assert_eq!(target_manifest.input_fragments.len(), 1);
    assert!(
        target_manifest.input_fragments[0].artifact_origin.is_none(),
        "cloned input WAL must be target-local"
    );
    let section_ref = target_manifest
        .late_state
        .as_ref()
        .expect("cloned image namespace must publish a target section");
    assert!(section_ref.key.starts_with(&format!("{target}/")));
    assert!(section_ref.artifact_origin.is_none());
    let target_section = target_manifest
        .load_late_state(&harness.store)
        .await
        .expect("target section read must succeed")
        .expect("target section must exist");
    assert!(
        target_manifest
            .visible_refs_are_local_with_late_state(Some(&target_section))
            .expect("target locality projection must succeed"),
        "the full clone must own its section and every section-resident source"
    );
    assert_eq!(target_section.source_inventory.len(), 1);
    let target_source = &target_section.source_inventory[0];
    assert!(target_source.key.starts_with(&format!("{target}/")));
    assert!(target_source.artifact_origin.is_none());

    let target_metadata = NamespaceManager::new(harness.store.clone())
        .get(&target)
        .await
        .expect("target metadata must load");
    let target_origin = ArtifactOrigin {
        namespace: NamespaceId::new(target.clone()).expect("target namespace must be valid"),
        incarnation: target_metadata
            .incarnation_id
            .expect("target must have an incarnation"),
    };
    assert_eq!(
        target_section
            .read_source_checked(&harness.store, target_source, &target_origin)
            .await
            .expect("cloned source must pass checked target-local read")
            .as_ref(),
        &[10, 11, 12, 13]
    );

    let target_fragment = WalReader::new(harness.store.clone())
        .read_input_fragment(&target, &target_manifest.input_fragments[0].id)
        .await
        .expect("cloned input WAL must decode under the target");
    let image_key = match &target_fragment.upserts[0].input {
        EncoderInputRef::ImageText { image, .. } => image.key.as_str(),
        other => panic!("expected cloned image-text input, got {other:?}"),
    };
    assert_eq!(image_key, target_source.key);
    assert_eq!(
        bm25_ids(&client, &base_url, &target, "cloneterm").await,
        BTreeSet::from(["cloned-image-text".to_string()]),
        "the cloned input fragment must remain visible to typed FTS replay"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn full_clone_rejects_input_wal_with_mismatched_embedded_id() {
    require_minio();
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(None).await;
    let client = client_with_bearer(&admin_bearer);
    let source = harness.artifact_origin_namespace("typed-clone-corrupt-source");
    let target = harness.artifact_origin_namespace("typed-clone-corrupt-target");
    create_late_namespace(&client, &base_url, &source, &["text"], false).await;
    append_units(
        &client,
        &base_url,
        &source,
        json!({
            "upserts": [{
                "id": "text",
                "input": { "type": "text", "text": "retained text" }
            }]
        }),
    )
    .await;

    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest read must succeed")
        .expect("source manifest must exist");
    let reference = &source_manifest.input_fragments[0];
    let source_key =
        zeppelin::wal::EncoderInputWalFragment::object_store_key(&source, &reference.id);
    let mut corrupted = WalReader::new(harness.store.clone())
        .read_input_fragment(&source, &reference.id)
        .await
        .expect("source input WAL must initially be valid");
    corrupted.id = ulid::Ulid::new();
    harness
        .store
        .put(
            &source_key,
            corrupted
                .to_bytes()
                .expect("mismatched-ID input WAL must serialize"),
        )
        .await
        .expect("test corruption overwrite must succeed");

    let response = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": source_manifest.version().to_string()
        }))
        .send()
        .await
        .expect("corrupt clone request must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("corrupt clone response must decode");
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

    harness.cleanup().await;
}

async fn assert_api_error(
    client: &reqwest::Client,
    url: String,
    body: Value,
    expected_status: StatusCode,
    expected_code: &str,
) {
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("rejected request must complete");
    let status = response.status();
    let body: Value = response.json().await.expect("error response must decode");
    assert_eq!(status, expected_status, "{body}");
    assert_eq!(body["code"], expected_code, "{body}");
}

#[tokio::test]
async fn typed_http_validation_is_fail_loud() {
    require_minio();
    let mut config = Config::default();
    config.server.max_retrieval_text_bytes = 8;
    config.server.max_retrieval_image_bytes = 4;
    config.server.max_retrieval_image_width = 10;
    config.server.max_retrieval_image_height = 10;
    config.server.max_retrieval_units_per_request = 2;
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let text_namespace = harness.artifact_origin_namespace("typed-validation-text");
    let image_namespace = harness.artifact_origin_namespace("typed-validation-image");
    let dense_namespace = harness.artifact_origin_namespace("typed-validation-dense");
    create_late_namespace(&client, &base_url, &text_namespace, &["text"], false).await;
    create_late_namespace(&client, &base_url, &image_namespace, &["image"], false).await;
    let dense = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": dense_namespace,
            "dimensions": 4
        }))
        .send()
        .await
        .expect("dense namespace create must complete");
    assert_eq!(dense.status(), StatusCode::CREATED);

    let text_url = format!("{base_url}/v1/namespaces/{text_namespace}/retrieval-units");
    let image_url = format!("{base_url}/v1/namespaces/{image_namespace}/retrieval-units");
    assert_api_error(
        &client,
        text_url.clone(),
        json!({}),
        StatusCode::BAD_REQUEST,
        "RETRIEVAL_UNIT_EMPTY",
    )
    .await;
    assert_api_error(
        &client,
        text_url.clone(),
        json!({ "deletes": ["one", "two", "three"] }),
        StatusCode::PAYLOAD_TOO_LARGE,
        "RETRIEVAL_UNIT_TOO_LARGE",
    )
    .await;
    assert_api_error(
        &client,
        text_url.clone(),
        json!({
            "upserts": [{
                "id": "too-long",
                "input": { "type": "text", "text": "123456789" }
            }]
        }),
        StatusCode::PAYLOAD_TOO_LARGE,
        "RETRIEVAL_UNIT_TOO_LARGE",
    )
    .await;
    assert_api_error(
        &client,
        text_url,
        json!({
            "upserts": [{
                "id": "wrong-modality",
                "input": {
                    "type": "image",
                    "image_base64": "AQ==",
                    "media_type": "image/jpeg",
                    "width": 1,
                    "height": 1
                }
            }]
        }),
        StatusCode::BAD_REQUEST,
        "UNSUPPORTED_INPUT_MODALITY",
    )
    .await;
    assert_api_error(
        &client,
        image_url.clone(),
        json!({
            "upserts": [{
                "id": "wrong-media",
                "input": {
                    "type": "image",
                    "image_base64": "AQ==",
                    "media_type": "image/gif",
                    "width": 1,
                    "height": 1
                }
            }]
        }),
        StatusCode::BAD_REQUEST,
        "UNSUPPORTED_IMAGE_MEDIA_TYPE",
    )
    .await;
    assert_api_error(
        &client,
        image_url.clone(),
        json!({
            "upserts": [{
                "id": "wide",
                "input": {
                    "type": "image",
                    "image_base64": "AQ==",
                    "media_type": "image/jpeg",
                    "width": 11,
                    "height": 1
                }
            }]
        }),
        StatusCode::BAD_REQUEST,
        "IMAGE_DIMENSIONS_EXCEEDED",
    )
    .await;
    assert_api_error(
        &client,
        image_url,
        json!({
            "upserts": [{
                "id": "large-image",
                "input": {
                    "type": "image",
                    "image_base64": "AQIDBAU=",
                    "media_type": "image/jpeg",
                    "width": 1,
                    "height": 1
                }
            }]
        }),
        StatusCode::PAYLOAD_TOO_LARGE,
        "RETRIEVAL_UNIT_TOO_LARGE",
    )
    .await;
    assert_api_error(
        &client,
        format!("{base_url}/v1/namespaces/{text_namespace}/vectors"),
        json!({
            "vectors": [{
                "id": "dense",
                "values": [1.0, 0.0, 0.0, 0.0]
            }]
        }),
        StatusCode::BAD_REQUEST,
        "UNSUPPORTED_INPUT_MODALITY",
    )
    .await;
    assert_api_error(
        &client,
        format!("{base_url}/v1/namespaces/{dense_namespace}/retrieval-units"),
        json!({
            "upserts": [{
                "id": "typed",
                "input": { "type": "text", "text": "short" }
            }]
        }),
        StatusCode::BAD_REQUEST,
        "UNSUPPORTED_INPUT_MODALITY",
    )
    .await;

    harness.cleanup().await;
}

#[tokio::test]
async fn typed_write_depth_matches_dense_and_adds_two_image_puts() {
    require_minio();
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let dense_namespace = harness.artifact_origin_namespace("typed-depth-dense");
    let late_namespace = harness.artifact_origin_namespace("typed-depth-late");
    NamespaceManager::new(store.clone())
        .create(&dense_namespace, 4, DistanceMetric::Cosine)
        .await
        .expect("dense namespace create must succeed");
    create_direct_late_namespace(&store, &late_namespace).await;

    counter.reset();
    WalWriter::new(store.clone())
        .append(
            &dense_namespace,
            vec![VectorEntry {
                id: "dense".to_string(),
                values: vec![1.0, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            Vec::new(),
        )
        .await
        .expect("dense append must succeed");
    let dense_puts = counter.total_observed_puts();

    counter.reset();
    WalWriter::new(store.clone())
        .append_retrieval_units(
            &late_namespace,
            vec![text_record("text", "text-only")],
            Vec::new(),
            Vec::new(),
        )
        .await
        .expect("text-only typed append must succeed");
    let text_puts = counter.total_observed_puts();

    counter.reset();
    let (record, source, bytes) =
        image_record(&late_namespace, "image", Bytes::from_static(b"image"));
    WalWriter::new(store)
        .append_retrieval_units(
            &late_namespace,
            vec![record],
            Vec::new(),
            vec![(source, bytes)],
        )
        .await
        .expect("image typed append must succeed");
    let image_puts = counter.total_observed_puts();

    eprintln!("typed-ingest write depth: dense={dense_puts} text={text_puts} image={image_puts}");
    assert_eq!(
        text_puts, dense_puts,
        "text-only typed ingest must have dense-write PUT depth"
    );
    assert_eq!(
        image_puts,
        text_puts + 2,
        "one image must add exactly one source PUT and one section PUT"
    );

    harness.cleanup().await;
}
