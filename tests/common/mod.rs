#[allow(dead_code)]
pub mod assertions;
#[allow(dead_code)]
pub mod counting;
#[allow(dead_code)]
pub mod fault_injection;
#[allow(dead_code)]
pub mod harness;
#[allow(dead_code)]
pub mod server;
#[allow(dead_code)]
pub mod vectors;

/// Publishes the authoritative metadata half of an active test namespace.
///
/// Raw-manifest fixtures use this when they need to bypass
/// [`zeppelin::namespace::NamespaceManager`] while still presenting the same
/// state tuple that a production compaction transaction requires.
#[allow(dead_code)]
pub async fn write_active_namespace_metadata(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
) {
    write_active_namespace_metadata_with_fts(
        store,
        namespace,
        dimensions,
        distance_metric,
        std::collections::HashMap::new(),
    )
    .await;
}

/// Publishes active test metadata with an explicit full-text configuration.
#[allow(dead_code)]
pub async fn write_active_namespace_metadata_with_fts(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
    full_text_search: std::collections::HashMap<String, zeppelin::fts::FtsFieldConfig>,
) {
    let now = chrono::Utc::now();
    let metadata = zeppelin::namespace::manager::NamespaceMetadata {
        name: namespace.to_string(),
        dimensions,
        distance_metric,
        index_type: zeppelin::types::IndexType::IvfFlat,
        vector_count: 0,
        created_at: now,
        updated_at: now,
        state: zeppelin::namespace::manager::NamespaceState::Active,
        destruction_record_key: None,
        deletion_intent: None,
        full_text_search,
        index_config: None,
        compaction_health: zeppelin::namespace::manager::CompactionHealth::default(),
        creation_kind: zeppelin::namespace::branching::NamespaceCreationKind::Root,
        branch_identity: None,
        branch_prepare: None,
        branch_activation: None,
        // A namespace that owns objects must carry a lifetime identity.
        // `local_origin` refuses to resolve an artifact origin without one, so
        // metadata written with `None` yields a namespace whose manifests
        // cannot be published — `compact`, `WalWriter::append` and
        // `Manifest::write` all fail with "local artifact origin requires an
        // incarnation binding".
        incarnation_id: Some(zeppelin::namespace::NamespaceIncarnationId::from_uuid(
            uuid::Uuid::new_v4(),
        )),
    };
    // The incarnation rides in S3 user metadata, not the JSON body, so this
    // must go through a user-metadata write exactly as the create path does.
    let user_metadata = metadata.user_metadata();
    match store
        .put_if_not_exists_with_user_metadata(
            &zeppelin::namespace::manager::NamespaceMetadata::s3_key(namespace),
            metadata.to_bytes().unwrap(),
            namespace,
            &user_metadata,
        )
        .await
    {
        Ok(()) => {}
        // Seeding the same fixture namespace twice is a no-op, matching the
        // unconditional `put` this replaced.
        Err(zeppelin::error::ZeppelinError::NamespaceAlreadyExists { .. }) => {}
        Err(error) => panic!("fixture namespace metadata write failed: {error}"),
    }
}

#[allow(dead_code)]
pub fn default_gc_upload_window() -> std::time::Duration {
    std::time::Duration::from_secs(
        zeppelin::config::GcConfig::default().compaction_upload_window_secs,
    )
}
