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
///
/// Mints a fresh lifetime identity. Fixtures that also publish a manifest want
/// [`seed_active_namespace_with_fts`] instead, so both halves agree.
#[allow(dead_code)]
pub async fn write_active_namespace_metadata_with_fts(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
    full_text_search: std::collections::HashMap<String, zeppelin::fts::FtsFieldConfig>,
) {
    write_active_namespace_metadata_for_incarnation(
        store,
        namespace,
        dimensions,
        distance_metric,
        full_text_search,
        uuid::Uuid::new_v4(),
    )
    .await;
}

/// Publishes active test metadata bound to a caller-chosen lifetime identity.
///
/// Callers that publish a manifest for the same namespace must pass the
/// incarnation they bound into the manifest: metadata and manifest are two
/// halves of one lifetime, and `NamespaceManager` treats a disagreement
/// between them as an integrity error rather than something to reconcile.
#[allow(dead_code)]
pub async fn write_active_namespace_metadata_for_incarnation(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
    full_text_search: std::collections::HashMap<String, zeppelin::fts::FtsFieldConfig>,
    incarnation: uuid::Uuid,
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
            incarnation,
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

/// Seeds an empty active namespace: metadata and initial manifest published
/// under one shared lifetime identity, which is returned.
///
/// This is the fixture equivalent of `NamespaceManager::create`. Prefer it over
/// hand-rolling `write_active_namespace_metadata` plus
/// `Manifest::new().write(..)` — that pair leaves the manifest *unbound*, and
/// every later artifact publication (`WalWriter::append`, `compact`) then dies
/// resolving its origin against a namespace with no incarnation.
#[allow(dead_code)]
pub async fn seed_active_namespace(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
) -> uuid::Uuid {
    seed_active_namespace_with_fts(
        store,
        namespace,
        dimensions,
        distance_metric,
        std::collections::HashMap::new(),
    )
    .await
}

/// Seeds an empty active namespace with an explicit full-text configuration.
#[allow(dead_code)]
pub async fn seed_active_namespace_with_fts(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
    full_text_search: std::collections::HashMap<String, zeppelin::fts::FtsFieldConfig>,
) -> uuid::Uuid {
    let incarnation = uuid::Uuid::new_v4();
    write_active_namespace_metadata_for_incarnation(
        store,
        namespace,
        dimensions,
        distance_metric,
        full_text_search,
        incarnation,
    )
    .await;
    publish_bound_manifest(
        store,
        namespace,
        zeppelin::wal::Manifest::new(),
        incarnation,
    )
    .await;
    incarnation
}

/// Seeds an active namespace whose first manifest already has contents.
///
/// Same contract as [`seed_active_namespace`] — one incarnation across both
/// halves — for fixtures that hand-build a manifest (pre-existing segments,
/// fragments, fencing tokens) instead of starting empty.
#[allow(dead_code)]
pub async fn seed_active_namespace_with_manifest(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    dimensions: usize,
    distance_metric: zeppelin::types::DistanceMetric,
    manifest: zeppelin::wal::Manifest,
) -> uuid::Uuid {
    let incarnation = uuid::Uuid::new_v4();
    write_active_namespace_metadata_for_incarnation(
        store,
        namespace,
        dimensions,
        distance_metric,
        std::collections::HashMap::new(),
        incarnation,
    )
    .await;
    publish_bound_manifest(store, namespace, manifest, incarnation).await;
    incarnation
}

/// Publishes a bound but otherwise empty manifest, minting the lifetime
/// identity it carries.
///
/// For fixtures that deliberately hold no namespace metadata — object-count and
/// GET-count assertions notice the extra metadata write, so those namespaces
/// stay manifest-only.
#[allow(dead_code)]
pub async fn seed_bound_manifest(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
) -> uuid::Uuid {
    let incarnation = uuid::Uuid::new_v4();
    publish_bound_manifest(
        store,
        namespace,
        zeppelin::wal::Manifest::new(),
        incarnation,
    )
    .await;
    incarnation
}

/// Binds `manifest` to `incarnation` and publishes it.
///
/// Fixtures that build a manifest with contents (segments, fragments, fencing
/// tokens) go through here so the binding is never forgotten.
#[allow(dead_code)]
pub async fn publish_bound_manifest(
    store: &zeppelin::storage::ZeppelinStore,
    namespace: &str,
    mut manifest: zeppelin::wal::Manifest,
    incarnation: uuid::Uuid,
) -> zeppelin::wal::Manifest {
    manifest
        .bind_namespace_incarnation(incarnation)
        .expect("fixture manifest must accept its namespace incarnation");
    manifest
        .write(store, namespace)
        .await
        .expect("fixture manifest write failed");
    manifest
}

#[allow(dead_code)]
pub fn default_gc_upload_window() -> std::time::Duration {
    std::time::Duration::from_secs(
        zeppelin::config::GcConfig::default().compaction_upload_window_secs,
    )
}
