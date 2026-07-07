use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::{info, instrument};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, IndexType};

const DEFAULT_NAMESPACE_REGISTRY_TTL: Duration = Duration::from_secs(5);
/// Consecutive compaction failures before a namespace is reported degraded.
pub const COMPACTION_DEGRADED_FAILURE_THRESHOLD: u32 = 5;

/// Lifecycle state stored in `meta.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceState {
    /// Namespace accepts reads and writes.
    #[default]
    Active,
    /// Namespace is being deleted; clients may observe status but not use it.
    Deleting,
}

impl NamespaceState {
    /// Stable lowercase API representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NamespaceState::Active => "active",
            NamespaceState::Deleting => "deleting",
        }
    }
}

/// Per-namespace indexing parameters persisted in `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceIndexConfig {
    /// Number of IVF centroids/clusters to train for future compactions.
    pub nlist: usize,
    /// Vector quantization mode.
    pub quantization: QuantizationType,
    /// Number of product-quantization subquantizers.
    pub pq_m: usize,
    /// Whether future compactions build a hierarchical index.
    pub hierarchical: bool,
    /// Whether future compactions build FTS segment indexes.
    pub fts_index: bool,
    /// Whether future compactions build bitmap segment indexes.
    pub bitmap_index: bool,
}

impl NamespaceIndexConfig {
    /// Build a persisted namespace config from the server default indexing config.
    #[must_use]
    pub fn from_indexing_config(config: &IndexingConfig) -> Self {
        Self {
            nlist: config.default_num_centroids,
            quantization: config.quantization,
            pq_m: config.pq_m,
            hierarchical: config.hierarchical,
            fts_index: config.fts_index,
            bitmap_index: config.bitmap_index,
        }
    }

    /// Overlay this namespace config onto a full server indexing config.
    #[must_use]
    pub fn apply_to_indexing_config(&self, base: &IndexingConfig) -> IndexingConfig {
        let mut config = base.clone();
        config.default_num_centroids = self.nlist;
        config.quantization = self.quantization;
        config.pq_m = self.pq_m;
        config.hierarchical = self.hierarchical;
        config.fts_index = self.fts_index;
        config.bitmap_index = self.bitmap_index;
        config
    }

    /// Validate parameters that depend on namespace shape.
    pub fn validate(&self, dimensions: usize) -> Result<()> {
        if self.nlist == 0 {
            return Err(ZeppelinError::Validation(
                "index_config.nlist must be >= 1".into(),
            ));
        }
        if self.pq_m == 0 {
            return Err(ZeppelinError::Validation(
                "index_config.pq_m must be >= 1".into(),
            ));
        }
        if self.quantization == QuantizationType::Product && dimensions % self.pq_m != 0 {
            return Err(ZeppelinError::Validation(format!(
                "index_config.pq_m ({}) must divide dimensions ({}) when quantization=product",
                self.pq_m, dimensions
            )));
        }
        Ok(())
    }
}

/// Stable compaction status stored in namespace metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStatus {
    /// No compaction outcome has been recorded yet.
    #[default]
    Never,
    /// Last compaction completed successfully.
    Success,
    /// Last compaction failed.
    Failure,
}

impl CompactionStatus {
    /// Stable API string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// Namespace compaction/index health persisted in `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionHealth {
    /// Last compaction completion/failure time.
    #[serde(default)]
    pub last_compaction_at: Option<DateTime<Utc>>,
    /// Last recorded compaction status.
    #[serde(default)]
    pub last_compaction_status: CompactionStatus,
    /// Last failure message, cleared on success.
    #[serde(default)]
    pub last_compaction_error: Option<String>,
    /// Consecutive failure count since the last success.
    #[serde(default)]
    pub consecutive_failures: u32,
}

/// Metadata for a namespace, stored as meta.json on S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    /// Unique namespace identifier.
    pub name: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Distance metric used for queries.
    pub distance_metric: DistanceMetric,
    /// Index algorithm type.
    pub index_type: IndexType,
    /// Total number of vectors (approximate).
    pub vector_count: u64,
    /// Timestamp when the namespace was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the last metadata update.
    pub updated_at: DateTime<Utc>,
    /// Lifecycle state for recoverable namespace deletion.
    #[serde(default)]
    pub state: NamespaceState,
    /// Per-field full-text search configuration.
    /// Empty map means FTS is not enabled for this namespace.
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    /// Per-namespace indexing parameters. `None` means legacy metadata and is
    /// resolved from the current server config by callers.
    #[serde(default)]
    pub index_config: Option<NamespaceIndexConfig>,
    /// Compaction/index health surfaced through namespace reads.
    #[serde(default)]
    pub compaction_health: CompactionHealth,
}

impl NamespaceMetadata {
    /// Return the S3 key for this namespace's metadata file.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/meta.json")
    }

    /// Serialize metadata to pretty-printed JSON bytes.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Deserialize metadata from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

#[derive(Debug, Clone)]
struct RegistryEntry {
    meta: NamespaceMetadata,
    fetched_at: Instant,
}

/// Result of an idempotent namespace create request.
#[derive(Debug, Clone)]
pub enum CreateNamespaceOutcome {
    /// The namespace did not exist and was created by this request.
    Created(NamespaceMetadata),
    /// The namespace already existed with the same immutable configuration.
    Existing(NamespaceMetadata),
}

/// Manages namespace CRUD operations with an in-memory cache backed by S3.
pub struct NamespaceManager {
    store: ZeppelinStore,
    /// In-memory registry for fast lookups.
    registry: DashMap<String, RegistryEntry>,
    registry_ttl: Duration,
}

impl NamespaceManager {
    /// Create a new namespace manager backed by the given store.
    pub fn new(store: ZeppelinStore) -> Self {
        Self::new_with_registry_ttl(store, DEFAULT_NAMESPACE_REGISTRY_TTL)
    }

    /// Create a namespace manager with an explicit registry TTL.
    #[must_use]
    pub fn new_with_registry_ttl(store: ZeppelinStore, registry_ttl: Duration) -> Self {
        Self {
            store,
            registry: DashMap::new(),
            registry_ttl,
        }
    }

    /// Create a new namespace.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn create(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
    ) -> Result<NamespaceMetadata> {
        self.create_with_fts(
            name,
            dimensions,
            distance_metric,
            std::collections::HashMap::new(),
        )
        .await
    }

    /// Create a new namespace with optional FTS field configuration.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_with_fts(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    ) -> Result<NamespaceMetadata> {
        self.create_with_fts_and_index_config(
            name,
            dimensions,
            distance_metric,
            full_text_search,
            None,
        )
        .await
    }

    /// Create a new namespace with optional FTS and per-namespace index config.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_with_fts_and_index_config(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
        index_config: Option<NamespaceIndexConfig>,
    ) -> Result<NamespaceMetadata> {
        // Validate namespace name
        if !is_valid_namespace_name(name) {
            return Err(ZeppelinError::Validation(format!(
                "invalid namespace name '{}': must be 1-255 chars, start with alphanumeric, \
                 and contain only alphanumeric, dash, underscore, or dot characters",
                name,
            )));
        }
        if dimensions == 0 {
            return Err(ZeppelinError::Validation(
                "dimensions must be > 0".to_string(),
            ));
        }
        for (field, config) in &full_text_search {
            config.validate(&format!("full_text_search.{field}"))?;
        }
        if let Some(index_config) = index_config.as_ref() {
            index_config.validate(dimensions)?;
        }

        // Atomic create: write meta.json only if it doesn't already exist.
        // Uses S3 `If-None-Match: *` (PutMode::Create) to prevent TOCTOU races
        // where two concurrent creators both pass an exists() check and silently
        // overwrite each other's configuration.
        let key = NamespaceMetadata::s3_key(name);

        let now = Utc::now();
        let meta = NamespaceMetadata {
            name: name.to_string(),
            dimensions,
            distance_metric,
            index_type: IndexType::default(),
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Active,
            full_text_search,
            index_config,
            compaction_health: CompactionHealth::default(),
        };

        // Atomic write — returns NamespaceAlreadyExists if meta.json exists
        match self
            .store
            .put_if_not_exists(&key, meta.to_bytes()?, name)
            .await
        {
            Ok(()) => {}
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                if let Ok(existing) = self.read_metadata_from_s3(name).await {
                    if existing.state == NamespaceState::Deleting {
                        return Err(ZeppelinError::NamespaceDeleting {
                            namespace: name.to_string(),
                        });
                    }
                }
                return Err(ZeppelinError::NamespaceAlreadyExists {
                    namespace: name.to_string(),
                });
            }
            Err(e) => return Err(e),
        }

        // Also initialize an empty manifest
        let manifest = crate::wal::Manifest::new();
        manifest.write(&self.store, name).await?;

        // Add to registry
        self.insert_registry(meta.clone());

        info!(namespace = name, dimensions, %distance_metric, "created namespace");
        Ok(meta)
    }

    /// Idempotently create a namespace by client-specified name.
    ///
    /// Same name plus identical immutable configuration returns the existing
    /// S3 metadata; same name plus different configuration remains a conflict.
    /// This keeps create-by-name useful for multi-process clients without
    /// silently changing an existing namespace's shape.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_idempotent_with_fts(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    ) -> Result<CreateNamespaceOutcome> {
        self.create_idempotent_with_fts_and_index_config(
            name,
            dimensions,
            distance_metric,
            full_text_search,
            None,
        )
        .await
    }

    /// Idempotently create a namespace by client name, including index config.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_idempotent_with_fts_and_index_config(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
        index_config: Option<NamespaceIndexConfig>,
    ) -> Result<CreateNamespaceOutcome> {
        if let Some(index_config) = index_config.as_ref() {
            index_config.validate(dimensions)?;
        }
        match self
            .create_with_fts_and_index_config(
                name,
                dimensions,
                distance_metric,
                full_text_search.clone(),
                index_config.clone(),
            )
            .await
        {
            Ok(meta) => Ok(CreateNamespaceOutcome::Created(meta)),
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                let existing = self.read_metadata_from_s3(name).await?;
                if existing.state == NamespaceState::Deleting {
                    return Err(ZeppelinError::NamespaceDeleting {
                        namespace: name.to_string(),
                    });
                }
                if namespace_config_matches(
                    &existing,
                    dimensions,
                    distance_metric,
                    &full_text_search,
                    &index_config,
                )? {
                    return Ok(CreateNamespaceOutcome::Existing(existing));
                }
                Err(ZeppelinError::NamespaceAlreadyExists {
                    namespace: name.to_string(),
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Get namespace metadata.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = self.get_including_deleting(name).await?;
        self.ensure_active(meta)
    }

    /// Get namespace metadata even if it is in the `deleting` state.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get_including_deleting(&self, name: &str) -> Result<NamespaceMetadata> {
        if let Some(meta) = self.fresh_registry_meta(name) {
            return Ok(meta);
        }

        self.read_metadata_from_s3(name).await
    }

    async fn read_metadata_from_s3(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get(&key).await {
            Ok(data) => {
                let meta = NamespaceMetadata::from_bytes(&data)?;
                self.insert_registry(meta.clone());
                Ok(meta)
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    async fn read_metadata_versioned(
        &self,
        name: &str,
    ) -> Result<(NamespaceMetadata, Option<String>)> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get_with_meta(&key).await {
            Ok((data, etag)) => {
                let meta = NamespaceMetadata::from_bytes(&data)?;
                self.insert_registry(meta.clone());
                Ok((meta, etag))
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    fn fresh_registry_meta(&self, name: &str) -> Option<NamespaceMetadata> {
        self.registry.get(name).and_then(|entry| {
            if entry.fetched_at.elapsed() < self.registry_ttl {
                Some(entry.meta.clone())
            } else {
                None
            }
        })
    }

    fn insert_registry(&self, meta: NamespaceMetadata) {
        self.registry.insert(
            meta.name.clone(),
            RegistryEntry {
                meta,
                fetched_at: Instant::now(),
            },
        );
    }

    fn ensure_active(&self, meta: NamespaceMetadata) -> Result<NamespaceMetadata> {
        if meta.state == NamespaceState::Deleting {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: meta.name,
            });
        }
        Ok(meta)
    }

    /// List all namespaces, optionally filtered by prefix.
    #[instrument(skip(self))]
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<NamespaceMetadata>> {
        // List immediate namespace prefixes that have meta.json. This must use
        // delimiter listing: a recursive bucket walk would visit every WAL,
        // segment, cluster, and nested meta.json object under every namespace.
        //
        // Namespace names are top-level path components, so a test prefix such
        // as `test-<uuid>` is a partial component, not a directory. Some object
        // store backends support delimiter listing with that partial prefix,
        // while the memory backend does not. List top-level namespace prefixes
        // and apply the namespace-name prefix before reading metadata.
        let mut namespace_prefixes = self.store.list_common_prefixes("").await?;
        if let Some(prefix) = prefix {
            namespace_prefixes.retain(|namespace_prefix| {
                namespace_prefix.trim_end_matches('/').starts_with(prefix)
            });
        }

        let mut namespaces = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut found = std::collections::HashSet::new();

        for prefix in &namespace_prefixes {
            let ns_name = prefix.trim_end_matches('/');
            if seen.insert(ns_name.to_string()) {
                match self.read_metadata_from_s3(ns_name).await {
                    Ok(meta) => {
                        found.insert(meta.name.clone());
                        namespaces.push(meta);
                    }
                    Err(ZeppelinError::NamespaceNotFound { .. }) => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        self.registry.retain(|name, _| match prefix {
            Some(scope) if name.starts_with(scope) => found.contains(name),
            Some(_) => true,
            None => found.contains(name),
        });

        Ok(namespaces)
    }

    /// Delete a namespace and all its data.
    ///
    /// Synchronous direct delete: flips `meta.json` to `deleting`, purges all
    /// namespace data while keeping the tombstone, then deletes `meta.json`
    /// last. The HTTP handler uses the same start/finish primitives but runs
    /// the purge in a background task and returns 202.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn delete(&self, name: &str) -> Result<()> {
        self.start_delete(name).await?;
        let outcome = self.finish_delete(name, Duration::MAX).await?;
        if !outcome.complete {
            return Err(ZeppelinError::NamespaceDeleteIncomplete {
                namespace: name.to_string(),
                remaining_keys: 1,
            });
        }

        Ok(())
    }

    /// Mark a namespace as deleting and remove fixed-cost roots.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn start_delete(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = self.mark_deleting(name).await?;

        self.registry.remove(name);
        let manifest_key = crate::wal::Manifest::s3_key(name);
        match self.store.delete(&manifest_key).await {
            Ok(()) | Err(ZeppelinError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }
        info!(
            namespace = name,
            state = NamespaceState::Deleting.as_str(),
            "namespace marked deleting"
        );
        Ok(meta)
    }

    /// Resume or complete deletion of a namespace already marked `deleting`.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn finish_delete(
        &self,
        name: &str,
        budget: Duration,
    ) -> Result<crate::storage::DeletePrefixOutcome> {
        let meta_key = NamespaceMetadata::s3_key(name);
        let meta = self.read_metadata_from_s3(name).await?;
        if meta.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} is not marked deleting"
            )));
        }

        let prefix = format!("{name}/");
        let outcome = self
            .store
            .delete_prefix_paged(&prefix, Some(&meta_key), budget)
            .await?;

        if !outcome.complete {
            return Ok(outcome);
        }

        let remaining = self.store.list_prefix(&prefix).await?;
        let non_meta_remaining = remaining.iter().filter(|key| *key != &meta_key).count();
        if non_meta_remaining != 0 {
            return Err(ZeppelinError::NamespaceDeleteIncomplete {
                namespace: name.to_string(),
                remaining_keys: non_meta_remaining,
            });
        }

        match self.store.delete(&meta_key).await {
            Ok(()) | Err(ZeppelinError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }
        self.registry.remove(name);

        info!(
            namespace = name,
            objects_deleted = outcome.deleted + 1,
            "deleted namespace"
        );
        Ok(outcome)
    }

    /// Persist a new desired index config for future compactions.
    #[instrument(skip(self, index_config), fields(namespace = name))]
    pub async fn update_index_config(
        &self,
        name: &str,
        index_config: NamespaceIndexConfig,
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let meta_name = meta.name.clone();
            self.ensure_active(meta.clone())?;
            index_config.validate(meta.dimensions)?;

            meta.index_config = Some(index_config.clone());
            meta.updated_at = Utc::now();
            let etag = etag.unwrap_or_default();
            match self
                .store
                .put_if_match(&key, meta.to_bytes()?, &etag, &meta_name)
                .await
            {
                Ok(()) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Record a successful compaction outcome in namespace health metadata.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_success(&self, name: &str) -> Result<NamespaceMetadata> {
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(Utc::now());
            health.last_compaction_status = CompactionStatus::Success;
            health.last_compaction_error = None;
            health.consecutive_failures = 0;
        })
        .await
    }

    /// Record a failed compaction outcome in namespace health metadata.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_failure(
        &self,
        name: &str,
        error: &ZeppelinError,
    ) -> Result<NamespaceMetadata> {
        let message = error.to_string();
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(Utc::now());
            health.last_compaction_status = CompactionStatus::Failure;
            health.last_compaction_error = Some(message.clone());
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        })
        .await
    }

    async fn update_compaction_health(
        &self,
        name: &str,
        update: impl Fn(&mut CompactionHealth),
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let meta_name = meta.name.clone();
            self.ensure_active(meta.clone())?;

            update(&mut meta.compaction_health);
            meta.updated_at = Utc::now();
            let degraded = meta.compaction_health.consecutive_failures
                >= COMPACTION_DEGRADED_FAILURE_THRESHOLD;
            let etag = etag.unwrap_or_default();
            match self
                .store
                .put_if_match(&key, meta.to_bytes()?, &etag, &meta_name)
                .await
            {
                Ok(()) => {
                    self.insert_registry(meta.clone());
                    crate::metrics::COMPACTION_NAMESPACE_DEGRADED
                        .with_label_values(&[name])
                        .set(i64::from(degraded));
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    async fn mark_deleting(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..2 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            if meta.state == NamespaceState::Deleting {
                return Ok(meta);
            }

            meta.state = NamespaceState::Deleting;
            meta.updated_at = Utc::now();
            let etag = etag.unwrap_or_default();
            match self
                .store
                .put_if_match(&key, meta.to_bytes()?, &etag, name)
                .await
            {
                Ok(()) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Scan S3 for existing namespaces and populate the registry.
    /// Used on startup to discover pre-existing data.
    #[instrument(skip(self))]
    pub async fn scan_and_register(&self) -> Result<usize> {
        let namespaces = self.list(None).await?;
        let count = namespaces.len();

        info!(namespaces = count, "scanned and registered namespaces");
        Ok(count)
    }

    /// Check if a namespace exists in the registry.
    pub fn exists_in_registry(&self, name: &str) -> bool {
        self.registry.contains_key(name)
    }

    /// Snapshot cached namespaces for background maintenance loops.
    #[must_use]
    pub fn cached_namespaces(&self, prefix: Option<&str>) -> Vec<NamespaceMetadata> {
        self.registry
            .iter()
            .filter_map(|entry| {
                let meta = &entry.value().meta;
                match prefix {
                    Some(prefix) if !meta.name.starts_with(prefix) => None,
                    _ => Some(meta.clone()),
                }
            })
            .collect()
    }
}

/// Validate a namespace name as both an S3 top-level key prefix and one URL
/// path segment. This deliberately matches the safe names produced by the
/// test helpers: `TestHarness::key()` may contain `/` for raw S3 keys, while
/// `api_ns()` produces slash-free namespace names suitable for HTTP paths.
#[must_use]
pub fn is_valid_namespace_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

fn namespace_config_matches(
    existing: &NamespaceMetadata,
    dimensions: usize,
    distance_metric: DistanceMetric,
    full_text_search: &std::collections::HashMap<String, FtsFieldConfig>,
    index_config: &Option<NamespaceIndexConfig>,
) -> Result<bool> {
    Ok(existing.dimensions == dimensions
        && existing.distance_metric == distance_metric
        && fts_config_value(&existing.full_text_search)? == fts_config_value(full_text_search)?
        && existing.index_config == *index_config)
}

fn fts_config_value(
    full_text_search: &std::collections::HashMap<String, FtsFieldConfig>,
) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(full_text_search)?)
}

#[cfg(test)]
mod tests {
    use super::is_valid_namespace_name;

    #[test]
    fn namespace_name_validator_accepts_s3_and_url_safe_names() {
        for name in ["tenant-a", "tenant_a", "tenant.a", "TenantA-123"] {
            assert!(
                is_valid_namespace_name(name),
                "expected namespace name to be valid: {name}"
            );
        }
    }

    #[test]
    fn namespace_name_validator_rejects_unsafe_names_at_creation_boundary() {
        let overlong = format!("a{}", "b".repeat(255));
        let invalid = [
            "",
            "tenant/a",
            "tenant a",
            "tenant%2Fa",
            "../tenant",
            "-tenant",
            "tenant?",
            "tenant#fragment",
            overlong.as_str(),
        ];

        for name in invalid {
            assert!(
                !is_valid_namespace_name(name),
                "expected namespace name to be invalid: {name:?}"
            );
        }
    }
}
