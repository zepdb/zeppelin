use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::{info, instrument};

use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, IndexType};

const DEFAULT_NAMESPACE_REGISTRY_TTL: Duration = Duration::from_secs(5);

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

/// Validate a namespace name: 1-255 chars, starts with alphanumeric,
/// only contains `[a-zA-Z0-9._-]`.
fn is_valid_namespace_name(name: &str) -> bool {
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
