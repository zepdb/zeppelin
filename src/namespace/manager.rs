use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::{Result, ZeppelinError};
use crate::fts::types::FtsFieldConfig;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, IndexType};

/// Metadata for a namespace, stored as meta.json on S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    pub name: String,
    pub dimensions: usize,
    pub distance_metric: DistanceMetric,
    pub index_type: IndexType,
    pub vector_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Per-field full-text search configuration.
    /// Empty map means FTS is not enabled for this namespace.
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
}

impl NamespaceMetadata {
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/meta.json")
    }

    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// Manages namespace CRUD operations with an in-memory cache backed by S3.
pub struct NamespaceManager {
    store: ZeppelinStore,
    /// In-memory registry for fast lookups.
    registry: DashMap<String, NamespaceMetadata>,
}

impl NamespaceManager {
    pub fn new(store: ZeppelinStore) -> Self {
        Self {
            store,
            registry: DashMap::new(),
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
            full_text_search,
        };

        // Atomic write — returns NamespaceAlreadyExists if meta.json exists
        self.store
            .put_if_not_exists(&key, meta.to_bytes()?, name)
            .await?;

        // Also initialize an empty manifest
        let manifest = crate::wal::Manifest::new();
        manifest.write(&self.store, name).await?;

        // Add to registry
        self.registry.insert(name.to_string(), meta.clone());

        info!(namespace = name, dimensions, %distance_metric, "created namespace");
        Ok(meta)
    }

    /// Get namespace metadata.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get(&self, name: &str) -> Result<NamespaceMetadata> {
        // Check registry first
        if let Some(meta) = self.registry.get(name) {
            return Ok(meta.clone());
        }

        // Fall back to S3
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get(&key).await {
            Ok(data) => {
                let meta = NamespaceMetadata::from_bytes(&data)?;
                self.registry.insert(name.to_string(), meta.clone());
                Ok(meta)
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// List all namespaces, optionally filtered by prefix.
    #[instrument(skip(self))]
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<NamespaceMetadata>> {
        // List all top-level prefixes that have meta.json
        let list_prefix = prefix.unwrap_or("");
        let keys = self.store.list_prefix(list_prefix).await?;

        let mut namespaces = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for key in &keys {
            if key.ends_with("/meta.json") {
                let ns_name = key.trim_end_matches("/meta.json");
                if seen.insert(ns_name.to_string()) {
                    match self.get(ns_name).await {
                        Ok(meta) => namespaces.push(meta),
                        Err(ZeppelinError::NamespaceNotFound { .. }) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        Ok(namespaces)
    }

    /// Delete a namespace and all its data.
    ///
    /// Deletes manifest first so concurrent queries see `None` manifest → empty
    /// results, rather than a manifest referencing already-deleted fragments.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn delete(&self, name: &str) -> Result<()> {
        // Verify it exists
        let key = NamespaceMetadata::s3_key(name);
        if !self.store.exists(&key).await? {
            return Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            });
        }

        // 1. Delete manifest first — queries will see None and return empty results
        let manifest_key = crate::wal::Manifest::s3_key(name);
        if let Err(e) = self.store.delete(&manifest_key).await {
            // Manifest may not exist (e.g., never initialized), log and continue
            tracing::warn!(key = %manifest_key, error = %e, "failed to delete manifest during namespace deletion");
        }

        // 2. Delete meta.json
        if let Err(e) = self.store.delete(&key).await {
            tracing::warn!(key = %key, error = %e, "failed to delete meta.json during namespace deletion");
        }

        // 3. Delete remaining keys (WAL fragments, segments, etc.)
        let prefix = format!("{name}/");
        let deleted = self.store.delete_prefix(&prefix).await?;

        // 4. Remove from registry
        self.registry.remove(name);

        info!(
            namespace = name,
            objects_deleted = deleted + 2, // +2 for manifest and meta
            "deleted namespace"
        );
        Ok(())
    }

    /// Update the vector count for a namespace.
    pub async fn update_vector_count(&self, name: &str, count: u64) -> Result<()> {
        let mut meta = self.get(name).await?;
        meta.vector_count = count;
        meta.updated_at = Utc::now();

        let key = NamespaceMetadata::s3_key(name);
        self.store.put(&key, meta.to_bytes()?).await?;
        self.registry.insert(name.to_string(), meta);
        Ok(())
    }

    /// Scan S3 for existing namespaces and populate the registry.
    /// Used on startup to discover pre-existing data.
    #[instrument(skip(self))]
    pub async fn scan_and_register(&self) -> Result<usize> {
        let keys = self.store.list_prefix("").await?;
        let mut count = 0;

        for key in &keys {
            if key.ends_with("/meta.json") {
                let ns_name = key.trim_end_matches("/meta.json");
                match self.store.get(key).await {
                    Ok(data) => {
                        if let Ok(meta) = NamespaceMetadata::from_bytes(&data) {
                            self.registry.insert(ns_name.to_string(), meta);
                            count += 1;
                        }
                    }
                    Err(_) => continue,
                }
            }
        }

        info!(namespaces = count, "scanned and registered namespaces");
        Ok(count)
    }

    /// Check if a namespace exists in the registry.
    pub fn exists_in_registry(&self, name: &str) -> bool {
        self.registry.contains_key(name)
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
