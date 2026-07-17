//! Authoritative namespace metadata and lifecycle management.
//!
//! A namespace is Zeppelin's top-level isolation boundary. This module owns the
//! persisted `{namespace}/meta.json` record that describes its vector shape,
//! search configuration, lifecycle state, and compaction health. It also
//! coordinates creation with an initial manifest and deletion with a durable
//! tombstone. Vector data, WAL fragments, and segments remain the responsibility
//! of their own modules.
//!
//! S3 or MinIO is authoritative. [`crate::namespace::manager::NamespaceManager`]
//! keeps a short-lived process-local [`dashmap::DashMap`] registry only to avoid
//! repeated metadata GETs. Read-only lookups may observe a cached record until
//! its TTL expires; mutation paths reload the object and use its ETag for
//! compare-and-swap publication.
//!
//! ## Lifecycle
//!
//! ```text
//! reserve meta.json as creating with If-None-Match: *
//!                 |
//!                 v
//!          create empty manifest
//!                 |
//!                 v
//!        CAS meta.json -> active
//!                 |
//!                 v
//!              active
//!                 |
//!                 | CAS meta.json -> deleting + bind evidence key
//!                 v
//!       write immutable destruction evidence
//!                 |
//!                 v
//!     delete manifest and namespace objects
//!                 |
//!                 | delete meta.json last
//!                 v
//!              absent
//! ```
//!
//! The `deleting` record is a tombstone: it prevents ordinary reads, writes,
//! and same-name recreation while cleanup can be resumed safely. Deleting the
//! metadata object last ensures an absent namespace never hides reachable data
//! behind an incomplete cleanup.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::namespace::manager::NamespaceMetadata`] and
//!    [`crate::namespace::manager::NamespaceState`] for the persisted contract.
//! 2. Read [`crate::namespace::manager::NamespaceManager::create_with_fts_and_index_config`]
//!    for atomic name reservation and initial manifest creation.
//! 3. Read [`crate::namespace::manager::NamespaceManager::get_including_deleting`]
//!    and [`crate::namespace::manager::NamespaceManager::list`] for cache and S3
//!    discovery behavior.
//! 4. Read [`crate::namespace::manager::NamespaceManager::prepare_governed_delete`],
//!    [`crate::namespace::manager::NamespaceManager::commit_governed_delete`],
//!    and [`crate::namespace::manager::NamespaceManager::finish_delete`] for the
//!    governed, evidence-producing, resumable deletion protocol. The lower-level
//!    [`crate::namespace::manager::NamespaceManager::start_delete`] helper exists
//!    only for managers constructed without preservation governance.
//! 5. Finish with [`crate::namespace::manager::NamespaceManager::update_index_config`]
//!    and the compaction-health methods for ETag-protected metadata updates.
//!
//! ## Rust concepts used here
//!
//! [`dashmap::DashMap`] provides sharded concurrent access to disposable cache
//! entries, similar to Java's `ConcurrentHashMap`; C would require explicit
//! bucket locks and lifetime management. Persisted state is cloned into owned
//! values before a map guard is released, so no borrow or lock guard crosses an
//! `.await`. Mutation helpers use `Result` and exhaustive matching to separate
//! missing namespaces, CAS conflicts, deletion tombstones, and storage failure.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, instrument};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::security::{DecisionId, PreservationService, PrincipalId, SecurityError};
use crate::storage::{ObjectUserMetadata, ZeppelinStore};
use crate::time::Clock;
use crate::types::{DistanceMetric, IndexType};

pub use super::types::{is_valid_namespace_name, NamespaceIncarnationId};
use super::NamespaceId;

/// Default lifetime of a process-local namespace registry entry.
const DEFAULT_NAMESPACE_REGISTRY_TTL: Duration = Duration::from_secs(5);
/// Maximum CAS attempts when adding an incarnation to legacy metadata.
const MAX_NAMESPACE_INCARNATION_MIGRATION_ATTEMPTS: usize = 8;
pub(crate) const NAMESPACE_INCARNATION_METADATA_KEY: &str = "zeppelin-namespace-incarnation";
/// Consecutive compaction failures before a namespace is reported degraded.
pub const COMPACTION_DEGRADED_FAILURE_THRESHOLD: u32 = 5;

/// Lifecycle state stored in `meta.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceState {
    /// Namespace accepts reads and writes.
    #[default]
    Active,
    /// Namespace name is reserved while its first manifest is published.
    Creating,
    /// Namespace is being deleted; clients may observe status but not use it.
    Deleting,
}

impl NamespaceState {
    /// Returns the stable lowercase representation used by API responses.
    ///
    /// # Returns
    ///
    /// Returns `"active"`, `"creating"`, or `"deleting"` without allocation.
    ///
    /// # Examples
    ///
    /// A deletion status response renders [`NamespaceState::Deleting`] as
    /// `"deleting"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NamespaceState::Active => "active",
            NamespaceState::Creating => "creating",
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
    /// Builds a persisted namespace override from server indexing defaults.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed boot-time settings whose namespace-relevant fields
    ///   should be frozen into `meta.json`.
    ///
    /// # Returns
    ///
    /// Returns an owned, serializable configuration independent of later
    /// process-default changes.
    ///
    /// # Examples
    ///
    /// If the server default uses 256 centroids and scalar quantization, a new
    /// namespace records those choices for future compactions.
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

    /// Overlays namespace-specific settings onto a complete server config.
    ///
    /// Settings that are not persisted per namespace, such as query limits,
    /// remain inherited from `base`.
    ///
    /// # Parameters
    ///
    /// - `base`: Borrowed current server indexing configuration.
    ///
    /// # Returns
    ///
    /// Returns an owned clone with the six namespace-controlled build fields
    /// replaced. The input is unchanged.
    ///
    /// # Performance
    ///
    /// Clones the small configuration value and performs no storage I/O.
    ///
    /// # Examples
    ///
    /// A namespace may retain `nlist = 128` while inheriting a newly tuned
    /// server-wide `default_nprobe` value.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `base.clone()` creates a separate owned structure before mutation. This
    /// resembles copying a Java value object or a C struct, but Rust prevents
    /// accidental mutation of the borrowed original.
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

    /// Validates namespace index settings against vector dimensionality.
    ///
    /// # Parameters
    ///
    /// - `dimensions`: Positive vector dimension already selected for the
    ///   namespace.
    ///
    /// # Returns
    ///
    /// Returns unit when the configuration can build an index of this shape.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] when `nlist` or `pq_m` is zero, or
    /// when product quantization cannot divide the vector into equal
    /// subquantizers. Validation has no side effects.
    ///
    /// # Examples
    ///
    /// Product quantization with 384 dimensions and `pq_m = 8` is valid;
    /// `pq_m = 10` is rejected because 384 is not divisible by 10.
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
    /// Returns the stable snake-case representation used by API responses.
    ///
    /// # Returns
    ///
    /// Returns a process-long string literal and performs no allocation.
    ///
    /// # Examples
    ///
    /// [`CompactionStatus::Failure`] renders as `"failure"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// Namespace compaction and index health persisted in `meta.json`.
///
/// The record is operational status, not the authority for which artifacts are
/// visible; the namespace manifest owns visibility. Defaulted fields preserve
/// compatibility with metadata written before health reporting existed.
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

/// Authoritative namespace metadata stored as `{namespace}/meta.json`.
///
/// This JSON object fixes the namespace's vector shape and search settings,
/// records its deletion tombstone, and exposes compaction health. New fields use
/// Serde defaults so older metadata remains readable. Changes are published as
/// whole-object ETag compare-and-swap writes rather than in-place field updates.
///
/// # Examples
///
/// An active 384-dimensional cosine namespace may have zero visible vectors,
/// an FTS configuration for `title`, and a compaction failure count. Queries
/// still discover actual WAL fragments and segments from its manifest.
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
    /// Immutable destruction evidence committed by the governed delete protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destruction_record_key: Option<String>,
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
    /// Runtime identity read from S3 user metadata, absent only for legacy
    /// objects written before incarnation IDs were introduced.
    #[serde(skip)]
    pub incarnation_id: Option<NamespaceIncarnationId>,
}

/// Immutable evidence that authorizes removal of one fenced live manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceDestructionRecord {
    /// Namespace lifetime being destroyed.
    pub(crate) namespace: NamespaceId,
    /// Exact fenced manifest generation whose visibility root is removed.
    pub(crate) manifest_version_destroyed: u64,
    /// Exact namespace-prefix object count observed while fenced.
    pub(crate) object_count: usize,
    /// Exact namespace-prefix byte count observed while fenced.
    pub(crate) byte_count: u64,
    /// Principal that requested destruction.
    pub(crate) actor: PrincipalId,
    /// Distinct approval principal when the policy required one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) approver: Option<PrincipalId>,
    /// Authorization decision bound to the destruction request.
    pub(crate) decision_id: DecisionId,
    /// Wall-clock evidence timestamp.
    pub(crate) ts: DateTime<Utc>,
}

impl NamespaceDestructionRecord {
    /// Strictly decode one governed-destruction evidence object.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| {
            ZeppelinError::Serialization(format!("destruction record is invalid: {error}"))
        })
    }

    /// Encode one governed-destruction evidence object as immutable JSON bytes.
    pub(crate) fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self).map(Bytes::from).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "destruction record serialization failed: {error}"
            ))
        })
    }
}

impl NamespaceMetadata {
    /// Builds the object-store key for a namespace metadata record.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Validated top-level namespace name without a slash.
    ///
    /// # Returns
    ///
    /// Returns an owned key in the form `{namespace}/meta.json`.
    ///
    /// # Examples
    ///
    /// Namespace `catalog` maps to `catalog/meta.json`.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/meta.json")
    }

    /// Serializes this metadata record as pretty-printed JSON bytes.
    ///
    /// JSON is self-describing and therefore safe for nested types with Serde
    /// defaults and optional fields.
    ///
    /// # Returns
    ///
    /// Returns owned immutable [`Bytes`] ready for an object-store PUT.
    ///
    /// # Errors
    ///
    /// Returns the shared JSON serialization error if any nested value cannot
    /// be encoded. No storage write occurs here.
    ///
    /// # Examples
    ///
    /// A caller serializes a complete replacement immediately before an ETag
    /// CAS; fields are not patched independently in S3.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`Bytes`] owns or shares an immutable byte buffer. It is closer to a
    /// reference-counted read-only byte array than a C pointer-length pair;
    /// cloning it does not necessarily copy the underlying allocation.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Decodes a complete namespace metadata record from JSON bytes.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed object body returned from authoritative storage.
    ///
    /// # Returns
    ///
    /// Returns an owned metadata value. Missing fields with `#[serde(default)]`
    /// receive compatibility defaults.
    ///
    /// # Errors
    ///
    /// Returns a JSON error when the object is malformed or contains an
    /// incompatible field representation. Corrupt metadata is never replaced
    /// with an empty namespace.
    ///
    /// # Examples
    ///
    /// Metadata written before `compaction_health` existed decodes with a
    /// default “never compacted” health record.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    fn user_metadata(&self) -> ObjectUserMetadata {
        let mut metadata = ObjectUserMetadata::new();
        if let Some(incarnation_id) = self.incarnation_id.as_ref() {
            metadata.insert(
                NAMESPACE_INCARNATION_METADATA_KEY,
                incarnation_id.as_string(),
            );
        }
        metadata
    }

    fn attach_user_metadata(mut self, metadata: &ObjectUserMetadata) -> Result<Self> {
        self.incarnation_id = metadata
            .get(NAMESPACE_INCARNATION_METADATA_KEY)
            .map(NamespaceIncarnationId::parse)
            .transpose()?;
        Ok(self)
    }
}

/// One disposable metadata snapshot held by the process-local registry.
#[derive(Debug, Clone)]
struct RegistryEntry {
    /// Owned metadata snapshot safe to return after releasing the map guard.
    meta: NamespaceMetadata,
    /// Monotonic instant when S3 last supplied or confirmed this snapshot.
    fetched_at: Instant,
}

/// Result of an idempotent namespace create request.
///
/// Both variants carry authoritative metadata. Callers can distinguish whether
/// this request performed initialization without treating a matching existing
/// namespace as an error.
///
/// # Examples
///
/// A deployment race yields one [`CreateNamespaceOutcome::Created`] and one
/// [`CreateNamespaceOutcome::Existing`] when both requests use identical
/// immutable settings.
#[derive(Debug, Clone)]
pub enum CreateNamespaceOutcome {
    /// The namespace did not exist and was created by this request.
    Created(NamespaceMetadata),
    /// The namespace already existed with the same immutable configuration.
    Existing(NamespaceMetadata),
}

/// Coordinates namespace CRUD against authoritative S3 metadata.
///
/// The registry is a bounded-staleness read optimization. It is disposable and
/// never used as the precondition for a metadata mutation; updates fetch the
/// current ETag and replace the complete `meta.json` object conditionally.
pub struct NamespaceManager {
    /// Shared gateway for authoritative metadata and manifest operations.
    store: ZeppelinStore,
    /// In-memory registry for fast lookups.
    registry: DashMap<String, RegistryEntry>,
    /// Maximum age at which a read-only lookup may reuse a registry entry.
    registry_ttl: Duration,
    /// Explicit wall clock used only for persisted metadata stamps.
    clock: Clock,
    /// Boot-composed preservation authority for every physical cleanup pass.
    preservation: Option<Arc<PreservationService>>,
}

impl NamespaceManager {
    /// Creates a manager using the default five-second registry TTL.
    ///
    /// # Parameters
    ///
    /// - `store`: Cloneable gateway to the authoritative object store.
    ///
    /// # Returns
    ///
    /// Returns an empty process-local manager. Existing namespaces are loaded
    /// lazily or by [`Self::scan_and_register`].
    ///
    /// # Examples
    ///
    /// A newly started node constructs a manager, then startup scans S3 to seed
    /// its disposable registry.
    pub fn new(store: ZeppelinStore) -> Self {
        Self::with_clock(store, DEFAULT_NAMESPACE_REGISTRY_TTL, Clock::system())
    }

    /// Creates a manager with an explicit metadata-cache TTL.
    ///
    /// # Parameters
    ///
    /// - `store`: Cloneable gateway to authoritative object storage.
    /// - `registry_ttl`: Maximum age for cached read-only metadata. Zero forces
    ///   every ordinary lookup to reload S3.
    ///
    /// # Returns
    ///
    /// Returns an empty manager configured with the requested TTL.
    ///
    /// # Examples
    ///
    /// Tests may use 100 milliseconds to observe a lifecycle change made by a
    /// second manager without waiting five seconds.
    #[must_use]
    pub fn new_with_registry_ttl(store: ZeppelinStore, registry_ttl: Duration) -> Self {
        Self::with_clock(store, registry_ttl, Clock::system())
    }

    /// Creates a manager with explicit registry lifetime and wall clock.
    #[must_use]
    pub fn with_clock(store: ZeppelinStore, registry_ttl: Duration, clock: Clock) -> Self {
        Self {
            store,
            registry: DashMap::new(),
            registry_ttl,
            clock,
            preservation: None,
        }
    }

    /// Attach the preservation authority to the deepest namespace purge seam.
    #[must_use]
    pub fn with_preservation_service(
        mut self,
        preservation: Option<Arc<PreservationService>>,
    ) -> Self {
        self.preservation = preservation;
        self
    }

    /// Creates an active vector namespace without full-text fields.
    ///
    /// This convenience entry point delegates to [`Self::create_with_fts`] with
    /// an empty FTS configuration and default per-namespace index settings.
    ///
    /// # Parameters
    ///
    /// - `name`: Client-selected namespace name valid as both an S3 prefix and
    ///   one URL path segment.
    /// - `dimensions`: Positive vector dimensionality.
    /// - `distance_metric`: Metric fixed for future indexing and queries.
    ///
    /// # Returns
    ///
    /// Returns the newly persisted metadata after both `meta.json` and the
    /// initial empty manifest are written.
    ///
    /// # Errors
    ///
    /// Propagates validation, create conflict, serialization, manifest, and
    /// storage errors from the full creation path.
    ///
    /// # Examples
    ///
    /// Creating `catalog` with 384 cosine dimensions reserves
    /// `catalog/meta.json` and initializes `catalog/manifest.json`.
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

    /// Creates a namespace with optional per-field full-text configuration.
    ///
    /// # Parameters
    ///
    /// - `name`: Valid S3- and URL-safe namespace name.
    /// - `dimensions`: Positive vector dimensionality.
    /// - `distance_metric`: Namespace-wide vector distance metric.
    /// - `full_text_search`: Owned field-to-analyzer configuration; an empty
    ///   map disables FTS.
    ///
    /// # Returns
    ///
    /// Returns persisted metadata after successful namespace initialization.
    ///
    /// # Errors
    ///
    /// Propagates invalid FTS settings and every failure documented by
    /// [`Self::create_with_fts_and_index_config`].
    ///
    /// # Examples
    ///
    /// A namespace can enable stemming for `title` while leaving all other
    /// attributes outside the lexical index.
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

    /// Atomically reserves a namespace name and initializes its empty manifest.
    ///
    /// The create-only `meta.json` PUT is the concurrency boundary: two creators
    /// cannot silently overwrite one another's dimensions or analyzers. The
    /// durable record remains `creating` until the first manifest exists and a
    /// metadata CAS publishes `active`. A replacement node can therefore finish
    /// an interrupted create without confusing later manifest loss with an empty
    /// namespace.
    ///
    /// ```text
    /// validate request
    ///       |
    ///       v
    /// PUT creating meta if absent ---- already exists --> conflict/deleting error
    ///       |
    ///       v
    /// write empty manifest ------- failure -------> creating meta remains
    ///       |
    ///       v
    /// CAS meta.json active ------- failure -------> restart recovery retries
    ///       |
    ///       v
    /// cache metadata and return active namespace
    /// ```
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace name accepted by [`is_valid_namespace_name`].
    /// - `dimensions`: Positive dimensionality shared by every vector.
    /// - `distance_metric`: Metric persisted for index build and query scoring.
    /// - `full_text_search`: Owned analyzer configuration for indexed fields.
    /// - `index_config`: Optional explicit segment-build settings; `None`
    ///   preserves legacy/default resolution behavior.
    ///
    /// # Returns
    ///
    /// Returns the active metadata after the name and initial manifest are
    /// persisted.
    ///
    /// # Errors
    ///
    /// Returns validation errors for an unsafe name, zero dimensions, invalid
    /// FTS settings, or incompatible index parameters. Returns
    /// [`ZeppelinError::NamespaceAlreadyExists`] when the create-only PUT loses
    /// to an active namespace, and [`ZeppelinError::NamespaceDeleting`] for a
    /// tombstoned name. Serialization and object-store failures propagate.
    ///
    /// Metadata is written before the initial manifest. If manifest creation or
    /// the activation CAS fails, a durable `creating` record remains. A later
    /// authoritative lookup explicitly resumes initialization; it never treats
    /// a missing manifest beneath `active` metadata as an empty namespace.
    ///
    /// # Side Effects
    ///
    /// Performs a conditional metadata PUT, writes an empty manifest, CASes the
    /// metadata active, inserts a registry snapshot, and emits a structured
    /// creation event.
    ///
    /// # Consistency
    ///
    /// S3 owns name uniqueness. No registry check can reserve a name. The
    /// create-only PUT prevents a time-of-check/time-of-use overwrite between
    /// nodes.
    ///
    /// # Performance
    ///
    /// The successful path performs two sequential object-store writes. A
    /// conflict may add one metadata GET to distinguish active from deleting.
    ///
    /// # Examples
    ///
    /// Two nodes concurrently create `catalog` with different dimensions. One
    /// create-only PUT wins; the other returns a conflict and cannot replace the
    /// winner's metadata.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Owned maps and optional index settings move into `NamespaceMetadata`, so
    /// the persisted value cannot borrow request memory that disappears after
    /// the async handler returns. Pattern matching keeps conflict, tombstone,
    /// and generic storage failures distinct.
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

        let now = self.clock.now();
        let meta = NamespaceMetadata {
            name: name.to_string(),
            dimensions,
            distance_metric,
            index_type: IndexType::default(),
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Creating,
            destruction_record_key: None,
            full_text_search,
            index_config,
            compaction_health: CompactionHealth::default(),
            incarnation_id: Some(NamespaceIncarnationId::new()),
        };

        // Atomic write — returns NamespaceAlreadyExists if meta.json exists
        let user_metadata = meta.user_metadata();
        match self
            .store
            .put_if_not_exists_with_user_metadata(&key, meta.to_bytes()?, name, &user_metadata)
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

        // Publish the first manifest before exposing the namespace as active.
        // Reusing the persisted creation timestamp makes a crash retry produce
        // byte-identical generation-one history.
        let mut manifest = crate::wal::Manifest::new_at(meta.created_at);
        manifest.bind_namespace_incarnation(
            meta.incarnation_id
                .as_ref()
                .ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "new namespace {name} is missing its incarnation identity"
                    ))
                })?
                .as_uuid(),
        )?;
        manifest.write(&self.store, name).await?;

        let meta = self.activate_created_namespace(name).await?;
        let meta = self.ensure_active(meta)?;

        info!(namespace = name, dimensions, %distance_metric, "created namespace");
        Ok(meta)
    }

    /// Completes a namespace whose durable name reservation is still creating.
    ///
    /// A process may stop after publishing `meta.json` but before publishing the
    /// first live manifest or activating the metadata. Only the explicit
    /// `creating` state authorizes this recovery. An `active` namespace with a
    /// missing manifest remains a loud integrity failure.
    async fn recover_creating_namespace(
        &self,
        mut meta: NamespaceMetadata,
    ) -> Result<NamespaceMetadata> {
        if meta.state != NamespaceState::Creating {
            return Ok(meta);
        }

        // A legacy interrupted create has no identity in meta.json user
        // metadata. Establish or recover that identity before inspecting or
        // publishing its bootstrap manifest. Active legacy namespaces are not
        // migrated by ordinary reads; this branch is part of creation recovery,
        // which is already a mutation path.
        if meta.incarnation_id.is_none() {
            meta = self
                .read_or_migrate_namespace_incarnation(&meta.name)
                .await?;
            if meta.state != NamespaceState::Creating {
                return self.ensure_active(meta);
            }
        }

        let name = meta.name.clone();
        match crate::wal::Manifest::read(&self.store, &name).await? {
            Some(mut manifest) => {
                let is_empty_bootstrap = manifest.fragments.is_empty()
                    && manifest.segments.is_empty()
                    && manifest.compaction_watermark.is_none()
                    && manifest.active_segment.is_none()
                    && manifest.next_sequence == 0
                    && manifest.pending_deletes.is_empty()
                    && manifest.fencing_token == 0;
                if !is_empty_bootstrap {
                    return Err(ZeppelinError::Serialization(format!(
                        "creating namespace {name} has non-empty bootstrap manifest generation {}",
                        manifest.version()
                    )));
                }
                let expected_incarnation = meta
                    .incarnation_id
                    .as_ref()
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(format!(
                            "creating namespace {name} is missing its incarnation identity"
                        ))
                    })?
                    .as_uuid();
                match (manifest.version(), manifest.namespace_incarnation()) {
                    (1, None) => {
                        manifest = crate::wal::Manifest::read_versioned_required_for_incarnation(
                            &self.store,
                            &name,
                            expected_incarnation,
                        )
                        .await?
                        .0;
                        if manifest.namespace_incarnation() != Some(expected_incarnation) {
                            return Err(ZeppelinError::Serialization(format!(
                                "creating namespace {name} manifest incarnation does not match metadata"
                            )));
                        }
                    }
                    (1 | 2, Some(actual)) if actual == expected_incarnation => {}
                    (_, Some(actual)) if actual != expected_incarnation => {
                        return Err(ZeppelinError::Serialization(format!(
                            "creating namespace {name} manifest incarnation does not match metadata"
                        )));
                    }
                    _ => {
                        return Err(ZeppelinError::Serialization(format!(
                            "creating namespace {name} has non-bootstrap manifest generation {}",
                            manifest.version()
                        )));
                    }
                }
            }
            None => {
                let mut manifest = crate::wal::Manifest::new_at(meta.created_at);
                manifest.bind_namespace_incarnation(
                    meta.incarnation_id
                        .as_ref()
                        .ok_or_else(|| {
                            ZeppelinError::Serialization(format!(
                                "creating namespace {name} is missing its incarnation identity"
                            ))
                        })?
                        .as_uuid(),
                )?;
                manifest.write(&self.store, &name).await?;
            }
        }

        let recovered = self.activate_created_namespace(&name).await?;
        info!(
            namespace = %name,
            state = recovered.state.as_str(),
            "recovered interrupted namespace creation"
        );
        Ok(recovered)
    }

    /// CAS-publishes an initialized namespace as active.
    ///
    /// The live manifest must already exist before this transition is called.
    /// Re-reading metadata on every retry preserves a concurrent deletion, and
    /// observing `active` makes the operation idempotent after a lost response.
    async fn activate_created_namespace(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            match meta.state {
                NamespaceState::Active | NamespaceState::Deleting => return Ok(meta),
                NamespaceState::Creating => {}
            }

            meta.state = NamespaceState::Active;
            meta.updated_at = self.clock.now();
            let etag = etag.unwrap_or_default();
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Idempotently creates a named namespace with optional FTS settings.
    ///
    /// Same name plus identical immutable configuration returns the existing
    /// S3 metadata; same name plus different configuration remains a conflict.
    /// This keeps create-by-name useful for multi-process clients without
    /// silently changing an existing namespace's shape.
    ///
    /// # Parameters
    ///
    /// - `name`: Stable client-selected namespace identifier.
    /// - `dimensions`: Requested vector dimensionality.
    /// - `distance_metric`: Requested vector distance metric.
    /// - `full_text_search`: Requested per-field FTS configuration.
    ///
    /// # Returns
    ///
    /// Returns [`CreateNamespaceOutcome::Created`] for the winning create or
    /// [`CreateNamespaceOutcome::Existing`] when authoritative metadata already
    /// has the same immutable configuration.
    ///
    /// # Errors
    ///
    /// Propagates validation and storage errors. A same-name namespace with a
    /// different configuration remains a conflict; a deletion tombstone remains
    /// unavailable.
    ///
    /// # Examples
    ///
    /// Two deployment processes can both request the same 384-dimensional
    /// `catalog` namespace. One receives `Created`; the other receives
    /// `Existing` if every immutable setting matches.
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

    /// Idempotently creates or verifies a namespace including index settings.
    ///
    /// This method first attempts the same atomic creation as the non-idempotent
    /// API. On a name conflict it reloads S3, rejects deletion tombstones, and
    /// compares every immutable request field with the persisted record.
    ///
    /// # Parameters
    ///
    /// - `name`: Stable client-selected namespace identifier.
    /// - `dimensions`: Requested positive vector dimensionality.
    /// - `distance_metric`: Requested vector metric.
    /// - `full_text_search`: Requested analyzer configuration.
    /// - `index_config`: Requested optional per-namespace index configuration.
    ///
    /// # Returns
    ///
    /// Returns whether this request created the namespace or matched an existing
    /// authoritative record.
    ///
    /// # Errors
    ///
    /// Returns validation errors for incompatible index settings, a deleting
    /// error for a tombstone, or an already-exists conflict when any immutable
    /// field differs. Storage and serialization failures propagate.
    ///
    /// # Side Effects
    ///
    /// May perform the two-write create flow. The existing path performs an S3
    /// metadata GET and refreshes the registry but does not rewrite metadata.
    ///
    /// # Consistency
    ///
    /// Equality is checked against freshly loaded S3 metadata, never only the
    /// process-local registry.
    ///
    /// # Examples
    ///
    /// Repeating a create with the same FTS analyzers and `nlist` returns
    /// `Existing`. Changing `nlist` under the same name returns a conflict.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method clones the request maps only because the first creation
    /// attempt consumes them; a conflict still needs the original logical values
    /// for comparison. Java references would remain usable automatically. Rust
    /// makes this ownership cost explicit.
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
                if !namespace_config_matches(
                    &existing,
                    dimensions,
                    distance_metric,
                    &full_text_search,
                    &index_config,
                )? {
                    return Err(ZeppelinError::NamespaceAlreadyExists {
                        namespace: name.to_string(),
                    });
                }
                let existing = self.recover_creating_namespace(existing).await?;
                let existing = self.ensure_active(existing)?;
                Ok(CreateNamespaceOutcome::Existing(existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Returns metadata for an active namespace.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose current metadata is required.
    ///
    /// # Returns
    ///
    /// Returns an owned metadata snapshot from a fresh-enough registry entry or
    /// an S3 reload.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found for an absent metadata object,
    /// namespace-deleting for a tombstone, or the underlying storage/JSON error.
    ///
    /// # Consistency
    ///
    /// Read-only lookup may reuse metadata for at most `registry_ttl`. S3
    /// remains authoritative, and mutation paths do not use this cached value as
    /// a write precondition.
    ///
    /// # Examples
    ///
    /// A normal query receives active metadata. A namespace already marked for
    /// deletion is rejected instead of being queried during cleanup.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = self.get_including_deleting(name).await?;
        self.ensure_active(meta)
    }

    /// Returns authoritative active metadata for a guarded write, migrating a
    /// legacy `meta.json` object when its user metadata has no incarnation.
    ///
    /// This is an explicit mutation-path seam for guarded writes. Ordinary
    /// [`Self::get`] and [`Self::list`] calls remain read-only so their object-
    /// store operation contracts do not change.
    ///
    /// Returning the complete metadata snapshot keeps dimensions and other
    /// validation fields paired with the same namespace lifetime as the
    /// incarnation. The result never combines an ordinary cached metadata body
    /// with an incarnation loaded after a delete/recreate race.
    ///
    /// # Errors
    ///
    /// Returns namespace lifecycle, storage, and decoding errors unchanged.
    /// Missing or empty authoritative ETags fail before any write because an
    /// unconditional legacy migration could overwrite concurrent metadata.
    /// Exhausting the bounded CAS retries returns a manifest conflict.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get_active_metadata_for_guarded_write(
        &self,
        name: &str,
    ) -> Result<NamespaceMetadata> {
        let meta = self.read_or_migrate_namespace_incarnation(name).await?;
        self.ensure_active(meta)
    }

    /// Loads metadata and establishes its incarnation without using the
    /// process registry as a precondition.
    ///
    /// Active and creating records are accepted. Deleting records fail before
    /// any metadata or manifest write. This narrower helper lets interrupted
    /// namespace creation recover a legacy record without turning ordinary
    /// active reads into migration writes.
    async fn read_or_migrate_namespace_incarnation(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);

        for _ in 0..MAX_NAMESPACE_INCARNATION_MIGRATION_ATTEMPTS {
            let (body, object_metadata) = match self.store.get_with_object_metadata(&key).await {
                Ok(value) => value,
                Err(ZeppelinError::NotFound { .. }) => {
                    return Err(ZeppelinError::NamespaceNotFound {
                        namespace: name.to_string(),
                    });
                }
                Err(error) => return Err(error),
            };
            let mut meta = NamespaceMetadata::from_bytes(&body)?
                .attach_user_metadata(&object_metadata.user_metadata)?;
            if meta.state == NamespaceState::Deleting {
                return Err(ZeppelinError::NamespaceDeleting {
                    namespace: meta.name,
                });
            }

            if meta.incarnation_id.is_some() {
                self.insert_registry(meta.clone());
                return Ok(meta);
            }

            let etag = object_metadata
                .e_tag
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "authoritative namespace metadata {key} has no non-empty ETag required for incarnation migration"
                    ))
                })?;
            // Mixed-version deployments may already have bound the manifest
            // before metadata received its header. Adopt that identity rather
            // than minting a conflicting lifetime. An active namespace without
            // any live manifest is an integrity error; an interrupted create is
            // allowed to establish a new identity before creating its first
            // manifest.
            let incarnation_id = match crate::wal::Manifest::read(&self.store, name).await? {
                Some(manifest) => manifest
                    .namespace_incarnation()
                    .map(NamespaceIncarnationId::from_uuid)
                    .unwrap_or_else(NamespaceIncarnationId::new),
                None if meta.state == NamespaceState::Creating => NamespaceIncarnationId::new(),
                None => {
                    return Err(ZeppelinError::Serialization(format!(
                        "active namespace {name} is missing its live manifest during incarnation migration"
                    )));
                }
            };
            let mut user_metadata = object_metadata.user_metadata;
            user_metadata.insert(
                NAMESPACE_INCARNATION_METADATA_KEY,
                incarnation_id.as_string(),
            );

            match self
                .store
                .put_if_match_with_user_metadata(&key, body, &etag, name, &user_metadata)
                .await
            {
                Ok(_) => {
                    meta.incarnation_id = Some(incarnation_id);
                    self.insert_registry(meta.clone());
                    info!(namespace = name, "migrated legacy namespace incarnation");
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Returns namespace metadata without rejecting the deletion tombstone.
    ///
    /// This lifecycle-aware form supports deletion status and cleanup workers;
    /// ordinary request paths should prefer [`Self::get`].
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose metadata snapshot is required.
    ///
    /// # Returns
    ///
    /// Returns active or deleting metadata from the TTL registry when fresh,
    /// otherwise from S3.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found, storage, or JSON decoding errors.
    ///
    /// # Performance
    ///
    /// A fresh registry hit clones metadata without remote I/O. A miss performs
    /// one full metadata GET.
    ///
    /// # Examples
    ///
    /// A status endpoint can report `deleting` while a background worker resumes
    /// prefix cleanup.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get_including_deleting(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = match self.fresh_registry_meta(name) {
            Some(meta) => meta,
            None => self.read_metadata_from_s3(name).await?,
        };
        self.recover_creating_namespace(meta).await
    }

    /// Loads authoritative metadata from S3 and refreshes the registry.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose `meta.json` object should be read.
    ///
    /// # Returns
    ///
    /// Returns decoded owned metadata and caches the same snapshot locally.
    ///
    /// # Errors
    ///
    /// Translates a missing object into namespace-not-found. Other storage and
    /// JSON failures propagate without inserting a replacement cache entry.
    ///
    /// # Side Effects
    ///
    /// Performs one object-store GET and updates the process-local registry.
    ///
    /// # Examples
    ///
    /// Listing discovers a `catalog/` prefix, loads `catalog/meta.json`, and
    /// registers the decoded namespace for later maintenance scans.
    async fn read_metadata_from_s3(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get_with_object_metadata(&key).await {
            Ok((data, object_metadata)) => {
                let meta = NamespaceMetadata::from_bytes(&data)?
                    .attach_user_metadata(&object_metadata.user_metadata)?;
                self.insert_registry(meta.clone());
                Ok(meta)
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Loads metadata together with the object version required for CAS.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose authoritative metadata will be mutated.
    ///
    /// # Returns
    ///
    /// Returns decoded metadata and the optional ETag supplied by the backend,
    /// while refreshing the registry snapshot.
    ///
    /// # Errors
    ///
    /// Translates a missing object into namespace-not-found and propagates
    /// storage or decoding failures.
    ///
    /// # Consistency
    ///
    /// Callers must pass the returned ETag to a conditional PUT; loading a value
    /// alone does not authorize an unconditional overwrite.
    ///
    /// # Examples
    ///
    /// An index-config update reads metadata at ETag `v12`, edits an owned clone,
    /// and publishes only if `v12` remains current.
    async fn read_metadata_versioned(
        &self,
        name: &str,
    ) -> Result<(NamespaceMetadata, Option<String>)> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get_with_object_metadata(&key).await {
            Ok((data, object_metadata)) => {
                let meta = NamespaceMetadata::from_bytes(&data)?
                    .attach_user_metadata(&object_metadata.user_metadata)?;
                self.insert_registry(meta.clone());
                Ok((meta, object_metadata.e_tag))
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// CAS-publishes metadata while preserving its namespace incarnation ID.
    async fn put_metadata_if_match(
        &self,
        key: &str,
        meta: &NamespaceMetadata,
        etag: &str,
        namespace: &str,
    ) -> Result<Option<String>> {
        let user_metadata = meta.user_metadata();
        self.store
            .put_if_match_with_user_metadata(key, meta.to_bytes()?, etag, namespace, &user_metadata)
            .await
    }

    /// Returns an owned registry snapshot when it remains inside the TTL.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace key in the process-local registry.
    ///
    /// # Returns
    ///
    /// Returns `Some` cloned metadata for a fresh entry, or `None` when missing
    /// or expired. Expired entries remain stored until a later refresh or list
    /// reconciliation.
    ///
    /// # Performance
    ///
    /// Acquires one DashMap shard guard and clones the metadata. The guard is
    /// dropped before the caller can perform async work.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `and_then` keeps the map guard scoped to the closure. Returning an owned
    /// clone avoids exposing a reference whose validity depends on a concurrent
    /// map entry and lock guard.
    fn fresh_registry_meta(&self, name: &str) -> Option<NamespaceMetadata> {
        self.registry.get(name).and_then(|entry| {
            (entry.fetched_at.elapsed() < self.registry_ttl).then(|| entry.meta.clone())
        })
    }

    /// Inserts an owned metadata snapshot with a fresh local timestamp.
    ///
    /// # Parameters
    ///
    /// - `meta`: Metadata confirmed by a successful read or write.
    ///
    /// # Returns
    ///
    /// Returns unit after replacing any prior entry for the same name.
    ///
    /// # Side Effects
    ///
    /// Mutates only the disposable in-memory registry.
    fn insert_registry(&self, meta: NamespaceMetadata) {
        self.registry.insert(
            meta.name.clone(),
            RegistryEntry {
                meta,
                fetched_at: Instant::now(),
            },
        );
    }

    /// Rejects non-active lifecycle states while preserving active metadata.
    ///
    /// # Parameters
    ///
    /// - `meta`: Owned lifecycle metadata snapshot.
    ///
    /// # Returns
    ///
    /// Returns the same owned value when active.
    ///
    /// # Errors
    ///
    /// Returns namespace-deleting when cleanup has begun. A still-creating
    /// namespace becomes a retryable manifest conflict; ordinary lookup paths
    /// first attempt explicit recovery and should not normally expose it.
    ///
    /// # Examples
    ///
    /// Query setup can pass loaded metadata through this helper and fail before
    /// reading a manifest for a tombstoned namespace.
    fn ensure_active(&self, meta: NamespaceMetadata) -> Result<NamespaceMetadata> {
        match meta.state {
            NamespaceState::Active => Ok(meta),
            NamespaceState::Creating => Err(ZeppelinError::ManifestConflict {
                namespace: meta.name,
            }),
            NamespaceState::Deleting => Err(ZeppelinError::NamespaceDeleting {
                namespace: meta.name,
            }),
        }
    }

    /// Lists authoritative namespace metadata, optionally filtered by name prefix.
    ///
    /// The method performs delimiter-based discovery of top-level namespace
    /// prefixes so it does not recursively walk WAL fragments or segment
    /// objects. Each discovered prefix is confirmed by loading `meta.json`.
    /// Missing metadata is treated as a concurrently removed namespace.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Optional lexical namespace-name prefix, not an S3 directory
    ///   boundary. `None` lists every top-level namespace.
    ///
    /// # Returns
    ///
    /// Returns decoded metadata for discovered namespaces. Ordering follows the
    /// backend prefix listing and is not guaranteed.
    ///
    /// # Errors
    ///
    /// Returns listing, metadata-read, or decoding errors other than a metadata
    /// object that disappeared during the scan.
    ///
    /// # Side Effects
    ///
    /// Performs one delimiter LIST plus one metadata GET per unique prefix,
    /// refreshes found registry entries, and removes cached entries proven absent
    /// within the requested scope.
    ///
    /// # Consistency
    ///
    /// The result is a sequence of S3 observations, not a transactional bucket
    /// snapshot. Concurrent creates or deletes may appear in a later scan.
    ///
    /// # Performance
    ///
    /// Cost scales with namespace count rather than every artifact in the
    /// bucket. Metadata GETs currently run sequentially.
    ///
    /// # Examples
    ///
    /// Prefix `tenant-a` can match `tenant-a-prod` and `tenant-a-test`; the
    /// filter is applied to namespace names after top-level discovery.
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
            // Reserved control-plane roots such as `_audit/` are not namespace
            // candidates. Filtering with the same creation grammar avoids an
            // unnecessary `<reserved>/meta.json` GET on every stateless boot.
            if !is_valid_namespace_name(ns_name) {
                continue;
            }
            if seen.insert(ns_name.to_string()) {
                match self.read_metadata_from_s3(ns_name).await {
                    Ok(meta) => {
                        let meta = self.recover_creating_namespace(meta).await?;
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

    /// Synchronously deletes a namespace through the resumable tombstone protocol.
    ///
    /// On an explicitly ungoverned manager, synchronous direct delete flips
    /// `meta.json` to `deleting`, purges all namespace data while keeping the
    /// tombstone, then deletes `meta.json` last. A production manager with the
    /// preservation service attached rejects this path before any mutation; its
    /// HTTP handler uses governed prepare/evidence/commit primitives instead.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace to tombstone and purge.
    ///
    /// # Returns
    ///
    /// Returns unit only after cleanup reports complete.
    ///
    /// # Errors
    ///
    /// Returns validation before mutation when preservation governance is
    /// attached. Otherwise propagates tombstone, manifest-delete, listing,
    /// object-delete, and final metadata-delete errors. Returns
    /// namespace-delete-incomplete if an unbounded pass unexpectedly stops
    /// early. Partial cleanup remains resumable because `meta.json` stays in
    /// `deleting` state until the end.
    ///
    /// # Side Effects
    ///
    /// On an ungoverned manager, conditionally updates metadata, deletes the
    /// manifest and namespace objects, removes registry state, and finally
    /// deletes the tombstone. A governed-manager rejection has no side effects.
    ///
    /// # Examples
    ///
    /// Administrative or test cleanup can await this method when it needs the
    /// namespace fully absent before continuing.
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

    /// Marks a namespace deleting and removes its manifest visibility root.
    ///
    /// This lower-level administrative/test helper predates governed destruction.
    /// An explicitly ungoverned manager writes the durable metadata tombstone
    /// before removing the manifest, so ordinary namespace access is rejected
    /// before data cleanup begins. A production manager with preservation
    /// attached rejects this path before tombstoning. HTTP deletion instead uses
    /// [`Self::prepare_governed_delete`], a manifest deletion fence, durable
    /// evidence publication, and [`Self::commit_governed_delete`].
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose deletion should begin or resume.
    ///
    /// # Returns
    ///
    /// Returns the persisted deleting metadata. Repeating the call for an
    /// existing tombstone is idempotent.
    ///
    /// # Errors
    ///
    /// Returns validation before mutation when preservation governance is
    /// attached. Otherwise returns namespace-not-found, CAS conflict after
    /// bounded retries, serialization, or storage errors. If manifest deletion
    /// fails, the tombstone remains authoritative and cleanup can be retried.
    ///
    /// # Side Effects
    ///
    /// On an ungoverned manager, CAS-updates `meta.json`, evicts the registry
    /// entry, deletes the manifest if present, and logs the lifecycle transition.
    /// A governed-manager rejection has no side effects.
    ///
    /// # Consistency
    ///
    /// The tombstone precedes destructive cleanup. A missing manifest is accepted
    /// as already removed; no stale cached metadata is used for publication.
    ///
    /// # Examples
    ///
    /// Tests or explicitly ungoverned administrative callers may use this helper.
    /// Governed HTTP deletion must use [`Self::prepare_governed_delete`].
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn start_delete(&self, name: &str) -> Result<NamespaceMetadata> {
        if self.preservation.is_some() {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} requires governed deletion"
            )));
        }
        let meta = self.mark_deleting(name, false).await?;

        self.registry.remove(name);
        self.remove_live_manifest(name).await?;
        info!(
            namespace = name,
            state = NamespaceState::Deleting.as_str(),
            "namespace marked deleting"
        );
        Ok(meta)
    }

    /// Tombstone a namespace while durably binding its destruction-record key.
    ///
    /// This is the first governed destruction step. It derives the immutable
    /// evidence key from the authoritative namespace incarnation loaded in the
    /// same CAS retry loop that publishes the tombstone. Legacy active metadata
    /// is migrated first and then reloaded. It does not remove the live manifest.
    pub async fn prepare_governed_delete(&self, name: &str) -> Result<NamespaceMetadata> {
        self.mark_deleting(name, true).await
    }

    /// Remove the live manifest only after the referenced evidence exists.
    ///
    /// This is the governed destruction commit step. It reloads authoritative
    /// tombstone metadata, verifies its exact evidence binding, requires the
    /// immutable evidence object to exist, verifies that the current manifest is
    /// the expected fenced generation, and refreshes preservation authority
    /// immediately before ending manifest visibility.
    pub async fn commit_governed_delete(
        &self,
        name: &str,
        destruction_record_key: &str,
        expected_manifest_version: u64,
    ) -> Result<()> {
        let meta = self.read_metadata_from_s3(name).await?;
        if meta.state != NamespaceState::Deleting
            || meta.destruction_record_key.as_deref() != Some(destruction_record_key)
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} destruction record is not bound to its tombstone"
            )));
        }
        let evidence =
            NamespaceDestructionRecord::from_bytes(&self.store.get(destruction_record_key).await?)?;
        let namespace = NamespaceId::new(name.to_string())?;
        if evidence.namespace != namespace
            || evidence.manifest_version_destroyed != expected_manifest_version
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} destruction evidence does not match the governed commit"
            )));
        }
        if let Some(manifest) = crate::wal::Manifest::read(&self.store, name).await? {
            manifest.require_destruction_fence(
                name,
                destruction_record_key,
                expected_manifest_version,
            )?;
        }
        if let Some(preservation) = &self.preservation {
            preservation.refresh_once().await?;
            let guard = preservation.guard_namespace(&namespace)?;
            if guard.is_locked() {
                preservation
                    .record_namespace_delete_deferral(&namespace, &guard)
                    .await?;
                return Err(SecurityError::PreservationLocked.into());
            }
        }
        self.registry.remove(name);
        self.remove_live_manifest(name).await
    }

    async fn remove_live_manifest(&self, name: &str) -> Result<()> {
        let manifest_key = crate::wal::Manifest::s3_key(name);
        match self.store.delete(&manifest_key).await {
            Ok(()) | Err(ZeppelinError::NotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Resumes bounded object cleanup and deletes the tombstone only when safe.
    ///
    /// ```text
    /// require meta.json state = deleting
    ///                 |
    ///                 v
    /// require fresh unlocked preservation state + bound evidence
    ///                 |
    ///                 v
    /// delete prefix except meta.json ---- budget ends --> incomplete outcome
    ///                 |
    ///                 v
    /// relist and require no non-meta keys
    ///                 |
    ///                 v
    /// delete meta.json last and evict cache
    /// ```
    ///
    /// # Parameters
    ///
    /// - `name`: Tombstoned namespace whose cleanup should continue.
    /// - `budget`: Approximate time budget forwarded to paged prefix deletion.
    ///
    /// # Returns
    ///
    /// Returns the paged deletion outcome. `complete = false` means the
    /// tombstone remains and a later call must resume. A complete result means
    /// all non-metadata objects were verified absent and `meta.json` was removed.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found if no tombstone exists, validation if the
    /// namespace is still active or governed evidence is missing, preservation
    /// locked if a fresh applicable lock exists, namespace-delete-incomplete if
    /// verification finds remaining objects, or storage/listing failures.
    /// Partial deletion may already have occurred.
    ///
    /// # Side Effects
    ///
    /// Lists and deletes object-store keys, deletes the metadata tombstone last,
    /// evicts local registry state, and logs completed deletion.
    ///
    /// # Consistency
    ///
    /// Prefix deletion alone is not trusted as proof of completion. With
    /// governance attached, each pass first consults fresh S3-authoritative lock
    /// state and verifies the tombstone's immutable evidence reference. The
    /// method relists authoritative storage and preserves the tombstone whenever
    /// any non-metadata key remains.
    ///
    /// # Performance
    ///
    /// Performs paged LIST/DELETE work. A completed pass adds a full verification
    /// LIST and one final metadata DELETE.
    ///
    /// # Examples
    ///
    /// A 25-second background pass may return incomplete after deleting a chunk;
    /// the next maintenance iteration calls this method again using the same
    /// durable tombstone.
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
        if let Some(preservation) = &self.preservation {
            let namespace = NamespaceId::new(name.to_string())?;
            preservation.refresh_once().await?;
            let guard = preservation.guard_namespace(&namespace)?;
            if guard.is_locked() {
                preservation
                    .record_namespace_delete_deferral(&namespace, &guard)
                    .await?;
                return Err(SecurityError::PreservationLocked.into());
            }
            let evidence_key = meta.destruction_record_key.as_deref().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {name} deletion tombstone has no destruction evidence reference"
                ))
            })?;
            let evidence =
                NamespaceDestructionRecord::from_bytes(&self.store.get(evidence_key).await?)?;
            if evidence.namespace != namespace {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} destruction evidence does not match its tombstone"
                )));
            }
        }

        if crate::wal::Manifest::read(&self.store, name)
            .await?
            .is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cleanup requires governed manifest removal"
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

    /// Publishes new per-namespace index settings for future compactions.
    ///
    /// Existing immutable segments are not rewritten. The new settings become
    /// inputs to later compactions after the metadata CAS succeeds.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose desired build settings should change.
    /// - `index_config`: Fully resolved replacement configuration.
    ///
    /// # Returns
    ///
    /// Returns the newly published metadata snapshot.
    ///
    /// # Errors
    ///
    /// Returns validation for incompatible namespace dimensions, deleting for a
    /// tombstone, storage/serialization errors, or manifest-conflict after ten
    /// CAS attempts. No segment artifact is modified by this method.
    ///
    /// # Side Effects
    ///
    /// Performs at least one metadata GET and conditional PUT, updates
    /// `updated_at`, and refreshes the registry on success.
    ///
    /// # Consistency
    ///
    /// Every retry reloads metadata and its ETag. A stale writer cannot
    /// overwrite a concurrent lifecycle or health update.
    ///
    /// # Examples
    ///
    /// Changing `nlist` from 128 to 256 affects the next segment build; segments
    /// already referenced by the manifest retain their original layout.
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
            meta.updated_at = self.clock.now();
            let etag = etag.unwrap_or_default();
            match self
                .put_metadata_if_match(&key, &meta, &etag, &meta_name)
                .await
            {
                Ok(_) => {
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

    /// Records a successful compaction and clears accumulated failure state.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose latest outcome should be stored.
    ///
    /// # Returns
    ///
    /// Returns metadata with a fresh completion time, `success` status, no error
    /// message, and zero consecutive failures.
    ///
    /// # Errors
    ///
    /// Propagates namespace, CAS, serialization, and storage failures from the
    /// shared health updater.
    ///
    /// # Examples
    ///
    /// A successful run after six failures resets both the health counter and
    /// the degraded Prometheus gauge.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_success(&self, name: &str) -> Result<NamespaceMetadata> {
        let now = self.clock.now();
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(now);
            health.last_compaction_status = CompactionStatus::Success;
            health.last_compaction_error = None;
            health.consecutive_failures = 0;
        })
        .await
    }

    /// Records a failed compaction and increments consecutive failure health.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose latest outcome should be stored.
    /// - `error`: Borrowed internal failure whose display text is persisted for
    ///   operator diagnostics.
    ///
    /// # Returns
    ///
    /// Returns metadata containing the failure time, status, message, and
    /// saturating consecutive-failure count.
    ///
    /// # Errors
    ///
    /// Propagates namespace, CAS, serialization, and storage failures from the
    /// shared health updater. The original compaction error is not returned by
    /// this metadata operation.
    ///
    /// # Examples
    ///
    /// The fifth consecutive failure publishes a count of five and marks the
    /// namespace degraded in metrics after the CAS succeeds.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_failure(
        &self,
        name: &str,
        error: &ZeppelinError,
    ) -> Result<NamespaceMetadata> {
        let message = error.to_string();
        let now = self.clock.now();
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(now);
            health.last_compaction_status = CompactionStatus::Failure;
            health.last_compaction_error = Some(message.clone());
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        })
        .await
    }

    /// Applies and publishes one retry-safe compaction-health transformation.
    ///
    /// The closure may run more than once. Each CAS conflict reloads the latest
    /// metadata, reapplies the transformation, and attempts a new conditional
    /// PUT so concurrent metadata changes are preserved.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose health record should change.
    /// - `update`: Reusable transformation applied to each freshly loaded health
    ///   record.
    ///
    /// # Returns
    ///
    /// Returns the metadata version whose conditional PUT succeeded.
    ///
    /// # Errors
    ///
    /// Returns namespace/deleting, serialization, storage, or final CAS conflict
    /// errors after ten attempts.
    ///
    /// # Side Effects
    ///
    /// Performs versioned metadata reads and conditional writes, refreshes the
    /// registry, and updates the degraded gauge only after publication succeeds.
    ///
    /// # Consistency
    ///
    /// ETag CAS prevents a health update from overwriting a concurrent index
    /// setting or deletion transition. The metric mirrors only committed
    /// metadata.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The parameter is `impl Fn`, not `FnOnce`, because a CAS retry may invoke
    /// it repeatedly. Rust encodes that reuse requirement in the type system;
    /// Java would rely on a reusable functional object, while C would pass a
    /// function pointer plus explicit context.
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
            meta.updated_at = self.clock.now();
            let degraded = meta.compaction_health.consecutive_failures
                >= COMPACTION_DEGRADED_FAILURE_THRESHOLD;
            let etag = etag.unwrap_or_default();
            match self
                .put_metadata_if_match(&key, &meta, &etag, &meta_name)
                .await
            {
                Ok(_) => {
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

    /// CAS-transitions active metadata to the durable deleting state.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose tombstone should be created or observed.
    ///
    /// # Returns
    ///
    /// Returns existing deleting metadata idempotently, or the newly published
    /// tombstone after a successful conditional PUT.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found, storage/serialization errors, or conflict
    /// after two CAS attempts.
    ///
    /// # Consistency
    ///
    /// Each retry reloads S3 and its ETag. The process registry is refreshed
    /// only with metadata read from or successfully written to S3.
    async fn mark_deleting(&self, name: &str, governed: bool) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            if governed && meta.incarnation_id.is_none() {
                if meta.state == NamespaceState::Deleting {
                    return Err(ZeppelinError::Serialization(format!(
                        "deleting namespace {name} has no incarnation for governed destruction"
                    )));
                }
                self.read_or_migrate_namespace_incarnation(name).await?;
                continue;
            }

            let governed_key = if governed {
                let incarnation = meta.incarnation_id.as_ref().ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "authoritative namespace metadata {key} omitted its incarnation"
                    ))
                })?;
                Some(format!(
                    "_audit/destruction/{}.json",
                    incarnation.as_uuid().simple()
                ))
            } else {
                None
            };

            if meta.state == NamespaceState::Deleting {
                match (&meta.destruction_record_key, &governed_key) {
                    (Some(existing), Some(expected)) if existing != expected => {
                        return Err(ZeppelinError::Validation(format!(
                            "namespace {name} deletion is bound to a different destruction record"
                        )));
                    }
                    (Some(_), Some(_)) | (None, None) => return Ok(meta),
                    (Some(_), None) => {
                        return Err(ZeppelinError::Validation(format!(
                            "namespace {name} requires governed deletion"
                        )));
                    }
                    (None, Some(expected)) => {
                        meta.destruction_record_key = Some(expected.clone());
                    }
                }
            } else {
                meta.state = NamespaceState::Deleting;
                meta.destruction_record_key = governed_key;
            }

            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no non-empty ETag required for deletion"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
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

    /// Scans S3 for namespaces and seeds the process-local registry.
    ///
    /// Startup uses this to discover pre-existing data on a stateless node.
    ///
    /// # Returns
    ///
    /// Returns the number of namespace metadata records found by
    /// [`Self::list`].
    ///
    /// # Errors
    ///
    /// Propagates list or metadata-load errors. Successfully loaded entries may
    /// already be present in the registry when a later entry fails.
    ///
    /// # Side Effects
    ///
    /// Performs the full namespace list flow, refreshes the registry, and logs
    /// the discovered count.
    ///
    /// # Examples
    ///
    /// A replacement compute node starts with an empty registry, scans the same
    /// S3 bucket, and discovers the namespaces created by earlier nodes.
    #[instrument(skip(self))]
    pub async fn scan_and_register(&self) -> Result<usize> {
        let namespaces = self.list(None).await?;
        let count = namespaces.len();

        info!(namespaces = count, "scanned and registered namespaces");
        Ok(count)
    }

    /// Checks only whether a name is present in the local registry.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace key to inspect.
    ///
    /// # Returns
    ///
    /// Returns `true` for any cached entry, including expired or deleting
    /// metadata. It does not prove current S3 existence.
    ///
    /// # Examples
    ///
    /// Tests can confirm that a successful create seeded the registry, but
    /// production authorization must use [`Self::get`] rather than this helper.
    pub fn exists_in_registry(&self, name: &str) -> bool {
        self.registry.contains_key(name)
    }

    /// Snapshots cached namespaces for background maintenance loops.
    ///
    /// This deliberately does not enforce registry TTL. Maintenance scans use
    /// the registry as a work-discovery hint and revalidate authoritative state
    /// inside the operation they run.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Optional lexical namespace-name prefix.
    ///
    /// # Returns
    ///
    /// Returns owned metadata clones in unspecified map iteration order.
    ///
    /// # Performance
    ///
    /// Iterates the process-local DashMap and clones every matching metadata
    /// record; it performs no object-store I/O.
    ///
    /// # Examples
    ///
    /// The compaction scheduler snapshots cached `tenant-a` namespaces, then
    /// each compaction path verifies current leases, metadata, and manifests.
    #[must_use]
    pub fn cached_namespaces(&self, prefix: Option<&str>) -> Vec<NamespaceMetadata> {
        self.registry
            .iter()
            .filter_map(|entry| {
                let meta = &entry.value().meta;
                if meta.state == NamespaceState::Creating {
                    return None;
                }
                match prefix {
                    Some(prefix) if !meta.name.starts_with(prefix) => None,
                    _ => Some(meta.clone()),
                }
            })
            .collect()
    }
}

/// Compares an idempotent create request with persisted immutable settings.
///
/// # Parameters
///
/// - `existing`: Fresh authoritative namespace metadata.
/// - `dimensions`: Requested vector dimensionality.
/// - `distance_metric`: Requested vector metric.
/// - `full_text_search`: Requested FTS field configuration.
/// - `index_config`: Requested optional index settings.
///
/// # Returns
///
/// Returns `true` only when every immutable setting matches. FTS maps are
/// compared through their serialized JSON value representation.
///
/// # Errors
///
/// Returns a serialization error if either FTS configuration cannot be
/// represented as JSON.
///
/// # Examples
///
/// Map insertion order does not create a false conflict, but changing one
/// analyzer or the vector dimensions returns `false`.
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

/// Converts an FTS configuration map into its order-independent JSON value.
///
/// # Parameters
///
/// - `full_text_search`: Borrowed field configuration map.
///
/// # Returns
///
/// Returns an owned JSON object used for semantic equality comparison.
///
/// # Errors
///
/// Returns the shared JSON serialization error if a nested configuration cannot
/// be represented.
///
/// # Rust Notes for Java/C Engineers
///
/// The borrowed map is not consumed. Serde constructs an owned value tree,
/// comparable to a Java JSON DOM; in C both allocation and recursive cleanup
/// would need explicit management.
fn fts_config_value(
    full_text_search: &std::collections::HashMap<String, FtsFieldConfig>,
) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(full_text_search)?)
}

#[cfg(test)]
mod tests {
    //! Unit tests for the namespace-name boundary shared by S3 keys and routes.

    use super::*;

    #[test]
    fn malformed_incarnation_metadata_fails_loudly() {
        let now = Utc::now();
        let metadata = NamespaceMetadata {
            name: "invalid-incarnation".to_string(),
            dimensions: 16,
            distance_metric: DistanceMetric::Euclidean,
            index_type: IndexType::IvfFlat,
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Active,
            destruction_record_key: None,
            full_text_search: std::collections::HashMap::new(),
            index_config: None,
            compaction_health: CompactionHealth::default(),
            incarnation_id: None,
        };
        let mut user_metadata = ObjectUserMetadata::new();
        user_metadata.insert(NAMESPACE_INCARNATION_METADATA_KEY, "not-a-uuid");

        let error = match metadata.attach_user_metadata(&user_metadata) {
            Ok(_) => panic!("malformed incarnation metadata unexpectedly decoded"),
            Err(error) => error,
        };

        assert!(matches!(error, ZeppelinError::Serialization(_)));
    }

    /// Verifies the accepted grammar covers the intended portable name forms.
    #[test]
    fn namespace_name_validator_accepts_s3_and_url_safe_names() {
        for name in ["tenant-a", "tenant_a", "tenant.a", "TenantA-123"] {
            assert!(
                is_valid_namespace_name(name),
                "expected namespace name to be valid: {name}"
            );
        }
    }

    /// Verifies unsafe paths, encodings, leading punctuation, and length fail.
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
