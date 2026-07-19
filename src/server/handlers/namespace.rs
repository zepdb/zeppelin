//! HTTP orchestration for namespace lifecycle and administrative operations.
//!
//! This module is the thin Axum boundary between namespace-oriented HTTP
//! routes and Zeppelin's domain services. It deserializes path and JSON input,
//! performs request-specific validation, calls
//! [`crate::namespace::manager::NamespaceManager`] and manifest-based services,
//! and turns successful results into stable JSON response shapes. Domain
//! failures remain [`crate::error::ZeppelinError`] values until
//! [`crate::server::handlers::ApiError`] maps them to the canonical HTTP error
//! envelope.
//!
//! S3 or MinIO remains authoritative throughout these handlers. Namespace
//! metadata describes identity, vector shape, lifecycle, desired index
//! settings, and compaction health. The [`crate::wal::Manifest`] separately
//! defines which immutable WAL fragments and segments are visible. For that
//! reason,
//! [`get_namespace`][crate::server::handlers::namespace::get_namespace]
//! combines metadata with a strong manifest read rather than trusting counters
//! in memory or recursively listing objects.
//!
//! The routed operations are create, get, delete, named snapshot management,
//! point-in-time clone, index-config patching, manual compaction, compaction
//! status, and cache hydration.
//! [`list_namespaces`][crate::server::handlers::namespace::list_namespaces]
//! exists for internal use but is deliberately not registered as an HTTP route,
//! preventing namespace enumeration. Rate limits, body limits, timeouts,
//! request IDs, and tracing are applied by [`crate::server::build_router`]
//! outside this file.
//!
//! ## Reading map
//!
//! 1. Start with
//!    [`CreateNamespaceRequest`][crate::server::handlers::namespace::CreateNamespaceRequest]
//!    and [`NamespaceResponse`][crate::server::handlers::namespace::NamespaceResponse]
//!    to learn the public namespace model.
//! 2. Read [`create_namespace`][crate::server::handlers::namespace::create_namespace],
//!    [`get_namespace`][crate::server::handlers::namespace::get_namespace], and
//!    [`delete_namespace`][crate::server::handlers::namespace::delete_namespace]
//!    for the main lifecycle.
//! 3. Continue with [`put_snapshot`][crate::server::handlers::namespace::put_snapshot],
//!    [`list_snapshots`][crate::server::handlers::namespace::list_snapshots],
//!    [`get_snapshot`][crate::server::handlers::namespace::get_snapshot], and
//!    [`delete_snapshot`][crate::server::handlers::namespace::delete_snapshot]
//!    for point-in-time retention pins.
//! 4. Read [`CloneNamespaceRequest`][crate::server::handlers::namespace::CloneNamespaceRequest]
//!    and [`clone_namespace`][crate::server::handlers::namespace::clone_namespace]
//!    for independent copy-based restoration.
//! 5. Finish with
//!    [`patch_index_config`][crate::server::handlers::namespace::patch_index_config],
//!    [`compact_namespace`][crate::server::handlers::namespace::compact_namespace],
//!    [`get_compaction_status`][crate::server::handlers::namespace::get_compaction_status],
//!    and [`trigger_hydration`][crate::server::handlers::namespace::trigger_hydration]
//!    for asynchronous administrative work.
//!
//! ## Namespace lifecycle and authority
//!
//! ```text
//! POST create
//!     |
//!     v
//! create meta.json if absent --> write empty manifest --> 201 Created
//!     | conflict
//!     +--> same named configuration --> 200 OK
//!     +--> different configuration --> 409 conflict
//!
//! GET namespace
//!     |
//!     +--> metadata snapshot
//!     +--> strong manifest verification (S3/MinIO authority)
//!     `--> combined status JSON; no WAL/segment reads or prefix LIST
//!
//! DELETE namespace
//!     |
//!     v
//! CAS metadata to "deleting" --> remove manifest visibility root --> 202
//!     |
//!     `--> background prefix cleanup --> delete tombstone last --> later 404
//! ```
//!
//! ## Point-in-time clone flow
//!
//! ```text
//! resolve retained source manifest
//!              |
//!              v
//! create temporary source snapshot pin
//!              |
//!              v
//! reserve fresh target namespace
//!              |
//!              v
//! copy every manifest-reachable immutable object (up to 16 concurrently)
//!              |
//!              v
//! rewrite target keys + conditionally publish target clone manifest
//!              |
//!              v
//! invalidate target cache + release source pin --> 201 Created
//!              |
//!              `-- failure: retain target, invalidate cache, release pin
//! ```
//!
//! ## Invariants
//!
//! - A response never treats the process-local manifest cache as more
//!   authoritative than object storage; strong reads verify the remote object.
//! - Namespace creation reserves `meta.json` before initializing the manifest.
//!   A manifest-write failure can therefore leave metadata for an operation
//!   that returned an error; the handler does not claim rollback.
//! - `202 Accepted` means background work was admitted or requested, not that
//!   compaction, deletion, index rewriting, or hydration has completed.
//! - Deletion writes a durable tombstone and removes the manifest before
//!   deleting data objects. Retried DELETE requests resume the protocol.
//! - A clone owns copied target objects and rewrites stored source keys; it must
//!   not leave the target dependent on source objects that can later be deleted.
//! - Cache invalidation changes only disposable process state. It never creates
//!   or removes manifest authority.
//!
//! ## Rust concepts used here
//!
//! Axum extractors destructure [`axum::extract::State`],
//! [`axum::extract::Path`], and [`axum::Json`] directly in function parameters.
//! This is similar to dependency injection plus request binding in Java; in C
//! it would normally require explicit parsing and context pointers. Rust's
//! types ensure a handler body runs only after extraction has produced the
//! declared owned values.
//!
//! Request bodies own their strings and maps, so moving them into async domain
//! calls cannot leave pointers into an HTTP buffer. Conversely,
//! [`NamespaceResponse::from_manifest`][crate::server::handlers::namespace::NamespaceResponse::from_manifest]
//! borrows the manifest with `&Manifest` and cannot retain that reference after
//! it returns. `tokio::spawn` requires its future to own cloned shared handles
//! and strings because the HTTP request may finish first. Java would rely on
//! garbage-collected references; C would require an explicit ownership
//! protocol. Rust checks those lifetimes at compile time.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::cache::hydration::HydrationTarget;
use crate::compaction::background::run_compaction_with_reserved_lease;
use crate::error::ZeppelinError;
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::namespace::branching::http::{
    BranchDescriptorResponse, BranchHealth, BranchLifecycle, BranchListResponse, BranchMode,
    BranchStatusDescriptor, BranchTargetIdentity,
};
use crate::namespace::branching::{BranchError, BranchLifecycleState};
use crate::namespace::manager::{
    CreateNamespaceOutcome, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
    COMPACTION_DEGRADED_FAILURE_THRESHOLD,
};
use crate::security::{
    Action, AllowDecision, AuditParams, AuthenticatedManifestArtifactInventory, Feature,
    IndexConfigValues, NamespaceDeleteAdmission, NamespaceForkAdmission, NamespaceId,
    PreservationBlockedSurface, Principal, RequestContext, SecurityError,
};
use crate::server::{
    authorize_namespace_action, namespace_graph, AppState, AuditRequest, RateLimitIdentity,
};
use crate::types::{DistanceMetric, IndexType};
use crate::wal::manifest::{NamedSnapshot, NamedSnapshotRef, SegmentRef};
use crate::wal::{FragmentCachePolicy, Manifest};

use super::{as_of, ApiError};

/// Maximum number of immutable source objects copied concurrently by one clone.
///
/// The bound limits object-store pressure and memory used by in-flight futures;
/// it does not limit the number of clone requests accepted by the server.
const CLONE_COPY_CONCURRENCY: usize = 16;

/// Reserved prefix for temporary snapshot pins that protect clone source data.
///
/// A random UUID suffix makes pins independent across concurrent clone
/// requests. The pin is an internal implementation detail and is released on
/// both the success and handled-failure paths.
const CLONE_INTERNAL_SNAPSHOT_PREFIX: &str = "__clone_";

/// Strict live-head fork request.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkRequest {
    /// Target namespace name.
    pub target: String,
}

/// Minimal public fork response; internal manifest details are intentionally omitted.
#[derive(Debug, Serialize)]
pub struct ForkResponse {
    /// Stable branch edge identifier.
    pub branch_id: String,
    /// Whether this request created the target.
    pub created: bool,
    /// Public branch mode.
    pub mode: BranchMode,
    /// Redacted source identity.
    pub source: ForkSourceIdentity,
    /// Redacted target identity.
    pub target: ForkTargetIdentity,
    /// Ancestry depth.
    pub depth: u16,
    /// Whether all artifacts are target-owned.
    pub materialized: bool,
    /// Reservation timestamp.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Redacted source identity returned by fork creation.
#[derive(Debug, Serialize)]
pub struct ForkSourceIdentity {
    /// Source namespace name.
    pub namespace: String,
    /// Source namespace incarnation.
    pub incarnation: String,
    /// Exact live-head generation selected by the fork.
    pub generation: u64,
}

/// Redacted target identity returned by fork creation.
#[derive(Debug, Serialize)]
pub struct ForkTargetIdentity {
    /// Target namespace name.
    pub namespace: String,
    /// Target namespace incarnation.
    pub incarnation: String,
    /// Prepared target manifest generation.
    pub generation: u64,
}

/// Create a live-head copy-on-write fork when branching is enabled.
#[instrument(skip(state, decision, principal, context, audit, rate_identity), fields(source = %source))]
pub async fn create_branch(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Extension(audit): Extension<AuditRequest>,
    Extension(rate_identity): Extension<RateLimitIdentity>,
    Path(source): Path<String>,
    Json(request): Json<ForkRequest>,
) -> Result<(StatusCode, Json<ForkResponse>), ApiError> {
    if !state.config.branching.enabled {
        return Err(ApiError(ZeppelinError::Branch(Box::new(
            BranchError::BranchingNotReady {
                feature: "namespace branching",
            },
        ))));
    }
    if !state.security.entitlements().has(Feature::Branching) {
        return Err(ApiError(
            SecurityError::FeatureNotLicensed(Feature::Branching).into(),
        ));
    }
    let source_id = NamespaceId::new(source).map_err(|e| ApiError(e.into()))?;
    let target_id = NamespaceId::new(request.target).map_err(|e| ApiError(e.into()))?;
    let source_read_decision = authorize_namespace_action(
        &state,
        &principal,
        &context,
        &audit,
        Action::NamespaceRead,
        source_id.as_str(),
    )
    .map_err(|error| ApiError(error.into()))?;
    let target_create_decision = authorize_namespace_action(
        &state,
        &principal,
        &context,
        &audit,
        Action::NamespaceCreate,
        target_id.as_str(),
    )
    .map_err(|error| ApiError(error.into()))?;
    require_unconstrained_clone_control(&decision, &source_read_decision, &target_create_decision)?;
    audit.set_params(AuditParams::NamespaceFork {
        source: source_id.clone(),
        target: target_id.clone(),
    });
    state
        .security
        .validate_namespace_copy_no_widening(decision.policy_version, &source_id, &target_id)
        .map_err(|error: SecurityError| ApiError(error.into()))?;
    let authorized = state
        .security
        .authorize_namespace_fork(NamespaceForkAdmission {
            source: source_id,
            target: target_id,
            principal,
            approver: audit.approval_principal(),
            context,
            fork_decision: decision,
            source_read_decision,
            target_create_decision,
            audit: state.audit.clone(),
            source_ip: rate_identity.ip,
            clock: state.clock.clone(),
        })
        .map_err(|error| ApiError(error.into()))?;
    let outcome = namespace_graph(&state)
        .fork(authorized)
        .await
        .map_err(ApiError::from)?;
    let created = outcome.created();
    let branch = outcome.branch();
    let (target_manifest, _) = Manifest::read_versioned_required_for_incarnation(
        &state.store,
        branch.identity.target_namespace.as_str(),
        branch.identity.target_incarnation.as_uuid(),
    )
    .await
    .map_err(ApiError::from)?;
    let materialized = target_manifest
        .visible_refs_are_local()
        .map_err(ApiError::from)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(ForkResponse {
            branch_id: branch.identity.branch_id.to_string(),
            created,
            mode: BranchMode::CopyOnWrite,
            source: ForkSourceIdentity {
                namespace: branch.identity.source_namespace.to_string(),
                incarnation: branch.identity.source_incarnation.to_string(),
                generation: branch.identity.source_generation.get(),
            },
            target: ForkTargetIdentity {
                namespace: branch.identity.target_namespace.to_string(),
                incarnation: branch.identity.target_incarnation.to_string(),
                generation: branch.identity.target_generation.get(),
            },
            depth: branch.identity.depth,
            materialized,
            created_at: branch.identity.created_at,
        }),
    ))
}

/// List direct branch roots for an enabled source namespace.
#[instrument(skip(state, principal, context), fields(source = %source))]
pub async fn list_branches(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Path(source): Path<String>,
) -> Result<Json<BranchListResponse>, ApiError> {
    if !state.config.branching.enabled {
        return Err(ApiError(ZeppelinError::Branch(Box::new(
            BranchError::BranchingNotReady {
                feature: "namespace branching",
            },
        ))));
    }
    // Secure route middleware already authorized NamespaceRead for the source.
    // Keep that decision request-local and use the principal only for each
    // target disclosure check; NamespaceRead does not mint an AuditRequest.
    let source_id = NamespaceId::new(source).map_err(|error| ApiError(error.into()))?;
    let request = state
        .security
        .authorize_branch_list(source_id, principal, context);
    let descriptors = namespace_graph(&state)
        .list_children(request)
        .await
        .map_err(ApiError::from)?;
    let mut branches = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let (lifecycle, health) = match descriptor.state {
            BranchLifecycleState::Preparing => (
                BranchLifecycle::Preparing,
                BranchHealth::AwaitingAuthenticatedRetry,
            ),
            BranchLifecycleState::Active => (BranchLifecycle::Active, BranchHealth::Ready),
            BranchLifecycleState::Deleting => {
                (BranchLifecycle::Deleting, BranchHealth::DeletionInProgress)
            }
        };
        branches.push(BranchDescriptorResponse {
            branch_id: descriptor.branch_id.to_string(),
            target: BranchTargetIdentity {
                namespace: descriptor.target.to_string(),
                incarnation: descriptor.target_incarnation.to_string(),
            },
            mode: BranchMode::CopyOnWrite,
            depth: descriptor.depth,
            lifecycle,
            health,
            materialized: descriptor.materialized,
            created_at: descriptor.created_at,
        });
    }
    branches.sort_by(|a, b| {
        a.target
            .namespace
            .cmp(&b.target.namespace)
            .then(a.branch_id.cmp(&b.branch_id))
    });
    Ok(Json(BranchListResponse { branches }))
}

/// JSON body that selects a namespace's immutable shape and index defaults.
///
/// Omitting `name` asks the server to generate a UUID-based name. Supplying a
/// name selects idempotent create-by-name semantics: an identical repeated
/// request returns the existing namespace, while a different immutable
/// configuration returns a conflict.
///
/// Deserialization supplies cosine distance and an empty FTS map when those
/// fields are omitted. [`create_namespace`] validates dimensions against the
/// server limit, then the namespace domain validates names, FTS analyzers, and
/// resolved index parameters.
///
/// # Examples
///
/// A body containing `{"name":"catalog","dimensions":384}` requests a
/// named cosine namespace with no full-text fields and server-derived index
/// settings.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Optional stable client name; `None` requests a generated UUID name.
    pub name: Option<String>,
    /// Number of floating-point components in every vector; must be in the
    /// inclusive range `1..=server.max_dimensions`.
    pub dimensions: usize,
    /// Namespace-wide vector distance metric; omitted JSON defaults to cosine.
    #[serde(default = "default_distance_metric")]
    pub distance_metric: DistanceMetric,
    /// Field-to-analyzer configuration; an omitted or empty map disables FTS.
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    /// Optional partial override resolved into a complete persisted build
    /// configuration for future compactions.
    #[serde(default)]
    pub index_config: Option<CreateNamespaceIndexConfig>,
}

/// Partial per-namespace index-build settings accepted at create or patch time.
///
/// Every field is optional so clients can override only the choices they care
/// about. Creation fills missing fields from the boot-time indexing defaults.
/// Patching fills missing fields from the namespace's current effective
/// configuration. The complete result is validated before metadata is written.
///
/// # Examples
///
/// `{"nlist":256,"quantization":"product","pq_m":8}` requests 256 IVF
/// clusters and eight product-quantization subspaces while inheriting the
/// remaining boolean index settings.
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceIndexConfig {
    /// Number of IVF centroids, and therefore coarse clusters; must be positive.
    #[serde(default)]
    pub nlist: Option<usize>,
    /// Compression mode used by newly built segment vectors.
    #[serde(default)]
    pub quantization: Option<QuantizationType>,
    /// Product-quantization subspace count; must be positive and divide vector
    /// dimensions when product quantization is selected.
    #[serde(default)]
    pub pq_m: Option<usize>,
    /// Whether future segment builds use hierarchical rather than flat IVF.
    #[serde(default)]
    pub hierarchical: Option<bool>,
    /// Whether future segment builds create full-text indexes for configured
    /// fields.
    #[serde(default)]
    pub fts_index: Option<bool>,
    /// Whether future segment builds create metadata bitmap indexes.
    #[serde(default)]
    pub bitmap_index: Option<bool>,
}

impl CreateNamespaceIndexConfig {
    /// Reports whether a patch omitted every index-config field.
    ///
    /// # Returns
    ///
    /// `true` only when all six options are `None`; explicit `false` values
    /// count as supplied settings.
    ///
    /// # Examples
    ///
    /// `{}` is empty and rejected by [`patch_index_config`], while
    /// `{"bitmap_index":false}` is a meaningful patch.
    fn is_empty(&self) -> bool {
        self.nlist.is_none()
            && self.quantization.is_none()
            && self.pq_m.is_none()
            && self.hierarchical.is_none()
            && self.fts_index.is_none()
            && self.bitmap_index.is_none()
    }
}

/// JSON patch that updates desired index settings for the next compaction.
///
/// This alias deliberately shares the optional-field wire shape used during
/// creation. [`patch_index_config`] rejects an all-omitted body and overlays
/// supplied fields on the current effective configuration.
pub type PatchNamespaceIndexConfigRequest = CreateNamespaceIndexConfig;

/// Supplies cosine distance when creation JSON omits `distance_metric`.
///
/// # Returns
///
/// [`DistanceMetric::Cosine`], with no allocation or shared-state access.
///
/// # Examples
///
/// Serde calls this helper while decoding `{"dimensions":384}`.
fn default_distance_metric() -> DistanceMetric {
    DistanceMetric::Cosine
}

/// Client-facing namespace metadata combined with manifest-derived live status.
///
/// Identity, lifecycle, desired index settings, and compaction health come from
/// [`NamespaceMetadata`]. Counts, storage estimates, and active-segment details
/// come from the supplied manifest. This separation matters: the metadata
/// record is not the visibility authority for WAL fragments or segments.
///
/// `approximate_storage_bytes` is the sum of sizes recorded in manifest
/// references, not a bucket inventory or billing value. An absent active
/// segment produces `null` quantization and a zero active-segment count.
///
/// # Examples
///
/// Immediately after creation, a response reports zero vectors, fragments,
/// segments, and bytes. After an upsert it can report one uncompacted fragment;
/// after compaction it instead reports an active segment and its quantization.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    /// Stable namespace identifier used in subsequent URL path segments.
    pub name: String,
    /// Number of components required in every stored and query vector.
    pub dimensions: usize,
    /// Namespace-wide metric used to compare query and stored vectors.
    pub distance_metric: DistanceMetric,
    /// Logical live vector count derived from manifest-visible artifacts.
    pub vector_count: u64,
    /// Number of manifest-visible WAL fragments not yet represented by a
    /// segment.
    pub uncompacted_fragments: usize,
    /// Number of immutable segment references in the current manifest.
    pub segment_count: usize,
    /// Lower-cost storage estimate from manifest object-size references; this
    /// performs no object listing or HEAD requests.
    pub approximate_storage_bytes: u64,
    /// Compression mode recorded by the active segment, or `null` when no
    /// active segment reference resolves.
    pub quantization: Option<QuantizationType>,
    /// Effective active index family: hierarchical when the active segment says
    /// so, otherwise the metadata index type.
    pub index_kind: IndexType,
    /// Complete desired settings for future compactions; legacy metadata
    /// without an override is resolved from current server defaults.
    pub index_config: NamespaceIndexConfig,
    /// Number of vectors represented by the active segment alone, excluding
    /// uncompacted WAL fragments.
    pub active_segment_vector_count: usize,
    /// RFC 3339 completion/failure time of the last recorded compaction, or
    /// `null` before any outcome is recorded.
    pub last_compaction_at: Option<String>,
    /// Stable status string: `never`, `success`, or `failure`.
    pub last_compaction_status: String,
    /// Last persisted compaction error, omitted from JSON unless present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_error: Option<String>,
    /// Number of consecutive failures since the most recent successful run.
    pub consecutive_compaction_failures: u32,
    /// Whether the failure count has reached
    /// [`COMPACTION_DEGRADED_FAILURE_THRESHOLD`].
    pub index_degraded: bool,
    /// RFC 3339 timestamp persisted when the namespace name was reserved.
    pub created_at: String,
    /// RFC 3339 timestamp of the latest namespace-metadata update.
    pub updated_at: String,
    /// Stable lifecycle state, currently `active` or `deleting`.
    pub state: String,
    /// Per-field lexical analyzer settings, omitted from JSON when FTS is not
    /// configured.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    /// Redacted target-local branch status, present only for branch targets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchStatusDescriptor>,
}

/// Acknowledges that namespace deletion entered or resumed its durable protocol.
///
/// The only current value is `"deleting"`. A `202` response does not mean all
/// objects are gone; clients can poll [`get_namespace`] until it returns 404.
#[derive(Debug, Serialize)]
pub struct DeleteNamespaceResponse {
    /// Lifecycle state persisted before background object cleanup begins.
    pub state: &'static str,
}

/// Identifies the active segment selected by an administrative hydration call.
///
/// The endpoint returns this body after a non-blocking request to the hydrator.
/// It does not wait for cache population, and the hydrator's bounded queue can
/// reject a job after this handler has selected the segment.
#[derive(Debug, Serialize)]
pub struct HydrateNamespaceResponse {
    /// Namespace whose current active segment was offered to the hydrator.
    pub namespace: String,
    /// Manifest-visible segment identifier selected for warm-set hydration.
    pub segment_id: String,
}

/// Acknowledges a persisted desired index-configuration update.
///
/// Metadata changes immediately, but immutable segments are not rewritten in
/// place. A later compaction builds a replacement using these settings.
#[derive(Debug, Serialize)]
pub struct UpdateIndexConfigResponse {
    /// Namespace whose authoritative metadata now carries the new settings.
    pub namespace: String,
    /// Fully resolved configuration persisted for the next segment build.
    pub index_config: NamespaceIndexConfig,
    /// Stable acknowledgment string, currently `"accepted"`.
    pub status: &'static str,
    /// Human-readable polling guidance for observing a later rewrite.
    pub observe: String,
}

/// Lightweight compaction readiness derived from one authoritative manifest.
///
/// `ready` means no WAL fragments are waiting for compaction. It does not
/// attest to compaction health, cache hydration, or the absence of older
/// retained manifest history.
///
/// # Examples
///
/// After one upsert and before compaction, `uncompacted_fragments` is commonly
/// one and `ready` is false. After publication it becomes zero and `ready` is
/// true with a newer `manifest_generation`.
#[derive(Debug, Serialize)]
pub struct CompactionStatusResponse {
    /// Namespace whose manifest supplied this snapshot.
    pub namespace: String,
    /// Monotonic generation of the strongly read live manifest.
    pub manifest_generation: u64,
    /// Count of manifest-visible WAL fragments not represented by a segment.
    pub uncompacted_fragments: usize,
    /// Count of immutable segment references in the live manifest.
    pub segment_count: usize,
    /// Active segment identifier when the manifest's pointer resolves.
    pub active_segment: Option<String>,
    /// Vectors in the active segment, or zero when none resolves.
    pub active_segment_vector_count: usize,
    /// `true` exactly when `uncompacted_fragments == 0`.
    pub ready: bool,
}

/// Result of evaluating one manual compaction trigger request.
///
/// `status` is `"noop"` with HTTP 200 when no fragments need work, or
/// `"accepted"` with HTTP 202 after a lease is acquired and a task is spawned.
/// The manifest fields describe the pre-trigger snapshot; accepted work is
/// observed later through [`get_compaction_status`].
#[derive(Debug, Serialize)]
pub struct CompactNamespaceResponse {
    /// Namespace evaluated for manual compaction.
    pub namespace: String,
    /// Stable trigger result: `"noop"` or `"accepted"`.
    pub status: &'static str,
    /// Manifest generation observed before returning the trigger response.
    pub manifest_generation: u64,
    /// Pre-trigger count of uncompacted manifest-visible fragments.
    pub uncompacted_fragments: usize,
    /// Pre-trigger count of manifest segment references.
    pub segment_count: usize,
    /// Pre-trigger active segment identifier, if one resolves.
    pub active_segment: Option<String>,
    /// Pre-trigger active-segment vector count.
    pub active_segment_vector_count: usize,
    /// Pre-trigger readiness flag.
    pub ready: bool,
}

/// Public metadata for one named point-in-time recovery pin.
///
/// The snapshot is a small immutable object that protects one retained manifest
/// generation. It does not duplicate vectors or segment data.
#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    /// Client-selected pin name within this namespace.
    pub name: String,
    /// Nonzero committed manifest generation protected from history pruning.
    pub generation: u64,
    /// RFC 3339 creation time retained across idempotent PUT retries.
    pub created_at: String,
}

/// Lexically ordered named snapshot pins for one namespace.
///
/// An empty `snapshots` array means the namespace has no named pins.
#[derive(Debug, Serialize)]
pub struct ListSnapshotsResponse {
    /// Snapshot metadata sorted by `name` by the manifest domain layer.
    pub snapshots: Vec<SnapshotResponse>,
}

/// JSON body selecting a fresh clone target and retained source point in time.
///
/// Unknown fields are rejected by Serde. `as_of` accepts the same generation,
/// RFC 3339 timestamp, or `snapshot:name` syntax as historical queries. The
/// target must differ from the source and must not already exist.
///
/// # Examples
///
/// `{"target":"catalog-restore","as_of":"snapshot:before-migration"}`
/// asks Zeppelin to copy the snapshot's reachable objects into a new writable
/// namespace.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneNamespaceRequest {
    /// Fresh target name to reserve using normal namespace-name validation.
    pub target: String,
    /// Retained source generation, RFC 3339 timestamp, or `snapshot:name`.
    pub as_of: String,
}

/// Describes a successfully materialized, independently writable clone.
///
/// The nested namespace status belongs to the target after its first manifest
/// publication. Source and target generation numbers differ because the target
/// starts its own history at generation one.
#[derive(Debug, Serialize)]
pub struct CloneNamespaceResponse {
    /// Active source namespace from which retained state was resolved.
    pub source: String,
    /// Newly created namespace that now owns copied artifact keys.
    pub target: String,
    /// Retained source manifest generation selected by `as_of`.
    pub generation: u64,
    /// Target manifest generation after publication, normally one.
    pub target_generation: u64,
    /// Stable materialization mode, currently `"copy"`.
    pub mode: &'static str,
    /// Target metadata combined with its newly published manifest.
    pub namespace: NamespaceResponse,
}

/// Namespace creation result with flattened metadata and client guidance.
///
/// Flattening makes fields such as `name` and `dimensions` top-level JSON
/// properties rather than nesting them under `namespace`.
#[derive(Debug, Serialize)]
pub struct CreateNamespaceResponse {
    /// Empty-manifest namespace status flattened into this response object.
    #[serde(flatten)]
    pub namespace: NamespaceResponse,
    /// Guidance that differs for generated names and idempotent client names.
    pub warning: String,
}

impl SnapshotResponse {
    /// Converts an addressable domain pin into its JSON representation.
    ///
    /// # Parameters
    ///
    /// - `snapshot`: Owned snapshot reference returned by the manifest layer.
    ///
    /// # Returns
    ///
    /// An owned response containing the name, generation, and RFC 3339 time.
    /// The storage key is intentionally not exposed.
    ///
    /// # Examples
    ///
    /// A domain reference for `daily` at generation 12 becomes
    /// `{"name":"daily","generation":12,...}` without its S3 key.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The parameter is moved, so its owned strings can be reused without
    /// cloning. Java would keep references valid through garbage collection; C
    /// would need a convention for which struct frees each allocation. Rust
    /// makes the ownership transfer explicit and prevents later reuse of the
    /// moved domain value.
    fn from_ref(snapshot: NamedSnapshotRef) -> Self {
        Self {
            name: snapshot.name,
            generation: snapshot.generation,
            created_at: snapshot.created_at.to_rfc3339(),
        }
    }
}

impl NamespaceResponse {
    /// Combines owned namespace metadata with a borrowed manifest status view.
    ///
    /// Metadata supplies stable configuration and lifecycle fields. The
    /// manifest supplies live vector/artifact counts and the active segment.
    /// Legacy metadata without `index_config` is presented using current server
    /// defaults; that compatibility behavior does not rewrite metadata.
    ///
    /// # Parameters
    ///
    /// - `meta`: Owned namespace metadata snapshot. Its strings and maps are
    ///   moved into the returned response.
    /// - `manifest`: Borrowed manifest that defines currently visible artifacts.
    /// - `default_indexing`: Borrowed server defaults used only when legacy
    ///   metadata has no persisted namespace index config.
    ///
    /// # Returns
    ///
    /// A fully owned JSON-ready response. If the manifest's active-segment ID
    /// is absent or does not match a segment reference, active-only fields are
    /// `None` or zero and `index_kind` falls back to metadata.
    ///
    /// # Consistency
    ///
    /// This conversion performs no I/O and cannot establish freshness itself.
    /// Callers that need current status must first obtain a strong manifest,
    /// as [`get_namespace`] does. Manifest-derived values never come from the
    /// metadata's legacy `vector_count` field.
    ///
    /// # Performance
    ///
    /// Scans segment references to resolve the active ID and scans fragments to
    /// derive uncompacted status. It allocates response strings and moves most
    /// metadata-owned fields without reading WAL or segment objects.
    ///
    /// # Examples
    ///
    /// Metadata for a 384-dimensional namespace plus a manifest containing one
    /// seven-vector WAL fragment yields `vector_count = 7`,
    /// `uncompacted_fragments = 1`, and `active_segment_vector_count = 0`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `meta` is consumed while `manifest` and `default_indexing` are shared
    /// borrows. This lets the response take ownership of metadata strings and
    /// maps without cloning them, while the compiler guarantees it cannot keep
    /// references into the borrowed manifest. In C terms, the borrows resemble
    /// non-null `const` pointers with checked lifetimes; Java has no direct
    /// equivalent of consuming the metadata value.
    ///
    /// # Errors
    ///
    /// Returns an artifact-origin validation error when an active branch's
    /// visible manifest cannot prove whether every live ref is target-owned.
    #[must_use]
    pub fn from_manifest(
        meta: NamespaceMetadata,
        manifest: &Manifest,
        default_indexing: &crate::config::IndexingConfig,
    ) -> Result<Self, ZeppelinError> {
        let index_kind = namespace_index_kind(&meta, manifest);
        let active_segment = active_segment_ref(manifest);
        let compaction_health = meta.compaction_health.clone();
        let materialized = if meta.branch_identity.is_some() && meta.state == NamespaceState::Active
        {
            manifest.visible_refs_are_local()?
        } else {
            false
        };
        let branch = meta
            .branch_identity
            .as_ref()
            .map(|identity| BranchStatusDescriptor {
                branch_id: identity.branch_id.to_string(),
                mode: BranchMode::CopyOnWrite,
                depth: identity.depth,
                lifecycle: match meta.state {
                    NamespaceState::Creating => BranchLifecycle::Preparing,
                    NamespaceState::Active => BranchLifecycle::Active,
                    NamespaceState::Deleting => BranchLifecycle::Deleting,
                },
                health: if matches!(meta.state, NamespaceState::Deleting) {
                    BranchHealth::DeletionInProgress
                } else {
                    BranchHealth::Ready
                },
                materialized,
                created_at: identity.created_at,
            });
        Ok(Self {
            name: meta.name,
            dimensions: meta.dimensions,
            distance_metric: meta.distance_metric,
            vector_count: manifest.vector_count(),
            uncompacted_fragments: manifest.uncompacted_fragments().len(),
            segment_count: manifest.segments.len(),
            approximate_storage_bytes: manifest.approximate_storage_bytes(),
            quantization: active_segment.map(|segment| segment.quantization),
            index_kind,
            index_config: meta
                .index_config
                .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(default_indexing)),
            active_segment_vector_count: active_segment.map_or(0, |segment| segment.vector_count),
            last_compaction_at: compaction_health
                .last_compaction_at
                .map(|timestamp| timestamp.to_rfc3339()),
            last_compaction_status: compaction_health
                .last_compaction_status
                .as_str()
                .to_string(),
            last_compaction_error: compaction_health.last_compaction_error,
            consecutive_compaction_failures: compaction_health.consecutive_failures,
            index_degraded: compaction_health.consecutive_failures
                >= COMPACTION_DEGRADED_FAILURE_THRESHOLD,
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.to_rfc3339(),
            state: meta.state.as_str().to_string(),
            full_text_search: meta.full_text_search,
            branch,
        })
    }
}

/// Creates or idempotently confirms a named pin for the current manifest.
///
/// The handler first requires an active namespace, strongly verifies the live
/// manifest, then asks [`NamedSnapshot::create`] to pin that committed
/// generation. Repeating the same name while the generation is unchanged
/// returns the original pin and timestamp, although the HTTP status remains
/// `201 Created`. Reusing the name after the live generation advances is a
/// conflict rather than silently retargeting the snapshot.
///
/// # Parameters
///
/// - `state`: Shared server services extracted from [`AppState`].
/// - `ns`: Namespace path segment; it must name an active namespace.
/// - `name`: Snapshot path segment validated by the manifest domain.
///
/// # Returns
///
/// HTTP 201 and the immutable pin's name, generation, and original creation
/// timestamp.
///
/// # Errors
///
/// Returns the canonical [`ApiError`] envelope for a missing namespace (404), a
/// deleting namespace (410), an invalid snapshot name or unavailable generation
/// (400), a name already bound to another generation (409), or storage and
/// decoding failures (500). A failed conditional pin creation does not
/// overwrite an existing pin.
///
/// # Side Effects
///
/// May refresh metadata and manifest caches, reads retained manifest history,
/// and conditionally writes one small snapshot object to S3/MinIO.
///
/// # Consistency
///
/// The strongly read live manifest selects the generation. The snapshot object
/// protects that generation from later history pruning, subject to the
/// history-check/publication race documented by [`NamedSnapshot::create`].
///
/// # Performance
///
/// Performs metadata lookup, one strong manifest verification, one history
/// GET, and one conditional pin PUT. An idempotent conflict path can add a pin
/// GET.
///
/// # Examples
///
/// PUT `.../snapshots/before-migration` at generation 12 creates a pin. A
/// repeated PUT at generation 12 returns the same timestamp. If an upsert has
/// advanced the live manifest to 13, the same PUT returns 409.
#[instrument(skip(state, decision), fields(namespace = %ns, snapshot = %name))]
pub async fn put_snapshot(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<(StatusCode, Json<SnapshotResponse>), ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let manifest = state
        .manifest_cache
        .get_strong_required(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;
    let snapshot = NamedSnapshot::create_at(
        &state.store,
        &ns,
        &name,
        manifest.version(),
        state.clock.now(),
    )
    .await
    .map_err(ApiError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(SnapshotResponse::from_ref(snapshot)),
    ))
}

/// Lists every named point-in-time pin for an active namespace.
///
/// The manifest domain decodes every pin and sorts the result lexically by
/// name. Corrupt or malformed pin objects fail the whole request; the handler
/// does not silently omit them.
///
/// # Parameters
///
/// - `state`: Shared server state and object-store gateway.
/// - `ns`: Namespace path segment whose pins should be enumerated.
///
/// # Returns
///
/// HTTP 200 with a lexically ordered array. An empty array means no named pins
/// exist.
///
/// # Errors
///
/// Returns 404 for a missing namespace, 410 for a deleting namespace, or the
/// mapped storage/key/decoding failure for the snapshot listing.
///
/// # Side Effects
///
/// May refresh the namespace metadata registry and performs read-only
/// object-store operations. It does not change retention.
///
/// # Performance
///
/// Performs one snapshot-prefix LIST, one full GET per pin, and an
/// `O(n log n)` in-memory sort.
///
/// # Examples
///
/// Pins named `weekly` and `daily` are returned as `daily`, then `weekly`,
/// regardless of S3 listing order.
#[instrument(skip(state, decision), fields(namespace = %ns))]
pub async fn list_snapshots(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
) -> Result<Json<ListSnapshotsResponse>, ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let snapshots = NamedSnapshot::list(&state.store, &ns)
        .await
        .map_err(ApiError::from)?
        .into_iter()
        .map(SnapshotResponse::from_ref)
        .collect();
    Ok(Json(ListSnapshotsResponse { snapshots }))
}

/// Returns one exact named point-in-time pin for an active namespace.
///
/// # Parameters
///
/// - `state`: Shared server state and storage gateway.
/// - `ns`: Namespace path segment that owns the pin.
/// - `name`: Exact snapshot name to validate and read.
///
/// # Returns
///
/// HTTP 200 with the pin's public name, generation, and RFC 3339 creation time.
///
/// # Errors
///
/// Returns 404 when either the namespace or valid snapshot name is absent, 410
/// for a deleting namespace, 400 for an invalid name, or a mapped storage or
/// decode error. Corrupt bytes are not treated as a missing pin.
///
/// # Side Effects
///
/// May refresh namespace metadata and performs one snapshot-object GET.
///
/// # Examples
///
/// GET `.../snapshots/daily` returns the generation protected by `daily`.
/// GET of a valid but unknown name returns `SNAPSHOT_NOT_FOUND` with HTTP 404.
#[instrument(skip(state, decision), fields(namespace = %ns, snapshot = %name))]
pub async fn get_snapshot(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let snapshot = NamedSnapshot::read(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(ZeppelinError::SnapshotNotFound {
                namespace: ns.clone(),
                name: name.clone(),
            })
        })?;
    Ok(Json(SnapshotResponse::from_ref(snapshot)))
}

/// Removes one named pin so its generation may later age out of retention.
///
/// The handler checks that the pin exists before deleting it. Deleting a pin
/// does not immediately remove manifest history or vector artifacts; a later
/// pruning and GC cycle decides when unreferenced immutable data is safe to
/// delete.
///
/// # Parameters
///
/// - `state`: Shared server state and storage gateway.
/// - `ns`: Active namespace that owns the pin.
/// - `name`: Exact snapshot name to validate, check, and delete.
///
/// # Returns
///
/// HTTP 204 with no body after the pin DELETE succeeds.
///
/// # Errors
///
/// Returns 404 for a missing namespace or snapshot, 410 for a deleting
/// namespace, 400 for an invalid name, or a mapped storage failure. This API is
/// not idempotent at the HTTP contract: deleting an already absent pin returns
/// 404 rather than 204.
///
/// # Side Effects
///
/// Performs a pin GET followed by a DELETE of the small snapshot object.
/// Retained data remains untouched by this handler.
///
/// # Consistency
///
/// The existence check and DELETE are separate object-store requests. A
/// concurrent deletion can race between them; the low-level delete result is
/// returned according to backend semantics.
///
/// # Examples
///
/// Removing `before-migration` returns 204. A later GET returns 404, while the
/// previously pinned generation can remain readable until retention pruning.
#[instrument(skip(state, decision, audit), fields(namespace = %ns, snapshot = %name))]
pub async fn delete_snapshot(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(audit): Extension<AuditRequest>,
    Path((ns, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    let namespace_id = NamespaceId::new(ns.clone()).map_err(|error| ApiError(error.into()))?;
    let guard = state
        .security
        .guard_namespace_destruction(&namespace_id)
        .map_err(|error| ApiError(error.into()))?;
    if guard.is_locked() {
        audit.set_params(AuditParams::preservation_blocked(
            PreservationBlockedSurface::SnapshotDelete,
            &guard,
        ));
        return Err(ApiError(SecurityError::PreservationLocked.into()));
    }
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    if NamedSnapshot::read(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?
        .is_none()
    {
        return Err(ApiError(ZeppelinError::SnapshotNotFound {
            namespace: ns,
            name,
        }));
    }
    NamedSnapshot::delete(&state.store, &ns, &name)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

impl CompactNamespaceResponse {
    /// Adds a trigger outcome to an already derived compaction-status snapshot.
    ///
    /// # Parameters
    ///
    /// - `status`: Process-long stable outcome string chosen by the handler.
    /// - `response`: Owned pre-trigger manifest status whose fields are moved
    ///   into the trigger response.
    ///
    /// # Returns
    ///
    /// A response with identical manifest observations plus `status`.
    ///
    /// # Examples
    ///
    /// A ready status snapshot combined with `"noop"` becomes the body of the
    /// HTTP 200 no-work response.
    fn from_status(status: &'static str, response: CompactionStatusResponse) -> Self {
        Self {
            namespace: response.namespace,
            status,
            manifest_generation: response.manifest_generation,
            uncompacted_fragments: response.uncompacted_fragments,
            segment_count: response.segment_count,
            active_segment: response.active_segment,
            active_segment_vector_count: response.active_segment_vector_count,
            ready: response.ready,
        }
    }
}

/// Creates a generated namespace or idempotently creates a client-named one.
///
/// The handler validates the server-wide dimension bound, resolves a complete
/// per-namespace index configuration, and delegates persistence to
/// [`crate::namespace::manager::NamespaceManager`]. A supplied name uses
/// idempotent comparison against fresh S3 metadata. An omitted name generates a
/// UUID (optionally prefixed by the test server) and uses create-only semantics.
///
/// # Parameters
///
/// - `state`: Shared server services, limits, defaults, and optional generated
///   name prefix.
/// - `req`: Owned, deserialized creation request. Axum rejects malformed JSON
///   before this function runs.
///
/// # Returns
///
/// Returns HTTP 201 for a newly created namespace. An identical retry with a
/// client-specified name returns HTTP 200 and the original metadata. The
/// response always uses an empty manifest view, so an idempotent retry against
/// a namespace that later acquired data still reports zero live counts; clients
/// should use [`get_namespace`] for current manifest statistics.
///
/// # Errors
///
/// Returns 400 for dimensions outside `1..=max_dimensions`, an unsafe name,
/// invalid FTS settings, or incompatible index parameters. Returns 409 when a
/// client name exists with different immutable settings and 410 while that name
/// is deleting. Serialization and object-store failures map to server errors.
///
/// Namespace creation writes metadata before the initial manifest. If the
/// manifest write fails, `meta.json` may remain even though the HTTP operation
/// returned an error; this handler does not report a rollback that did not
/// occur.
///
/// # Side Effects
///
/// A new namespace conditionally writes metadata, writes generation-one empty
/// manifest state/history, refreshes the process-local namespace registry, and
/// emits structured logs. The matching-existing path reads authoritative
/// metadata and does not rewrite it.
///
/// # Consistency
///
/// S3's create-if-absent metadata write owns name uniqueness. The local
/// registry is a cache, not the reservation authority. The idempotent path
/// compares dimensions, metric, FTS, and resolved index settings rather than
/// silently changing an existing namespace.
///
/// # Performance
///
/// Successful creation performs two sequential object-store writes. A named
/// conflict can add a metadata GET to classify the existing namespace. No WAL
/// or segment data is read for the response.
///
/// # Examples
///
/// POST `{"name":"catalog","dimensions":384}` returns 201 the first time
/// and 200 for an identical retry. Repeating it with 768 dimensions returns
/// `NAMESPACE_ALREADY_EXISTS` rather than changing the stored vector shape.
///
/// # Rust Notes for Java/C Engineers
///
/// Destructuring `Json(req)` yields an owned request. Moving its FTS map and
/// resolved config into the async manager call avoids copies and prevents the
/// persisted metadata from borrowing request memory. The `match` on
/// [`CreateNamespaceOutcome`] is exhaustive: adding a new domain outcome forces
/// this handler to decide its HTTP meaning at compile time.
#[instrument(skip(state, _decision, principal, context, audit), fields(dimensions = req.dimensions))]
pub async fn create_namespace(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Extension(audit): Extension<AuditRequest>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<CreateNamespaceResponse>), ApiError> {
    if req.dimensions == 0 || req.dimensions > state.config.server.max_dimensions {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "dimensions {} must be between 1 and {}",
            req.dimensions, state.config.server.max_dimensions
        ))));
    }
    let index_config = resolve_namespace_index_config(
        req.index_config.as_ref(),
        &state.config.indexing,
        req.dimensions,
    )
    .map_err(ApiError::from)?;

    if let Some(name) = req.name {
        let target_decision = crate::server::authorize_namespace_action(
            &state,
            &principal,
            &context,
            &audit,
            Action::NamespaceCreate,
            &name,
        )
        .map_err(ZeppelinError::from)
        .map_err(ApiError::from)?;
        require_unconstrained_namespace_operation(&target_decision)?;
        audit.set_params(AuditParams::NamespaceCreate {
            namespace: NamespaceId::new(name.clone()).map_err(ZeppelinError::from)?,
        });
        info!(namespace = %name, dimensions = req.dimensions, "creating namespace by client name");
        let outcome = state
            .namespace_manager
            .create_idempotent_with_fts_and_index_config(
                &name,
                req.dimensions,
                req.distance_metric,
                req.full_text_search,
                Some(index_config),
            )
            .await
            .map_err(ApiError::from)?;
        let (status, meta) = match outcome {
            CreateNamespaceOutcome::Created(meta) => {
                info!(namespace = %name, "namespace created by client name");
                (StatusCode::CREATED, meta)
            }
            CreateNamespaceOutcome::Existing(meta) => {
                info!(namespace = %name, "namespace already existed with matching config");
                (StatusCode::OK, meta)
            }
        };
        return Ok((
            status,
            Json(CreateNamespaceResponse {
                namespace: NamespaceResponse::from_manifest(
                    meta,
                    &Manifest::new_at(state.clock.now()),
                    &state.config.indexing,
                )
                .map_err(ApiError::from)?,
                warning:
                    "Client-specified namespace names are idempotent for identical configuration."
                        .to_string(),
            }),
        ));
    }

    let name = generated_namespace_name(state.namespace_name_prefix.as_deref());
    let target_decision = crate::server::authorize_namespace_action(
        &state,
        &principal,
        &context,
        &audit,
        Action::NamespaceCreate,
        &name,
    )
    .map_err(ZeppelinError::from)
    .map_err(ApiError::from)?;
    require_unconstrained_namespace_operation(&target_decision)?;
    audit.set_params(AuditParams::NamespaceCreate {
        namespace: NamespaceId::new(name.clone()).map_err(ZeppelinError::from)?,
    });
    info!(namespace = %name, dimensions = req.dimensions, "creating generated namespace");
    let meta = state
        .namespace_manager
        .create_with_fts_and_index_config(
            &name,
            req.dimensions,
            req.distance_metric,
            req.full_text_search,
            Some(index_config),
        )
        .await
        .map_err(ApiError::from)?;

    info!(namespace = %name, "generated namespace created");
    Ok((
        StatusCode::CREATED,
        Json(CreateNamespaceResponse {
            namespace: NamespaceResponse::from_manifest(
                meta,
                &Manifest::new_at(state.clock.now()),
                &state.config.indexing,
            )
            .map_err(ApiError::from)?,
            warning: "Save this namespace name. It cannot be recovered if lost.".to_string(),
        }),
    ))
}

/// Materializes retained source state as a fresh, independently writable clone.
///
/// This operation resolves a historical manifest, pins it against pruning,
/// creates target metadata, copies every immutable object reachable from that
/// manifest, rewrites source-prefixed stored keys, and publishes a new target
/// manifest history beginning at generation one. It copies data rather than
/// sharing source keys, so deleting the source cannot break the successful
/// target.
///
/// # Parameters
///
/// - `state`: Shared namespace, manifest, storage, and cache services.
/// - `source`: Active source namespace from the URL path.
/// - `req`: Owned target name and generation/timestamp/snapshot selector.
///
/// # Returns
///
/// HTTP 201 with the selected source generation, newly published target
/// generation, `"copy"` mode, and target namespace status.
///
/// # Errors
///
/// Returns 400 when source and target names match or an input is invalid, 404
/// for a missing source, 410 for a deleting source or point in time no longer
/// retained, and 409 when the target already exists. Snapshot pin, copy,
/// manifest publication, serialization, and storage failures also propagate.
///
/// Once target creation succeeds, a later copy or publication failure retains
/// the target and invalidates its local manifest cache entry. A concurrently
/// acknowledged target write may already have advanced the bootstrap manifest;
/// deleting the namespace after a clone failure would destroy that write. The
/// original failure is returned and the retained target is reported in a
/// structured warning so an administrator can inspect or delete it explicitly.
///
/// # Side Effects
///
/// Creates a temporary source snapshot, creates target metadata and an initial
/// empty manifest, performs bounded-concurrency server-side object copies,
/// publishes the rewritten target manifest, writes it through to the target
/// manifest cache, and deletes the temporary pin. Failed attempts after target
/// activation retain the target and may leave unreachable copied objects.
///
/// # Consistency
///
/// The internal pin keeps the selected source generation rooted while copies
/// run. Destination copies use copy-if-absent so pre-existing target artifacts
/// are conflicts, not overwritten data. Target manifest publication is the
/// visibility boundary: copied objects are not query-visible until it succeeds.
/// The target must be fresh, so this endpoint is not idempotent after success;
/// retrying the same completed clone returns a target-name conflict.
///
/// # Performance
///
/// Resolving a point in time may list/read manifest history. The clone performs
/// one server-side copy per manifest-reachable WAL or segment artifact, with at
/// most `CLONE_COPY_CONCURRENCY` in flight, then writes the target manifest.
/// Cost scales with the selected manifest's reachable object set and copies
/// full object contents inside the configured store.
///
/// # Examples
///
/// Cloning `catalog` at `snapshot:before-migration` to `catalog-restore` copies
/// only artifacts reachable at the pinned generation. Later writes to either
/// namespace and deletion of the source do not alter the other namespace.
///
/// If one object copy fails after target activation, the response carries the
/// storage error and the target remains reserved. This fail-closed behavior
/// prevents cleanup from racing and deleting an acknowledged target write.
///
/// # Rust Notes for Java/C Engineers
///
/// The request strings move into local owned values. Cloned store/metadata
/// fields are deliberate owned snapshots needed across `.await` points. The
/// cleanup branches use `match` rather than exceptions: success and each failure
/// path must explicitly release the temporary pin. Java would commonly encode
/// this with `try/finally`; C would use cleanup labels. Rust has RAII for local
/// memory, but remote S3 objects still require explicit asynchronous cleanup.
#[instrument(skip(state, clone_decision, principal, context, audit, req), fields(source = %source))]
pub async fn clone_namespace(
    State(state): State<AppState>,
    Extension(clone_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Extension(audit): Extension<AuditRequest>,
    Path(source): Path<String>,
    Json(req): Json<CloneNamespaceRequest>,
) -> Result<(StatusCode, Json<CloneNamespaceResponse>), ApiError> {
    let target = req.target;
    let as_of = req.as_of;
    if target == source {
        return Err(ApiError(ZeppelinError::Validation(
            "clone target must differ from source namespace".into(),
        )));
    }
    let source_read_decision = crate::server::authorize_namespace_action(
        &state,
        &principal,
        &context,
        &audit,
        Action::NamespaceRead,
        &source,
    )
    .map_err(ZeppelinError::from)
    .map_err(ApiError::from)?;
    let target_create_decision = crate::server::authorize_namespace_action(
        &state,
        &principal,
        &context,
        &audit,
        Action::NamespaceCreate,
        &target,
    )
    .map_err(ZeppelinError::from)
    .map_err(ApiError::from)?;
    require_unconstrained_clone_control(
        &clone_decision,
        &source_read_decision,
        &target_create_decision,
    )?;
    let source_id = NamespaceId::new(source.clone()).map_err(ZeppelinError::from)?;
    let target_id = NamespaceId::new(target.clone()).map_err(ZeppelinError::from)?;
    state
        .security
        .validate_namespace_copy_no_widening(clone_decision.policy_version, &source_id, &target_id)
        .map_err(ZeppelinError::from)
        .map_err(ApiError::from)?;
    audit.set_params(AuditParams::NamespaceClone {
        source: source_id.clone(),
        target: target_id.clone(),
    });

    let source_meta = state
        .namespace_manager
        .get(&source)
        .await
        .map_err(ApiError::from)?;
    let source_manifest = as_of::resolve_manifest(&state.store, &source, &as_of)
        .await
        .map_err(ApiError::from)?;
    let source_generation = source_manifest.version();
    let clone_pin_name = internal_clone_pin_name();
    NamedSnapshot::create_at(
        &state.store,
        &source,
        &clone_pin_name,
        source_generation,
        state.clock.now(),
    )
    .await
    .map_err(ApiError::from)?;

    let index_config = source_meta
        .index_config
        .clone()
        .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(&state.config.indexing));

    let target_meta = match state
        .namespace_manager
        .create_with_fts_and_index_config(
            &target,
            source_meta.dimensions,
            source_meta.distance_metric,
            source_meta.full_text_search.clone(),
            Some(index_config),
        )
        .await
    {
        Ok(meta) => meta,
        Err(e) => {
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(e));
        }
    };
    let target_incarnation = match target_meta.incarnation_id.as_ref() {
        Some(incarnation) => incarnation.as_uuid(),
        None => {
            retain_failed_clone_target(&state, &target, "missing namespace incarnation");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError(ZeppelinError::Serialization(format!(
                "clone target {target} is missing its namespace incarnation"
            ))));
        }
    };
    let (target_base_manifest, target_base_version) =
        match Manifest::read_versioned_required_for_incarnation(
            &state.store,
            &target,
            target_incarnation,
        )
        .await
        {
            Ok(base) => base,
            Err(e) => {
                retain_failed_clone_target(&state, &target, "bootstrap manifest read failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(e));
            }
        };
    if let Err(error) = target_base_manifest.require_empty_clone_base(&target, target_incarnation) {
        retain_failed_clone_target(&state, &target, "clone target changed before base capture");
        release_internal_clone_pin(&state, &source, &clone_pin_name).await;
        return Err(ApiError::from(error));
    }

    let source_has_foreign_artifacts = match source_manifest.has_foreign_visible_artifacts() {
        Ok(value) => value,
        Err(error) => {
            retain_failed_clone_target(&state, &target, "source origin validation failed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(error));
        }
    };
    let target_manifest = if source_has_foreign_artifacts {
        let signing_enabled = match state.store.object_signer_node() {
            Ok(signer) => signer.is_some(),
            Err(error) => {
                retain_failed_clone_target(&state, &target, "object signer lookup failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        };
        let authenticated_source_inventory = if signing_enabled {
            match AuthenticatedManifestArtifactInventory::authenticate(
                &state.store,
                &source,
                &source_manifest,
            )
            .await
            {
                Ok(inventory) => Some(inventory),
                Err(error) => {
                    retain_failed_clone_target(
                        &state,
                        &target,
                        "source receipt authentication failed",
                    );
                    release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                    return Err(ApiError::from(map_clone_source_integrity_error(error)));
                }
            }
        } else {
            None
        };
        let target_origin = match target_base_manifest.local_origin() {
            Ok(origin) => origin,
            Err(error) => {
                retain_failed_clone_target(&state, &target, "target origin validation failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        };
        let unpublished = match state
            .compactor
            .build_unpublished_owned_segment(
                &source,
                &source_manifest,
                &target_origin,
                authenticated_source_inventory.as_ref(),
            )
            .await
        {
            Ok(candidate) => candidate,
            Err(error) => {
                retain_failed_clone_target(&state, &target, "owned segment build failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        };

        let mut manifest = target_base_manifest.clone();
        let publication_guard = unpublished
            .map(|candidate| candidate.install_into_at(&mut manifest, state.clock.now()));
        if signing_enabled && manifest.receipt_upgrade_needed(&target) {
            if let Err(error) = manifest
                .hydrate_receipt_artifacts(&state.store, &target)
                .await
            {
                retain_failed_clone_target(&state, &target, "receipt inventory failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        }
        if let Err(error) = state.security.validate_namespace_copy_no_widening(
            clone_decision.policy_version,
            &source_id,
            &target_id,
        ) {
            retain_failed_clone_target(&state, &target, "authorization proof changed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError(ZeppelinError::from(error)));
        }
        let visible_refs_are_local = match manifest.visible_refs_are_local() {
            Ok(local) => local,
            Err(error) => {
                retain_failed_clone_target(&state, &target, "owned manifest origin check failed");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        };
        if !visible_refs_are_local
            || manifest.branch_lineage().is_some()
            || !manifest.branch_roots().is_empty()
            || !manifest.pending_deletes.is_empty()
        {
            retain_failed_clone_target(&state, &target, "owned manifest validation failed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError(ZeppelinError::Validation(format!(
                "owned clone target {target} retained branch control or cleanup state"
            ))));
        }
        if let Some(guard) = publication_guard.as_ref() {
            if let Err(error) = guard.require_current() {
                retain_failed_clone_target(&state, &target, "owned segment publication expired");
                release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                return Err(ApiError::from(error));
            }
        }
        if let Err(error) = manifest
            .write_conditional(&state.store, &target, &target_base_version)
            .await
        {
            retain_failed_clone_target(&state, &target, "conditional manifest publication failed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(error));
        }
        manifest
    } else {
        let mut manifest =
            match materialize_clone_manifest(&state, &source, &target, source_manifest).await {
                Ok(manifest) => manifest,
                Err(error) => {
                    retain_failed_clone_target(&state, &target, "artifact materialization failed");
                    release_internal_clone_pin(&state, &source, &clone_pin_name).await;
                    return Err(ApiError::from(error));
                }
            };
        if let Err(error) =
            manifest.prepare_clone_publication(&target, target_incarnation, &target_base_manifest)
        {
            retain_failed_clone_target(&state, &target, "manifest preparation failed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(error));
        }
        if let Err(error) = state.security.validate_namespace_copy_no_widening(
            clone_decision.policy_version,
            &source_id,
            &target_id,
        ) {
            retain_failed_clone_target(&state, &target, "authorization proof changed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError(ZeppelinError::from(error)));
        }
        if let Err(error) = manifest
            .write_conditional(&state.store, &target, &target_base_version)
            .await
        {
            retain_failed_clone_target(&state, &target, "conditional manifest publication failed");
            release_internal_clone_pin(&state, &source, &clone_pin_name).await;
            return Err(ApiError::from(error));
        }
        manifest
    };
    state
        .manifest_cache
        .insert(&target, target_manifest.clone());
    release_internal_clone_pin(&state, &source, &clone_pin_name).await;

    info!(
        source = %source,
        target = %target,
        source_generation,
        target_generation = target_manifest.version(),
        mode = "copy",
        "namespace clone materialized"
    );

    Ok((
        StatusCode::CREATED,
        Json(CloneNamespaceResponse {
            source,
            target: target.clone(),
            generation: source_generation,
            target_generation: target_manifest.version(),
            mode: "copy",
            namespace: NamespaceResponse::from_manifest(
                target_meta,
                &target_manifest,
                &state.config.indexing,
            )
            .map_err(ApiError::from)?,
        }),
    ))
}

/// Require all control-plane decisions for a raw copy to be unconstrained.
///
/// Clone copies immutable artifacts byte-for-byte and returns namespace-wide
/// aggregates. It cannot honor row filters, field masks, or write stamps by
/// partially copying or fabricating response values, so each independently
/// authorized source/target control action must be unconstrained and from the
/// same authoritative policy version.
fn require_unconstrained_clone_control(
    clone_decision: &AllowDecision,
    source_read_decision: &AllowDecision,
    target_create_decision: &AllowDecision,
) -> Result<(), ApiError> {
    if clone_decision.policy_version != source_read_decision.policy_version
        || clone_decision.policy_version != target_create_decision.policy_version
    {
        return Err(ApiError(
            crate::security::SecurityError::ConstraintViolation.into(),
        ));
    }
    for decision in [clone_decision, source_read_decision, target_create_decision] {
        require_unconstrained_namespace_operation(decision)?;
    }
    Ok(())
}

/// Generates a collision-resistant name in the reserved clone-pin namespace.
///
/// # Returns
///
/// An owned string beginning with `__clone_` followed by a simple UUID. It is
/// valid under snapshot-name rules and carries no source or target user data.
///
/// # Examples
///
/// A result has the shape `__clone_550e8400e29b41d4a716446655440000`.
fn internal_clone_pin_name() -> String {
    format!(
        "{CLONE_INTERNAL_SNAPSHOT_PREFIX}{}",
        Uuid::new_v4().simple()
    )
}

/// Best-effort releases a temporary source snapshot after clone processing.
///
/// Cleanup must not hide the clone operation's primary success or failure.
/// Missing pins are accepted as already released; other failures are logged so
/// maintenance can diagnose an unexpectedly retained internal pin.
///
/// # Parameters
///
/// - `state`: Borrowed server state whose store owns the pin.
/// - `source`: Source namespace that contains the temporary snapshot.
/// - `name`: Exact generated internal pin name.
///
/// # Side Effects
///
/// Performs one snapshot-object DELETE and may emit a warning. It does not
/// prune manifest history or data artifacts directly.
///
/// # Examples
///
/// After target publication, deleting `__clone_<uuid>` lets normal retention
/// eventually prune the source generation. If the pin is already absent,
/// cleanup completes silently.
async fn release_internal_clone_pin(state: &AppState, source: &str, name: &str) {
    match NamedSnapshot::delete(&state.store, source, name).await {
        Ok(()) | Err(ZeppelinError::NotFound { .. }) => {}
        Err(e) => warn!(
            source,
            snapshot = name,
            error = %e,
            "failed to release temporary clone snapshot pin"
        ),
    }
}

/// Retains an activated clone target after a later clone failure.
///
/// Target activation makes the namespace independently writable before the
/// clone's final manifest CAS. A concurrent request can therefore acknowledge a
/// write while artifact copying is still in flight. No read-then-delete proof
/// can exclude a write in the gap before destructive cleanup, so failure paths
/// invalidate only disposable cache state and leave S3 state authoritative.
///
/// # Parameters
///
/// - `state`: Borrowed services used for cache invalidation and reporting.
/// - `target`: Activated target namespace whose durable state is retained.
/// - `stage`: Static clone stage that failed after activation.
///
/// # Side Effects
///
/// Invalidates disposable manifest cache state and emits a structured warning.
/// It performs no object-store mutation.
///
/// # Performance
///
/// Performs no network or object-store work.
///
/// # Examples
///
/// If the seventh object copy fails, this helper makes subsequent reads miss
/// process-local cache state while preserving any concurrent target write.
fn retain_failed_clone_target(state: &AppState, target: &str, stage: &'static str) {
    state
        .manifest_cache
        .invalidate_at(target, state.clock.now());
    warn!(
        target,
        stage,
        "clone failed after target activation; target retained to preserve concurrent writes"
    );
}

/// Redacts authenticated source divergence behind the branch integrity contract.
fn map_clone_source_integrity_error(error: ZeppelinError) -> ZeppelinError {
    match error {
        error @ (ZeppelinError::Storage(_) | ZeppelinError::Io(_)) => error,
        _ => BranchError::BranchIntegrity.into(),
    }
}

/// Resolves creation overrides against complete server indexing defaults.
///
/// # Parameters
///
/// - `request`: Optional borrowed partial creation config. `None` keeps every
///   server default.
/// - `defaults`: Borrowed boot-time indexing configuration.
/// - `dimensions`: Namespace vector width used to validate product
///   quantization.
///
/// # Returns
///
/// A complete owned [`NamespaceIndexConfig`] suitable for persistence.
///
/// # Errors
///
/// Returns validation errors when `nlist` or `pq_m` is zero, or when product
/// quantization's `pq_m` does not divide `dimensions`. No state is changed.
///
/// # Examples
///
/// With server `nlist = 128`, a request containing only `{"nlist":256}`
/// returns 256 plus all other fields copied from the defaults.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<&T>` expresses “a partial request may be absent” without null
/// pointers. Each inner `Option` is exhaustively tested; scalar and enum values
/// used here are `Copy`, so reading them from a shared borrow does not move the
/// request. Java would typically use nullable boxed fields; C would pair each
/// field with a presence flag.
fn resolve_namespace_index_config(
    request: Option<&CreateNamespaceIndexConfig>,
    defaults: &crate::config::IndexingConfig,
    dimensions: usize,
) -> Result<NamespaceIndexConfig, ZeppelinError> {
    let mut config = NamespaceIndexConfig::from_indexing_config(defaults);
    if let Some(request) = request {
        if let Some(nlist) = request.nlist {
            config.nlist = nlist;
        }
        if let Some(quantization) = request.quantization {
            config.quantization = quantization;
        }
        if let Some(pq_m) = request.pq_m {
            config.pq_m = pq_m;
        }
        if let Some(hierarchical) = request.hierarchical {
            config.hierarchical = hierarchical;
        }
        if let Some(fts_index) = request.fts_index {
            config.fts_index = fts_index;
        }
        if let Some(bitmap_index) = request.bitmap_index {
            config.bitmap_index = bitmap_index;
        }
    }
    config.validate(dimensions)?;
    Ok(config)
}

/// Overlays supplied patch fields on a complete current configuration.
///
/// # Parameters
///
/// - `config`: Owned effective configuration to update in memory.
/// - `request`: Borrowed optional-field patch; omitted fields preserve `config`.
/// - `dimensions`: Namespace vector width used for final validation.
///
/// # Returns
///
/// The fully resolved and validated replacement configuration.
///
/// # Errors
///
/// Returns the same index-parameter validation errors as creation. Validation
/// occurs before the caller performs any metadata write.
///
/// # Examples
///
/// Applying `{"bitmap_index":false}` to a config with `nlist = 128` keeps
/// `nlist` unchanged and disables bitmaps for future segment builds.
fn apply_namespace_index_config_patch(
    mut config: NamespaceIndexConfig,
    request: &PatchNamespaceIndexConfigRequest,
    dimensions: usize,
) -> Result<NamespaceIndexConfig, ZeppelinError> {
    if let Some(nlist) = request.nlist {
        config.nlist = nlist;
    }
    if let Some(quantization) = request.quantization {
        config.quantization = quantization;
    }
    if let Some(pq_m) = request.pq_m {
        config.pq_m = pq_m;
    }
    if let Some(hierarchical) = request.hierarchical {
        config.hierarchical = hierarchical;
    }
    if let Some(fts_index) = request.fts_index {
        config.fts_index = fts_index;
    }
    if let Some(bitmap_index) = request.bitmap_index {
        config.bitmap_index = bitmap_index;
    }
    config.validate(dimensions)?;
    Ok(config)
}

/// Builds a unique server-generated namespace name.
///
/// # Parameters
///
/// - `prefix`: Optional borrowed test/harness prefix. Production normally
///   passes `None`.
///
/// # Returns
///
/// A UUID string, or `<prefix>-<uuid>` when a prefix is configured. The
/// namespace manager still performs authoritative name validation and
/// create-if-absent reservation.
///
/// # Examples
///
/// Production may return `550e8400-e29b-41d4-a716-446655440000`; a test prefix
/// `run-7` yields `run-7-<uuid>` so cleanup can scope its object keys.
fn generated_namespace_name(prefix: Option<&str>) -> String {
    let uuid = Uuid::new_v4().to_string();
    match prefix {
        Some(prefix) => format!("{prefix}-{uuid}"),
        None => uuid,
    }
}

/// Copies one retained manifest's artifacts and prepares it for target publish.
///
/// This helper deliberately stops before writing the target manifest. It drops
/// source pending-delete bookkeeping, computes the live reachable artifact
/// set, rewrites explicit stored keys, clears the source fencing token, assigns
/// a fresh timestamp, resets generation to zero, and copies each target object
/// with a create-only operation.
///
/// ```text
/// owned source manifest
///         |
///         +--> clear pending deletes and rewrite explicit keys
///         +--> reset fencing token, timestamp, and generation
///         |
///         v
/// copy reachable source objects -- any failure --> partial target copies
///         |
///         v
/// return unpublished target manifest (caller writes visibility root)
/// ```
///
/// # Parameters
///
/// - `state`: Borrowed server state whose store performs server-side copies.
/// - `source`: Source namespace prefix expected on every reachable key.
/// - `target`: Fresh target namespace prefix for copied objects.
/// - `manifest`: Owned retained source manifest to transform in place.
///
/// # Returns
///
/// The rewritten, generation-zero manifest after every object copy succeeds.
/// It is not visible in the target until the caller writes it.
///
/// # Errors
///
/// Returns an index error if a manifest key escapes the source prefix, or the
/// first copy/storage error observed. Some copies may already exist under the
/// target prefix; after target activation [`clone_namespace`] retains them to
/// avoid destructively racing a concurrent target write.
///
/// # Side Effects
///
/// Performs one copy-if-absent operation per reachable artifact. It does not
/// write metadata, publish a manifest, update caches, or release the source pin.
///
/// # Consistency
///
/// The caller must hold a named source pin while this function runs. Clearing
/// `pending_deletes` prevents source GC backlog from becoming target cleanup
/// intent. Copy-if-absent prevents overwriting any destination object.
///
/// # Performance
///
/// At most [`CLONE_COPY_CONCURRENCY`] copies are polled simultaneously. The
/// ordered map makes planning deterministic, but completion order is not.
/// Memory is linear in the number of reachable object keys.
///
/// # Examples
///
/// A source manifest at generation 42 with one WAL fragment and one segment is
/// transformed into generation zero with target-prefixed explicit references.
/// After copies finish, the caller adopts the target bootstrap generation and
/// conditionally publishes its successor.
///
/// # Rust Notes for Java/C Engineers
///
/// `manifest` is moved in and returned as a new owned value. The iterator builds
/// owned async closures for each copy; cloning `ZeppelinStore` shares its
/// underlying client rather than duplicating S3 contents. `buffer_unordered`
/// provides bounded asynchronous concurrency without one OS thread per object.
async fn materialize_clone_manifest(
    state: &AppState,
    source: &str,
    target: &str,
    mut manifest: Manifest,
) -> Result<Manifest, ZeppelinError> {
    manifest.pending_deletes.clear();
    if manifest.receipt_artifacts(source).is_err() {
        manifest
            .hydrate_receipt_artifacts(&state.store, source)
            .await?;
    }
    let copies = clone_copy_map(source, target, &manifest)?;
    manifest.normalize_copy_clone_artifact_ownership()?;
    rewrite_manifest_stored_keys(source, target, &mut manifest)?;
    manifest.rewrite_receipt_artifacts_for_clone(source, target)?;
    manifest.fencing_token = 0;
    manifest.updated_at = state.clock.now();
    manifest.reset_version_for_clone();

    futures::stream::iter(copies.into_iter().map(|(from, to)| {
        let store = state.store.clone();
        let target = target.to_string();
        async move { store.copy_if_not_exists(&from, &to, &target).await }
    }))
    .buffer_unordered(CLONE_COPY_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;

    Ok(manifest)
}

#[cfg(test)]
mod fork_response_tests {
    use super::{
        BranchHealth, BranchLifecycle, BranchMode, BranchStatusDescriptor, ForkResponse,
        ForkSourceIdentity, ForkTargetIdentity,
    };

    #[test]
    fn response_contains_only_redacted_public_fork_fields() {
        let response = ForkResponse {
            branch_id: "branch-1".to_string(),
            created: true,
            mode: BranchMode::CopyOnWrite,
            source: ForkSourceIdentity {
                namespace: "source".to_string(),
                incarnation: "source-inc".to_string(),
                generation: 42,
            },
            target: ForkTargetIdentity {
                namespace: "target".to_string(),
                incarnation: "target-inc".to_string(),
                generation: 1,
            },
            depth: 1,
            materialized: false,
            created_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        };
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["source"]["generation"], 42);
        assert_eq!(value["target"]["generation"], 1);
        assert_eq!(value["mode"], "copy_on_write");
        assert_eq!(value["materialized"], false);
        assert!(value.get("source_manifest_sha256").is_none());
        assert!(value.get("fencing_token").is_none());
    }

    #[test]
    fn target_status_contains_no_parent_identity() {
        let status = BranchStatusDescriptor {
            branch_id: "branch-1".to_string(),
            mode: BranchMode::CopyOnWrite,
            depth: 2,
            lifecycle: BranchLifecycle::Active,
            health: BranchHealth::Ready,
            materialized: false,
            created_at: chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        };
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["branch_id"], "branch-1");
        assert_eq!(value["lifecycle"], "active");
        assert!(value.get("source").is_none());
        assert!(value.get("parent_namespace").is_none());
    }
}

/// Maps every manifest-reachable source key to its target-prefixed destination.
///
/// # Parameters
///
/// - `source`: Namespace prefix all input keys must have.
/// - `target`: Namespace prefix to substitute.
/// - `manifest`: Borrowed manifest whose live reachable artifacts are expanded.
///
/// # Returns
///
/// An ordered, duplicate-free map from exact source keys to exact target keys.
/// The manifest remains usable by the caller.
///
/// # Errors
///
/// Returns a typed branching-not-ready error when the complete reachable
/// inventory contains an artifact owned by another namespace lifetime. Phase
/// 06 owns collision-safe materialization of that multi-origin logical view.
/// No copies occur in this helper.
///
/// # Examples
///
/// `source/wal/01.wal` maps to `restore/wal/01.wal`; the manifest's exact,
/// hydrated receipt inventory supplies every segment sidecar and hierarchical
/// routing node.
fn clone_copy_map(
    source: &str,
    target: &str,
    manifest: &Manifest,
) -> Result<BTreeMap<String, String>, ZeppelinError> {
    // Materialize the complete physical inventory before classifying it. A
    // foreign-backed manifest is a valid read view, but raw prefix substitution
    // cannot safely make it an independently owned clone: equal segment IDs or
    // WAL ULIDs from different origins can collide at the destination. Fail
    // before scheduling any copy; Phase 06 rebuilds such views through the
    // production owned-view materialization seam.
    let source_keys = manifest
        .receipt_artifacts(source)?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let source_prefix = format!("{source}/");
    if source_keys
        .iter()
        .any(|source_key| !source_key.starts_with(&source_prefix))
    {
        return Err(
            crate::namespace::branching::BranchError::BranchingNotReady {
                feature: "copy clone of a foreign-backed manifest",
            }
            .into(),
        );
    }
    source_keys
        .into_iter()
        .map(|source_key| {
            let target_key = rewrite_namespace_key(source, target, &source_key)?;
            Ok((source_key, target_key))
        })
        .collect()
}

/// Rewrites explicit object keys embedded in segment references for the target.
///
/// Computed artifact locations already receive the target namespace when query
/// or GC code derives them. This helper updates only keys persisted directly in
/// sketch, bootstrap, membership, and grouped-cluster references.
///
/// # Parameters
///
/// - `source`: Prefix expected on each embedded key.
/// - `target`: Replacement namespace prefix.
/// - `manifest`: Mutable unpublished clone manifest.
///
/// # Errors
///
/// Returns an index error on the first embedded key outside the source prefix.
/// Earlier fields may already have been rewritten in memory, but no storage
/// publication occurs here.
///
/// # Side Effects
///
/// Mutates only the owned in-memory manifest supplied by the caller.
///
/// # Examples
///
/// A stored membership key `source/segments/s1/membership.bin` becomes
/// `restore/segments/s1/membership.bin`; the segment ID remains `s1`.
fn rewrite_manifest_stored_keys(
    source: &str,
    target: &str,
    manifest: &mut Manifest,
) -> Result<(), ZeppelinError> {
    for segment in &mut manifest.segments {
        if let Some(sketch) = &mut segment.sketch {
            sketch.key = rewrite_namespace_key(source, target, &sketch.key)?;
        }
        if let Some(bootstrap) = &mut segment.bootstrap {
            bootstrap.key = rewrite_namespace_key(source, target, &bootstrap.key)?;
        }
        if let Some(membership) = &mut segment.membership {
            membership.key = rewrite_namespace_key(source, target, &membership.key)?;
        }
        for object_ref in &mut segment.cluster_objects {
            object_ref.key = rewrite_namespace_key(source, target, &object_ref.key)?;
        }
    }
    Ok(())
}

/// Replaces one exact leading namespace component in an object-store key.
///
/// # Parameters
///
/// - `source`: Expected source namespace without the trailing slash.
/// - `target`: Destination namespace without the trailing slash.
/// - `key`: Complete source object key.
///
/// # Returns
///
/// A newly allocated target key with the suffix preserved byte-for-byte.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] when `key` does not begin with
/// `<source>/`; this fails closed instead of copying or publishing a reference
/// outside the source namespace.
///
/// # Examples
///
/// Rewriting source `catalog`, target `restore`, and key
/// `catalog/wal/01.wal` returns `restore/wal/01.wal`.
fn rewrite_namespace_key(source: &str, target: &str, key: &str) -> Result<String, ZeppelinError> {
    let source_prefix = format!("{source}/");
    let suffix = key.strip_prefix(&source_prefix).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "clone source manifest key {key:?} is outside source prefix {source_prefix:?}"
        ))
    })?;
    Ok(format!("{target}/{suffix}"))
}

/// Builds metadata-only responses for all namespaces discovered in storage.
///
/// This function is intentionally not registered by
/// [`crate::server::build_router`], so `GET /v1/namespaces` receives 405 rather
/// than enumerating tenant names. If an internal caller invokes it, each result
/// is combined with one empty manifest, so all live artifact statistics are
/// zero; it is a metadata inventory, not a status endpoint.
///
/// # Parameters
///
/// - `state`: Shared namespace manager and indexing defaults.
///
/// # Returns
///
/// An unordered JSON array of namespace metadata responses. Empty means no
/// top-level metadata records were discovered.
///
/// # Errors
///
/// Propagates common-prefix listing, per-namespace metadata GET, and metadata
/// decoding failures through [`ApiError`]. A missing metadata object that
/// disappears during listing is skipped by the domain manager.
///
/// # Side Effects
///
/// Refreshes process-local namespace registry entries and emits a count log. It
/// does not read manifests or modify object storage.
///
/// # Performance
///
/// Performs one delimiter LIST plus one metadata GET per discovered namespace;
/// those GETs currently run sequentially. It does not recursively walk WAL or
/// segment objects.
///
/// # Examples
///
/// An internal inventory over `catalog` and `inventory` returns two metadata
/// rows with zero live counts. External `GET /v1/namespaces` remains disabled.
#[allow(dead_code)]
#[instrument(skip(state))]
pub async fn list_namespaces(
    State(state): State<AppState>,
) -> Result<Json<Vec<NamespaceResponse>>, ApiError> {
    let namespaces = state
        .namespace_manager
        .list(None)
        .await
        .map_err(ApiError::from)?;

    info!(count = namespaces.len(), "listed namespaces");
    let mut responses = Vec::with_capacity(namespaces.len());
    for meta in namespaces {
        let manifest = if meta.state == NamespaceState::Active {
            state
                .manifest_cache
                .get_strong_required(&state.store, &meta.name)
                .await
                .map_err(ApiError::from)?
        } else {
            Manifest::new_at(state.clock.now())
        };
        responses.push(
            NamespaceResponse::from_manifest(meta, &manifest, &state.config.indexing)
                .map_err(ApiError::from)?,
        );
    }
    Ok(Json(responses))
}

/// Returns lifecycle metadata plus strongly verified manifest statistics.
///
/// Unlike ordinary data operations, this status endpoint deliberately accepts
/// a deletion tombstone so clients can observe `state = "deleting"` until the
/// background worker removes metadata. The manifest may already be gone at
/// that point; the manifest cache represents a missing live manifest as empty,
/// so a deleting response can contain zero artifact statistics.
///
/// # Parameters
///
/// - `state`: Shared metadata manager, manifest cache, store, and defaults.
/// - `ns`: Namespace path segment to inspect.
///
/// # Returns
///
/// HTTP 200 with a fully owned [`NamespaceResponse`]. Active and deleting
/// namespaces are both representable; absence after final deletion returns an
/// error instead.
///
/// # Errors
///
/// Returns 404 when metadata is absent. Metadata, object-store, ETag, and
/// manifest decoding errors map through [`ApiError`]. This endpoint does not
/// return 410 merely because metadata is already `deleting`.
///
/// # Side Effects
///
/// May refresh the namespace metadata registry and always strongly verifies or
/// fetches the live manifest through the process-local cache.
///
/// # Consistency
///
/// Manifest-derived fields are based on an S3/MinIO verification performed for
/// this strong read, not solely on TTL. Metadata lookup can use its bounded-TTL
/// registry because this operation does not publish metadata changes.
///
/// # Performance
///
/// A metadata cache miss performs one metadata GET. The strong manifest path
/// normally performs one conditional or full manifest GET and can coalesce
/// concurrent readers. It performs no prefix LIST, HEAD, WAL GET, or segment
/// GET; counts and bytes come from the manifest body.
///
/// # Examples
///
/// After an upsert publishes one seven-vector WAL fragment, GET reports seven
/// vectors and one uncompacted fragment. During deletion it may briefly report
/// `deleting` with zero counts; after tombstone removal the same GET returns 404.
#[instrument(skip(state, decision), fields(namespace = %ns))]
pub async fn get_namespace(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    let meta = state
        .namespace_manager
        .get_including_deleting(&ns)
        .await
        .map_err(ApiError::from)?;

    // Stats are manifest aggregates. This strong read is the same manifest
    // freshness path used by read handlers; the response below does not list,
    // HEAD, or fetch WAL/segment objects.
    let manifest = match meta.state {
        NamespaceState::Active => {
            state
                .manifest_cache
                .get_strong_required(&state.store, &ns)
                .await
        }
        NamespaceState::Creating => {
            return Err(ApiError(ZeppelinError::ManifestConflict { namespace: ns }));
        }
        NamespaceState::Deleting => state.manifest_cache.get_strong(&state.store, &ns).await,
    }
    .map_err(ApiError::from)?;

    Ok(Json(
        NamespaceResponse::from_manifest(meta, &manifest, &state.config.indexing)
            .map_err(ApiError::from)?,
    ))
}

/// Reports compaction readiness from a strongly verified live manifest.
///
/// This endpoint is a polling surface for [`compact_namespace`]. It does not
/// inspect compaction tasks or metadata health; `ready` is exactly the absence
/// of uncompacted WAL fragment references in the manifest.
///
/// # Parameters
///
/// - `state`: Shared active-namespace and manifest services.
/// - `ns`: Namespace path segment whose readiness should be read.
///
/// # Returns
///
/// HTTP 200 with manifest generation, fragment/segment counts, active segment,
/// and a derived readiness flag.
///
/// # Errors
///
/// Returns 404 for a missing namespace, 410 for a deletion tombstone, or mapped
/// metadata, storage, and manifest errors.
///
/// # Side Effects
///
/// May refresh metadata and manifest caches. It does not acquire a lease or
/// start compaction.
///
/// # Performance
///
/// Performs active metadata lookup and one strong manifest verification, then
/// scans in-memory fragment and segment references. It reads no artifact body.
///
/// # Examples
///
/// A manifest with two fragments returns `ready = false`. After a successful
/// compaction publishes a segment and removes both references, a later poll
/// returns a newer generation with `ready = true`.
#[instrument(skip(state, decision), fields(namespace = %ns))]
pub async fn get_compaction_status(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
) -> Result<Json<CompactionStatusResponse>, ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let manifest = state
        .manifest_cache
        .get_strong_required(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(compaction_status_from_manifest(&ns, &manifest)))
}

/// Reject unsupported row- or attribute-constrained namespace-wide work.
///
/// Namespace status, retention, index configuration, hydration, compaction,
/// and deletion operate on or reveal the entire namespace. They cannot honor a
/// row filter or field mask by editing a response or partially mutating state.
/// Dropping those constraints would widen authority, while projecting fake
/// values would fabricate state, so these handlers fail closed before storage
/// work.
fn require_unconstrained_namespace_operation(decision: &AllowDecision) -> Result<(), ApiError> {
    if decision.mandatory_filter.is_some()
        || decision.field_mask.is_some()
        || !decision.write_constraints.is_empty()
    {
        Err(ApiError(
            crate::security::SecurityError::ConstraintViolation.into(),
        ))
    } else {
        Ok(())
    }
}

/// Starts one lease-protected compaction cycle when manifest work is pending.
///
/// The handler strongly reads the current manifest before acquiring a lease.
/// With no uncompacted fragments it returns a synchronous no-op. Otherwise it
/// acquires the namespace lease, moves the lease and shared service handles into
/// a detached Tokio task, and immediately returns the pre-trigger status.
///
/// ```text
/// strong manifest read
///        |
///        +-- no fragments --> 200 "noop"
///        |
///        v
/// acquire lease -- held elsewhere --> 409 retryable conflict
///        |
///        v
/// spawn fenced compaction --> 202 "accepted"
///        |
///        +-- success --> publish manifest + invalidate cache + log
///        `-- failure --> invalidate cache + log; HTTP response is unchanged
/// ```
///
/// # Parameters
///
/// - `state`: Shared namespace manager, manifest cache, lease manager, and
///   compactor.
/// - `ns`: Active namespace selected for one manual cycle.
///
/// # Returns
///
/// HTTP 200 with `status = "noop"` when no fragment, receipt upgrade, or
/// foreign-backed view needs compaction, or HTTP 202 with `status = "accepted"`
/// after lease acquisition and task spawn. Both bodies describe the manifest
/// observed before work began.
///
/// # Errors
///
/// Returns 404 for a missing namespace, 410 for a deleting namespace, 409 when
/// another valid lease holder owns the namespace, or mapped metadata/manifest/
/// lease storage errors before the task is spawned. Errors after HTTP 202 are
/// logged and reflected through later status and health reads, not returned to
/// the completed request.
///
/// # Side Effects
///
/// May acquire a lease and spawn a task that reads immutable WAL artifacts,
/// builds and uploads a segment, conditionally publishes the manifest, records
/// compaction health, releases the lease best-effort, and invalidates manifest
/// cache state on either outcome.
///
/// # Consistency
///
/// The background path combines a lease/fencing token with manifest CAS; a
/// stale writer must not publish over a newer lease holder. The initial
/// no-work check is only an observation: publication authority remains in the
/// lease-protected domain operation. A foreign-backed branch is always admitted
/// because explicit compaction is its requested materialization boundary even
/// when it has no target-local WAL.
///
/// # Performance
///
/// The request path pays for metadata lookup, one strong manifest verification,
/// and lease acquisition. Accepted work continues asynchronously and can incur
/// substantial S3 reads/writes and CPU for index construction. The handler does
/// not wait for that cost.
///
/// # Examples
///
/// After one upsert, POST `/compact` returns 202 with one uncompacted fragment.
/// Polling `/compact/status` eventually observes zero fragments and a newer
/// generation. A simultaneous second trigger normally receives a retryable 409
/// while the first task holds the lease.
///
/// # Rust Notes for Java/C Engineers
///
/// `tokio::spawn` requires an owned, `Send + 'static` future because the request
/// stack frame will disappear before compaction finishes. Cloning each `Arc`
/// increments a reference count rather than copying the service. The owned
/// namespace string, FTS map, and lease move into the task. Java relies on heap
/// reachability for similar callbacks; C requires explicit reference counting
/// and cleanup. Rust verifies the task cannot retain borrowed request locals.
#[instrument(skip(state, decision), fields(namespace = %ns))]
pub async fn compact_namespace(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<CompactNamespaceResponse>), ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let before = state
        .manifest_cache
        .get_strong_required(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;

    let receipt_upgrade_available = state
        .store
        .object_signer_node()
        .map_err(ApiError::from)?
        .is_some()
        && before.receipt_upgrade_needed(&ns);
    let materialize_foreign = before
        .has_foreign_visible_artifacts()
        .map_err(ApiError::from)?;
    if before.uncompacted_fragments().is_empty()
        && !receipt_upgrade_available
        && !materialize_foreign
    {
        return Ok((
            StatusCode::OK,
            Json(CompactNamespaceResponse::from_status(
                "noop",
                compaction_status_from_manifest(&ns, &before),
            )),
        ));
    }

    let reservation = state
        .compaction_lifecycle
        .reserve()
        .map_err(ApiError::from)?;
    let lease = state
        .lease_manager
        .acquire(&ns)
        .await
        .map_err(ApiError::from)?;
    info!(
        namespace = %ns,
        fencing_token = lease.fencing_token,
        "manual compaction accepted"
    );

    let compactor = state.compactor.clone();
    let lease_manager = state.lease_manager.clone();
    let manifest_cache = state.manifest_cache.clone();
    let cache = state.cache.clone();
    let clock = state.clock.clone();
    let ns_for_task = ns.clone();
    let fts_configs = meta.full_text_search.clone();
    state
        .server_tasks
        .spawn("manual namespace compaction", async move {
            match run_compaction_with_reserved_lease(
                &compactor,
                &lease_manager,
                &ns_for_task,
                lease,
                &fts_configs,
                FragmentCachePolicy::ReadOnly(&cache),
                reservation,
            )
            .await
            {
                Ok(result) => {
                    manifest_cache.invalidate_at(&ns_for_task, clock.now());
                    info!(
                        namespace = %ns_for_task,
                        vectors_compacted = result.vectors_compacted,
                        fragments_removed = result.fragments_removed,
                        "manual compaction completed"
                    );
                }
                Err(e) => {
                    manifest_cache.invalidate_at(&ns_for_task, clock.now());
                    tracing::error!(
                        namespace = %ns_for_task,
                        error = %e,
                        "manual compaction failed"
                    );
                }
            }
        });

    Ok((
        StatusCode::ACCEPTED,
        Json(CompactNamespaceResponse::from_status(
            "accepted",
            compaction_status_from_manifest(&ns, &before),
        )),
    ))
}

/// Persists desired per-namespace index settings for the next compaction.
///
/// Omitted fields preserve the namespace's current effective settings. Legacy
/// metadata without an override first inherits current server defaults. The
/// handler validates the complete result, then the namespace manager publishes
/// a whole-metadata replacement with ETag compare-and-swap. It does not itself
/// schedule or run compaction.
///
/// # Parameters
///
/// - `state`: Shared metadata manager and server indexing defaults.
/// - `ns`: Active namespace whose desired build settings should change.
/// - `req`: Owned optional-field patch; at least one field must be supplied.
///
/// # Returns
///
/// HTTP 202 with the complete persisted configuration and guidance to observe a
/// later compaction through namespace GET.
///
/// # Errors
///
/// Returns 400 for an empty patch or invalid resolved parameters, 404 for a
/// missing namespace, 410 for a deleting namespace, 409 if repeated metadata
/// CAS attempts lose, or mapped storage/serialization errors. A post-write
/// invariant check also returns a server-side index error if the returned
/// metadata unexpectedly omits `index_config`.
///
/// # Side Effects
///
/// Performs a fresh versioned metadata GET and conditional PUT, updates
/// `updated_at`, and refreshes the metadata registry. Existing immutable
/// segments and the manifest are unchanged.
///
/// # Consistency
///
/// Every CAS retry reloads authoritative metadata, so a stale patch cannot
/// overwrite a concurrent deletion or health update. New settings affect only
/// a future segment publication. Repeating an identical patch is behaviorally
/// safe but still republishes metadata and advances `updated_at`.
///
/// # Performance
///
/// Performs at least one metadata GET and conditional PUT, with up to ten CAS
/// attempts inside the manager. No segment objects are read or rewritten.
///
/// # Examples
///
/// PATCH `{"nlist":256}` returns 202 immediately. The active segment keeps its
/// old cluster count until a later compaction publishes a replacement built
/// with 256 centroids.
#[instrument(skip(state, decision, audit), fields(namespace = %ns))]
pub async fn patch_index_config(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(audit): Extension<AuditRequest>,
    Path(ns): Path<String>,
    Json(req): Json<PatchNamespaceIndexConfigRequest>,
) -> Result<(StatusCode, Json<UpdateIndexConfigResponse>), ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    if req.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "index_config patch must include at least one field".into(),
        )));
    }
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let current = meta
        .index_config
        .clone()
        .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(&state.config.indexing));
    let old_for_audit = IndexConfigValues::from(&current);
    let next = apply_namespace_index_config_patch(current, &req, meta.dimensions)
        .map_err(ApiError::from)?;
    audit.set_params(AuditParams::IndexConfigPatch {
        namespace: NamespaceId::new(ns.clone()).map_err(ZeppelinError::from)?,
        old: old_for_audit,
        new: IndexConfigValues::from(&next),
    });
    let updated = state
        .namespace_manager
        .update_index_config(state.lease_manager.as_ref(), &ns, next)
        .await
        .map_err(ApiError::from)?;
    let index_config = updated.index_config.ok_or_else(|| {
        ApiError(ZeppelinError::Index(
            "index_config update did not persist index_config".into(),
        ))
    })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(UpdateIndexConfigResponse {
            namespace: ns.clone(),
            index_config,
            status: "accepted",
            observe: format!(
                "applies asynchronously on the next compaction; observe GET /v1/namespaces/{ns}"
            ),
        }),
    ))
}

/// Tombstones a namespace and resumes destructive cleanup in the background.
///
/// Phase one is completed before HTTP acceptance: metadata is conditionally
/// changed to `deleting` while binding a deterministic destruction-record key.
/// The live manifest is then CAS-fenced against further publication, and the
/// exact immutable evidence record is created from that fenced generation before
/// the manifest is removed. The handler evicts disposable WAL-lock and
/// manifest-cache state;
/// after its must-audit response barrier succeeds, middleware spawns a 25-second
/// bounded prefix-deletion pass. Every cleanup pass rechecks preservation state,
/// and the metadata tombstone is deleted last only after S3/MinIO verifies no
/// other namespace keys remain.
///
/// # Parameters
///
/// - `state`: Shared lifecycle manager plus process-local WAL and manifest
///   caches.
/// - `ns`: Namespace whose deletion should begin or resume.
///
/// # Returns
///
/// HTTP 202 with `{"state":"deleting"}` after the trailing intent, immutable
/// destruction evidence, tombstone, and fenced manifest removal succeed. The must-audit
/// middleware spawns cleanup only after its own durable audit barrier succeeds.
/// Completion is observed when [`get_namespace`] returns 404.
///
/// # Errors
///
/// Returns 404 after deletion has fully removed metadata; 409 for preservation
/// locks or repeated tombstone CAS conflicts; 503 when fresh lock authority is
/// unavailable; 500 when required audit or destruction-evidence durability is
/// unavailable; or mapped metadata, manifest, and serialization failures. If
/// evidence or manifest publication fails, the durable trailing intent and
/// deletion fence can already exist even though this request returns an error;
/// their bound evidence key makes retry deterministic.
///
/// Background listing/deletion failures happen after HTTP 202 and are logged;
/// they leave the tombstone so a later DELETE can resume safely.
///
/// # Side Effects
///
/// CAS-updates metadata, CAS-publishes a manifest deletion fence, publishes
/// immutable destruction evidence, deletes the live manifest, removes the
/// process-local WAL writer lock, and invalidates the manifest cache. Successful
/// durable response auditing spawns prefix cleanup; the worker can delete every
/// object under the namespace and finally `meta.json`.
///
/// # Consistency
///
/// Intent-before-fence-before-evidence-before-tombstone ordering makes root
/// publication and deletion compete on one manifest CAS while leaving a losing
/// source active. Manifest removal then ends visibility before artifact
/// deletion. Branch targets additionally persist an S3-timestamped grace marker
/// and retain their parent root until that deadline. DELETE is resumable while
/// the intent/tombstone exists; retries reuse its evidence key and after
/// completion return 404.
///
/// # Performance
///
/// Before returning, the handler pays for fresh preservation state, versioned
/// metadata read/CAS, evidence lookup or create-only publication, manifest
/// read/CAS fencing, two exact namespace object censuses for new evidence, and a
/// manifest DELETE. The detached pass spends up to 25 seconds on paged LIST and
/// DELETE work; large namespaces may require a later retry.
///
/// # Examples
///
/// DELETE `catalog` returns 202. Status GET may briefly report `deleting`; data
/// requests receive the deleting error. If the first 25-second pass exhausts
/// its budget, a repeated DELETE starts another pass. Final metadata removal
/// makes GET return 404.
///
/// # Rust Notes for Java/C Engineers
///
/// Cloning the manager's `Arc` and moving an owned namespace string into the
/// spawned task separates task lifetime from request lifetime. There is no
/// borrowed stack pointer to become invalid. Remote cleanup is not covered by
/// Rust RAII, so the tombstone is the durable equivalent of a resumable cleanup
/// record after process cancellation or crash.
#[instrument(skip(state, decision, principal, context, audit), fields(namespace = %ns))]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Extension(audit): Extension<AuditRequest>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<DeleteNamespaceResponse>), ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    let namespace_id = NamespaceId::new(ns.clone()).map_err(|error| ApiError(error.into()))?;
    let cached_guard = state
        .security
        .guard_namespace_destruction(&namespace_id)
        .map_err(|error| ApiError(error.into()))?;
    let guard = if cached_guard.is_locked() {
        state
            .security
            .record_namespace_delete_deferral(&namespace_id, &cached_guard)
            .await
            .map_err(ApiError::from)?;
        cached_guard
    } else {
        state
            .security
            .guard_namespace_destruction_strong(&namespace_id)
            .await
            .map_err(ApiError)?
            .0
    };
    if guard.is_locked() {
        audit.set_params(AuditParams::preservation_blocked(
            PreservationBlockedSurface::NamespaceDelete,
            &guard,
        ));
        return Err(ApiError(SecurityError::PreservationLocked.into()));
    }

    audit.set_params(AuditParams::NamespaceDelete {
        namespace: namespace_id.clone(),
    });
    let envelope = state
        .security
        .authorize_namespace_delete(NamespaceDeleteAdmission {
            namespace: namespace_id,
            principal,
            context,
            allow: decision,
            approver: audit.approval_principal_id(),
            store: state.store.clone(),
            clock: state.clock.clone(),
        });
    namespace_graph(&state)
        .delete(envelope)
        .await
        .map_err(ApiError::from)?;

    state.wal_writer.remove_lock(&ns);

    info!(namespace = %ns, state = "deleting", "namespace delete accepted");
    Ok((
        StatusCode::ACCEPTED,
        Json(DeleteNamespaceResponse { state: "deleting" }),
    ))
}

/// Requests non-blocking warm-set hydration for the current active segment.
///
/// Hydration is a cache optimization, never an authority change. The handler
/// rejects the request when hydration is disabled, requires an active namespace,
/// strongly reads the manifest, clones the active segment descriptor, and
/// offers it to the hydrator's bounded channel without awaiting object loads.
///
/// # Parameters
///
/// - `state`: Shared optional hydrator, namespace manager, manifest cache, and
///   store.
/// - `ns`: Active namespace whose current active segment should be warmed.
///
/// # Returns
///
/// HTTP 202 naming the selected segment after the non-blocking hydration request
/// is made. This does not guarantee the job entered the bounded queue or that
/// any cache object has been written; queue-full/closed outcomes are logged and
/// counted inside the hydrator, whose API returns unit.
///
/// # Errors
///
/// Returns 409 before namespace lookup when hydration is disabled, 404 for a
/// missing namespace, 410 for a deleting namespace, 400 when the strong
/// manifest has no resolvable active segment, or mapped manifest/storage errors.
///
/// # Side Effects
///
/// Strongly verifies the manifest and may enqueue an owned hydration job. A
/// worker can later GET immutable segment objects and populate disposable local
/// disk cache, while updating hydration metrics and logs.
///
/// # Consistency
///
/// The selected descriptor comes from the strongly verified live manifest.
/// If a later compaction changes the active segment, the queued job retains its
/// owned snapshot; hydration cannot change query visibility or override S3.
/// Repeated requests can submit repeated jobs and are not deduplicated here.
///
/// # Performance
///
/// The HTTP path performs metadata lookup and one strong manifest verification,
/// then uses a non-blocking channel send. It does not wait for potentially large
/// segment GETs or cache writes.
///
/// # Examples
///
/// POST `/hydrate` after compaction returns 202 with `segment_id = "s42"` in
/// well under the time needed to download that segment. A later query can hit
/// disk cache after the worker completes. An empty namespace returns 400.
#[instrument(skip(state, decision), fields(namespace = %ns))]
pub async fn trigger_hydration(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<HydrateNamespaceResponse>), ApiError> {
    require_unconstrained_namespace_operation(&decision)?;
    let hydrator = state
        .hydrator
        .as_ref()
        .ok_or(ApiError(ZeppelinError::HydrationDisabled))?;

    let metadata = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;
    let manifest = state
        .manifest_cache
        .get_strong_required(&state.store, &ns)
        .await
        .map_err(ApiError::from)?;
    let authoritative_origin = metadata.artifact_origin().map_err(ApiError::from)?;
    let target = match authoritative_origin.as_ref() {
        Some(origin) => HydrationTarget::from_active_manifest_with_origin(&manifest, origin),
        None => HydrationTarget::from_active_manifest(&manifest),
    }
    .map_err(ApiError::from)?
    .ok_or_else(|| {
        ApiError(ZeppelinError::Validation(format!(
            "namespace {ns} has no active segment to hydrate"
        )))
    })?;
    let segment_id = target.segment().id.clone();

    hydrator.request_hydration(&target);
    info!(
        namespace = %ns,
        segment_id = %segment_id,
        "namespace hydration accepted"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(HydrateNamespaceResponse {
            namespace: ns,
            segment_id,
        }),
    ))
}

/// Derives the compact polling view from one borrowed manifest snapshot.
///
/// # Parameters
///
/// - `namespace`: Namespace name copied into the response.
/// - `manifest`: Borrowed strongly read manifest to summarize.
///
/// # Returns
///
/// An owned status response. `ready` is true exactly when no uncompacted
/// fragments are referenced; an absent or unresolved active segment yields
/// `None` and zero active vectors.
///
/// # Performance
///
/// Scans manifest fragment/segment references in memory and allocates the
/// namespace and optional active ID. It performs no S3/MinIO request.
///
/// # Examples
///
/// Generation 8 with one active segment and no fragments becomes a ready
/// response carrying generation 8 and that segment's vector count.
fn compaction_status_from_manifest(
    namespace: &str,
    manifest: &Manifest,
) -> CompactionStatusResponse {
    let active_segment = active_segment_ref(manifest);
    let uncompacted_fragments = manifest.uncompacted_fragments().len();
    CompactionStatusResponse {
        namespace: namespace.to_string(),
        manifest_generation: manifest.version(),
        uncompacted_fragments,
        segment_count: manifest.segments.len(),
        active_segment: active_segment.map(|segment| segment.id.clone()),
        active_segment_vector_count: active_segment.map_or(0, |segment| segment.vector_count),
        ready: uncompacted_fragments == 0,
    }
}

/// Resolves the manifest's active-segment ID to its borrowed segment reference.
///
/// # Parameters
///
/// - `manifest`: Borrowed manifest containing the optional ID and segment list.
///
/// # Returns
///
/// `Some(&SegmentRef)` when both the ID and matching entry exist; otherwise
/// `None`. The returned reference cannot outlive `manifest`.
///
/// # Performance
///
/// Performs a linear scan of `manifest.segments` and no allocation or I/O.
///
/// # Examples
///
/// If `active_segment` is `s2` and the list contains `s1, s2`, this returns a
/// borrow of `s2`. A stale pointer to an absent ID returns `None`.
fn active_segment_ref(manifest: &Manifest) -> Option<&SegmentRef> {
    let active_segment = manifest.active_segment.as_ref()?;
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment)
}

/// Chooses the index family reported for the namespace's active data.
///
/// # Parameters
///
/// - `meta`: Borrowed metadata containing the namespace's baseline index type.
/// - `manifest`: Borrowed manifest whose active segment can record hierarchical
///   layout.
///
/// # Returns
///
/// [`IndexType::Hierarchical`] when the resolved active segment is marked
/// hierarchical; otherwise the metadata `index_type` value.
///
/// # Examples
///
/// Legacy IVF-flat metadata plus a hierarchical active segment reports
/// `hierarchical`. Before first compaction, the metadata type is reported.
fn namespace_index_kind(meta: &NamespaceMetadata, manifest: &Manifest) -> IndexType {
    if active_segment_ref(manifest).is_some_and(|segment| segment.hierarchical) {
        IndexType::Hierarchical
    } else {
        meta.index_type
    }
}
