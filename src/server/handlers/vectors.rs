//! HTTP ingestion, deletion, and point lookup for namespace vectors.
//!
//! This module is the HTTP boundary between Axum and Zeppelin's WAL-backed
//! vector domain. Clients enter through
//! [`crate::server::handlers::vectors::upsert_vectors`],
//! [`crate::server::handlers::vectors::delete_vectors`], or
//! [`crate::server::handlers::vectors::get_vectors`]. The write handlers
//! validate a complete request before
//! moving its operations into [`crate::wal::WalWriter::append`]; the lookup
//! handler selects one authoritative [`crate::wal::Manifest`] snapshot,
//! overlays visible WAL operations, and then resolves older records through the
//! active immutable segment. This file does not implement vector similarity
//! search, compaction, namespace creation, or a list/scan endpoint.
//!
//! S3 or MinIO is the source of truth. A WAL fragment PUT is not an
//! acknowledgment or visibility boundary: the fragment becomes committed only
//! when the writer publishes a manifest that references it. The in-memory
//! [`crate::cache::manifest_cache::ManifestCache`] is only a write-through and
//! read optimization. Lookup uses the local [`crate::cache::DiskCache`] for WAL
//! fragment bytes, but currently reads membership, cluster, and attribute
//! artifacts directly through [`crate::storage::ZeppelinStore`].
//!
//! ## Reading map
//!
//! 1. Start with [`crate::server::handlers::vectors::UpsertVectorsRequest`],
//!    [`crate::server::handlers::vectors::DeleteVectorsRequest`], and
//!    [`crate::server::handlers::vectors::GetVectorsRequest`] for the wire
//!    contract.
//! 2. Read [`crate::server::handlers::vectors::upsert_vectors`] and
//!    [`crate::server::handlers::vectors::delete_vectors`] for validation and
//!    WAL publication.
//! 3. Read [`crate::server::handlers::vectors::get_vectors`] for manifest
//!    selection and response projection.
//! 4. Follow `fetch_vectors_by_id` and `fetch_strong_wal_records` for strong
//!    versus eventual WAL visibility.
//! 5. Finish with `fetch_segment_records`, `fetch_segment_cluster`, and
//!    `fetch_segment_attrs` for membership-directed immutable segment reads.
//!
//! ## Write and publication flow
//!
//! ```text
//! POST upsert or DELETE IDs
//!            |
//!            v
//! parse + validate whole request ---- invalid ----> canonical HTTP error
//!            |
//!            v
//! confirm namespace metadata and dimensions
//!            |
//!            v
//! upload one immutable WAL fragment
//!            |  object exists, but readers cannot see it yet
//!            v
//! publish manifest with ETag CAS ----- failure ----> error; orphan cleanup
//!            |                                      belongs to WalWriter
//!            | success
//!            v
//! write through committed manifest cache -> acknowledge HTTP request
//! ```
//!
//! A retry of the same upsert or delete creates another immutable fragment; the
//! HTTP operation has no idempotency key. Repeating an upsert is logically
//! replace-by-ID, and repeating a delete is logically tombstoning the same ID,
//! but retries still consume WAL and manifest generations.
//!
//! ## Point-lookup flow
//!
//! ```text
//! POST /vectors/get
//!        |
//!        v
//! validate unique IDs + projection
//!        |
//!        v
//! choose one manifest snapshot
//!   | strong: verify S3/MinIO   | eventual: TTL cache may be stale
//!   v                           v
//! replay all WAL operations     read delete-bearing WAL fragments only
//!   | latest op wins            | recent WAL upserts may be omitted
//!   +-------------+-------------+
//!                 v
//! unresolved, non-tombstoned IDs
//!                 |
//!                 v
//! active segment membership GET
//!      | ID only             | values and/or attributes
//!      v                     v
//! build records       cluster GET + optional attrs GET
//!                 \           /
//!                  v         v
//!         project fields + restore request order
//! ```
//!
//! The membership artifact is the compact ID-to-cluster directory. A manifest
//! with an active segment but no membership artifact cannot satisfy point
//! lookup and fails loudly; the handler does not scan every cluster. When a
//! grouped cluster-object descriptor does not name a cluster, lookup uses the
//! legacy per-cluster key derived from
//! [`crate::wal::manifest::SegmentRef::cluster_owner`]. That is a
//! persisted-layout compatibility path, not a fallback for a missing object.
//!
//! ## Invariants
//!
//! - Request validation completes before a handler starts durable write work.
//! - A successful write response means the immutable fragment is referenced by
//!   the committed manifest returned by the WAL writer.
//! - Cache contents never override manifest authority or make an artifact
//!   visible.
//! - Strong lookup replays manifest-ordered uncompacted WAL fragments so the
//!   latest visible upsert or tombstone wins over older WAL and segment data.
//! - Eventual lookup may omit a recent upsert or return the older compacted
//!   value, but effective tombstones loaded from the selected manifest prevent
//!   deleted segment records from being resurrected.
//! - Results and missing IDs preserve request order, and duplicate request IDs
//!   are rejected rather than collapsed.
//! - Missing, corrupt, or contradictory manifest-selected artifacts are
//!   server errors; the handler never substitutes empty data.
//!
//! ## Rust concepts used here
//!
//! Axum extractors move a request-scoped [`crate::server::AppState`] clone, path
//! string, headers, and body buffer into each `async fn`. The state's stores and
//! services are internally shared, mostly through [`std::sync::Arc`], so this
//! resembles Java sharing service references; unlike ordinary C pointers, Rust
//! keeps those services alive across every `.await` and prevents unsafely
//! concurrent mutation.
//!
//! Serde derives turn owned JSON or MessagePack input into domain values.
//! `FetchProjection<'a>` instead borrows an optional field-name slice from the
//! request and is `Copy`, so helper calls copy only booleans and a checked
//! borrowed view rather than cloning field strings. `Result` plus `?` makes
//! every storage, decoding, and invariant failure explicit, while exhaustive
//! `match` expressions force both consistency modes to be handled.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use axum::extract::{Extension, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::index::filter::{combine_filters, evaluate_filter};
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, deserialize_attrs, deserialize_cluster_from_object,
};
use crate::index::ivf_flat::membership::deserialize_membership;
use crate::namespace::manager::NamespaceMetadata;
use crate::query;
use crate::security::{
    apply_field_mask, filter_matches_write_scope, filter_references_denied_field, AllowDecision,
    AuditParams, FieldMask, NamespaceId, SecurityError,
};
use crate::server::{AppState, AuditRequest};
use crate::types::{AttributeValue, ConsistencyLevel, Filter, VectorEntry, VectorId};
use crate::wal::manifest::SegmentRef;
use crate::wal::{FragmentCachePolicy, Manifest, ManifestAppendGuard};

use super::ApiError;

/// Row-oriented request body for inserting or replacing vectors by ID.
///
/// The namespace comes from the URL. JSON requests always use this shape;
/// MessagePack requests may use the same row shape or the private columnar
/// representation decoded by `MessagePackUpsertRequest`.
///
/// # Examples
///
/// ```text
/// {"vectors":[{"id":"item-7","values":[0.1,0.2],"attributes":{"color":"blue"}}]}
/// ```
///
/// If `item-7` already exists, successful manifest publication makes this
/// entry the newer logical value. The request object itself performs no
/// validation or I/O; [`upsert_vectors`] owns those checks and side effects.
#[derive(Debug, Deserialize)]
pub struct UpsertVectorsRequest {
    /// Ordered entries to append as one WAL write; the handler rejects an empty
    /// or oversized batch and validates every ID, coordinate, and dimension.
    pub vectors: Vec<UpsertVectorInput>,
}

/// One row-oriented upsert input before server-owned identity resolution.
#[derive(Debug, Deserialize)]
pub struct UpsertVectorInput {
    /// Existing caller-visible ID to update. A constrained create omits this
    /// field and receives an opaque server-owned ID in the response.
    #[serde(default)]
    pub id: Option<VectorId>,
    /// Dense coordinates validated against the namespace dimension.
    pub values: Vec<f32>,
    /// Optional metadata subject to policy stamps and write constraints.
    #[serde(default)]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

impl UpsertVectorInput {
    fn into_vector_entry(self, id: VectorId) -> VectorEntry {
        VectorEntry {
            id,
            values: self.values,
            attributes: self.attributes,
        }
    }
}

/// MessagePack envelope accepting exactly one ingestion representation.
///
/// `#[serde(deny_unknown_fields)]` rejects misspelled or unsupported keys
/// instead of silently ignoring them. The two [`Option`] fields temporarily
/// represent four parseable states; `parse_msgpack_upsert_request` narrows them
/// to the two valid states before any write occurs.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<T>` is a tagged value, not a nullable pointer. Serde maps a missing
/// field to `None`, and the later tuple match must explicitly distinguish both,
/// neither, or exactly one representation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagePackUpsertRequest {
    /// Conventional row records, equivalent to JSON's `vectors` array.
    vectors: Option<Vec<UpsertVectorInput>>,
    /// Compact columnar records with all coordinates in one little-endian byte
    /// string.
    columnar: Option<ColumnarUpsertRequest>,
}

/// Columnar MessagePack representation optimized for high-dimensional batches.
///
/// IDs and optional attributes are row-oriented, while `values_f32_le` stores
/// `ids.len() * dimensions` coordinates in row-major order. The representation
/// reduces per-coordinate MessagePack overhead; conversion restores ordinary
/// owned [`VectorEntry`] values before shared validation and WAL publication.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnarUpsertRequest {
    /// Vector IDs in row order; entry `i` owns coordinate row `i`.
    ids: Vec<VectorId>,
    /// Positive number of `f32` coordinates in every row.
    dimensions: usize,
    /// Contiguous row-major IEEE-754 coordinates encoded as little-endian
    /// four-byte values.
    #[serde(deserialize_with = "deserialize_f32_le_bytes")]
    values_f32_le: Vec<f32>,
    /// Optional row-aligned metadata. A missing field supplies `None` for every
    /// ID; a present vector must have exactly one entry per ID.
    #[serde(default)]
    attributes: Option<Vec<Option<HashMap<String, AttributeValue>>>>,
}

/// Successful upsert acknowledgment returned after manifest publication.
///
/// The count is the number of request entries, not a deduplicated live-vector
/// count. Duplicate IDs in an upsert batch are currently accepted and still
/// contribute separately to this value.
///
/// # Examples
///
/// A two-entry request returns `{"upserted":2}` after its WAL fragment is
/// committed and the manifest cache receives the writer's committed snapshot.
#[derive(Debug, Serialize)]
pub struct UpsertVectorsResponse {
    /// Number of entries appended from the accepted request batch.
    pub upserted: usize,
    /// Server-owned identities returned in request-index order for rows whose
    /// input omitted `id`. Existing explicit-ID responses keep their historical
    /// shape by omitting this empty collection.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub generated_ids: Vec<GeneratedVectorId>,
}

/// Correlates one server-owned vector ID with its request position.
#[derive(Debug, Serialize)]
pub struct GeneratedVectorId {
    /// Zero-based index in the submitted `vectors` array.
    pub index: usize,
    /// Opaque stable ID used for future fetch, update, and delete operations.
    pub id: VectorId,
}

/// Request body for appending vector tombstones by ID.
///
/// Deletion is logical and append-only: the handler writes IDs into a new WAL
/// fragment and publishes that fragment through the manifest. It does not edit
/// an older fragment or segment in place.
///
/// # Examples
///
/// `{"ids":["item-7","item-8"]}` makes both IDs absent from later strong
/// reads after the manifest commit, whether or not either ID existed before.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteVectorsRequest {
    /// Tombstone IDs stored in request order. The handler requires at least one
    /// ID and applies the shared configured length and ASCII syntax rules.
    /// Duplicate IDs remain accepted and retain their request order.
    #[serde(default)]
    pub ids: Option<Vec<VectorId>>,
    /// Exact attribute predicate selecting live records to tombstone.
    #[serde(default)]
    pub filter: Option<Filter>,
}

/// Request body for ordered point lookup with explicit projection and freshness.
///
/// Point lookup is not similarity search: every requested ID is either returned
/// in [`GetVectorsResponse::results`] or copied into
/// [`GetVectorsResponse::missing`]. IDs must be unique and valid.
///
/// # Examples
///
/// ```text
/// {
///   "ids":["item-7","item-9"],
///   "include_vector":false,
///   "include_attributes":true,
///   "attribute_fields":["color"],
///   "consistency":"strong"
/// }
/// ```
///
/// A found `item-7` returns only its `color` attribute. An absent or tombstoned
/// `item-9` appears in `missing` at its request-relative position.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetVectorsRequest {
    /// Unique IDs to fetch. Found and missing outputs preserve this order.
    pub ids: Vec<VectorId>,
    /// Whether found records carry full vector coordinates; defaults to `true`.
    #[serde(default = "default_true")]
    pub include_vector: bool,
    /// Whether found records carry metadata maps; defaults to `true`.
    #[serde(default = "default_true")]
    pub include_attributes: bool,
    /// Optional metadata allow-list. It is valid only when attributes are
    /// included and must contain at least one non-empty field name.
    #[serde(default)]
    pub attribute_fields: Option<Vec<String>>,
    /// Freshness policy. Strong includes committed uncompacted WAL upserts;
    /// eventual may omit them but still applies effective WAL tombstones.
    #[serde(default)]
    pub consistency: ConsistencyLevel,
}

/// Partition of requested IDs into live records and absent logical records.
///
/// Both vectors preserve their relative request ordering. An ID is missing when
/// it has no visible WAL or active-segment value under the chosen consistency
/// snapshot, or when a visible tombstone suppresses it.
#[derive(Debug, Serialize)]
pub struct GetVectorsResponse {
    /// Found live records in the same relative order as their requested IDs.
    pub results: Vec<GetVectorRecord>,
    /// Requested IDs that were absent or tombstoned, preserving relative order.
    pub missing: Vec<VectorId>,
}

/// One live point-lookup record after applying the requested projection.
///
/// Omitted values serialize by absence rather than JSON `null`, allowing a
/// client to distinguish an excluded field from a present empty attribute map.
#[derive(Debug, Clone, Serialize)]
pub struct GetVectorRecord {
    /// Stable namespace-local identity of the record.
    pub id: VectorId,
    /// Owned full-precision coordinates, or `None` when vector projection was
    /// disabled. Segment lookup clones only the selected row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<f32>>,
    /// Owned projected metadata, or `None` when metadata was excluded, absent,
    /// or an allow-list selected no present fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Cheap projection policy borrowed from one validated lookup request.
///
/// The field slice is borrowed rather than cloned for every WAL or segment row.
/// Deriving `Copy` means passing this value copies two booleans and one slice
/// pair; it never duplicates the strings or extends their lifetime.
#[derive(Debug, Clone, Copy)]
struct FetchProjection<'a> {
    /// Whether returned records retain coordinate buffers.
    include_vector: bool,
    /// Whether returned records retain metadata maps.
    include_attributes: bool,
    /// Optional borrowed metadata allow-list; `None` includes every field.
    attribute_fields: Option<&'a [String]>,
}

/// Supplies the wire default for omitted lookup projection flags.
///
/// # Returns
///
/// Always returns `true`, preserving the API's full-record default.
///
/// # Examples
///
/// A get request containing only `{"ids":["item-7"]}` includes both vector
/// coordinates and attributes because Serde calls this helper for each missing
/// flag.
fn default_true() -> bool {
    true
}

/// Validates and commits one batch of vector upserts through the WAL.
///
/// The handler accepts row-oriented JSON by default and row- or column-oriented
/// MessagePack when the base `Content-Type` is `application/msgpack`. It rejects
/// the entire batch before durable work if the batch is empty or too large, an
/// ID is empty, oversized, or outside the supported ASCII alphabet, a coordinate
/// is non-finite, or a row dimension differs from namespace metadata.
///
/// Raw bytes are parsed here instead of using Axum's `Json` extractor so parse
/// failures pass through [`ApiError`] and receive Zeppelin's canonical JSON
/// envelope rather than Axum's independent rejection format.
///
/// # Parameters
///
/// - `state`: Request-scoped clone of the shared services and server limits.
/// - `ns`: Namespace name extracted from the URL. Namespace lookup confirms it
///   exists and supplies the required vector dimension.
/// - `headers`: Request headers used only to select MessagePack versus JSON.
/// - `body`: Complete request bytes, already subject to router body limits.
///
/// # Returns
///
/// HTTP `200 OK` and [`UpsertVectorsResponse`] containing the accepted entry
/// count after the fragment is durably referenced by a committed manifest.
///
/// # Errors
///
/// Returns validation or payload errors for malformed input and configured
/// limits, namespace errors for a missing/deleting namespace, and WAL, storage,
/// serialization, or manifest-conflict errors from publication. [`ApiError`]
/// maps these typed failures into the canonical error envelope.
///
/// Validation failures happen before a fragment PUT. Once `WalWriter::append`
/// starts, a failed manifest publication may have created an unreferenced
/// immutable fragment; the writer attempts exact-key cleanup and reports the
/// original failure if cleanup also fails.
///
/// # Side Effects
///
/// May read namespace metadata, upload one immutable WAL fragment, participate
/// in a group-committed manifest CAS, emit tracing/metrics, and insert the
/// committed manifest into the process-local manifest cache.
///
/// # Consistency
///
/// The response is issued only after the manifest commit, which is the
/// visibility boundary. The cache insertion cannot publish data and merely
/// avoids a subsequent manifest GET in this process. This unfenced handler uses
/// the v1 single-writer-per-namespace path; the WAL writer still protects the
/// manifest update with ETag CAS.
///
/// Repeating the same request is logically replace-by-ID but not artifact-level
/// idempotent: a successful retry appends another immutable fragment and
/// advances the manifest again.
///
/// # Performance
///
/// Parsing and validation are linear in request bytes and total coordinates.
/// Publication serializes and uploads the whole batch once, then needs manifest
/// read/CAS work; concurrent appends through the same writer may share a group
/// commit. The handler performs no index build—compaction handles that later.
///
/// # Examples
///
/// A two-row request for a 768-dimensional namespace is validated in full,
/// uploaded as one fragment, and acknowledged with `{"upserted":2}` after the
/// manifest references it. If row two has 767 dimensions, neither row is
/// written. If a manifest CAS ultimately fails after upload, the client receives
/// an error rather than a false acknowledgment.
///
/// # Rust Notes for Java/C Engineers
///
/// The handler borrows `&req.vectors` while validating, then moves the owned
/// vector into `append`. That move transfers every entry and heap buffer without
/// a deep copy and prevents later use through `req`. Java normally shares object
/// references here; C would require an explicit ownership-transfer convention.
/// The `?`-style conversions are written with `map_err` where the boundary must
/// wrap a domain error as [`ApiError`].
#[instrument(skip(state, decision, audit, headers, body), fields(namespace = %ns))]
pub async fn upsert_vectors(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(audit): Extension<AuditRequest>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<(StatusCode, Json<UpsertVectorsResponse>), ApiError> {
    let req = parse_upsert_request(&headers, &body)?;
    if req.vectors.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "vectors array cannot be empty".into(),
        )));
    }
    if req.vectors.len() > state.config.server.max_batch_size {
        return Err(ApiError(ZeppelinError::PayloadTooLarge {
            resource: "upsert batch",
            actual: req.vectors.len(),
            limit: state.config.server.max_batch_size,
        }));
    }

    for vector in &req.vectors {
        if let Some(id) = vector.id.as_ref() {
            if id.is_empty() {
                return Err(ApiError(ZeppelinError::Validation(
                    "vector id cannot be empty".into(),
                )));
            }
            if id.len() > state.config.server.max_vector_id_length {
                return Err(ApiError(ZeppelinError::Validation(format!(
                    "vector id length {} exceeds maximum of {}",
                    id.len(),
                    state.config.server.max_vector_id_length
                ))));
            }
            if !is_valid_vector_id(id) {
                return Err(ApiError(ZeppelinError::Validation(format!(
                    "vector id '{id}' contains invalid characters; \
                     only alphanumeric, dash, underscore, and dot are allowed"
                ))));
            }
        }
        // Reject NaN/inf before anything durable is written: one non-finite
        // value poisons distance orderings and k-means centroids permanently.
        if let Some((dim_idx, kind)) = super::find_non_finite(&vector.values) {
            let identity = vector.id.as_deref().unwrap_or("<server-owned>");
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector '{}' contains a non-finite value ({kind}) at dimension {dim_idx}",
                identity
            ))));
        }
    }

    let count = req.vectors.len();
    let mut generated_ids = Vec::new();
    let mut server_owned_ids = BTreeSet::new();
    let mut vectors = Vec::with_capacity(count);
    for (index, vector) in req.vectors.into_iter().enumerate() {
        let id = match vector.id.as_ref() {
            Some(id) => id.clone(),
            None if decision.mandatory_filter.is_some() => {
                let id = generate_server_owned_vector_id(state.config.server.max_vector_id_length)?;
                generated_ids.push(GeneratedVectorId {
                    index,
                    id: id.clone(),
                });
                server_owned_ids.insert(id.clone());
                id
            }
            None => {
                return Err(ApiError(ZeppelinError::Validation(
                    "vector id is required unless a mandatory write scope authorizes server-owned creation"
                        .into(),
                )));
            }
        };
        vectors.push(vector.into_vector_entry(id));
    }
    audit.set_params(AuditParams::vector_upsert(
        NamespaceId::new(ns.clone()).map_err(ZeppelinError::from)?,
        count,
        decision.is_attribute_admin_write(),
    ));

    info!(count, "upserting vectors");

    // Derived-row security checks require metadata from authoritative S3, not
    // a disposable registry snapshot. This also performs the one-time CAS
    // migration for namespaces created before incarnation metadata existed.
    let meta = if upsert_requires_existing_rows(&decision) {
        state
            .namespace_manager
            .get_active_metadata_for_guarded_write(&ns)
            .await
    } else {
        state.namespace_manager.get(&ns).await
    }
    .map_err(ApiError::from)?;

    for vec in &vectors {
        if vec.values.len() != meta.dimensions {
            // Name the offending vector: in a 50k-vector batch, "expected
            // 128, got 64" alone leaves the client hunting for the bad entry.
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector '{}' has dimension mismatch: expected {}, got {}",
                vec.id,
                meta.dimensions,
                vec.values.len()
            ))));
        }
    }

    let manifest_guard = apply_upsert_security_constraints(
        &state,
        &ns,
        &meta,
        &mut vectors,
        &server_owned_ids,
        &decision,
    )
    .await
    .map_err(ApiError::from)?;

    // WalWriter::append now does group commit internally (concurrent appends to
    // one namespace coalesce into a shared manifest CAS), so there is no
    // separate batch-writer path.
    let (_, manifest) = match manifest_guard {
        Some(guard) => {
            state
                .wal_writer
                .append_upserts_if_manifest_unchanged(&ns, vectors, guard)
                .await
        }
        None => state.wal_writer.append(&ns, vectors, vec![]).await,
    }
    .map_err(ApiError::from)?;

    // Write-through: insert fresh manifest so next query skips S3 GET.
    state.manifest_cache.insert(&ns, manifest);

    info!(upserted = count, "vectors upserted");
    Ok((
        StatusCode::OK,
        Json(UpsertVectorsResponse {
            upserted: count,
            generated_ids,
        }),
    ))
}

/// Generates one opaque vector identity for a constrained create.
fn generate_server_owned_vector_id(max_vector_id_length: usize) -> Result<VectorId, ApiError> {
    let id = format!("zv1_{}", ulid::Ulid::new());
    if id.len() > max_vector_id_length {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "server.max_vector_id_length must be at least {} for server-owned vector identities",
            id.len()
        ))));
    }
    Ok(id)
}

/// Validates and narrows a whole upsert batch against one authoritative snapshot.
async fn apply_upsert_security_constraints(
    state: &AppState,
    ns: &str,
    meta: &NamespaceMetadata,
    vectors: &mut [VectorEntry],
    server_owned_ids: &BTreeSet<VectorId>,
    decision: &AllowDecision,
) -> Result<Option<ManifestAppendGuard>, ZeppelinError> {
    let constraints = &decision.write_constraints;
    if vectors.iter().any(|vector| {
        vector.attributes.as_ref().is_some_and(|attributes| {
            attributes
                .keys()
                .any(|field| constraints.forbidden_fields().contains(field))
        })
    }) {
        return Err(crate::security::SecurityError::ConstraintViolation.into());
    }

    let needs_existing_rows = upsert_requires_existing_rows(decision);
    let (existing_rows, guard) = if needs_existing_rows {
        let expected_incarnation = required_namespace_incarnation(meta, ns)?;
        let (manifest, storage_version) = Manifest::read_versioned_required_for_incarnation(
            &state.store,
            ns,
            expected_incarnation,
        )
        .await?;
        let guard = ManifestAppendGuard::new(ns, &manifest, storage_version)?;
        let ids = vectors
            .iter()
            .map(|vector| vector.id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let response = fetch_vectors_by_id(
            state,
            ns,
            &ids,
            ConsistencyLevel::Strong,
            FetchProjection {
                include_vector: false,
                include_attributes: true,
                attribute_fields: None,
            },
            manifest,
        )
        .await?;
        let rows = response
            .results
            .into_iter()
            .map(|record| (record.id, record.attributes))
            .collect::<HashMap<_, _>>();
        (rows, Some(guard))
    } else {
        (HashMap::new(), None)
    };

    for vector in vectors {
        let is_server_owned_create = server_owned_ids.contains(&vector.id);
        if let Some(existing_attributes) = existing_rows.get(&vector.id) {
            // A generated identity is a create capability, never an implicit
            // update. Treat the vanishingly unlikely collision as a denied
            // batch so it cannot overwrite an existing row.
            if is_server_owned_create {
                return Err(crate::security::SecurityError::ConstraintViolation.into());
            }
            if decision.mandatory_filter.as_ref().is_some_and(|filter| {
                !existing_attributes
                    .as_ref()
                    .is_some_and(|attributes| filter_matches_write_scope(filter, attributes))
            }) {
                return Err(crate::security::SecurityError::ConstraintViolation.into());
            }

            if let Some(existing_attributes) = existing_attributes {
                let attributes = vector.attributes.get_or_insert_with(HashMap::new);
                for field in constraints.forbidden_fields() {
                    if constraints.stamp().contains_key(field) {
                        continue;
                    }
                    if let Some(value) = existing_attributes.get(field) {
                        attributes.insert(field.clone(), value.clone());
                    }
                }
            }
        } else if decision.mandatory_filter.is_some() && !is_server_owned_create {
            // Caller-chosen identities are update-only inside a mandatory
            // scope. Denying both an absent ID and a hidden collision closes
            // the write-side existence oracle; scoped creates receive an
            // opaque server-owned identity above.
            return Err(crate::security::SecurityError::ConstraintViolation.into());
        }

        if !constraints.stamp().is_empty() {
            let attributes = vector.attributes.get_or_insert_with(HashMap::new);
            for (field, value) in constraints.stamp() {
                attributes.insert(field.clone(), value.clone());
            }
        }

        if let Some(filter) = decision.mandatory_filter.as_ref() {
            if !vector
                .attributes
                .as_ref()
                .is_some_and(|attributes| filter_matches_write_scope(filter, attributes))
            {
                return Err(crate::security::SecurityError::ConstraintViolation.into());
            }
        }
    }
    Ok(guard)
}

/// Returns whether an upsert derives its accepted row from stored attributes.
fn upsert_requires_existing_rows(decision: &AllowDecision) -> bool {
    decision.mandatory_filter.is_some() || !decision.write_constraints.forbidden_fields().is_empty()
}

/// Returns the S3 metadata-backed namespace lifetime required by guarded writes.
fn required_namespace_incarnation(
    meta: &NamespaceMetadata,
    namespace: &str,
) -> Result<uuid::Uuid, ZeppelinError> {
    meta.incarnation_id
        .as_ref()
        .map(|incarnation| incarnation.as_uuid())
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "guarded write for namespace {namespace} requires namespace incarnation metadata"
            ))
        })
}

/// Decodes an upsert body according to its normalized media type.
///
/// # Parameters
///
/// - `headers`: Borrowed request headers. Only `Content-Type` is inspected.
/// - `body`: Borrowed complete request bytes; no copy is made before Serde
///   decoding.
///
/// # Returns
///
/// An owned row-oriented request regardless of the wire representation.
///
/// # Errors
///
/// Returns a validation error when the selected JSON or MessagePack decoder
/// rejects the body, or when the MessagePack envelope/columnar shape is invalid.
/// No namespace or object-store work has happened.
///
/// # Examples
///
/// `Content-Type: application/msgpack; charset=binary` selects MessagePack.
/// Missing content type selects JSON. A body labeled `text/plain` is therefore
/// attempted as JSON and fails with the normal validation envelope if invalid.
fn parse_upsert_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<UpsertVectorsRequest, ApiError> {
    if is_msgpack_content_type(headers) {
        parse_msgpack_upsert_request(body)
    } else {
        serde_json::from_slice(body).map_err(|e| {
            ApiError(ZeppelinError::Validation(format!(
                "invalid request body: {e}"
            )))
        })
    }
}

/// Recognizes the MessagePack media type while ignoring parameters and case.
///
/// # Parameters
///
/// - `headers`: Borrowed request headers to inspect.
///
/// # Returns
///
/// `true` only when the first semicolon-delimited `Content-Type` component,
/// after trimming, equals `application/msgpack` case-insensitively. Missing,
/// non-UTF-8, and other values return `false` and therefore select JSON.
///
/// # Examples
///
/// `Application/MsgPack; profile=bulk` returns `true`; `application/json` and a
/// missing header return `false`.
fn is_msgpack_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/msgpack"))
}

/// Narrows a MessagePack envelope to exactly one supported ingestion shape.
///
/// # Parameters
///
/// - `body`: Complete MessagePack bytes borrowed for decoding.
///
/// # Returns
///
/// An owned row-oriented [`UpsertVectorsRequest`]. A row envelope moves its
/// existing entries directly; a columnar envelope allocates one coordinate
/// vector per ID during conversion.
///
/// # Errors
///
/// Returns validation for malformed MessagePack, unknown fields, both `vectors`
/// and `columnar`, neither field, or an invalid columnar layout. No durable work
/// occurs.
///
/// # Examples
///
/// `{vectors: [...]}` and `{columnar: {...}}` are accepted. An envelope carrying
/// both is rejected rather than choosing one and silently ignoring the other.
fn parse_msgpack_upsert_request(body: &[u8]) -> Result<UpsertVectorsRequest, ApiError> {
    let req: MessagePackUpsertRequest = rmp_serde::from_slice(body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;

    match (req.vectors, req.columnar) {
        (Some(vectors), None) => Ok(UpsertVectorsRequest { vectors }),
        (None, Some(columnar)) => columnar.into_upsert_request(),
        (Some(_), Some(_)) | (None, None) => Err(ApiError(ZeppelinError::Validation(
            "msgpack upsert body must contain exactly one of vectors or columnar".into(),
        ))),
    }
}

impl ColumnarUpsertRequest {
    /// Reconstructs row-oriented vector entries from aligned columnar buffers.
    ///
    /// # Parameters
    ///
    /// - `self`: Owned columnar request. Conversion consumes its ID, coordinate,
    ///   and metadata buffers.
    ///
    /// # Returns
    ///
    /// A row-oriented request in original ID order. Every output row owns
    /// exactly `dimensions` coordinates and its corresponding optional metadata.
    /// Missing metadata input becomes one `None` entry per ID.
    ///
    /// # Errors
    ///
    /// Returns validation when dimensions are zero, `ids.len() * dimensions`
    /// overflows `usize`, the coordinate count is not exactly that product, or
    /// a supplied attributes vector has a different row count. ID syntax,
    /// finite coordinates, batch size, and namespace dimension are checked later
    /// by [`upsert_vectors`].
    ///
    /// # Performance
    ///
    /// Allocates one output vector and one `Vec<f32>` per row, copying each
    /// coordinate once from the contiguous decoded buffer. IDs and metadata are
    /// moved rather than cloned.
    ///
    /// # Examples
    ///
    /// IDs `[a, b]`, dimensions `2`, and values `[1, 2, 3, 4]` become rows
    /// `a -> [1, 2]` and `b -> [3, 4]`. Three values fail before any row is
    /// published.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `chunks_exact` exposes checked borrowed slices into the coordinate buffer;
    /// `zip` then walks IDs, slices, and metadata in lockstep. Because prior
    /// length checks prove the iterators align, no row is silently dropped.
    /// Consuming `self` lets Rust move strings/maps into results and automatically
    /// free the original coordinate buffer after its rows have been copied.
    fn into_upsert_request(self) -> Result<UpsertVectorsRequest, ApiError> {
        if self.dimensions == 0 {
            return Err(ApiError(ZeppelinError::Validation(
                "columnar dimensions must be greater than zero".into(),
            )));
        }
        let expected_values = self.ids.len().checked_mul(self.dimensions).ok_or_else(|| {
            ApiError(ZeppelinError::Validation(
                "columnar values length overflows usize".into(),
            ))
        })?;
        if self.values_f32_le.len() != expected_values {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "columnar values_f32_le contains {} floats, expected {} ({} ids * {} dimensions)",
                self.values_f32_le.len(),
                expected_values,
                self.ids.len(),
                self.dimensions
            ))));
        }
        if let Some(attributes) = self.attributes.as_ref() {
            if attributes.len() != self.ids.len() {
                return Err(ApiError(ZeppelinError::Validation(format!(
                    "columnar attributes contains {} entries, expected {}",
                    attributes.len(),
                    self.ids.len()
                ))));
            }
        }

        let attributes = self
            .attributes
            .unwrap_or_else(|| vec![None; self.ids.len()]);
        let vectors = self
            .ids
            .into_iter()
            .zip(self.values_f32_le.chunks_exact(self.dimensions))
            .zip(attributes)
            .map(|((id, values), attributes)| UpsertVectorInput {
                id: Some(id),
                values: values.to_vec(),
                attributes,
            })
            .collect();
        Ok(UpsertVectorsRequest { vectors })
    }
}

/// Deserializes a MessagePack byte string into little-endian `f32` values.
///
/// The custom visitor also accepts an owned byte buffer or a sequence of byte
/// values because different Serde data formats can expose byte payloads through
/// different visitor methods.
///
/// # Parameters
///
/// - `deserializer`: Serde decoder positioned at `values_f32_le`.
///
/// # Returns
///
/// An owned vector containing one native `f32` per four input bytes, preserving
/// wire order.
///
/// # Errors
///
/// Returns the decoder's error for an incompatible value or a custom error when
/// the byte length is not divisible by four.
///
/// # Examples
///
/// Bytes `00 00 80 3f 00 00 00 c0` decode to `[1.0, -2.0]` on both
/// little- and big-endian hosts. A five-byte payload is rejected.
///
/// # Rust Notes for Java/C Engineers
///
/// The generic `Deserializer` and `Visitor` are compile-time trait dispatch,
/// comparable to a Java generic parser interface or a C table of callbacks.
/// Rust monomorphizes the concrete decoder while the visitor methods express
/// all accepted wire representations without unsafe pointer casts.
fn deserialize_f32_le_bytes<'de, D>(deserializer: D) -> Result<Vec<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    /// Serde visitor that normalizes borrowed, owned, or sequenced bytes.
    struct F32LeVisitor;

    impl<'de> Visitor<'de> for F32LeVisitor {
        type Value = Vec<f32>;

        /// Describes the expected wire value in decoder error messages.
        ///
        /// # Parameters
        ///
        /// - `formatter`: Borrowed formatter supplied by Serde.
        ///
        /// # Returns
        ///
        /// The formatter result after writing the short expectation text.
        ///
        /// # Examples
        ///
        /// A type mismatch can report that `little-endian f32 bytes` were
        /// expected.
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("little-endian f32 bytes")
        }

        /// Decodes a borrowed byte slice without first cloning the byte payload.
        ///
        /// # Parameters
        ///
        /// - `value`: Decoder-owned bytes borrowed for this call.
        ///
        /// # Returns
        ///
        /// Owned decoded coordinates, or a Serde error for invalid byte length.
        ///
        /// # Examples
        ///
        /// A MessagePack decoder that can lend its input uses this path.
        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            f32_values_from_le_bytes(value)
        }

        /// Decodes bytes whose buffer ownership has already moved from Serde.
        ///
        /// # Parameters
        ///
        /// - `value`: Owned encoded bytes; decoding borrows them temporarily.
        ///
        /// # Returns
        ///
        /// Owned decoded coordinates, or a Serde error for invalid byte length.
        ///
        /// # Examples
        ///
        /// A decoder that materializes an owned binary field uses this path; the
        /// encoded buffer is released after conversion.
        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            f32_values_from_le_bytes(&value)
        }

        /// Collects a Serde byte sequence before decoding it as one binary field.
        ///
        /// # Parameters
        ///
        /// - `seq`: Sequence accessor yielding byte values.
        ///
        /// # Returns
        ///
        /// Owned decoded coordinates. Sequence decode errors and invalid total
        /// length propagate through the accessor's error type.
        ///
        /// # Examples
        ///
        /// An array `[0, 0, 128, 63]` is collected and decoded as `1.0`.
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::new();
            while let Some(byte) = seq.next_element()? {
                bytes.push(byte);
            }
            f32_values_from_le_bytes(&bytes)
        }
    }

    deserializer.deserialize_bytes(F32LeVisitor)
}

/// Converts a complete little-endian byte slice into native `f32` values.
///
/// # Parameters
///
/// - `bytes`: Borrowed encoded coordinates whose length must be a multiple of
///   `std::mem::size_of::<f32>()`.
///
/// # Returns
///
/// An owned vector in input order. Empty bytes produce an empty vector, which
/// later request validation may reject depending on row layout.
///
/// # Errors
///
/// Returns a caller-selected Serde error when a partial four-byte value remains.
///
/// # Performance
///
/// Allocates exactly one result vector and performs one endian-aware conversion
/// per coordinate in linear time.
///
/// # Examples
///
/// Eight bytes representing `1.0` and `-2.0` return two values; six bytes fail
/// rather than truncating the final coordinate.
fn f32_values_from_le_bytes<E>(bytes: &[u8]) -> Result<Vec<f32>, E>
where
    E: de::Error,
{
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        return Err(E::custom(format!(
            "values_f32_le byte length {} is not divisible by {}",
            bytes.len(),
            std::mem::size_of::<f32>()
        )));
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Commits one batch of vector tombstones through the WAL.
///
/// Deletion is an immutable logical operation. The handler verifies that the
/// JSON body contains at least one ID and that the namespace exists, then moves
/// the IDs into a new WAL fragment. It does not fetch the old records, require
/// that they exist, or edit segment objects in place.
///
/// # Parameters
///
/// - `state`: Request-scoped clone of shared namespace, WAL, cache, and storage
///   services.
/// - `ns`: Namespace extracted from the URL.
/// - `body`: Complete JSON request bytes. Unlike upsert, delete does not select
///   a MessagePack representation from `Content-Type`.
///
/// # Returns
///
/// HTTP `204 No Content` after the authoritative manifest references the
/// tombstone fragment.
///
/// # Errors
///
/// Returns validation for malformed JSON, an empty ID list, or an ID that
/// violates the configured byte-length or ASCII syntax contract; a namespace
/// error when the namespace is absent/deleting; and WAL, storage,
/// serialization, or manifest-publication errors. The handler does not apply
/// the upsert batch-size or duplicate-ID checks to deletes.
///
/// ID validation runs before namespace storage I/O and before request values
/// are projected into audit parameters. Validation and namespace failures
/// create no fragment. Publication failure can occur after fragment upload;
/// [`crate::wal::WalWriter`] owns best-effort orphan cleanup and returns the
/// original error rather than claiming success.
///
/// # Side Effects
///
/// May read namespace metadata, upload one immutable WAL fragment, publish a
/// manifest generation with CAS, emit tracing/metrics, and write the committed
/// manifest through to the local manifest cache.
///
/// # Consistency
///
/// After acknowledgment, strong lookup observes the tombstones immediately
/// through the committed manifest. Eventual lookup deliberately skips WAL
/// upserts but reads delete-bearing fragments so compacted records do not
/// reappear. A later committed upsert for the same ID supersedes the tombstone.
///
/// Repeating a delete is logically idempotent but creates another immutable
/// fragment and manifest generation because this route has no idempotency key.
///
/// # Performance
///
/// JSON parsing and fragment construction are linear in ID bytes. Publication
/// performs one fragment PUT plus manifest read/CAS work, which may be shared
/// with concurrent same-process appends through group commit. The handler does
/// not read cluster or attribute objects.
///
/// # Examples
///
/// Deleting `item-7` after it was compacted appends a tombstone; the segment
/// remains immutable, but both strong and eventual point lookup suppress its
/// row. Deleting an unknown ID also succeeds and records a tombstone. An empty
/// list returns a validation error without writing anything.
///
/// # Rust Notes for Java/C Engineers
///
/// `req.ids` is moved into `append`, transferring its allocation into the WAL
/// fragment without cloning every string. Rust prevents this handler from using
/// that vector afterward. Java has no compiler-enforced move; C would need an
/// explicit “callee now owns this allocation” rule.
#[instrument(skip(state, decision, audit, body), fields(namespace = %ns))]
pub async fn delete_vectors(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(audit): Extension<AuditRequest>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<StatusCode, ApiError> {
    let req: DeleteVectorsRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    let selection = validate_delete_vectors_request(req, state.config.server.max_vector_id_length)
        .map_err(ApiError::from)?;

    if let (DeleteSelection::Filter(caller_filter), Some(mask)) =
        (&selection, decision.field_mask.as_ref())
    {
        if filter_references_denied_field(caller_filter, mask.denied_fields()) {
            return Err(ApiError(SecurityError::ConstraintViolation.into()));
        }
    }

    let namespace_id = NamespaceId::new(ns.clone()).map_err(|error| ApiError(error.into()))?;
    let preservation_filter = match &selection {
        DeleteSelection::Ids(_) => None,
        DeleteSelection::Filter(filter) => Some(filter),
    };
    let guard = state
        .security
        .guard_vector_destruction(&namespace_id, preservation_filter)
        .map_err(|error| ApiError(error.into()))?;
    if guard.is_locked() {
        audit.set_params(AuditParams::preservation_blocked(
            crate::security::PreservationBlockedSurface::VectorDelete,
            &guard,
        ));
        return Err(ApiError(SecurityError::PreservationLocked.into()));
    }

    let requires_guard =
        decision.mandatory_filter.is_some() || matches!(&selection, DeleteSelection::Filter(_));
    let meta = if requires_guard {
        state
            .namespace_manager
            .get_active_metadata_for_guarded_write(&ns)
            .await
    } else {
        state.namespace_manager.get(&ns).await
    }
    .map_err(ApiError::from)?;

    let (delete_ids, guard) = match selection {
        DeleteSelection::Ids(ids) if decision.mandatory_filter.is_none() => (ids, None),
        DeleteSelection::Ids(ids) => {
            let expected_incarnation =
                required_namespace_incarnation(&meta, &ns).map_err(ApiError::from)?;
            let (manifest, storage_version) = Manifest::read_versioned_required_for_incarnation(
                &state.store,
                &ns,
                expected_incarnation,
            )
            .await
            .map_err(ApiError::from)?;
            let guard = ManifestAppendGuard::new(&ns, &manifest, storage_version)
                .map_err(ApiError::from)?;
            let filter = decision.mandatory_filter.as_ref().ok_or_else(|| {
                ApiError(ZeppelinError::Index(
                    "constrained ID delete lost its mandatory filter".into(),
                ))
            })?;
            let ids = select_requested_ids_matching_filter(&state, &ns, &ids, filter, manifest)
                .await
                .map_err(ApiError::from)?;
            (ids, Some(guard))
        }
        DeleteSelection::Filter(caller_filter) => {
            let expected_incarnation =
                required_namespace_incarnation(&meta, &ns).map_err(ApiError::from)?;
            let (manifest, storage_version) = Manifest::read_versioned_required_for_incarnation(
                &state.store,
                &ns,
                expected_incarnation,
            )
            .await
            .map_err(ApiError::from)?;
            let guard = ManifestAppendGuard::new(&ns, &manifest, storage_version)
                .map_err(ApiError::from)?;
            let effective_filter =
                combine_filters(decision.mandatory_filter.clone(), Some(caller_filter))
                    .ok_or_else(|| {
                        ApiError(ZeppelinError::Index(
                            "filter delete did not produce an effective filter".into(),
                        ))
                    })?;
            let ids = select_all_ids_matching_filter(&state, &ns, &effective_filter, manifest)
                .await
                .map_err(ApiError::from)?;
            (ids, Some(guard))
        }
    };

    let count = delete_ids.len();
    audit.set_params(AuditParams::vector_delete(
        NamespaceId::new(ns.clone()).map_err(ZeppelinError::from)?,
        &delete_ids,
    ));
    if delete_ids.is_empty() {
        info!(deleted = 0, "no scoped vectors matched delete request");
        return Ok(StatusCode::NO_CONTENT);
    }

    info!(count, "deleting vectors");
    let (_, manifest) = match guard {
        Some(guard) => {
            state
                .wal_writer
                .append_deletes_if_manifest_unchanged(&ns, delete_ids, guard)
                .await
        }
        None => state.wal_writer.append(&ns, vec![], delete_ids).await,
    }
    .map_err(ApiError::from)?;

    // Write-through: insert fresh manifest so next query skips S3 GET.
    state.manifest_cache.insert(&ns, manifest);

    info!(deleted = count, "vectors deleted");
    Ok(StatusCode::NO_CONTENT)
}

enum DeleteSelection {
    Ids(Vec<VectorId>),
    Filter(Filter),
}

fn validate_delete_vectors_request(
    req: DeleteVectorsRequest,
    max_vector_id_length: usize,
) -> Result<DeleteSelection, ZeppelinError> {
    match (req.ids, req.filter) {
        (Some(ids), None) => {
            if ids.is_empty() {
                return Err(ZeppelinError::Validation(
                    "ids array cannot be empty".into(),
                ));
            }
            for id in &ids {
                validate_vector_id_for_request(id, max_vector_id_length)?;
            }
            Ok(DeleteSelection::Ids(ids))
        }
        (None, Some(filter)) => Ok(DeleteSelection::Filter(filter)),
        (Some(_), Some(_)) | (None, None) => Err(ZeppelinError::Validation(
            "delete request must contain exactly one of ids or filter".into(),
        )),
    }
}

async fn select_requested_ids_matching_filter(
    state: &AppState,
    ns: &str,
    requested_ids: &[VectorId],
    filter: &Filter,
    manifest: Manifest,
) -> Result<Vec<VectorId>, ZeppelinError> {
    let unique_ids = requested_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let response = fetch_vectors_by_id(
        state,
        ns,
        &unique_ids,
        ConsistencyLevel::Strong,
        FetchProjection {
            include_vector: false,
            include_attributes: true,
            attribute_fields: None,
        },
        manifest,
    )
    .await?;
    let matching = response
        .results
        .into_iter()
        .filter(|record| {
            record
                .attributes
                .as_ref()
                .is_some_and(|attributes| evaluate_filter(filter, attributes))
        })
        .map(|record| record.id)
        .collect::<HashSet<_>>();
    Ok(requested_ids
        .iter()
        .filter(|id| matching.contains(*id))
        .cloned()
        .collect())
}

async fn select_all_ids_matching_filter(
    state: &AppState,
    ns: &str,
    filter: &Filter,
    manifest: Manifest,
) -> Result<Vec<VectorId>, ZeppelinError> {
    let mut ids = BTreeSet::new();
    if let Some(segment) = active_segment(&manifest)? {
        let membership_ref = segment.membership.as_ref().ok_or_else(|| {
            ZeppelinError::Membership("filter delete requires segment membership artifact".into())
        })?;
        let membership = deserialize_membership(&state.store.get(&membership_ref.key).await?)?;
        ids.extend(membership.entries.into_iter().map(|(id, _)| id));
    }
    let fragments = state
        .wal_reader
        .read_fragments_from_refs_unchecked(
            ns,
            manifest.uncompacted_fragments(),
            FragmentCachePolicy::ReadWrite(&state.cache),
        )
        .await?;
    for fragment in fragments {
        ids.extend(fragment.vectors.into_iter().map(|vector| vector.id));
        ids.extend(fragment.deletes);
    }

    let ids = ids.into_iter().collect::<Vec<_>>();
    let response = fetch_vectors_by_id(
        state,
        ns,
        &ids,
        ConsistencyLevel::Strong,
        FetchProjection {
            include_vector: false,
            include_attributes: true,
            attribute_fields: None,
        },
        manifest,
    )
    .await?;
    Ok(response
        .results
        .into_iter()
        .filter(|record| {
            record
                .attributes
                .as_ref()
                .is_some_and(|attributes| evaluate_filter(filter, attributes))
        })
        .map(|record| record.id)
        .collect())
}

/// Fetches live records by ID under an explicit projection and consistency mode.
///
/// The handler validates an ordered, duplicate-free JSON request, verifies the
/// namespace, obtains one manifest snapshot through `read_manifest_for_query`,
/// and delegates resolution to `fetch_vectors_by_id`. The response partitions
/// every requested ID into a projected live record or the missing list.
///
/// # Parameters
///
/// - `state`: Request-scoped clone of shared storage, WAL readers, caches, and
///   server limits.
/// - `ns`: Namespace extracted from the URL.
/// - `body`: Complete JSON request bytes, already subject to router body limits.
///
/// # Returns
///
/// A JSON [`GetVectorsResponse`]. Found records and missing IDs each retain
/// their relative request order.
///
/// # Errors
///
/// Returns validation for malformed JSON, invalid or duplicate IDs, and
/// inconsistent projection options; namespace errors for an absent/deleting
/// namespace; and manifest, WAL, membership, storage, or segment-decoding
/// errors from lookup. A manifest-selected missing/corrupt artifact is an
/// internal server failure, never a fabricated missing ID.
///
/// The active segment must carry a membership artifact. A legacy descriptor
/// without one fails explicitly when unresolved IDs require segment lookup;
/// this handler does not fall back to scanning clusters.
///
/// # Side Effects
///
/// Performs read-only domain work but may refresh/populate the manifest cache,
/// populate the immutable WAL disk cache, issue object-store GETs, and emit
/// tracing/metrics. It never publishes a manifest or modifies an artifact.
///
/// # Consistency
///
/// Strong mode revalidates the manifest against S3/MinIO and replays every
/// visible uncompacted WAL fragment before consulting the active segment.
/// Eventual mode may use a TTL-cached manifest and skips WAL upserts, so it may
/// miss a new ID or return its older compacted value; it still loads effective
/// WAL tombstones represented by the selected manifest.
///
/// All phases use the same owned manifest snapshot. A later publication cannot
/// partly alter this request's chosen fragment or segment set.
///
/// # Performance
///
/// Validation is linear in requested IDs. A strong read may issue one manifest
/// conditional GET and reads visible WAL fragments concurrently through the
/// disk cache. Eventual reads only delete-bearing fragments. Segment lookup
/// performs one full membership GET, then at most one cluster GET per selected
/// cluster and one attribute GET per selected cluster when attributes are
/// requested. ID-only projection avoids cluster and attribute GETs.
///
/// # Examples
///
/// Suppose `item-7` was compacted, updated in a visible WAL fragment, `item-8`
/// was tombstoned, and `item-9` never existed. A strong request returns the new
/// `item-7` and reports `item-8` and `item-9` missing. An eventual request may
/// return the old compacted `item-7`, still reports tombstoned `item-8` missing,
/// and never invents `item-9`.
///
/// # Rust Notes for Java/C Engineers
///
/// `req.attribute_fields.as_deref()` converts `Option<Vec<String>>` into an
/// optional borrowed slice. Helpers can inspect field names without taking the
/// request apart or allocating another list. The owned `manifest` is moved into
/// the resolver, guaranteeing one stable snapshot across awaits; Java would
/// rely on convention not to swap a shared object, while C would require
/// explicit lifetime management.
#[instrument(skip(state, decision, body), fields(namespace = %ns))]
pub async fn get_vectors(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<Json<GetVectorsResponse>, ApiError> {
    let req: GetVectorsRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    validate_get_vectors_request(&req, state.config.server.max_vector_id_length)
        .map_err(ApiError::from)?;

    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    let manifest = query::read_manifest_for_query(
        &state.store,
        &ns,
        req.consistency,
        Some(&state.manifest_cache),
    )
    .await
    .map_err(ApiError::from)?;

    let caller_projection = FetchProjection {
        include_vector: req.include_vector,
        include_attributes: req.include_attributes,
        attribute_fields: req.attribute_fields.as_deref(),
    };
    let storage_projection = if decision.mandatory_filter.is_some() {
        FetchProjection {
            include_vector: req.include_vector,
            include_attributes: true,
            attribute_fields: None,
        }
    } else {
        caller_projection
    };
    let response = fetch_vectors_by_id(
        &state,
        &ns,
        &req.ids,
        req.consistency,
        storage_projection,
        manifest,
    )
    .await
    .map_err(ApiError::from)?;
    let response = apply_fetch_security_constraints(
        &req.ids,
        response,
        caller_projection,
        decision.mandatory_filter.as_ref(),
        decision.field_mask.as_ref(),
    );

    info!(
        found = response.results.len(),
        missing = response.missing.len(),
        "vectors fetched"
    );
    Ok(Json(response))
}

/// Applies row scoping, caller projection, and field masking in request order.
fn apply_fetch_security_constraints(
    requested_ids: &[VectorId],
    response: GetVectorsResponse,
    caller_projection: FetchProjection<'_>,
    mandatory_filter: Option<&crate::types::Filter>,
    field_mask: Option<&FieldMask>,
) -> GetVectorsResponse {
    let mut found = response
        .results
        .into_iter()
        .map(|record| (record.id.clone(), record))
        .collect::<HashMap<_, _>>();
    let mut results = Vec::with_capacity(found.len());
    let mut missing = Vec::with_capacity(response.missing.len());

    for id in requested_ids {
        let Some(mut record) = found.remove(id) else {
            missing.push(id.clone());
            continue;
        };
        if mandatory_filter.is_some_and(|filter| {
            !record
                .attributes
                .as_ref()
                .is_some_and(|attributes| evaluate_filter(filter, attributes))
        }) {
            missing.push(id.clone());
            continue;
        }

        record.attributes = project_attributes(record.attributes, caller_projection);
        if let (Some(mask), Some(attributes)) = (field_mask, record.attributes.as_mut()) {
            apply_field_mask(mask, attributes);
            if attributes.is_empty() {
                record.attributes = None;
            }
        }
        results.push(record);
    }

    GetVectorsResponse { results, missing }
}

/// Validates the cross-field and per-ID contract for point lookup.
///
/// # Parameters
///
/// - `req`: Borrowed decoded request; validation does not consume or mutate it.
/// - `max_vector_id_length`: Configured maximum ID length in UTF-8 bytes.
///
/// # Returns
///
/// `Ok(())` when IDs and projection options can be resolved unambiguously.
///
/// # Errors
///
/// Returns [`ZeppelinError::Validation`] for an empty ID list, an invalid ID,
/// duplicate IDs, `attribute_fields` while attributes are disabled, an empty
/// field allow-list, or an empty field name. All checks are local and have no
/// partial side effects.
///
/// # Performance
///
/// Allocates a hash table sized for the IDs and scans IDs/field names once.
/// The set borrows ID text, so it does not clone each string.
///
/// # Examples
///
/// IDs `[a, b]` with `attribute_fields=[color]` and attributes enabled pass.
/// IDs `[a, a]`, or a field allow-list while `include_attributes=false`, fail
/// before any manifest read.
///
/// # Rust Notes for Java/C Engineers
///
/// `HashSet<&String>` stores checked borrows into the request instead of owning
/// duplicate strings. Rust proves those references cannot outlive `req`. Java's
/// set would store object references with garbage-collected lifetime; C would
/// need to ensure every pointed-to string remains allocated manually.
fn validate_get_vectors_request(
    req: &GetVectorsRequest,
    max_vector_id_length: usize,
) -> Result<(), ZeppelinError> {
    if req.ids.is_empty() {
        return Err(ZeppelinError::Validation(
            "ids array cannot be empty".into(),
        ));
    }
    let mut seen = HashSet::with_capacity(req.ids.len());
    for id in &req.ids {
        validate_vector_id_for_request(id, max_vector_id_length)?;
        if !seen.insert(id) {
            return Err(ZeppelinError::Validation(format!(
                "duplicate vector id '{id}' in request"
            )));
        }
    }

    if !req.include_attributes && req.attribute_fields.is_some() {
        return Err(ZeppelinError::Validation(
            "attribute_fields requires include_attributes=true".into(),
        ));
    }
    if let Some(fields) = req.attribute_fields.as_ref() {
        if fields.is_empty() {
            return Err(ZeppelinError::Validation(
                "attribute_fields cannot be empty".into(),
            ));
        }
        if fields.iter().any(String::is_empty) {
            return Err(ZeppelinError::Validation(
                "attribute_fields cannot contain empty field names".into(),
            ));
        }
    }

    Ok(())
}

/// Validates one externally supplied vector ID against the shared HTTP rules.
///
/// Query handlers reuse this helper when an ID is used as a vector source, so
/// point lookup and reranking accept the same namespace-local identifier shape.
///
/// # Parameters
///
/// - `id`: Borrowed UTF-8 ID from a decoded request.
/// - `max_vector_id_length`: Configured maximum in bytes.
///
/// # Returns
///
/// `Ok(())` for a non-empty, within-limit ID containing only ASCII letters,
/// digits, dash, underscore, or dot.
///
/// # Errors
///
/// Returns [`ZeppelinError::Validation`] naming the violated empty, length, or
/// character rule. The function performs no I/O or mutation.
///
/// # Performance
///
/// Runs in `O(id.len())` time with no allocation on success. Error formatting
/// allocates only on failure.
///
/// # Examples
///
/// `product-42_v2.1` passes. An empty string, `product/42`, and a non-ASCII ID
/// fail. An ID exactly equal to the configured byte limit passes.
pub(crate) fn validate_vector_id_for_request(
    id: &str,
    max_vector_id_length: usize,
) -> Result<(), ZeppelinError> {
    if id.is_empty() {
        return Err(ZeppelinError::Validation(
            "vector id cannot be empty".into(),
        ));
    }
    if id.len() > max_vector_id_length {
        return Err(ZeppelinError::Validation(format!(
            "vector id length {} exceeds maximum of {}",
            id.len(),
            max_vector_id_length
        )));
    }
    if !is_valid_vector_id(id) {
        return Err(ZeppelinError::Validation(format!(
            "vector id '{}' contains invalid characters; \
             only alphanumeric, dash, underscore, and dot are allowed",
            id
        )));
    }
    Ok(())
}

/// Resolves one vector's coordinates for query features that reference an ID.
///
/// This is the single-ID adapter used by query/rerank code. It applies the same
/// WAL-versus-segment visibility rules as the HTTP point-lookup endpoint but
/// projects away attributes.
///
/// # Parameters
///
/// - `state`: Shared services used for WAL, membership, and segment reads.
/// - `ns`: Namespace that owns both the ID and supplied manifest.
/// - `id`: Borrowed ID. The caller is responsible for prior syntax validation.
/// - `consistency`: WAL freshness policy that must match how `manifest` was
///   selected.
/// - `manifest`: Owned visibility snapshot consumed by the lookup.
///
/// # Returns
///
/// `Some` with owned full-precision coordinates for a live record, or `None`
/// when the ID is absent or tombstoned under the selected snapshot.
///
/// # Errors
///
/// Propagates WAL/cache/storage, membership, active-segment, cluster-decoding,
/// and internal projection invariant failures. A corrupt or missing artifact is
/// an error, not `None`.
///
/// # Side Effects
///
/// May read or populate the WAL disk cache and issue direct membership/cluster
/// object-store GETs. It does not mutate authoritative state.
///
/// # Consistency
///
/// Strong includes visible WAL upserts and tombstones; eventual skips WAL
/// upserts but applies effective tombstones before consulting the active
/// segment. The supplied manifest, rather than a fresh read here, fixes the
/// artifact set.
///
/// # Performance
///
/// Allocates one owned ID for the batch adapter. Storage cost otherwise matches
/// `fetch_vector_values_by_ids`; a compacted lookup loads the full membership
/// artifact and the containing cluster object.
///
/// # Examples
///
/// Query-by-ID for `item-7` returns its just-committed WAL coordinates in strong
/// mode. If `item-7` is tombstoned, it returns `None`; if its manifest-selected
/// cluster object is missing, it returns an error.
///
/// # Rust Notes for Java/C Engineers
///
/// `id.to_string()` deliberately creates one owned string because the batch
/// function accepts owned [`VectorId`] values that can safely survive awaits.
/// The returned `Vec<f32>` owns its buffer and is independent of temporary WAL
/// or cluster decode values.
#[allow(dead_code)]
pub(crate) async fn fetch_vector_values_by_id(
    state: &AppState,
    ns: &str,
    id: &str,
    consistency: ConsistencyLevel,
    manifest: Manifest,
) -> Result<Option<Vec<f32>>, ZeppelinError> {
    Ok(
        fetch_vector_values_by_id_with_trace(state, ns, id, consistency, manifest)
            .await?
            .0,
    )
}

/// Resolves one vector while retaining the exact segment artifacts consumed.
pub(crate) async fn fetch_vector_values_by_id_with_trace(
    state: &AppState,
    ns: &str,
    id: &str,
    consistency: ConsistencyLevel,
    manifest: Manifest,
) -> Result<(Option<Vec<f32>>, BTreeSet<String>), ZeppelinError> {
    let (values, touched) =
        fetch_vector_values_by_ids_with_trace(state, ns, &[id.to_string()], consistency, manifest)
            .await?;
    Ok((values.into_iter().next().map(|(_, values)| values), touched))
}

/// Resolves a stored query seed while enforcing the current mandatory filter.
#[allow(dead_code)]
pub(crate) async fn fetch_vector_values_by_id_scoped(
    state: &AppState,
    ns: &str,
    id: &str,
    consistency: ConsistencyLevel,
    manifest: Manifest,
    mandatory_filter: Option<&crate::types::Filter>,
) -> Result<Option<Vec<f32>>, ZeppelinError> {
    Ok(fetch_vector_values_by_id_scoped_with_trace(
        state,
        ns,
        id,
        consistency,
        manifest,
        mandatory_filter,
    )
    .await?
    .0)
}

/// Resolves a scoped query seed and returns its exact immutable segment reads.
pub(crate) async fn fetch_vector_values_by_id_scoped_with_trace(
    state: &AppState,
    ns: &str,
    id: &str,
    consistency: ConsistencyLevel,
    manifest: Manifest,
    mandatory_filter: Option<&crate::types::Filter>,
) -> Result<(Option<Vec<f32>>, BTreeSet<String>), ZeppelinError> {
    if mandatory_filter.is_none() {
        return fetch_vector_values_by_id_with_trace(state, ns, id, consistency, manifest).await;
    }
    let projection = FetchProjection {
        include_vector: true,
        include_attributes: true,
        attribute_fields: None,
    };
    let (response, touched) = fetch_vectors_by_id_with_trace(
        state,
        ns,
        &[id.to_string()],
        consistency,
        projection,
        manifest,
    )
    .await?;
    let Some(record) = response.results.into_iter().next() else {
        return Ok((None, touched));
    };
    if let Some(filter) = mandatory_filter {
        let matches = record
            .attributes
            .as_ref()
            .is_some_and(|attributes| evaluate_filter(filter, attributes));
        if !matches {
            return Ok((None, touched));
        }
    }
    Ok((record.values, touched))
}

/// Resolves full vector coordinates for a set of IDs without HTTP response data.
///
/// Query features such as reranking use this adapter to share the point-lookup
/// merge logic while receiving an ID-keyed map rather than projected records.
/// Attributes are never loaded from WAL records or segment sidecars.
///
/// # Parameters
///
/// - `state`: Shared services used for cache and object-store reads.
/// - `ns`: Namespace owning the IDs and manifest artifacts.
/// - `ids`: Borrowed requested IDs. Callers should pass unique, validated IDs;
///   the internal merge map has one slot per logical ID.
/// - `consistency`: Strong or eventual WAL policy.
/// - `manifest`: Owned visibility snapshot selected by the caller.
///
/// # Returns
///
/// An owned map from every found live ID to its full-precision coordinates.
/// Missing and tombstoned IDs are omitted. Hash-map iteration order is not part
/// of the contract.
///
/// # Errors
///
/// Propagates lookup failures. It also returns an index error if the shared
/// resolver ever produces a record without coordinates despite the forced
/// vector projection; that indicates an internal invariant violation.
///
/// # Side Effects
///
/// May read/populate the WAL disk cache and issue membership and cluster GETs.
/// Attribute objects are not fetched.
///
/// # Performance
///
/// Adds one result hash map and moves coordinate vectors out of temporary
/// records without cloning them. Under segment lookup, all IDs in one cluster
/// share a single cluster-object GET.
///
/// # Examples
///
/// Requesting `[item-7, item-8, absent]` can return a two-entry map for the live
/// IDs. A caller needing request ordering must use its original ID slice rather
/// than iterating this map.
///
/// # Rust Notes for Java/C Engineers
///
/// `into_iter()` consumes each record so its ID and coordinate allocation move
/// directly into the result map. Java would normally retain references to the
/// record's fields; C would need to detach pointers and prevent double free.
/// Rust statically makes the consumed record unusable.
#[allow(dead_code)]
pub(crate) async fn fetch_vector_values_by_ids(
    state: &AppState,
    ns: &str,
    ids: &[VectorId],
    consistency: ConsistencyLevel,
    manifest: Manifest,
) -> Result<HashMap<VectorId, Vec<f32>>, ZeppelinError> {
    Ok(
        fetch_vector_values_by_ids_with_trace(state, ns, ids, consistency, manifest)
            .await?
            .0,
    )
}

/// Resolves vectors while retaining the exact immutable segment reads.
pub(crate) async fn fetch_vector_values_by_ids_with_trace(
    state: &AppState,
    ns: &str,
    ids: &[VectorId],
    consistency: ConsistencyLevel,
    manifest: Manifest,
) -> Result<(HashMap<VectorId, Vec<f32>>, BTreeSet<String>), ZeppelinError> {
    let projection = FetchProjection {
        include_vector: true,
        include_attributes: false,
        attribute_fields: None,
    };
    let (response, touched) =
        fetch_vectors_by_id_with_trace(state, ns, ids, consistency, projection, manifest).await?;
    let values = response
        .results
        .into_iter()
        .map(|record| {
            let values = record.values.ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "fetch by id returned no vector values for {}",
                    record.id
                ))
            })?;
            Ok((record.id, values))
        })
        .collect::<Result<HashMap<_, _>, ZeppelinError>>()?;
    Ok((values, touched))
}

/// Merges WAL and active-segment state into ordered projected point results.
///
/// This is the core lookup operation shared by the HTTP endpoint and query
/// features. It first resolves the selected manifest's WAL semantics, then asks
/// the active segment only for IDs not already supplied or suppressed by the
/// WAL. Finally it consumes the ID-keyed map in request order.
///
/// # Parameters
///
/// - `state`: Shared WAL reader, disk cache, and object-store services.
/// - `ns`: Namespace owning every referenced artifact.
/// - `ids`: Requested IDs in desired output order. Callers should validate
///   uniqueness; a duplicate would consume the one map entry on first use and
///   classify the later occurrence as missing.
/// - `consistency`: Determines whether WAL upserts are replayed or skipped.
/// - `projection`: Cheap borrowed policy selecting coordinates and metadata.
/// - `manifest`: Owned, already selected visibility snapshot.
///
/// # Returns
///
/// A [`GetVectorsResponse`] whose two collections preserve relative request
/// ordering and together account for every input ID.
///
/// # Errors
///
/// Propagates WAL/cache/storage/decode errors, membership inconsistencies,
/// missing active-segment descriptors, missing required artifacts, and row
/// alignment failures. No failed source is silently treated as “ID missing.”
///
/// # Side Effects
///
/// Performs read-only lookup but may fill the WAL disk cache and issue direct
/// object-store GETs for membership, cluster, and attribute artifacts.
///
/// # Consistency
///
/// In strong mode, manifest-order WAL replay implements last-visible-operation
/// wins: a later tombstone removes an earlier upsert, and a later upsert removes
/// an earlier tombstone. Those results override the immutable segment. In
/// eventual mode, only effective tombstones are read; unresolved IDs come from
/// the active segment, so recent WAL inserts/updates may be absent or stale.
///
/// Retained non-active segment descriptors are never searched. The supplied
/// manifest and its `active_segment` define the one compacted view for this
/// operation.
///
/// # Performance
///
/// Allocates requested/found/deleted hash tables in `O(ids)` space. WAL cost is
/// described by `fetch_strong_wal_records` or the tombstone reader. Segment cost
/// includes a full membership scan plus grouped cluster reads for unresolved
/// IDs. Restoring order is one final linear pass over `ids`.
///
/// # Examples
///
/// With segment value `A=v1`, later WAL operations `A=v2`, `delete B`, and
/// `C=v1`, strong lookup of `[B, A, C, D]` returns `A=v2`, `C=v1` and reports
/// `B`, `D` missing in request-relative order. Eventual may return segment
/// `A=v1`, omits WAL-only `C`, and still suppresses `B`.
///
/// # Rust Notes for Java/C Engineers
///
/// The requested set holds `&str` borrows into `ids`, avoiding string copies
/// while source maps own any returned records. `HashMap<String, _>` can be
/// queried and removed with `&str` because Rust's `Borrow` contract guarantees
/// compatible hashing. Mutable maps remain local to this future, so no lock is
/// required across the asynchronous reads.
async fn fetch_vectors_by_id(
    state: &AppState,
    ns: &str,
    ids: &[VectorId],
    consistency: ConsistencyLevel,
    projection: FetchProjection<'_>,
    manifest: Manifest,
) -> Result<GetVectorsResponse, ZeppelinError> {
    Ok(
        fetch_vectors_by_id_with_trace(state, ns, ids, consistency, projection, manifest)
            .await?
            .0,
    )
}

/// Point lookup plus the exact manifest-owned segment artifacts it consumed.
async fn fetch_vectors_by_id_with_trace(
    state: &AppState,
    ns: &str,
    ids: &[VectorId],
    consistency: ConsistencyLevel,
    projection: FetchProjection<'_>,
    manifest: Manifest,
) -> Result<(GetVectorsResponse, BTreeSet<String>), ZeppelinError> {
    let active_ids: Vec<ulid::Ulid> = manifest
        .uncompacted_fragments()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    state.fragment_cache.evict_compacted(ns, &active_ids);
    let requested: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut found = HashMap::new();
    let mut deleted = HashSet::new();

    match consistency {
        ConsistencyLevel::Strong => {
            fetch_strong_wal_records(
                state,
                ns,
                &manifest,
                &requested,
                projection,
                &mut found,
                &mut deleted,
            )
            .await?;
        }
        ConsistencyLevel::Eventual => {
            deleted = state
                .wal_reader
                .read_delete_ids_from_refs_unchecked(
                    ns,
                    manifest.uncompacted_fragments(),
                    FragmentCachePolicy::ReadWrite(&state.cache),
                    Some(&state.fragment_cache),
                )
                .await?;
        }
    }

    let segment_ids: Vec<VectorId> = ids
        .iter()
        .filter(|id| !found.contains_key(id.as_str()) && !deleted.contains(id.as_str()))
        .cloned()
        .collect();
    let (segment_records, touched) =
        fetch_segment_records(state, ns, &manifest, &segment_ids, projection).await?;
    found.extend(segment_records);

    let mut results = Vec::new();
    let mut missing = Vec::new();
    for id in ids {
        match found.remove(id.as_str()) {
            Some(record) => results.push(record),
            None => missing.push(id.clone()),
        }
    }

    Ok((GetVectorsResponse { results, missing }, touched))
}

/// Replays all visible uncompacted WAL operations into lookup overlay state.
///
/// Fragments are returned in manifest order. Within each fragment, tombstones
/// are applied before upserts; [`crate::wal::WalFragment`] construction forbids
/// the same ID from appearing in both lists of one fragment. Processing later
/// fragments overwrites earlier logical state.
///
/// # Parameters
///
/// - `state`: Shared WAL reader and immutable-byte disk cache.
/// - `ns`: Namespace used to derive fragment keys and verify GC races.
/// - `manifest`: Borrowed visibility snapshot whose fragment order is replayed.
/// - `requested`: Borrowed set limiting work to requested IDs.
/// - `projection`: Output projection applied before retained WAL records enter
///   the result map.
/// - `found`: Exclusive mutable map updated with latest live WAL records.
/// - `deleted`: Exclusive mutable set updated with effective WAL tombstones.
///
/// # Returns
///
/// `Ok(())` after mutating `found` and `deleted` to reflect all visible WAL
/// operations. An empty fragment list leaves both collections unchanged.
///
/// # Errors
///
/// Propagates cache, storage, missing-fragment revalidation, or unchecked decode
/// failures. Because fragments are immutable and manifest-selected, failure is
/// reported rather than skipping a source. Maps may contain a prefix of replayed
/// operations when an error occurs, but the caller discards them with the failed
/// request and performs no write.
///
/// # Side Effects
///
/// May populate the WAL disk cache and emits reader diagnostics. Authoritative
/// state is unchanged.
///
/// # Consistency
///
/// This function trusts the supplied manifest's ordered references as its
/// visibility boundary. A tombstone removes any earlier found record; an upsert
/// removes any earlier tombstone and replaces any earlier record. The active
/// segment is consulted only after this overlay is complete.
///
/// # Performance
///
/// Reads all referenced fragments concurrently, then scans their operations in
/// manifest order. CPU is linear in total fragment operations even though only
/// requested IDs are retained; projecting early avoids retaining unwanted
/// coordinate/metadata buffers.
///
/// # Examples
///
/// For fragments `upsert A=v1`, `delete A`, then `upsert A=v2`, replay ends
/// with `A=v2` in `found` and no `A` in `deleted`. For `delete B` as the final
/// operation, `B` is removed from `found` and remains tombstoned.
///
/// # Rust Notes for Java/C Engineers
///
/// The two `&mut` parameters are exclusive borrows held by this future. Rust
/// prevents the caller or another task from simultaneously reading or mutating
/// those maps until the awaited call ends—stronger than passing ordinary Java
/// references or C pointers. Fragment entries are then moved out of each owned
/// fragment, avoiding deep clones.
async fn fetch_strong_wal_records(
    state: &AppState,
    ns: &str,
    manifest: &Manifest,
    requested: &HashSet<&str>,
    projection: FetchProjection<'_>,
    found: &mut HashMap<VectorId, GetVectorRecord>,
    deleted: &mut HashSet<VectorId>,
) -> Result<(), ZeppelinError> {
    if manifest.uncompacted_fragments().is_empty() {
        return Ok(());
    }

    let fragments = state
        .wal_reader
        .read_query_fragments_from_refs_unchecked(
            ns,
            manifest.uncompacted_fragments(),
            FragmentCachePolicy::ReadWrite(&state.cache),
            Some(&state.fragment_cache),
        )
        .await?;
    for fragment in &fragments {
        for id in &fragment.deletes {
            if requested.contains(id.as_str()) {
                found.remove(id.as_str());
                deleted.insert(id.clone());
            }
        }
        for vector in &fragment.vectors {
            if requested.contains(vector.id.as_str()) {
                deleted.remove(vector.id.as_str());
                found.insert(
                    vector.id.clone(),
                    project_vector_entry(vector.clone(), projection),
                );
            }
        }
    }
    Ok(())
}

/// Resolves unresolved IDs through the active segment's membership directory.
///
/// Membership maps IDs to logical IVF clusters without searching by distance.
/// This function validates the artifact against the manifest descriptor, groups
/// requested IDs by cluster, and loads only the cluster/attribute objects needed
/// by the requested projection.
///
/// # Parameters
///
/// - `state`: Shared object-store gateway. These segment reads currently bypass
///   the local raw-object cache.
/// - `ns`: Namespace prefix used only for legacy/per-cluster object keys.
/// - `manifest`: Borrowed visibility snapshot selecting the active segment.
/// - `ids`: Unresolved, non-tombstoned IDs. Ordering is not preserved in this
///   helper's map; the caller restores it.
/// - `projection`: Coordinates and metadata requested by the caller.
///
/// # Returns
///
/// An owned map of IDs found in active-segment membership to projected records.
/// Empty input, no active segment, or IDs absent from membership returns an
/// empty/partial map without error.
///
/// # Errors
///
/// Returns an index error when `active_segment` names no retained descriptor;
/// a membership error when the descriptor has no membership artifact, the
/// object is malformed, cluster counts disagree, or an entry is out of range;
/// and storage/index errors for missing or corrupt cluster/attribute objects,
/// membership-to-cluster contradictions, or short attribute row alignment.
///
/// There is no membership-scan fallback. A required missing object or corrupt
/// descriptor fails loudly even if another retained segment might contain the
/// requested ID.
///
/// # Side Effects
///
/// Performs one full membership GET whenever non-empty input reaches an active
/// segment. Depending on projection, it then performs direct cluster and
/// attribute GETs. It changes neither cache nor authoritative state.
///
/// # Consistency
///
/// Only the descriptor selected by `manifest.active_segment` is read. Segment,
/// membership, cluster, and attribute artifacts are immutable; the manifest
/// reference is what makes this exact set visible. Incremental compaction may
/// route a logical cluster to an older segment owner, which downstream key
/// helpers preserve.
///
/// # Performance
///
/// Decoding/scanning membership is linear in all segment entries, not only
/// requested IDs. IDs are grouped so each selected logical cluster incurs one
/// cluster GET and, when requested, one attrs GET. For each requested ID, row
/// resolution linearly searches that cluster's ID list. ID-only projection
/// returns directly from membership and avoids both object types. Attributes-
/// only projection still loads cluster IDs to establish row alignment.
///
/// # Examples
///
/// Membership maps `A -> 2`, `B -> 2`, and `C -> 5`. A vectors-only request for
/// `[A, C]` loads clusters 2 and 5 once each and no attrs objects. An ID-only
/// request for `[A, B]` performs only the membership GET. If membership maps `A`
/// to cluster 2 but cluster 2 lacks `A`, lookup returns an index error.
///
/// # Rust Notes for Java/C Engineers
///
/// `HashMap<usize, Vec<String>>` takes ownership of matching IDs decoded from
/// membership, then `into_values()` or the later loop consumes those groups.
/// Rust frees nonmatching decoded entries automatically. `let Some(segment) =
/// ... else` makes the no-active-segment branch explicit and leaves `segment`
/// non-null for the remainder, unlike a nullable Java/C pointer checked by
/// convention.
async fn fetch_segment_records(
    state: &AppState,
    ns: &str,
    manifest: &Manifest,
    ids: &[VectorId],
    projection: FetchProjection<'_>,
) -> Result<(HashMap<VectorId, GetVectorRecord>, BTreeSet<String>), ZeppelinError> {
    if ids.is_empty() {
        return Ok((HashMap::new(), BTreeSet::new()));
    }
    let Some(segment) = active_segment(manifest)? else {
        return Ok((HashMap::new(), BTreeSet::new()));
    };
    let membership_ref = segment.membership.as_ref().ok_or_else(|| {
        ZeppelinError::Membership("fetch by id requires segment membership artifact".into())
    })?;
    let mut touched = BTreeSet::from([membership_ref.key.clone()]);
    let membership_bytes = state.store.get(&membership_ref.key).await?;
    let membership = deserialize_membership(&membership_bytes)?;
    if membership.cluster_count as usize != segment.cluster_count {
        return Err(ZeppelinError::Membership(format!(
            "membership cluster_count {} does not match segment cluster_count {}",
            membership.cluster_count, segment.cluster_count
        )));
    }

    let requested: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut ids_by_cluster: HashMap<usize, Vec<VectorId>> = HashMap::new();
    for (id, cluster_idx) in membership.entries {
        if requested.contains(id.as_str()) {
            let cluster_idx = cluster_idx as usize;
            if cluster_idx >= segment.cluster_count {
                return Err(ZeppelinError::Membership(format!(
                    "membership id {id} references out-of-range cluster {cluster_idx}"
                )));
            }
            ids_by_cluster.entry(cluster_idx).or_default().push(id);
        }
    }

    if !projection.include_vector && !projection.include_attributes {
        let records = ids_by_cluster
            .into_values()
            .flatten()
            .map(|id| {
                (
                    id.clone(),
                    GetVectorRecord {
                        id,
                        values: None,
                        attributes: None,
                    },
                )
            })
            .collect();
        return Ok((records, touched));
    }

    let mut records = HashMap::new();
    for (cluster_idx, cluster_ids) in ids_by_cluster {
        touched.insert(segment_cluster_artifact_key(ns, segment, cluster_idx));
        let cluster = fetch_segment_cluster(state, ns, segment, cluster_idx).await?;
        let attrs = if projection.include_attributes {
            touched.insert(attrs_key(
                ns,
                segment.cluster_owner(cluster_idx),
                cluster_idx,
            ));
            Some(fetch_segment_attrs(state, ns, segment, cluster_idx, cluster.ids.len()).await?)
        } else {
            None
        };

        for id in cluster_ids {
            let row_idx = cluster
                .ids
                .iter()
                .position(|candidate| candidate == &id)
                .ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "membership maps id {id} to cluster {cluster_idx}, but cluster lacks it"
                    ))
                })?;
            let raw_attrs = attrs
                .as_ref()
                .and_then(|cluster_attrs| cluster_attrs.get(row_idx).cloned().flatten());
            records.insert(
                id.clone(),
                GetVectorRecord {
                    id,
                    values: projection
                        .include_vector
                        .then(|| cluster.vectors[row_idx].clone()),
                    attributes: project_attributes(raw_attrs, projection),
                },
            );
        }
    }

    Ok((records, touched))
}

/// Resolves the manifest's active segment ID to its retained descriptor.
///
/// # Parameters
///
/// - `manifest`: Borrowed visibility snapshot to inspect.
///
/// # Returns
///
/// `Ok(None)` when the manifest has no active compacted view, or `Ok(Some)` with
/// a descriptor borrowed from `manifest.segments` when the ID is present.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] when `active_segment` names an ID absent
/// from the segment descriptors. That contradiction is not treated as an empty
/// namespace.
///
/// # Performance
///
/// Linearly scans retained descriptors and performs no allocation or I/O.
///
/// # Examples
///
/// A new manifest with no active ID returns `None`. An active ID `seg-9` returns
/// a borrow of the matching descriptor; naming `seg-9` with no descriptor fails.
///
/// # Rust Notes for Java/C Engineers
///
/// The returned `&SegmentRef` borrows storage inside `manifest`: it cannot be
/// null in the `Some` branch and cannot outlive the manifest. This resembles a
/// Java object reference or C `const SegmentRef *`, but Rust checks both
/// non-null state and lifetime at compile time.
fn active_segment(manifest: &Manifest) -> Result<Option<&SegmentRef>, ZeppelinError> {
    let Some(segment_id) = manifest.active_segment.as_ref() else {
        return Ok(None);
    };
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *segment_id)
        .map(Some)
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "active segment {segment_id} is missing from manifest segments"
            ))
        })
}

/// Loads and decodes one logical cluster from its manifest-selected object.
///
/// New segment descriptors can group multiple logical clusters in one object.
/// If no grouped descriptor names `cluster_idx`, the function derives the
/// legacy one-object-per-cluster key using [`SegmentRef::cluster_owner`]. It
/// never retries a failed grouped-object GET against the legacy key.
///
/// # Parameters
///
/// - `state`: Shared object-store gateway.
/// - `ns`: Namespace prefix for legacy key construction.
/// - `segment`: Active segment descriptor, including grouped layout and
///   incremental-compaction ownership.
/// - `cluster_idx`: Valid logical cluster index selected by membership.
///
/// # Returns
///
/// Owned row-aligned IDs and full-precision coordinate vectors for the selected
/// logical cluster. A grouped object is decoded down to only that section.
///
/// # Errors
///
/// Propagates a missing/storage failure for the chosen exact key and index
/// errors for malformed object directories, missing grouped sections, or
/// malformed cluster bytes. It does not silently try alternate data.
///
/// # Side Effects
///
/// Performs one full object-store GET. The current point-lookup path does not
/// populate or consult the segment disk cache for this object.
///
/// # Performance
///
/// A legacy object contains one cluster. A grouped object can transfer bytes
/// for neighboring logical clusters even though only `cluster_idx` is decoded;
/// decoded IDs and vectors allocate owned row buffers.
///
/// # Examples
///
/// If manifest metadata says clusters `[2, 5]` live in `group-1.bin`, fetching
/// cluster 5 GETs that key and selects section 5. If no grouped entry names
/// cluster 5 and its owner is `seg-old`, the derived key is
/// `.../seg-old/cluster_5.bin`.
///
/// # Rust Notes for Java/C Engineers
///
/// The grouped key is cloned because it must remain owned after the iterator's
/// borrow ends and across `.await`. The descriptor itself stays borrowed. In C,
/// retaining a pointer into a descriptor while asynchronous work continues
/// would require manual lifetime discipline; Rust enforces it.
async fn fetch_segment_cluster(
    state: &AppState,
    ns: &str,
    segment: &SegmentRef,
    cluster_idx: usize,
) -> Result<crate::index::ivf_flat::build::ClusterData, ZeppelinError> {
    let key = segment_cluster_artifact_key(ns, segment, cluster_idx);
    let data = state.store.get(&key).await?;
    deserialize_cluster_from_object(&data, cluster_idx)
}

/// Resolves the sole manifest-selected object key containing one cluster.
fn segment_cluster_artifact_key(ns: &str, segment: &SegmentRef, cluster_idx: usize) -> String {
    segment
        .cluster_objects
        .iter()
        .find(|object| object.clusters.contains(&cluster_idx))
        .map(|object| object.key.clone())
        .unwrap_or_else(|| cluster_key(ns, segment.cluster_owner(cluster_idx), cluster_idx))
}

/// Loads and validates the row-aligned attribute sidecar for one cluster.
///
/// Attribute objects remain one-per-cluster even when full-vector cluster data
/// is grouped. Incremental compaction ownership is resolved through
/// [`SegmentRef::cluster_owner`] so an untouched cluster can keep its older
/// immutable sidecar.
///
/// # Parameters
///
/// - `state`: Shared object-store gateway.
/// - `ns`: Namespace object-key prefix.
/// - `segment`: Active segment descriptor containing cluster ownership.
/// - `cluster_idx`: Logical cluster selected by membership.
/// - `cluster_len`: Number of rows decoded from cluster data and therefore the
///   minimum valid attribute-row count.
///
/// # Returns
///
/// Owned optional metadata maps in artifact row order. Extra trailing rows are
/// currently retained; fewer rows than the cluster contains are rejected.
///
/// # Errors
///
/// Propagates storage/missing-object and JSON decode errors, or returns an index
/// error when the sidecar has fewer rows than the cluster. No empty/default
/// sidecar is substituted.
///
/// # Side Effects
///
/// Performs one full object-store GET and no cache update or authoritative
/// mutation.
///
/// # Performance
///
/// Transfers and decodes the complete cluster sidecar, allocating maps and
/// attribute values for every stored row even when the caller allow-lists only
/// a few fields later.
///
/// # Examples
///
/// A three-vector cluster requires at least three sidecar entries, including
/// explicit `None` rows. Two entries fail loudly because row-to-vector metadata
/// association would be ambiguous.
async fn fetch_segment_attrs(
    state: &AppState,
    ns: &str,
    segment: &SegmentRef,
    cluster_idx: usize,
    cluster_len: usize,
) -> Result<Vec<Option<HashMap<String, AttributeValue>>>, ZeppelinError> {
    let key = attrs_key(ns, segment.cluster_owner(cluster_idx), cluster_idx);
    let data = state.store.get(&key).await?;
    let attrs = deserialize_attrs(&data)?;
    if attrs.len() < cluster_len {
        return Err(ZeppelinError::Index(format!(
            "attrs length {} shorter than cluster {cluster_idx} vector count {cluster_len}",
            attrs.len()
        )));
    }
    Ok(attrs)
}

/// Consumes a WAL vector entry and applies the requested response projection.
///
/// # Parameters
///
/// - `vector`: Owned latest-visible WAL entry.
/// - `projection`: Borrowed policy selecting coordinates and metadata fields.
///
/// # Returns
///
/// An owned [`GetVectorRecord`] with the same ID. Included values move directly
/// into the record; excluded buffers are dropped. Metadata follows
/// `project_attributes` semantics.
///
/// # Examples
///
/// Projecting `A=[1,2], {color:red, tenant:x}` with vectors disabled and fields
/// `[color]` returns ID `A`, no values, and `{color:red}`.
///
/// # Rust Notes for Java/C Engineers
///
/// Taking `VectorEntry` by value makes ownership transfer explicit. Included
/// coordinate storage moves without copying; excluded storage is freed by RAII.
/// Java would typically share an array until garbage collection, while C would
/// need separate free/transfer branches.
fn project_vector_entry(vector: VectorEntry, projection: FetchProjection<'_>) -> GetVectorRecord {
    GetVectorRecord {
        id: vector.id,
        values: projection.include_vector.then_some(vector.values),
        attributes: project_attributes(vector.attributes, projection),
    }
}

/// Applies metadata inclusion and field allow-list rules to an owned map.
///
/// # Parameters
///
/// - `attrs`: Owned metadata, or `None` when the source record has no map.
/// - `projection`: Borrowed inclusion flag and optional field-name slice.
///
/// # Returns
///
/// `None` when attributes are disabled, absent, or no requested field is
/// present. With no allow-list, returns the original map without cloning it.
/// With an allow-list, returns a newly allocated map containing cloned present
/// keys and values. Duplicate allow-list names collapse naturally in the map.
///
/// # Performance
///
/// Disabled/absent metadata is constant time aside from dropping owned data.
/// Full inclusion moves the map. Field projection performs one hash lookup per
/// requested field and clones only selected values; list-valued attributes may
/// therefore allocate.
///
/// # Examples
///
/// `{color:red, tenant:x}` with fields `[color, missing]` yields `{color:red}`.
/// Fields `[missing]` yield `None`, which omits the response property rather than
/// serializing an empty object.
///
/// # Rust Notes for Java/C Engineers
///
/// The `?` on `Option` returns `None` immediately without exceptions or a null
/// dereference. Pattern matching then distinguishes moving the entire map from
/// borrowing it while constructing a smaller owned map.
fn project_attributes(
    attrs: Option<HashMap<String, AttributeValue>>,
    projection: FetchProjection<'_>,
) -> Option<HashMap<String, AttributeValue>> {
    if !projection.include_attributes {
        return None;
    }
    let attrs = attrs?;
    let Some(fields) = projection.attribute_fields else {
        return Some(attrs);
    };
    let projected: HashMap<String, AttributeValue> = fields
        .iter()
        .filter_map(|field| {
            attrs
                .get(field)
                .cloned()
                .map(|value| (field.clone(), value))
        })
        .collect();
    (!projected.is_empty()).then_some(projected)
}

/// Checks whether every ID byte belongs to the supported ASCII alphabet.
///
/// This character-only predicate deliberately treats the empty string as valid;
/// [`validate_vector_id_for_request`] performs the separate non-empty and length
/// checks before calling it.
///
/// # Parameters
///
/// - `id`: Borrowed UTF-8 text to inspect byte by byte.
///
/// # Returns
///
/// `true` when every byte is an ASCII letter, digit, dash, underscore, or dot;
/// otherwise `false`.
///
/// # Performance
///
/// Scans once in `O(id.len())`, short-circuits on the first invalid byte, and
/// allocates nothing.
///
/// # Examples
///
/// `product-42_v2.1` returns `true`; `product/42`, a space, or any non-ASCII
/// UTF-8 byte returns `false`. The empty string returns `true` here and is
/// rejected by the outer validator.
///
/// # Rust Notes for Java/C Engineers
///
/// `bytes()` exposes values rather than raw pointers, and the iterator pipeline
/// compiles to a simple short-circuiting loop. Unlike C `isalnum`, behavior is
/// locale-independent and cannot read beyond the string. Unlike Java's Unicode
/// character predicates, this API intentionally accepts ASCII only.
fn is_valid_vector_id(id: &str) -> bool {
    id.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}
