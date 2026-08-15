//! Typed multimodal ingestion for late-interaction namespaces.

use std::collections::{BTreeMap, HashMap};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::embedding::{
    ArtifactChecksum, EncoderInputRef, ImageObjectRef, InputModality, RetrievalUnitRecord,
    SemanticState, TextContentRef,
};
use crate::error::ZeppelinError;
use crate::security::{
    Action, AllowDecision, AuditParams, NamespaceId, Principal, RequestContext, SecurityError,
};
use crate::server::{
    authorize_secondary_namespace_action, AppState, AuditRequest, RateLimitIdentity,
};
use crate::types::{AttributeValue, IndexType, VectorId};
use crate::wal::late_section::SourceInventoryRef;

use super::vectors::validate_vector_id_for_request;
use super::ApiError;

type PendingImageSource = ((EncoderInputRef, SourceInventoryRef), Bytes);

/// One all-or-nothing typed-input append.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalUnitsRequest {
    /// Typed source records to insert or replace.
    #[serde(default)]
    pub upserts: Vec<RetrievalUnitInput>,
    /// Logical record tombstones.
    #[serde(default)]
    pub deletes: Vec<VectorId>,
}

/// Caller-facing retrieval-unit record before source bytes are retained.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalUnitInput {
    /// Stable namespace-local record identity.
    pub id: VectorId,
    /// Typed text/image payload.
    pub input: RetrievalUnitPayload,
    /// Optional grouping identity.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// Optional ordinal inside the parent.
    #[serde(default)]
    pub unit_ordinal: Option<u32>,
    /// Inline attributes used by filters and configured FTS fields.
    #[serde(default)]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// JSON payload variants accepted by the Phase-5 ingestion boundary.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetrievalUnitPayload {
    /// Inline UTF-8 text.
    Text {
        /// Exact retained text.
        text: String,
    },
    /// One base64-encoded image.
    Image {
        /// Standard base64 encoded source bytes.
        image_base64: String,
        /// Declared media type checked against the server allowlist.
        media_type: String,
        /// Declared pixel width.
        width: u32,
        /// Declared pixel height.
        height: u32,
    },
    /// One image paired with inline UTF-8 text.
    ImageText {
        /// Standard base64 encoded source bytes.
        image_base64: String,
        /// Declared media type checked against the server allowlist.
        media_type: String,
        /// Declared pixel width.
        width: u32,
        /// Declared pixel height.
        height: u32,
        /// Exact retained text.
        text: String,
    },
}

/// Acknowledgement emitted only after the root manifest CAS makes the batch visible.
#[derive(Debug, Serialize)]
pub struct RetrievalUnitsResponse {
    /// Number of typed records upserted.
    pub upserted: usize,
    /// Number of tombstones appended.
    pub deleted: usize,
    /// Committed root-manifest generation.
    pub manifest_generation: u64,
    /// Semantic materialization state in the same committed manifest snapshot.
    pub semantic_state: SemanticState,
    /// Highest mutation sequence with contiguous semantic coverage.
    pub semantic_sequence: u64,
}

/// Validate, retain, and atomically publish typed retrieval-unit mutations.
#[instrument(
    skip(state, decision, principal, context, audit, headers, request),
    fields(namespace = %namespace)
)]
#[allow(clippy::too_many_arguments)]
pub async fn append_retrieval_units(
    State(state): State<AppState>,
    Extension(decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Extension(audit): Extension<AuditRequest>,
    Extension(rate_identity): Extension<RateLimitIdentity>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RetrievalUnitsRequest>,
) -> Result<(StatusCode, Json<RetrievalUnitsResponse>), ApiError> {
    let operation_count = request.upserts.len() + request.deletes.len();
    if operation_count == 0 {
        return Err(ApiError(ZeppelinError::RetrievalUnitEmpty));
    }
    if operation_count > state.config.server.max_retrieval_units_per_request {
        return Err(ApiError(ZeppelinError::RetrievalUnitTooLarge {
            resource: "request records",
            actual: operation_count,
            limit: state.config.server.max_retrieval_units_per_request,
        }));
    }
    for input in &request.upserts {
        validate_vector_id_for_request(&input.id, state.config.server.max_vector_id_length)
            .map_err(ApiError::from)?;
    }
    for id in &request.deletes {
        validate_vector_id_for_request(id, state.config.server.max_vector_id_length)
            .map_err(ApiError::from)?;
    }

    let metadata = state
        .namespace_manager
        .get(&namespace)
        .await
        .map_err(ApiError::from)?;
    if metadata.index_type != IndexType::LateInteractionFde {
        return Err(ApiError(ZeppelinError::UnsupportedInputModality {
            modality: "retrieval_unit",
        }));
    }
    let admission = metadata.late_interaction.as_ref().ok_or_else(|| {
        ApiError(ZeppelinError::Serialization(
            "late-interaction namespace is missing admission config".to_string(),
        ))
    })?;

    // Phase 5 fails closed for scoped row mutation. It does not silently bypass
    // the dense path's existing-row proof while typed point lookup is not yet a
    // public API.
    if decision.mandatory_filter.is_some()
        || decision.field_mask.is_some()
        || !decision.write_constraints.is_empty()
    {
        return Err(ApiError(SecurityError::ConstraintViolation.into()));
    }

    let delete_decision = if request.deletes.is_empty() {
        None
    } else {
        let decision = authorize_secondary_namespace_action(
            &state,
            &headers,
            &principal,
            &context,
            rate_identity.ip,
            &audit,
            Action::VectorDelete,
            &namespace,
        )
        .map_err(ZeppelinError::from)
        .map_err(ApiError::from)?;
        if decision.mandatory_filter.is_some()
            || decision.field_mask.is_some()
            || !decision.write_constraints.is_empty()
        {
            return Err(ApiError(SecurityError::ConstraintViolation.into()));
        }
        let namespace_id =
            NamespaceId::new(namespace.clone()).map_err(|error| ApiError(error.into()))?;
        let guard = state
            .security
            .guard_vector_destruction(&namespace_id, None)
            .map_err(|error| ApiError(error.into()))?;
        if guard.is_locked() {
            audit.set_params(AuditParams::preservation_blocked(
                crate::security::PreservationBlockedSurface::VectorDelete,
                &guard,
            ));
            return Err(ApiError(SecurityError::PreservationLocked.into()));
        }
        Some(decision)
    };

    let mut records = Vec::with_capacity(request.upserts.len());
    let mut source_uploads = BTreeMap::<String, (SourceInventoryRef, Bytes)>::new();
    for input in request.upserts {
        let modality = input.input.modality();
        if !admission.accepted_modalities.contains(&modality) {
            return Err(ApiError(ZeppelinError::UnsupportedInputModality {
                modality: modality.as_str(),
            }));
        }
        let (encoder_input, source) =
            build_encoder_input(&state, &namespace, input.input).map_err(ApiError::from)?;
        let content_hash = encoder_input.content_hash().map_err(ApiError::from)?;
        if let Some((mut source, bytes)) = source {
            let key = SourceInventoryRef::object_store_key(&namespace, content_hash);
            set_image_key(&mut source.0, &key);
            source.1.key = key.clone();
            match source_uploads.get(&key) {
                Some(existing) if existing == &(source.1.clone(), bytes.clone()) => {}
                Some(_) => {
                    return Err(ApiError(ZeppelinError::Validation(
                        "content-addressed source key received conflicting bytes".to_string(),
                    )));
                }
                None => {
                    source_uploads.insert(key, (source.1, bytes));
                }
            }
            records.push(RetrievalUnitRecord {
                id: input.id,
                content_hash: source.0.content_hash().map_err(ApiError::from)?,
                input: source.0,
                parent_id: input.parent_id,
                unit_ordinal: input.unit_ordinal,
                attributes: input.attributes,
            });
        } else {
            records.push(RetrievalUnitRecord {
                id: input.id,
                content_hash,
                input: encoder_input,
                parent_id: input.parent_id,
                unit_ordinal: input.unit_ordinal,
                attributes: input.attributes,
            });
        }
    }

    let upserted = records.len();
    let deleted = request.deletes.len();
    if delete_decision.is_some() {
        audit.set_params(AuditParams::vector_delete(
            NamespaceId::new(namespace.clone()).map_err(ZeppelinError::from)?,
            &request.deletes,
        ));
    } else {
        audit.set_params(AuditParams::vector_upsert(
            NamespaceId::new(namespace.clone()).map_err(ZeppelinError::from)?,
            upserted,
            decision.is_attribute_admin_write(),
        ));
    }

    let (_, manifest) = state
        .wal_writer
        .append_retrieval_units(
            &namespace,
            records,
            request.deletes,
            source_uploads.into_values().collect(),
        )
        .await
        .map_err(ApiError::from)?;
    state.manifest_cache.insert(&namespace, manifest.clone());
    let (semantic_state, semantic_sequence) = manifest
        .semantic_coverage
        .as_ref()
        .map_or((SemanticState::Pending, 0), |coverage| {
            (coverage.state, coverage.contiguous_sequence)
        });
    info!(upserted, deleted, "retrieval units appended");
    Ok((
        StatusCode::OK,
        Json(RetrievalUnitsResponse {
            upserted,
            deleted,
            manifest_generation: manifest.version(),
            semantic_state,
            semantic_sequence,
        }),
    ))
}

impl RetrievalUnitPayload {
    const fn modality(&self) -> InputModality {
        match self {
            Self::Text { .. } => InputModality::Text,
            Self::Image { .. } => InputModality::Image,
            Self::ImageText { .. } => InputModality::ImageText,
        }
    }
}

fn build_encoder_input(
    state: &AppState,
    namespace: &str,
    input: RetrievalUnitPayload,
) -> Result<(EncoderInputRef, Option<PendingImageSource>), ZeppelinError> {
    match input {
        RetrievalUnitPayload::Text { text } => {
            validate_text(&text, state.config.server.max_retrieval_text_bytes)?;
            Ok((
                EncoderInputRef::Text {
                    content: TextContentRef::Inline(text),
                },
                None,
            ))
        }
        RetrievalUnitPayload::Image {
            image_base64,
            media_type,
            width,
            height,
        } => {
            let (image, source, bytes) =
                build_image(state, namespace, image_base64, media_type, width, height)?;
            let input = EncoderInputRef::Image { image };
            Ok((input.clone(), Some(((input, source), bytes))))
        }
        RetrievalUnitPayload::ImageText {
            image_base64,
            media_type,
            width,
            height,
            text,
        } => {
            validate_text(&text, state.config.server.max_retrieval_text_bytes)?;
            let (image, source, bytes) =
                build_image(state, namespace, image_base64, media_type, width, height)?;
            let input = EncoderInputRef::ImageText {
                image,
                text: TextContentRef::Inline(text),
            };
            Ok((input.clone(), Some(((input, source), bytes))))
        }
    }
}

fn build_image(
    state: &AppState,
    _namespace: &str,
    image_base64: String,
    media_type: String,
    width: u32,
    height: u32,
) -> Result<(ImageObjectRef, SourceInventoryRef, Bytes), ZeppelinError> {
    if !state
        .config
        .server
        .retrieval_image_media_types
        .contains(&media_type)
    {
        return Err(ZeppelinError::UnsupportedImageMediaType { media_type });
    }
    if width == 0
        || height == 0
        || width > state.config.server.max_retrieval_image_width
        || height > state.config.server.max_retrieval_image_height
    {
        return Err(ZeppelinError::ImageDimensionsExceeded {
            width,
            height,
            max_width: state.config.server.max_retrieval_image_width,
            max_height: state.config.server.max_retrieval_image_height,
        });
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(image_base64)
        .map(Bytes::from)
        .map_err(|_| ZeppelinError::Validation("image_base64 is not valid base64".to_string()))?;
    if bytes.is_empty() {
        return Err(ZeppelinError::RetrievalUnitEmpty);
    }
    if bytes.len() > state.config.server.max_retrieval_image_bytes {
        return Err(ZeppelinError::RetrievalUnitTooLarge {
            resource: "image bytes",
            actual: bytes.len(),
            limit: state.config.server.max_retrieval_image_bytes,
        });
    }
    let encoded_size_bytes = u64::try_from(bytes.len())
        .map_err(|_| ZeppelinError::Validation("image byte length does not fit u64".to_string()))?;
    let checksum = ArtifactChecksum::digest(&bytes);
    let image = ImageObjectRef {
        key: String::new(),
        checksum,
        media_type: media_type.clone(),
        encoded_size_bytes,
        width,
        height,
    };
    let source = SourceInventoryRef {
        key: String::new(),
        checksum,
        size_bytes: encoded_size_bytes,
        media_type,
        artifact_origin: None,
    };
    Ok((image, source, bytes))
}

fn validate_text(text: &str, limit: usize) -> Result<(), ZeppelinError> {
    if text.is_empty() {
        return Err(ZeppelinError::RetrievalUnitEmpty);
    }
    if text.len() > limit {
        return Err(ZeppelinError::RetrievalUnitTooLarge {
            resource: "text bytes",
            actual: text.len(),
            limit,
        });
    }
    Ok(())
}

fn set_image_key(input: &mut EncoderInputRef, key: &str) {
    match input {
        EncoderInputRef::Text { .. } => {}
        EncoderInputRef::Image { image } | EncoderInputRef::ImageText { image, .. } => {
            image.key = key.to_string();
        }
    }
}
