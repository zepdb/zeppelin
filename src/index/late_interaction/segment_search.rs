//! Two-wave exact search over one immutable late-interaction segment.
//!
//! Wave one depends on the segment's candidate kind. The production flat-SQ8
//! kind hydrates one resident `ZFQ1` artifact through the bootstrap cache and
//! selects candidates without any per-query planned read; the retained IVF
//! kind routes resident centroids and fetches routed clusters as one
//! cross-object read plan. Every retained exact matrix plus attribute payload
//! forms one second (truth) plan; no row-dependent read occurs outside it.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;

use crate::cache::DiskCache;
use crate::embedding::{ArtifactChecksum, ContentHash};
use crate::error::{Result, ZeppelinError};
use crate::index::filter::evaluate_filter_on_optional_attributes;
use crate::storage::read_plan::{
    execute_read_plan, ReadPlan, ReadPlanConfig, ReadPlanError, ReadRequest,
};
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, Filter, VectorId};
use crate::wal::{LateCandidateKind, LateInteractionSegmentRef};

use super::attribute_artifact::decode_attribute_row;
use super::candidate::{
    FdeCandidateIndex, FetchedLateCandidateCluster, LateCandidate, ResidentLateCandidateIndex,
};
use super::flat_candidate::ResidentFlatCandidateIndex;
use super::matrix_artifact::decode_matrix_row;
use super::{max_sim, MultiVectorMatrixRef};

/// Runtime byte and shape limits for one segment execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentSearchBounds {
    /// Maximum complete resident-bootstrap bytes.
    pub(crate) max_resident_bytes: usize,
    /// Maximum complete routed-cluster bytes.
    pub(crate) max_cluster_bytes: usize,
    /// Maximum vectors decoded from one exact document payload.
    pub(crate) max_vectors_per_document: usize,
    /// Maximum bytes decoded from one exact attribute payload.
    pub(crate) max_attribute_payload_bytes: usize,
}

impl SegmentSearchBounds {
    fn validate(self) -> Result<()> {
        if self.max_resident_bytes == 0
            || self.max_cluster_bytes == 0
            || self.max_vectors_per_document == 0
            || self.max_attribute_payload_bytes == 0
        {
            return Err(segment_error("segment search bounds must be positive"));
        }
        Ok(())
    }
}

/// Inputs for one immutable-segment candidate and exact-scoring pass.
pub(crate) struct SegmentSearchRequest<'a> {
    /// Object storage containing every section-rooted immutable artifact.
    pub(crate) store: &'a ZeppelinStore,
    /// Disposable immutable-byte cache used by production query paths.
    pub(crate) bootstrap_cache: Option<&'a DiskCache>,
    /// Manifest-selected immutable segment descriptor.
    pub(crate) segment: &'a LateInteractionSegmentRef,
    /// Already-transformed exact query matrix.
    pub(crate) exact_query: MultiVectorMatrixRef<'a>,
    /// Candidate query FDE produced from the same profile and generation.
    pub(crate) candidate_query_fde: &'a [f32],
    /// Server-owned mandatory filter.
    pub(crate) mandatory_filter: Option<&'a Filter>,
    /// Caller-supplied request filter.
    pub(crate) request_filter: Option<&'a Filter>,
    /// Baseline IDs superseded or deleted by post-segment input replay.
    pub(crate) excluded_ids: &'a BTreeSet<VectorId>,
    /// Maximum exact results returned after MaxSim scoring.
    pub(crate) top_k: usize,
    /// Shared cross-object read-plan controls for both waves.
    pub(crate) read_plan: &'a ReadPlanConfig,
    /// Runtime resident, cluster, matrix, and attribute limits.
    pub(crate) bounds: SegmentSearchBounds,
}

/// Observable shape of one planned wave.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SegmentWaveTrace {
    /// Caller-visible logical ranges supplied to the planner.
    pub(crate) logical_ranges: usize,
    /// Physical ranged requests emitted by the planner.
    pub(crate) planned_requests: usize,
    /// Physical bytes, including coalesced gaps.
    pub(crate) planned_bytes: u64,
}

/// Read-plan evidence for both row-dependent waves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SegmentSearchTrace {
    /// Routed candidate-cluster wave.
    pub(crate) candidate_wave: SegmentWaveTrace,
    /// Exact matrix and attribute wave.
    pub(crate) truth_wave: SegmentWaveTrace,
}

/// One exact segment result without exposing its approximate FDE score.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SegmentScoredRow {
    /// Retrieval-unit identity.
    pub(crate) id: VectorId,
    /// Raw higher-is-better exact MaxSim score.
    pub(crate) score: f32,
    /// Source content identity rooted by the candidate-cluster checksum.
    pub(crate) content_hash: ContentHash,
    /// Authoritative source mutation sequence.
    pub(crate) source_sequence: u64,
    /// Optional caller-provided parent identity.
    pub(crate) parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub(crate) unit_ordinal: Option<u32>,
    /// Exact attributes decoded in wave two.
    pub(crate) attributes: Option<HashMap<String, AttributeValue>>,
}

/// Exact rows plus the two physical read-plan summaries.
#[derive(Debug, Default)]
pub(crate) struct SegmentSearchOutput {
    /// Deterministically sorted exact results.
    pub(crate) rows: Vec<SegmentScoredRow>,
    /// Candidate and truth wave accounting.
    pub(crate) trace: SegmentSearchTrace,
}

/// Search one immutable late segment using exactly two row-dependent read waves.
pub(crate) async fn search_segment(
    request: SegmentSearchRequest<'_>,
) -> Result<SegmentSearchOutput> {
    request.bounds.validate()?;
    if request.top_k == 0 {
        return Ok(SegmentSearchOutput::default());
    }
    validate_query_shape(request.segment, &request.exact_query)?;

    let (candidates, candidate_trace) = match request.segment.candidate_kind {
        LateCandidateKind::FlatSq8 => {
            let flat = request.segment.flat_candidate_ref()?;
            let flat_size = usize::try_from(flat.size_bytes)
                .map_err(|_| segment_error("flat candidate artifact size exceeds usize"))?;
            if flat_size > request.bounds.max_resident_bytes {
                return Err(segment_error(
                    "flat candidate artifact exceeds the resident byte budget",
                ));
            }
            let flat_bytes = fetch_resident_artifact(&request, &flat.key, &flat.checksum).await?;
            let resident = ResidentFlatCandidateIndex::from_bytes(
                &flat_bytes,
                flat,
                request.bounds.max_resident_bytes,
            )?;
            let candidates = resident.select_candidates(
                request.candidate_query_fde,
                request.excluded_ids,
                request.mandatory_filter,
                request.request_filter,
            )?;
            // Wave one performs no per-query planned read for the flat kind:
            // the artifact hydrates once per segment generation through the
            // bootstrap cache, so its trace is empty by construction.
            (candidates, SegmentWaveTrace::default())
        }
        LateCandidateKind::Ivf => {
            let candidate_index = request.segment.ivf_candidate_index()?;
            let bootstrap = &candidate_index.bootstrap;
            let bootstrap_size = usize::try_from(bootstrap.size_bytes)
                .map_err(|_| segment_error("candidate bootstrap size exceeds usize"))?;
            if bootstrap_size > request.bounds.max_resident_bytes {
                return Err(segment_error(
                    "candidate bootstrap exceeds the resident byte budget",
                ));
            }
            let bootstrap_bytes =
                fetch_resident_artifact(&request, &bootstrap.key, &bootstrap.checksum).await?;
            let resident = ResidentLateCandidateIndex::from_index_ref(
                &bootstrap_bytes,
                candidate_index,
                request.bounds.max_resident_bytes,
            )?;

            let routed = resident.route(request.candidate_query_fde)?;
            if routed.is_empty() {
                return Ok(SegmentSearchOutput::default());
            }
            let candidate_requests = routed
                .iter()
                .map(|cluster| {
                    complete_object_request(&cluster.key, cluster.size_bytes, "candidate cluster")
                })
                .collect::<Result<Vec<_>>>()?;
            let candidate_plan = ReadPlan::build(&candidate_requests, request.read_plan)
                .map_err(map_read_plan_error)?;
            let candidate_trace = trace_for(&candidate_requests, &candidate_plan);
            let candidate_bytes = execute_read_plan(request.store, &candidate_plan)
                .await
                .map_err(map_read_plan_error)?;
            let fetched = routed
                .iter()
                .zip(&candidate_bytes)
                .map(|(reference, bytes)| FetchedLateCandidateCluster {
                    reference,
                    bytes: bytes.as_ref(),
                })
                .collect::<Vec<_>>();
            let candidates = resident.candidates_from_fetched(
                request.candidate_query_fde,
                request.excluded_ids,
                request.mandatory_filter,
                request.request_filter,
                &fetched,
                request.bounds.max_cluster_bytes,
            )?;
            (candidates, candidate_trace)
        }
    };
    if candidates.is_empty() {
        return Ok(SegmentSearchOutput {
            rows: Vec::new(),
            trace: SegmentSearchTrace {
                candidate_wave: candidate_trace,
                truth_wave: SegmentWaveTrace::default(),
            },
        });
    }

    let truth_requests = build_truth_requests(request.segment, &candidates)?;
    let truth_plan =
        ReadPlan::build(&truth_requests, request.read_plan).map_err(map_read_plan_error)?;
    let truth_trace = trace_for(&truth_requests, &truth_plan);
    let truth_bytes = execute_read_plan(request.store, &truth_plan)
        .await
        .map_err(map_read_plan_error)?;
    let mut rows = score_truth_wave(&request, candidates, &truth_bytes)?;
    rows.sort_by(compare_scored_rows);
    rows.truncate(request.top_k);
    Ok(SegmentSearchOutput {
        rows,
        trace: SegmentSearchTrace {
            candidate_wave: candidate_trace,
            truth_wave: truth_trace,
        },
    })
}

fn validate_query_shape(
    segment: &LateInteractionSegmentRef,
    query: &MultiVectorMatrixRef<'_>,
) -> Result<()> {
    if usize::try_from(segment.vector_dimension).ok() != Some(query.vector_dimension()) {
        return Err(ZeppelinError::DimensionMismatch {
            expected: usize::try_from(segment.vector_dimension).unwrap_or(usize::MAX),
            actual: query.vector_dimension(),
        });
    }
    let candidate_fde_dimension = match segment.candidate_kind {
        crate::wal::LateCandidateKind::Ivf => {
            segment
                .ivf_candidate_index()?
                .bootstrap
                .recipe
                .fde_dimension
        }
        crate::wal::LateCandidateKind::FlatSq8 => {
            segment.flat_candidate_ref()?.recipe.fde_dimension
        }
    };
    if segment.fde_dimension != candidate_fde_dimension {
        return Err(segment_error(
            "segment and candidate FDE dimensions disagree",
        ));
    }
    Ok(())
}

/// Fetch one resident candidate artifact through the bootstrap-cache authority.
///
/// The cache key is the artifact's SHA-256 alone: every candidate artifact is
/// content-addressed and validated against its manifest checksum after
/// hydration, and the full object key exceeds the disk cache's one-component
/// filename limit on common filesystems.
async fn fetch_resident_artifact(
    request: &SegmentSearchRequest<'_>,
    key: &str,
    checksum: &ArtifactChecksum,
) -> Result<bytes::Bytes> {
    if let Some(cache) = request.bootstrap_cache {
        let cache_key = format!("late-candidate-sha256-{}", checksum.to_hex());
        cache
            .get_or_fetch(&cache_key, || request.store.get(key))
            .await
    } else {
        request.store.get(key).await
    }
}

fn complete_object_request(key: &str, size_bytes: u64, label: &str) -> Result<ReadRequest> {
    if key.is_empty() || size_bytes == 0 {
        return Err(segment_error(format!("{label} reference is empty")));
    }
    let end = usize::try_from(size_bytes)
        .map_err(|_| segment_error(format!("{label} size exceeds usize")))?;
    Ok(ReadRequest {
        object_key: key.to_string(),
        range: 0..end,
    })
}

// `pub(crate)` on the four functions below exists solely for the ignored
// Phase 9 flat-candidate benchmark module; nothing outside this crate can see
// them and no production caller changes.
pub(crate) fn build_truth_requests(
    segment: &LateInteractionSegmentRef,
    candidates: &[LateCandidate],
) -> Result<Vec<ReadRequest>> {
    let mut requests = Vec::with_capacity(
        candidates
            .len()
            .checked_mul(2)
            .ok_or_else(|| segment_error("truth logical-range count overflows"))?,
    );
    for candidate in candidates {
        let matrix_ref = segment
            .matrix_objects
            .iter()
            .find(|reference| reference.key == candidate.matrix_locator.object_key)
            .ok_or_else(|| {
                segment_error("candidate matrix locator is not rooted by the segment")
            })?;
        validate_range_within_object(
            candidate.matrix_locator.byte_offset,
            candidate.matrix_locator.byte_length,
            matrix_ref.size_bytes,
            "matrix",
        )?;
        let attribute_ref = segment
            .attribute_objects
            .iter()
            .find(|reference| reference.key == candidate.attr_locator.object_key)
            .ok_or_else(|| {
                segment_error("candidate attribute locator is not rooted by the segment")
            })?;
        validate_range_within_object(
            candidate.attr_locator.byte_offset,
            candidate.attr_locator.byte_length,
            attribute_ref.size_bytes,
            "attribute",
        )?;
        requests.push(locator_request(
            &candidate.matrix_locator.object_key,
            candidate.matrix_locator.byte_offset,
            candidate.matrix_locator.byte_length,
            "matrix",
        )?);
        requests.push(locator_request(
            &candidate.attr_locator.object_key,
            candidate.attr_locator.byte_offset,
            candidate.attr_locator.byte_length,
            "attribute",
        )?);
    }
    Ok(requests)
}

fn validate_range_within_object(
    offset: u64,
    length: u64,
    object_size: u64,
    label: &str,
) -> Result<()> {
    if length == 0
        || offset
            .checked_add(length)
            .is_none_or(|end| end > object_size)
    {
        return Err(segment_error(format!(
            "candidate {label} locator exceeds its rooted object"
        )));
    }
    Ok(())
}

fn locator_request(key: &str, offset: u64, length: u64, label: &str) -> Result<ReadRequest> {
    let start = usize::try_from(offset)
        .map_err(|_| segment_error(format!("candidate {label} offset exceeds usize")))?;
    let end_u64 = offset
        .checked_add(length)
        .ok_or_else(|| segment_error(format!("candidate {label} range overflows")))?;
    let end = usize::try_from(end_u64)
        .map_err(|_| segment_error(format!("candidate {label} range exceeds usize")))?;
    Ok(ReadRequest {
        object_key: key.to_string(),
        range: Range { start, end },
    })
}

pub(crate) fn score_truth_wave(
    request: &SegmentSearchRequest<'_>,
    candidates: Vec<LateCandidate>,
    truth_bytes: &[bytes::Bytes],
) -> Result<Vec<SegmentScoredRow>> {
    let expected = candidates
        .len()
        .checked_mul(2)
        .ok_or_else(|| segment_error("truth output count overflows"))?;
    if truth_bytes.len() != expected {
        return Err(segment_error(
            "truth wave output count does not match its logical ranges",
        ));
    }
    let vector_dimension = usize::try_from(request.segment.vector_dimension)
        .map_err(|_| segment_error("segment vector dimension exceeds usize"))?;
    let mut rows = Vec::with_capacity(candidates.len());
    for (candidate_index, candidate) in candidates.into_iter().enumerate() {
        let matrix_index = candidate_index
            .checked_mul(2)
            .ok_or_else(|| segment_error("truth matrix output index overflows"))?;
        let attribute_index = matrix_index
            .checked_add(1)
            .ok_or_else(|| segment_error("truth attribute output index overflows"))?;
        let embedding = decode_matrix_row(
            truth_bytes
                .get(matrix_index)
                .ok_or_else(|| segment_error("truth wave omitted a matrix payload"))?,
            &candidate.matrix_locator,
            request.segment.matrix_dtype,
            vector_dimension,
            request.bounds.max_vectors_per_document,
        )?;
        let attributes = decode_attribute_row(
            truth_bytes
                .get(attribute_index)
                .ok_or_else(|| segment_error("truth wave omitted an attribute payload"))?,
            &candidate.attr_locator,
            request.bounds.max_attribute_payload_bytes,
        )?;
        if attributes != candidate.metadata.attributes {
            return Err(segment_error(
                "wave-one filter attributes disagree with exact attributes",
            ));
        }
        if !filter_matches(request.mandatory_filter, attributes.as_ref())
            || !filter_matches(request.request_filter, attributes.as_ref())
        {
            return Err(segment_error(
                "candidate failed the final exact attribute assertion",
            ));
        }
        let document = embedding.matrix_ref()?;
        let score = max_sim(&request.exact_query, &document)?;
        if !score.is_finite() {
            return Err(segment_error("exact MaxSim score is not finite"));
        }
        rows.push(SegmentScoredRow {
            id: candidate.id,
            score,
            content_hash: candidate.metadata.content_hash,
            source_sequence: candidate.metadata.source_sequence,
            parent_id: candidate.metadata.parent_id,
            unit_ordinal: candidate.metadata.unit_ordinal,
            attributes,
        });
    }
    Ok(rows)
}

fn trace_for(requests: &[ReadRequest], plan: &ReadPlan) -> SegmentWaveTrace {
    SegmentWaveTrace {
        logical_ranges: requests.len(),
        planned_requests: plan.planned_request_count(),
        planned_bytes: plan.planned_bytes(),
    }
}

pub(crate) fn compare_scored_rows(left: &SegmentScoredRow, right: &SegmentScoredRow) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.id.cmp(&right.id))
}

pub(crate) fn filter_matches(
    filter: Option<&Filter>,
    attributes: Option<&HashMap<String, AttributeValue>>,
) -> bool {
    filter
        .map(|filter| evaluate_filter_on_optional_attributes(filter, attributes))
        .unwrap_or(true)
}

fn segment_error(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!("invalid late segment search: {}", reason.into()))
}

fn map_read_plan_error(error: ReadPlanError) -> ZeppelinError {
    match error {
        ReadPlanError::Storage(error) => error,
        other => segment_error(format!("read plan failed: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use crate::embedding::{
        ArtifactChecksum, ContentHash, EmbeddingProfileId, FdeGenerationId, MatrixDtype,
        MultiVectorEmbedding, MultiVectorEpochId,
    };
    use crate::index::late_interaction::attribute_artifact::{
        build_attribute_blocks, AttributeBlockInputRow, BuiltAttributeBlock,
    };
    use crate::index::late_interaction::candidate::{
        build_late_candidate_index, BuiltLateCandidateIndex, LateCandidateBuildConfig,
        LateCandidateInputRow, LateRoutingMetric,
    };
    use crate::index::late_interaction::flat_candidate::{
        build_flat_candidate_artifact, LateFlatCandidateBuildConfig,
    };
    use crate::index::late_interaction::matrix_artifact::{
        build_matrix_blocks, BuiltMatrixBlock, MatrixBlockInputRow,
    };
    use crate::storage::read_plan::ReadPlanConfig;
    use crate::storage::ZeppelinStore;
    use crate::types::{AttributeValue, Filter};
    use crate::wal::{LateCandidateKind, LateInteractionSegmentRef};

    use super::{search_segment, SegmentSearchBounds, SegmentSearchRequest};

    const MAX_BYTES: usize = 1024 * 1024;

    struct Fixture {
        store: ZeppelinStore,
        segment: LateInteractionSegmentRef,
    }

    async fn fixture(candidate_k: usize, corrupt_matrix: bool) -> Fixture {
        fixture_of_kind(candidate_k, corrupt_matrix, LateCandidateKind::FlatSq8).await
    }

    async fn fixture_of_kind(
        candidate_k: usize,
        corrupt_matrix: bool,
        kind: LateCandidateKind,
    ) -> Fixture {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let epoch = MultiVectorEpochId::new([3; 32]);
        let generation = FdeGenerationId::new([4; 32]);
        let ids = ["blocked-high", "allowed-low"];
        let colors = ["red", "blue"];
        let matrices = vec![
            MatrixBlockInputRow::encoded(
                ids[0].to_string(),
                0,
                content_hash(ids[0]),
                MatrixDtype::F16,
                MultiVectorEmbedding::new(vec![2.0, 0.0], 1, 2, 4).expect("blocked matrix"),
            ),
            MatrixBlockInputRow::encoded(
                ids[1].to_string(),
                1,
                content_hash(ids[1]),
                MatrixDtype::F16,
                MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("allowed matrix"),
            ),
        ];
        let matrix_blocks = build_matrix_blocks(
            "test",
            "segment",
            MatrixDtype::F16,
            epoch,
            generation,
            2,
            MAX_BYTES,
            matrices,
        )
        .expect("matrix blocks");
        let attributes = ids
            .iter()
            .zip(colors)
            .enumerate()
            .map(|(ordinal, (id, color))| AttributeBlockInputRow {
                id: (*id).to_string(),
                ordinal: ordinal as u32,
                attributes: Some(HashMap::from([(
                    "color".to_string(),
                    AttributeValue::String(color.to_string()),
                )])),
            })
            .collect();
        let attribute_blocks = build_attribute_blocks("test", "segment", MAX_BYTES, attributes)
            .expect("attribute blocks");
        let candidate_rows = ids
            .iter()
            .zip(colors)
            .enumerate()
            .map(|(index, (id, color))| LateCandidateInputRow {
                id: (*id).to_string(),
                fde: if index == 0 {
                    vec![2.0, 0.0]
                } else {
                    vec![1.0, 0.0]
                },
                content_hash: content_hash(id),
                source_sequence: index as u64 + 1,
                parent_id: Some("parent".to_string()),
                unit_ordinal: Some(index as u32),
                matrix_locator: matrix_blocks[0].locators[index].clone(),
                attr_locator: attribute_blocks[0].locators[index].clone(),
                filter_attributes: Some(HashMap::from([(
                    "color".to_string(),
                    AttributeValue::String(color.to_string()),
                )])),
            })
            .collect();
        let (candidate_index, flat_candidate) = match kind {
            LateCandidateKind::Ivf => {
                let candidate = build_late_candidate_index(
                    "test",
                    "segment",
                    generation,
                    LateCandidateBuildConfig {
                        fde_dimension: 2,
                        nlist: 1,
                        probe_budget: 1,
                        candidate_k,
                        routing_metric: LateRoutingMetric::NegativeL2,
                        kmeans_max_iters: 20,
                        kmeans_epsilon: 1e-6,
                        max_cluster_bytes: MAX_BYTES,
                        max_bootstrap_bytes: MAX_BYTES,
                    },
                    candidate_rows,
                )
                .expect("candidate index");
                upload_candidate(&store, &candidate).await;
                (Some(candidate.index_ref), None)
            }
            LateCandidateKind::FlatSq8 => {
                let flat = build_flat_candidate_artifact(
                    "test",
                    "segment",
                    generation,
                    LateFlatCandidateBuildConfig {
                        fde_dimension: 2,
                        candidate_k,
                        max_artifact_bytes: MAX_BYTES,
                    },
                    candidate_rows,
                )
                .expect("flat candidate artifact");
                store
                    .put_create(&flat.reference.key, flat.bytes.clone())
                    .await
                    .expect("flat upload");
                (None, Some(flat.reference))
            }
        };
        upload_attributes(&store, &attribute_blocks).await;
        upload_matrices(&store, &matrix_blocks, corrupt_matrix).await;
        let segment = LateInteractionSegmentRef {
            id: "segment".to_string(),
            profile: EmbeddingProfileId::new("test-profile"),
            semantic_epoch: epoch,
            fde_generation: generation,
            matrix_dtype: MatrixDtype::F16,
            record_count: 2,
            total_vector_count: 2,
            vector_dimension: 2,
            fde_dimension: 2,
            coverage_sequence: 2,
            candidate_index,
            matrix_objects: matrix_blocks
                .into_iter()
                .map(|block| block.reference)
                .collect(),
            attribute_objects: attribute_blocks
                .into_iter()
                .map(|block| block.reference)
                .collect(),
            fts_objects: Vec::new(),
            artifact_origin: None,
            candidate_kind: kind,
            flat_candidate,
        };
        Fixture { store, segment }
    }

    fn content_hash(id: &str) -> ContentHash {
        ContentHash::new(*ArtifactChecksum::digest(id.as_bytes()).as_bytes())
    }

    async fn upload_candidate(store: &ZeppelinStore, candidate: &BuiltLateCandidateIndex) {
        store
            .put_create(
                &candidate.index_ref.bootstrap.key,
                candidate.bootstrap_bytes.clone(),
            )
            .await
            .expect("bootstrap upload");
        for cluster in &candidate.clusters {
            store
                .put_create(&cluster.reference.key, cluster.bytes.clone())
                .await
                .expect("cluster upload");
        }
    }

    async fn upload_attributes(store: &ZeppelinStore, blocks: &[BuiltAttributeBlock]) {
        for block in blocks {
            store
                .put_create(&block.reference.key, block.bytes.clone())
                .await
                .expect("attribute upload");
        }
    }

    async fn upload_matrices(store: &ZeppelinStore, blocks: &[BuiltMatrixBlock], corrupt: bool) {
        for block in blocks {
            let mut bytes = block.bytes.to_vec();
            if corrupt {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            store
                .put_create(&block.reference.key, bytes.into())
                .await
                .expect("matrix upload");
        }
    }

    fn bounds() -> SegmentSearchBounds {
        SegmentSearchBounds {
            max_resident_bytes: MAX_BYTES,
            max_cluster_bytes: MAX_BYTES,
            max_vectors_per_document: 4,
            max_attribute_payload_bytes: 4096,
        }
    }

    fn plan() -> ReadPlanConfig {
        ReadPlanConfig::new(4096, MAX_BYTES, 2).expect("read plan")
    }

    #[tokio::test]
    async fn flat_selection_reads_only_one_truth_wave() {
        let fixture = fixture(2, false).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let output = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &BTreeSet::new(),
            top_k: 2,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect("segment search");

        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.trace.candidate_wave.logical_ranges, 0);
        assert_eq!(output.trace.candidate_wave.planned_requests, 0);
        assert_eq!(output.trace.candidate_wave.planned_bytes, 0);
        assert_eq!(output.trace.truth_wave.logical_ranges, 4);
        assert_eq!(output.trace.truth_wave.planned_requests, 2);
    }

    #[tokio::test]
    async fn ivf_kind_still_plans_a_candidate_wave() {
        let fixture = fixture_of_kind(2, false, LateCandidateKind::Ivf).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let output = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &BTreeSet::new(),
            top_k: 2,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect("segment search");

        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.trace.candidate_wave.logical_ranges, 1);
        assert_eq!(output.trace.candidate_wave.planned_requests, 1);
        assert_eq!(output.trace.truth_wave.logical_ranges, 4);
        assert_eq!(output.trace.truth_wave.planned_requests, 2);
    }

    #[tokio::test]
    async fn candidate_filter_runs_before_candidate_k() {
        let fixture = fixture(1, false).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let blue = Filter::Eq {
            field: "color".to_string(),
            value: AttributeValue::String("blue".to_string()),
        };
        let output = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: Some(&blue),
            excluded_ids: &BTreeSet::new(),
            top_k: 1,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect("filtered segment search");

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].id, "allowed-low");
        assert_eq!(output.trace.truth_wave.logical_ranges, 2);
    }

    #[tokio::test]
    async fn overlay_exclusion_runs_before_candidate_k() {
        let fixture = fixture(1, false).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let excluded_ids = BTreeSet::from(["blocked-high".to_string()]);
        let output = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &excluded_ids,
            top_k: 1,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect("excluded segment search");

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].id, "allowed-low");
        assert_eq!(output.trace.truth_wave.logical_ranges, 2);
    }

    #[tokio::test]
    async fn corrupt_truth_payload_fails_checksum_validation() {
        let fixture = fixture(2, true).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let error = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &BTreeSet::new(),
            top_k: 2,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect_err("corrupt matrix payload must fail");

        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn zero_k_performs_no_reads() {
        let fixture = fixture(2, false).await;
        let query = MultiVectorEmbedding::new(vec![1.0, 0.0], 1, 2, 4).expect("query");
        let output = search_segment(SegmentSearchRequest {
            store: &fixture.store,
            bootstrap_cache: None,
            segment: &fixture.segment,
            exact_query: query.matrix_ref().expect("query matrix"),
            candidate_query_fde: &[1.0, 0.0],
            mandatory_filter: None,
            request_filter: None,
            excluded_ids: &BTreeSet::new(),
            top_k: 0,
            read_plan: &plan(),
            bounds: bounds(),
        })
        .await
        .expect("zero-k search");

        assert!(output.rows.is_empty());
        assert_eq!(output.trace, Default::default());
    }
}
