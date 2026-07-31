//! Candidate-only FDE IVF artifacts for immutable late-interaction segments.
//!
//! This module deliberately does not implement [`crate::index::VectorIndex`].
//! It owns only the first-stage candidate contract: resident centroid routing,
//! immutable cluster payload validation, exact metadata filtering before the
//! candidate budget, and raw-inner-product FDE ranking. Exact MaxSim remains a
//! separate second wave owned by the late-interaction query module.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::embedding::{ArtifactChecksum, ContentHash, FdeGenerationId};
use crate::error::{Result, ZeppelinError};
use crate::index::bitmap::build::build_cluster_bitmaps;
use crate::index::bitmap::{AttributeBitmaps, ClusterBitmapIndex};
use crate::index::distance::{dot_product_distance, euclidean_distance};
use crate::index::filter::evaluate_filter_on_optional_attributes;
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::storage::{NamespaceObjectFamily, NamespaceObjectKey};
use crate::types::{AttributeValue, Filter, VectorId};

use super::matrix_artifact::MatrixBlockLocator;

const BOOTSTRAP_MAGIC: &[u8; 4] = b"ZLB1";
const CLUSTER_MAGIC: &[u8; 4] = b"ZLC1";
const CANDIDATE_ARTIFACT_VERSION: u8 = 1;
const ENVELOPE_HEADER_LEN: usize = 4 + 1 + size_of::<u64>();

/// Persisted candidate-artifact format version.
pub(crate) const LATE_CANDIDATE_FORMAT_VERSION: u32 = 1;

/// Cluster-routing score used by a late candidate segment.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LateRoutingMetric {
    /// Rank centroids by the negated squared Euclidean distance.
    NegativeL2,
}

/// Persisted operating point for one candidate segment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateCandidateRecipe {
    /// Candidate FDE generation represented by every row.
    pub fde_generation: FdeGenerationId,
    /// Coordinates in every document and query FDE.
    pub fde_dimension: u32,
    /// Exact number of trained centroids.
    pub nlist: u32,
    /// Exact number of clusters routed for one query.
    pub probe_budget: u32,
    /// Candidate frontier retained after filtering and FDE scoring.
    pub candidate_k: u32,
    /// Centroid-routing score; independent from row scoring.
    pub routing_metric: LateRoutingMetric,
}

/// Direct locator for one exact attribute payload fetched in wave two.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttributeLocator {
    /// Exact immutable object key.
    pub(crate) object_key: String,
    /// Absolute start of the encoded attribute payload.
    pub(crate) byte_offset: u64,
    /// Exact encoded attribute payload length.
    pub(crate) byte_length: u64,
    /// SHA-256 over the ranged payload.
    pub(crate) payload_checksum: ArtifactChecksum,
}

/// Manifest-visible reference to one immutable candidate cluster object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateCandidateClusterRef {
    /// Exact immutable object key.
    pub key: String,
    /// SHA-256 over the complete cluster object.
    pub checksum: ArtifactChecksum,
    /// Complete cluster object size.
    pub size_bytes: u64,
    /// Zero-based centroid identity.
    pub cluster_id: u32,
    /// Zero-based physical shard within the logical centroid.
    pub shard_id: u32,
    /// Rows assigned to this centroid.
    pub row_count: u32,
    /// Persisted codec version.
    pub format_version: u32,
}

/// Manifest-visible reference to resident candidate bootstrap metadata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateCandidateBootstrapRef {
    /// Exact immutable object key.
    pub key: String,
    /// SHA-256 over the complete bootstrap object.
    pub checksum: ArtifactChecksum,
    /// Complete bootstrap object size.
    pub size_bytes: u64,
    /// Persisted routing and candidate operating point.
    pub recipe: LateCandidateRecipe,
    /// Total live rows covered by the candidate index.
    pub row_count: u64,
    /// Persisted codec version.
    pub format_version: u32,
}

/// Complete manifest-rooted candidate artifact descriptor.
///
/// Cluster references are duplicated outside the bootstrap so GC can expand
/// reachability without fetching or decoding an immutable artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LateCandidateIndexRef {
    /// Resident centroid bootstrap selected by the segment.
    pub bootstrap: LateCandidateBootstrapRef,
    /// Every physical cluster shard in canonical cluster/shard order.
    pub clusters: Vec<LateCandidateClusterRef>,
}

/// One candidate passed from the FDE wave to exact MaxSim.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LateCandidate {
    /// Retrieval-unit identity.
    pub(crate) id: VectorId,
    /// Raw-inner-product document-FDE score, higher is better.
    pub(crate) approx_fde_score: f32,
    /// Direct ranged-read locator for the exact document matrix.
    pub(crate) matrix_locator: MatrixBlockLocator,
    /// Direct ranged-read locator for exact attributes.
    pub(crate) attr_locator: AttributeLocator,
    /// Baseline identity needed to merge newer overlays and tombstones.
    pub(crate) metadata: LateCandidateMetadata,
}

/// Segment-baseline identity carried through candidate selection.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LateCandidateMetadata {
    /// Source content identity represented by the segment row.
    pub(crate) content_hash: ContentHash,
    /// Authoritative source mutation sequence.
    pub(crate) source_sequence: u64,
    /// Optional caller-provided parent identity.
    pub(crate) parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub(crate) unit_ordinal: Option<u32>,
    /// Exact attributes used by the wave-one filter evaluator.
    pub(crate) attributes: Option<HashMap<String, AttributeValue>>,
}

/// Explicit build controls supplied by late-interaction configuration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LateCandidateBuildConfig {
    /// Coordinates in every input FDE.
    pub(crate) fde_dimension: usize,
    /// Exact number of centroids to train.
    pub(crate) nlist: usize,
    /// Exact number of clusters routed per query.
    pub(crate) probe_budget: usize,
    /// Frontier retained after filtering and FDE scoring.
    pub(crate) candidate_k: usize,
    /// Centroid-routing metric selected by the lab.
    pub(crate) routing_metric: LateRoutingMetric,
    /// Maximum k-means iterations.
    pub(crate) kmeans_max_iters: usize,
    /// K-means convergence threshold.
    pub(crate) kmeans_epsilon: f64,
    /// Hard maximum complete cluster-object bytes.
    pub(crate) max_cluster_bytes: usize,
    /// Hard maximum resident bootstrap-object bytes.
    pub(crate) max_bootstrap_bytes: usize,
}

/// One live retrieval unit supplied by late segment compaction.
#[derive(Clone, Debug)]
pub(crate) struct LateCandidateInputRow {
    /// Retrieval-unit identity.
    pub(crate) id: VectorId,
    /// Raw uncompressed document FDE.
    pub(crate) fde: Vec<f32>,
    /// Source content identity represented by this row.
    pub(crate) content_hash: ContentHash,
    /// Authoritative source mutation sequence.
    pub(crate) source_sequence: u64,
    /// Optional caller-provided parent identity.
    pub(crate) parent_id: Option<String>,
    /// Optional ordinal within the parent.
    pub(crate) unit_ordinal: Option<u32>,
    /// Direct locator for the exact document matrix.
    pub(crate) matrix_locator: MatrixBlockLocator,
    /// Direct locator for exact attributes.
    pub(crate) attr_locator: AttributeLocator,
    /// Exact filter representation carried in wave one.
    pub(crate) filter_attributes: Option<HashMap<String, AttributeValue>>,
}

/// One encoded cluster object ready for immutable upload.
pub(crate) struct BuiltLateCandidateCluster {
    /// Authoritative object descriptor.
    pub(crate) reference: LateCandidateClusterRef,
    /// Complete immutable object bytes.
    pub(crate) bytes: Bytes,
}

/// Complete candidate-index build output prior to manifest publication.
pub(crate) struct BuiltLateCandidateIndex {
    /// Complete manifest-rooted artifact descriptor.
    pub(crate) index_ref: LateCandidateIndexRef,
    /// Complete resident-bootstrap bytes.
    pub(crate) bootstrap_bytes: Bytes,
    /// Complete immutable cluster objects in cluster-id order.
    pub(crate) clusters: Vec<BuiltLateCandidateCluster>,
}

/// One fetched cluster payload associated with its manifest-visible reference.
pub(crate) struct FetchedLateCandidateCluster<'a> {
    /// Reference selected by resident centroid routing.
    pub(crate) reference: &'a LateCandidateClusterRef,
    /// Complete fetched immutable object.
    pub(crate) bytes: &'a [u8],
}

/// Candidate-only search seam used by the two-wave late query path.
pub(crate) trait FdeCandidateIndex {
    /// Borrow the persisted operating point.
    fn recipe(&self) -> &LateCandidateRecipe;

    /// Rank resident centroids and return exactly the configured probe set.
    fn route(&self, query_fde: &[f32]) -> Result<Vec<LateCandidateClusterRef>>;

    /// Validate, filter, and score one complete routed cluster wave.
    fn candidates_from_fetched(
        &self,
        query_fde: &[f32],
        excluded_ids: &BTreeSet<VectorId>,
        mandatory_filter: Option<&Filter>,
        request_filter: Option<&Filter>,
        fetched: &[FetchedLateCandidateCluster<'_>],
        max_cluster_bytes: usize,
    ) -> Result<Vec<LateCandidate>>;
}

/// Resident centroid bootstrap for one manifest-selected late segment.
#[derive(Clone, Debug)]
pub(crate) struct ResidentLateCandidateIndex {
    reference: LateCandidateBootstrapRef,
    centroids: Vec<Vec<f32>>,
    clusters: Vec<LateCandidateClusterRef>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapWire {
    recipe: LateCandidateRecipe,
    row_count: u64,
    centroids: Vec<Vec<f32>>,
    clusters: Vec<LateCandidateClusterRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterWire {
    fde_generation: FdeGenerationId,
    fde_dimension: u32,
    cluster_id: u32,
    shard_id: u32,
    rows: Vec<ClusterRowWire>,
    filter_bitmaps: ClusterBitmapWire,
}

#[derive(Serialize)]
struct ClusterWireRef<'a> {
    fde_generation: FdeGenerationId,
    fde_dimension: u32,
    cluster_id: u32,
    shard_id: u32,
    rows: &'a [ClusterRowWire],
    filter_bitmaps: ClusterBitmapWire,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterRowWire {
    id: VectorId,
    fde: Vec<f32>,
    content_hash: ContentHash,
    source_sequence: u64,
    parent_id: Option<String>,
    unit_ordinal: Option<u32>,
    matrix_locator: MatrixBlockLocator,
    attr_locator: AttributeLocator,
    filter_attributes: Option<BTreeMap<String, AttributeValue>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterBitmapWire {
    vector_count: u32,
    fields: BTreeMap<String, AttributeBitmaps>,
}

/// Build one deterministic candidate-only FDE IVF artifact set.
pub(crate) fn build_late_candidate_index(
    namespace: &str,
    segment_id: &str,
    fde_generation: FdeGenerationId,
    config: LateCandidateBuildConfig,
    mut rows: Vec<LateCandidateInputRow>,
) -> Result<BuiltLateCandidateIndex> {
    validate_build_config(&config, rows.len())?;
    validate_segment_id(segment_id)?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));

    let mut ids = BTreeSet::new();
    for row in &rows {
        validate_input_row(row, config.fde_dimension)?;
        if !ids.insert(row.id.clone()) {
            return Err(candidate_error("candidate build contains a duplicate id"));
        }
    }

    let vector_refs: Vec<&[f32]> = rows.iter().map(|row| row.fde.as_slice()).collect();
    let centroids = train_kmeans(
        &vector_refs,
        config.fde_dimension,
        config.nlist,
        config.kmeans_max_iters,
        config.kmeans_epsilon,
    )?;
    if centroids.len() != config.nlist {
        return Err(candidate_error(
            "candidate k-means returned a different centroid count",
        ));
    }
    validate_vectors(&centroids, config.fde_dimension, "candidate centroid")?;

    let mut assignments = vec![Vec::new(); config.nlist];
    for row in rows {
        let cluster_id = nearest_centroid(&row.fde, &centroids)?;
        assignments[cluster_id].push(row);
    }
    if assignments.iter().any(Vec::is_empty) {
        return Err(candidate_error(
            "candidate k-means produced an empty cluster",
        ));
    }

    let recipe = LateCandidateRecipe {
        fde_generation,
        fde_dimension: u32::try_from(config.fde_dimension)
            .map_err(|_| candidate_error("candidate FDE dimension exceeds u32"))?,
        nlist: u32::try_from(config.nlist)
            .map_err(|_| candidate_error("candidate nlist exceeds u32"))?,
        probe_budget: u32::try_from(config.probe_budget)
            .map_err(|_| candidate_error("candidate probe budget exceeds u32"))?,
        candidate_k: u32::try_from(config.candidate_k)
            .map_err(|_| candidate_error("candidate K exceeds u32"))?,
        routing_metric: config.routing_metric,
    };

    let mut built_clusters = Vec::with_capacity(config.nlist);
    for (cluster_id, cluster_rows) in assignments.into_iter().enumerate() {
        let cluster_id = u32::try_from(cluster_id)
            .map_err(|_| candidate_error("candidate cluster id exceeds u32"))?;
        let rows = cluster_rows
            .into_iter()
            .map(cluster_row_wire)
            .collect::<Result<Vec<_>>>()?;
        for (shard_id, shard_rows) in shard_cluster_rows(
            fde_generation,
            recipe.fde_dimension,
            cluster_id,
            rows,
            config.max_cluster_bytes,
        )?
        .into_iter()
        .enumerate()
        {
            let shard_id = u32::try_from(shard_id)
                .map_err(|_| candidate_error("candidate cluster shard id exceeds u32"))?;
            let cluster_wire = ClusterWireRef {
                fde_generation,
                fde_dimension: recipe.fde_dimension,
                cluster_id,
                shard_id,
                filter_bitmaps: build_cluster_bitmap_wire(&shard_rows)?,
                rows: &shard_rows,
            };
            let bytes = encode_envelope(CLUSTER_MAGIC, &cluster_wire, "candidate cluster")?;
            if bytes.len() > config.max_cluster_bytes {
                return Err(candidate_error(
                    "candidate cluster sharding exceeded the configured object bound",
                ));
            }
            let checksum = ArtifactChecksum::digest(&bytes);
            let key = late_segment_key(
                namespace,
                segment_id,
                &format!(
                    "candidate-cluster-{cluster_id}-shard-{shard_id}-{}.bin",
                    checksum.to_hex()
                ),
            )?;
            let reference = LateCandidateClusterRef {
                key,
                checksum,
                size_bytes: u64::try_from(bytes.len())
                    .map_err(|_| candidate_error("candidate cluster size exceeds u64"))?,
                cluster_id,
                shard_id,
                row_count: u32::try_from(shard_rows.len())
                    .map_err(|_| candidate_error("candidate cluster row count exceeds u32"))?,
                format_version: LATE_CANDIDATE_FORMAT_VERSION,
            };
            built_clusters.push(BuiltLateCandidateCluster { reference, bytes });
        }
    }

    let row_count = built_clusters.iter().try_fold(0_u64, |total, cluster| {
        total
            .checked_add(u64::from(cluster.reference.row_count))
            .ok_or_else(|| candidate_error("candidate row count overflows u64"))
    })?;
    let cluster_refs: Vec<LateCandidateClusterRef> = built_clusters
        .iter()
        .map(|cluster| cluster.reference.clone())
        .collect();
    let bootstrap_wire = BootstrapWire {
        recipe: recipe.clone(),
        row_count,
        centroids,
        clusters: cluster_refs,
    };
    let bootstrap_bytes = encode_envelope(BOOTSTRAP_MAGIC, &bootstrap_wire, "candidate bootstrap")?;
    if bootstrap_bytes.len() > config.max_bootstrap_bytes {
        return Err(candidate_error(format!(
            "candidate bootstrap is {} bytes, resident maximum is {}",
            bootstrap_bytes.len(),
            config.max_bootstrap_bytes
        )));
    }
    let checksum = ArtifactChecksum::digest(&bootstrap_bytes);
    let key = late_segment_key(
        namespace,
        segment_id,
        &format!("candidate-bootstrap-{}.bin", checksum.to_hex()),
    )?;
    let bootstrap_ref = LateCandidateBootstrapRef {
        key,
        checksum,
        size_bytes: u64::try_from(bootstrap_bytes.len())
            .map_err(|_| candidate_error("candidate bootstrap size exceeds u64"))?,
        recipe,
        row_count,
        format_version: LATE_CANDIDATE_FORMAT_VERSION,
    };
    let index_ref = LateCandidateIndexRef {
        bootstrap: bootstrap_ref,
        clusters: built_clusters
            .iter()
            .map(|cluster| cluster.reference.clone())
            .collect(),
    };
    Ok(BuiltLateCandidateIndex {
        index_ref,
        bootstrap_bytes,
        clusters: built_clusters,
    })
}

/// Reconstruct every persisted baseline row for a full-rebuild compaction.
///
/// The bootstrap and every section-rooted physical cluster must be supplied
/// exactly once. This path performs no routing, filtering, or truncation.
pub(crate) fn decode_all_candidate_rows(
    index_ref: &LateCandidateIndexRef,
    bootstrap_bytes: &[u8],
    cluster_payloads: &[FetchedLateCandidateCluster<'_>],
    max_resident_bytes: usize,
    max_cluster_bytes: usize,
) -> Result<Vec<LateCandidateInputRow>> {
    let resident =
        ResidentLateCandidateIndex::from_index_ref(bootstrap_bytes, index_ref, max_resident_bytes)?;
    if cluster_payloads.len() != index_ref.clusters.len() {
        return Err(candidate_error(format!(
            "full candidate decode received {} clusters, expected {}",
            cluster_payloads.len(),
            index_ref.clusters.len()
        )));
    }
    let mut payloads = BTreeMap::new();
    for payload in cluster_payloads {
        let identity = (payload.reference.cluster_id, payload.reference.shard_id);
        let Some(expected) = index_ref
            .clusters
            .iter()
            .find(|reference| (reference.cluster_id, reference.shard_id) == identity)
        else {
            return Err(candidate_error(
                "full candidate decode received an unrooted cluster",
            ));
        };
        if expected != payload.reference || payloads.insert(identity, payload.bytes).is_some() {
            return Err(candidate_error(
                "full candidate decode received a mismatched or duplicate cluster",
            ));
        }
    }

    let mut ids = BTreeSet::new();
    let mut rows = Vec::new();
    for reference in &index_ref.clusters {
        let identity = (reference.cluster_id, reference.shard_id);
        let bytes = payloads
            .get(&identity)
            .ok_or_else(|| candidate_error("full candidate decode omitted a rooted cluster"))?;
        let cluster = decode_cluster(bytes, reference, resident.recipe(), max_cluster_bytes)?;
        for row in cluster.rows {
            if !ids.insert(row.id.clone()) {
                return Err(candidate_error(
                    "full candidate decode contains a duplicate retrieval-unit id",
                ));
            }
            rows.push(LateCandidateInputRow {
                id: row.id,
                fde: row.fde,
                content_hash: row.content_hash,
                source_sequence: row.source_sequence,
                parent_id: row.parent_id,
                unit_ordinal: row.unit_ordinal,
                matrix_locator: row.matrix_locator,
                attr_locator: row.attr_locator,
                filter_attributes: row
                    .filter_attributes
                    .map(|attributes| attributes.into_iter().collect::<HashMap<_, _>>()),
            });
        }
    }
    if u64::try_from(rows.len()).ok() != Some(index_ref.bootstrap.row_count) {
        return Err(candidate_error(
            "full candidate decode row count disagrees with its bootstrap",
        ));
    }
    Ok(rows)
}

impl ResidentLateCandidateIndex {
    /// Validate and hydrate a manifest-rooted candidate artifact bundle.
    pub(crate) fn from_index_ref(
        bytes: &[u8],
        reference: &LateCandidateIndexRef,
        max_resident_bytes: usize,
    ) -> Result<Self> {
        let resident = Self::from_bytes(bytes, &reference.bootstrap, max_resident_bytes)?;
        if resident.clusters != reference.clusters {
            return Err(candidate_error(
                "candidate bootstrap cluster refs disagree with the manifest-rooted bundle",
            ));
        }
        Ok(resident)
    }

    /// Validate and hydrate one manifest-selected resident bootstrap.
    pub(crate) fn from_bytes(
        bytes: &[u8],
        reference: &LateCandidateBootstrapRef,
        max_resident_bytes: usize,
    ) -> Result<Self> {
        validate_reference_bytes(
            bytes,
            reference.size_bytes,
            reference.checksum,
            max_resident_bytes,
            "candidate bootstrap",
        )?;
        if reference.format_version != LATE_CANDIDATE_FORMAT_VERSION {
            return Err(candidate_error(
                "unsupported candidate bootstrap reference version",
            ));
        }
        let wire: BootstrapWire = decode_envelope(bytes, BOOTSTRAP_MAGIC, "candidate bootstrap")?;
        if wire.recipe != reference.recipe || wire.row_count != reference.row_count {
            return Err(candidate_error(
                "candidate bootstrap disagrees with its manifest reference",
            ));
        }
        validate_recipe(&wire.recipe)?;
        let dimension = usize::try_from(wire.recipe.fde_dimension)
            .map_err(|_| candidate_error("candidate FDE dimension exceeds usize"))?;
        validate_vectors(&wire.centroids, dimension, "candidate centroid")?;
        if wire.centroids.len()
            != usize::try_from(wire.recipe.nlist)
                .map_err(|_| candidate_error("candidate nlist exceeds usize"))?
            || wire.clusters.len() < wire.centroids.len()
        {
            return Err(candidate_error(
                "candidate bootstrap centroid and cluster counts disagree with nlist",
            ));
        }
        let mut row_count = 0_u64;
        let mut expected_cluster_id = 0_u32;
        let mut expected_shard_id = 0_u32;
        for (index, cluster) in wire.clusters.iter().enumerate() {
            validate_cluster_ref(cluster)?;
            if cluster.cluster_id != expected_cluster_id || cluster.shard_id != expected_shard_id {
                return Err(candidate_error(
                    "candidate bootstrap cluster references are not canonical",
                ));
            }
            if usize::try_from(cluster.cluster_id)
                .ok()
                .is_none_or(|cluster_id| cluster_id >= wire.centroids.len())
            {
                return Err(candidate_error(
                    "candidate bootstrap cluster id exceeds nlist",
                ));
            }
            row_count = row_count
                .checked_add(u64::from(cluster.row_count))
                .ok_or_else(|| candidate_error("candidate row count overflows u64"))?;
            let next = wire.clusters.get(index + 1);
            if next.is_some_and(|next| next.cluster_id == cluster.cluster_id) {
                expected_shard_id = expected_shard_id
                    .checked_add(1)
                    .ok_or_else(|| candidate_error("candidate shard id overflows u32"))?;
            } else {
                expected_cluster_id = expected_cluster_id
                    .checked_add(1)
                    .ok_or_else(|| candidate_error("candidate cluster id overflows u32"))?;
                expected_shard_id = 0;
            }
        }
        if expected_cluster_id != wire.recipe.nlist {
            return Err(candidate_error(
                "candidate bootstrap does not cover every logical cluster",
            ));
        }
        if row_count != wire.row_count {
            return Err(candidate_error(
                "candidate bootstrap cluster rows disagree with total row count",
            ));
        }
        Ok(Self {
            reference: reference.clone(),
            centroids: wire.centroids,
            clusters: wire.clusters,
        })
    }
}

impl FdeCandidateIndex for ResidentLateCandidateIndex {
    fn recipe(&self) -> &LateCandidateRecipe {
        &self.reference.recipe
    }

    fn route(&self, query_fde: &[f32]) -> Result<Vec<LateCandidateClusterRef>> {
        validate_query(query_fde, self.reference.recipe.fde_dimension)?;
        let probe_budget = usize::try_from(self.reference.recipe.probe_budget)
            .map_err(|_| candidate_error("candidate probe budget exceeds usize"))?;
        let mut ranked: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(cluster_id, centroid)| {
                let score = match self.reference.recipe.routing_metric {
                    LateRoutingMetric::NegativeL2 => -euclidean_distance(query_fde, centroid),
                };
                (cluster_id, score)
            })
            .collect();
        ranked.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(ranked
            .into_iter()
            .take(probe_budget)
            .flat_map(|(cluster_id, _)| {
                self.clusters
                    .iter()
                    .filter(move |reference| {
                        usize::try_from(reference.cluster_id).ok() == Some(cluster_id)
                    })
                    .cloned()
            })
            .collect())
    }

    fn candidates_from_fetched(
        &self,
        query_fde: &[f32],
        excluded_ids: &BTreeSet<VectorId>,
        mandatory_filter: Option<&Filter>,
        request_filter: Option<&Filter>,
        fetched: &[FetchedLateCandidateCluster<'_>],
        max_cluster_bytes: usize,
    ) -> Result<Vec<LateCandidate>> {
        validate_query(query_fde, self.reference.recipe.fde_dimension)?;
        if max_cluster_bytes == 0 {
            return Err(candidate_error(
                "candidate cluster byte limit must be positive",
            ));
        }
        let expected = self.route(query_fde)?;
        if fetched.len() != expected.len() {
            return Err(candidate_error(format!(
                "candidate wave returned {} clusters, expected {}",
                fetched.len(),
                expected.len()
            )));
        }

        let expected_by_id: BTreeMap<(u32, u32), &LateCandidateClusterRef> = expected
            .iter()
            .map(|reference| ((reference.cluster_id, reference.shard_id), reference))
            .collect();
        let mut fetched_ids = BTreeSet::new();
        let mut seen_ids = BTreeSet::new();
        let mut candidates = Vec::new();
        for payload in fetched {
            let shard_identity = (payload.reference.cluster_id, payload.reference.shard_id);
            let Some(expected_ref) = expected_by_id.get(&shard_identity) else {
                return Err(candidate_error(
                    "candidate wave returned an unrequested cluster",
                ));
            };
            if *expected_ref != payload.reference || !fetched_ids.insert(shard_identity) {
                return Err(candidate_error(
                    "candidate wave returned a mismatched or duplicate cluster",
                ));
            }
            let cluster = decode_cluster(
                payload.bytes,
                payload.reference,
                &self.reference.recipe,
                max_cluster_bytes,
            )?;
            for row in cluster.rows {
                if !seen_ids.insert(row.id.clone()) {
                    return Err(candidate_error(
                        "candidate wave contains a duplicate retrieval-unit id",
                    ));
                }
                if excluded_ids.contains(&row.id) {
                    continue;
                }
                let attributes = row.filter_attributes.as_ref().map(|attributes| {
                    attributes
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<HashMap<_, _>>()
                });
                if !filter_matches(mandatory_filter, attributes.as_ref())
                    || !filter_matches(request_filter, attributes.as_ref())
                {
                    continue;
                }
                let approx_fde_score = -dot_product_distance(query_fde, &row.fde);
                if !approx_fde_score.is_finite() {
                    return Err(candidate_error(
                        "candidate raw inner-product score is not finite",
                    ));
                }
                candidates.push(LateCandidate {
                    id: row.id,
                    approx_fde_score,
                    matrix_locator: row.matrix_locator,
                    attr_locator: row.attr_locator,
                    metadata: LateCandidateMetadata {
                        content_hash: row.content_hash,
                        source_sequence: row.source_sequence,
                        parent_id: row.parent_id,
                        unit_ordinal: row.unit_ordinal,
                        attributes,
                    },
                });
            }
        }
        if fetched_ids.len() != expected_by_id.len() {
            return Err(candidate_error("candidate wave omitted a routed cluster"));
        }
        candidates.sort_by(|left, right| {
            right
                .approx_fde_score
                .total_cmp(&left.approx_fde_score)
                .then_with(|| left.id.cmp(&right.id))
        });
        candidates.truncate(
            usize::try_from(self.reference.recipe.candidate_k)
                .map_err(|_| candidate_error("candidate K exceeds usize"))?,
        );
        Ok(candidates)
    }
}

fn cluster_row_wire(row: LateCandidateInputRow) -> Result<ClusterRowWire> {
    let filter_attributes = row.filter_attributes.map(|attributes| {
        attributes
            .into_iter()
            .collect::<BTreeMap<String, AttributeValue>>()
    });
    Ok(ClusterRowWire {
        id: row.id,
        fde: row.fde,
        content_hash: row.content_hash,
        source_sequence: row.source_sequence,
        parent_id: row.parent_id,
        unit_ordinal: row.unit_ordinal,
        matrix_locator: row.matrix_locator,
        attr_locator: row.attr_locator,
        filter_attributes,
    })
}

fn shard_cluster_rows(
    fde_generation: FdeGenerationId,
    fde_dimension: u32,
    cluster_id: u32,
    rows: Vec<ClusterRowWire>,
    max_cluster_bytes: usize,
) -> Result<Vec<Vec<ClusterRowWire>>> {
    if rows.is_empty() {
        return Err(candidate_error("candidate logical cluster cannot be empty"));
    }
    let empty_rows = Vec::new();
    let empty_wire = ClusterWireRef {
        fde_generation,
        fde_dimension,
        cluster_id,
        shard_id: u32::MAX,
        rows: &empty_rows,
        filter_bitmaps: build_cluster_bitmap_wire(&empty_rows)?,
    };
    let empty_payload = rmp_serde::to_vec_named(&empty_wire).map_err(|error| {
        candidate_error(format!(
            "failed to size candidate cluster envelope: {error}"
        ))
    })?;
    let fixed_payload_bytes = empty_payload
        .len()
        .checked_sub(1)
        .ok_or_else(|| candidate_error("candidate cluster sizing underflow"))?;

    let mut shards = Vec::new();
    let mut current = Vec::new();
    let mut current_row_bytes = 0_usize;
    for row in rows {
        let row_bytes = rmp_serde::to_vec_named(&row)
            .map_err(|error| candidate_error(format!("failed to size candidate row: {error}")))?
            .len();
        let candidate_count = current
            .len()
            .checked_add(1)
            .ok_or_else(|| candidate_error("candidate shard row count overflows"))?;
        let candidate_row_bytes = current_row_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| candidate_error("candidate shard bytes overflow"))?;
        let candidate_bytes = ENVELOPE_HEADER_LEN
            .checked_add(fixed_payload_bytes)
            .and_then(|bytes| bytes.checked_add(messagepack_array_header_len(candidate_count)))
            .and_then(|bytes| bytes.checked_add(candidate_row_bytes))
            .ok_or_else(|| candidate_error("candidate cluster size overflows"))?;
        if !current.is_empty() && candidate_bytes > max_cluster_bytes {
            shards.push(std::mem::take(&mut current));
            current_row_bytes = 0;
        }
        current_row_bytes = current_row_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| candidate_error("candidate shard bytes overflow"))?;
        current.push(row);
        let single_or_current_bytes = ENVELOPE_HEADER_LEN
            .checked_add(fixed_payload_bytes)
            .and_then(|bytes| bytes.checked_add(messagepack_array_header_len(current.len())))
            .and_then(|bytes| bytes.checked_add(current_row_bytes))
            .ok_or_else(|| candidate_error("candidate cluster size overflows"))?;
        if single_or_current_bytes > max_cluster_bytes {
            return Err(candidate_error(format!(
                "one candidate row cannot fit the {max_cluster_bytes}-byte cluster object bound"
            )));
        }
    }
    if !current.is_empty() {
        shards.push(current);
    }
    let mut bounded = Vec::new();
    for shard in shards {
        split_cluster_shard_to_bound(
            fde_generation,
            fde_dimension,
            cluster_id,
            shard,
            max_cluster_bytes,
            &mut bounded,
        )?;
    }
    Ok(bounded)
}

const fn messagepack_array_header_len(length: usize) -> usize {
    if length <= 15 {
        1
    } else if length <= u16::MAX as usize {
        3
    } else {
        5
    }
}

fn split_cluster_shard_to_bound(
    fde_generation: FdeGenerationId,
    fde_dimension: u32,
    cluster_id: u32,
    rows: Vec<ClusterRowWire>,
    max_cluster_bytes: usize,
    output: &mut Vec<Vec<ClusterRowWire>>,
) -> Result<()> {
    let wire = ClusterWireRef {
        fde_generation,
        fde_dimension,
        cluster_id,
        shard_id: u32::MAX,
        filter_bitmaps: build_cluster_bitmap_wire(&rows)?,
        rows: &rows,
    };
    if encode_envelope(CLUSTER_MAGIC, &wire, "candidate cluster")?.len() <= max_cluster_bytes {
        output.push(rows);
        return Ok(());
    }
    if rows.len() == 1 {
        return Err(candidate_error(format!(
            "one candidate row and its bitmap cannot fit the {max_cluster_bytes}-byte cluster object bound"
        )));
    }
    let mut left = rows;
    let right = left.split_off(left.len() / 2);
    split_cluster_shard_to_bound(
        fde_generation,
        fde_dimension,
        cluster_id,
        left,
        max_cluster_bytes,
        output,
    )?;
    split_cluster_shard_to_bound(
        fde_generation,
        fde_dimension,
        cluster_id,
        right,
        max_cluster_bytes,
        output,
    )
}

fn build_cluster_bitmap_wire(rows: &[ClusterRowWire]) -> Result<ClusterBitmapWire> {
    let attributes: Vec<Option<HashMap<String, AttributeValue>>> = rows
        .iter()
        .map(|row| {
            row.filter_attributes.as_ref().map(|attributes| {
                attributes
                    .iter()
                    .map(|(field, value)| (field.clone(), value.clone()))
                    .collect()
            })
        })
        .collect();
    let refs: Vec<Option<&HashMap<String, AttributeValue>>> =
        attributes.iter().map(Option::as_ref).collect();
    let index = build_cluster_bitmaps(&refs);
    if usize::try_from(index.vector_count).ok() != Some(rows.len()) {
        return Err(candidate_error(
            "candidate bitmap builder row count disagrees with its physical shard",
        ));
    }
    Ok(ClusterBitmapWire {
        vector_count: index.vector_count,
        fields: index.fields.into_iter().collect(),
    })
}

impl ClusterBitmapWire {
    fn to_index(&self) -> ClusterBitmapIndex {
        ClusterBitmapIndex {
            vector_count: self.vector_count,
            fields: self
                .fields
                .iter()
                .map(|(field, bitmaps)| (field.clone(), bitmaps.clone()))
                .collect(),
        }
    }
}

fn decode_cluster(
    bytes: &[u8],
    reference: &LateCandidateClusterRef,
    recipe: &LateCandidateRecipe,
    max_cluster_bytes: usize,
) -> Result<ClusterWire> {
    validate_cluster_ref(reference)?;
    validate_reference_bytes(
        bytes,
        reference.size_bytes,
        reference.checksum,
        max_cluster_bytes,
        "candidate cluster",
    )?;
    let wire: ClusterWire = decode_envelope(bytes, CLUSTER_MAGIC, "candidate cluster")?;
    let filter_bitmap_index = wire.filter_bitmaps.to_index();
    if wire.fde_generation != recipe.fde_generation
        || wire.fde_dimension != recipe.fde_dimension
        || wire.cluster_id != reference.cluster_id
        || wire.shard_id != reference.shard_id
        || u32::try_from(wire.rows.len()).ok() != Some(reference.row_count)
        || filter_bitmap_index.vector_count != reference.row_count
    {
        return Err(candidate_error(
            "candidate cluster disagrees with its reference or bootstrap recipe",
        ));
    }
    let dimension = usize::try_from(recipe.fde_dimension)
        .map_err(|_| candidate_error("candidate FDE dimension exceeds usize"))?;
    let mut ids = BTreeSet::new();
    for row in &wire.rows {
        if row.id.is_empty() || !ids.insert(row.id.clone()) {
            return Err(candidate_error(
                "candidate cluster has an empty or duplicate id",
            ));
        }
        validate_vector(&row.fde, dimension, "candidate row FDE")?;
        validate_matrix_locator(&row.matrix_locator)?;
        validate_attribute_locator(&row.attr_locator)?;
        validate_filter_attributes(row.filter_attributes.as_ref())?;
    }
    Ok(wire)
}

fn validate_build_config(config: &LateCandidateBuildConfig, row_count: usize) -> Result<()> {
    if config.fde_dimension == 0
        || config.nlist == 0
        || config.nlist > row_count
        || config.probe_budget == 0
        || config.probe_budget > config.nlist
        || config.candidate_k == 0
        || config.kmeans_max_iters == 0
        || !config.kmeans_epsilon.is_finite()
        || config.kmeans_epsilon < 0.0
        || config.max_cluster_bytes <= ENVELOPE_HEADER_LEN
        || config.max_bootstrap_bytes <= ENVELOPE_HEADER_LEN
    {
        return Err(candidate_error(
            "invalid explicit late candidate build configuration",
        ));
    }
    u32::try_from(config.fde_dimension)
        .map_err(|_| candidate_error("candidate FDE dimension exceeds u32"))?;
    u32::try_from(config.nlist).map_err(|_| candidate_error("candidate nlist exceeds u32"))?;
    u32::try_from(config.probe_budget)
        .map_err(|_| candidate_error("candidate probe budget exceeds u32"))?;
    u32::try_from(config.candidate_k).map_err(|_| candidate_error("candidate K exceeds u32"))?;
    Ok(())
}

fn validate_recipe(recipe: &LateCandidateRecipe) -> Result<()> {
    if recipe.fde_dimension == 0
        || recipe.nlist == 0
        || recipe.probe_budget == 0
        || recipe.probe_budget > recipe.nlist
        || recipe.candidate_k == 0
    {
        return Err(candidate_error("invalid persisted candidate recipe"));
    }
    Ok(())
}

fn validate_input_row(row: &LateCandidateInputRow, dimension: usize) -> Result<()> {
    if row.id.is_empty() {
        return Err(candidate_error("candidate input id cannot be empty"));
    }
    validate_vector(&row.fde, dimension, "candidate input FDE")?;
    validate_matrix_locator(&row.matrix_locator)?;
    validate_attribute_locator(&row.attr_locator)?;
    let ordered = row.filter_attributes.as_ref().map(|attributes| {
        attributes
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    });
    validate_filter_attributes(ordered.as_ref())
}

fn validate_query(query: &[f32], dimension: u32) -> Result<()> {
    let dimension = usize::try_from(dimension)
        .map_err(|_| candidate_error("candidate FDE dimension exceeds usize"))?;
    validate_vector(query, dimension, "candidate query FDE")
}

fn validate_vectors(vectors: &[Vec<f32>], dimension: usize, label: &str) -> Result<()> {
    if vectors.is_empty() {
        return Err(candidate_error(format!("{label} set cannot be empty")));
    }
    for vector in vectors {
        validate_vector(vector, dimension, label)?;
    }
    Ok(())
}

fn validate_vector(vector: &[f32], dimension: usize, label: &str) -> Result<()> {
    if vector.len() != dimension {
        return Err(ZeppelinError::DimensionMismatch {
            expected: dimension,
            actual: vector.len(),
        });
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(candidate_error(format!(
            "{label} contains a non-finite value"
        )));
    }
    Ok(())
}

fn validate_matrix_locator(locator: &MatrixBlockLocator) -> Result<()> {
    validate_locator_range(
        &locator.object_key,
        locator.byte_offset,
        locator.byte_length,
        "matrix",
    )?;
    if locator.vector_count == 0 {
        return Err(candidate_error("candidate matrix locator has zero vectors"));
    }
    Ok(())
}

fn validate_attribute_locator(locator: &AttributeLocator) -> Result<()> {
    validate_locator_range(
        &locator.object_key,
        locator.byte_offset,
        locator.byte_length,
        "attribute",
    )
}

fn validate_locator_range(key: &str, offset: u64, length: u64, label: &str) -> Result<()> {
    if key.is_empty() || length == 0 || offset.checked_add(length).is_none() {
        return Err(candidate_error(format!(
            "candidate {label} locator is invalid"
        )));
    }
    Ok(())
}

fn validate_filter_attributes(attributes: Option<&BTreeMap<String, AttributeValue>>) -> Result<()> {
    let Some(attributes) = attributes else {
        return Ok(());
    };
    for (field, value) in attributes {
        if field.is_empty() {
            return Err(candidate_error(
                "candidate filter attribute field cannot be empty",
            ));
        }
        match value {
            AttributeValue::Float(value) if !value.is_finite() => {
                return Err(candidate_error(
                    "candidate filter attribute contains a non-finite float",
                ));
            }
            AttributeValue::FloatList(values) if values.iter().any(|value| !value.is_finite()) => {
                return Err(candidate_error(
                    "candidate filter attribute contains a non-finite float list value",
                ));
            }
            AttributeValue::String(_)
            | AttributeValue::Integer(_)
            | AttributeValue::Float(_)
            | AttributeValue::Bool(_)
            | AttributeValue::StringList(_)
            | AttributeValue::IntegerList(_)
            | AttributeValue::FloatList(_) => {}
        }
    }
    Ok(())
}

fn validate_cluster_ref(reference: &LateCandidateClusterRef) -> Result<()> {
    if reference.key.is_empty()
        || reference.size_bytes == 0
        || reference.row_count == 0
        || reference.format_version != LATE_CANDIDATE_FORMAT_VERSION
    {
        return Err(candidate_error("invalid candidate cluster reference"));
    }
    Ok(())
}

fn validate_reference_bytes(
    bytes: &[u8],
    expected_size: u64,
    expected_checksum: ArtifactChecksum,
    maximum_bytes: usize,
    label: &str,
) -> Result<()> {
    if maximum_bytes == 0 || bytes.len() > maximum_bytes {
        return Err(candidate_error(format!(
            "{label} exceeds its configured byte limit"
        )));
    }
    if u64::try_from(bytes.len()).ok() != Some(expected_size) {
        return Err(candidate_error(format!("{label} size mismatch")));
    }
    if ArtifactChecksum::digest(bytes) != expected_checksum {
        return Err(candidate_error(format!("{label} checksum mismatch")));
    }
    Ok(())
}

fn nearest_centroid(vector: &[f32], centroids: &[Vec<f32>]) -> Result<usize> {
    let mut best: Option<(usize, f32)> = None;
    for (cluster_id, centroid) in centroids.iter().enumerate() {
        if centroid.len() != vector.len() {
            return Err(ZeppelinError::DimensionMismatch {
                expected: vector.len(),
                actual: centroid.len(),
            });
        }
        let distance = euclidean_distance(vector, centroid);
        if !distance.is_finite() {
            return Err(candidate_error("candidate centroid distance is not finite"));
        }
        if best.as_ref().is_none_or(|(best_id, best_distance)| {
            distance < *best_distance || (distance == *best_distance && cluster_id < *best_id)
        }) {
            best = Some((cluster_id, distance));
        }
    }
    best.map(|(cluster_id, _)| cluster_id)
        .ok_or_else(|| candidate_error("candidate centroid set cannot be empty"))
}

fn filter_matches(
    filter: Option<&Filter>,
    attributes: Option<&HashMap<String, AttributeValue>>,
) -> bool {
    filter
        .map(|filter| evaluate_filter_on_optional_attributes(filter, attributes))
        .unwrap_or(true)
}

fn late_segment_key(namespace: &str, segment_id: &str, file_name: &str) -> Result<String> {
    let key = format!(
        "{}{segment_id}/{file_name}",
        NamespaceObjectFamily::LateSegment.namespace_prefix(namespace)
    );
    let owned = NamespaceObjectKey::classify(namespace, key.clone())?;
    if owned.family() != NamespaceObjectFamily::LateSegment {
        return Err(candidate_error(
            "candidate artifact key is outside the late-segment family",
        ));
    }
    Ok(key)
}

fn validate_segment_id(segment_id: &str) -> Result<()> {
    if segment_id.is_empty() || segment_id.contains('/') {
        return Err(candidate_error(
            "candidate segment id must be one non-empty path component",
        ));
    }
    Ok(())
}

fn encode_envelope<T: Serialize>(magic: &[u8; 4], wire: &T, label: &str) -> Result<Bytes> {
    let payload = rmp_serde::to_vec_named(wire)
        .map_err(|error| candidate_error(format!("failed to encode {label}: {error}")))?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| candidate_error(format!("{label} payload exceeds u64")))?;
    let capacity = ENVELOPE_HEADER_LEN
        .checked_add(payload.len())
        .ok_or_else(|| candidate_error(format!("{label} size overflows")))?;
    let mut bytes = BytesMut::with_capacity(capacity);
    bytes.extend_from_slice(magic);
    bytes.put_u8(CANDIDATE_ARTIFACT_VERSION);
    bytes.put_u64_le(payload_len);
    bytes.extend_from_slice(&payload);
    Ok(bytes.freeze())
}

fn decode_envelope<T: DeserializeOwned>(bytes: &[u8], magic: &[u8; 4], label: &str) -> Result<T> {
    if bytes.len() < ENVELOPE_HEADER_LEN || &bytes[..4] != magic {
        return Err(candidate_error(format!("invalid {label} magic or header")));
    }
    if bytes[4] != CANDIDATE_ARTIFACT_VERSION {
        return Err(candidate_error(format!("unsupported {label} version")));
    }
    let mut length_bytes = [0_u8; size_of::<u64>()];
    length_bytes.copy_from_slice(&bytes[5..ENVELOPE_HEADER_LEN]);
    let payload_len = usize::try_from(u64::from_le_bytes(length_bytes))
        .map_err(|_| candidate_error(format!("{label} payload length exceeds usize")))?;
    let expected_len = ENVELOPE_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| candidate_error(format!("{label} size overflows")))?;
    if bytes.len() != expected_len {
        return Err(candidate_error(format!(
            "{label} payload length or trailing bytes mismatch"
        )));
    }
    rmp_serde::from_slice(&bytes[ENVELOPE_HEADER_LEN..])
        .map_err(|error| candidate_error(format!("failed to decode {label}: {error}")))
}

fn candidate_error(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!(
        "invalid late candidate artifact: {}",
        reason.into()
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use crate::embedding::{ArtifactChecksum, FdeGenerationId};
    use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
    use crate::types::{AttributeValue, Filter};

    use super::{
        build_late_candidate_index, decode_all_candidate_rows, decode_cluster, AttributeLocator,
        FdeCandidateIndex, FetchedLateCandidateCluster, LateCandidateBuildConfig,
        LateCandidateInputRow, LateRoutingMetric, ResidentLateCandidateIndex,
    };
    use crate::index::late_interaction::matrix_artifact::MatrixBlockLocator;

    fn config(candidate_k: usize, nlist: usize, probe_budget: usize) -> LateCandidateBuildConfig {
        LateCandidateBuildConfig {
            fde_dimension: 2,
            nlist,
            probe_budget,
            candidate_k,
            routing_metric: LateRoutingMetric::NegativeL2,
            kmeans_max_iters: 20,
            kmeans_epsilon: 1e-6,
            max_cluster_bytes: 1024 * 1024,
            max_bootstrap_bytes: 1024 * 1024,
        }
    }

    fn row(id: &str, fde: [f32; 2], color: &str) -> LateCandidateInputRow {
        LateCandidateInputRow {
            id: id.to_string(),
            fde: fde.to_vec(),
            content_hash: crate::embedding::ContentHash::new(
                ArtifactChecksum::digest(id.as_bytes())
                    .as_bytes()
                    .to_owned(),
            ),
            source_sequence: u64::from(id.as_bytes()[0]),
            parent_id: Some("parent".to_string()),
            unit_ordinal: Some(0),
            matrix_locator: MatrixBlockLocator {
                object_key: format!("target/late/segments/segment/{id}-matrix.bin"),
                byte_offset: 11,
                byte_length: 16,
                vector_count: 2,
                payload_checksum: ArtifactChecksum::digest(id.as_bytes()),
            },
            attr_locator: AttributeLocator {
                object_key: format!("target/late/segments/segment/{id}-attrs.bin"),
                byte_offset: 7,
                byte_length: 8,
                payload_checksum: ArtifactChecksum::digest(color.as_bytes()),
            },
            filter_attributes: Some(HashMap::from([(
                "color".to_string(),
                AttributeValue::String(color.to_string()),
            )])),
        }
    }

    #[test]
    fn candidate_artifacts_are_deterministic_and_round_trip() {
        let rows = vec![
            row("d", [10.0, 10.0], "blue"),
            row("b", [0.1, 0.2], "red"),
            row("a", [0.0, 0.0], "blue"),
            row("c", [9.9, 10.1], "red"),
        ];
        let generation = FdeGenerationId::new([9; 32]);
        let first = build_late_candidate_index(
            "target",
            "segment",
            generation,
            config(3, 2, 2),
            rows.clone(),
        )
        .expect("first candidate build");
        let second =
            build_late_candidate_index("target", "segment", generation, config(3, 2, 2), rows)
                .expect("second candidate build");

        assert_eq!(first.bootstrap_bytes, second.bootstrap_bytes);
        assert_eq!(first.index_ref, second.index_ref);
        assert_eq!(first.index_ref.clusters.len(), first.clusters.len());
        assert_eq!(first.clusters.len(), 2);
        assert!(first
            .clusters
            .iter()
            .zip(&second.clusters)
            .all(|(left, right)| left.reference == right.reference && left.bytes == right.bytes));

        let resident = ResidentLateCandidateIndex::from_index_ref(
            &first.bootstrap_bytes,
            &first.index_ref,
            1024 * 1024,
        )
        .expect("resident bootstrap");
        assert_eq!(&resident.reference, &first.index_ref.bootstrap);
        assert_eq!(resident.recipe().nlist, 2);
        assert_eq!(resident.route(&[0.0, 0.0]).expect("route").len(), 2);

        let payloads: Vec<FetchedLateCandidateCluster<'_>> = first
            .index_ref
            .clusters
            .iter()
            .map(|reference| {
                let cluster = first
                    .clusters
                    .iter()
                    .find(|cluster| cluster.reference == *reference)
                    .expect("rooted cluster bytes");
                FetchedLateCandidateCluster {
                    reference,
                    bytes: &cluster.bytes,
                }
            })
            .collect();
        let decoded = decode_all_candidate_rows(
            &first.index_ref,
            &first.bootstrap_bytes,
            &payloads,
            1024 * 1024,
            1024 * 1024,
        )
        .expect("full candidate decode");
        assert_eq!(decoded.len(), 4);
        assert_eq!(
            decoded
                .iter()
                .map(|row| row.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["a", "b", "c", "d"])
        );

        let persisted_frontier = resident
            .candidates_from_fetched(
                &[1.0, 1.0],
                &BTreeSet::new(),
                None,
                None,
                &payloads,
                1024 * 1024,
            )
            .expect("full-probe persisted frontier");
        assert_eq!(
            persisted_frontier
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "d", "b"]
        );
    }

    #[test]
    fn routing_uses_negative_l2_instead_of_centroid_dot_product() {
        let built = build_late_candidate_index(
            "target",
            "segment",
            FdeGenerationId::new([7; 32]),
            config(1, 2, 1),
            vec![
                row("near", [1.0, 0.0], "blue"),
                row("far", [100.0, 100.0], "blue"),
            ],
        )
        .expect("candidate build");
        let resident = ResidentLateCandidateIndex::from_index_ref(
            &built.bootstrap_bytes,
            &built.index_ref,
            1024 * 1024,
        )
        .expect("resident bootstrap");
        let routed = resident.route(&[1.0, 0.0]).expect("route");

        assert_eq!(routed.len(), 1);
        let cluster = built
            .clusters
            .iter()
            .find(|cluster| cluster.reference == routed[0])
            .expect("routed cluster bytes");
        let payload = FetchedLateCandidateCluster {
            reference: &routed[0],
            bytes: &cluster.bytes,
        };
        let candidates = resident
            .candidates_from_fetched(
                &[1.0, 0.0],
                &BTreeSet::new(),
                None,
                None,
                &[payload],
                1024 * 1024,
            )
            .expect("routed candidates");
        assert_eq!(candidates[0].id, "near");
    }

    #[test]
    fn exact_filters_run_before_candidate_truncation() {
        let built = build_late_candidate_index(
            "target",
            "segment",
            FdeGenerationId::new([5; 32]),
            config(1, 1, 1),
            vec![
                row("blocked-high", [10.0, 0.0], "red"),
                row("allowed-low", [1.0, 0.0], "blue"),
            ],
        )
        .expect("candidate build");
        let resident = ResidentLateCandidateIndex::from_index_ref(
            &built.bootstrap_bytes,
            &built.index_ref,
            1024 * 1024,
        )
        .expect("resident bootstrap");
        let routed = resident.route(&[1.0, 0.0]).expect("route");
        let payloads: Vec<FetchedLateCandidateCluster<'_>> = routed
            .iter()
            .map(|reference| {
                let cluster = built
                    .clusters
                    .iter()
                    .find(|cluster| cluster.reference == *reference)
                    .expect("routed cluster bytes");
                FetchedLateCandidateCluster {
                    reference,
                    bytes: &cluster.bytes,
                }
            })
            .collect();
        let filter = Filter::Eq {
            field: "color".to_string(),
            value: AttributeValue::String("blue".to_string()),
        };
        let mandatory_filter = Filter::NotEq {
            field: "color".to_string(),
            value: AttributeValue::String("red".to_string()),
        };
        let decoded_cluster = decode_cluster(
            &built.clusters[0].bytes,
            &built.clusters[0].reference,
            &built.index_ref.bootstrap.recipe,
            1024 * 1024,
        )
        .expect("cluster bitmap payload");
        assert_eq!(decoded_cluster.filter_bitmaps.vector_count, 2);
        let bitmap_matches =
            evaluate_filter_bitmap(&filter, &decoded_cluster.filter_bitmaps.to_index())
                .expect("color equality is bitmap-supported");
        assert_eq!(bitmap_matches.len(), 1);
        let candidates = resident
            .candidates_from_fetched(
                &[1.0, 0.0],
                &BTreeSet::new(),
                Some(&mandatory_filter),
                Some(&filter),
                &payloads,
                1024 * 1024,
            )
            .expect("filtered candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "allowed-low");
        assert_eq!(candidates[0].approx_fde_score, 1.0);
        assert_eq!(candidates[0].metadata.parent_id.as_deref(), Some("parent"));

        let excluded = BTreeSet::from(["blocked-high".to_string()]);
        let candidates = resident
            .candidates_from_fetched(&[1.0, 0.0], &excluded, None, None, &payloads, 1024 * 1024)
            .expect("excluded candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "allowed-low");
    }

    #[test]
    fn corrupt_cluster_checksum_fails_without_skipping() {
        let built = build_late_candidate_index(
            "target",
            "segment",
            FdeGenerationId::new([3; 32]),
            config(1, 1, 1),
            vec![row("one", [1.0, 0.0], "blue")],
        )
        .expect("candidate build");
        let resident = ResidentLateCandidateIndex::from_index_ref(
            &built.bootstrap_bytes,
            &built.index_ref,
            1024 * 1024,
        )
        .expect("resident bootstrap");
        let routed = resident.route(&[1.0, 0.0]).expect("route");
        let mut corrupt = built.clusters[0].bytes.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        let payload = FetchedLateCandidateCluster {
            reference: &routed[0],
            bytes: &corrupt,
        };

        let error = resident
            .candidates_from_fetched(
                &[1.0, 0.0],
                &BTreeSet::new(),
                None,
                None,
                &[payload],
                1024 * 1024,
            )
            .expect_err("corrupt cluster must fail");
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn logical_clusters_are_sharded_below_the_object_bound() {
        let mut bounded = config(20, 1, 1);
        bounded.max_cluster_bytes = 1_200;
        let rows = (0..20)
            .map(|index| row(&format!("row-{index:02}"), [index as f32, 1.0], "blue"))
            .collect();
        let built = build_late_candidate_index(
            "target",
            "segment",
            FdeGenerationId::new([1; 32]),
            bounded,
            rows,
        )
        .expect("sharded candidate build");

        assert!(built.clusters.len() > 1);
        assert!(built
            .clusters
            .iter()
            .all(|cluster| cluster.bytes.len() <= bounded.max_cluster_bytes));
        assert!(built
            .clusters
            .iter()
            .enumerate()
            .all(|(shard_id, cluster)| {
                cluster.reference.cluster_id == 0
                    && usize::try_from(cluster.reference.shard_id).ok() == Some(shard_id)
            }));

        let resident = ResidentLateCandidateIndex::from_index_ref(
            &built.bootstrap_bytes,
            &built.index_ref,
            1024 * 1024,
        )
        .expect("resident bootstrap");
        assert_eq!(
            resident.route(&[0.0, 1.0]).expect("route").len(),
            built.clusters.len()
        );
    }
}
