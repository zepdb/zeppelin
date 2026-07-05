//! Search phase for IVF-Flat index.
//!
//! 1. Compute distance from query to all centroids.
//! 2. Select top-`nprobe` closest centroids.
//! 3. For each selected cluster, fetch and scan all vectors.
//! 4. Apply post-filter with oversampling if a filter is present.
//! 5. Return sorted top-k results.

use dashmap::DashMap;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::debug;

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::build::{
    attrs_key, cluster_key, cluster_object_header_range_len, cluster_object_layout,
    cluster_object_sections, deserialize_attrs, deserialize_cluster_from_object,
    deserialize_colocated_sq_cluster_from_object, ClusterObjectLayout, ClusterObjectRange,
};
use super::sketch::{AdaptiveClusterBudget, ClusterScore};
use super::IvfFlatIndex;

use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
use crate::index::bitmap::{bitmap_key, ClusterBitmapIndex};

type ClusterAttrs = Vec<Option<HashMap<String, AttributeValue>>>;

// Previous smooth fixed budget: keeps the cap monotonic in nprobe and makes
// high-nprobe sentinel runs become a structural no-op when cap >= probe count.
const SKETCH_BASE_MIN_CLUSTERS: usize = 6;
const SKETCH_CLUSTER_LINEAR_FRACTION: f32 = 0.3125;
const SKETCH_CLUSTER_QUADRATIC_SCALE: f32 = 150.0;
// Always fetch a small sketch-ranked core so sharp/easy queries can spend less
// than the old fixed budget without risking single-cluster brittleness.
const SKETCH_ADAPTIVE_FLOOR_CLUSTERS: usize = 4;
// Hard queries can borrow up to roughly twice the previous fixed budget while
// the score-gap cutoff keeps the mean near the old operating point.
const SKETCH_ADAPTIVE_MAX_MULTIPLIER: usize = 2;
// Include extra clusters whose aggregate sketch distance is close to the best
// cluster for this query; the cutoff uses no cross-query state.
const SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN: f32 = 0.13;
// Grouped cluster objects cover multiple logical clusters per S3 GET. The
// object cap starts from the cluster cap, scales by actual manifest arity, and
// keeps a small hard-query allowance for flat score distributions.
const SKETCH_ADAPTIVE_FLOOR_OBJECTS: usize = 3;
const SKETCH_ADAPTIVE_THIN_OBJECT_FLOOR: usize = 5;
const SKETCH_THIN_OBJECT_MAX_ARITY: usize = 3;
const SKETCH_ADAPTIVE_THIN_RELATIVE_SCORE_MARGIN: f32 = 0.12;
const SKETCH_ADAPTIVE_OBJECT_CAP_EXTRA: usize = 2;
const SKETCH_SCAN_STATS_ENV: &str = "ZEPPELIN_SKETCH_SCAN_STATS";
const SQ_BYTE_STATS_ENV: &str = "ZEPPELIN_SQ_BYTE_STATS";

/// A candidate result during search, before final ranking.
struct Candidate {
    id: String,
    score: f32,
    attributes: Option<HashMap<String, AttributeValue>>,
    cluster_idx: usize,
    row_idx: usize,
}

#[derive(Debug, Clone)]
struct ClusterFetchObject {
    key: String,
    clusters: Vec<usize>,
    live_range: Option<std::ops::Range<usize>>,
}

static CLUSTER_OBJECT_LAYOUT_CACHE: OnceLock<DashMap<String, Arc<ClusterObjectLayout>>> =
    OnceLock::new();
static SQ_BYTE_STATS_QUERY_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum SqBytePhase {
    Sq,
    Rerank,
    Other,
}

struct SqSearchByteStats {
    query_id: u64,
    sq_gets: AtomicU64,
    sq_bytes: AtomicU64,
    sq_logical_bytes: AtomicU64,
    rerank_gets: AtomicU64,
    rerank_bytes: AtomicU64,
    rerank_logical_bytes: AtomicU64,
    other_gets: AtomicU64,
    other_bytes: AtomicU64,
    selected_clusters: AtomicU64,
    sq_objects: AtomicU64,
    coarse_candidates: AtomicU64,
    rerank_candidates: AtomicU64,
    rerank_clusters: AtomicU64,
    rerank_objects: AtomicU64,
    final_results: AtomicU64,
}

impl SqSearchByteStats {
    fn new_if_enabled(enabled: bool) -> Option<Arc<Self>> {
        if !enabled || std::env::var_os(SQ_BYTE_STATS_ENV).is_none() {
            return None;
        }
        Some(Arc::new(Self {
            query_id: SQ_BYTE_STATS_QUERY_ID.fetch_add(1, Ordering::Relaxed),
            sq_gets: AtomicU64::new(0),
            sq_bytes: AtomicU64::new(0),
            sq_logical_bytes: AtomicU64::new(0),
            rerank_gets: AtomicU64::new(0),
            rerank_bytes: AtomicU64::new(0),
            rerank_logical_bytes: AtomicU64::new(0),
            other_gets: AtomicU64::new(0),
            other_bytes: AtomicU64::new(0),
            selected_clusters: AtomicU64::new(0),
            sq_objects: AtomicU64::new(0),
            coarse_candidates: AtomicU64::new(0),
            rerank_candidates: AtomicU64::new(0),
            rerank_clusters: AtomicU64::new(0),
            rerank_objects: AtomicU64::new(0),
            final_results: AtomicU64::new(0),
        }))
    }

    fn record_get(&self, phase: SqBytePhase, bytes: usize) {
        self.record_gets(phase, 1, bytes);
    }

    fn record_gets(&self, phase: SqBytePhase, gets: usize, bytes: usize) {
        let gets = gets as u64;
        let bytes = bytes as u64;
        match phase {
            SqBytePhase::Sq => {
                self.sq_gets.fetch_add(gets, Ordering::Relaxed);
                self.sq_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            SqBytePhase::Rerank => {
                self.rerank_gets.fetch_add(gets, Ordering::Relaxed);
                self.rerank_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            SqBytePhase::Other => {
                self.other_gets.fetch_add(gets, Ordering::Relaxed);
                self.other_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    fn record_logical_sq_bytes(&self, bytes: usize) {
        self.sq_logical_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_logical_rerank_bytes(&self, bytes: usize) {
        self.rerank_logical_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn set_usize(field: &AtomicU64, value: usize) {
        field.store(value as u64, Ordering::Relaxed);
    }

    fn emit(&self, effective_nprobe: usize, fetch_k: usize) {
        let sq_gets = self.sq_gets.load(Ordering::Relaxed);
        let sq_bytes = self.sq_bytes.load(Ordering::Relaxed);
        let sq_logical_bytes = self.sq_logical_bytes.load(Ordering::Relaxed);
        let rerank_gets = self.rerank_gets.load(Ordering::Relaxed);
        let rerank_bytes = self.rerank_bytes.load(Ordering::Relaxed);
        let rerank_logical_bytes = self.rerank_logical_bytes.load(Ordering::Relaxed);
        let other_gets = self.other_gets.load(Ordering::Relaxed);
        let other_bytes = self.other_bytes.load(Ordering::Relaxed);
        let total_gets = sq_gets + rerank_gets + other_gets;
        let total_bytes = sq_bytes + rerank_bytes + other_bytes;
        eprintln!(
            "zeppelin_sq_byte_stats query={} nprobe={effective_nprobe} fetch_k={fetch_k} \
selected_clusters={} sq_objects={} coarse_candidates={} rerank_candidates={} rerank_clusters={} \
rerank_objects={} final_results={} sq_gets={sq_gets} sq_bytes={sq_bytes} \
sq_logical_bytes={sq_logical_bytes} sq_slack_bytes={} rerank_gets={rerank_gets} \
rerank_bytes={rerank_bytes} rerank_logical_bytes={rerank_logical_bytes} rerank_slack_bytes={} \
other_gets={other_gets} other_bytes={other_bytes} total_gets={total_gets} total_bytes={total_bytes}",
            self.query_id,
            self.selected_clusters.load(Ordering::Relaxed),
            self.sq_objects.load(Ordering::Relaxed),
            self.coarse_candidates.load(Ordering::Relaxed),
            self.rerank_candidates.load(Ordering::Relaxed),
            self.rerank_clusters.load(Ordering::Relaxed),
            self.rerank_objects.load(Ordering::Relaxed),
            self.final_results.load(Ordering::Relaxed),
            sq_bytes.saturating_sub(sq_logical_bytes),
            rerank_bytes.saturating_sub(rerank_logical_bytes),
        );
    }
}

fn cluster_object_layout_cache() -> &'static DashMap<String, Arc<ClusterObjectLayout>> {
    CLUSTER_OBJECT_LAYOUT_CACHE.get_or_init(DashMap::new)
}

/// Fetch data from cache if available, otherwise from S3.
async fn fetch_with_cache(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    key: &str,
) -> Result<bytes::Bytes> {
    if let Some(c) = cache {
        c.get_or_fetch(key, || store.get(key)).await
    } else {
        store.get(key).await
    }
}

async fn fetch_with_cache_counted(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    key: &str,
    stats: Option<&SqSearchByteStats>,
    phase: SqBytePhase,
) -> Result<bytes::Bytes> {
    if let Some(c) = cache {
        c.get_or_fetch(key, || async {
            let data = store.get(key).await?;
            if let Some(stats) = stats {
                stats.record_get(phase, data.len());
            }
            Ok(data)
        })
        .await
    } else {
        let data = store.get(key).await?;
        if let Some(stats) = stats {
            stats.record_get(phase, data.len());
        }
        Ok(data)
    }
}

fn cluster_fetch_object(index: &IvfFlatIndex, cluster_idx: usize) -> Result<ClusterFetchObject> {
    if let Some(object_ref) = index.cluster_object(cluster_idx)? {
        let live_range = object_ref.live_range()?;
        if live_range.as_ref().is_some_and(|range| range.start != 0) {
            return Err(ZeppelinError::Index(format!(
                "cluster object {} advertised nonzero live offset; flat-scan range must be self-contained",
                object_ref.key
            )));
        }
        return Ok(ClusterFetchObject {
            key: object_ref.key.clone(),
            clusters: object_ref.clusters.clone(),
            live_range,
        });
    }

    Ok(ClusterFetchObject {
        key: cluster_key(
            &index.namespace,
            index.cluster_owner(cluster_idx),
            cluster_idx,
        ),
        clusters: vec![cluster_idx],
        live_range: None,
    })
}

async fn fetch_cluster_object_for_flat_scan(
    cache: Option<&Arc<DiskCache>>,
    store: &ZeppelinStore,
    object: &ClusterFetchObject,
    use_live_range: bool,
) -> Result<bytes::Bytes> {
    if use_live_range {
        if let Some(range) = object.live_range.clone() {
            // A locally cached full object supersedes the ranged fetch; the
            // live span is a prefix, so slicing preserves parse semantics.
            if let Some(c) = cache {
                if let Some(data) = c.get(&object.key).await {
                    if data.len() >= range.end {
                        return Ok(data.slice(range));
                    }
                }
            }
            return store.get_range(&object.key, range).await;
        }
    }
    fetch_with_cache(cache, store, &object.key).await
}

fn cluster_fetch_objects(
    index: &IvfFlatIndex,
    clusters: &[usize],
) -> Result<Vec<ClusterFetchObject>> {
    let mut objects = Vec::new();
    let mut seen_keys = HashSet::new();
    for &cluster_idx in clusters {
        let object = cluster_fetch_object(index, cluster_idx)?;
        if seen_keys.insert(object.key.clone()) {
            objects.push(object);
        }
    }
    Ok(objects)
}

fn expand_clusters_to_objects(index: &IvfFlatIndex, clusters: &[usize]) -> Result<Vec<usize>> {
    let mut expanded = Vec::new();
    let mut seen_clusters = HashSet::new();
    for object in cluster_fetch_objects(index, clusters)? {
        for cluster_idx in object.clusters {
            if seen_clusters.insert(cluster_idx) {
                expanded.push(cluster_idx);
            }
        }
    }
    Ok(expanded)
}

fn emit_scan_stats(effective_nprobe: usize, objects: usize, clusters: usize, grouped: bool) {
    if std::env::var_os(SKETCH_SCAN_STATS_ENV).is_some() {
        eprintln!(
            "zeppelin_scan_stats nprobe={effective_nprobe} object_gets={objects} clusters_covered={clusters} grouped={grouped}"
        );
    }
}

/// Execute an IVF-Flat search against the stored index.
///
/// # Arguments
/// * `index`    - The in-memory index handle (centroids + metadata).
/// * `query`    - The query vector.
/// * `top_k`    - Number of results to return.
/// * `nprobe`   - Number of clusters to probe.
/// * `filter`   - Optional post-filter.
/// * `distance_metric` - Distance metric for ranking.
/// * `store`    - S3 store for reading cluster data.
/// * `oversample_factor` - Oversampling multiplier when filters are active.
/// * `cache`    - Optional disk cache for cluster data.
/// * `rerank_coalesce_gap_bytes` - Max gap between rerank vector ranges to coalesce.
#[allow(clippy::too_many_arguments)]
pub async fn search_ivf_flat(
    index: &IvfFlatIndex,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<SearchResult>> {
    // Validate query dimension.
    if query.len() != index.dim {
        return Err(ZeppelinError::DimensionMismatch {
            expected: index.dim,
            actual: query.len(),
        });
    }

    if top_k == 0 {
        return Ok(Vec::new());
    }

    let num_clusters = index.centroids.len();
    let effective_nprobe = nprobe.min(num_clusters);

    // --- Step 1: Rank centroids by distance to query ---
    let mut centroid_dists: Vec<(usize, f32)> = index
        .centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, compute_distance(query, c, distance_metric)))
        .collect();

    // Sort ascending (lower distance = closer).
    centroid_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let probe_clusters: Vec<usize> = centroid_dists
        .iter()
        .take(effective_nprobe)
        .map(|(idx, _)| *idx)
        .collect();
    // --- Step 2: Determine fetch size (oversample if filtering) ---
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };
    let sq_byte_stats =
        SqSearchByteStats::new_if_enabled(matches!(index.quantization, QuantizationType::Scalar));

    let scan_clusters = select_scan_clusters(
        index,
        query,
        distance_metric,
        filter,
        &probe_clusters,
        effective_nprobe,
        fetch_k,
    )?;

    debug!(
        nprobe = effective_nprobe,
        probe_clusters = ?probe_clusters,
        scan_clusters = ?scan_clusters,
        "probing clusters"
    );

    // --- Step 3: Scan selected clusters ---
    // The resident sketch is only allowed to decide which cluster objects to
    // fetch. Scalar-quantized indexes still score every vector in those
    // selected clusters with SQ8 before exact rerank.
    let candidates = match index.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                fetch_k,
                store,
                cache,
                sq_byte_stats.clone(),
                rerank_coalesce_gap_bytes,
            )
            .await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                fetch_k,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::None => {
            scan_clusters_flat(
                index,
                &scan_clusters,
                query,
                distance_metric,
                filter,
                store,
                cache,
                index.resident_sketch.is_some() && filter.is_none(),
            )
            .await?
        }
    };

    debug!(
        total_candidates = candidates.len(),
        fetch_k = fetch_k,
        "scanned clusters"
    );

    // --- Step 4: Sort all candidates by distance ---
    let mut sorted = candidates;
    sorted.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // --- Step 5: Apply post-filter if present ---
    let results: Vec<SearchResult> = if let Some(f) = filter {
        sorted
            .into_iter()
            .filter(|c| {
                match &c.attributes {
                    Some(attrs) => evaluate_filter(f, attrs),
                    None => false, // No attributes means filter cannot match.
                }
            })
            .take(top_k)
            .map(|c| SearchResult {
                id: c.id,
                score: c.score,
                attributes: c.attributes,
            })
            .collect()
    } else {
        let top_candidates: Vec<Candidate> = sorted.into_iter().take(top_k).collect();
        enrich_unfiltered_results(
            index,
            top_candidates,
            store,
            cache,
            sq_byte_stats.as_deref(),
        )
        .await?
    };

    debug!(returned = results.len(), top_k = top_k, "search complete");
    if let Some(stats) = &sq_byte_stats {
        SqSearchByteStats::set_usize(&stats.final_results, results.len());
        stats.emit(effective_nprobe, fetch_k);
    }

    Ok(results)
}

fn select_scan_clusters(
    index: &IvfFlatIndex,
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    probe_clusters: &[usize],
    effective_nprobe: usize,
    retrieval_top_k: usize,
) -> Result<Vec<usize>> {
    let Some(sketch) = &index.resident_sketch else {
        return expand_clusters_to_objects(index, probe_clusters);
    };

    // Attribute filters require exact per-row attrs during coarse pruning.
    // The current resident sketch intentionally stores no attribute values, so
    // filtered queries keep the legacy cluster set and preserve existing
    // semantics. Unfiltered benchmark/query traffic uses the sketch path.
    if filter.is_some() {
        return expand_clusters_to_objects(index, probe_clusters);
    }

    let cluster_budget = adaptive_sketch_budget(effective_nprobe);
    if cluster_budget.max_clusters() >= probe_clusters.len() {
        let clusters = expand_clusters_to_objects(index, probe_clusters)?;
        let objects = cluster_fetch_objects(index, probe_clusters)?.len();
        emit_scan_stats(
            effective_nprobe,
            objects,
            clusters.len(),
            !index.cluster_objects.is_empty(),
        );
        return Ok(clusters);
    }

    if !index.cluster_objects.is_empty() {
        let ranked_clusters =
            sketch.rank_clusters(query, distance_metric, probe_clusters, retrieval_top_k)?;
        let selected = select_grouped_object_clusters(index, &ranked_clusters, effective_nprobe)?;
        emit_scan_stats(
            effective_nprobe,
            selected.object_count,
            selected.clusters.len(),
            true,
        );
        return Ok(selected.clusters);
    }

    sketch.select_clusters(
        query,
        distance_metric,
        probe_clusters,
        cluster_budget,
        retrieval_top_k,
    )
}

struct SelectedObjectClusters {
    clusters: Vec<usize>,
    object_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveObjectBudget {
    floor_objects: usize,
    max_objects: usize,
    relative_score_margin: f32,
}

fn select_grouped_object_clusters(
    index: &IvfFlatIndex,
    ranked_clusters: &[ClusterScore],
    effective_nprobe: usize,
) -> Result<SelectedObjectClusters> {
    if ranked_clusters.is_empty() {
        return Err(ZeppelinError::Index(
            "grouped object selection received no ranked clusters".into(),
        ));
    }
    let budget = adaptive_object_budget(index, effective_nprobe);
    let mut object_candidates = Vec::new();
    let mut candidate_keys = HashSet::new();
    for score in ranked_clusters {
        let object = cluster_fetch_object(index, score.cluster_idx)?;
        if candidate_keys.insert(object.key.clone()) {
            object_candidates.push(object);
        }
    }

    let score_by_cluster: HashMap<usize, ClusterScore> = ranked_clusters
        .iter()
        .copied()
        .map(|score| (score.cluster_idx, score))
        .collect();
    let rank_by_cluster: HashMap<usize, usize> = ranked_clusters
        .iter()
        .enumerate()
        .map(|(rank, score)| (score.cluster_idx, rank))
        .collect();
    let mut selected_object_idxs = HashSet::new();
    let mut covered_clusters = HashSet::new();
    let mut clusters = Vec::new();
    let best_distance_score = ranked_clusters
        .iter()
        .map(|score| score.aggregate_score)
        .fold(f32::INFINITY, f32::min);
    let distance_cutoff =
        best_distance_score + best_distance_score.abs() * budget.relative_score_margin;

    while selected_object_idxs.len() < budget.max_objects {
        let ranking = if selected_object_idxs.len() <= budget.floor_objects {
            ObjectCandidateRanking::DistanceCore
        } else {
            ObjectCandidateRanking::MassExpansion
        };
        let Some(candidate) = best_object_candidate(
            &object_candidates,
            &score_by_cluster,
            &rank_by_cluster,
            &covered_clusters,
            &selected_object_idxs,
            ranking,
            distance_cutoff,
        ) else {
            break;
        };
        let within_floor = selected_object_idxs.len() < budget.floor_objects;
        if !within_floor && candidate.aggregate_score > distance_cutoff {
            break;
        }

        selected_object_idxs.insert(candidate.object_idx);
        for &cluster_idx in &object_candidates[candidate.object_idx].clusters {
            if covered_clusters.insert(cluster_idx) {
                clusters.push(cluster_idx);
            }
        }
    }

    if clusters.is_empty() {
        return Err(ZeppelinError::Index(
            "grouped object selection produced no clusters".into(),
        ));
    }

    Ok(SelectedObjectClusters {
        clusters,
        object_count: selected_object_idxs.len(),
    })
}

#[derive(Debug, Clone, Copy)]
struct ObjectCandidateScore {
    object_idx: usize,
    mass_count: usize,
    aggregate_score: f32,
    best_rank: usize,
}

#[derive(Debug, Clone, Copy)]
enum ObjectCandidateRanking {
    DistanceCore,
    MassExpansion,
}

fn best_object_candidate(
    object_candidates: &[ClusterFetchObject],
    score_by_cluster: &HashMap<usize, ClusterScore>,
    rank_by_cluster: &HashMap<usize, usize>,
    covered_clusters: &HashSet<usize>,
    selected_object_idxs: &HashSet<usize>,
    ranking: ObjectCandidateRanking,
    distance_cutoff: f32,
) -> Option<ObjectCandidateScore> {
    let mut best = None;
    for (object_idx, object) in object_candidates.iter().enumerate() {
        if selected_object_idxs.contains(&object_idx) {
            continue;
        }
        let candidate = score_object_candidate(
            object_idx,
            object,
            score_by_cluster,
            rank_by_cluster,
            covered_clusters,
        );
        if matches!(ranking, ObjectCandidateRanking::MassExpansion)
            && candidate.aggregate_score > distance_cutoff
        {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|best| object_candidate_better(&candidate, best, ranking))
        {
            best = Some(candidate);
        }
    }
    best
}

fn score_object_candidate(
    object_idx: usize,
    object: &ClusterFetchObject,
    score_by_cluster: &HashMap<usize, ClusterScore>,
    rank_by_cluster: &HashMap<usize, usize>,
    covered_clusters: &HashSet<usize>,
) -> ObjectCandidateScore {
    let mut mass_count = 0usize;
    let mut aggregate_score = f32::INFINITY;
    let mut best_rank = usize::MAX;

    for cluster_idx in &object.clusters {
        if covered_clusters.contains(cluster_idx) {
            continue;
        }
        let Some(score) = score_by_cluster.get(cluster_idx) else {
            continue;
        };
        mass_count += score.mass_count;
        if score.aggregate_score < aggregate_score {
            aggregate_score = score.aggregate_score;
        }
        best_rank = best_rank.min(
            rank_by_cluster
                .get(cluster_idx)
                .copied()
                .unwrap_or(usize::MAX),
        );
    }

    ObjectCandidateScore {
        object_idx,
        mass_count,
        aggregate_score,
        best_rank,
    }
}

fn object_candidate_better(
    candidate: &ObjectCandidateScore,
    best: &ObjectCandidateScore,
    ranking: ObjectCandidateRanking,
) -> bool {
    match ranking {
        ObjectCandidateRanking::DistanceCore => best
            .aggregate_score
            .partial_cmp(&candidate.aggregate_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| candidate.mass_count.cmp(&best.mass_count))
            .then_with(|| best.object_idx.cmp(&candidate.object_idx))
            .is_gt(),
        ObjectCandidateRanking::MassExpansion => candidate
            .mass_count
            .cmp(&best.mass_count)
            .then_with(|| {
                best.aggregate_score
                    .partial_cmp(&candidate.aggregate_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| best.best_rank.cmp(&candidate.best_rank))
            .is_gt(),
    }
}

fn adaptive_object_budget(index: &IvfFlatIndex, effective_nprobe: usize) -> AdaptiveObjectBudget {
    let max_arity = max_cluster_object_arity(index);
    let max_objects = grouped_object_cap_for_arity(max_arity, effective_nprobe);
    let is_thin = max_arity <= SKETCH_THIN_OBJECT_MAX_ARITY;
    let floor = if is_thin {
        SKETCH_ADAPTIVE_THIN_OBJECT_FLOOR
    } else {
        SKETCH_ADAPTIVE_FLOOR_OBJECTS
    };
    let relative_score_margin = if is_thin {
        SKETCH_ADAPTIVE_THIN_RELATIVE_SCORE_MARGIN
    } else {
        SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN
    };
    AdaptiveObjectBudget {
        floor_objects: floor.min(max_objects),
        max_objects,
        relative_score_margin,
    }
}

fn max_cluster_object_arity(index: &IvfFlatIndex) -> usize {
    index
        .cluster_objects
        .iter()
        .map(|object_ref| object_ref.clusters.len().max(1))
        .max()
        .unwrap_or(1)
}

fn grouped_object_cap_for_arity(max_arity: usize, effective_nprobe: usize) -> usize {
    let cluster_cap = sketch_adaptive_cluster_cap(effective_nprobe);
    if cluster_cap >= effective_nprobe {
        effective_nprobe
    } else {
        let legacy_pair_cap = cluster_cap.div_ceil(2).max(1);
        cluster_cap
            .div_ceil(max_arity)
            .saturating_add(SKETCH_ADAPTIVE_OBJECT_CAP_EXTRA)
            .min(legacy_pair_cap)
            .max(SKETCH_ADAPTIVE_FLOOR_OBJECTS.min(effective_nprobe))
    }
}

fn adaptive_sketch_budget(effective_nprobe: usize) -> AdaptiveClusterBudget {
    let max_clusters = sketch_adaptive_cluster_cap(effective_nprobe);
    AdaptiveClusterBudget::new(
        SKETCH_ADAPTIVE_FLOOR_CLUSTERS.min(max_clusters),
        max_clusters,
        SKETCH_ADAPTIVE_RELATIVE_SCORE_MARGIN,
    )
}

fn sketch_adaptive_cluster_cap(effective_nprobe: usize) -> usize {
    sketch_base_cluster_budget(effective_nprobe)
        .saturating_mul(SKETCH_ADAPTIVE_MAX_MULTIPLIER)
        .min(effective_nprobe)
}

fn sketch_base_cluster_budget(effective_nprobe: usize) -> usize {
    let nprobe = effective_nprobe as f32;
    ((nprobe * SKETCH_CLUSTER_LINEAR_FRACTION + (nprobe * nprobe / SKETCH_CLUSTER_QUADRATIC_SCALE))
        .ceil() as usize)
        .max(SKETCH_BASE_MIN_CLUSTERS)
        .min(effective_nprobe)
}

/// Scan clusters using full-precision vectors (no quantization).
async fn scan_clusters_flat(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    use_live_range: bool,
) -> Result<Vec<Candidate>> {
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // Phase 1: Parallel prefetch — all S3 I/O fires concurrently.
    let want_attrs = filter.is_some();
    let fetch_objects = cluster_fetch_objects(index, probe_clusters)?;
    let prefetched = futures::future::join_all(fetch_objects.iter().map(|object| {
        let object_key = object.key.clone();
        let object_clusters = object.clusters.clone();
        async move {
            let object_res =
                fetch_cluster_object_for_flat_scan(cache, store, object, use_live_range).await;
            let cluster_meta =
                futures::future::join_all(object_clusters.iter().map(|&cluster_idx| async move {
                    let owner = index.cluster_owner(cluster_idx);
                    let (prefilter, attrs) = tokio::join!(
                        try_bitmap_prefilter(
                            &index.namespace,
                            owner,
                            cluster_idx,
                            filter,
                            has_bitmaps,
                            store,
                            cache,
                        ),
                        async {
                            if want_attrs {
                                load_attrs(index, cluster_idx, filter, store, cache, None).await
                            } else {
                                Ok(None)
                            }
                        },
                    );
                    (cluster_idx, prefilter, attrs)
                }))
                .await;
            (object_key, object_clusters, object_res, cluster_meta)
        }
    }))
    .await;

    // Phase 2: Sequential compute — CPU-bound, no I/O.
    let mut candidates = Vec::new();
    for (object_key, object_clusters, object_res, cluster_meta) in prefetched {
        let object_data = object_res?;
        let grouped_sections = cluster_object_sections(&object_data)?;
        if grouped_sections.is_some() && index.cluster_objects.is_empty() {
            return Err(ZeppelinError::Index(format!(
                "legacy cluster key {object_key} contained grouped cluster data"
            )));
        }

        for (cluster_idx, prefilter, attrs) in cluster_meta {
            if !object_clusters.contains(&cluster_idx) {
                return Err(ZeppelinError::Index(format!(
                    "cluster metadata mismatch for object {object_key}: cluster {cluster_idx}"
                )));
            }
            let attrs = attrs?;
            let cluster = deserialize_cluster_from_object(&object_data, cluster_idx)?;

            for (j, vec) in cluster.vectors.iter().enumerate() {
                if let Some(ref bm) = prefilter {
                    if !bm.contains(j as u32) {
                        continue;
                    }
                }

                let score = compute_distance(query, vec, distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();

                candidates.push(Candidate {
                    id: cluster.ids[j].clone(),
                    score,
                    attributes: vector_attrs,
                    cluster_idx,
                    row_idx: j,
                });
            }
        }
    }

    Ok(candidates)
}

/// Scan clusters using SQ8 quantized distances, then rerank top candidates
/// with full-precision vectors.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_sq(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    byte_stats: Option<Arc<SqSearchByteStats>>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{sq_calibration_key, SqCalibration};

    // Load SQ calibration. New segments embed it in centroids; legacy segments
    // keep the old sidecar.
    let calibration = if let Some(calibration) = &index.sq_calibration {
        calibration.clone()
    } else {
        let cal_key = sq_calibration_key(&index.namespace, &index.segment_id);
        let cal_data = fetch_with_cache_counted(
            cache,
            store,
            &cal_key,
            byte_stats.as_deref(),
            SqBytePhase::Other,
        )
        .await?;
        SqCalibration::from_bytes(&cal_data)?
    };
    let prefer_colocated_clusters = index.sq_calibration.is_some();

    // Phase 1: Coarse ranking with quantized distances — parallel prefetch.
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // When a filter is present but NOT resolved by a bitmap index, we must
    // apply it DURING the coarse scan — before truncating to `rerank_count`.
    // Otherwise a selective filter's matches get truncated away by
    // approximate-distance ranking and the query silently under-fills top_k
    // (Task 6). That needs the per-cluster attrs in the coarse phase, so fetch
    // them alongside the SQ codes whenever a filter is active. Bitmap-resolved
    // clusters (prefilter Some) keep their fast path and ignore attrs.
    let want_attr_filter = filter.is_some();

    let fetch_objects = cluster_fetch_objects(index, probe_clusters)?;
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.selected_clusters, probe_clusters.len());
        SqSearchByteStats::set_usize(&stats.sq_objects, fetch_objects.len());
    }
    let sq_prefetched = futures::future::join_all(fetch_objects.iter().cloned().map(|object| {
        let stats = byte_stats.clone();
        async move {
            load_sq_object_for_coarse(
                index,
                object,
                prefer_colocated_clusters,
                store,
                cache,
                stats.as_deref(),
            )
            .await
        }
    }))
    .await;

    let meta_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let owner = index.cluster_owner(cluster_idx);
        let stats = byte_stats.clone();
        async move {
            let (prefilter, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    owner,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                async {
                    if want_attr_filter {
                        load_attrs(index, cluster_idx, filter, store, cache, stats.as_deref()).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, attrs)
        }
    }))
    .await;

    let mut sq_by_cluster = HashMap::new();
    let mut vector_ranges_by_cluster: HashMap<usize, Vec<Range<usize>>> = HashMap::new();
    let mut prefetched_objects: HashMap<String, bytes::Bytes> = HashMap::new();
    for object_res in sq_prefetched {
        let fetched = object_res?;
        if let Some(full_object) = fetched.full_object {
            prefetched_objects.insert(fetched.object_key.clone(), full_object);
        }
        for (cluster_idx, ranges) in fetched.vector_ranges {
            if vector_ranges_by_cluster
                .insert(cluster_idx, ranges)
                .is_some()
            {
                return Err(ZeppelinError::Index(format!(
                    "SQ coarse fetched duplicate vector ranges for cluster {cluster_idx}"
                )));
            }
        }
        for (cluster_idx, sq_cluster) in fetched.sq_clusters {
            if sq_by_cluster.insert(cluster_idx, sq_cluster).is_some() {
                return Err(ZeppelinError::Index(format!(
                    "SQ coarse fetched duplicate cluster {cluster_idx}"
                )));
            }
        }
    }

    let mut meta_by_cluster = HashMap::new();
    for (cluster_idx, prefilter, attrs) in meta_prefetched {
        let attrs = attrs?;
        if meta_by_cluster
            .insert(cluster_idx, (prefilter, attrs))
            .is_some()
        {
            return Err(ZeppelinError::Index(format!(
                "SQ coarse fetched duplicate metadata for cluster {cluster_idx}"
            )));
        }
    }

    let mut coarse_candidates: Vec<(String, f32, usize, usize)> = Vec::new();
    for &cluster_idx in probe_clusters {
        let sq_cluster = sq_by_cluster.remove(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "SQ coarse missing cluster data for cluster {cluster_idx}"
            ))
        })?;
        let (prefilter, attrs) = meta_by_cluster.remove(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "SQ coarse missing metadata for cluster {cluster_idx}"
            ))
        })?;

        for (j, codes) in sq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx_score = calibration.asymmetric_distance(query, codes, distance_metric);
            coarse_candidates.push((sq_cluster.ids[j].clone(), approx_score, cluster_idx, j));
        }
    }

    // Sort by approximate distance and take top candidates for reranking.
    coarse_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Rerank factor: take more candidates than needed for full-precision
    // reranking. Truncation now happens AFTER filtering, so the survivors are
    // real matches, not filtered-away noise (Task 6).
    let rerank_count = fetch_k * 4;
    coarse_candidates.truncate(rerank_count);
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.coarse_candidates, coarse_candidates.len());
        SqSearchByteStats::set_usize(&stats.rerank_candidates, coarse_candidates.len());
    }

    debug!(
        coarse_candidates = coarse_candidates.len(),
        rerank_count = rerank_count,
        "SQ8 coarse ranking complete, starting rerank"
    );

    // Phase 2: Rerank with full-precision vectors — parallel prefetch.
    let mut cluster_candidates: HashMap<usize, Vec<RerankNeed>> = HashMap::new();
    for (id, _, cluster_idx, row_idx) in &coarse_candidates {
        let vector_range = vector_ranges_by_cluster
            .get(cluster_idx)
            .and_then(|ranges| ranges.get(*row_idx))
            .cloned();
        cluster_candidates
            .entry(*cluster_idx)
            .or_default()
            .push(RerankNeed {
                id: id.clone(),
                row_idx: *row_idx,
                vector_range,
            });
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_cluster_ids: Vec<usize> = cluster_candidates.keys().copied().collect();
    let rerank_objects = cluster_fetch_objects(index, &rerank_cluster_ids)?;
    if let Some(stats) = &byte_stats {
        SqSearchByteStats::set_usize(&stats.rerank_clusters, rerank_cluster_ids.len());
        SqSearchByteStats::set_usize(&stats.rerank_objects, rerank_objects.len());
    }
    let attrs_future = futures::future::join_all(rerank_cluster_ids.iter().map(|&cluster_idx| {
        let stats = byte_stats.clone();
        async move {
            let attrs = if want_rerank_attrs {
                load_attrs(index, cluster_idx, filter, store, cache, stats.as_deref()).await
            } else {
                Ok(None)
            };
            (cluster_idx, attrs)
        }
    }));
    let clusters_future = futures::future::join_all(rerank_objects.iter().cloned().map(|object| {
        let stats = byte_stats.clone();
        let cluster_candidates = &cluster_candidates;
        let prefetched_objects = &prefetched_objects;
        async move {
            load_full_clusters_for_rerank(
                index,
                object,
                cluster_candidates,
                prefetched_objects,
                store,
                cache,
                stats.as_deref(),
                rerank_coalesce_gap_bytes,
            )
            .await
        }
    }));
    let (attrs_prefetched, rerank_prefetched) = tokio::join!(attrs_future, clusters_future);

    let mut attrs_by_cluster = HashMap::new();
    for (cluster_idx, attrs) in attrs_prefetched {
        attrs_by_cluster.insert(cluster_idx, attrs?);
    }

    let mut candidates = Vec::new();
    for object_res in rerank_prefetched {
        let vectors = object_res?;
        for fetched in vectors {
            let cluster_idx = fetched.cluster_idx;
            let attrs = attrs_by_cluster.get(&cluster_idx).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing rerank attrs entry for cluster {cluster_idx}"
                ))
            })?;
            let score = compute_distance(query, &fetched.vector, distance_metric);
            let vector_attrs = attrs
                .as_ref()
                .and_then(|a| a.get(fetched.row_idx))
                .cloned()
                .flatten();
            candidates.push(Candidate {
                id: fetched.id,
                score,
                attributes: vector_attrs,
                cluster_idx: fetched.cluster_idx,
                row_idx: fetched.row_idx,
            });
        }
    }

    Ok(candidates)
}

struct CoarseObjectSqFetch {
    object_key: String,
    sq_clusters: Vec<(usize, crate::index::quantization::sq::SqClusterData)>,
    vector_ranges: Vec<(usize, Vec<Range<usize>>)>,
    full_object: Option<bytes::Bytes>,
}

#[derive(Clone)]
struct RerankNeed {
    id: String,
    row_idx: usize,
    vector_range: Option<Range<usize>>,
}

struct RerankRangeRequest {
    cluster_idx: usize,
    need: RerankNeed,
    range: Range<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct CoalescedRerankRange {
    range: Range<usize>,
    request_indices: Vec<usize>,
}

struct RerankFetchedVector {
    cluster_idx: usize,
    id: String,
    row_idx: usize,
    vector: Vec<f32>,
}

/// Fetch SQ coarse data for every selected cluster in one physical object.
///
/// v4 grouped objects expose a contiguous SQ block, so the hot path reads one
/// range per selected object and never downloads full vectors during coarse
/// scoring. Legacy grouped/per-cluster objects keep the old full-object or
/// sidecar behavior so existing immutable data remains readable.
async fn load_sq_object_for_coarse(
    index: &IvfFlatIndex,
    object: ClusterFetchObject,
    prefer_colocated: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<CoarseObjectSqFetch> {
    use crate::index::quantization::sq::{deserialize_sq_cluster, sq_cluster_key};

    if !prefer_colocated {
        let mut sq_clusters = Vec::with_capacity(object.clusters.len());
        for &cluster_idx in &object.clusters {
            let sq_key = sq_cluster_key(
                &index.namespace,
                index.cluster_owner(cluster_idx),
                cluster_idx,
            );
            let sq_data =
                fetch_with_cache_counted(cache, store, &sq_key, stats, SqBytePhase::Sq).await?;
            if let Some(stats) = stats {
                stats.record_logical_sq_bytes(sq_data.len());
            }
            sq_clusters.push((cluster_idx, deserialize_sq_cluster(&sq_data)?));
        }
        return Ok(CoarseObjectSqFetch {
            object_key: object.key,
            sq_clusters,
            vector_ranges: Vec::new(),
            full_object: None,
        });
    }

    if let Some(layout) = load_cluster_object_layout(index, &object, store, cache, stats).await? {
        if layout.sections.iter().all(|section| section.sq.is_some()) {
            let sq_bytes =
                fetch_object_sq_range(&object.key, &layout, &object.clusters, store, stats).await?;
            let mut sq_clusters = Vec::with_capacity(object.clusters.len());
            let mut vector_ranges = Vec::with_capacity(object.clusters.len());
            for &cluster_idx in &object.clusters {
                let section = layout.section(cluster_idx).ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "cluster {cluster_idx} missing from layout for {}",
                        object.key
                    ))
                })?;
                let sq_range = section.sq.as_ref().ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "cluster {cluster_idx} missing SQ range in {}",
                        object.key
                    ))
                })?;
                let sq_slice = slice_relative_range(
                    &sq_bytes.bytes,
                    &sq_range.clone(),
                    sq_bytes.base_offset,
                    "SQ range",
                    &object.key,
                )?;
                let sq_cluster = deserialize_sq_cluster(sq_slice)?;
                let ranges = full_vector_ranges_from_sq_ids(section, &sq_cluster.ids, index.dim)?;
                sq_clusters.push((cluster_idx, sq_cluster));
                vector_ranges.push((cluster_idx, ranges));
            }
            return Ok(CoarseObjectSqFetch {
                object_key: object.key,
                sq_clusters,
                vector_ranges,
                full_object: None,
            });
        }
    }

    let object_data =
        fetch_with_cache_counted(cache, store, &object.key, stats, SqBytePhase::Sq).await?;
    let mut sq_clusters = Vec::with_capacity(object.clusters.len());
    for &cluster_idx in &object.clusters {
        if let Some(sq_cluster) =
            deserialize_colocated_sq_cluster_from_object(&object_data, cluster_idx)?
        {
            if let Some(stats) = stats {
                let sq_bytes: usize = sq_cluster.codes.iter().map(Vec::len).sum::<usize>()
                    + sq_cluster.ids.iter().map(|id| 4 + id.len()).sum::<usize>()
                    + 8;
                stats.record_logical_sq_bytes(sq_bytes);
            }
            sq_clusters.push((cluster_idx, sq_cluster));
            continue;
        }

        let sq_key = sq_cluster_key(
            &index.namespace,
            index.cluster_owner(cluster_idx),
            cluster_idx,
        );
        let sq_data =
            fetch_with_cache_counted(cache, store, &sq_key, stats, SqBytePhase::Sq).await?;
        if let Some(stats) = stats {
            stats.record_logical_sq_bytes(sq_data.len());
        }
        sq_clusters.push((cluster_idx, deserialize_sq_cluster(&sq_data)?));
    }

    Ok(CoarseObjectSqFetch {
        object_key: object.key,
        sq_clusters,
        vector_ranges: Vec::new(),
        full_object: Some(object_data),
    })
}

struct RangeBytes {
    base_offset: usize,
    bytes: bytes::Bytes,
}

async fn fetch_object_sq_range(
    object_key: &str,
    layout: &ClusterObjectLayout,
    clusters: &[usize],
    store: &ZeppelinStore,
    stats: Option<&SqSearchByteStats>,
) -> Result<RangeBytes> {
    let mut start = usize::MAX;
    let mut end = 0usize;
    let mut logical_bytes = 0usize;
    for &cluster_idx in clusters {
        let section = layout.section(cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing from layout for {object_key}"
            ))
        })?;
        let sq = section.sq.as_ref().ok_or_else(|| {
            ZeppelinError::Index(format!(
                "cluster {cluster_idx} missing SQ range in {object_key}"
            ))
        })?;
        logical_bytes = logical_bytes
            .checked_add(sq.end - sq.start)
            .ok_or_else(|| ZeppelinError::Index("SQ logical byte count overflows".into()))?;
        start = start.min(sq.start);
        end = end.max(sq.end);
    }
    if start == usize::MAX || start >= end {
        return Err(ZeppelinError::Index(format!(
            "empty SQ range for object {object_key}"
        )));
    }
    let bytes = store.get_range(object_key, start..end).await?;
    if let Some(stats) = stats {
        stats.record_get(SqBytePhase::Sq, bytes.len());
        stats.record_logical_sq_bytes(logical_bytes);
    }
    Ok(RangeBytes {
        base_offset: start,
        bytes,
    })
}

fn full_vector_ranges_from_sq_ids(
    section: &ClusterObjectRange,
    ids: &[String],
    dim: usize,
) -> Result<Vec<Range<usize>>> {
    let vector_bytes = dim
        .checked_mul(4)
        .ok_or_else(|| ZeppelinError::Index("full-vector byte width overflows".into()))?;
    let mut offset = section
        .full
        .start
        .checked_add(8)
        .ok_or_else(|| ZeppelinError::Index("full-vector section header overflows".into()))?;
    let mut ranges = Vec::with_capacity(ids.len());
    for id in ids {
        let id_len = id.as_bytes().len();
        let vector_start = offset
            .checked_add(4)
            .and_then(|offset| offset.checked_add(id_len))
            .ok_or_else(|| ZeppelinError::Index("full-vector row offset overflows".into()))?;
        let vector_end = vector_start
            .checked_add(vector_bytes)
            .ok_or_else(|| ZeppelinError::Index("full-vector row range overflows".into()))?;
        ranges.push(vector_start..vector_end);
        offset = vector_end;
    }
    if offset != section.full.end {
        return Err(ZeppelinError::Index(format!(
            "derived full-vector ranges do not cover cluster {}: derived_end={}, section_end={}",
            section.cluster_idx, offset, section.full.end
        )));
    }
    Ok(ranges)
}

async fn load_full_clusters_for_rerank(
    index: &IvfFlatIndex,
    object: ClusterFetchObject,
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
    prefetched_objects: &HashMap<String, bytes::Bytes>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<RerankFetchedVector>> {
    let needed_clusters: Vec<usize> = object
        .clusters
        .iter()
        .copied()
        .filter(|cluster_idx| cluster_candidates.contains_key(cluster_idx))
        .collect();
    if needed_clusters.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(object_data) = prefetched_objects.get(&object.key) {
        return rerank_vectors_from_object(
            &object.key,
            object_data,
            &needed_clusters,
            cluster_candidates,
        );
    }

    if load_cluster_object_layout(index, &object, store, cache, stats)
        .await?
        .is_some()
    {
        if all_needed_vectors_have_ranges(&needed_clusters, cluster_candidates) {
            return fetch_rerank_vectors_by_range(
                &object.key,
                &needed_clusters,
                cluster_candidates,
                index.dim,
                store,
                stats,
                rerank_coalesce_gap_bytes,
            )
            .await;
        }
    }

    let object_data =
        fetch_with_cache_counted(cache, store, &object.key, stats, SqBytePhase::Rerank).await?;
    rerank_vectors_from_object(
        &object.key,
        &object_data,
        &needed_clusters,
        cluster_candidates,
    )
}

fn all_needed_vectors_have_ranges(
    needed_clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
) -> bool {
    needed_clusters.iter().all(|cluster_idx| {
        cluster_candidates
            .get(cluster_idx)
            .map(|needs| needs.iter().all(|need| need.vector_range.is_some()))
            .unwrap_or(false)
    })
}

async fn fetch_rerank_vectors_by_range(
    object_key: &str,
    clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
    dim: usize,
    store: &ZeppelinStore,
    stats: Option<&SqSearchByteStats>,
    rerank_coalesce_gap_bytes: usize,
) -> Result<Vec<RerankFetchedVector>> {
    let mut requested = Vec::new();
    for &cluster_idx in clusters {
        let needs = cluster_candidates.get(&cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "missing rerank candidates for cluster {cluster_idx}"
            ))
        })?;
        for need in needs {
            let range = need.vector_range.clone().ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing vector range for cluster {cluster_idx} row {}",
                    need.row_idx
                ))
            })?;
            requested.push(RerankRangeRequest {
                cluster_idx,
                need: need.clone(),
                range,
            });
        }
    }
    let logical_bytes = requested.iter().try_fold(0usize, |acc, request| {
        let len = request.range.end - request.range.start;
        acc.checked_add(len)
            .ok_or_else(|| ZeppelinError::Index("rerank logical byte count overflows".into()))
    })?;
    let coalesced = coalesce_rerank_ranges(&requested, rerank_coalesce_gap_bytes)?;
    let ranges: Vec<Range<usize>> = coalesced
        .iter()
        .map(|coalesced| coalesced.range.clone())
        .collect();
    debug!(
        object_key,
        logical_ranges = requested.len(),
        physical_ranges = ranges.len(),
        gap_bytes = rerank_coalesce_gap_bytes,
        "coalesced rerank vector ranges"
    );
    let vector_bytes = futures::future::join_all(
        ranges
            .iter()
            .cloned()
            .map(|range| store.get_range(object_key, range)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    if let Some(stats) = stats {
        let physical_bytes =
            vector_bytes
                .iter()
                .map(bytes::Bytes::len)
                .try_fold(0usize, |acc, len| {
                    acc.checked_add(len).ok_or_else(|| {
                        ZeppelinError::Index("rerank physical byte count overflows".into())
                    })
                })?;
        stats.record_gets(SqBytePhase::Rerank, ranges.len(), physical_bytes);
        stats.record_logical_rerank_bytes(logical_bytes);
    }
    if vector_bytes.len() != coalesced.len() {
        return Err(ZeppelinError::Index(format!(
            "range fetch count mismatch for {object_key}: requested={}, got={}",
            coalesced.len(),
            vector_bytes.len()
        )));
    }

    let mut fetched: Vec<Option<RerankFetchedVector>> = std::iter::repeat_with(|| None)
        .take(requested.len())
        .collect();
    for (coalesced_range, bytes) in coalesced.iter().zip(vector_bytes) {
        for &request_idx in &coalesced_range.request_indices {
            let request = requested.get(request_idx).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coalesced range for {object_key} references missing request {request_idx}"
                ))
            })?;
            let vector_bytes = slice_relative_range(
                &bytes,
                &request.range,
                coalesced_range.range.start,
                "rerank vector range",
                object_key,
            )?;
            let vector = parse_f32_vector(vector_bytes, dim)?;
            fetched[request_idx] = Some(RerankFetchedVector {
                cluster_idx: request.cluster_idx,
                id: request.need.id.clone(),
                row_idx: request.need.row_idx,
                vector,
            });
        }
    }

    fetched
        .into_iter()
        .enumerate()
        .map(|(idx, fetched)| {
            fetched.ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coalesced rerank fetch for {object_key} missing request {idx}"
                ))
            })
        })
        .collect()
}

fn coalesce_rerank_ranges(
    requests: &[RerankRangeRequest],
    max_gap_bytes: usize,
) -> Result<Vec<CoalescedRerankRange>> {
    let mut sorted = Vec::with_capacity(requests.len());
    for (idx, request) in requests.iter().enumerate() {
        if request.range.start >= request.range.end {
            return Err(ZeppelinError::Index(format!(
                "invalid rerank vector range: request={idx}, start={}, end={}",
                request.range.start, request.range.end
            )));
        }
        sorted.push((idx, request.range.clone()));
    }
    sorted.sort_by(|a, b| {
        a.1.start
            .cmp(&b.1.start)
            .then_with(|| a.1.end.cmp(&b.1.end))
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut coalesced: Vec<CoalescedRerankRange> = Vec::new();
    for (request_idx, range) in sorted {
        let Some(last) = coalesced.last_mut() else {
            coalesced.push(CoalescedRerankRange {
                range,
                request_indices: vec![request_idx],
            });
            continue;
        };

        let overlaps = range.start < last.range.end;
        let mergeable_gap = if overlaps {
            true
        } else {
            range.start - last.range.end < max_gap_bytes
        };
        if mergeable_gap {
            last.range.end = last.range.end.max(range.end);
            last.request_indices.push(request_idx);
        } else {
            coalesced.push(CoalescedRerankRange {
                range,
                request_indices: vec![request_idx],
            });
        }
    }

    Ok(coalesced)
}

fn rerank_vectors_from_object(
    object_key: &str,
    object_data: &[u8],
    needed_clusters: &[usize],
    cluster_candidates: &HashMap<usize, Vec<RerankNeed>>,
) -> Result<Vec<RerankFetchedVector>> {
    let mut vectors = Vec::new();
    for &cluster_idx in needed_clusters {
        let needs = cluster_candidates
            .get(&cluster_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing rerank candidate IDs for cluster {cluster_idx}"
                ))
            })?
            .clone();
        let cluster = deserialize_cluster_from_object(object_data, cluster_idx).map_err(|e| {
            ZeppelinError::Index(format!(
                "failed to deserialize cluster {cluster_idx} from {object_key}: {e}"
            ))
        })?;
        let row_by_id: HashMap<&str, usize> = cluster
            .ids
            .iter()
            .enumerate()
            .map(|(row_idx, id)| (id.as_str(), row_idx))
            .collect();
        for need in needs {
            let row_idx = row_by_id.get(need.id.as_str()).copied().ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "candidate {} missing from cluster {cluster_idx} in {object_key}",
                    need.id
                ))
            })?;
            vectors.push(RerankFetchedVector {
                cluster_idx,
                id: need.id,
                row_idx,
                vector: cluster.vectors[row_idx].clone(),
            });
        }
    }
    Ok(vectors)
}

fn parse_f32_vector(data: &[u8], dim: usize) -> Result<Vec<f32>> {
    let expected_len = dim
        .checked_mul(4)
        .ok_or_else(|| ZeppelinError::Index("vector byte length overflows".into()))?;
    if data.len() != expected_len {
        return Err(ZeppelinError::Index(format!(
            "vector range length mismatch: expected {expected_len}, got {}",
            data.len()
        )));
    }
    Ok(data
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

async fn load_cluster_object_layout(
    _index: &IvfFlatIndex,
    object: &ClusterFetchObject,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Option<Arc<ClusterObjectLayout>>> {
    if !cluster_object_key_may_have_layout(&object.key, object.clusters.len()) {
        return Ok(None);
    }

    if let Some(c) = cache {
        if let Some(layout) = c.get_decoded::<ClusterObjectLayout>(&object.key)? {
            return Ok(Some(layout));
        }
    }
    if let Some(layout) = cluster_object_layout_cache()
        .get(&object.key)
        .map(|entry| Arc::clone(entry.value()))
    {
        if let Some(c) = cache {
            c.insert_decoded(&object.key, Arc::clone(&layout));
        }
        return Ok(Some(layout));
    }

    let header_len = cluster_object_header_range_len(object.clusters.len())?;
    let header = store.get_range(&object.key, 0..header_len).await?;
    if let Some(stats) = stats {
        stats.record_get(SqBytePhase::Other, header.len());
    }
    let Some(layout) = cluster_object_layout(&header)? else {
        return Ok(None);
    };
    validate_layout_matches_manifest(&object.key, &layout, &object.clusters)?;

    let layout = Arc::new(layout);
    cluster_object_layout_cache().insert(object.key.clone(), Arc::clone(&layout));
    if let Some(c) = cache {
        c.insert_decoded(&object.key, Arc::clone(&layout));
    }
    Ok(Some(layout))
}

fn cluster_object_key_may_have_layout(key: &str, cluster_count: usize) -> bool {
    cluster_count > 1
        || key
            .rsplit('/')
            .next()
            .map(|filename| {
                filename.starts_with("cluster_group_") || filename.starts_with("cluster_pair_")
            })
            .unwrap_or(false)
}

fn validate_layout_matches_manifest(
    object_key: &str,
    layout: &ClusterObjectLayout,
    manifest_clusters: &[usize],
) -> Result<()> {
    let layout_clusters: BTreeSet<usize> = layout
        .sections
        .iter()
        .map(|section| section.cluster_idx)
        .collect();
    let manifest_clusters: BTreeSet<usize> = manifest_clusters.iter().copied().collect();
    if layout_clusters != manifest_clusters {
        return Err(ZeppelinError::Index(format!(
            "cluster object {object_key} layout mismatch: manifest={manifest_clusters:?}, header={layout_clusters:?}"
        )));
    }
    Ok(())
}

fn slice_relative_range<'a>(
    bytes: &'a [u8],
    absolute: &Range<usize>,
    base_offset: usize,
    label: &str,
    object_key: &str,
) -> Result<&'a [u8]> {
    if absolute.start < base_offset || absolute.end < absolute.start {
        return Err(ZeppelinError::Index(format!(
            "{label} for {object_key} is outside fetched base: start={}, end={}, base={base_offset}",
            absolute.start, absolute.end
        )));
    }
    let start = absolute.start - base_offset;
    let end = absolute.end - base_offset;
    if end > bytes.len() {
        return Err(ZeppelinError::Index(format!(
            "{label} for {object_key} exceeds fetched bytes: relative_end={end}, len={}",
            bytes.len()
        )));
    }
    Ok(&bytes[start..end])
}

/// Scan clusters using PQ-encoded distances with ADC lookup tables,
/// then rerank top candidates with full-precision vectors.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_pq(
    index: &IvfFlatIndex,
    probe_clusters: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::pq::{
        deserialize_pq_cluster, pq_cluster_key, pq_codebook_key, PqCodebook,
    };

    // Load PQ codebook.
    let cb_key = pq_codebook_key(&index.namespace, &index.segment_id);
    let cb_data = fetch_with_cache(cache, store, &cb_key).await?;
    let codebook = PqCodebook::from_bytes(&cb_data)?;

    // Precompute ADC lookup table.
    let adc_table = codebook.build_adc_table(query, distance_metric);

    // Phase 1: Coarse ranking with PQ distances — parallel prefetch.
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // Apply a non-bitmap attribute filter DURING the coarse scan so a selective
    // filter's matches survive truncation (Task 6). Fetch attrs alongside the
    // PQ codes whenever a filter is active.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let owner = index.cluster_owner(cluster_idx);
        let pq_key = pq_cluster_key(&index.namespace, owner, cluster_idx);
        async move {
            let (prefilter, pq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    owner,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &pq_key),
                async {
                    if want_attr_filter {
                        load_attrs(index, cluster_idx, filter, store, cache, None).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, pq_res, attrs)
        }
    }))
    .await;

    let mut coarse_candidates: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, pq_res, attrs) in coarse_prefetched {
        let pq_data = pq_res?;
        let attrs = attrs?;
        let pq_cluster = deserialize_pq_cluster(&pq_data)?;

        for (j, codes) in pq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx_score = codebook.adc_distance(&adc_table, codes);
            coarse_candidates.push((pq_cluster.ids[j].clone(), approx_score, cluster_idx));
        }
    }

    // Sort and take top candidates for reranking.
    coarse_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let rerank_count = fetch_k * 4;
    coarse_candidates.truncate(rerank_count);

    debug!(
        coarse_candidates = coarse_candidates.len(),
        "PQ coarse ranking complete, starting rerank"
    );

    // Phase 2: Rerank with full-precision vectors — parallel prefetch.
    let mut cluster_candidates: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cluster_idx) in &coarse_candidates {
        cluster_candidates
            .entry(*cluster_idx)
            .or_default()
            .push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(cluster_candidates.iter().map(|(&cluster_idx, needed_ids)| {
            let cluster_object = cluster_fetch_object(index, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let cluster_fetch = async {
                    let object = cluster_object?;
                    fetch_with_cache(cache, store, &object.key).await
                };
                let (cluster_res, attrs) = tokio::join!(cluster_fetch, async {
                    if want_rerank_attrs {
                        load_attrs(index, cluster_idx, filter, store, cache, None).await
                    } else {
                        Ok(None)
                    }
                },);
                (cluster_idx, needed_ids, cluster_res, attrs)
            }
        }))
        .await;

    let mut candidates = Vec::new();
    for (cluster_idx, needed_ids, cluster_res, attrs) in rerank_prefetched {
        let cluster_data = cluster_res?;
        let attrs = attrs?;
        let cluster = deserialize_cluster_from_object(&cluster_data, cluster_idx)?;

        let needed_set: HashSet<&str> = needed_ids.iter().map(|s| s.as_str()).collect();

        for (j, id) in cluster.ids.iter().enumerate() {
            if needed_set.contains(id.as_str()) {
                let score = compute_distance(query, &cluster.vectors[j], distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();
                candidates.push(Candidate {
                    id: id.clone(),
                    score,
                    attributes: vector_attrs,
                    cluster_idx,
                    row_idx: j,
                });
            }
        }
    }

    Ok(candidates)
}

/// Try to load a cluster's bitmap index and evaluate the filter against it.
/// Returns `Some(bitmap)` if pre-filtering succeeded (positions to include),
/// or `None` if bitmaps are unavailable or the filter can't be resolved.
async fn try_bitmap_prefilter(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    filter: Option<&Filter>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Option<roaring::RoaringBitmap> {
    let filter = filter?;
    if !has_bitmaps {
        return None;
    }

    let bkey = bitmap_key(namespace, segment_id, cluster_idx);
    let data = match fetch_with_cache(cache, store, &bkey).await {
        Ok(d) => d,
        Err(_) => return None,
    };
    let bitmap_index = match ClusterBitmapIndex::from_bytes(&data) {
        Ok(idx) => idx,
        Err(e) => {
            tracing::debug!(cluster = cluster_idx, error = %e, "failed to load bitmap index");
            return None;
        }
    };

    evaluate_filter_bitmap(filter, &bitmap_index)
}

/// Whether row `j` of a cluster survives filtering during the COARSE quantized
/// scan (Task 6: filter before truncation, not after).
///
/// - A bitmap prefilter, when present, is authoritative (fast path, I2).
/// - Otherwise a non-bitmap filter is evaluated against the row's attributes;
///   a filtered row with no attributes cannot match.
/// - No filter ⇒ always passes (unfiltered queries keep the old behavior, I3).
///
/// Shared with the hierarchical leaf scan, which has the same two-phase
/// quantized structure and the same truncation hazard.
pub(crate) fn coarse_row_passes(
    filter: Option<&Filter>,
    prefilter: &Option<roaring::RoaringBitmap>,
    attrs: &Option<Vec<Option<HashMap<String, AttributeValue>>>>,
    j: usize,
) -> bool {
    if let Some(bm) = prefilter {
        return bm.contains(j as u32);
    }
    match filter {
        None => true,
        Some(f) => match attrs
            .as_ref()
            .and_then(|a| a.get(j))
            .and_then(|a| a.as_ref())
        {
            Some(a) => evaluate_filter(f, a),
            None => false,
        },
    }
}

/// Enrich final unfiltered results by fetching attrs for only their clusters.
async fn enrich_unfiltered_results(
    index: &IvfFlatIndex,
    candidates: Vec<Candidate>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Vec<SearchResult>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    let mut cluster_indices = Vec::new();
    for candidate in &candidates {
        if seen.insert(candidate.cluster_idx) {
            cluster_indices.push(candidate.cluster_idx);
        }
    }

    let attrs_fetches =
        futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| async move {
            (
                cluster_idx,
                load_attrs(index, cluster_idx, None, store, cache, stats).await,
            )
        }))
        .await;

    let mut attrs_by_cluster: HashMap<usize, Option<ClusterAttrs>> = HashMap::new();
    for (cluster_idx, attrs) in attrs_fetches {
        attrs_by_cluster.insert(cluster_idx, attrs?);
    }

    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let cluster_attrs = attrs_by_cluster
            .get(&candidate.cluster_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "missing attrs for final result cluster {}",
                    candidate.cluster_idx
                ))
            })?;
        let attributes = match cluster_attrs {
            Some(cluster_attrs) => cluster_attrs
                .get(candidate.row_idx)
                .ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "attrs row {} missing in cluster {}",
                        candidate.row_idx, candidate.cluster_idx
                    ))
                })?
                .clone(),
            None => None,
        };
        results.push(SearchResult {
            id: candidate.id,
            score: candidate.score,
            attributes,
        });
    }

    Ok(results)
}

/// Load attribute data for a cluster. Shared helper for all scan methods.
async fn load_attrs(
    index: &IvfFlatIndex,
    cluster_idx: usize,
    _filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
    stats: Option<&SqSearchByteStats>,
) -> Result<Option<ClusterAttrs>> {
    if !index.cluster_may_have_attrs(cluster_idx) {
        return Ok(None);
    }
    let akey = attrs_key(
        &index.namespace,
        index.cluster_owner(cluster_idx),
        cluster_idx,
    );
    let data = fetch_with_cache_counted(cache, store, &akey, stats, SqBytePhase::Other).await?;
    Ok(Some(deserialize_attrs(&data)?))
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> IvfFlatIndex {
        IvfFlatIndex {
            centroids: std::sync::Arc::new(vec![vec![0.0, 0.0], vec![10.0, 10.0]]),
            num_vectors: 4,
            dim: 2,
            namespace: "test_ns".to_string(),
            segment_id: "seg_001".to_string(),
            quantization: QuantizationType::None,
            sq_calibration: None,
            bitmap_fields: Vec::new(),
            cluster_owners: Vec::new(),
            cluster_objects: Vec::new(),
            cluster_object_by_cluster: Vec::new(),
            resident_sketch: None,
            sketch_ref: None,
            bootstrap_ref: None,
        }
    }

    #[test]
    fn test_dimension_mismatch() {
        let index = make_index();
        let query = vec![1.0, 2.0, 3.0]; // dim=3 vs index dim=2

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = rt.block_on(async {
            let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
            ZeppelinStore::new(mem)
        });

        let result = rt.block_on(search_ivf_flat(
            &index,
            &query,
            10,
            2,
            None,
            DistanceMetric::Euclidean,
            &store,
            3,
            None,
            crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
        ));
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::DimensionMismatch { expected, actual } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 3);
            }
            other => panic!("expected DimensionMismatch, got: {other}"),
        }
    }

    #[test]
    fn test_top_k_zero() {
        let index = make_index();
        let query = vec![1.0, 2.0];

        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = rt.block_on(async {
            let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
            ZeppelinStore::new(mem)
        });

        let results = rt
            .block_on(search_ivf_flat(
                &index,
                &query,
                0,
                2,
                None,
                DistanceMetric::Euclidean,
                &store,
                3,
                None,
                crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
            ))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn adaptive_sketch_cap_scales_monotonically() {
        let mut prev = 0usize;
        for nprobe in 1..=128 {
            let budget = adaptive_sketch_budget(nprobe);
            let cap = budget.max_clusters();
            assert!(
                cap >= prev,
                "budget cap must be monotonic: nprobe={nprobe} cap={cap} prev={prev}"
            );
            assert!(cap <= nprobe);
            prev = cap;
        }

        assert_eq!(adaptive_sketch_budget(8).max_clusters(), 8);
        assert_eq!(adaptive_sketch_budget(16).max_clusters(), 14);
        assert_eq!(adaptive_sketch_budget(128).max_clusters(), 128);
        assert_eq!(grouped_object_cap_for_arity(4, 16), 6);
        assert_eq!(grouped_object_cap_for_arity(4, 128), 128);
    }

    fn rerank_request(range: Range<usize>) -> RerankRangeRequest {
        RerankRangeRequest {
            cluster_idx: 0,
            need: RerankNeed {
                id: "id".to_string(),
                row_idx: 0,
                vector_range: Some(range.clone()),
            },
            range,
        }
    }

    #[test]
    fn coalesce_rerank_ranges_sorts_and_merges_small_gaps() {
        let requests = vec![
            rerank_request(300..310),
            rerank_request(100..110),
            rerank_request(120..130),
        ];

        let coalesced = coalesce_rerank_ranges(&requests, 11).unwrap();

        assert_eq!(
            coalesced,
            vec![
                CoalescedRerankRange {
                    range: 100..130,
                    request_indices: vec![1, 2],
                },
                CoalescedRerankRange {
                    range: 300..310,
                    request_indices: vec![0],
                },
            ]
        );
    }

    #[test]
    fn coalesce_rerank_ranges_does_not_merge_gap_equal_to_threshold() {
        let requests = vec![rerank_request(100..110), rerank_request(120..130)];

        let coalesced = coalesce_rerank_ranges(&requests, 10).unwrap();

        assert_eq!(
            coalesced,
            vec![
                CoalescedRerankRange {
                    range: 100..110,
                    request_indices: vec![0],
                },
                CoalescedRerankRange {
                    range: 120..130,
                    request_indices: vec![1],
                },
            ]
        );
    }

    #[test]
    fn coalesce_rerank_ranges_rejects_empty_ranges() {
        let requests = vec![rerank_request(100..100)];

        let err = coalesce_rerank_ranges(&requests, 1024).unwrap_err();

        assert!(err.to_string().contains("invalid rerank vector range"));
    }
}
