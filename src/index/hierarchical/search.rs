//! Search phase for the hierarchical ANN index.
//!
//! Uses beam search to navigate the centroid tree, probing `beam_width`
//! candidates at each level. At the leaf level, scans data clusters
//! (identical to IVF-Flat scan) with optional quantized two-phase search.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::debug;

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, deserialize_attrs, deserialize_cluster,
    deserialize_colocated_sq_cluster,
};
use crate::index::ivf_flat::search::coarse_row_passes;
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::{deserialize_tree_node, tree_node_key, HierarchicalIndex};

use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
use crate::index::bitmap::{bitmap_key, ClusterBitmapIndex};

type ClusterAttrs = Vec<Option<HashMap<String, AttributeValue>>>;

/// A candidate result during search, before final ranking.
struct Candidate {
    id: String,
    score: f32,
    attributes: Option<HashMap<String, AttributeValue>>,
    cluster_idx: usize,
    row_idx: usize,
}

/// Fetch data from cache or S3.
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

/// Execute a hierarchical beam search.
///
/// 1. Load root node, rank centroids, keep top `beam_width` children.
/// 2. At each level: load child nodes, rank all centroids, keep top `beam_width`.
/// 3. At leaf level: scan the selected clusters for nearest neighbors.
#[allow(clippy::too_many_arguments)]
pub async fn search_hierarchical(
    index: &HierarchicalIndex,
    query: &[f32],
    top_k: usize,
    beam_width: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<SearchResult>> {
    if query.len() != index.meta.dim {
        return Err(ZeppelinError::DimensionMismatch {
            expected: index.meta.dim,
            actual: query.len(),
        });
    }

    if top_k == 0 {
        return Ok(Vec::new());
    }

    let ns = &index.namespace;
    let seg = &index.segment_id;
    let effective_beam = beam_width.max(1);

    // --- Navigate the tree with beam search ---
    // Start at root.
    let root_key = tree_node_key(ns, seg, &index.meta.root_node_id);
    let root_data = fetch_with_cache(cache, store, &root_key).await?;
    let root_node = deserialize_tree_node(&root_data)?;

    // Rank root centroids.
    let mut beam: Vec<(String, f32)> = root_node
        .centroids
        .iter()
        .zip(root_node.children.iter())
        .map(|(c, child_id)| {
            let dist = compute_distance(query, c, distance_metric);
            (child_id.clone(), dist)
        })
        .collect();

    beam.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    beam.truncate(effective_beam);

    debug!(
        root_children = root_node.children.len(),
        beam_size = beam.len(),
        is_leaf = root_node.is_leaf,
        "beam search: root level"
    );

    if root_node.is_leaf {
        // Root is a leaf — scan the selected clusters directly.
        let cluster_indices: Vec<usize> = beam
            .iter()
            .filter_map(|(id, _)| id.parse::<usize>().ok())
            .collect();
        let candidates = scan_leaf_clusters(
            index,
            &cluster_indices,
            query,
            top_k,
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
        )
        .await?;
        return finalize_candidates(ns, seg, candidates, top_k, filter, store, cache).await;
    }

    // Partition root beam into leaf clusters vs internal nodes.
    // Root `is_leaf == false` does NOT mean all children are internal nodes —
    // hybrid nodes have a mix of leaf cluster IDs (numeric, e.g. "42") and
    // internal node IDs (e.g. "n_2_ULID"). Without this partition, leaf cluster
    // IDs enter the descent loop, fail to load as tree nodes, and get silently
    // skipped — causing 0 results for namespaces with mostly-leaf root children.
    let mut root_leaf_clusters: Vec<usize> = Vec::new();
    let mut current_ids: Vec<String> = Vec::new();
    for (id, _) in beam {
        if let Ok(idx) = id.parse::<usize>() {
            root_leaf_clusters.push(idx);
        } else {
            current_ids.push(id);
        }
    }

    let mut accumulated: Vec<Candidate> = Vec::new();

    // Scan any leaf clusters found at root level.
    if !root_leaf_clusters.is_empty() {
        debug!(
            leaf_count = root_leaf_clusters.len(),
            internal_count = current_ids.len(),
            "root beam: partitioned into leaf clusters and internal nodes"
        );
        let leaf_candidates = scan_leaf_clusters(
            index,
            &root_leaf_clusters,
            query,
            top_k,
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
        )
        .await?;
        accumulated.extend(leaf_candidates);
    }

    if current_ids.is_empty() {
        // All root beam entries were leaf clusters — return results.
        return finalize_candidates(ns, seg, accumulated, top_k, filter, store, cache).await;
    }

    loop {
        let mut next_beam: Vec<(String, f32, bool)> = Vec::new(); // (child_id, dist, is_leaf)

        // Parallel prefetch all beam nodes at this level.
        let node_results = futures::future::join_all(current_ids.iter().map(|node_id| {
            let nkey = tree_node_key(ns, seg, node_id);
            async move { (node_id.clone(), fetch_with_cache(cache, store, &nkey).await) }
        }))
        .await;

        for (_, node_res) in node_results {
            let node_data = node_res?;
            let node = deserialize_tree_node(&node_data)?;

            for (c, child_id) in node.children.iter().enumerate() {
                let dist = compute_distance(query, &node.centroids[c], distance_metric);
                // Classify per-child: leaf cluster indices parse as usize,
                // internal node IDs have format "n_{depth}_{ulid}" and never do.
                let child_is_leaf = node.is_leaf || child_id.parse::<usize>().is_ok();
                next_beam.push((child_id.clone(), dist, child_is_leaf));
            }
        }

        // Sort by distance and keep top beam_width.
        next_beam.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        next_beam.truncate(effective_beam);

        let any_internal = next_beam.iter().any(|(_, _, is_leaf)| !*is_leaf);

        debug!(
            candidates = next_beam.len(),
            any_internal, "beam search: descending level"
        );

        // Separate leaf cluster entries from internal node entries.
        let mut leaf_clusters: Vec<usize> = Vec::new();
        let mut internal_ids: Vec<String> = Vec::new();

        for (id, _, is_leaf) in &next_beam {
            if *is_leaf {
                if let Ok(idx) = id.parse::<usize>() {
                    leaf_clusters.push(idx);
                }
            } else {
                internal_ids.push(id.clone());
            }
        }

        // Scan any leaf clusters found at this level.
        if !leaf_clusters.is_empty() {
            let leaf_candidates = scan_leaf_clusters(
                index,
                &leaf_clusters,
                query,
                top_k,
                filter,
                distance_metric,
                store,
                oversample_factor,
                cache,
            )
            .await?;
            accumulated.extend(leaf_candidates);
        }

        if internal_ids.is_empty() {
            // No more internal nodes to descend — return merged results.
            return finalize_candidates(ns, seg, accumulated, top_k, filter, store, cache).await;
        }

        current_ids = internal_ids;
    }
}

/// Scan leaf clusters and return ranked results.
/// Dispatches to flat, SQ8, or PQ scan based on index quantization.
#[allow(clippy::too_many_arguments)]
async fn scan_leaf_clusters(
    index: &HierarchicalIndex,
    cluster_indices: &[usize],
    query: &[f32],
    top_k: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    store: &ZeppelinStore,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };

    debug!(nprobe = cluster_indices.len(), clusters = ?cluster_indices, "probing leaf clusters");

    let ns = &index.namespace;
    let seg = &index.segment_id;
    let has_bitmaps = !index.bitmap_fields.is_empty();

    let candidates = match index.meta.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                fetch_k,
                index.meta.sq_calibration.as_deref(),
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                fetch_k,
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::None => {
            scan_clusters_flat(
                ns,
                seg,
                cluster_indices,
                query,
                distance_metric,
                filter,
                has_bitmaps,
                store,
                cache,
            )
            .await?
        }
    };

    debug!(
        total_candidates = candidates.len(),
        fetch_k, "scanned leaf clusters"
    );

    // Sort and apply filter.
    let mut sorted = candidates;
    sorted.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let results: Vec<Candidate> = if let Some(f) = filter {
        sorted
            .into_iter()
            .filter(|c| match &c.attributes {
                Some(attrs) => evaluate_filter(f, attrs),
                None => false,
            })
            .take(top_k)
            .collect()
    } else {
        sorted.into_iter().take(top_k).collect()
    };

    debug!(
        returned = results.len(),
        top_k, "hierarchical search complete"
    );
    Ok(results)
}

/// Flat scan of leaf clusters (no quantization).
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_flat(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    // Phase 1: Parallel prefetch — all S3 I/O fires concurrently.
    let want_attrs = filter.is_some();
    let prefetched = futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| {
        let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
        async move {
            let (cluster_res, prefilter, attrs) = tokio::join!(
                fetch_with_cache(cache, store, &cvec_key),
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                async {
                    if want_attrs {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, cluster_res, prefilter, attrs)
        }
    }))
    .await;

    // Phase 2: Sequential compute — CPU-bound, no I/O.
    let mut candidates = Vec::new();
    for (cluster_idx, cluster_res, prefilter, attrs) in prefetched {
        let cluster_data = cluster_res?;
        let attrs = attrs?;
        let cluster = deserialize_cluster(&cluster_data)?;

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

    Ok(candidates)
}

/// SQ8 two-phase scan of leaf clusters.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_sq(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    sq_calibration: Option<&[u8]>,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{sq_calibration_key, SqCalibration};

    let calibration = if let Some(calibration) = sq_calibration {
        SqCalibration::from_bytes(calibration)?
    } else {
        let cal_key = sq_calibration_key(namespace, segment_id);
        let cal_data = fetch_with_cache(cache, store, &cal_key).await?;
        SqCalibration::from_bytes(&cal_data)?
    };
    let prefer_colocated_clusters = sq_calibration.is_some();

    // Phase 1: coarse ranking — parallel prefetch. A non-bitmap attribute
    // filter must be applied DURING this scan, before truncating to
    // `rerank_count` — otherwise a selective filter's matches get truncated
    // away by approximate-distance ranking and the query silently under-fills
    // top_k (Task 6). Fetch attrs alongside the SQ codes whenever a filter is
    // active; bitmap-resolved clusters keep their fast path and ignore attrs.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched =
        futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| async move {
            let (prefilter, sq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                load_sq_cluster_for_coarse(
                    namespace,
                    segment_id,
                    cluster_idx,
                    prefer_colocated_clusters,
                    store,
                    cache,
                ),
                async {
                    if want_attr_filter {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, sq_res, attrs)
        }))
        .await;

    let mut coarse: Vec<(String, f32, usize)> = Vec::new();
    let mut prefetched_clusters: HashMap<usize, bytes::Bytes> = HashMap::new();
    for (cluster_idx, prefilter, sq_res, attrs) in coarse_prefetched {
        let (sq_cluster, cluster_data) = sq_res?;
        if let Some(cluster_data) = cluster_data {
            prefetched_clusters.insert(cluster_idx, cluster_data);
        }
        let attrs = attrs?;
        for (j, codes) in sq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx = calibration.asymmetric_distance(query, codes, distance_metric);
            coarse.push((sq_cluster.ids[j].clone(), approx, cluster_idx));
        }
    }

    coarse.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let rerank_count = fetch_k * 4;
    coarse.truncate(rerank_count);

    debug!(
        coarse_candidates = coarse.len(),
        rerank_count, "SQ8 coarse ranking complete, starting rerank"
    );

    // Phase 2: rerank with full-precision — parallel prefetch.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(by_cluster.iter().map(|(&cluster_idx, needed_ids)| {
            let prefetched_cluster = prefetched_clusters.get(&cluster_idx).cloned();
            let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let cluster_fetch = async {
                    if let Some(cluster_data) = prefetched_cluster {
                        Ok(cluster_data)
                    } else {
                        fetch_with_cache(cache, store, &cvec_key).await
                    }
                };
                let (cluster_res, attrs) = tokio::join!(cluster_fetch, async {
                    if want_rerank_attrs {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
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
        let cluster = deserialize_cluster(&cluster_data)?;
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

async fn load_sq_cluster_for_coarse(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    prefer_colocated: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<(
    crate::index::quantization::sq::SqClusterData,
    Option<bytes::Bytes>,
)> {
    use crate::index::quantization::sq::{deserialize_sq_cluster, sq_cluster_key};

    if prefer_colocated {
        let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
        let cluster_data = fetch_with_cache(cache, store, &cvec_key).await?;
        if let Some(sq_cluster) = deserialize_colocated_sq_cluster(&cluster_data)? {
            return Ok((sq_cluster, Some(cluster_data)));
        }

        let sq_key = sq_cluster_key(namespace, segment_id, cluster_idx);
        let sq_data = fetch_with_cache(cache, store, &sq_key).await?;
        let sq_cluster = deserialize_sq_cluster(&sq_data)?;
        return Ok((sq_cluster, Some(cluster_data)));
    }

    let sq_key = sq_cluster_key(namespace, segment_id, cluster_idx);
    let sq_data = fetch_with_cache(cache, store, &sq_key).await?;
    let sq_cluster = deserialize_sq_cluster(&sq_data)?;
    Ok((sq_cluster, None))
}

/// PQ two-phase scan of leaf clusters.
#[allow(clippy::too_many_arguments)]
async fn scan_clusters_pq(
    namespace: &str,
    segment_id: &str,
    cluster_indices: &[usize],
    query: &[f32],
    distance_metric: DistanceMetric,
    filter: Option<&Filter>,
    fetch_k: usize,
    has_bitmaps: bool,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::pq::{
        deserialize_pq_cluster, pq_cluster_key, pq_codebook_key, PqCodebook,
    };

    let cb_key = pq_codebook_key(namespace, segment_id);
    let cb_data = fetch_with_cache(cache, store, &cb_key).await?;
    let codebook = PqCodebook::from_bytes(&cb_data)?;
    let adc_table = codebook.build_adc_table(query, distance_metric);

    // Phase 1: coarse ranking — parallel prefetch. Apply a non-bitmap
    // attribute filter DURING the coarse scan so a selective filter's matches
    // survive truncation (Task 6). Fetch attrs alongside the PQ codes whenever
    // a filter is active.
    let want_attr_filter = filter.is_some();

    let coarse_prefetched = futures::future::join_all(cluster_indices.iter().map(|&cluster_idx| {
        let pq_key = pq_cluster_key(namespace, segment_id, cluster_idx);
        async move {
            let (prefilter, pq_res, attrs) = tokio::join!(
                try_bitmap_prefilter(
                    namespace,
                    segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &pq_key),
                async {
                    if want_attr_filter {
                        load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await
                    } else {
                        Ok(None)
                    }
                },
            );
            (cluster_idx, prefilter, pq_res, attrs)
        }
    }))
    .await;

    let mut coarse: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, pq_res, attrs) in coarse_prefetched {
        let pq_data = pq_res?;
        let attrs = attrs?;
        let pq_cluster = deserialize_pq_cluster(&pq_data)?;
        for (j, codes) in pq_cluster.codes.iter().enumerate() {
            if !coarse_row_passes(filter, &prefilter, &attrs, j) {
                continue;
            }
            let approx = codebook.adc_distance(&adc_table, codes);
            coarse.push((pq_cluster.ids[j].clone(), approx, cluster_idx));
        }
    }

    coarse.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let rerank_count = fetch_k * 4;
    coarse.truncate(rerank_count);

    debug!(
        coarse_candidates = coarse.len(),
        "PQ coarse ranking complete, starting rerank"
    );

    // Phase 2: rerank — parallel prefetch.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let want_rerank_attrs = filter.is_some();
    let rerank_prefetched =
        futures::future::join_all(by_cluster.iter().map(|(&cluster_idx, needed_ids)| {
            let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let (cluster_res, attrs) =
                    tokio::join!(fetch_with_cache(cache, store, &cvec_key), async {
                        if want_rerank_attrs {
                            load_attrs(namespace, segment_id, cluster_idx, filter, store, cache)
                                .await
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
        let cluster = deserialize_cluster(&cluster_data)?;
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

async fn finalize_candidates(
    namespace: &str,
    segment_id: &str,
    mut candidates: Vec<Candidate>,
    top_k: usize,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<SearchResult>> {
    candidates.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(top_k);

    if filter.is_some() {
        return Ok(candidates
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                score: candidate.score,
                attributes: candidate.attributes,
            })
            .collect());
    }

    enrich_unfiltered_results(namespace, segment_id, candidates, store, cache).await
}

async fn enrich_unfiltered_results(
    namespace: &str,
    segment_id: &str,
    candidates: Vec<Candidate>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
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
                load_attrs(namespace, segment_id, cluster_idx, None, store, cache).await,
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
            })?
            .as_ref()
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "attrs absent for final result cluster {}",
                    candidate.cluster_idx
                ))
            })?;
        let attributes = cluster_attrs
            .get(candidate.row_idx)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "attrs row {} missing in cluster {}",
                    candidate.row_idx, candidate.cluster_idx
                ))
            })?
            .clone();
        results.push(SearchResult {
            id: candidate.id,
            score: candidate.score,
            attributes,
        });
    }

    Ok(results)
}

/// Try to load a cluster's bitmap index and evaluate the filter against it.
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

/// Load attribute data for a cluster.
async fn load_attrs(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    _filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Option<ClusterAttrs>> {
    let akey = attrs_key(namespace, segment_id, cluster_idx);
    let data = fetch_with_cache(cache, store, &akey).await?;
    Ok(Some(deserialize_attrs(&data)?))
}
