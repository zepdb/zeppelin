//! Search phase for the hierarchical ANN index.
//!
//! Uses beam search to navigate the centroid tree, probing `beam_width`
//! candidates at each level. At the leaf level, scans data clusters
//! (identical to IVF-Flat scan) with optional quantized two-phase search.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, warn};

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, deserialize_attrs, deserialize_cluster,
};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::{deserialize_tree_node, tree_node_key, HierarchicalIndex};

/// A candidate result during search, before final ranking.
struct Candidate {
    id: String,
    score: f32,
    attributes: Option<HashMap<String, AttributeValue>>,
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
        return scan_leaf_clusters(
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
        .await;
    }

    // Descend through internal levels.
    let mut current_ids: Vec<String> = beam.into_iter().map(|(id, _)| id).collect();

    loop {
        let mut next_beam: Vec<(String, f32, bool)> = Vec::new(); // (child_id, dist, is_leaf)
        let mut any_leaf = false;
        let mut any_internal = false;

        for node_id in &current_ids {
            let nkey = tree_node_key(ns, seg, node_id);
            let node_data = match fetch_with_cache(cache, store, &nkey).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(node_id = %node_id, error = %e, "failed to load tree node, skipping");
                    continue;
                }
            };
            let node = deserialize_tree_node(&node_data)?;

            if node.is_leaf {
                any_leaf = true;
            } else {
                any_internal = true;
            }

            for (c, child_id) in node.children.iter().enumerate() {
                let dist = compute_distance(query, &node.centroids[c], distance_metric);
                next_beam.push((child_id.clone(), dist, node.is_leaf));
            }
        }

        // Sort by distance and keep top beam_width.
        next_beam.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        next_beam.truncate(effective_beam);

        debug!(
            candidates = next_beam.len(),
            any_leaf,
            any_internal,
            "beam search: descending level"
        );

        // If we've reached leaf level, scan clusters.
        if any_leaf && !any_internal {
            let cluster_indices: Vec<usize> = next_beam
                .iter()
                .filter_map(|(id, _, _)| id.parse::<usize>().ok())
                .collect();
            return scan_leaf_clusters(
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
            .await;
        }

        // Mixed level: separate leaf and internal children.
        if any_leaf && any_internal {
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

            // Scan leaf clusters.
            let leaf_results = scan_leaf_clusters(
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

            // Continue descending internal nodes.
            current_ids = internal_ids;
            if current_ids.is_empty() {
                return Ok(leaf_results);
            }
            // For simplicity in mixed case, just continue descent.
            // The leaf results will be merged with internal results below.
            // TODO: proper merge for mixed-depth trees.
            continue;
        }

        // All internal — continue descent.
        current_ids = next_beam.into_iter().map(|(id, _, _)| id).collect();

        if current_ids.is_empty() {
            return Ok(Vec::new());
        }
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
) -> Result<Vec<SearchResult>> {
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };

    let ns = &index.namespace;
    let seg = &index.segment_id;

    let candidates = match index.meta.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(ns, seg, cluster_indices, query, distance_metric, filter, fetch_k, store, cache).await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(ns, seg, cluster_indices, query, distance_metric, filter, fetch_k, store, cache).await?
        }
        QuantizationType::None => {
            scan_clusters_flat(ns, seg, cluster_indices, query, distance_metric, filter, store, cache).await?
        }
    };

    // Sort and apply filter.
    let mut sorted = candidates;
    sorted.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let results: Vec<SearchResult> = if let Some(f) = filter {
        sorted
            .into_iter()
            .filter(|c| match &c.attributes {
                Some(attrs) => evaluate_filter(f, attrs),
                None => false,
            })
            .take(top_k)
            .map(|c| SearchResult {
                id: c.id,
                score: c.score,
                attributes: c.attributes,
            })
            .collect()
    } else {
        sorted
            .into_iter()
            .take(top_k)
            .map(|c| SearchResult {
                id: c.id,
                score: c.score,
                attributes: c.attributes,
            })
            .collect()
    };

    debug!(returned = results.len(), top_k, "hierarchical search complete");
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
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    let mut candidates = Vec::new();

    for &cluster_idx in cluster_indices {
        let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
        let cluster_data = match fetch_with_cache(cache, store, &cvec_key).await {
            Ok(data) => data,
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read cluster, skipping");
                continue;
            }
        };
        let cluster = deserialize_cluster(&cluster_data)?;
        let attrs = load_attrs(namespace, segment_id, cluster_idx, filter, store, cache).await;

        for (j, vec) in cluster.vectors.iter().enumerate() {
            let score = compute_distance(query, vec, distance_metric);
            let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();
            candidates.push(Candidate {
                id: cluster.ids[j].clone(),
                score,
                attributes: vector_attrs,
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
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{
        deserialize_sq_cluster, sq_calibration_key, sq_cluster_key, SqCalibration,
    };

    let cal_key = sq_calibration_key(namespace, segment_id);
    let cal_data = fetch_with_cache(cache, store, &cal_key).await?;
    let calibration = SqCalibration::from_bytes(&cal_data)?;

    // Phase 1: coarse ranking.
    let mut coarse: Vec<(String, f32, usize)> = Vec::new();

    for &cluster_idx in cluster_indices {
        let sq_key = sq_cluster_key(namespace, segment_id, cluster_idx);
        match fetch_with_cache(cache, store, &sq_key).await {
            Ok(sq_data) => {
                let sq_cluster = deserialize_sq_cluster(&sq_data)?;
                for (j, codes) in sq_cluster.codes.iter().enumerate() {
                    let approx = calibration.asymmetric_distance(query, codes, distance_metric);
                    coarse.push((sq_cluster.ids[j].clone(), approx, cluster_idx));
                }
            }
            Err(_) => {
                // Fallback to flat.
                let cvec_key = cluster_key(namespace, segment_id, cluster_idx);
                if let Ok(data) = fetch_with_cache(cache, store, &cvec_key).await {
                    let cluster = deserialize_cluster(&data)?;
                    for (j, vec) in cluster.vectors.iter().enumerate() {
                        let score = compute_distance(query, vec, distance_metric);
                        coarse.push((cluster.ids[j].clone(), score, cluster_idx));
                    }
                }
            }
        }
    }

    coarse.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let rerank_count = fetch_k * 4;
    coarse.truncate(rerank_count);

    // Phase 2: rerank with full-precision.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let mut candidates = Vec::new();
    for (cluster_idx, needed_ids) in &by_cluster {
        let cvec_key = cluster_key(namespace, segment_id, *cluster_idx);
        let cluster_data = match fetch_with_cache(cache, store, &cvec_key).await {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster = deserialize_cluster(&cluster_data)?;
        let attrs = load_attrs(namespace, segment_id, *cluster_idx, filter, store, cache).await;
        let needed_set: std::collections::HashSet<&str> =
            needed_ids.iter().map(|s| s.as_str()).collect();

        for (j, id) in cluster.ids.iter().enumerate() {
            if needed_set.contains(id.as_str()) {
                let score = compute_distance(query, &cluster.vectors[j], distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();
                candidates.push(Candidate {
                    id: id.clone(),
                    score,
                    attributes: vector_attrs,
                });
            }
        }
    }

    Ok(candidates)
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

    // Phase 1: coarse ranking.
    let mut coarse: Vec<(String, f32, usize)> = Vec::new();

    for &cluster_idx in cluster_indices {
        let pq_key = pq_cluster_key(namespace, segment_id, cluster_idx);
        match fetch_with_cache(cache, store, &pq_key).await {
            Ok(pq_data) => {
                let pq_cluster = deserialize_pq_cluster(&pq_data)?;
                for (j, codes) in pq_cluster.codes.iter().enumerate() {
                    let approx = codebook.adc_distance(&adc_table, codes);
                    coarse.push((pq_cluster.ids[j].clone(), approx, cluster_idx));
                }
            }
            Err(_) => continue,
        }
    }

    coarse.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let rerank_count = fetch_k * 4;
    coarse.truncate(rerank_count);

    // Phase 2: rerank.
    let mut by_cluster: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cidx) in &coarse {
        by_cluster.entry(*cidx).or_default().push(id.clone());
    }

    let mut candidates = Vec::new();
    for (cluster_idx, needed_ids) in &by_cluster {
        let cvec_key = cluster_key(namespace, segment_id, *cluster_idx);
        let cluster_data = match fetch_with_cache(cache, store, &cvec_key).await {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster = deserialize_cluster(&cluster_data)?;
        let attrs = load_attrs(namespace, segment_id, *cluster_idx, filter, store, cache).await;
        let needed_set: std::collections::HashSet<&str> =
            needed_ids.iter().map(|s| s.as_str()).collect();

        for (j, id) in cluster.ids.iter().enumerate() {
            if needed_set.contains(id.as_str()) {
                let score = compute_distance(query, &cluster.vectors[j], distance_metric);
                let vector_attrs = attrs.as_ref().and_then(|a| a.get(j)).cloned().flatten();
                candidates.push(Candidate {
                    id: id.clone(),
                    score,
                    attributes: vector_attrs,
                });
            }
        }
    }

    Ok(candidates)
}

/// Load attribute data for a cluster.
async fn load_attrs(
    namespace: &str,
    segment_id: &str,
    cluster_idx: usize,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Option<Vec<Option<HashMap<String, AttributeValue>>>> {
    let akey = attrs_key(namespace, segment_id, cluster_idx);
    if filter.is_some() {
        match fetch_with_cache(cache, store, &akey).await {
            Ok(data) => match deserialize_attrs(&data) {
                Ok(a) => Some(a),
                Err(e) => {
                    warn!(cluster = cluster_idx, error = %e, "failed to parse attrs");
                    None
                }
            },
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read attrs");
                None
            }
        }
    } else {
        match fetch_with_cache(cache, store, &akey).await {
            Ok(data) => deserialize_attrs(&data).ok(),
            Err(_) => None,
        }
    }
}
