//! Search phase for IVF-Flat index.
//!
//! 1. Compute distance from query to all centroids.
//! 2. Select top-`nprobe` closest centroids.
//! 3. For each selected cluster, fetch and scan all vectors.
//! 4. Apply post-filter with oversampling if a filter is present.
//! 5. Return sorted top-k results.

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::index::distance::compute_distance;
use crate::index::filter::{evaluate_filter, oversampled_k};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::build::{attrs_key, cluster_key, deserialize_attrs, deserialize_cluster};
use super::IvfFlatIndex;

use crate::index::bitmap::evaluate::evaluate_filter_bitmap;
use crate::index::bitmap::{bitmap_key, ClusterBitmapIndex};

/// A candidate result during search, before final ranking.
struct Candidate {
    id: String,
    score: f32,
    attributes: Option<HashMap<String, AttributeValue>>,
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

    debug!(
        nprobe = effective_nprobe,
        clusters = ?probe_clusters,
        "probing clusters"
    );

    // Speculative prefetch: warm the cache for the next closest cluster beyond nprobe.
    // This fires a background S3 GET so the next query with overlapping clusters hits cache.
    if effective_nprobe < num_clusters {
        if let Some(&(next_cluster_idx, _)) = centroid_dists.get(effective_nprobe) {
            if let Some(c) = cache {
                let cvec_key = cluster_key(&index.namespace, &index.segment_id, next_cluster_idx);
                let cache_clone = c.clone();
                let store_clone = store.clone();
                tokio::spawn(async move {
                    let _ = cache_clone
                        .get_or_fetch(&cvec_key, || store_clone.get(&cvec_key))
                        .await;
                });
            }
        }
    }

    // --- Step 2: Determine fetch size (oversample if filtering) ---
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };

    // --- Step 3: Scan selected clusters ---
    // Use quantized search path if quantization is available.
    let candidates = match index.quantization {
        QuantizationType::Scalar => {
            scan_clusters_sq(
                index,
                &probe_clusters,
                query,
                distance_metric,
                filter,
                fetch_k,
                store,
                cache,
            )
            .await?
        }
        QuantizationType::Product => {
            scan_clusters_pq(
                index,
                &probe_clusters,
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
                &probe_clusters,
                query,
                distance_metric,
                filter,
                store,
                cache,
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

    debug!(returned = results.len(), top_k = top_k, "search complete");

    Ok(results)
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
) -> Result<Vec<Candidate>> {
    let has_bitmaps = !index.bitmap_fields.is_empty();

    // Phase 1: Parallel prefetch — all S3 I/O fires concurrently.
    let prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let cvec_key = cluster_key(&index.namespace, &index.segment_id, cluster_idx);
        async move {
            let (cluster_res, prefilter, attrs) = tokio::join!(
                fetch_with_cache(cache, store, &cvec_key),
                try_bitmap_prefilter(
                    &index.namespace,
                    &index.segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                load_attrs(index, cluster_idx, filter, store, cache),
            );
            (cluster_idx, cluster_res, prefilter, attrs)
        }
    }))
    .await;

    // Phase 2: Sequential compute — CPU-bound, no I/O.
    let mut candidates = Vec::new();
    for (cluster_idx, cluster_res, prefilter, attrs) in prefetched {
        let cluster_data = match cluster_res {
            Ok(data) => data,
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read cluster, skipping");
                continue;
            }
        };
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
            });
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
) -> Result<Vec<Candidate>> {
    use crate::index::quantization::sq::{
        deserialize_sq_cluster, sq_calibration_key, sq_cluster_key, SqCalibration,
    };

    // Load SQ calibration.
    let cal_key = sq_calibration_key(&index.namespace, &index.segment_id);
    let cal_data = fetch_with_cache(cache, store, &cal_key).await?;
    let calibration = SqCalibration::from_bytes(&cal_data)?;

    // Phase 1: Coarse ranking with quantized distances — parallel prefetch.
    let has_bitmaps = !index.bitmap_fields.is_empty();

    let coarse_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let sq_key = sq_cluster_key(&index.namespace, &index.segment_id, cluster_idx);
        async move {
            let (prefilter, sq_res) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    &index.segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &sq_key),
            );
            (cluster_idx, prefilter, sq_res)
        }
    }))
    .await;

    let mut coarse_candidates: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, sq_res) in coarse_prefetched {
        let sq_data = match sq_res {
            Ok(data) => data,
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read SQ cluster, falling back to flat");
                let cvec_key = cluster_key(&index.namespace, &index.segment_id, cluster_idx);
                if let Ok(data) = fetch_with_cache(cache, store, &cvec_key).await {
                    let cluster = deserialize_cluster(&data)?;
                    for (j, vec) in cluster.vectors.iter().enumerate() {
                        if let Some(ref bm) = prefilter {
                            if !bm.contains(j as u32) {
                                continue;
                            }
                        }
                        let score = compute_distance(query, vec, distance_metric);
                        coarse_candidates.push((cluster.ids[j].clone(), score, cluster_idx));
                    }
                }
                continue;
            }
        };
        let sq_cluster = deserialize_sq_cluster(&sq_data)?;

        for (j, codes) in sq_cluster.codes.iter().enumerate() {
            if let Some(ref bm) = prefilter {
                if !bm.contains(j as u32) {
                    continue;
                }
            }
            let approx_score = calibration.asymmetric_distance(query, codes, distance_metric);
            coarse_candidates.push((sq_cluster.ids[j].clone(), approx_score, cluster_idx));
        }
    }

    // Sort by approximate distance and take top candidates for reranking.
    coarse_candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Rerank factor: take more candidates than needed for full-precision reranking.
    let rerank_count = fetch_k * 4; // 4x reranking factor
    coarse_candidates.truncate(rerank_count);

    debug!(
        coarse_candidates = coarse_candidates.len(),
        rerank_count = rerank_count,
        "SQ8 coarse ranking complete, starting rerank"
    );

    // Phase 2: Rerank with full-precision vectors — parallel prefetch.
    let mut cluster_candidates: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, _, cluster_idx) in &coarse_candidates {
        cluster_candidates
            .entry(*cluster_idx)
            .or_default()
            .push(id.clone());
    }

    let rerank_prefetched =
        futures::future::join_all(cluster_candidates.iter().map(|(&cluster_idx, needed_ids)| {
            let cvec_key = cluster_key(&index.namespace, &index.segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let (cluster_res, attrs) = tokio::join!(
                    fetch_with_cache(cache, store, &cvec_key),
                    load_attrs(index, cluster_idx, filter, store, cache),
                );
                (cluster_idx, needed_ids, cluster_res, attrs)
            }
        }))
        .await;

    let mut candidates = Vec::new();
    for (_, needed_ids, cluster_res, attrs) in rerank_prefetched {
        let cluster_data = match cluster_res {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster = deserialize_cluster(&cluster_data)?;

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

    let coarse_prefetched = futures::future::join_all(probe_clusters.iter().map(|&cluster_idx| {
        let pq_key = pq_cluster_key(&index.namespace, &index.segment_id, cluster_idx);
        async move {
            let (prefilter, pq_res) = tokio::join!(
                try_bitmap_prefilter(
                    &index.namespace,
                    &index.segment_id,
                    cluster_idx,
                    filter,
                    has_bitmaps,
                    store,
                    cache,
                ),
                fetch_with_cache(cache, store, &pq_key),
            );
            (cluster_idx, prefilter, pq_res)
        }
    }))
    .await;

    let mut coarse_candidates: Vec<(String, f32, usize)> = Vec::new();
    for (cluster_idx, prefilter, pq_res) in coarse_prefetched {
        let pq_data = match pq_res {
            Ok(data) => data,
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read PQ cluster, skipping");
                continue;
            }
        };
        let pq_cluster = deserialize_pq_cluster(&pq_data)?;

        for (j, codes) in pq_cluster.codes.iter().enumerate() {
            if let Some(ref bm) = prefilter {
                if !bm.contains(j as u32) {
                    continue;
                }
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

    let rerank_prefetched =
        futures::future::join_all(cluster_candidates.iter().map(|(&cluster_idx, needed_ids)| {
            let cvec_key = cluster_key(&index.namespace, &index.segment_id, cluster_idx);
            let needed_ids = needed_ids.clone();
            async move {
                let (cluster_res, attrs) = tokio::join!(
                    fetch_with_cache(cache, store, &cvec_key),
                    load_attrs(index, cluster_idx, filter, store, cache),
                );
                (needed_ids, cluster_res, attrs)
            }
        }))
        .await;

    let mut candidates = Vec::new();
    for (needed_ids, cluster_res, attrs) in rerank_prefetched {
        let cluster_data = match cluster_res {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster = deserialize_cluster(&cluster_data)?;

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

/// Load attribute data for a cluster. Shared helper for all scan methods.
async fn load_attrs(
    index: &IvfFlatIndex,
    cluster_idx: usize,
    filter: Option<&Filter>,
    store: &ZeppelinStore,
    cache: Option<&Arc<DiskCache>>,
) -> Option<Vec<Option<HashMap<String, AttributeValue>>>> {
    let akey = attrs_key(&index.namespace, &index.segment_id, cluster_idx);
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
        // Load attrs for result enrichment even without filters.
        match fetch_with_cache(cache, store, &akey).await {
            Ok(data) => deserialize_attrs(&data).ok(),
            Err(_) => None,
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> IvfFlatIndex {
        IvfFlatIndex {
            centroids: vec![vec![0.0, 0.0], vec![10.0, 10.0]],
            num_vectors: 4,
            dim: 2,
            namespace: "test_ns".to_string(),
            segment_id: "seg_001".to_string(),
            quantization: QuantizationType::None,
            bitmap_fields: Vec::new(),
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
            ))
            .unwrap();
        assert!(results.is_empty());
    }
}
