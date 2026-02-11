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
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, Filter, SearchResult};

use super::build::{attrs_key, cluster_key, deserialize_attrs, deserialize_cluster};
use super::IvfFlatIndex;

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

    // --- Step 2: Determine fetch size (oversample if filtering) ---
    let fetch_k = if filter.is_some() {
        oversampled_k(top_k, oversample_factor)
    } else {
        top_k
    };

    // --- Step 3: Scan selected clusters ---
    let mut candidates: Vec<Candidate> = Vec::new();

    for &cluster_idx in &probe_clusters {
        // Fetch cluster vector data.
        let cvec_key = cluster_key(&index.namespace, &index.segment_id, cluster_idx);
        let cluster_data = match fetch_with_cache(cache, store, &cvec_key).await {
            Ok(data) => data,
            Err(e) => {
                warn!(cluster = cluster_idx, error = %e, "failed to read cluster, skipping");
                continue;
            }
        };
        let cluster = deserialize_cluster(&cluster_data)?;

        // Fetch attributes if we need them for filtering.
        let attrs: Option<Vec<Option<HashMap<String, AttributeValue>>>> = if filter.is_some() {
            let akey = attrs_key(&index.namespace, &index.segment_id, cluster_idx);
            match fetch_with_cache(cache, store, &akey).await {
                Ok(data) => Some(deserialize_attrs(&data)?),
                Err(e) => {
                    warn!(cluster = cluster_idx, error = %e, "failed to read attrs, skipping filter for cluster");
                    None
                }
            }
        } else {
            // Even without a filter, load attrs so we can return them in results.
            let akey = attrs_key(&index.namespace, &index.segment_id, cluster_idx);
            match fetch_with_cache(cache, store, &akey).await {
                Ok(data) => Some(deserialize_attrs(&data)?),
                Err(_) => None,
            }
        };

        // Score each vector in the cluster.
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

    debug!(
        total_candidates = candidates.len(),
        fetch_k = fetch_k,
        "scanned clusters"
    );

    // --- Step 4: Sort all candidates by distance ---
    candidates.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // --- Step 5: Apply post-filter if present ---
    let results: Vec<SearchResult> = if let Some(f) = filter {
        candidates
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
        candidates
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
