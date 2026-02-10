use std::collections::{HashMap, HashSet};

use tracing::{debug, instrument};

use crate::error::Result;
use crate::index::distance::compute_distance;
use crate::index::filter::evaluate_filter;
use crate::index::IvfFlatIndex;
use crate::server::handlers::query::QueryResponse;
use crate::storage::ZeppelinStore;
use crate::types::{ConsistencyLevel, DistanceMetric, Filter, SearchResult};
use crate::wal::Manifest;
use crate::wal::WalReader;

/// Execute a query against a namespace, combining WAL scan and segment search.
#[instrument(skip(store, wal_reader, query, filter), fields(namespace = namespace))]
pub async fn execute_query(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    distance_metric: DistanceMetric,
    oversample_factor: usize,
) -> Result<QueryResponse> {
    let manifest = Manifest::read(store, namespace).await?.unwrap_or_default();

    let mut scanned_fragments = 0;
    let mut scanned_segments = 0;

    // WAL scan (always for Strong, never for Eventual)
    let wal_results = match consistency {
        ConsistencyLevel::Strong => {
            let (results, frag_count) =
                wal_scan(store, wal_reader, namespace, query, filter, distance_metric).await?;
            scanned_fragments = frag_count;
            results
        }
        ConsistencyLevel::Eventual => Vec::new(),
    };

    // Segment search
    let segment_results = if let Some(ref segment_id) = manifest.active_segment {
        let results = segment_search(
            store,
            namespace,
            segment_id,
            query,
            top_k,
            nprobe,
            filter,
            distance_metric,
            oversample_factor,
        )
        .await?;
        scanned_segments = 1;
        results
    } else {
        Vec::new()
    };

    // Merge results
    let results = merge_results(wal_results, segment_results, top_k, consistency);

    debug!(
        result_count = results.len(),
        scanned_fragments,
        scanned_segments,
        "query complete"
    );

    Ok(QueryResponse {
        results,
        scanned_fragments,
        scanned_segments,
    })
}

/// Scan all uncompacted WAL fragments, deduplicate, apply deletes, score, and filter.
async fn wal_scan(
    _store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    query: &[f32],
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
) -> Result<(Vec<SearchResult>, usize)> {
    let fragments = wal_reader.read_uncompacted_fragments(namespace).await?;
    let frag_count = fragments.len();

    if fragments.is_empty() {
        return Ok((Vec::new(), 0));
    }

    // Collect all delete tombstones
    let mut deleted_ids: HashSet<String> = HashSet::new();
    // Latest vector state per ID (latest fragment wins)
    let mut latest_vectors: HashMap<String, (Vec<f32>, Option<HashMap<String, crate::types::AttributeValue>>)> =
        HashMap::new();

    // Process fragments in ULID order (oldest first, so later overwrites earlier)
    for fragment in &fragments {
        for del_id in &fragment.deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id);
        }
        for vec in &fragment.vectors {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(vec.id.clone(), (vec.values.clone(), vec.attributes.clone()));
        }
    }

    // Score surviving vectors
    let mut results: Vec<SearchResult> = latest_vectors
        .into_iter()
        .filter(|(_, (values, attrs))| {
            if let Some(f) = filter {
                match attrs {
                    Some(a) => evaluate_filter(f, a),
                    None => false,
                }
            } else {
                let _ = values; // suppress unused warning
                true
            }
        })
        .map(|(id, (values, attributes))| {
            let score = compute_distance(query, &values, distance_metric);
            SearchResult {
                id,
                score,
                attributes,
            }
        })
        .collect();

    results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal));

    debug!(
        surviving_vectors = results.len(),
        total_fragments = frag_count,
        "WAL scan complete"
    );

    Ok((results, frag_count))
}

/// Search a single segment via the IVF-Flat index.
async fn segment_search(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    oversample_factor: usize,
) -> Result<Vec<SearchResult>> {
    let index = IvfFlatIndex::load(store, namespace, segment_id).await?;

    use crate::index::ivf_flat::search::search_ivf_flat;
    let results = search_ivf_flat(
        &index,
        query,
        top_k,
        nprobe,
        filter,
        distance_metric,
        store,
        oversample_factor,
    )
    .await?;

    Ok(results)
}

/// Merge WAL results and segment results.
///
/// For Strong consistency: filter segment results to remove any IDs that were
/// deleted or updated in the WAL, then merge both sorted lists and truncate to top_k.
fn merge_results(
    wal_results: Vec<SearchResult>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            // WAL results already have the latest state.
            // Remove segment results whose IDs appear in WAL results (WAL is authoritative).
            let wal_ids: HashSet<String> =
                wal_results.iter().map(|r| r.id.clone()).collect();
            let mut merged: Vec<SearchResult> = wal_results;

            for sr in segment_results {
                if !wal_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            merged.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            merged.truncate(top_k);
            merged
        }
        ConsistencyLevel::Eventual => {
            let mut results = segment_results;
            results.truncate(top_k);
            results
        }
    }
}
