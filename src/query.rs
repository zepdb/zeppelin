use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, instrument};

use crate::cache::DiskCache;
use crate::error::Result;
use crate::fts::bm25::Bm25Params;
use crate::fts::inverted_index::{fts_index_key, InvertedIndex};
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::types::FtsFieldConfig;
use crate::fts::wal_scan::wal_bm25_scan;
use crate::index::distance::compute_distance;
use crate::index::filter::evaluate_filter;
use crate::index::HierarchicalIndex;
use crate::index::IvfFlatIndex;
use crate::server::handlers::query::QueryResponse;
use crate::storage::ZeppelinStore;
use crate::types::{ConsistencyLevel, DistanceMetric, Filter, SearchResult};
use crate::wal::Manifest;
use crate::wal::WalReader;

/// Execute a query against a namespace, combining WAL scan and segment search.
#[allow(clippy::too_many_arguments)]
#[instrument(skip(store, wal_reader, query, filter, cache), fields(namespace = namespace))]
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
    cache: Option<&Arc<DiskCache>>,
) -> Result<QueryResponse> {
    let manifest = Manifest::read(store, namespace).await?.unwrap_or_default();

    let mut scanned_fragments = 0;
    let mut scanned_segments = 0;

    // WAL scan (always for Strong, never for Eventual)
    // Uses the manifest we already read for snapshot consistency — avoids re-reading
    // a newer manifest whose fragments may have been deleted by compaction.
    let wal_start = std::time::Instant::now();
    let wal_results = match consistency {
        ConsistencyLevel::Strong => {
            let (results, frag_count) = wal_scan(
                wal_reader,
                namespace,
                &manifest,
                query,
                filter,
                distance_metric,
            )
            .await?;
            scanned_fragments = frag_count;
            results
        }
        ConsistencyLevel::Eventual => Vec::new(),
    };
    let wal_duration = wal_start.elapsed();
    debug!(
        wal_duration_ms = wal_duration.as_millis() as u64,
        fragments_scanned = scanned_fragments,
        "query phase: WAL scan"
    );

    // Segment search
    let segment_start = std::time::Instant::now();
    let segment_results = if let Some(ref segment_id) = manifest.active_segment {
        // Look up bitmap_fields from the manifest's SegmentRef.
        let bitmap_fields = manifest
            .segments
            .iter()
            .find(|s| s.id == *segment_id)
            .map(|s| s.bitmap_fields.clone())
            .unwrap_or_default();
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
            cache,
            &bitmap_fields,
        )
        .await?;
        scanned_segments = 1;
        results
    } else {
        Vec::new()
    };
    let segment_duration = segment_start.elapsed();
    debug!(
        segment_duration_ms = segment_duration.as_millis() as u64,
        segments_scanned = scanned_segments,
        "query phase: segment search"
    );

    // Merge results
    let merge_start = std::time::Instant::now();
    let results = merge_results(wal_results, segment_results, top_k, consistency);
    let merge_duration = merge_start.elapsed();
    debug!(
        merge_duration_ms = merge_duration.as_millis() as u64,
        final_results = results.len(),
        "query phase: merge"
    );

    Ok(QueryResponse {
        results,
        scanned_fragments,
        scanned_segments,
    })
}

/// Scan all uncompacted WAL fragments, deduplicate, apply deletes, score, and filter.
/// Reads fragments from the provided manifest snapshot (not re-reading manifest from S3).
async fn wal_scan(
    wal_reader: &WalReader,
    namespace: &str,
    manifest: &Manifest,
    query: &[f32],
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
) -> Result<(Vec<SearchResult>, usize)> {
    let refs = manifest.uncompacted_fragments().to_vec();
    let fragments = wal_reader
        .read_fragments_from_refs(namespace, &refs)
        .await?;
    let frag_count = fragments.len();

    if fragments.is_empty() {
        return Ok((Vec::new(), 0));
    }

    // Collect all delete tombstones
    let mut deleted_ids: HashSet<String> = HashSet::new();
    // Latest vector state per ID (latest fragment wins)
    #[allow(clippy::type_complexity)]
    let mut latest_vectors: HashMap<
        String,
        (
            Vec<f32>,
            Option<HashMap<String, crate::types::AttributeValue>>,
        ),
    > = HashMap::new();

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

    results.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    debug!(
        surviving_vectors = results.len(),
        total_fragments = frag_count,
        "WAL scan complete"
    );

    Ok((results, frag_count))
}

/// Search a single segment via IVF-Flat or Hierarchical index.
///
/// Detection: if a `tree_meta.json` exists for the segment, use hierarchical
/// search; otherwise fall back to IVF-Flat.
#[allow(clippy::too_many_arguments)]
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
    cache: Option<&Arc<DiskCache>>,
    bitmap_fields: &[String],
) -> Result<Vec<SearchResult>> {
    // Detect hierarchical index by probing for tree_meta.json.
    let tree_meta_key = crate::index::hierarchical::tree_meta_key(namespace, segment_id);
    if store.get(&tree_meta_key).await.is_ok() {
        let mut index = HierarchicalIndex::load(store, namespace, segment_id).await?;
        index.bitmap_fields = bitmap_fields.to_vec();
        use crate::index::hierarchical::search::search_hierarchical;
        let results = search_hierarchical(
            &index,
            query,
            top_k,
            nprobe, // beam_width uses nprobe
            filter,
            distance_metric,
            store,
            oversample_factor,
            cache,
        )
        .await?;
        return Ok(results);
    }

    let mut index = IvfFlatIndex::load(store, namespace, segment_id).await?;
    index.bitmap_fields = bitmap_fields.to_vec();
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
        cache,
    )
    .await?;

    Ok(results)
}

/// Execute a BM25 full-text search query against a namespace.
///
/// Combines WAL brute-force scan with segment inverted index search.
#[allow(clippy::too_many_arguments)]
#[instrument(skip(store, wal_reader, rank_by, fts_configs, filter), fields(namespace = namespace))]
pub async fn execute_bm25_query(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
) -> Result<QueryResponse> {
    let manifest = Manifest::read(store, namespace).await?.unwrap_or_default();

    let mut scanned_fragments = 0;
    let mut scanned_segments = 0;

    // WAL BM25 scan (always for Strong, never for Eventual)
    let wal_start = std::time::Instant::now();
    let mut wal_deleted_ids = std::collections::HashSet::new();
    let wal_results = match consistency {
        ConsistencyLevel::Strong => {
            let refs = manifest.uncompacted_fragments().to_vec();
            let fragments = wal_reader
                .read_fragments_from_refs(namespace, &refs)
                .await?;
            let scan_result = wal_bm25_scan(&fragments, rank_by, fts_configs, last_as_prefix);
            scanned_fragments = scan_result.fragment_count;
            wal_deleted_ids = scan_result.deleted_ids;
            // Apply post-filter to WAL results
            let mut results = scan_result.results;
            if let Some(f) = filter {
                results.retain(|r| match &r.attributes {
                    Some(attrs) => crate::index::filter::evaluate_filter(f, attrs),
                    None => false,
                });
            }
            results
        }
        ConsistencyLevel::Eventual => Vec::new(),
    };
    let wal_duration = wal_start.elapsed();
    debug!(
        wal_duration_ms = wal_duration.as_millis() as u64,
        fragments_scanned = scanned_fragments,
        "BM25 query phase: WAL scan"
    );

    // Segment BM25 search
    let segment_start = std::time::Instant::now();
    let segment_results = if let Some(ref segment_id) = manifest.active_segment {
        let fts_fields = manifest
            .segments
            .iter()
            .find(|s| s.id == *segment_id)
            .map(|s| s.fts_fields.clone())
            .unwrap_or_default();

        if !fts_fields.is_empty() {
            let results = segment_bm25_search(
                store,
                namespace,
                segment_id,
                rank_by,
                fts_configs,
                &fts_fields,
                filter,
                last_as_prefix,
            )
            .await?;
            scanned_segments = 1;
            results
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let segment_duration = segment_start.elapsed();
    debug!(
        segment_duration_ms = segment_duration.as_millis() as u64,
        segments_scanned = scanned_segments,
        "BM25 query phase: segment search"
    );

    // Merge results — BM25 is higher-is-better
    // Pass deleted IDs so segment results for deleted docs are excluded
    let results = merge_bm25_results(
        wal_results,
        segment_results,
        top_k,
        consistency,
        &wal_deleted_ids,
    );

    Ok(QueryResponse {
        results,
        scanned_fragments,
        scanned_segments,
    })
}

/// Search a segment's inverted indexes for a BM25 query.
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    fts_fields: &[String],
    filter: Option<&Filter>,
    last_as_prefix: bool,
) -> Result<Vec<SearchResult>> {
    use crate::index::ivf_flat::build::{attrs_key, deserialize_attrs};

    // Load the IVF-Flat index to get centroid count
    let index = IvfFlatIndex::load(store, namespace, segment_id).await?;
    let num_clusters = index.num_clusters();

    let field_queries = rank_by.extract_field_queries();
    let mut all_results: HashMap<
        String,
        (f32, Option<HashMap<String, crate::types::AttributeValue>>),
    > = HashMap::new();

    // Search each cluster's inverted index
    for cluster_idx in 0..num_clusters {
        let fts_key = fts_index_key(namespace, segment_id, cluster_idx);
        let fts_data = match store.get(&fts_key).await {
            Ok(data) => data,
            Err(crate::error::ZeppelinError::NotFound { .. }) => continue,
            Err(e) => return Err(e),
        };

        let inv_index = InvertedIndex::from_bytes(&fts_data)?;

        // Load attribute data for this cluster to get doc IDs and attributes
        let akey = attrs_key(namespace, segment_id, cluster_idx);
        let attrs_data = match store.get(&akey).await {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster_attrs = deserialize_attrs(&attrs_data)?;

        // Load cluster to get IDs
        let ckey = crate::index::ivf_flat::build::cluster_key(namespace, segment_id, cluster_idx);
        let cluster_data = store.get(&ckey).await?;
        let cluster = crate::index::ivf_flat::build::deserialize_cluster(&cluster_data)?;

        // For each field+query, search the inverted index
        let mut position_field_scores: HashMap<u32, HashMap<String, f32>> = HashMap::new();

        for (field, query) in &field_queries {
            let config = match fts_configs.get(field.as_str()) {
                Some(c) => c,
                None => continue,
            };

            if !fts_fields.contains(field) {
                continue;
            }

            let query_tokens = tokenize_text(query, config, last_as_prefix);

            let params = Bm25Params {
                k1: config.k1,
                b: config.b,
            };
            let results = if last_as_prefix {
                inv_index.search_prefix(field, &query_tokens, &params)
            } else {
                inv_index.search(field, &query_tokens, &params)
            };

            for (pos, score) in results {
                let entry = position_field_scores.entry(pos).or_default();
                *entry.entry(field.to_string()).or_insert(0.0) += score;
            }
        }

        // Evaluate rank_by expression and collect results
        for (pos, field_scores) in position_field_scores {
            let final_score = evaluate_rank_by(rank_by, &field_scores);
            if final_score <= 0.0 {
                continue;
            }

            let pos_usize = pos as usize;
            if pos_usize >= cluster.ids.len() {
                continue;
            }

            let id = cluster.ids[pos_usize].clone();
            let attrs = cluster_attrs.get(pos_usize).cloned().flatten();

            // Apply post-filter
            if let Some(f) = filter {
                match &attrs {
                    Some(a) => {
                        if !evaluate_filter(f, a) {
                            continue;
                        }
                    }
                    None => continue,
                }
            }

            // Accumulate: same ID might appear in multiple clusters (shouldn't, but be safe)
            let entry = all_results.entry(id.clone()).or_insert((0.0, attrs));
            if final_score > entry.0 {
                entry.0 = final_score;
            }
        }
    }

    let mut results: Vec<SearchResult> = all_results
        .into_iter()
        .map(|(id, (score, attributes))| SearchResult {
            id,
            score,
            attributes,
        })
        .collect();

    // Sort descending by score
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(results)
}

/// Merge BM25 WAL and segment results (higher score = better).
/// `wal_deleted_ids` contains IDs explicitly deleted in the WAL — these must
/// not appear in the final results even if they exist in the segment.
fn merge_bm25_results(
    wal_results: Vec<SearchResult>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
    wal_deleted_ids: &HashSet<String>,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            let wal_ids: HashSet<String> = wal_results.iter().map(|r| r.id.clone()).collect();
            let mut merged: Vec<SearchResult> = wal_results;

            for sr in segment_results {
                // Exclude if WAL has a newer version OR if explicitly deleted
                if !wal_ids.contains(&sr.id) && !wal_deleted_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            // Sort DESCENDING (higher BM25 score = more relevant)
            merged.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            merged.truncate(top_k);
            merged
        }
        ConsistencyLevel::Eventual => {
            let mut results = segment_results;
            results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(top_k);
            results
        }
    }
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
            let wal_ids: HashSet<String> = wal_results.iter().map(|r| r.id.clone()).collect();
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
