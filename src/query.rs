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
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;
use crate::wal::WalReader;

/// Parameters for a vector query, grouped to avoid excessive function arguments.
pub struct QueryParams<'a> {
    pub store: &'a ZeppelinStore,
    pub wal_reader: &'a WalReader,
    pub namespace: &'a str,
    pub query: &'a [f32],
    pub top_k: usize,
    pub nprobe: usize,
    pub filter: Option<&'a Filter>,
    pub consistency: ConsistencyLevel,
    pub distance_metric: DistanceMetric,
    pub oversample_factor: usize,
    pub cache: Option<&'a Arc<DiskCache>>,
}

/// Execute a query against a namespace, combining WAL scan and segment search.
#[instrument(skip(params), fields(namespace = params.namespace))]
pub async fn execute_query(params: QueryParams<'_>) -> Result<QueryResponse> {
    let QueryParams {
        store,
        wal_reader,
        namespace,
        query,
        top_k,
        nprobe,
        filter,
        consistency,
        distance_metric,
        oversample_factor,
        cache,
    } = params;
    let manifest = Manifest::read(store, namespace).await?.unwrap_or_default();

    let mut scanned_fragments = 0;
    let mut scanned_segments = 0;

    // WAL scan (always for Strong, never for Eventual)
    // Uses the manifest we already read for snapshot consistency — avoids re-reading
    // a newer manifest whose fragments may have been deleted by compaction.
    // Short-circuit: skip WAL scan if no uncompacted fragments exist.
    let wal_start = std::time::Instant::now();
    let wal_results = match consistency {
        ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
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
        _ => Vec::new(),
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
        // Look up the full SegmentRef from the manifest.
        let segment_ref = manifest
            .segments
            .iter()
            .find(|s| s.id == *segment_id)
            .cloned();
        if let Some(seg_ref) = segment_ref {
            let results = segment_search(
                store,
                namespace,
                &seg_ref,
                query,
                top_k,
                nprobe,
                filter,
                distance_metric,
                oversample_factor,
                cache,
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
/// Uses `SegmentRef` metadata to determine index type (hierarchical vs flat)
/// without probing S3, and loads the IVF-Flat index with pre-known metadata
/// to skip cluster-count probing and quantization detection.
#[allow(clippy::too_many_arguments)]
async fn segment_search(
    store: &ZeppelinStore,
    namespace: &str,
    segment_ref: &SegmentRef,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    oversample_factor: usize,
    cache: Option<&Arc<DiskCache>>,
) -> Result<Vec<SearchResult>> {
    let segment_id = &segment_ref.id;

    // Use manifest metadata to determine index type — no S3 probe needed.
    if segment_ref.hierarchical {
        let mut index = HierarchicalIndex::load(store, namespace, segment_id).await?;
        index.bitmap_fields = segment_ref.bitmap_fields.clone();
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

    // Use manifest metadata to skip cluster-count probing and quant detection.
    let mut index = IvfFlatIndex::load_from_manifest(
        store,
        namespace,
        segment_id,
        segment_ref.vector_count,
        segment_ref.quantization,
    )
    .await?;
    index.bitmap_fields = segment_ref.bitmap_fields.clone();
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
    // Short-circuit: skip WAL scan if no uncompacted fragments exist.
    let wal_start = std::time::Instant::now();
    let mut wal_deleted_ids = std::collections::HashSet::new();
    let wal_results = match consistency {
        ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
            let refs = manifest.uncompacted_fragments().to_vec();
            let fragments = wal_reader
                .read_fragments_from_refs(namespace, &refs)
                .await?;
            let scan_result = wal_bm25_scan(&fragments, rank_by, fts_configs, last_as_prefix, None, Some(top_k));
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
        _ => Vec::new(),
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
        let segment_ref = manifest
            .segments
            .iter()
            .find(|s| s.id == *segment_id)
            .cloned();

        if let Some(seg_ref) = segment_ref {
            if !seg_ref.fts_fields.is_empty() {
                let results = segment_bm25_search(
                    store,
                    namespace,
                    &seg_ref,
                    rank_by,
                    fts_configs,
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
    segment_ref: &SegmentRef,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
) -> Result<Vec<SearchResult>> {
    use crate::index::ivf_flat::build::{attrs_key, deserialize_attrs};

    let segment_id = &segment_ref.id;
    let fts_fields = &segment_ref.fts_fields;

    // Load the IVF-Flat index using manifest metadata to skip cluster probing.
    let index = IvfFlatIndex::load_from_manifest(
        store,
        namespace,
        segment_id,
        segment_ref.vector_count,
        segment_ref.quantization,
    )
    .await?;
    let num_clusters = index.num_clusters();

    let field_queries = rank_by.extract_field_queries();
    let mut all_results: HashMap<
        String,
        (f32, Option<HashMap<String, crate::types::AttributeValue>>),
    > = HashMap::new();

    // Parallel prefetch all cluster data (fts index, attrs, cluster vectors).
    let prefetched = futures::future::join_all((0..num_clusters).map(|cluster_idx| {
        let fts_key = fts_index_key(namespace, segment_id, cluster_idx);
        let akey = attrs_key(namespace, segment_id, cluster_idx);
        let ckey = crate::index::ivf_flat::build::cluster_key(namespace, segment_id, cluster_idx);
        async move {
            let (fts_res, attrs_res, cluster_res) =
                tokio::join!(store.get(&fts_key), store.get(&akey), store.get(&ckey),);
            (cluster_idx, fts_res, attrs_res, cluster_res)
        }
    }))
    .await;

    // Process prefetched results — CPU-bound, no I/O.
    for (_cluster_idx, fts_res, attrs_res, cluster_res) in prefetched {
        let fts_data = match fts_res {
            Ok(data) => data,
            Err(crate::error::ZeppelinError::NotFound { .. }) => continue,
            Err(e) => return Err(e),
        };

        let inv_index = InvertedIndex::from_bytes(&fts_data)?;

        let attrs_data = match attrs_res {
            Ok(data) => data,
            Err(_) => continue,
        };
        let cluster_attrs = deserialize_attrs(&attrs_data)?;

        let cluster_data = match cluster_res {
            Ok(data) => data,
            Err(e) => return Err(e),
        };
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
