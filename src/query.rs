use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use tracing::{debug, instrument};

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::error::Result;
use crate::fts::bm25::Bm25Params;
use crate::fts::inverted_index::{fts_index_key, InvertedIndex};
use crate::fts::rank_by::{evaluate_rank_by, RankBy};
use crate::fts::tokenizer::tokenize_text;
use crate::fts::wal_cache::WalFtsCache;
use crate::fts::wal_scan::wal_bm25_scan;
use crate::fts::FtsFieldConfig;
use crate::index::distance::compute_distance;
use crate::index::filter::evaluate_filter;
use crate::index::topk::{partial_topk_by, TopK};
use crate::index::HierarchicalIndex;
use crate::index::IvfFlatIndex;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult};
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;
use crate::wal::WalReader;

/// Query result containing ranked search results and scan statistics.
///
/// Serialized directly as the HTTP query response body.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Ranked search results ordered by relevance.
    pub results: Vec<SearchResult>,
    /// Number of WAL fragments scanned during the query.
    pub scanned_fragments: usize,
    /// Number of compacted segments scanned during the query.
    pub scanned_segments: usize,
}

/// Parameters for a vector query, grouped to avoid excessive function arguments.
pub struct QueryParams<'a> {
    /// S3 storage backend.
    pub store: &'a ZeppelinStore,
    /// WAL fragment reader.
    pub wal_reader: &'a WalReader,
    /// Target namespace name.
    pub namespace: &'a str,
    /// Query vector.
    pub query: &'a [f32],
    /// Maximum number of results to return.
    pub top_k: usize,
    /// Number of IVF clusters to probe.
    pub nprobe: usize,
    /// Optional attribute filter.
    pub filter: Option<&'a Filter>,
    /// Read consistency level (strong or eventual).
    pub consistency: ConsistencyLevel,
    /// Distance metric for scoring.
    pub distance_metric: DistanceMetric,
    /// Oversample multiplier for filtered queries.
    pub oversample_factor: usize,
    /// Maximum gap, in bytes, between rerank vector ranges merged into one GET.
    pub rerank_coalesce_gap_bytes: usize,
    /// Optional disk cache for cluster data.
    pub cache: Option<&'a Arc<DiskCache>>,
    /// Optional manifest cache to avoid redundant S3 reads.
    pub manifest_cache: Option<&'a Arc<ManifestCache>>,
    /// Whether attributes should be included in returned results.
    pub include_attributes: bool,
}

fn distance_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id))
}

fn bm25_result_cmp(a: &SearchResult, b: &SearchResult) -> Ordering {
    b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id))
}

#[cfg(test)]
static WAL_ATTR_CLONES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn clone_wal_result_attrs(
    attrs: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    #[cfg(test)]
    WAL_ATTR_CLONES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    attrs.clone()
}

pub(crate) async fn read_manifest_for_query(
    store: &ZeppelinStore,
    namespace: &str,
    consistency: ConsistencyLevel,
    manifest_cache: Option<&Arc<ManifestCache>>,
) -> Result<Manifest> {
    match manifest_cache {
        Some(mc) => match consistency {
            ConsistencyLevel::Strong => mc.get_strong(store, namespace).await,
            ConsistencyLevel::Eventual => mc.get(store, namespace).await,
        },
        None => Ok(Manifest::read(store, namespace).await?.unwrap_or_default()),
    }
}

/// Execute a query against a namespace, combining WAL scan and segment search.
#[instrument(skip(params), fields(namespace = params.namespace))]
pub async fn execute_query(params: QueryParams<'_>) -> Result<QueryResponse> {
    let manifest = read_manifest_for_query(
        params.store,
        params.namespace,
        params.consistency,
        params.manifest_cache,
    )
    .await?;
    execute_query_with_manifest(params, manifest).await
}

/// Execute a vector query against an already-read manifest snapshot.
///
/// Batch query uses this to share one manifest freshness check across entries.
pub(crate) async fn execute_query_with_manifest(
    params: QueryParams<'_>,
    manifest: Manifest,
) -> Result<QueryResponse> {
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
        rerank_coalesce_gap_bytes,
        cache,
        manifest_cache: _,
        include_attributes,
    } = params;

    // WAL work and segment search are independent — they share only the
    // manifest snapshot — so run them concurrently. Strong scans and scores
    // WAL vectors. Eventual skips WAL vector scoring but still reads
    // tombstones, because deletes are correctness rather than freshness.
    let wal_future = async {
        // Short-circuit: skip WAL scan if no uncompacted fragments exist.
        let wal_start = std::time::Instant::now();
        let scan_result = match consistency {
            ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
                wal_scan(
                    wal_reader,
                    namespace,
                    &manifest,
                    query,
                    filter,
                    distance_metric,
                    cache,
                    include_attributes,
                    top_k,
                )
                .await?
            }
            ConsistencyLevel::Eventual if !manifest.uncompacted_fragments().is_empty() => {
                let deleted_ids = wal_reader
                    .read_delete_ids_from_refs_unchecked(
                        namespace,
                        manifest.uncompacted_fragments(),
                        cache,
                    )
                    .await?;
                WalScanResult {
                    results: Vec::new(),
                    overriding_ids: HashSet::new(),
                    fragment_count: 0,
                    deleted_ids,
                }
            }
            _ => WalScanResult {
                results: Vec::new(),
                overriding_ids: HashSet::new(),
                fragment_count: 0,
                deleted_ids: HashSet::new(),
            },
        };
        debug!(
            wal_duration_ms = wal_start.elapsed().as_millis() as u64,
            fragments_scanned = scan_result.fragment_count,
            "query phase: WAL scan"
        );
        Ok::<_, crate::error::ZeppelinError>(scan_result)
    };

    let segment_future = async {
        let segment_start = std::time::Instant::now();
        // Look up the full SegmentRef from the manifest.
        let segment_ref = manifest.active_segment.as_ref().and_then(|segment_id| {
            manifest
                .segments
                .iter()
                .find(|s| s.id == *segment_id)
                .cloned()
        });
        let (results, scanned) = if let Some(seg_ref) = segment_ref {
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
                rerank_coalesce_gap_bytes,
                cache,
                include_attributes,
            )
            .await?;
            (results, 1)
        } else {
            (Vec::new(), 0)
        };
        debug!(
            segment_duration_ms = segment_start.elapsed().as_millis() as u64,
            segments_scanned = scanned,
            "query phase: segment search"
        );
        Ok::<_, crate::error::ZeppelinError>((results, scanned))
    };

    let (wal_result, segment_result) = tokio::join!(wal_future, segment_future);
    let WalScanResult {
        results: wal_results,
        overriding_ids: wal_overriding_ids,
        fragment_count: scanned_fragments,
        deleted_ids: wal_deleted_ids,
    } = wal_result?;
    let (segment_results, scanned_segments) = segment_result?;

    // Merge results — pass WAL tombstones so segment results for deleted
    // vectors are excluded (a delete of an already-compacted vector exists
    // only as a WAL tombstone until the next compaction).
    let merge_start = std::time::Instant::now();
    let results = merge_results(
        wal_results,
        &wal_overriding_ids,
        segment_results,
        top_k,
        consistency,
        &wal_deleted_ids,
    );
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

/// Result of an ANN WAL scan.
struct WalScanResult {
    /// Top-k scored search results, sorted ascending by distance.
    results: Vec<SearchResult>,
    /// All live WAL IDs after dedup/delete processing, including IDs that
    /// filter out or rank outside top-k. These suppress stale segment hits.
    overriding_ids: HashSet<String>,
    /// Number of WAL fragments scanned.
    fragment_count: usize,
    /// IDs that were explicitly deleted in the WAL.
    /// Used by the merge step to exclude these from segment results.
    deleted_ids: HashSet<String>,
}

/// Scan all uncompacted WAL fragments, deduplicate, apply deletes, score, and filter.
/// Reads fragments from the provided manifest snapshot (not re-reading manifest from S3).
#[allow(clippy::too_many_arguments)]
async fn wal_scan(
    wal_reader: &WalReader,
    namespace: &str,
    manifest: &Manifest,
    query: &[f32],
    filter: Option<&Filter>,
    distance_metric: DistanceMetric,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
    top_k: usize,
) -> Result<WalScanResult> {
    let refs = manifest.uncompacted_fragments().to_vec();
    // Skip checksum validation on query reads — fragments were already
    // validated on write. Saves ~11% CPU on fragment deserialization.
    let fragments = wal_reader
        .read_fragments_from_refs_unchecked(namespace, &refs, cache)
        .await?;
    let frag_count = fragments.len();

    if fragments.is_empty() {
        return Ok(WalScanResult {
            results: Vec::new(),
            overriding_ids: HashSet::new(),
            fragment_count: 0,
            deleted_ids: HashSet::new(),
        });
    }

    // Collect all delete tombstones
    let mut deleted_ids: HashSet<String> = HashSet::new();
    // Latest vector state per ID (latest fragment wins), borrowed from the
    // fragment batch so vector payloads and attrs are not cloned before top-k.
    #[allow(clippy::type_complexity)]
    let mut latest_vectors: HashMap<
        &str,
        (
            &[f32],
            Option<&HashMap<String, crate::types::AttributeValue>>,
        ),
    > = HashMap::new();

    // Process fragments in ULID order (oldest first, so later overwrites earlier)
    for fragment in &fragments {
        for del_id in &fragment.deletes {
            deleted_ids.insert(del_id.clone());
            latest_vectors.remove(del_id.as_str());
        }
        for vec in &fragment.vectors {
            deleted_ids.remove(&vec.id);
            latest_vectors.insert(
                vec.id.as_str(),
                (vec.values.as_slice(), vec.attributes.as_ref()),
            );
        }
    }

    struct ScoredWalVector<'a> {
        id: &'a str,
        score: f32,
        attrs: Option<&'a HashMap<String, AttributeValue>>,
    }

    let mut overriding_ids = HashSet::with_capacity(latest_vectors.len());
    let mut top_results = TopK::new(top_k, |a: &ScoredWalVector<'_>, b: &ScoredWalVector<'_>| {
        a.score.total_cmp(&b.score).then_with(|| a.id.cmp(b.id))
    });

    // Score surviving vectors, but keep only the bounded top-k candidates.
    for (id, (values, attrs)) in &latest_vectors {
        overriding_ids.insert((*id).to_string());
        let passes_filter = {
            if let Some(f) = filter {
                match attrs {
                    Some(a) => evaluate_filter(f, a),
                    None => false,
                }
            } else {
                true
            }
        };
        if !passes_filter {
            continue;
        }

        let score = compute_distance(query, values, distance_metric);
        top_results.push(ScoredWalVector {
            id,
            score,
            attrs: *attrs,
        });
    }

    let results: Vec<SearchResult> = top_results
        .into_sorted_vec()
        .into_iter()
        .map(|scored| SearchResult {
            id: scored.id.to_string(),
            score: scored.score,
            attributes: if include_attributes {
                scored.attrs.map(clone_wal_result_attrs)
            } else {
                None
            },
        })
        .collect();

    debug!(
        surviving_vectors = overriding_ids.len(),
        topk_returned = results.len(),
        total_fragments = frag_count,
        "WAL scan complete"
    );

    Ok(WalScanResult {
        results,
        overriding_ids,
        fragment_count: frag_count,
        deleted_ids,
    })
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
    rerank_coalesce_gap_bytes: usize,
    cache: Option<&Arc<DiskCache>>,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    let segment_id = &segment_ref.id;

    // Use manifest metadata to determine index type — no S3 probe needed.
    if segment_ref.hierarchical {
        let mut index = HierarchicalIndex::load(store, namespace, segment_id, cache).await?;
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
            include_attributes,
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
        segment_ref.cluster_owners.clone(),
        segment_ref.cluster_objects.clone(),
        segment_ref.sketch.clone(),
        segment_ref.bootstrap.clone(),
        cache,
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
        include_attributes,
        rerank_coalesce_gap_bytes,
    )
    .await?;

    Ok(results)
}

/// Execute a BM25 full-text search query against a namespace.
///
/// Combines WAL brute-force scan with segment inverted index search.
#[allow(clippy::too_many_arguments)]
#[instrument(
    skip(
        store,
        wal_reader,
        rank_by,
        fts_configs,
        filter,
        manifest_cache,
        fts_cache,
        cache
    ),
    fields(namespace = namespace)
)]
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
    manifest_cache: Option<&Arc<ManifestCache>>,
    fts_cache: Option<&Arc<WalFtsCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    include_attributes: bool,
) -> Result<QueryResponse> {
    let manifest = read_manifest_for_query(store, namespace, consistency, manifest_cache).await?;
    execute_bm25_query_with_manifest(
        store,
        wal_reader,
        namespace,
        rank_by,
        fts_configs,
        top_k,
        filter,
        consistency,
        last_as_prefix,
        fts_cache,
        cache,
        max_full_scan_clusters,
        include_attributes,
        manifest,
    )
    .await
}

/// Execute a BM25 query against an already-read manifest snapshot.
///
/// Batch query uses this to share one manifest freshness check across entries.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_bm25_query_with_manifest(
    store: &ZeppelinStore,
    wal_reader: &WalReader,
    namespace: &str,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    top_k: usize,
    filter: Option<&Filter>,
    consistency: ConsistencyLevel,
    last_as_prefix: bool,
    fts_cache: Option<&Arc<WalFtsCache>>,
    cache: Option<&Arc<DiskCache>>,
    max_full_scan_clusters: usize,
    include_attributes: bool,
    manifest: Manifest,
) -> Result<QueryResponse> {
    // Evict compacted fragments from the FTS cache to prevent unbounded growth
    if let Some(cache) = fts_cache {
        let active_ids: Vec<ulid::Ulid> = manifest
            .uncompacted_fragments()
            .iter()
            .map(|f| f.id)
            .collect();
        cache.evict_compacted(&active_ids);
    }

    // WAL BM25 work and segment BM25 search are independent — run them
    // concurrently over the same manifest snapshot. Strong scans and scores
    // WAL docs; Eventual reads only WAL tombstones.
    let wal_future = async {
        // Short-circuit: skip WAL scan if no uncompacted fragments exist.
        let wal_start = std::time::Instant::now();
        let mut scanned_fragments = 0;
        let mut wal_deleted_ids = std::collections::HashSet::new();
        let mut wal_overriding_ids = std::collections::HashSet::new();
        let wal_results = match consistency {
            ConsistencyLevel::Strong if !manifest.uncompacted_fragments().is_empty() => {
                let refs = manifest.uncompacted_fragments().to_vec();
                // Skip checksum validation — already validated on write.
                let fragments = wal_reader
                    .read_fragments_from_refs_unchecked(namespace, &refs, cache)
                    .await?;
                let scan_result = wal_bm25_scan(
                    &fragments,
                    rank_by,
                    fts_configs,
                    last_as_prefix,
                    fts_cache.map(|c| c.as_ref()),
                    Some(top_k),
                );
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
                if !include_attributes {
                    for result in &mut results {
                        result.attributes = None;
                    }
                }
                wal_overriding_ids = results.iter().map(|r| r.id.clone()).collect();
                results
            }
            ConsistencyLevel::Eventual if !manifest.uncompacted_fragments().is_empty() => {
                wal_deleted_ids = wal_reader
                    .read_delete_ids_from_refs_unchecked(
                        namespace,
                        manifest.uncompacted_fragments(),
                        cache,
                    )
                    .await?;
                Vec::new()
            }
            _ => Vec::new(),
        };
        debug!(
            wal_duration_ms = wal_start.elapsed().as_millis() as u64,
            fragments_scanned = scanned_fragments,
            "BM25 query phase: WAL scan"
        );
        Ok::<_, crate::error::ZeppelinError>((
            wal_results,
            scanned_fragments,
            wal_deleted_ids,
            wal_overriding_ids,
        ))
    };

    let segment_future = async {
        let segment_start = std::time::Instant::now();
        let segment_ref = manifest.active_segment.as_ref().and_then(|segment_id| {
            manifest
                .segments
                .iter()
                .find(|s| s.id == *segment_id)
                .cloned()
        });
        let (results, scanned) = match segment_ref {
            Some(seg_ref) if !seg_ref.fts_fields.is_empty() => {
                let segment_top_k = if manifest.uncompacted_fragments().is_empty() {
                    top_k
                } else {
                    usize::MAX
                };
                let results = segment_bm25_search(
                    store,
                    namespace,
                    &seg_ref,
                    rank_by,
                    fts_configs,
                    filter,
                    last_as_prefix,
                    max_full_scan_clusters,
                    segment_top_k,
                    include_attributes,
                )
                .await?;
                (results, 1)
            }
            _ => (Vec::new(), 0),
        };
        debug!(
            segment_duration_ms = segment_start.elapsed().as_millis() as u64,
            segments_scanned = scanned,
            "BM25 query phase: segment search"
        );
        Ok::<_, crate::error::ZeppelinError>((results, scanned))
    };

    let (wal_result, segment_result) = tokio::join!(wal_future, segment_future);
    let (wal_results, scanned_fragments, wal_deleted_ids, wal_overriding_ids) = wal_result?;
    let (segment_results, scanned_segments) = segment_result?;

    // Merge results — BM25 is higher-is-better
    // Pass deleted IDs so segment results for deleted docs are excluded
    let mut results = merge_bm25_results(
        wal_results,
        &wal_overriding_ids,
        segment_results,
        top_k,
        consistency,
        &wal_deleted_ids,
    );
    if !include_attributes {
        for result in &mut results {
            result.attributes = None;
        }
    }

    Ok(QueryResponse {
        results,
        scanned_fragments,
        scanned_segments,
    })
}

/// Search a segment's inverted indexes for a BM25 query.
///
/// Uses the global FTS index when available (1 S3 GET instead of N),
/// falling back to full per-cluster scan for older segments.
///
/// When `max_full_scan_clusters > 0` and the segment lacks a global FTS index,
/// returns an error if the cluster count exceeds the limit (circuit breaker).
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search(
    store: &ZeppelinStore,
    namespace: &str,
    segment_ref: &SegmentRef,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    max_full_scan_clusters: usize,
    top_k: usize,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    if segment_ref.has_global_fts {
        return segment_bm25_search_global(
            store,
            namespace,
            segment_ref,
            rank_by,
            fts_configs,
            filter,
            last_as_prefix,
            top_k,
            include_attributes,
        )
        .await;
    }
    // Circuit breaker: reject full-scan if cluster count exceeds limit
    if max_full_scan_clusters > 0 && segment_ref.cluster_count > max_full_scan_clusters {
        return Err(crate::error::ZeppelinError::Validation(format!(
            "BM25 query on segment {} requires full scan of {} clusters (limit: {}). \
             Recompact with fts_index=true.",
            segment_ref.id, segment_ref.cluster_count, max_full_scan_clusters
        )));
    }
    tracing::warn!(
        namespace = namespace,
        segment_id = %segment_ref.id,
        cluster_count = segment_ref.cluster_count,
        "BM25 falling back to full cluster scan — segment missing global FTS index. \
         Recompact with fts_index=true for 10-100x faster BM25 queries."
    );
    segment_bm25_search_full_scan(
        store,
        namespace,
        segment_ref,
        rank_by,
        fts_configs,
        filter,
        last_as_prefix,
        top_k,
        include_attributes,
    )
    .await
}

/// BM25 search using the global FTS index (fast path).
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search_global(
    store: &ZeppelinStore,
    namespace: &str,
    segment_ref: &SegmentRef,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    top_k: usize,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    use crate::fts::bm25::Bm25Params;
    use crate::fts::global_index::{global_fts_key, GlobalInvertedIndex};
    use crate::fts::rank_by::evaluate_rank_by;

    let segment_id = &segment_ref.id;

    // Load global FTS index (1 S3 GET, ~50KB)
    let gkey = global_fts_key(namespace, segment_id);
    let global_data = store.get(&gkey).await?;
    let global_index = GlobalInvertedIndex::from_bytes(&global_data)?;

    let field_queries = rank_by.extract_field_queries();

    // Score documents via global index
    let mut position_field_scores: HashMap<(u16, u32), HashMap<String, f32>> = HashMap::new();

    for (field, query) in &field_queries {
        let config = match fts_configs.get(field.as_str()) {
            Some(c) => c,
            None => continue,
        };

        let query_tokens = tokenize_text(query, config, last_as_prefix);
        let params = Bm25Params {
            k1: config.k1,
            b: config.b,
        };

        let results = if last_as_prefix {
            global_index.search_prefix(field, &query_tokens, &params)
        } else {
            global_index.search(field, &query_tokens, &params)
        };

        for (cluster_idx, position, score) in results {
            let entry = position_field_scores
                .entry((cluster_idx, position))
                .or_default();
            *entry.entry(field.to_string()).or_insert(0.0) += score;
        }
    }

    if position_field_scores.is_empty() {
        return Ok(Vec::new());
    }

    // Identify which clusters we need to fetch attrs from
    let needed_clusters: HashSet<u16> = position_field_scores.keys().map(|(c, _)| *c).collect();

    let load_attrs = filter.is_some() || include_attributes;
    let cluster_data = fetch_bm25_cluster_attrs_and_ids(
        store,
        namespace,
        segment_ref,
        &needed_clusters,
        load_attrs,
    )
    .await?;

    // Collect results
    let mut all_results: HashMap<
        String,
        (f32, Option<HashMap<String, crate::types::AttributeValue>>),
    > = HashMap::new();

    for ((cluster_idx, position), field_scores) in position_field_scores {
        let final_score = evaluate_rank_by(rank_by, &field_scores);
        if final_score <= 0.0 {
            continue;
        }

        let (attrs, ids) = match cluster_data.get(&cluster_idx) {
            Some(data) => data,
            None => continue,
        };

        let pos = position as usize;
        if pos >= ids.len() {
            continue;
        }

        let id = ids[pos].clone();
        let attr = attrs
            .as_ref()
            .and_then(|cluster_attrs| cluster_attrs.get(pos))
            .cloned()
            .flatten();

        // Apply post-filter
        if let Some(f) = filter {
            match &attr {
                Some(a) => {
                    if !evaluate_filter(f, a) {
                        continue;
                    }
                }
                None => continue,
            }
        }

        let response_attr = if include_attributes { attr } else { None };
        let entry = all_results
            .entry(id)
            .or_insert((0.0, response_attr.clone()));
        if final_score > entry.0 {
            entry.0 = final_score;
            entry.1 = response_attr;
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

    partial_topk_by(&mut results, top_k, bm25_result_cmp);

    Ok(results)
}

#[allow(clippy::type_complexity)]
async fn fetch_bm25_cluster_attrs_and_ids(
    store: &ZeppelinStore,
    namespace: &str,
    segment_ref: &SegmentRef,
    needed_clusters: &HashSet<u16>,
    load_attrs: bool,
) -> Result<
    HashMap<
        u16,
        (
            Option<Vec<Option<HashMap<String, AttributeValue>>>>,
            Vec<String>,
        ),
    >,
> {
    use crate::index::ivf_flat::build::{
        attrs_key, cluster_key, cluster_object_sections, deserialize_attrs, deserialize_cluster,
        deserialize_cluster_from_object,
    };

    if segment_ref.cluster_objects.is_empty() {
        let cluster_attrs_results =
            futures::future::join_all(needed_clusters.iter().map(|&cluster_idx| {
                let owner = segment_ref.cluster_owner(cluster_idx as usize);
                let akey = attrs_key(namespace, owner, cluster_idx as usize);
                let ckey = cluster_key(namespace, owner, cluster_idx as usize);
                async move {
                    if load_attrs {
                        let (attrs_res, cluster_res) =
                            tokio::join!(store.get(&akey), store.get(&ckey));
                        (cluster_idx, Some(attrs_res), cluster_res)
                    } else {
                        (cluster_idx, None, store.get(&ckey).await)
                    }
                }
            }))
            .await;

        let mut cluster_data = HashMap::new();
        for (cluster_idx, attrs_res, cluster_res) in cluster_attrs_results {
            let attrs = match attrs_res {
                Some(Ok(data)) => Some(deserialize_attrs(&data)?),
                Some(Err(_)) => continue,
                None => None,
            };
            let cluster = match cluster_res {
                Ok(data) => deserialize_cluster(&data)?,
                Err(_) => continue,
            };
            cluster_data.insert(cluster_idx, (attrs, cluster.ids));
        }
        return Ok(cluster_data);
    }

    let mut attrs_by_cluster = HashMap::new();
    if load_attrs {
        let attrs_results = futures::future::join_all(needed_clusters.iter().map(|&cluster_idx| {
            let owner = segment_ref.cluster_owner(cluster_idx as usize);
            let akey = attrs_key(namespace, owner, cluster_idx as usize);
            async move { (cluster_idx, store.get(&akey).await) }
        }))
        .await;
        for (cluster_idx, attrs_res) in attrs_results {
            let attrs = match attrs_res {
                Ok(data) => deserialize_attrs(&data)?,
                Err(_) => continue,
            };
            attrs_by_cluster.insert(cluster_idx, attrs);
        }
    }

    let object_fetches = segment_ref.cluster_objects.iter().filter_map(|object_ref| {
        let clusters: Vec<u16> = object_ref
            .clusters
            .iter()
            .copied()
            .filter_map(|cluster_idx| {
                u16::try_from(cluster_idx)
                    .ok()
                    .filter(|idx| needed_clusters.contains(idx))
            })
            .collect();
        (!clusters.is_empty()).then_some((object_ref.key.as_str(), clusters))
    });

    let object_results = futures::future::join_all(
        object_fetches.map(|(key, clusters)| async move { (clusters, store.get(key).await) }),
    )
    .await;

    let mut cluster_data = HashMap::new();
    for (clusters, object_res) in object_results {
        let object_data = match object_res {
            Ok(data) => data,
            Err(_) => continue,
        };
        if cluster_object_sections(&object_data)?.is_some() {
            for cluster_idx in clusters {
                let attrs = if load_attrs {
                    let Some(attrs) = attrs_by_cluster.remove(&cluster_idx) else {
                        continue;
                    };
                    Some(attrs)
                } else {
                    None
                };
                let cluster = deserialize_cluster_from_object(&object_data, cluster_idx as usize)?;
                cluster_data.insert(cluster_idx, (attrs, cluster.ids));
            }
        } else {
            if clusters.len() != 1 {
                return Err(crate::error::ZeppelinError::Index(format!(
                    "legacy cluster object must reference exactly one cluster, got {}",
                    clusters.len()
                )));
            }
            let cluster_idx = clusters[0];
            let attrs = if load_attrs {
                let Some(attrs) = attrs_by_cluster.remove(&cluster_idx) else {
                    continue;
                };
                Some(attrs)
            } else {
                None
            };
            let cluster = deserialize_cluster(&object_data)?;
            cluster_data.insert(cluster_idx, (attrs, cluster.ids));
        }
    }

    Ok(cluster_data)
}

/// BM25 search using full per-cluster scan (backward compat fallback).
///
/// At 1M scale this is O(N × clusters) and can take 15+ seconds.
/// Segments should be recompacted with `fts_index=true` to build a
/// global FTS index, which reduces BM25 queries to 1 S3 GET.
#[allow(clippy::too_many_arguments)]
async fn segment_bm25_search_full_scan(
    store: &ZeppelinStore,
    namespace: &str,
    segment_ref: &SegmentRef,
    rank_by: &RankBy,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    filter: Option<&Filter>,
    last_as_prefix: bool,
    top_k: usize,
    include_attributes: bool,
) -> Result<Vec<SearchResult>> {
    use crate::index::ivf_flat::build::{attrs_key, deserialize_attrs};

    let segment_id = &segment_ref.id;
    let fts_fields = &segment_ref.fts_fields;

    // Load the IVF-Flat index using manifest metadata to skip cluster probing.
    // BM25 full-scan is a cold fallback path — no cache handle is threaded here.
    let index = IvfFlatIndex::load_from_manifest(
        store,
        namespace,
        segment_id,
        segment_ref.vector_count,
        segment_ref.quantization,
        segment_ref.cluster_owners.clone(),
        segment_ref.cluster_objects.clone(),
        None,
        None,
        None,
    )
    .await?;
    let num_clusters = index.num_clusters();

    let field_queries = rank_by.extract_field_queries();
    let load_attrs = filter.is_some() || include_attributes;
    let mut all_results: HashMap<String, (f32, Option<HashMap<String, AttributeValue>>)> =
        HashMap::new();

    // Parallel prefetch all cluster data (fts index, attrs, cluster vectors).
    let prefetched = futures::future::join_all((0..num_clusters).map(|cluster_idx| {
        // Per-cluster keys route through cluster_owner(): a carried-over
        // cluster's objects live under an older segment's ID.
        let owner = segment_ref.cluster_owner(cluster_idx);
        let fts_key = fts_index_key(namespace, owner, cluster_idx);
        let akey = attrs_key(namespace, owner, cluster_idx);
        let ckey = crate::index::ivf_flat::build::cluster_key(namespace, owner, cluster_idx);
        async move {
            if load_attrs {
                let (fts_res, attrs_res, cluster_res) =
                    tokio::join!(store.get(&fts_key), store.get(&akey), store.get(&ckey),);
                (cluster_idx, fts_res, Some(attrs_res), cluster_res)
            } else {
                let (fts_res, cluster_res) = tokio::join!(store.get(&fts_key), store.get(&ckey),);
                (cluster_idx, fts_res, None, cluster_res)
            }
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

        let cluster_attrs = match attrs_res {
            Some(Ok(data)) => Some(deserialize_attrs(&data)?),
            Some(Err(_)) => continue,
            None => None,
        };

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
            let attrs = cluster_attrs
                .as_ref()
                .and_then(|cluster_attrs| cluster_attrs.get(pos_usize))
                .cloned()
                .flatten();

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
            let response_attrs = if include_attributes { attrs } else { None };
            let entry = all_results
                .entry(id.clone())
                .or_insert((0.0, response_attrs.clone()));
            if final_score > entry.0 {
                entry.0 = final_score;
                entry.1 = response_attrs;
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

    partial_topk_by(&mut results, top_k, bm25_result_cmp);

    Ok(results)
}

/// Merge BM25 WAL and segment results (higher score = better).
/// `wal_deleted_ids` contains IDs explicitly deleted in the WAL — these must
/// not appear in the final results even if they exist in the segment.
fn merge_bm25_results(
    wal_results: Vec<SearchResult>,
    wal_overriding_ids: &HashSet<String>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
    wal_deleted_ids: &HashSet<String>,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            let mut merged = TopK::new(
                top_k,
                bm25_result_cmp as fn(&SearchResult, &SearchResult) -> Ordering,
            );

            for sr in wal_results {
                merged.push(sr);
            }
            for sr in segment_results {
                // Exclude if WAL has a newer version OR if explicitly deleted
                if !wal_overriding_ids.contains(&sr.id) && !wal_deleted_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            merged.into_sorted_vec()
        }
        ConsistencyLevel::Eventual => {
            let mut results: Vec<SearchResult> = segment_results
                .into_iter()
                .filter(|sr| !wal_deleted_ids.contains(&sr.id))
                .collect();
            partial_topk_by(&mut results, top_k, bm25_result_cmp);
            results
        }
    }
}

/// Merge WAL results and segment results.
///
/// For Strong consistency: filter segment results to remove any IDs that were
/// deleted or updated in the WAL, then merge both sorted lists and truncate to top_k.
///
/// `wal_deleted_ids` contains IDs explicitly deleted in the WAL — these must
/// be excluded from segment results even though they produce no WAL result
/// (a tombstone for an already-compacted vector has no live WAL entry).
fn merge_results(
    wal_results: Vec<SearchResult>,
    wal_overriding_ids: &HashSet<String>,
    segment_results: Vec<SearchResult>,
    top_k: usize,
    consistency: ConsistencyLevel,
    wal_deleted_ids: &HashSet<String>,
) -> Vec<SearchResult> {
    match consistency {
        ConsistencyLevel::Strong => {
            // WAL results already have the latest state.
            // Remove segment results whose IDs appear in WAL results (WAL is
            // authoritative) or were deleted in the WAL.
            let mut merged = TopK::new(
                top_k,
                distance_result_cmp as fn(&SearchResult, &SearchResult) -> Ordering,
            );

            for sr in wal_results {
                merged.push(sr);
            }
            for sr in segment_results {
                if !wal_overriding_ids.contains(&sr.id) && !wal_deleted_ids.contains(&sr.id) {
                    merged.push(sr);
                }
            }

            merged.into_sorted_vec()
        }
        ConsistencyLevel::Eventual => {
            let mut results: Vec<SearchResult> = segment_results
                .into_iter()
                .filter(|sr| !wal_deleted_ids.contains(&sr.id))
                .collect();
            partial_topk_by(&mut results, top_k, distance_result_cmp);
            results
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::single_match)]
#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(id: &str, score: f32) -> SearchResult {
        SearchResult {
            id: id.to_string(),
            score,
            attributes: None,
        }
    }

    fn wal_vector_with_attrs(id: &str, offset: f32) -> crate::types::VectorEntry {
        let mut attrs = HashMap::new();
        attrs.insert(
            "payload".to_string(),
            AttributeValue::String(format!("payload-{id}")),
        );
        crate::types::VectorEntry {
            id: id.to_string(),
            values: vec![offset, 0.0],
            attributes: Some(attrs),
        }
    }

    #[tokio::test]
    async fn test_wal_scan_materializes_attrs_only_for_returned_topk() {
        let store = crate::storage::ZeppelinStore::new(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        let namespace = "wal-scan-attrs-clone";
        crate::wal::manifest::Manifest::new()
            .write(&store, namespace)
            .await
            .unwrap();

        let vectors: Vec<_> = (0..100)
            .map(|idx| wal_vector_with_attrs(&format!("v_{idx:03}"), idx as f32))
            .collect();
        let (_, manifest) = crate::wal::WalWriter::new(store.clone())
            .append(namespace, vectors, vec![])
            .await
            .unwrap();
        let wal_reader = WalReader::new(store.clone());

        WAL_ATTR_CLONES.store(0, std::sync::atomic::Ordering::Relaxed);
        let without_attrs = wal_scan(
            &wal_reader,
            namespace,
            &manifest,
            &[0.0, 0.0],
            None,
            DistanceMetric::Euclidean,
            None,
            false,
            5,
        )
        .await
        .unwrap();
        assert_eq!(without_attrs.results.len(), 5);
        assert!(without_attrs
            .results
            .iter()
            .all(|result| result.attributes.is_none()));
        assert_eq!(
            WAL_ATTR_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "include_attributes=false must not clone WAL attrs"
        );

        WAL_ATTR_CLONES.store(0, std::sync::atomic::Ordering::Relaxed);
        let with_attrs = wal_scan(
            &wal_reader,
            namespace,
            &manifest,
            &[0.0, 0.0],
            None,
            DistanceMetric::Euclidean,
            None,
            true,
            5,
        )
        .await
        .unwrap();
        assert_eq!(with_attrs.results.len(), 5);
        assert!(with_attrs
            .results
            .iter()
            .all(|result| result.attributes.is_some()));
        assert_eq!(
            WAL_ATTR_CLONES.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "WAL attrs should be cloned only for returned top-k results"
        );
    }

    #[test]
    fn test_merge_strong_excludes_wal_deleted_segment_results() {
        // Vector "compacted" lives only in the segment; its WAL tombstone
        // produces no WAL result. The merge must still drop it.
        let wal_results = vec![make_result("fresh", 0.1)];
        let segment_results = vec![make_result("compacted", 0.2), make_result("kept", 0.3)];
        let deleted: HashSet<String> = ["compacted".to_string()].into_iter().collect();
        let overriding: HashSet<String> = ["fresh".to_string()].into_iter().collect();

        let merged = merge_results(
            wal_results,
            &overriding,
            segment_results,
            10,
            ConsistencyLevel::Strong,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "kept"]);
    }

    #[test]
    fn test_merge_strong_wal_overrides_segment() {
        // Same ID in WAL and segment: WAL version wins, no duplicate.
        let wal_results = vec![make_result("v1", 0.5)];
        let segment_results = vec![make_result("v1", 0.1)];
        let overriding: HashSet<String> = ["v1".to_string()].into_iter().collect();

        let merged = merge_results(
            wal_results,
            &overriding,
            segment_results,
            10,
            ConsistencyLevel::Strong,
            &HashSet::new(),
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].id, "v1");
        assert_eq!(merged[0].score, 0.5);
    }

    #[test]
    fn test_merge_eventual_applies_tombstones() {
        // Eventual skips WAL vector scoring, but deletes are correctness:
        // segment results with WAL tombstones must be excluded immediately.
        let segment_results = vec![make_result("a", 0.1), make_result("b", 0.2)];
        let deleted: HashSet<String> = ["a".to_string()].into_iter().collect();

        let merged = merge_results(
            Vec::new(),
            &HashSet::new(),
            segment_results,
            10,
            ConsistencyLevel::Eventual,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["b"]);
    }

    #[test]
    fn test_merge_bm25_eventual_applies_tombstones() {
        // Same invariant for BM25/rank_by: Eventual segment hits must not
        // resurrect a document deleted in the WAL.
        let segment_results = vec![make_result("a", 2.0), make_result("b", 1.0)];
        let deleted: HashSet<String> = ["a".to_string()].into_iter().collect();

        let merged = merge_bm25_results(
            Vec::new(),
            &HashSet::new(),
            segment_results,
            10,
            ConsistencyLevel::Eventual,
            &deleted,
        );

        let ids: Vec<&str> = merged.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["b"]);
    }

    fn make_segment_ref(cluster_count: usize, has_global_fts: bool) -> SegmentRef {
        SegmentRef {
            id: "seg_test".to_string(),
            vector_count: 10000,
            cluster_count,
            quantization: crate::index::quantization::QuantizationType::None,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: vec!["text".to_string()],
            has_global_fts,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
        }
    }

    #[tokio::test]
    async fn test_bm25_circuit_breaker_rejects_over_limit() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let seg = make_segment_ref(600, false);

        let result = segment_bm25_search(
            &store,
            "ns",
            &seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            500,
            10,
            true,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            crate::error::ZeppelinError::Validation(msg) => {
                assert!(msg.contains("600 clusters"));
                assert!(msg.contains("limit: 500"));
            }
            _ => panic!("expected Validation error, got: {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_bm25_circuit_breaker_allows_under_limit() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let seg = make_segment_ref(400, false);

        // Under limit: should attempt the scan (will fail with NotFound on the index,
        // not with a Validation error)
        let result = segment_bm25_search(
            &store,
            "ns",
            &seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            500,
            10,
            true,
        )
        .await;

        // Should NOT be a Validation error (it'll be NotFound or similar from missing data)
        match &result {
            Err(crate::error::ZeppelinError::Validation(_)) => {
                panic!("should not have triggered circuit breaker");
            }
            _ => {} // Any other result is fine (expected to fail on missing data)
        }
    }

    #[tokio::test]
    async fn test_bm25_circuit_breaker_disabled_when_zero() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = crate::storage::ZeppelinStore::new(mem);
        let rank_by = RankBy::Bm25 {
            field: "text".to_string(),
            query: "hello".to_string(),
        };
        let fts_configs = HashMap::new();
        let seg = make_segment_ref(9999, false);

        // Limit=0 means disabled — should not reject
        let result = segment_bm25_search(
            &store,
            "ns",
            &seg,
            &rank_by,
            &fts_configs,
            None,
            false,
            0,
            10,
            true,
        )
        .await;

        match &result {
            Err(crate::error::ZeppelinError::Validation(_)) => {
                panic!("should not have triggered circuit breaker when limit=0");
            }
            _ => {} // Expected to fail on missing data, not circuit breaker
        }
    }
}
