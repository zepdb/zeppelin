use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::SearchResult;
use zeppelin::wal::Manifest;

/// Assert that an object exists at the given key on S3.
pub async fn assert_s3_object_exists(store: &ZeppelinStore, key: &str) {
    assert!(
        store.exists(key).await.expect("exists check failed"),
        "expected object at key '{key}' to exist"
    );
}

/// Assert that an object does NOT exist at the given key on S3.
pub async fn assert_s3_object_not_exists(store: &ZeppelinStore, key: &str) {
    assert!(
        !store.exists(key).await.expect("exists check failed"),
        "expected object at key '{key}' to NOT exist"
    );
}

/// Assert that an object at the given key has the expected content.
pub async fn assert_s3_object_content(store: &ZeppelinStore, key: &str, expected: &[u8]) {
    let data = store
        .get(key)
        .await
        .unwrap_or_else(|e| panic!("failed to get object at '{key}': {e}"));
    assert_eq!(
        data.as_ref(),
        expected,
        "object content at '{key}' does not match expected"
    );
}

/// Assert that the manifest for a namespace contains a fragment with the given ULID.
pub async fn assert_manifest_contains_fragment(
    store: &ZeppelinStore,
    namespace: &str,
    fragment_id: &ulid::Ulid,
) {
    let manifest = Manifest::read(store, namespace)
        .await
        .expect("failed to read manifest")
        .unwrap_or_else(|| panic!("manifest not found for namespace '{namespace}'"));

    assert!(
        manifest.fragments.iter().any(|f| &f.id == fragment_id),
        "manifest for '{namespace}' does not contain fragment {fragment_id}"
    );
}

/// Assert that the manifest for a namespace contains a segment with the given ID.
pub async fn assert_manifest_contains_segment(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) {
    let manifest = Manifest::read(store, namespace)
        .await
        .expect("failed to read manifest")
        .unwrap_or_else(|| panic!("manifest not found for namespace '{namespace}'"));

    assert!(
        manifest.segments.iter().any(|s| s.id == segment_id),
        "manifest for '{namespace}' does not contain segment {segment_id}"
    );
}

/// Assert that search results contain all expected vector IDs (order-independent).
pub fn assert_search_results_contain(results: &[SearchResult], expected_ids: &[&str]) {
    let result_ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
    for id in expected_ids {
        assert!(
            result_ids.contains(id),
            "search results do not contain expected ID '{id}'. Got: {result_ids:?}"
        );
    }
}

/// Assert that the top result has the expected ID.
pub fn assert_top_result(results: &[SearchResult], expected_id: &str) {
    assert!(
        !results.is_empty(),
        "search results are empty, expected top result '{expected_id}'"
    );
    assert_eq!(
        results[0].id, expected_id,
        "top result is '{}', expected '{expected_id}'",
        results[0].id
    );
}

/// Compute recall@k: what fraction of the true top-k are in the returned results.
/// `ground_truth` should be the true nearest neighbor IDs in order.
/// `results` are the search engine's returned results.
pub fn recall_at_k(results: &[SearchResult], ground_truth: &[&str], k: usize) -> f64 {
    let k = k.min(ground_truth.len()).min(results.len());
    if k == 0 {
        return 1.0;
    }

    let truth_set: std::collections::HashSet<&str> = ground_truth.iter().take(k).copied().collect();
    let result_set: std::collections::HashSet<&str> =
        results.iter().take(k).map(|r| r.id.as_str()).collect();

    let overlap = truth_set.intersection(&result_set).count();
    overlap as f64 / k as f64
}

/// Assert that recall@k meets a minimum threshold.
pub fn assert_recall_at_k(
    results: &[SearchResult],
    ground_truth: &[&str],
    k: usize,
    min_recall: f64,
) {
    let recall = recall_at_k(results, ground_truth, k);
    assert!(
        recall >= min_recall,
        "recall@{k} = {recall:.3}, expected >= {min_recall:.3}. \
         Results: {:?}, Truth: {:?}",
        results.iter().take(k).map(|r| &r.id).collect::<Vec<_>>(),
        ground_truth.iter().take(k).collect::<Vec<_>>()
    );
}

/// Assert that an operation returns a specific ZeppelinError variant.
pub fn assert_not_found_error(result: &Result<(), ZeppelinError>) {
    match result {
        Err(ZeppelinError::NotFound { .. }) => {}
        other => panic!("expected NotFound error, got: {other:?}"),
    }
}

pub fn assert_namespace_not_found_error(result: &Result<(), ZeppelinError>) {
    match result {
        Err(ZeppelinError::NamespaceNotFound { .. }) => {}
        other => panic!("expected NamespaceNotFound error, got: {other:?}"),
    }
}
