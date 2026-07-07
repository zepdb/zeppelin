//! Exact-key reachability for storage garbage collection.

use std::collections::BTreeSet;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::fts::global_index::global_fts_key;
use crate::fts::inverted_index::fts_index_key;
use crate::index::bitmap::bitmap_key;
use crate::index::hierarchical::tree_meta_key;
use crate::index::ivf_flat::build::{attrs_key, centroids_key, cluster_key};
use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::Manifest;
use crate::wal::Lease;

/// Lease-scoped side-object recording compaction uploads that have not yet
/// been committed into the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionStaging {
    /// Fencing token of the lease that owns these in-flight uploads.
    pub fencing_token: u64,
    /// Exact S3 keys uploaded by the compaction and not yet manifest-live.
    #[serde(default)]
    pub keys: BTreeSet<String>,
}

/// S3 key for a compaction staging side object.
#[must_use]
pub fn staging_key(namespace: &str, fencing_token: u64) -> String {
    format!("{namespace}/_staging/{fencing_token}.json")
}

/// Exact-key set of every S3 object still referenced by `manifest`.
#[must_use]
pub fn reachable_keys(namespace: &str, manifest: &Manifest) -> BTreeSet<String> {
    reachable_keys_with_staging(namespace, manifest, &BTreeSet::new())
}

/// Exact-key set of every manifest-referenced object plus active staged uploads.
#[must_use]
pub fn reachable_keys_with_staging(
    namespace: &str,
    manifest: &Manifest,
    staging: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();

    for fragment in &manifest.fragments {
        keys.insert(WalFragment::s3_key(namespace, &fragment.id));
    }

    for segment in &manifest.segments {
        if segment.hierarchical {
            keys.insert(tree_meta_key(namespace, &segment.id));
        } else {
            keys.insert(centroids_key(namespace, &segment.id));
        }

        if let Some(sketch) = &segment.sketch {
            keys.insert(sketch.key.clone());
        }
        if let Some(bootstrap) = &segment.bootstrap {
            keys.insert(bootstrap.key.clone());
        }
        if let Some(membership) = &segment.membership {
            keys.insert(membership.key.clone());
        }

        if segment.cluster_objects.is_empty() {
            for cluster_idx in 0..segment.cluster_count {
                keys.insert(cluster_key(
                    namespace,
                    segment.cluster_owner(cluster_idx),
                    cluster_idx,
                ));
            }
        } else {
            for object_ref in &segment.cluster_objects {
                keys.insert(object_ref.key.clone());
            }
        }

        for cluster_idx in 0..segment.cluster_count {
            let owner = segment.cluster_owner(cluster_idx);
            keys.insert(attrs_key(namespace, owner, cluster_idx));

            if !segment.bitmap_fields.is_empty() {
                keys.insert(bitmap_key(namespace, owner, cluster_idx));
            }

            if !segment.fts_fields.is_empty() {
                keys.insert(fts_index_key(namespace, owner, cluster_idx));
            }

            match segment.quantization {
                QuantizationType::Scalar => {
                    keys.insert(sq_cluster_key(namespace, owner, cluster_idx));
                }
                QuantizationType::Product => {
                    keys.insert(pq_cluster_key(namespace, owner, cluster_idx));
                }
                QuantizationType::None => {}
            }
        }

        match segment.quantization {
            QuantizationType::Scalar => {
                keys.insert(sq_calibration_key(namespace, &segment.id));
            }
            QuantizationType::Product => {
                keys.insert(pq_codebook_key(namespace, &segment.id));
            }
            QuantizationType::None => {}
        }

        if segment.has_global_fts {
            keys.insert(global_fts_key(namespace, &segment.id));
        }
    }

    keys.extend(manifest.pending_deletes.iter().cloned());
    keys.extend(staging.iter().cloned());
    keys
}

/// A manifest-derived orphan candidate for the future two-pass GC sweep.
///
/// This is not an authoritative refcount. It only records that `key` left the
/// exact reachable union at a manifest CAS time; delete decisions must still
/// revalidate against `reachable_keys()` from a fresh manifest snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcCandidate {
    /// Exact S3 key that left the manifest-derived reachable union.
    pub key: String,
    /// Manifest commit time at which the key was first observed unreachable.
    pub first_seen_unreachable_at: DateTime<Utc>,
}

/// Build GC candidates from the exact key delta across a manifest CAS.
///
/// The returned keys are `reachable(old_manifest) - reachable(new_manifest)`.
/// This preserves carried-object correctness by construction: any object still
/// referenced by at least one live segment remains in the reachable union and
/// is not emitted. Crash-orphans that never entered a manifest are invisible to
/// this delta and remain the periodic LIST sweep's responsibility.
#[must_use]
pub fn gc_candidates_from_manifest_delta(
    namespace: &str,
    old_manifest: &Manifest,
    new_manifest: &Manifest,
    commit_time: DateTime<Utc>,
) -> Vec<GcCandidate> {
    let old_reachable = reachable_keys(namespace, old_manifest);
    let new_reachable = reachable_keys(namespace, new_manifest);

    old_reachable
        .difference(&new_reachable)
        .map(|key| GcCandidate {
            key: key.clone(),
            first_seen_unreachable_at: commit_time,
        })
        .collect()
}

/// Keep only candidates that are still unreachable in `manifest`.
///
/// Candidate state is an accelerator, not truth. The future delete stage must
/// call this shape of check after reading the authoritative manifest so a key
/// that became live again is skipped.
#[must_use]
pub fn revalidate_unreachable_candidates(
    namespace: &str,
    manifest: &Manifest,
    candidates: &[GcCandidate],
) -> Vec<GcCandidate> {
    let reachable = reachable_keys(namespace, manifest);
    candidates
        .iter()
        .filter(|candidate| !reachable.contains(&candidate.key))
        .cloned()
        .collect()
}

/// Write the exact staged-key set for the current compaction lease.
pub async fn write_compaction_staging(
    store: &ZeppelinStore,
    namespace: &str,
    fencing_token: u64,
    keys: BTreeSet<String>,
) -> Result<()> {
    let staging = CompactionStaging {
        fencing_token,
        keys,
    };
    let data = Bytes::from(serde_json::to_vec_pretty(&staging)?);
    store
        .put(&staging_key(namespace, fencing_token), data)
        .await
}

/// Clear the staged-key side object for a compaction lease.
pub async fn clear_compaction_staging(
    store: &ZeppelinStore,
    namespace: &str,
    fencing_token: u64,
) -> Result<()> {
    match store.delete(&staging_key(namespace, fencing_token)).await {
        Ok(()) => Ok(()),
        Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Return staged keys for the currently active lease, excluding stale or
/// expired lease records so a dead compactor cannot pin uploads forever.
pub async fn active_staged_keys(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<BTreeSet<String>> {
    let lease_data = match store.get(&format!("{namespace}/lease.json")).await {
        Ok(data) => data,
        Err(crate::error::ZeppelinError::NotFound { .. }) => return Ok(BTreeSet::new()),
        Err(e) => return Err(e),
    };
    let lease: Lease = serde_json::from_slice(&lease_data)?;
    if lease.expires_at <= Utc::now() {
        return Ok(BTreeSet::new());
    }

    let mut staged = BTreeSet::new();
    let prefix = format!("{namespace}/_staging/");
    for key in store.list_prefix(&prefix).await? {
        let data = store.get(&key).await?;
        let entry: CompactionStaging = serde_json::from_slice(&data)?;
        if entry.fencing_token == lease.fencing_token {
            staged.extend(entry.keys);
        }
    }
    Ok(staged)
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use ulid::Ulid;

    use crate::fts::global_index::global_fts_key;
    use crate::fts::inverted_index::fts_index_key;
    use crate::index::bitmap::bitmap_key;
    use crate::index::hierarchical::tree_meta_key;
    use crate::index::ivf_flat::build::{attrs_key, bootstrap_key, centroids_key, cluster_key};
    use crate::index::ivf_flat::membership::membership_key;
    use crate::index::ivf_flat::sketch::sketch_key;
    use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
    use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
    use crate::index::quantization::QuantizationType;
    use crate::wal::fragment::WalFragment;
    use crate::wal::manifest::{
        BootstrapRef, ClusterDataObjectRef, FragmentRef, Manifest, MembershipRef, SegmentRef,
        SketchRef,
    };

    const NS: &str = "gc_ns";

    fn fragment_ref(id: Ulid) -> FragmentRef {
        FragmentRef {
            id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 100,
        }
    }

    fn segment_ref(id: &str, cluster_count: usize) -> SegmentRef {
        SegmentRef {
            id: id.to_string(),
            vector_count: 10,
            cluster_count,
            quantization: QuantizationType::None,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
        }
    }

    fn candidate_keys(candidates: &[GcCandidate]) -> BTreeSet<String> {
        candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect()
    }

    struct ReachabilityCase {
        name: &'static str,
        manifest: Manifest,
        present: Vec<String>,
        absent: Vec<String>,
    }

    #[test]
    fn reachable_keys_are_the_exact_manifest_references() {
        let frag_a = Ulid::from_parts(1, 10);
        let frag_b = Ulid::from_parts(2, 20);

        let mut carried_manifest = Manifest::new();
        let mut carried = segment_ref("seg_new", 3);
        carried.cluster_owners = vec![
            "seg_old".to_string(),
            "seg_new".to_string(),
            "seg_older".to_string(),
        ];
        carried_manifest.segments.push(carried);

        let mut legacy_manifest = Manifest::new();
        legacy_manifest.segments.push(segment_ref("seg_legacy", 2));

        let mut pending_manifest = Manifest::new();
        pending_manifest.pending_deletes = vec![
            "gc_ns/wal/pruned-but-undeleted.wal".to_string(),
            "gc_ns/segments/seg_pruned/cluster_0.bin".to_string(),
        ];

        let mut fragments_manifest = Manifest::new();
        fragments_manifest.fragments = vec![fragment_ref(frag_a), fragment_ref(frag_b)];

        let mut metadata_manifest = Manifest::new();
        let mut metadata = segment_ref("seg_meta", 2);
        metadata.quantization = QuantizationType::Product;
        metadata.bitmap_fields = vec!["color".to_string()];
        metadata.fts_fields = vec!["body".to_string()];
        metadata.has_global_fts = true;
        metadata.sketch = Some(SketchRef {
            key: sketch_key(NS, "seg_meta"),
            version: 3,
            code_dims: 8,
            bytes_per_vector: 8,
            size_bytes: 512,
        });
        metadata.bootstrap = Some(BootstrapRef {
            key: bootstrap_key(NS, "seg_meta"),
            size_bytes: 1024,
        });
        metadata.membership = Some(MembershipRef {
            key: membership_key(NS, "seg_meta"),
            size_bytes: 256,
            entry_count: 10,
        });
        metadata.cluster_objects = vec![ClusterDataObjectRef {
            key: format!("{NS}/segments/seg_meta/cluster_group_0.bin"),
            clusters: vec![0, 1],
            live_offset: 0,
            live_len: 0,
            size_bytes: 2048,
        }];
        metadata_manifest.segments.push(metadata);

        let mut no_global_fts_manifest = Manifest::new();
        let mut no_global_fts = segment_ref("seg_no_global_fts", 1);
        no_global_fts.fts_fields = vec!["body".to_string()];
        no_global_fts.has_global_fts = false;
        no_global_fts_manifest.segments.push(no_global_fts);

        let mut hierarchical_manifest = Manifest::new();
        let mut hierarchical = segment_ref("seg_tree", 1);
        hierarchical.hierarchical = true;
        hierarchical.quantization = QuantizationType::Scalar;
        hierarchical_manifest.segments.push(hierarchical);

        let mut multi_manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 2);
        first.cluster_owners = vec!["seg_shared".to_string(), "seg_first".to_string()];
        let mut second = segment_ref("seg_second", 2);
        second.cluster_owners = vec!["seg_second".to_string(), "seg_shared".to_string()];
        multi_manifest.segments = vec![first, second];

        let cases = vec![
            ReachabilityCase {
                name: "carried cluster owners",
                manifest: carried_manifest,
                present: vec![
                    cluster_key(NS, "seg_old", 0),
                    cluster_key(NS, "seg_new", 1),
                    cluster_key(NS, "seg_older", 2),
                    attrs_key(NS, "seg_old", 0),
                    attrs_key(NS, "seg_older", 2),
                ],
                absent: vec![cluster_key(NS, "seg_new", 0)],
            },
            ReachabilityCase {
                name: "legacy self-owned layout",
                manifest: legacy_manifest,
                present: vec![
                    cluster_key(NS, "seg_legacy", 0),
                    cluster_key(NS, "seg_legacy", 1),
                    attrs_key(NS, "seg_legacy", 0),
                    attrs_key(NS, "seg_legacy", 1),
                    centroids_key(NS, "seg_legacy"),
                ],
                absent: vec![cluster_key(NS, "some_other_segment", 0)],
            },
            ReachabilityCase {
                name: "pending deletes are still reachable",
                manifest: pending_manifest,
                present: vec![
                    "gc_ns/wal/pruned-but-undeleted.wal".to_string(),
                    "gc_ns/segments/seg_pruned/cluster_0.bin".to_string(),
                ],
                absent: Vec::new(),
            },
            ReachabilityCase {
                name: "fragments",
                manifest: fragments_manifest,
                present: vec![
                    WalFragment::s3_key(NS, &frag_a),
                    WalFragment::s3_key(NS, &frag_b),
                ],
                absent: Vec::new(),
            },
            ReachabilityCase {
                name: "per-segment metadata",
                manifest: metadata_manifest,
                present: vec![
                    centroids_key(NS, "seg_meta"),
                    sketch_key(NS, "seg_meta"),
                    bootstrap_key(NS, "seg_meta"),
                    membership_key(NS, "seg_meta"),
                    format!("{NS}/segments/seg_meta/cluster_group_0.bin"),
                    attrs_key(NS, "seg_meta", 0),
                    attrs_key(NS, "seg_meta", 1),
                    bitmap_key(NS, "seg_meta", 0),
                    bitmap_key(NS, "seg_meta", 1),
                    fts_index_key(NS, "seg_meta", 0),
                    fts_index_key(NS, "seg_meta", 1),
                    pq_codebook_key(NS, "seg_meta"),
                    pq_cluster_key(NS, "seg_meta", 0),
                    pq_cluster_key(NS, "seg_meta", 1),
                    global_fts_key(NS, "seg_meta"),
                ],
                absent: vec![
                    cluster_key(NS, "seg_meta", 0),
                    sq_calibration_key(NS, "seg_meta"),
                    global_fts_key(NS, "seg_no_global_fts"),
                ],
            },
            ReachabilityCase {
                name: "global fts omitted when manifest says absent",
                manifest: no_global_fts_manifest,
                present: vec![fts_index_key(NS, "seg_no_global_fts", 0)],
                absent: vec![global_fts_key(NS, "seg_no_global_fts")],
            },
            ReachabilityCase {
                name: "hierarchical metadata",
                manifest: hierarchical_manifest,
                present: vec![
                    tree_meta_key(NS, "seg_tree"),
                    attrs_key(NS, "seg_tree", 0),
                    sq_calibration_key(NS, "seg_tree"),
                    sq_cluster_key(NS, "seg_tree", 0),
                ],
                absent: vec![centroids_key(NS, "seg_tree")],
            },
            ReachabilityCase {
                name: "multiple live segments share one object",
                manifest: multi_manifest,
                present: vec![
                    cluster_key(NS, "seg_shared", 0),
                    cluster_key(NS, "seg_shared", 1),
                    cluster_key(NS, "seg_first", 1),
                    cluster_key(NS, "seg_second", 0),
                ],
                absent: Vec::new(),
            },
        ];

        for case in cases {
            let reachable = reachable_keys(NS, &case.manifest);
            for key in case.present {
                assert!(
                    reachable.contains(&key),
                    "{}: expected reachable key {key}",
                    case.name
                );
            }
            for key in case.absent {
                assert!(
                    !reachable.contains(&key),
                    "{}: key must not be marked reachable: {key}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn reachable_keys_are_deduplicated() {
        let mut manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 1);
        first.cluster_owners = vec!["seg_shared".to_string()];
        let mut second = segment_ref("seg_second", 1);
        second.cluster_owners = vec!["seg_shared".to_string()];
        manifest.segments = vec![first, second];

        let reachable = reachable_keys(NS, &manifest);
        let shared_key = cluster_key(NS, "seg_shared", 0);
        assert_eq!(
            reachable.iter().filter(|key| *key == &shared_key).count(),
            1,
            "shared carried object must appear once"
        );
    }

    #[test]
    fn cas_delta_candidates_are_exact_keys_that_left_reachability() {
        let commit_time = Utc::now();
        let compacted_fragment = Ulid::from_parts(10, 1);
        let new_fragment = Ulid::from_parts(11, 1);

        let mut old_manifest = Manifest::new();
        old_manifest.fragments = vec![fragment_ref(compacted_fragment)];
        old_manifest.segments = vec![segment_ref("seg_old", 2)];

        let mut new_segment = segment_ref("seg_new", 2);
        new_segment.cluster_owners = vec!["seg_old".to_string(), "seg_new".to_string()];
        let mut new_manifest = Manifest::new();
        new_manifest.fragments = vec![fragment_ref(new_fragment)];
        new_manifest.segments = vec![new_segment];

        let candidates =
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time);

        assert_eq!(
            candidate_keys(&candidates),
            BTreeSet::from([
                WalFragment::s3_key(NS, &compacted_fragment),
                centroids_key(NS, "seg_old"),
                cluster_key(NS, "seg_old", 1),
                attrs_key(NS, "seg_old", 1),
            ]),
            "only keys whose live reference set dropped to zero are candidates"
        );
        assert_eq!(
            candidates.len(),
            4,
            "each key that left reachability must be emitted exactly once"
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.first_seen_unreachable_at == commit_time),
            "every delta candidate must be stamped with the manifest commit time"
        );
        assert!(
            !candidate_keys(&candidates).contains(&WalFragment::s3_key(NS, &new_fragment)),
            "keys added by the new manifest are not orphan candidates"
        );
        assert!(
            !candidate_keys(&candidates).contains(&cluster_key(NS, "seg_old", 0)),
            "carried cluster object remains reachable through the new segment"
        );
    }

    #[test]
    fn shared_carried_object_is_candidate_only_after_last_reference_releases_it() {
        let commit_time = Utc::now();
        let shared_cluster_key = cluster_key(NS, "seg_shared", 0);
        let shared_attrs_key = attrs_key(NS, "seg_shared", 0);

        let mut old_manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 1);
        first.cluster_owners = vec!["seg_shared".to_string()];
        let mut second = segment_ref("seg_second", 1);
        second.cluster_owners = vec!["seg_shared".to_string()];
        old_manifest.segments = vec![first, second.clone()];

        let mut one_reference_released = Manifest::new();
        one_reference_released.segments = vec![second.clone()];
        let first_release = gc_candidates_from_manifest_delta(
            NS,
            &old_manifest,
            &one_reference_released,
            commit_time,
        );
        let first_release_keys = candidate_keys(&first_release);
        assert!(
            !first_release_keys.contains(&shared_cluster_key),
            "shared cluster data must survive while any segment still references it"
        );
        assert!(
            !first_release_keys.contains(&shared_attrs_key),
            "shared sidecar data must survive while any segment still references it"
        );

        let no_references = Manifest::new();
        let second_release = gc_candidates_from_manifest_delta(
            NS,
            &one_reference_released,
            &no_references,
            commit_time,
        );
        assert!(
            candidate_keys(&second_release)
                .is_superset(&BTreeSet::from([shared_cluster_key, shared_attrs_key,])),
            "shared carried keys become candidates when the last reference is gone"
        );
    }

    #[test]
    fn crash_orphan_absent_from_both_manifests_is_not_a_delta_candidate() {
        let commit_time = Utc::now();
        let crash_orphan = format!("{NS}/segments/crash_orphan/cluster_0.bin");

        let mut old_manifest = Manifest::new();
        old_manifest.segments = vec![segment_ref("seg_live", 1)];
        let mut new_manifest = Manifest::new();
        new_manifest.segments = vec![segment_ref("seg_live", 1)];

        let candidates =
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time);

        assert!(
            !candidate_keys(&candidates).contains(&crash_orphan),
            "PUT-without-CAS orphans never entered a manifest and require the LIST backstop"
        );
    }

    #[test]
    fn candidate_revalidation_recomputes_reachability_from_manifest_before_deletion() {
        let commit_time = Utc::now();
        let live_key = cluster_key(NS, "seg_live", 0);
        let dead_key = format!("{NS}/wal/dead.wal");

        let mut manifest = Manifest::new();
        manifest.segments = vec![segment_ref("seg_live", 1)];

        let candidates = vec![
            GcCandidate {
                key: live_key,
                first_seen_unreachable_at: commit_time,
            },
            GcCandidate {
                key: dead_key.clone(),
                first_seen_unreachable_at: commit_time,
            },
        ];

        let still_unreachable = revalidate_unreachable_candidates(NS, &manifest, &candidates);

        assert_eq!(
            candidate_keys(&still_unreachable),
            BTreeSet::from([dead_key]),
            "delete decisions must re-read exact manifest reachability, not trust candidate state"
        );
    }

    #[test]
    fn reachable_keys_with_staging_unions_active_staged_keys() {
        let mut manifest = Manifest::new();
        manifest.segments.push(segment_ref("seg_live", 1));
        let staged: BTreeSet<String> = [
            format!("{NS}/segments/seg_staged/centroids.bin"),
            format!("{NS}/segments/seg_staged/cluster_group_0.bin"),
        ]
        .into_iter()
        .collect();

        let reachable = reachable_keys_with_staging(NS, &manifest, &staged);

        assert!(reachable.contains(&centroids_key(NS, "seg_live")));
        for key in staged {
            assert!(
                reachable.contains(&key),
                "active lease staged key must be treated as reachable: {key}"
            );
        }
    }
}
