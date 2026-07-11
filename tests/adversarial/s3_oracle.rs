use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use xxhash_rust::xxh3::xxh3_64;
use zeppelin::compaction::gc::{
    load_gc_candidates, reachable_keys, reachable_keys_with_retained_history_and_staging,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::{ManifestHistoryRef, SketchRef};
use zeppelin::wal::Manifest;

use super::oracle::{Violation, ViolationId};

const SKETCH_V4_HEADER_LEN: usize = 44;
const SKETCH_CODE_BLOCK_DIMS: usize = 256;
const SKETCH_V4_VERSION: u32 = 4;
const SKETCH_V4_ROTATION_SCHEME: u32 = 1;
const SKETCH_V4_BIT_WIDTH: u32 = 2;

#[derive(Debug, Default)]
pub struct S3Tracker {
    history_hashes: BTreeMap<(String, u64), u64>,
}

impl S3Tracker {
    pub fn forget_namespace(&mut self, namespace: &str) {
        self.history_hashes.retain(|(ns, _), _| ns != namespace);
    }

    pub async fn check_namespace(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        op_index: u64,
        compact_status: &serde_json::Value,
        inject_missing_reachable: bool,
    ) -> Vec<Violation> {
        self.check_namespace_with_fault_window(
            store,
            namespace,
            op_index,
            compact_status,
            inject_missing_reachable,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_namespace_with_fault_window(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        op_index: u64,
        compact_status: &serde_json::Value,
        inject_missing_reachable: bool,
        fault_window_active: bool,
    ) -> Vec<Violation> {
        self.check_namespace_with_fault_context(
            store,
            namespace,
            op_index,
            compact_status,
            inject_missing_reachable,
            fault_window_active,
            &BTreeSet::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_namespace_with_fault_context(
        &mut self,
        store: &ZeppelinStore,
        namespace: &str,
        op_index: u64,
        compact_status: &serde_json::Value,
        inject_missing_reachable: bool,
        fault_window_active: bool,
        known_tainted_keys: &BTreeSet<String>,
    ) -> Vec<Violation> {
        let manifest = match read_manifest_for_oracle(store, namespace).await {
            Ok(Some(manifest)) => manifest,
            Ok(None) => {
                return vec![violation(
                    ViolationId::I15ManifestLineage,
                    op_index,
                    namespace,
                    "live manifest was missing during S3 oracle check",
                    json!({ "namespace": namespace }),
                )];
            }
            Err(error) => {
                if fault_window_active {
                    eprintln!(
                        "tolerated manifest read failure in active fault window for \
                         {namespace}: {error}"
                    );
                    return Vec::new();
                }
                return vec![violation(
                    ViolationId::I15ManifestLineage,
                    op_index,
                    namespace,
                    "live manifest read-failed during S3 oracle check",
                    json!({ "namespace": namespace, "error": error }),
                )];
            }
        };

        let mut violations = Vec::new();
        let status_generation = compact_status
            .get("manifest_generation")
            .and_then(serde_json::Value::as_u64);
        if status_generation != Some(manifest.version()) {
            violations.push(violation(
                ViolationId::I15ManifestLineage,
                op_index,
                namespace,
                "compact/status generation differed from live manifest",
                json!({
                    "status_generation": status_generation,
                    "manifest_generation": manifest.version(),
                    "compact_status": compact_status,
                }),
            ));
        }

        let listed = match list_prefix_for_oracle(store, &format!("{namespace}/")).await {
            Ok(listed) => listed.into_iter().collect::<BTreeSet<_>>(),
            Err(error) if fault_window_active => {
                eprintln!(
                    "tolerated prefix LIST failure in active fault window for \
                     {namespace}: {error}"
                );
                return violations;
            }
            Err(error) => {
                violations.push(violation(
                    ViolationId::I14S3Reachability,
                    op_index,
                    namespace,
                    "S3 prefix LIST read-failed during reachability check",
                    json!({ "error": error }),
                ));
                return violations;
            }
        };
        let reachable = match reachable_keys_for_s3_oracle(store, namespace, &manifest).await {
            Ok(reachable) => reachable,
            Err(error) if fault_window_active => {
                eprintln!(
                    "tolerated retained-history reachability read failure in active \
                     fault window for {namespace}: {error}"
                );
                reachable_keys(namespace, &manifest)
            }
            Err(error) => {
                violations.push(violation(
                    ViolationId::I14S3Reachability,
                    op_index,
                    namespace,
                    "retained-history reachability read-failed",
                    json!({ "error": error }),
                ));
                reachable_keys(namespace, &manifest)
            }
        };
        let mut missing = reachable
            .difference(&listed)
            .filter(|key| !known_tainted_keys.contains(*key))
            .cloned()
            .collect::<Vec<String>>();
        if inject_missing_reachable {
            missing.push(format!("{namespace}/__adversarial_missing_live_key"));
        }
        if !missing.is_empty() {
            violations.push(violation(
                ViolationId::I14S3Reachability,
                op_index,
                namespace,
                "manifest-reachable S3 keys were absent from storage",
                json!({ "missing": missing }),
            ));
        }

        let history = match list_history_for_oracle(store, namespace).await {
            Ok(history) => history,
            Err(error) if fault_window_active => {
                eprintln!(
                    "tolerated history LIST failure in active fault window for \
                     {namespace}: {error}"
                );
                return violations;
            }
            Err(error) => {
                violations.push(violation(
                    ViolationId::I15ManifestLineage,
                    op_index,
                    namespace,
                    "manifest history LIST read-failed",
                    json!({ "error": error }),
                ));
                return violations;
            }
        };
        for entry in history {
            let bytes = match history_bytes_for_oracle(store, namespace, &entry.key).await {
                Ok(bytes) => bytes,
                Err(error) if fault_window_active => {
                    eprintln!(
                        "tolerated history GET failure in active fault window for \
                         {namespace}: key={}; {error}",
                        entry.key
                    );
                    continue;
                }
                Err(error) => {
                    violations.push(violation(
                        ViolationId::I15ManifestLineage,
                        op_index,
                        namespace,
                        "manifest history GET read-failed",
                        json!({ "key": entry.key, "error": error }),
                    ));
                    continue;
                }
            };
            let hash = xxh3_64(&bytes);
            let key = (namespace.to_string(), entry.version);
            if let Some(previous) = self.history_hashes.get(&key).copied() {
                if previous != hash {
                    if fault_window_active {
                        eprintln!(
                            "tolerated immutable-history byte mismatch in active fault \
                             window for {namespace}: key={}",
                            entry.key
                        );
                    } else {
                        violations.push(violation(
                            ViolationId::I15ManifestLineage,
                            op_index,
                            namespace,
                            "manifest history bytes changed for an immutable generation",
                            json!({
                                "generation": entry.version,
                                "key": entry.key,
                                "previous_hash": previous,
                                "current_hash": hash,
                            }),
                        ));
                    }
                }
            } else {
                self.history_hashes.insert(key, hash);
            }
            let history_manifest = match Manifest::from_bytes(&bytes) {
                Ok(manifest) => manifest,
                Err(error) if fault_window_active => {
                    eprintln!(
                        "tolerated history decode failure in active fault window for \
                         {namespace}: key={}; {error}",
                        entry.key
                    );
                    continue;
                }
                Err(error) => {
                    violations.push(violation(
                        ViolationId::I15ManifestLineage,
                        op_index,
                        namespace,
                        "manifest history decode read-failed",
                        json!({ "key": entry.key, "error": error.to_string() }),
                    ));
                    continue;
                }
            };
            if history_manifest.version() != entry.version {
                violations.push(violation(
                    ViolationId::I15ManifestLineage,
                    op_index,
                    namespace,
                    "manifest history key contained a different generation",
                    json!({
                        "key": entry.key,
                        "key_generation": entry.version,
                        "manifest_generation": history_manifest.version(),
                    }),
                ));
            }
        }

        violations
    }
}

pub async fn check_clone_manifest(
    store: &ZeppelinStore,
    target: &str,
    response: &serde_json::Value,
    op_index: u64,
) -> Vec<Violation> {
    let manifest = match read_manifest_for_oracle(store, target).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            return vec![violation(
                ViolationId::I9Clone,
                op_index,
                target,
                "clone target manifest was missing after successful clone",
                json!({ "target": target }),
            )];
        }
        Err(error) => {
            return vec![violation(
                ViolationId::I9Clone,
                op_index,
                target,
                "clone target manifest read-failed after successful clone",
                json!({ "target": target, "error": error }),
            )];
        }
    };

    let mut violations = Vec::new();
    if !manifest.pending_deletes.is_empty() {
        violations.push(violation(
            ViolationId::I9Clone,
            op_index,
            target,
            "clone target retained pending deletes",
            json!({ "pending_deletes": manifest.pending_deletes }),
        ));
    }
    if manifest.fencing_token != 0 {
        violations.push(violation(
            ViolationId::I9Clone,
            op_index,
            target,
            "clone target manifest retained a fencing token",
            json!({ "fencing_token": manifest.fencing_token }),
        ));
    }
    let target_generation = response
        .get("target_generation")
        .and_then(serde_json::Value::as_u64);
    if target_generation != Some(manifest.version()) {
        violations.push(violation(
            ViolationId::I9Clone,
            op_index,
            target,
            "clone response target_generation differed from manifest",
            json!({
                "response_target_generation": target_generation,
                "manifest_generation": manifest.version(),
            }),
        ));
    }

    let target_prefix = format!("{target}/");
    let escaped = reachable_keys(target, &manifest)
        .into_iter()
        .filter(|key| !key.starts_with(&target_prefix))
        .collect::<Vec<_>>();
    if !escaped.is_empty() {
        violations.push(violation(
            ViolationId::I9Clone,
            op_index,
            target,
            "clone target manifest referenced keys outside the target prefix",
            json!({ "escaped_keys": escaped }),
        ));
    }

    violations
}

pub async fn check_quiescent_namespace(
    store: &ZeppelinStore,
    namespace: &str,
    expected_live: usize,
    compact_status: &serde_json::Value,
    op_index: u64,
) -> Vec<Violation> {
    let manifest = match read_manifest_for_oracle(store, namespace).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            return vec![violation(
                ViolationId::I16Quiescence,
                op_index,
                namespace,
                "manifest was missing at quiescence",
                json!({ "namespace": namespace }),
            )];
        }
        Err(error) => {
            return vec![violation(
                ViolationId::I16Quiescence,
                op_index,
                namespace,
                "quiescent manifest read-failed",
                json!({ "namespace": namespace, "error": error }),
            )];
        }
    };

    let mut violations = Vec::new();
    let ready = compact_status
        .get("ready")
        .and_then(serde_json::Value::as_bool);
    let uncompacted = compact_status
        .get("uncompacted_fragments")
        .and_then(serde_json::Value::as_u64);
    if ready != Some(true) || uncompacted != Some(0) {
        violations.push(violation(
            ViolationId::I16Quiescence,
            op_index,
            namespace,
            "namespace was not compact-ready at quiescence",
            json!({ "compact_status": compact_status }),
        ));
    }
    if manifest.vector_count() < expected_live as u64 {
        violations.push(violation(
            ViolationId::I16Quiescence,
            op_index,
            namespace,
            "manifest vector_count undercounted model live count at quiescence",
            json!({
                "manifest_vector_count": manifest.vector_count(),
                "expected_live": expected_live,
            }),
        ));
    }
    let reachable = match reachable_keys_for_s3_oracle(store, namespace, &manifest).await {
        Ok(reachable) => reachable,
        Err(error) => {
            violations.push(violation(
                ViolationId::I16Quiescence,
                op_index,
                namespace,
                "quiescent reachability read-failed",
                json!({ "error": error }),
            ));
            return violations;
        }
    };
    let listed = match list_prefix_for_oracle(store, &format!("{namespace}/")).await {
        Ok(listed) => listed,
        Err(error) => {
            violations.push(violation(
                ViolationId::I16Quiescence,
                op_index,
                namespace,
                "quiescent prefix LIST read-failed",
                json!({ "error": error }),
            ));
            return violations;
        }
    };
    let stray_wal = listed
        .into_iter()
        .filter(|key| key.starts_with(&format!("{namespace}/wal/")))
        .filter(|key| !reachable.contains(key))
        .collect::<Vec<_>>();
    if !stray_wal.is_empty() {
        violations.push(violation(
            ViolationId::I16Quiescence,
            op_index,
            namespace,
            "unreachable WAL objects remained after quiescence GC",
            json!({ "stray_wal": stray_wal }),
        ));
    }
    let candidates = match load_gc_candidates_for_oracle(store, namespace).await {
        Ok(candidates) => candidates,
        Err(error) => {
            violations.push(violation(
                ViolationId::I16Quiescence,
                op_index,
                namespace,
                "quiescent GC candidate read-failed",
                json!({ "error": error }),
            ));
            return violations;
        }
    };
    let reachable_candidates = candidates
        .into_iter()
        .filter(|candidate| reachable.contains(&candidate.key))
        .map(|candidate| candidate.key)
        .collect::<Vec<_>>();
    if !reachable_candidates.is_empty() {
        violations.push(violation(
            ViolationId::I16Quiescence,
            op_index,
            namespace,
            "GC candidates included reachable keys at quiescence",
            json!({ "reachable_candidates": reachable_candidates }),
        ));
    }

    violations
}

pub async fn check_v4_sketch_publication(
    store: &ZeppelinStore,
    namespace: &str,
    op_index: u64,
) -> Vec<Violation> {
    let manifest = match read_manifest_for_oracle(store, namespace).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            return vec![violation(
                ViolationId::I17SketchPublication,
                op_index,
                namespace,
                "manifest was missing during v4 sketch publication check",
                json!({ "namespace": namespace }),
            )];
        }
        Err(error) => {
            return vec![violation(
                ViolationId::I17SketchPublication,
                op_index,
                namespace,
                "sketch manifest read-failed",
                json!({ "namespace": namespace, "error": error }),
            )];
        }
    };

    let mut violations = Vec::new();
    for segment in &manifest.segments {
        let Some(sketch) = segment.sketch.as_ref() else {
            continue;
        };
        match object_bytes_for_oracle(store, &sketch.key).await {
            Ok(bytes) => {
                violations.extend(check_v4_sketch_ref(namespace, op_index, sketch, &bytes))
            }
            Err(error) => violations.push(violation(
                ViolationId::I17SketchPublication,
                op_index,
                namespace,
                "referenced sketch object could not be read",
                json!({
                    "segment_id": segment.id,
                    "sketch_key": sketch.key,
                    "error": error.to_string(),
                }),
            )),
        }
    }
    violations
}

fn check_v4_sketch_ref(
    namespace: &str,
    op_index: u64,
    sketch: &SketchRef,
    bytes: &[u8],
) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut reject = |detail: &str, evidence: serde_json::Value| {
        violations.push(violation(
            ViolationId::I17SketchPublication,
            op_index,
            namespace,
            detail,
            evidence,
        ));
    };

    if sketch.version != SKETCH_V4_VERSION {
        reject(
            "quiescent sketch ref did not publish version 4",
            json!({ "sketch_key": sketch.key, "ref_version": sketch.version }),
        );
    }
    if sketch.size_bytes != bytes.len() as u64 {
        reject(
            "sketch ref size differed from stored object length",
            json!({
                "sketch_key": sketch.key,
                "ref_size_bytes": sketch.size_bytes,
                "stored_size_bytes": bytes.len(),
            }),
        );
    }
    if bytes.len() < SKETCH_V4_HEADER_LEN {
        reject(
            "stored sketch was too short for a v4 header",
            json!({ "sketch_key": sketch.key, "stored_size_bytes": bytes.len() }),
        );
        return violations;
    }
    if !bytes.starts_with(b"ZSK1") {
        reject(
            "stored sketch magic was not ZSK1",
            json!({ "sketch_key": sketch.key }),
        );
    }

    let stored_version = read_u32(bytes, 4);
    let stored_dim = read_u32(bytes, 8) as usize;
    let stored_code_dims = read_u32(bytes, 12) as usize;
    let stored_rotation_seed = read_u64(bytes, 28);
    let stored_rotation_scheme = read_u32(bytes, 36);
    let stored_bit_width = read_u32(bytes, 40);
    if stored_version != SKETCH_V4_VERSION {
        reject(
            "stored sketch header did not encode version 4",
            json!({ "sketch_key": sketch.key, "stored_version": stored_version }),
        );
    }
    if stored_rotation_scheme != SKETCH_V4_ROTATION_SCHEME
        || stored_bit_width != SKETCH_V4_BIT_WIDTH
    {
        reject(
            "stored sketch header did not encode the v4 two-bit scheme",
            json!({
                "sketch_key": sketch.key,
                "stored_rotation_scheme": stored_rotation_scheme,
                "stored_bit_width": stored_bit_width,
            }),
        );
    }
    let expected_code_dims = stored_dim
        .checked_add(SKETCH_CODE_BLOCK_DIMS - 1)
        .map(|dims| dims / SKETCH_CODE_BLOCK_DIMS * SKETCH_CODE_BLOCK_DIMS);
    if expected_code_dims != Some(stored_code_dims) || sketch.code_dims != stored_code_dims {
        reject(
            "sketch ref code dimensions differed from the padded v4 header shape",
            json!({
                "sketch_key": sketch.key,
                "stored_dim": stored_dim,
                "stored_code_dims": stored_code_dims,
                "expected_code_dims": expected_code_dims,
                "ref_code_dims": sketch.code_dims,
            }),
        );
    }
    let expected_row_bytes = stored_code_dims
        .checked_div(4)
        .and_then(|planes| planes.checked_add(2 * std::mem::size_of::<f32>()));
    if expected_row_bytes != Some(sketch.bytes_per_vector) {
        reject(
            "sketch ref row width differed from the v4 two-plane layout",
            json!({
                "sketch_key": sketch.key,
                "stored_code_dims": stored_code_dims,
                "expected_bytes_per_vector": expected_row_bytes,
                "ref_bytes_per_vector": sketch.bytes_per_vector,
            }),
        );
    }
    if sketch.rotation_seed != Some(stored_rotation_seed) {
        reject(
            "sketch ref rotation seed differed from the stored v4 header",
            json!({
                "sketch_key": sketch.key,
                "ref_rotation_seed": sketch.rotation_seed,
                "stored_rotation_seed": stored_rotation_seed,
            }),
        );
    }

    violations
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("v4 sketch header length was checked"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("v4 sketch header length was checked"),
    )
}

async fn reachable_keys_for_s3_oracle(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: &Manifest,
) -> Result<BTreeSet<String>, String> {
    match reachable_keys_with_retained_history_and_staging(
        store,
        namespace,
        manifest,
        &BTreeSet::new(),
    )
    .await
    {
        Ok(keys) => Ok(keys),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            match reachable_keys_with_retained_history_and_staging(
                store,
                namespace,
                manifest,
                &BTreeSet::new(),
            )
            .await
            {
                Ok(keys) => Ok(keys),
                Err(retry_error) => Err(format!(
                    "first_error={first_error}; retry_error={retry_error}"
                )),
            }
        }
    }
}

async fn list_prefix_for_oracle(
    store: &ZeppelinStore,
    prefix: &str,
) -> Result<Vec<String>, String> {
    match store.list_prefix(prefix).await {
        Ok(keys) => Ok(keys),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            store.list_prefix(prefix).await.map_err(|retry_error| {
                format!("first_error={first_error}; retry_error={retry_error}")
            })
        }
    }
}

async fn list_history_for_oracle(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Vec<ManifestHistoryRef>, String> {
    match Manifest::list_history(store, namespace).await {
        Ok(history) => Ok(history),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            Manifest::list_history(store, namespace)
                .await
                .map_err(|retry_error| {
                    format!("first_error={first_error}; retry_error={retry_error}")
                })
        }
    }
}

async fn read_manifest_for_oracle(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Option<Manifest>, String> {
    match Manifest::read(store, namespace).await {
        Ok(manifest) => Ok(manifest),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            Manifest::read(store, namespace)
                .await
                .map_err(|retry_error| {
                    format!("first_error={first_error}; retry_error={retry_error}")
                })
        }
    }
}

async fn history_bytes_for_oracle(
    store: &ZeppelinStore,
    _namespace: &str,
    key: &str,
) -> Result<bytes::Bytes, String> {
    object_bytes_for_oracle(store, key).await
}

async fn object_bytes_for_oracle(store: &ZeppelinStore, key: &str) -> Result<bytes::Bytes, String> {
    match store.get(key).await {
        Ok(bytes) => Ok(bytes),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            match store.get(key).await {
                Ok(bytes) => Ok(bytes),
                Err(retry_error) => Err(format!(
                    "first_error={first_error}; retry_error={retry_error}"
                )),
            }
        }
    }
}

async fn load_gc_candidates_for_oracle(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Vec<zeppelin::compaction::gc::GcCandidate>, String> {
    match load_gc_candidates(store, namespace).await {
        Ok(candidates) => Ok(candidates),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            load_gc_candidates(store, namespace)
                .await
                .map_err(|retry_error| {
                    format!("first_error={first_error}; retry_error={retry_error}")
                })
        }
    }
}

fn violation(
    id: ViolationId,
    op_index: u64,
    namespace: &str,
    detail: &str,
    evidence: serde_json::Value,
) -> Violation {
    Violation {
        id,
        op_index,
        namespace: namespace.to_string(),
        detail: detail.to_string(),
        evidence,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use ulid::Ulid;
    use zeppelin::wal::manifest::{FragmentRef, SketchRef};

    use super::*;
    use crate::adversarial::chaos::StoreOp;
    use crate::adversarial::faults::store_proxy::store_fault_proxy;
    use crate::adversarial::faults::{
        Boundary, FaultEvent, FaultKind, FaultProfile, FaultSchedule, FaultScheduler,
        InjectedErrorKind, TargetSelector,
    };

    const V4_ROTATION_SEED: u64 = 0x5a45_5050_454c_494e;

    #[tokio::test]
    async fn known_tainted_missing_key_is_not_an_s3_reachability_violation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let fragment_id = Ulid::from(1_u128);
        let missing_key = format!("ns/wal/{fragment_id}.wal");
        let mut manifest = Manifest::new();
        manifest.add_fragment(FragmentRef {
            id: fragment_id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 1,
        });
        manifest.write(&store, "ns").await.unwrap();

        let untainted = S3Tracker::default()
            .check_namespace(&store, "ns", 7, &json!({ "manifest_generation": 1 }), false)
            .await;
        assert!(
            untainted
                .iter()
                .any(|violation| violation.id == ViolationId::I14S3Reachability),
            "{untainted:#?}"
        );

        let known_tainted = BTreeSet::from([missing_key]);
        let tainted = S3Tracker::default()
            .check_namespace_with_fault_context(
                &store,
                "ns",
                7,
                &json!({ "manifest_generation": 1 }),
                false,
                false,
                &known_tainted,
            )
            .await;
        assert!(tainted.is_empty(), "{tainted:#?}");
    }

    #[tokio::test]
    async fn manifest_read_failure_returns_violation_instead_of_panicking() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: Some(10),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http503,
                },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        let violations = S3Tracker::default()
            .check_namespace(
                &faulted,
                "ns",
                7,
                &json!({ "manifest_generation": 1 }),
                false,
            )
            .await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I15ManifestLineage);
        assert!(violations[0].detail.contains("read-failed"));
    }

    #[tokio::test]
    async fn manifest_read_failure_is_tolerated_only_in_active_fault_window() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: Some(10),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http503,
                },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        let violations = S3Tracker::default()
            .check_namespace_with_fault_window(
                &faulted,
                "ns",
                7,
                &json!({ "manifest_generation": 1 }),
                false,
                true,
            )
            .await;

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[tokio::test]
    async fn quiescent_reachability_read_failure_returns_i16() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: Some(10),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("ns/manifests/".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http503,
                },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        let violations = check_quiescent_namespace(
            &faulted,
            "ns",
            0,
            &json!({ "ready": true, "uncompacted_fragments": 0 }),
            7,
        )
        .await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I16Quiescence);
        assert!(violations[0].detail.contains("reachability read-failed"));
    }

    #[tokio::test]
    async fn clone_manifest_read_failure_returns_i9_instead_of_panicking() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "target").await.unwrap();
        let faulted = pre_fail_store(&inner, StoreOp::Get, "target/manifest.json");

        let violations =
            check_clone_manifest(&faulted, "target", &json!({ "target_generation": 1 }), 11).await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I9Clone);
        assert!(violations[0].detail.contains("read-failed"));
    }

    #[tokio::test]
    async fn quiescent_manifest_read_failure_returns_i16_instead_of_panicking() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let faulted = pre_fail_store(&inner, StoreOp::Get, "ns/manifest.json");

        let violations = check_quiescent_namespace(
            &faulted,
            "ns",
            0,
            &json!({ "ready": true, "uncompacted_fragments": 0 }),
            12,
        )
        .await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I16Quiescence);
        assert!(violations[0].detail.contains("read-failed"));
    }

    #[tokio::test]
    async fn quiescent_gc_candidate_read_failure_returns_i16_instead_of_panicking() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let faulted = pre_fail_store(&inner, StoreOp::Get, "ns/_gc/candidates.json");

        let violations = check_quiescent_namespace(
            &faulted,
            "ns",
            0,
            &json!({ "ready": true, "uncompacted_fragments": 0 }),
            13,
        )
        .await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I16Quiescence);
        assert!(violations[0].detail.contains("candidate read-failed"));
    }

    #[tokio::test]
    async fn sketch_manifest_read_failure_returns_i17_instead_of_panicking() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&inner, "ns").await.unwrap();
        let faulted = pre_fail_store(&inner, StoreOp::Get, "ns/manifest.json");

        let violations = check_v4_sketch_publication(&faulted, "ns", 14).await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I17SketchPublication);
        assert!(violations[0].detail.contains("read-failed"));
    }

    #[test]
    fn v4_sketch_publication_requires_seed_and_matches_stored_shape() {
        let bytes = valid_v4_sketch_bytes();
        let mut sketch = SketchRef {
            key: "ns/segments/seg/coarse_sketch.bin".to_string(),
            version: 4,
            code_dims: 256,
            bytes_per_vector: 72,
            size_bytes: bytes.len() as u64,
            rotation_seed: None,
        };

        let violations = check_v4_sketch_ref("ns", 9, &sketch, &bytes);
        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I17SketchPublication);

        sketch.rotation_seed = Some(V4_ROTATION_SEED);
        assert!(
            check_v4_sketch_ref("ns", 9, &sketch, &bytes).is_empty(),
            "matching v4 sketch ref and bytes must pass"
        );
    }

    fn valid_v4_sketch_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"ZSK1");
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&256u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&V4_ROTATION_SEED.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&[0; 72]);
        bytes
    }

    fn pre_fail_store(
        inner: &ZeppelinStore,
        store_op: StoreOp,
        key_substring: &str,
    ) -> ZeppelinStore {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-read-failure".to_string(),
                start_op: 0,
                end_op: Some(10),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(store_op),
                    key_substring: Some(key_substring.to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http503,
                },
            }],
        });
        store_fault_proxy(inner, scheduler)
    }
}
