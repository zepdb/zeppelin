use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use xxhash_rust::xxh3::xxh3_64;
use zeppelin::compaction::gc::{
    load_gc_candidates, reachable_keys, reachable_keys_with_retained_history_and_staging,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::SketchRef;
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
        let Some(manifest) = Manifest::read(store, namespace)
            .await
            .unwrap_or_else(|error| panic!("manifest read failed for {namespace}: {error}"))
        else {
            return vec![violation(
                ViolationId::I15ManifestLineage,
                op_index,
                namespace,
                "live manifest was missing during S3 oracle check",
                json!({ "namespace": namespace }),
            )];
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

        let listed = store
            .list_prefix(&format!("{namespace}/"))
            .await
            .unwrap_or_else(|error| panic!("S3 prefix list failed for {namespace}: {error}"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        let reachable = reachable_keys_for_s3_oracle(store, namespace, &manifest).await;
        let mut missing = reachable
            .difference(&listed)
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

        for entry in Manifest::list_history(store, namespace)
            .await
            .unwrap_or_else(|error| panic!("history list failed for {namespace}: {error}"))
        {
            let Some(bytes) = history_bytes_for_oracle(store, namespace, &entry.key).await else {
                continue;
            };
            let hash = xxh3_64(&bytes);
            let key = (namespace.to_string(), entry.version);
            if let Some(previous) = self.history_hashes.insert(key, hash) {
                if previous != hash {
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
            let history_manifest = Manifest::from_bytes(&bytes).unwrap_or_else(|error| {
                panic!("history manifest decode failed for {}: {error}", entry.key)
            });
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
    let Some(manifest) = Manifest::read(store, target)
        .await
        .unwrap_or_else(|error| panic!("clone target manifest read failed for {target}: {error}"))
    else {
        return vec![violation(
            ViolationId::I9Clone,
            op_index,
            target,
            "clone target manifest was missing after successful clone",
            json!({ "target": target }),
        )];
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
    let Some(manifest) = Manifest::read(store, namespace)
        .await
        .unwrap_or_else(|error| panic!("quiescent manifest read failed for {namespace}: {error}"))
    else {
        return vec![violation(
            ViolationId::I16Quiescence,
            op_index,
            namespace,
            "manifest was missing at quiescence",
            json!({ "namespace": namespace }),
        )];
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
    let reachable = reachable_keys_for_s3_oracle(store, namespace, &manifest).await;
    let listed = store
        .list_prefix(&format!("{namespace}/"))
        .await
        .unwrap_or_else(|error| panic!("quiescent prefix list failed for {namespace}: {error}"));
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
    let candidates = load_gc_candidates(store, namespace)
        .await
        .unwrap_or_else(|error| {
            panic!("quiescent GC candidate load failed for {namespace}: {error}")
        });
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
    let Some(manifest) = Manifest::read(store, namespace)
        .await
        .unwrap_or_else(|error| panic!("sketch manifest read failed for {namespace}: {error}"))
    else {
        return vec![violation(
            ViolationId::I17SketchPublication,
            op_index,
            namespace,
            "manifest was missing during v4 sketch publication check",
            json!({ "namespace": namespace }),
        )];
    };

    let mut violations = Vec::new();
    for segment in &manifest.segments {
        let Some(sketch) = segment.sketch.as_ref() else {
            continue;
        };
        match store.get(&sketch.key).await {
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
) -> BTreeSet<String> {
    match reachable_keys_with_retained_history_and_staging(
        store,
        namespace,
        manifest,
        &BTreeSet::new(),
    )
    .await
    {
        Ok(keys) => keys,
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
                Ok(keys) => keys,
                Err(retry_error) => {
                    eprintln!(
                        "retained-history reachability raced during S3 oracle check for {namespace}; \
                         using live manifest reachability only: first_error={first_error}; \
                         retry_error={retry_error}"
                    );
                    reachable_keys(namespace, manifest)
                }
            }
        }
    }
}

async fn history_bytes_for_oracle(
    store: &ZeppelinStore,
    namespace: &str,
    key: &str,
) -> Option<bytes::Bytes> {
    match store.get(key).await {
        Ok(bytes) => Some(bytes),
        Err(first_error) => {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            match store.get(key).await {
                Ok(bytes) => Some(bytes),
                Err(retry_error) => {
                    eprintln!(
                        "retained history key disappeared during S3 oracle check for \
                         {namespace}: key={key}; first_error={first_error}; \
                         retry_error={retry_error}"
                    );
                    None
                }
            }
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
    use zeppelin::wal::manifest::SketchRef;

    use super::*;

    const V4_ROTATION_SEED: u64 = 0x5a45_5050_454c_494e;

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
}
