use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use xxhash_rust::xxh3::xxh3_64;
use zeppelin::compaction::gc::{
    load_gc_candidates, reachable_keys, reachable_keys_with_retained_history_and_staging,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::Manifest;

use super::oracle::{Violation, ViolationId};

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
        let reachable = reachable_keys_with_retained_history_and_staging(
            store,
            namespace,
            &manifest,
            &BTreeSet::new(),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("reachable retained-history check failed for {namespace}: {error}")
        });
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
            let bytes = store
                .get(&entry.key)
                .await
                .unwrap_or_else(|error| panic!("history read failed for {}: {error}", entry.key));
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
    let reachable = reachable_keys_with_retained_history_and_staging(
        store,
        namespace,
        &manifest,
        &BTreeSet::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("quiescent reachability failed for {namespace}: {error}"));
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
