//! Signed structural retrieval receipts.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::types::SearchResult;
use crate::wal::manifest::{manifest_root_signing_bytes, Manifest, ReceiptBindingVersion};
use crate::wal::WalFragment;

use super::{
    DecisionId, MerklePath, MerkleTree, PolicyVersion, Principal, PrincipalId, SecurityError,
};

/// Verification strength claimed by a receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMode {
    /// Signature, result digest, policy binding, and Merkle structure only.
    Structural,
}

/// Retrieval source kind bound into one executed traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalSourceKind {
    /// Approximate nearest-neighbor retrieval.
    Ann,
    /// BM25 lexical retrieval.
    Bm25,
}

/// Closed scoring metric bound into one executed retrieval source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalMetric {
    /// BM25 lexical relevance.
    Bm25,
    /// Cosine distance.
    Cosine,
    /// Squared Euclidean distance.
    Euclidean,
    /// Negated dot-product similarity.
    DotProduct,
}

impl From<crate::types::DistanceMetric> for TraversalMetric {
    fn from(metric: crate::types::DistanceMetric) -> Self {
        match metric {
            crate::types::DistanceMetric::Cosine => Self::Cosine,
            crate::types::DistanceMetric::Euclidean => Self::Euclidean,
            crate::types::DistanceMetric::DotProduct => Self::DotProduct,
        }
    }
}

/// Exact controls and production routing evidence for one retrieval source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalSourceParams {
    /// Zero-based request-source position.
    pub source_index: usize,
    /// Executed source family.
    pub kind: TraversalSourceKind,
    /// Effective IVF/beam width, when ANN participated.
    pub nprobe: Option<usize>,
    /// Stable distance or lexical metric.
    pub metric: TraversalMetric,
    /// Centroid or leaf indexes captured at the production execution seam.
    pub probed_centroids: Vec<usize>,
    /// Physical cluster indexes whose row or sidecar artifacts were actually scanned.
    pub scanned_clusters: Vec<usize>,
    /// Hierarchical routing-node IDs fetched by the production beam traversal.
    pub probed_routing_nodes: Vec<String>,
    /// Whether this source loaded per-cluster attributes for filtering or projection.
    pub attributes_loaded: bool,
}

/// Exact multi-source traversal bound into a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalParams {
    /// Requested final result count.
    pub top_k: usize,
    /// Executed sources in request order.
    pub sources: Vec<TraversalSourceParams>,
}

/// One exact immutable object bound to the manifest root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TouchedArtifact {
    /// Full object-store key.
    pub key: String,
    /// SHA-256 of the immutable object body.
    pub content_hash: [u8; 32],
    /// Inclusion path from this key/hash leaf to `manifest_root`.
    pub merkle_path: MerklePath,
}

/// Signed proof of one authorized retrieval context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalReceipt {
    /// Collision-resistant receipt identity.
    pub receipt_id: Ulid,
    /// Namespace queried.
    pub namespace: String,
    /// Authenticated identity used for authorization.
    pub principal_id: PrincipalId,
    /// Parent identity for a delegated principal.
    pub delegation_parent: Option<PrincipalId>,
    /// Authoritative policy generation used for the decision.
    pub policy_version: PolicyVersion,
    /// Checksum of the exact immutable policy snapshot used for the decision.
    pub policy_checksum: Option<String>,
    /// Exact authorization decision identity.
    pub decision_id: DecisionId,
    /// Hash of the server-owned mandatory filter, never the predicate itself.
    pub enforced_filter_hash: Option<[u8; 32]>,
    /// Hash of the historical policy-owned filter before token narrowing.
    pub policy_filter_hash: Option<[u8; 32]>,
    /// Hash of canonical request JSON.
    pub query_hash: [u8; 32],
    /// Effective retrieval controls.
    pub traversal: TraversalParams,
    /// Canonical root over the visible immutable object inventory.
    pub manifest_root: [u8; 32],
    /// Canonical digest of the exact query-routing manifest projection.
    pub manifest_state_digest: [u8; 32],
    /// Stable projection version used to compute `manifest_state_digest`.
    pub manifest_binding_version: ReceiptBindingVersion,
    /// Manifest generation used by the query.
    pub manifest_version: u64,
    /// All immutable objects represented by this structural proof.
    pub touched: Vec<TouchedArtifact>,
    /// Canonical root over lazy policy-scope artifacts consumed by this query.
    pub derived_root: Option<[u8; 32]>,
    /// Lazy policy-scope artifacts bound directly by the receipt signature.
    pub derived_touched: Vec<TouchedArtifact>,
    /// Hash of canonical result JSON.
    pub result_digest: [u8; 32],
    /// Trusted issue time.
    pub issued_at: DateTime<Utc>,
    /// Explicit proof-strength label.
    pub verification_mode: VerificationMode,
    /// Node signer that issued this receipt.
    pub signer_node: String,
    /// Node signature over every preceding receipt field.
    pub signature: Vec<u8>,
    /// Node signature carried by the selected manifest generation.
    pub manifest_root_signature: Vec<u8>,
    /// Node signer that published the selected manifest generation.
    pub manifest_signer_node: String,
    /// Fencing generation bound by the manifest-root signature.
    pub manifest_fencing_token: u64,
}

/// Stable first-divergence fields returned by structural verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptDivergence {
    /// Receipt signature did not verify.
    Signature,
    /// Caller-supplied results did not match the signed digest.
    ResultDigest,
    /// Caller-supplied canonical query did not match the signed request.
    QueryHash,
    /// Touched-key inventory was malformed, duplicated, or did not rebuild the root.
    MerklePath,
    /// The selected generation's root signature did not verify.
    ManifestRootSignature,
    /// Authoritative history disagreed with the signed generation.
    ManifestHistory,
    /// Privileged historical policy resolution disagreed with the receipt.
    PolicyFilterHash,
    /// Optional re-fetch observed different immutable bytes.
    ArtifactRefetch,
}

/// Whether policy-filter consistency could be independently resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyFilterCheck {
    /// The exact historical policy predicate (including absence) was checked.
    Checked,
    /// The policy component was checked; delegated narrowing remains redacted.
    CheckedDelegatedPolicyComponent,
    /// The verifying caller was not authorized to resolve policy predicates.
    Unchecked,
}

/// Strict request body for `/v1/verify`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyReceiptRequest {
    /// Receipt returned by an earlier query.
    pub receipt: RetrievalReceipt,
    /// Exact result array returned with that receipt.
    pub results: Vec<SearchResult>,
    /// Exact canonical JSON query document used to issue the receipt.
    pub query: serde_json::Value,
    /// Re-download and hash every touched artifact.
    #[serde(default)]
    pub refetch: bool,
}

/// Structural receipt-verification outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyReceiptResponse {
    /// True only when every applicable structural check succeeded.
    pub valid: bool,
    /// First check that diverged, in the documented verification order.
    pub first_divergence: Option<ReceiptDivergence>,
    /// Whether policy-filter consistency was independently inspected.
    pub policy_filter_check: PolicyFilterCheck,
    /// Whether the named retained manifest generation was still available and checked.
    pub manifest_history_checked: bool,
    /// Number of touched objects re-fetched from authoritative storage.
    pub refetched_artifacts: usize,
}

impl VerifyReceiptResponse {
    fn invalid(
        divergence: ReceiptDivergence,
        policy_filter_check: PolicyFilterCheck,
        history_checked: bool,
        refetched_artifacts: usize,
    ) -> Self {
        Self {
            valid: false,
            first_divergence: Some(divergence),
            policy_filter_check,
            manifest_history_checked: history_checked,
            refetched_artifacts,
        }
    }
}

/// Parameters captured at the single-query orchestration boundary.
pub(crate) struct ReceiptIssue<'a> {
    pub store: &'a ZeppelinStore,
    pub namespace: &'a str,
    pub principal: &'a Principal,
    pub decision_id: DecisionId,
    pub policy_version: PolicyVersion,
    pub policy_checksum: Option<&'a str>,
    pub mandatory_filter: Option<&'a crate::types::Filter>,
    pub policy_filter: Option<&'a crate::types::Filter>,
    pub query: &'a serde_json::Value,
    pub traversal: TraversalParams,
    pub results: &'a [SearchResult],
    pub manifest: &'a Manifest,
    pub derived_artifacts: &'a BTreeMap<String, [u8; 32]>,
    pub derived_touched_artifacts: &'a BTreeSet<String>,
    pub derived_artifacts_complete: bool,
    pub touched_artifacts: &'a BTreeSet<String>,
    pub issued_at: DateTime<Utc>,
}

/// Issue one signed receipt over an already-completed single query.
pub(crate) fn issue_receipt(issue: ReceiptIssue<'_>) -> ZeppelinResult<RetrievalReceipt> {
    let ReceiptIssue {
        store,
        namespace,
        principal,
        decision_id,
        policy_version,
        policy_checksum,
        mandatory_filter,
        policy_filter,
        query,
        traversal,
        results,
        manifest,
        derived_artifacts,
        derived_touched_artifacts,
        derived_artifacts_complete,
        touched_artifacts,
        issued_at,
    } = issue;
    if !derived_artifacts_complete {
        return Err(SecurityError::ReceiptsUnavailableUnhashed.into());
    }
    let artifacts = manifest.receipt_artifacts(namespace)?;
    let tree = MerkleTree::build(artifacts)?;
    let root = manifest
        .merkle_root()
        .filter(|root| root == &tree.root())
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?;
    let recomputed_state_digest = manifest.recompute_receipt_state_digest(namespace)?;
    let manifest_state_digest = manifest
        .receipt_state_digest()
        .filter(|digest| digest == &recomputed_state_digest)
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?;
    let manifest_binding_version = manifest
        .receipt_binding_version()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?;
    let manifest_root_signature = manifest
        .root_signature()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?
        .to_vec();
    let manifest_signer_node = manifest
        .root_signer_node()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?
        .to_string();
    let touched_keys = logical_touched_artifact_keys(namespace, manifest, touched_artifacts);
    let touched = touched_keys
        .iter()
        .map(|key| {
            let content_hash = artifacts.get(key).ok_or_else(|| {
                SecurityError::InvalidReceipt(format!(
                    "query touched artifact {key} outside the signed manifest inventory"
                ))
            })?;
            Ok(TouchedArtifact {
                key: key.clone(),
                content_hash: *content_hash,
                merkle_path: tree.proof(key).ok_or_else(|| {
                    SecurityError::InvalidReceipt(format!(
                        "missing Merkle proof for visible artifact {key}"
                    ))
                })?,
            })
        })
        .collect::<Result<Vec<_>, SecurityError>>()?;
    let (derived_root, derived_touched) = if derived_artifacts.is_empty() {
        (None, Vec::new())
    } else {
        let derived_tree = MerkleTree::build(derived_artifacts)?;
        let derived_root = derived_tree.root();
        let derived_touched = derived_touched_artifacts
            .iter()
            .map(|key| {
                let content_hash = derived_artifacts.get(key).ok_or_else(|| {
                    SecurityError::InvalidReceipt(format!(
                        "query touched derived artifact {key} outside the signed derived inventory"
                    ))
                })?;
                Ok(TouchedArtifact {
                    key: key.clone(),
                    content_hash: *content_hash,
                    merkle_path: derived_tree.proof(key).ok_or_else(|| {
                        SecurityError::InvalidReceipt(format!(
                            "missing Merkle proof for derived artifact {key}"
                        ))
                    })?,
                })
            })
            .collect::<Result<Vec<_>, SecurityError>>()?;
        (Some(derived_root), derived_touched)
    };

    let mut receipt = RetrievalReceipt {
        receipt_id: Ulid::new(),
        namespace: namespace.to_string(),
        principal_id: principal.id.clone(),
        delegation_parent: principal.delegation_parent.clone(),
        policy_version,
        policy_checksum: policy_checksum.map(str::to_string),
        decision_id,
        enforced_filter_hash: mandatory_filter.map(canonical_json_hash).transpose()?,
        policy_filter_hash: policy_filter.map(canonical_json_hash).transpose()?,
        query_hash: canonical_json_hash(query)?,
        traversal,
        manifest_root: root,
        manifest_state_digest,
        manifest_binding_version,
        manifest_version: manifest.version(),
        touched,
        derived_root,
        derived_touched,
        result_digest: canonical_json_hash(results)?,
        issued_at,
        verification_mode: VerificationMode::Structural,
        signer_node: String::new(),
        signature: Vec::new(),
        manifest_root_signature,
        manifest_signer_node,
        manifest_fencing_token: manifest.fencing_token(),
    };
    receipt.signer_node = store
        .object_signer_node()?
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?;
    let unsigned = receipt.unsigned_bytes()?;
    let (signer_node, signature) = store
        .sign_object(&unsigned)?
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)?;
    if signer_node != receipt.signer_node {
        return Err(ZeppelinError::Config(
            "receipt signer changed while issuing one receipt".to_string(),
        ));
    }
    receipt.signature = signature;
    Ok(receipt)
}

/// Resolve the exact immutable artifact set consumed by one query.
///
/// The manifest root still commits to the complete reachable inventory. The
/// execution layer records successful cache/object reads at their physical
/// seams. Receipt issuance adds the WAL fragments consumed by the selected
/// consistency path and then requires every key to exist in the signed manifest
/// inventory; no compatibility-name filter may silently discard a traced read.
fn logical_touched_artifact_keys(
    namespace: &str,
    manifest: &Manifest,
    execution_artifacts: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut touched = manifest
        .fragments
        .iter()
        .map(|fragment| WalFragment::s3_key(namespace, &fragment.id))
        .collect::<BTreeSet<_>>();
    touched.extend(execution_artifacts.iter().cloned());
    touched
}

impl RetrievalReceipt {
    fn unsigned_bytes(&self) -> ZeppelinResult<Vec<u8>> {
        let mut unsigned = self.clone();
        unsigned.signature.clear();
        canonical_json_bytes(&unsigned)
    }
}

fn retained_manifest_matches_receipt(
    manifest: &Manifest,
    namespace: &str,
    receipt: &RetrievalReceipt,
) -> bool {
    if manifest.fencing_token() != receipt.manifest_fencing_token
        || manifest.merkle_root() != Some(receipt.manifest_root)
        || manifest.receipt_state_digest() != Some(receipt.manifest_state_digest)
        || manifest.receipt_binding_version() != Some(receipt.manifest_binding_version)
        || manifest.root_signature() != Some(receipt.manifest_root_signature.as_slice())
        || manifest.root_signer_node() != Some(receipt.manifest_signer_node.as_str())
    {
        return false;
    }
    let artifacts_match = manifest
        .receipt_artifacts(namespace)
        .ok()
        .and_then(|artifacts| MerkleTree::build(artifacts).ok())
        .is_some_and(|tree| tree.root() == receipt.manifest_root);
    let state_matches = manifest
        .recompute_receipt_state_digest(namespace)
        .is_ok_and(|digest| digest == receipt.manifest_state_digest);
    artifacts_match && state_matches
}

/// Verify one receipt in the specified first-divergence order.
pub(crate) async fn verify_receipt(
    store: &ZeppelinStore,
    security: &super::SecurityKernel,
    verifier: &Principal,
    context: &super::RequestContext,
    request: &VerifyReceiptRequest,
) -> ZeppelinResult<VerifyReceiptResponse> {
    let receipt = &request.receipt;
    if !super::delegation::verify_published_signature(
        store,
        &receipt.signer_node,
        &receipt.unsigned_bytes()?,
        &receipt.signature,
    )
    .await?
    {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::Signature,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    if canonical_json_hash(&request.results)? != receipt.result_digest {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::ResultDigest,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    if canonical_json_hash(&request.query)? != receipt.query_hash {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::QueryHash,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    let mut unique = BTreeSet::new();
    if receipt.touched.iter().any(|artifact| {
        !unique.insert(artifact.key.as_str())
            || !artifact.merkle_path.verify(
                &artifact.key,
                &artifact.content_hash,
                &receipt.manifest_root,
            )
    }) {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::MerklePath,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    let mut derived_unique = BTreeSet::new();
    let derived_valid = match receipt.derived_root {
        None => receipt.derived_touched.is_empty(),
        Some(derived_root) if !receipt.derived_touched.is_empty() => {
            receipt.derived_touched.iter().all(|artifact| {
                derived_unique.insert(artifact.key.as_str())
                    && artifact.merkle_path.verify(
                        &artifact.key,
                        &artifact.content_hash,
                        &derived_root,
                    )
            })
        }
        Some(_) => false,
    };
    if !derived_valid {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::MerklePath,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    let root_binding = manifest_root_signing_bytes(
        receipt.manifest_root,
        receipt.manifest_version,
        receipt.manifest_fencing_token,
        receipt.manifest_binding_version,
        receipt.manifest_state_digest,
    )?;
    if !super::delegation::verify_published_signature(
        store,
        &receipt.manifest_signer_node,
        &root_binding,
        &receipt.manifest_root_signature,
    )
    .await?
    {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::ManifestRootSignature,
            PolicyFilterCheck::Unchecked,
            false,
            0,
        ));
    }

    let mut history =
        Manifest::read_history(store, &receipt.namespace, receipt.manifest_version).await?;
    if history.is_none() {
        history = Manifest::read(store, &receipt.namespace)
            .await?
            .filter(|live| live.version() == receipt.manifest_version);
    }
    let history_checked = history.is_some();
    if history.as_ref().is_some_and(|manifest| {
        !retained_manifest_matches_receipt(manifest, &receipt.namespace, receipt)
    }) {
        return Ok(VerifyReceiptResponse::invalid(
            ReceiptDivergence::ManifestHistory,
            PolicyFilterCheck::Unchecked,
            true,
            0,
        ));
    }

    let namespace = super::NamespaceId::new(receipt.namespace.clone())?;
    let policy_resolution = security
        .receipt_policy_filter(super::kernel::ReceiptPolicyLookup {
            verifier,
            context,
            receipt_principal: &receipt.principal_id,
            delegation_parent: receipt.delegation_parent.as_ref(),
            namespace: &namespace,
            version: receipt.policy_version,
            checksum: receipt.policy_checksum.as_deref(),
        })
        .await?;
    let policy_filter_check = match policy_resolution {
        super::kernel::ReceiptPolicyResolution::Resolved { filter, delegated } => {
            let expected_policy_hash = filter.as_ref().map(canonical_json_hash).transpose()?;
            let final_direct_mismatch =
                !delegated && receipt.enforced_filter_hash != expected_policy_hash;
            if receipt.policy_filter_hash != expected_policy_hash || final_direct_mismatch {
                return Ok(VerifyReceiptResponse {
                    valid: false,
                    first_divergence: Some(ReceiptDivergence::PolicyFilterHash),
                    policy_filter_check: if delegated {
                        PolicyFilterCheck::CheckedDelegatedPolicyComponent
                    } else {
                        PolicyFilterCheck::Checked
                    },
                    manifest_history_checked: history_checked,
                    refetched_artifacts: 0,
                });
            }
            if delegated {
                PolicyFilterCheck::CheckedDelegatedPolicyComponent
            } else {
                PolicyFilterCheck::Checked
            }
        }
        super::kernel::ReceiptPolicyResolution::Diverged { delegated } => {
            return Ok(VerifyReceiptResponse {
                valid: false,
                first_divergence: Some(ReceiptDivergence::PolicyFilterHash),
                policy_filter_check: if delegated {
                    PolicyFilterCheck::CheckedDelegatedPolicyComponent
                } else {
                    PolicyFilterCheck::Checked
                },
                manifest_history_checked: history_checked,
                refetched_artifacts: 0,
            });
        }
        super::kernel::ReceiptPolicyResolution::Unchecked => PolicyFilterCheck::Unchecked,
    };

    let mut refetched = 0;
    if request.refetch {
        for artifact in &receipt.touched {
            let observed = store.get(&artifact.key).await?;
            refetched += 1;
            if <[u8; 32]>::from(Sha256::digest(&observed)) != artifact.content_hash {
                return Ok(VerifyReceiptResponse::invalid(
                    ReceiptDivergence::ArtifactRefetch,
                    policy_filter_check,
                    history_checked,
                    refetched,
                ));
            }
        }
        for artifact in &receipt.derived_touched {
            let observed = store.get(&artifact.key).await?;
            refetched += 1;
            if <[u8; 32]>::from(Sha256::digest(&observed)) != artifact.content_hash {
                return Ok(VerifyReceiptResponse::invalid(
                    ReceiptDivergence::ArtifactRefetch,
                    policy_filter_check,
                    history_checked,
                    refetched,
                ));
            }
        }
    }

    Ok(VerifyReceiptResponse {
        valid: true,
        first_divergence: None,
        policy_filter_check,
        manifest_history_checked: history_checked,
        refetched_artifacts: refetched,
    })
}

/// SHA-256 over canonical recursively key-sorted JSON.
pub(crate) fn canonical_json_hash<T: Serialize + ?Sized>(value: &T) -> ZeppelinResult<[u8; 32]> {
    Ok(Sha256::digest(canonical_json_bytes(value)?).into())
}

pub(crate) fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> ZeppelinResult<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|error| {
        ZeppelinError::Serialization(format!("receipt canonicalization failed: {error}"))
    })?;
    serde_json::to_vec(&canonicalize_value(value)).map_err(|error| {
        ZeppelinError::Serialization(format!("receipt canonical encoding failed: {error}"))
    })
}

fn canonicalize_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_value).collect())
        }
        serde_json::Value::Object(values) => {
            let ordered = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_value(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        scalar => scalar,
    }
}
