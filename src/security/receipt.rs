//! Signed structural retrieval receipts.
//!
//! This file owns the receipt lifecycle: assembling a [`RetrievalReceipt`] over
//! an already-completed query, signing it with the node's published Ed25519
//! key, and verifying one later in a stable first-divergence order. It also
//! owns `AuthenticatedManifestArtifactInventory`, the related primitive that
//! lets non-query callers authenticate a manifest's immutable-artifact
//! inventory before they consume any artifact body. Finally, it owns the
//! canonical JSON encoding (`canonical_json_bytes`) that every hash in the
//! security subsystem is computed over — `audit_chain.rs` uses it too.
//!
//! It deliberately does **not** own the things it attests to. Query execution
//! collects the traversal parameters and the touched-artifact set; `merkle.rs`
//! owns tree construction and inclusion-path checking; `wal/manifest.rs` owns
//! the artifact hash inventory, the root envelope, and history retention;
//! `delegation.rs` owns the keys and signature checking; `kernel.rs` owns
//! whether the verifying caller may resolve historical policy at all. This file
//! binds those pieces together and reports exactly where they disagree.
//!
//! ## Where this sits
//!
//! - `server/handlers/query.rs` calls `issue_receipt` after the query has
//!   finished, then emits an `AuditParams::ReceiptIssued` audit record.
//! - `server/handlers/receipt.rs` (`/v1/verify`) calls `verify_receipt` with a
//!   caller-supplied [`VerifyReceiptRequest`].
//! - `compaction/mod.rs` and `server/handlers/namespace.rs` use
//!   `AuthenticatedManifestArtifactInventory` when they read another
//!   namespace's immutable artifacts during branch materialization or copy.
//!
//! ```text
//! issue (query path, after execution)     verify (/v1/verify, in this order)
//! ----------------------------------      ----------------------------------
//! manifest artifact inventory             1. receipt signature
//!   key -> sha256(object body)            2. result digest
//!         |                               3. query hash
//!         v                               4. touched Merkle paths
//!  MerkleTree::build -> manifest_root     5. derived Merkle paths
//!         |                               6. manifest root signature
//!         | must equal manifest's own     7. retained generation agreement
//!         | signed root, or fail closed   8. policy filter hash
//!         v                               9. optional artifact re-fetch
//!  inclusion path per touched key                     |
//!         |                                           v
//!         v                              valid, or the first divergence
//!  canonical JSON -> Ed25519 (node key)
//! ```
//!
//! ## What a receipt actually proves
//!
//! Accept these claims and no more. [`VerificationMode`] has exactly one
//! variant, `Structural`, and the name is the contract.
//!
//! Proven when verification returns `valid`:
//!
//! - A holder of the private key behind the published signer `signer_node`
//!   attested to every other field of the receipt. The signature covers
//!   recursively key-sorted canonical JSON of the receipt with `signature`
//!   cleared, so `signer_node` itself is bound.
//! - The results and query the caller supplied are identical, under that same
//!   canonicalization, to the ones the issuer hashed.
//! - Every key in `touched` is a member of the artifact inventory whose Merkle
//!   root is `manifest_root`, at exactly the content hash recorded, with
//!   duplicate keys rejected. The same holds for `derived_touched` against
//!   `derived_root`.
//! - That `manifest_root` — bound together with the manifest version, fencing
//!   token, binding version, execution-state digest, and optional control-state
//!   digest — was signed by a published manifest signer.
//! - If the named generation is still readable (retained history, or the live
//!   manifest at that version), it agrees with the receipt field for field and
//!   rebuilds the same root and execution digest.
//! - With `refetch`, the named objects still hold bytes hashing to the recorded
//!   digests.
//!
//! **Not** proven, and important to say out loud:
//!
//! - *Not* that the search was executed correctly, or that the results are the
//!   true top-k. The traversal parameters are attested claims about routing, not
//!   a replayable proof of retrieval.
//! - *Not* that `touched` is complete. Membership of each listed key is proven;
//!   an unlisted read is not detectable from the receipt.
//! - *Not* independent of the issuing node. The same node signs both the
//!   manifest root envelope and the receipt, so possession of that key is enough
//!   to produce a fully self-consistent receipt. There is no external witness or
//!   transparency log; the trust root is the signer inventory published under
//!   `_security/signers/`.
//! - *Not* freshness or single-use. `receipt_id` and `issued_at` are attested
//!   claims; nothing here prevents replaying an old, still-valid receipt.
//! - *Not* the manifest binding, when `manifest_history_checked` is `false`.
//!   That means the generation was no longer retained, the check was skipped,
//!   and the result can still be `valid` on the node signature alone.
//! - *Not* the policy binding, when `policy_filter_check` is `Unchecked` — the
//!   verifier lacked `SecurityAdminRead`, or the deployment had no policy
//!   authority or checksum to resolve against.
//! - For `CheckedDelegatedPolicyComponent`, only the *parent's* policy predicate
//!   was resolved. A delegated token's own narrowing stays redacted, so
//!   `enforced_filter_hash` is intentionally not required to equal the policy
//!   hash for a delegated principal.
//!
//! ## Fail-closed issuance
//!
//! A receipt is never weakened to make issuance succeed. If the manifest lacks a
//! complete artifact-hash inventory, a Merkle root, a root signature, a signer
//! identity, a binding version, or a matching execution digest — or if the query
//! could not hash every derived artifact it consumed — `issue_receipt` returns
//! `SecurityError::ReceiptsUnavailableUnhashed` and the caller gets no receipt at
//! all. Legacy manifests reach receipt-capable state only through an explicit
//! compaction upgrade; the query path never backfills hashes.
//!
//! ## Reading map
//!
//! 1. [`RetrievalReceipt`] — every field that the signature binds.
//! 2. `issue_receipt` — assembly order, and each fail-closed precondition.
//! 3. `verify_receipt` — the checks in exactly the order
//!    [`ReceiptDivergence`] documents.
//! 4. `retained_manifest_matches_receipt` — what "the retained generation
//!    agrees" concretely means.
//! 5. `AuthenticatedManifestArtifactInventory` — the separate authenticate-then-
//!    `verify_body` contract used outside the query path.
//! 6. `canonical_json_bytes` and `canonicalize_value` — the crate's hashing
//!    canonicalization, shared with `audit_chain.rs`.
//!
//! ## Invariants
//!
//! - **Canonicalize before hashing.** Every digest goes through
//!   `canonical_json_bytes`, which recursively sorts object keys through a
//!   `BTreeMap`. Hashing `serde_json` output directly would make a digest depend
//!   on map iteration order.
//! - **Receipts stay self-describing.** `manifest_control_state_digest` carries
//!   `skip_serializing_if`, and omitting it is precisely what lets a legacy V1
//!   receipt reserialize to its exact already-signed bytes. This type must never
//!   move to a non-self-describing format.
//! - **Artifact bodies are verified at the reader.**
//!   `AuthenticatedManifestArtifactInventory::authenticate` performs signer
//!   reads only; it never fetches data artifacts. Callers pass the exact key and
//!   the exact bytes they consumed to `verify_body`, which closes the
//!   verify-then-reread window in workflows that copy or rebuild immutable data.
//!   A key outside the inventory and a body whose digest disagrees both fail.
//! - **Divergences are outcomes; failures are errors.** A structural
//!   disagreement returns `Ok(VerifyReceiptResponse { valid: false, .. })` with
//!   `first_divergence` set. Storage failures and undecodable input propagate as
//!   errors instead of being reported as a clean "invalid".
//! - **Immutability is assumed, not enforced here.** The content hashes are only
//!   meaningful because WAL fragments and segments are write-once. `refetch`
//!   detects a violation after the fact; it does not prevent one.
//!
//! ## Rust concepts used here
//!
//! `ReceiptIssue<'a>` is a struct of borrows rather than a long argument list.
//! The lifetime makes the contract explicit: `issue_receipt` may read all of it
//! during the call and may not retain any of it afterwards, which the compiler
//! checks. In Java this would be a parameter object holding references with no
//! such guarantee; in C, a `const`-qualified struct of pointers whose validity
//! nobody verifies.
//!
//! Digests are `[u8; 32]` — fixed-size arrays that are `Copy`, so passing one
//! around moves 32 bytes on the stack with no allocation and no shared
//! ownership. `BTreeSet` and `BTreeMap` are chosen over their hashing
//! counterparts specifically because their iteration order is deterministic and
//! that order feeds a signature.
//!
//! `AuthenticatedManifestArtifactInventory` keeps its map private and is
//! `pub(crate)`. That is the type system carrying an invariant: a caller cannot
//! reach in and treat authenticated hashes as an ordinary key catalogue, and
//! cannot construct the type without passing the full authentication path.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::types::SearchResult;
use crate::wal::manifest::{manifest_root_signing_bytes, Manifest, ReceiptBindingVersion};

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
    /// Optional versioned retention/lineage digest bound by the root envelope.
    ///
    /// The absent field is skipped so reserializing a legacy V1 receipt keeps
    /// its exact already-signed bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_control_state_digest: Option<[u8; 32]>,
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

/// Authenticated immutable-object inventory carried by one exact manifest.
///
/// Construction verifies the manifest's complete receipt binding before any
/// artifact body can be accepted: reachable-key/hash closure, Merkle root,
/// execution digest, versioned control digest, and the published node
/// signature over the complete root envelope. The inventory remains opaque so
/// callers cannot accidentally treat its hashes as an unsigned key catalogue.
///
/// Artifact readers then pass each exact physical key and the exact bytes they
/// consumed to [`Self::verify_body`]. Keeping storage I/O at the reader avoids a
/// verify-then-reread race in workflows that rebuild or copy immutable data.
#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedManifestArtifactInventory {
    artifacts: BTreeMap<String, [u8; 32]>,
}

impl AuthenticatedManifestArtifactInventory {
    /// Authenticate one manifest's exact immutable-artifact inventory.
    ///
    /// This performs signer-inventory reads but does not fetch data artifacts.
    /// The caller must verify every body it consumes with [`Self::verify_body`].
    pub(crate) async fn authenticate(
        store: &ZeppelinStore,
        namespace: &str,
        manifest: &Manifest,
    ) -> ZeppelinResult<Self> {
        let artifacts = manifest.receipt_artifacts(namespace)?;
        let rebuilt_root = MerkleTree::build(artifacts)?.root();
        let manifest_root = manifest.merkle_root().ok_or_else(|| {
            authenticated_manifest_error("manifest omitted its authenticated artifact root")
        })?;
        if manifest_root != rebuilt_root {
            return Err(authenticated_manifest_error(
                "manifest artifact inventory did not rebuild its authenticated root",
            ));
        }

        let binding_version = manifest.receipt_binding_version().ok_or_else(|| {
            authenticated_manifest_error("manifest omitted its receipt binding version")
        })?;
        let manifest_state_digest = manifest.receipt_state_digest().ok_or_else(|| {
            authenticated_manifest_error("manifest omitted its execution-state digest")
        })?;
        if manifest.recompute_receipt_state_digest(namespace)? != manifest_state_digest {
            return Err(authenticated_manifest_error(
                "manifest execution state diverged from its authenticated digest",
            ));
        }

        let control_state_digest = manifest.control_state_digest();
        match binding_version {
            ReceiptBindingVersion::V1 | ReceiptBindingVersion::V2Origins => {
                if control_state_digest.is_some() {
                    return Err(authenticated_manifest_error(
                        "manifest binding version forbids a control-state digest",
                    ));
                }
            }
            ReceiptBindingVersion::V3Roots | ReceiptBindingVersion::V4Lineage => {
                let expected = control_state_digest.ok_or_else(|| {
                    authenticated_manifest_error(
                        "manifest binding version omitted its control-state digest",
                    )
                })?;
                if manifest.recompute_control_state_digest(namespace)? != expected {
                    return Err(authenticated_manifest_error(
                        "manifest control state diverged from its authenticated digest",
                    ));
                }
            }
        }

        let signer_node = manifest.root_signer_node().ok_or_else(|| {
            authenticated_manifest_error("manifest omitted its root signer identity")
        })?;
        let signature = manifest
            .root_signature()
            .ok_or_else(|| authenticated_manifest_error("manifest omitted its root signature"))?;
        let signing_bytes = manifest_root_signing_bytes(
            manifest_root,
            manifest.version(),
            manifest.fencing_token(),
            binding_version,
            manifest_state_digest,
            control_state_digest,
        )?;
        if !super::delegation::verify_published_signature(
            store,
            signer_node,
            &signing_bytes,
            signature,
        )
        .await?
        {
            return Err(authenticated_manifest_error(
                "manifest root signature did not verify",
            ));
        }

        Ok(Self {
            artifacts: artifacts.clone(),
        })
    }

    /// Verify one exact physical key and the exact immutable body read for it.
    ///
    /// Keys outside the authenticated inventory and bodies whose SHA-256 does
    /// not match the signed hash both fail closed. No storage read is performed.
    pub(crate) fn verify_body(&self, key: &str, bytes: &[u8]) -> ZeppelinResult<()> {
        let expected = self.artifacts.get(key).ok_or_else(|| {
            authenticated_manifest_error(
                "consumed artifact is outside the authenticated manifest inventory",
            )
        })?;
        let observed = <[u8; 32]>::from(Sha256::digest(bytes));
        if &observed != expected {
            return Err(authenticated_manifest_error(
                "consumed artifact body diverged from its authenticated hash",
            ));
        }
        Ok(())
    }
}

fn authenticated_manifest_error(reason: &'static str) -> ZeppelinError {
    SecurityError::InvalidReceipt(reason.to_string()).into()
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
    let touched = touched_artifacts
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
        manifest_control_state_digest: manifest.control_state_digest(),
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
        || manifest.control_state_digest() != receipt.manifest_control_state_digest
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
        receipt.manifest_control_state_digest,
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

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use ulid::Ulid;

    use super::*;
    use crate::security::delegation::PublishedObjectSigner;
    use crate::storage::store::ObjectSigner;
    use crate::wal::fragment::WalFragment;
    use crate::wal::manifest::FragmentRef;

    async fn signed_manifest_fixture() -> (
        ZeppelinStore,
        Arc<PublishedObjectSigner>,
        Manifest,
        String,
        Bytes,
    ) {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key_file = tempfile::NamedTempFile::new().expect("signing-key fixture must create");
        std::fs::write(key_file.path(), "11".repeat(32)).expect("signing-key fixture must write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(key_file.path(), std::fs::Permissions::from_mode(0o600))
                .expect("signing-key fixture permissions must tighten");
        }
        let signer = PublishedObjectSigner::compose(
            store.signer_detached_clone(),
            key_file.path().to_path_buf(),
        )
        .await
        .expect("published object signer must compose");
        let object_signer: Arc<dyn ObjectSigner> = signer.clone();
        store
            .install_object_signer(object_signer)
            .expect("published object signer must install");

        let namespace = "authenticated-manifest-inventory";
        let fragment_id = Ulid::from_parts(1, 7);
        let artifact_key = WalFragment::s3_key(namespace, &fragment_id);
        let artifact_body = Bytes::from_static(b"exact immutable artifact body");
        store
            .put(&artifact_key, artifact_body.clone())
            .await
            .expect("artifact fixture must upload");
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .expect("manifest fixture must bind an incarnation");
        manifest.add_fragment(FragmentRef {
            id: fragment_id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: artifact_body.len() as u64,
            artifact_origin: None,
        });
        manifest
            .write(&store, namespace)
            .await
            .expect("signed manifest fixture must publish");
        (store, signer, manifest, artifact_key, artifact_body)
    }

    #[tokio::test]
    async fn authenticated_inventory_accepts_only_exact_inventory_key_and_body() {
        let (store, _signer, manifest, artifact_key, artifact_body) =
            signed_manifest_fixture().await;
        let inventory = AuthenticatedManifestArtifactInventory::authenticate(
            &store,
            "authenticated-manifest-inventory",
            &manifest,
        )
        .await
        .expect("signed manifest inventory must authenticate");

        inventory
            .verify_body(&artifact_key, &artifact_body)
            .expect("exact physical key and bytes must verify");
        assert!(matches!(
            inventory.verify_body(&artifact_key, b"tampered"),
            Err(ZeppelinError::Security(SecurityError::InvalidReceipt(_)))
        ));
        assert!(matches!(
            inventory.verify_body("another/physical/key", &artifact_body),
            Err(ZeppelinError::Security(SecurityError::InvalidReceipt(_)))
        ));
    }

    #[tokio::test]
    async fn authenticated_inventory_rejects_execution_divergence_and_unknown_signer() {
        let (store, _signer, manifest, _artifact_key, _artifact_body) =
            signed_manifest_fixture().await;
        let mut execution_tampered = manifest.clone();
        execution_tampered.fragments[0].vector_count += 1;
        assert!(matches!(
            AuthenticatedManifestArtifactInventory::authenticate(
                &store,
                "authenticated-manifest-inventory",
                &execution_tampered,
            )
            .await,
            Err(ZeppelinError::Security(SecurityError::InvalidReceipt(_)))
        ));

        let unknown_signer_store = ZeppelinStore::new(Arc::new(InMemory::new()));
        assert!(matches!(
            AuthenticatedManifestArtifactInventory::authenticate(
                &unknown_signer_store,
                "authenticated-manifest-inventory",
                &manifest,
            )
            .await,
            Err(ZeppelinError::Security(SecurityError::InvalidReceipt(_)))
        ));
    }
}
