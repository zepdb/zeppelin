//! One-shot, lease-fenced mutations of a source namespace's branch-root map.
//!
//! This module deliberately does not retry across manifest generations. A fork
//! view is bound to one exact source head; if another writer advances that
//! head, the later namespace-graph orchestrator must rebuild the view and its
//! digests before attempting another publication.

// Phase 04 intentionally lands this private storage primitive one phase before
// NamespaceGraph becomes its production caller. Feature-gated integration
// fixtures exercise it in this phase; Phase 05 removes the temporary liveness
// gap by composing the graph.
#![allow(dead_code)]

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::namespace::branching::BranchError;
use crate::namespace::manager::{NamespaceIndexConfig, NamespaceManager, NamespaceMetadata};
use crate::namespace::{BranchRoot, ManifestGeneration, NamespaceId, SourceDataPlaneConfigDigest};
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, IndexType};
use crate::wal::{Lease, LeaseManager, Manifest};

const SOURCE_DATA_PLANE_CONFIG_DOMAIN: &str = "zeppelin.source-data-plane-config.v1";

/// Complete identity required to publish one exact source-generation root.
///
/// The generation and manifest digest come from the caller's prepared fork
/// view. The mutation re-derives both from its ETag-bound source-head read; it
/// never silently retargets the root to a newer source generation.
#[derive(Debug, Clone)]
pub(crate) struct InsertBranchRootRequest {
    pub(crate) source_namespace: NamespaceId,
    pub(crate) root: BranchRoot,
    pub(crate) max_children: usize,
}

/// Complete identity required to remove one already-published source root.
///
/// Missing roots remain errors at this seam. Phase 07 is responsible for
/// proving target visibility is durably gone before it may reinterpret absence
/// as an idempotent lifecycle outcome.
#[derive(Debug, Clone)]
pub(crate) struct RemoveBranchRootRequest {
    pub(crate) source_namespace: NamespaceId,
    /// Exact source lifetime named by the target's immutable fork identity.
    pub(crate) expected_source_incarnation: crate::namespace::NamespaceIncarnationId,
    pub(crate) expected_root: BranchRoot,
}

#[derive(Serialize)]
struct SourceDataPlaneConfigBindingV1<'a> {
    domain: &'static str,
    dimensions: usize,
    distance_metric: DistanceMetric,
    index_type: IndexType,
    full_text_search: BTreeMap<&'a str, &'a FtsFieldConfig>,
    index_config: &'a NamespaceIndexConfig,
}

/// Compute the canonical interpretation-config digest used by roots and forks.
///
/// Namespace metadata stores FTS fields in a `HashMap`; collecting borrowed
/// entries into a `BTreeMap` before JSON serialization makes the digest
/// independent of randomized map iteration order.
pub(crate) fn source_data_plane_config_digest(
    metadata: &NamespaceMetadata,
    resolved_index_config: &NamespaceIndexConfig,
) -> Result<SourceDataPlaneConfigDigest> {
    let full_text_search = metadata
        .full_text_search
        .iter()
        .map(|(field, config)| (field.as_str(), config))
        .collect::<BTreeMap<_, _>>();
    let binding = SourceDataPlaneConfigBindingV1 {
        domain: SOURCE_DATA_PLANE_CONFIG_DOMAIN,
        dimensions: metadata.dimensions,
        distance_metric: metadata.distance_metric,
        index_type: metadata.index_type,
        full_text_search,
        index_config: resolved_index_config,
    };
    let bytes = serde_json::to_vec(&binding)?;
    Ok(SourceDataPlaneConfigDigest::new(
        Sha256::digest(bytes).into(),
    ))
}

/// Publish one exact root using the source namespace's normal writer lease.
///
/// An exact already-published root is returned idempotently. Otherwise the
/// expected root must name this authoritative metadata/config snapshot and the
/// exact ETag-bound live manifest bytes. Publication performs one manifest CAS;
/// a conflict is returned to the namespace graph without a hidden retry.
pub(crate) async fn insert_branch_root(
    store: &ZeppelinStore,
    namespace_manager: &NamespaceManager,
    lease_manager: &LeaseManager,
    request: InsertBranchRootRequest,
) -> Result<BranchRoot> {
    let namespace = request.source_namespace.to_string();
    let mut lease = lease_manager.acquire(&namespace).await?;
    let result = async {
        let metadata = namespace_manager
            .get_active_metadata_for_guarded_write(&namespace)
            .await?;
        let resolved_index_config = metadata.index_config.clone().unwrap_or_else(|| {
            NamespaceIndexConfig::from_indexing_config(&IndexingConfig::default())
        });
        insert_branch_root_with_lease(
            store,
            namespace_manager,
            lease_manager,
            &mut lease,
            &resolved_index_config,
            request,
        )
        .await
    }
    .await;
    if let Err(error) = lease_manager.release(&namespace, &lease).await {
        warn!(
            namespace,
            error = %error,
            "branch-root insertion lease release failed (best-effort)"
        );
    }
    result
}

pub(crate) async fn insert_branch_root_with_lease(
    store: &ZeppelinStore,
    namespace_manager: &NamespaceManager,
    lease_manager: &LeaseManager,
    lease: &mut Lease,
    resolved_index_config: &NamespaceIndexConfig,
    request: InsertBranchRootRequest,
) -> Result<BranchRoot> {
    let namespace = request.source_namespace.as_str();
    let metadata = namespace_manager
        .get_active_metadata_for_guarded_write(namespace)
        .await?;
    let incarnation = required_incarnation(&metadata)?;
    let (mut manifest, version) =
        Manifest::read_versioned_required_for_incarnation(store, namespace, incarnation.as_uuid())
            .await?;
    if version.is_deletion_fenced() {
        return Err(ZeppelinError::NamespaceDeleting {
            namespace: namespace.to_string(),
        });
    }

    if let Some(existing) = manifest.branch_roots().get(&request.root.branch_id) {
        return if existing == &request.root {
            verify_rooted_history(store, namespace, &manifest, existing, None).await?;
            Ok(existing.clone())
        } else {
            Err(BranchError::BranchRootConflict {
                branch_id: request.root.branch_id,
            }
            .into())
        };
    }

    resolved_index_config.validate(metadata.dimensions)?;
    if metadata
        .index_config
        .as_ref()
        .is_some_and(|authoritative| authoritative != resolved_index_config)
    {
        return Err(BranchError::BranchRootInvalid {
            branch_id: Some(request.root.branch_id),
            reason: "resolved source index config does not match authoritative metadata"
                .to_string(),
        }
        .into());
    }
    let config_digest = source_data_plane_config_digest(&metadata, resolved_index_config)?;
    if config_digest != request.root.source_config_sha256 {
        return Err(BranchError::BranchRootInvalid {
            branch_id: Some(request.root.branch_id),
            reason: "source data-plane config digest does not match authoritative metadata"
                .to_string(),
        }
        .into());
    }

    let generation = ManifestGeneration::new(manifest.version())?;
    if generation != request.root.source_generation {
        return Err(BranchError::BranchRootInvalid {
            branch_id: Some(request.root.branch_id),
            reason: format!(
                "prepared source generation {} does not equal live generation {}",
                request.root.source_generation.get(),
                generation.get()
            ),
        }
        .into());
    }
    let exact_digest = version.exact_manifest_digest()?;
    if exact_digest != request.root.source_manifest_sha256 {
        return Err(BranchError::ManifestDigestMismatch { generation }.into());
    }
    let exact_bytes = version.exact_manifest_bytes()?;

    manifest.insert_branch_root_candidate(request.root.clone(), request.max_children)?;

    // Renewal is the authoritative ownership check immediately before the
    // fenced CAS. The local validation catches an impossible zero-duration
    // configuration without treating the lease memo as remote authority.
    let renewed = lease_manager.renew(namespace, lease).await?;
    if !lease_manager.validate(&renewed) {
        return Err(ZeppelinError::LeaseExpired {
            namespace: namespace.to_string(),
        });
    }
    if manifest.fencing_token() > renewed.fencing_token {
        return Err(ZeppelinError::FencingTokenStale {
            namespace: namespace.to_string(),
            our_token: renewed.fencing_token,
            manifest_token: manifest.fencing_token(),
        });
    }
    manifest.fencing_token = renewed.fencing_token;
    *lease = renewed;
    manifest
        .write_conditional(store, namespace, &version)
        .await?;

    verify_rooted_history(
        store,
        namespace,
        &manifest,
        &request.root,
        Some(exact_bytes),
    )
    .await?;
    Ok(request.root)
}

/// Remove one exact root using the source namespace's normal writer lease.
///
/// The complete persisted body is compared before publication. The candidate
/// is published with one fenced CAS and no cross-generation retry.
pub(crate) async fn remove_branch_root(
    store: &ZeppelinStore,
    namespace_manager: &NamespaceManager,
    lease_manager: &LeaseManager,
    request: RemoveBranchRootRequest,
) -> Result<()> {
    let namespace = request.source_namespace.to_string();
    let lease = lease_manager.acquire(&namespace).await?;
    let result =
        remove_branch_root_with_lease(store, namespace_manager, lease_manager, &lease, request)
            .await;
    if let Err(error) = lease_manager.release(&namespace, &lease).await {
        warn!(
            namespace,
            error = %error,
            "branch-root removal lease release failed (best-effort)"
        );
    }
    result
}

pub(crate) async fn remove_branch_root_with_lease(
    store: &ZeppelinStore,
    namespace_manager: &NamespaceManager,
    lease_manager: &LeaseManager,
    lease: &crate::wal::Lease,
    request: RemoveBranchRootRequest,
) -> Result<()> {
    let namespace = request.source_namespace.as_str();
    let metadata = namespace_manager
        .get_active_metadata_for_guarded_write(namespace)
        .await?;
    let incarnation = required_incarnation(&metadata)?;
    if incarnation != &request.expected_source_incarnation {
        return Err(BranchError::SourceIncarnationChanged {
            namespace: request.source_namespace.clone(),
        }
        .into());
    }
    let (mut manifest, version) = Manifest::read_versioned_required_for_incarnation(
        store,
        namespace,
        request.expected_source_incarnation.as_uuid(),
    )
    .await?;
    manifest.remove_branch_root_candidate(&request.expected_root)?;

    let renewed = lease_manager.renew(namespace, lease).await?;
    if !lease_manager.validate(&renewed) {
        return Err(ZeppelinError::LeaseExpired {
            namespace: namespace.to_string(),
        });
    }
    if manifest.fencing_token() > renewed.fencing_token {
        return Err(ZeppelinError::FencingTokenStale {
            namespace: namespace.to_string(),
            our_token: renewed.fencing_token,
            manifest_token: manifest.fencing_token(),
        });
    }
    manifest.fencing_token = renewed.fencing_token;
    manifest
        .write_conditional(store, namespace, &version)
        .await?;
    Ok(())
}

fn required_incarnation(
    metadata: &NamespaceMetadata,
) -> Result<&crate::namespace::NamespaceIncarnationId> {
    metadata.incarnation_id.as_ref().ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "active namespace {} has no authoritative incarnation",
            metadata.name
        ))
    })
}

async fn verify_rooted_history(
    store: &ZeppelinStore,
    namespace: &str,
    root_manifest: &Manifest,
    root: &BranchRoot,
    expected_bytes: Option<Bytes>,
) -> Result<()> {
    let key = Manifest::history_key(namespace, root.source_generation.get());
    let history_bytes = store.get(&key).await?;
    if expected_bytes
        .as_ref()
        .is_some_and(|expected| expected != &history_bytes)
    {
        return Err(BranchError::ManifestDigestMismatch {
            generation: root.source_generation,
        }
        .into());
    }
    root_manifest.validate_rooted_history_bytes(root.source_generation, &history_bytes)
}
