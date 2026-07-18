//! Resumable namespace-fork lifecycle coordination.
//!
//! `NamespaceGraph` is the single owner of the cross-object prepare protocol.
//! It reserves a non-visible target, freezes one exact source generation under
//! the source writer lease, publishes the direct-child root, and then repairs
//! the target through its durable preparation milestones. It never activates a
//! branch; authorization and activation are deliberately deferred to phase 08.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tracing::warn;

use crate::cache::manifest_cache::ManifestCache;
use crate::config::{BranchingConfig, IndexingConfig};
use crate::error::{Result, ZeppelinError};
use crate::namespace::branch_root::{
    insert_branch_root_with_lease, remove_branch_root, source_data_plane_config_digest,
    InsertBranchRootRequest, RemoveBranchRootRequest,
};
use crate::namespace::branching::deletion::{
    persist_branch_visibility_removal, AuthorizedNamespaceDelete, DeletionBoundary,
    DeletionLifecycleEvent,
};
use crate::namespace::branching::{
    ArtifactOrigin, BranchDescriptor, BranchError, BranchListRequest, BranchMaintenanceReport,
    BranchPrepareStage, ForkDataPlaneConfig, ForkIdentity, ForkPrepareIntent,
    ForkReservationIdentity, NamespaceCreationKind, NamespaceDeleteOutcome, PrepareForkOutcome,
    PrepareForkRequest, PreparedBranch,
};
use crate::namespace::manager::{
    CompactionHealth, NamespaceIndexConfig, NamespaceManager, NamespaceMetadata, NamespaceState,
    ReserveMetadataOutcome, RootReleaseState,
};
use crate::namespace::{
    BranchId, BranchRoot, ManifestGeneration, NamespaceId, NamespaceIncarnationId,
};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::wal::manifest::{BranchLineageSeed, PreparedManifestPublication, PreparedZeroCopyFork};
use crate::wal::{Lease, LeaseManager, Manifest};

const MAX_PREPARE_ATTEMPTS: usize = 16;

/// Deep lifecycle boundary for namespace graph mutations and repair.
pub(crate) struct NamespaceGraph {
    store: ZeppelinStore,
    namespace_manager: Arc<NamespaceManager>,
    lease_manager: Arc<LeaseManager>,
    clock: Clock,
    manifest_cache: Arc<ManifestCache>,
    branching: BranchingConfig,
    indexing: IndexingConfig,
    gc_horizon_floor_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrepareStop {
    Complete,
    #[cfg(feature = "branching-test-support")]
    AfterReservation,
    #[cfg(feature = "branching-test-support")]
    AfterRoot,
}

struct PreparedCandidate {
    branch: PreparedBranch,
    publication: PreparedManifestPublication,
}

enum RootedProgress {
    Candidate(PreparedCandidate),
    #[cfg(feature = "branching-test-support")]
    StoppedAfterRoot,
}

impl NamespaceGraph {
    /// Compose the graph from the concrete authoritative services it coordinates.
    #[must_use]
    pub(crate) fn new(
        store: ZeppelinStore,
        namespace_manager: Arc<NamespaceManager>,
        lease_manager: Arc<LeaseManager>,
        clock: Clock,
        manifest_cache: Arc<ManifestCache>,
        branching: BranchingConfig,
        indexing: IndexingConfig,
        gc_horizon_floor_secs: Option<u64>,
    ) -> Self {
        Self {
            store,
            namespace_manager,
            lease_manager,
            clock,
            manifest_cache,
            branching,
            indexing,
            gc_horizon_floor_secs,
        }
    }

    /// Prepare one zero-copy target without making it visible.
    pub(crate) async fn prepare_fork(
        &self,
        request: PrepareForkRequest,
    ) -> Result<PrepareForkOutcome> {
        self.prepare_fork_with_stop(request, PrepareStop::Complete)
            .await?
            .ok_or_else(|| {
                ZeppelinError::Serialization(
                    "complete fork preparation stopped before returning an outcome".to_string(),
                )
            })
    }

    /// Delete through the graph guard, refusing to bypass live child roots.
    pub(crate) async fn delete(
        &self,
        request: AuthorizedNamespaceDelete,
    ) -> Result<NamespaceDeleteOutcome> {
        let name = request.namespace.as_str();
        let (preservation, _head_proof) = request
            .governance
            .preservation_boundary(&request.namespace, DeletionBoundary::Fence)
            .await?;
        if preservation.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        let (metadata, _) = self.namespace_manager.read_metadata_versioned(name).await?;
        if matches!(metadata.state, NamespaceState::Deleting) {
            return Ok(NamespaceDeleteOutcome::AlreadyDeleting);
        }
        let (manifest, _) = Manifest::read_versioned_required(&self.store, name).await?;
        if !manifest.branch_roots().is_empty() {
            if let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind {
                if metadata.branch_identity.is_some() && !manifest.branch_roots().is_empty() {
                    // Child roots are checked against the target manifest below;
                    // the source root itself is not a child of the target.
                    let target_manifest = Manifest::read_versioned_required_for_incarnation(
                        &self.store,
                        name,
                        reservation.target_incarnation.as_uuid(),
                    )
                    .await
                    .ok()
                    .map(|(value, _)| value);
                    if target_manifest
                        .as_ref()
                        .is_some_and(|value| !value.branch_roots().is_empty())
                    {
                        return Err(BranchError::BranchHasLiveChildren {
                            branch_id: reservation.branch_id,
                        }
                        .into());
                    }
                }
            }
            return Err(BranchError::NamespaceHasLiveBranches {
                namespace: request.namespace.to_string(),
            }
            .into());
        }
        if let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind {
            let identity =
                metadata
                    .branch_identity
                    .as_ref()
                    .ok_or(BranchError::BranchRootMismatch {
                        branch_id: reservation.branch_id,
                    })?;
            if identity.branch_id != reservation.branch_id
                || identity.target_namespace != request.namespace
                || identity.target_incarnation != reservation.target_incarnation
            {
                return Err(BranchError::BranchRootMismatch {
                    branch_id: reservation.branch_id,
                }
                .into());
            }
            let source_manifest = Manifest::read_versioned_required_for_incarnation(
                &self.store,
                reservation.source_namespace.as_str(),
                reservation.source_incarnation.as_uuid(),
            )
            .await?
            .0;
            let root = source_manifest
                .branch_roots()
                .get(&reservation.branch_id)
                .cloned()
                .ok_or(BranchError::BranchRootMissing {
                    branch_id: reservation.branch_id,
                })?;
            remove_branch_root(
                &self.store,
                &self.namespace_manager,
                &self.lease_manager,
                RemoveBranchRootRequest {
                    source_namespace: reservation.source_namespace.clone(),
                    expected_root: root,
                },
            )
            .await?;
            // Release the parent visibility root before deleting target-owned
            // metadata and artifacts.  A crash after this point leaves the
            // target discoverable for resumable cleanup, never a live parent
            // root pointing at an already-absent target.
            self.namespace_manager.delete(name).await?;
        } else {
            self.namespace_manager.delete(name).await?;
        }
        Ok(NamespaceDeleteOutcome::Deleted)
    }

    /// Resume cleanup for a durable deleting tombstone.
    ///
    /// This is the retry seam used by background maintenance.  It only
    /// completes ordinary namespaces whose live manifest has already been
    /// removed; branch targets remain governed by the marker/grace/root
    /// release protocol and are rejected until that protocol is installed.
    pub(crate) async fn resume_delete(
        &self,
        namespace: &NamespaceId,
        governance: Arc<dyn crate::namespace::branching::deletion::DeletionGovernance>,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        let (preservation, _head_proof) = governance
            .preservation_boundary(namespace, DeletionBoundary::CleanupBatch)
            .await?;
        if preservation.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        let metadata = self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await?
            .0;
        if metadata.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {} is not marked deleting",
                namespace
            )));
        }
        if matches!(metadata.creation_kind, NamespaceCreationKind::Fork(_)) {
            let reservation = match &metadata.creation_kind {
                NamespaceCreationKind::Fork(reservation) => reservation,
                NamespaceCreationKind::Root => unreachable!(),
            };
            let branch_id = metadata
                .branch_identity
                .as_ref()
                .map(|identity| identity.branch_id)
                .unwrap_or(reservation.branch_id);
            let incarnation = metadata.incarnation_id.ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "branch namespace {} has no incarnation for visibility marker",
                    namespace
                ))
            })?;
            let floor_secs = self.gc_horizon_floor_secs.ok_or_else(|| {
                ZeppelinError::Validation(
                    "branch deletion requires a checked GC horizon floor".to_string(),
                )
            })?;
            let visibility = persist_branch_visibility_removal(
                &self.store,
                namespace,
                branch_id,
                incarnation,
                Duration::from_secs(floor_secs),
            )
            .await?;
            self.namespace_manager
                .record_visibility_removal(namespace.as_str(), visibility)
                .await?;
            let (meta_after, _) = self
                .namespace_manager
                .read_metadata_versioned(namespace.as_str())
                .await?;
            let intent = meta_after.deletion_intent.as_ref().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "branch namespace {} lost its deletion intent",
                    namespace
                ))
            })?;
            let visibility = intent.visibility.as_ref().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "branch namespace {} has no persisted visibility deadline",
                    namespace
                ))
            })?;
            if self.clock.now() < visibility.not_before {
                return Ok(NamespaceDeleteOutcome::AlreadyDeleting);
            }
            let parent_root = intent.parent_root.clone().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "branch namespace {} has no exact parent root",
                    namespace
                ))
            })?;
            let source_namespace = match &metadata.creation_kind {
                NamespaceCreationKind::Fork(reservation) => reservation.source_namespace.clone(),
                NamespaceCreationKind::Root => unreachable!(),
            };
            let source_incarnation = match &metadata.creation_kind {
                NamespaceCreationKind::Fork(reservation) => reservation.source_incarnation.clone(),
                NamespaceCreationKind::Root => unreachable!(),
            };
            let (parent_manifest, _) = Manifest::read_versioned_required_for_incarnation(
                &self.store,
                source_namespace.as_str(),
                source_incarnation.as_uuid(),
            )
            .await?;
            if let Some(current_root) = parent_manifest.branch_roots().get(&parent_root.branch_id) {
                if current_root != &parent_root {
                    return Err(ZeppelinError::Validation(format!(
                        "parent root for branch {} changed before release",
                        namespace
                    )));
                }
                remove_branch_root(
                    &self.store,
                    &self.namespace_manager,
                    &self.lease_manager,
                    RemoveBranchRootRequest {
                        source_namespace,
                        expected_root: parent_root,
                    },
                )
                .await?;
                self.namespace_manager
                    .record_root_release(
                        namespace.as_str(),
                        RootReleaseState::Released {
                            acked_at: self.clock.now(),
                        },
                    )
                    .await?;
            } else {
                self.namespace_manager
                    .record_root_release(
                        namespace.as_str(),
                        RootReleaseState::Converged {
                            observed_at: self.clock.now(),
                        },
                    )
                    .await?;
            }
            let outcome = self
                .namespace_manager
                .finish_delete(namespace.as_str(), budget)
                .await?;
            return Ok(if outcome.complete {
                NamespaceDeleteOutcome::Deleted
            } else {
                NamespaceDeleteOutcome::AlreadyDeleting
            });
        }
        let outcome = self
            .namespace_manager
            .finish_delete(namespace.as_str(), budget)
            .await?;
        if outcome.complete {
            Ok(NamespaceDeleteOutcome::Deleted)
        } else {
            governance
                .settle_lifecycle_audit(DeletionLifecycleEvent::CleanupIncomplete {
                    namespace: namespace.clone(),
                    remaining: outcome.deleted.saturating_add(1),
                })
                .await?;
            Ok(NamespaceDeleteOutcome::AlreadyDeleting)
        }
    }

    /// List only the direct children represented by the authoritative root map.
    pub(crate) async fn list_children(
        &self,
        request: BranchListRequest,
    ) -> Result<Vec<BranchDescriptor>> {
        let (manifest, _) =
            Manifest::read_versioned_required(&self.store, request.source.as_str()).await?;
        let mut children = Vec::with_capacity(manifest.branch_roots().len());
        for root in manifest.branch_roots().values() {
            let metadata = self
                .namespace_manager
                .read_metadata_versioned(root.target_namespace.as_str())
                .await?
                .0;
            let identity =
                metadata
                    .branch_identity
                    .as_ref()
                    .ok_or(BranchError::BranchRootMismatch {
                        branch_id: root.branch_id,
                    })?;
            if identity.branch_id != root.branch_id
                || identity.target_namespace != root.target_namespace
                || identity.target_incarnation != root.target_incarnation
            {
                return Err(BranchError::BranchRootMismatch {
                    branch_id: root.branch_id,
                }
                .into());
            }
            let state = match metadata.state {
                NamespaceState::Creating => "creating",
                NamespaceState::Active => "active",
                NamespaceState::Deleting => "deleting",
            };
            children.push(BranchDescriptor {
                target: root.target_namespace.clone(),
                branch_id: root.branch_id,
                state,
            });
        }
        children.sort_by(|left, right| {
            left.target
                .cmp(&right.target)
                .then(left.branch_id.cmp(&right.branch_id))
        });
        Ok(children)
    }

    /// Feature-only deterministic crash point immediately after the root CAS.
    #[cfg(feature = "branching-test-support")]
    pub(crate) async fn prepare_fork_until_root_for_test(
        &self,
        request: PrepareForkRequest,
    ) -> Result<()> {
        let outcome = self
            .prepare_fork_with_stop(request, PrepareStop::AfterRoot)
            .await?;
        if outcome.is_some() {
            return Err(ZeppelinError::Serialization(
                "stop-after-root fork preparation unexpectedly completed".to_string(),
            ));
        }
        Ok(())
    }

    /// Feature-only deterministic crash point after create-only reservation.
    #[cfg(feature = "branching-test-support")]
    pub(crate) async fn prepare_fork_until_reserved_for_test(
        &self,
        request: PrepareForkRequest,
    ) -> Result<()> {
        let outcome = self
            .prepare_fork_with_stop(request, PrepareStop::AfterReservation)
            .await?;
        if outcome.is_some() {
            return Err(ZeppelinError::Serialization(
                "stop-after-reservation fork preparation unexpectedly completed".to_string(),
            ));
        }
        Ok(())
    }

    async fn prepare_fork_with_stop(
        &self,
        request: PrepareForkRequest,
        stop: PrepareStop,
    ) -> Result<Option<PrepareForkOutcome>> {
        self.validate_request(&request)?;
        let (target, newly_reserved) = self.reserve_or_read(&request).await?;
        self.validate_retry_identity(&request, &target)?;

        #[cfg(feature = "branching-test-support")]
        if stop == PrepareStop::AfterReservation {
            let stage = target.branch_prepare.as_ref().map(|prepare| prepare.stage);
            if stage == Some(BranchPrepareStage::Reserved) {
                return Ok(None);
            }
        }

        if target
            .branch_prepare
            .as_ref()
            .is_some_and(|prepare| prepare.stage == BranchPrepareStage::ManifestPublished)
        {
            let prepared = self.verify_prepared_target(&target).await?;
            return Ok(Some(PrepareForkOutcome::ExistingPrepared(prepared)));
        }

        if target
            .branch_prepare
            .as_ref()
            .is_some_and(|prepare| prepare.stage == BranchPrepareStage::Rooted)
        {
            let candidate = self.rebuild_rooted_candidate(&target).await?;
            self.publish_and_mark_prepared(&target.name, &candidate)
                .await?;
            let verified = self
                .verify_prepared_target(
                    &self
                        .namespace_manager
                        .read_creating_intent_strong(&target.name)
                        .await?
                        .0,
                )
                .await?;
            return Ok(Some(if newly_reserved {
                PrepareForkOutcome::Prepared(verified)
            } else {
                PrepareForkOutcome::ExistingPrepared(verified)
            }));
        }

        let source_name = request.source.as_str();
        let mut lease = self.lease_manager.acquire(source_name).await?;
        let rooted = self
            .root_and_install_identity(&request, &mut lease, stop)
            .await;
        if let Err(error) = self.lease_manager.release(source_name, &lease).await {
            warn!(
                namespace = source_name,
                error = %error,
                "fork source lease release failed (best-effort)"
            );
        }
        let rooted = rooted?;
        let candidate = match rooted {
            RootedProgress::Candidate(candidate) => candidate,
            #[cfg(feature = "branching-test-support")]
            RootedProgress::StoppedAfterRoot => return Ok(None),
        };

        self.publish_and_mark_prepared(request.target.as_str(), &candidate)
            .await?;
        let final_metadata = self
            .namespace_manager
            .read_creating_intent_strong(request.target.as_str())
            .await?
            .0;
        let verified = self.verify_prepared_target(&final_metadata).await?;
        Ok(Some(if newly_reserved {
            PrepareForkOutcome::Prepared(verified)
        } else {
            PrepareForkOutcome::ExistingPrepared(verified)
        }))
    }

    fn validate_request(&self, request: &PrepareForkRequest) -> Result<()> {
        if !self.branching.enabled {
            return Err(BranchError::BranchingNotReady {
                feature: "namespace fork preparation",
            }
            .into());
        }
        if request.source == request.target {
            return Err(BranchError::IntentMismatch {
                target: request.target.clone(),
            }
            .into());
        }
        if self.branching.max_children_per_namespace == 0 {
            return Err(BranchError::BranchLimitExceeded { limit: 0 }.into());
        }
        Ok(())
    }

    async fn reserve_or_read(
        &self,
        request: &PrepareForkRequest,
    ) -> Result<(NamespaceMetadata, bool)> {
        match self
            .namespace_manager
            .read_creating_intent_strong(request.target.as_str())
            .await
        {
            Ok((existing, _)) => return Ok((existing, false)),
            Err(ZeppelinError::NamespaceNotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        let source = self.active_source_metadata(request.source.as_str()).await?;
        let source_incarnation = source.incarnation_id.clone().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch source {} has no authoritative incarnation",
                request.source
            ))
        })?;
        let depth = self.target_depth(&source)?;
        let resolved_index_config = self.resolved_index_config(&source);
        resolved_index_config.validate(source.dimensions)?;
        let provisional = self.data_plane_snapshot(&source, &resolved_index_config)?;
        let created_at = self.clock.now();
        let target_incarnation = NamespaceIncarnationId::new();
        let reservation = ForkReservationIdentity {
            branch_id: BranchId::new(),
            source_namespace: request.source.clone(),
            source_incarnation,
            target_namespace: request.target.clone(),
            target_incarnation: target_incarnation.clone(),
            created_at,
            depth,
        };
        let metadata = NamespaceMetadata {
            name: request.target.to_string(),
            dimensions: source.dimensions,
            distance_metric: source.distance_metric,
            index_type: source.index_type,
            vector_count: 0,
            created_at,
            updated_at: created_at,
            state: NamespaceState::Creating,
            destruction_record_key: None,
            deletion_intent: None,
            full_text_search: source.full_text_search.clone(),
            index_config: Some(resolved_index_config),
            compaction_health: CompactionHealth::default(),
            creation_kind: NamespaceCreationKind::Fork(reservation.clone()),
            branch_identity: None,
            branch_prepare: Some(ForkPrepareIntent {
                branch_id: reservation.branch_id,
                target_incarnation: target_incarnation.clone(),
                stage: BranchPrepareStage::Reserved,
                provisional: Some(provisional),
            }),
            incarnation_id: Some(target_incarnation),
        };

        match self
            .namespace_manager
            .reserve_metadata_creating(metadata)
            .await?
        {
            ReserveMetadataOutcome::Reserved(metadata) => Ok((metadata, true)),
            ReserveMetadataOutcome::Existing(metadata) => Ok((metadata, false)),
        }
    }

    fn validate_retry_identity(
        &self,
        request: &PrepareForkRequest,
        target: &NamespaceMetadata,
    ) -> Result<ForkReservationIdentity> {
        let NamespaceCreationKind::Fork(reservation) = &target.creation_kind else {
            return Err(BranchError::TargetAlreadyExists {
                target: request.target.clone(),
            }
            .into());
        };
        if reservation.source_namespace != request.source
            || reservation.target_namespace != request.target
            || target.name != request.target.as_str()
            || target.incarnation_id.as_ref() != Some(&reservation.target_incarnation)
        {
            return Err(BranchError::IntentMismatch {
                target: request.target.clone(),
            }
            .into());
        }
        Ok(reservation.clone())
    }

    fn target_depth(&self, source: &NamespaceMetadata) -> Result<u16> {
        let source_depth = match &source.creation_kind {
            NamespaceCreationKind::Root => 0,
            NamespaceCreationKind::Fork(reservation) => reservation.depth,
        };
        let depth =
            source_depth
                .checked_add(1)
                .ok_or_else(|| BranchError::BranchDepthExceeded {
                    depth: u16::MAX,
                    limit: self.branching.max_depth,
                })?;
        if depth > self.branching.max_depth {
            return Err(BranchError::BranchDepthExceeded {
                depth,
                limit: self.branching.max_depth,
            }
            .into());
        }
        Ok(depth)
    }

    async fn active_source_metadata(&self, namespace: &str) -> Result<NamespaceMetadata> {
        match self
            .namespace_manager
            .get_active_metadata_for_guarded_write(namespace)
            .await
        {
            Ok(metadata) => Ok(metadata),
            Err(ZeppelinError::NamespaceDeleting { .. }) => {
                let namespace = NamespaceId::parse(namespace.to_string()).map_err(|_| {
                    ZeppelinError::Validation(format!("invalid branch source: {namespace}"))
                })?;
                Err(BranchError::SourceDeleting { namespace }.into())
            }
            Err(error) => Err(error),
        }
    }

    fn resolved_index_config(&self, metadata: &NamespaceMetadata) -> NamespaceIndexConfig {
        metadata
            .index_config
            .clone()
            .unwrap_or_else(|| NamespaceIndexConfig::from_indexing_config(&self.indexing))
    }

    fn data_plane_snapshot(
        &self,
        metadata: &NamespaceMetadata,
        resolved_index_config: &NamespaceIndexConfig,
    ) -> Result<ForkDataPlaneConfig> {
        let full_text_search = metadata
            .full_text_search
            .iter()
            .map(|(field, config)| serde_json::to_value(config).map(|value| (field.clone(), value)))
            .collect::<std::result::Result<_, _>>()?;
        Ok(ForkDataPlaneConfig {
            dimensions: metadata.dimensions,
            distance_metric: metadata.distance_metric,
            index_type: metadata.index_type,
            full_text_search,
            index_config: resolved_index_config.clone(),
        })
    }

    async fn root_and_install_identity(
        &self,
        request: &PrepareForkRequest,
        lease: &mut Lease,
        stop: PrepareStop,
    ) -> Result<RootedProgress> {
        #[cfg(not(feature = "branching-test-support"))]
        let _ = stop;
        for _ in 0..MAX_PREPARE_ATTEMPTS {
            let (mut target, target_etag) = self
                .namespace_manager
                .read_creating_intent_strong(request.target.as_str())
                .await?;
            let reservation = self.validate_retry_identity(request, &target)?;
            let prepare = target.branch_prepare.as_ref().ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "creating fork {} has no preparation milestone",
                    request.target
                ))
            })?;
            if prepare.stage != BranchPrepareStage::Reserved {
                let candidate = self.rebuild_rooted_candidate(&target).await?;
                return Ok(RootedProgress::Candidate(candidate));
            }

            let source = self.active_source_metadata(request.source.as_str()).await?;
            if source.incarnation_id.as_ref() != Some(&reservation.source_incarnation) {
                return Err(BranchError::SourceIncarnationChanged {
                    namespace: request.source.clone(),
                }
                .into());
            }
            self.validate_immutable_shape(&source, &target, &reservation)?;
            let (source_manifest, source_version) =
                Manifest::read_versioned_required_for_incarnation(
                    &self.store,
                    request.source.as_str(),
                    reservation.source_incarnation.as_uuid(),
                )
                .await?;
            if source_version.is_deletion_fenced() {
                return Err(BranchError::SourceDeleting {
                    namespace: request.source.clone(),
                }
                .into());
            }

            let existing_root = source_manifest
                .branch_roots()
                .get(&reservation.branch_id)
                .cloned();
            let (candidate, resolved_index_config) = if let Some(root) = existing_root.as_ref() {
                // Once the root exists, the reservation is frozen to its
                // provisional configuration. A later parent PATCH must not
                // rewrite the target before crash recovery installs identity.
                (self.rebuild_candidate_from_root(&target, root).await?, None)
            } else {
                let resolved_index_config = self.resolved_index_config(&source);
                resolved_index_config.validate(source.dimensions)?;
                let latest_snapshot = self.data_plane_snapshot(&source, &resolved_index_config)?;
                if prepare.provisional.as_ref() != Some(&latest_snapshot)
                    || target.index_config.as_ref() != Some(&resolved_index_config)
                {
                    target.index_config = Some(resolved_index_config.clone());
                    target.updated_at = self.clock.now();
                    target.branch_prepare = Some(ForkPrepareIntent {
                        branch_id: reservation.branch_id,
                        target_incarnation: reservation.target_incarnation.clone(),
                        stage: BranchPrepareStage::Reserved,
                        provisional: Some(latest_snapshot),
                    });
                    match self
                        .namespace_manager
                        .cas_update_creating_intent(&target, &target_etag)
                        .await
                    {
                        Ok(_) | Err(ZeppelinError::ManifestConflict { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
                let candidate = self.build_candidate(
                    &source_manifest,
                    source_version.exact_manifest_digest()?,
                    &reservation,
                    source_data_plane_config_digest(&source, &resolved_index_config)?,
                )?;
                (candidate, Some(resolved_index_config))
            };

            if existing_root.is_none() {
                let resolved_index_config = resolved_index_config.as_ref().ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "new branch root lost its resolved source configuration".to_string(),
                    )
                })?;
                match insert_branch_root_with_lease(
                    &self.store,
                    &self.namespace_manager,
                    &self.lease_manager,
                    lease,
                    resolved_index_config,
                    InsertBranchRootRequest {
                        source_namespace: request.source.clone(),
                        root: candidate.branch.root.clone(),
                        max_children: self.branching.max_children_per_namespace,
                    },
                )
                .await
                {
                    Ok(root) if root == candidate.branch.root => {}
                    Ok(_) => {
                        return Err(BranchError::BranchRootMismatch {
                            branch_id: reservation.branch_id,
                        }
                        .into())
                    }
                    Err(ZeppelinError::ManifestConflict { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }

            #[cfg(feature = "branching-test-support")]
            if stop == PrepareStop::AfterRoot {
                return Ok(RootedProgress::StoppedAfterRoot);
            }

            let renewed = self
                .lease_manager
                .renew(request.source.as_str(), lease)
                .await?;
            if !self.lease_manager.validate(&renewed) {
                return Err(ZeppelinError::LeaseExpired {
                    namespace: request.source.to_string(),
                });
            }
            *lease = renewed;

            target.branch_identity = Some(candidate.branch.identity.clone());
            target.branch_prepare = Some(ForkPrepareIntent {
                branch_id: reservation.branch_id,
                target_incarnation: reservation.target_incarnation.clone(),
                stage: BranchPrepareStage::Rooted,
                provisional: None,
            });
            target.updated_at = self.clock.now();
            match self
                .namespace_manager
                .cas_update_creating_intent(&target, &target_etag)
                .await
            {
                Ok(_) => return Ok(RootedProgress::Candidate(candidate)),
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: request.source.to_string(),
        })
    }

    fn validate_immutable_shape(
        &self,
        source: &NamespaceMetadata,
        target: &NamespaceMetadata,
        reservation: &ForkReservationIdentity,
    ) -> Result<()> {
        if source.dimensions != target.dimensions
            || source.distance_metric != target.distance_metric
            || source.index_type != target.index_type
            || serde_json::to_value(&source.full_text_search)?
                != serde_json::to_value(&target.full_text_search)?
            || target.created_at != reservation.created_at
        {
            return Err(BranchError::IntentMismatch {
                target: reservation.target_namespace.clone(),
            }
            .into());
        }
        Ok(())
    }

    fn build_candidate(
        &self,
        source_manifest: &Manifest,
        source_manifest_sha256: crate::namespace::ManifestDigest,
        reservation: &ForkReservationIdentity,
        source_config_sha256: crate::namespace::SourceDataPlaneConfigDigest,
    ) -> Result<PreparedCandidate> {
        let source_generation = ManifestGeneration::new(source_manifest.version())?;
        let source_identity = ArtifactOrigin {
            namespace: reservation.source_namespace.clone(),
            incarnation: reservation.source_incarnation.clone(),
        };
        let target_identity = ArtifactOrigin {
            namespace: reservation.target_namespace.clone(),
            incarnation: reservation.target_incarnation.clone(),
        };
        let PreparedZeroCopyFork { manifest, lineage } = Manifest::prepare_zero_copy_fork(
            source_manifest,
            &source_identity,
            &target_identity,
            BranchLineageSeed {
                branch_id: reservation.branch_id,
                parent_namespace: reservation.source_namespace.clone(),
                parent_incarnation: reservation.source_incarnation.clone(),
                fork_generation: source_generation,
                fork_manifest_sha256: source_manifest_sha256,
                source_config_sha256,
                depth: reservation.depth,
                created_at: reservation.created_at,
            },
            reservation.created_at,
        )?;
        let publication = manifest.preseal_generation_one(&self.store, &target_identity)?;
        let root = BranchRoot {
            branch_id: reservation.branch_id,
            source_generation,
            source_manifest_sha256,
            fork_view_sha256: lineage.fork_view_sha256,
            source_config_sha256,
            target_namespace: reservation.target_namespace.clone(),
            target_incarnation: reservation.target_incarnation.clone(),
            created_at: reservation.created_at,
        };
        let identity = ForkIdentity {
            branch_id: reservation.branch_id,
            source_namespace: reservation.source_namespace.clone(),
            source_incarnation: reservation.source_incarnation.clone(),
            target_namespace: reservation.target_namespace.clone(),
            target_incarnation: reservation.target_incarnation.clone(),
            created_at: reservation.created_at,
            depth: reservation.depth,
            source_generation,
            source_manifest_sha256,
            fork_view_sha256: lineage.fork_view_sha256,
            source_config_sha256,
            target_generation: ManifestGeneration::new(1)?,
            target_manifest_sha256: publication.digest(),
        };
        Ok(PreparedCandidate {
            branch: PreparedBranch {
                identity,
                lineage,
                root,
            },
            publication,
        })
    }

    async fn rebuild_candidate_from_root(
        &self,
        target: &NamespaceMetadata,
        root: &BranchRoot,
    ) -> Result<PreparedCandidate> {
        let reservation = match &target.creation_kind {
            NamespaceCreationKind::Fork(reservation) => reservation,
            NamespaceCreationKind::Root => {
                return Err(BranchError::TargetAlreadyExists {
                    target: NamespaceId::parse(target.name.clone()).map_err(|_| {
                        ZeppelinError::Validation(format!(
                            "invalid target namespace in metadata: {}",
                            target.name
                        ))
                    })?,
                }
                .into())
            }
        };
        if root.branch_id != reservation.branch_id
            || root.target_namespace != reservation.target_namespace
            || root.target_incarnation != reservation.target_incarnation
            || root.created_at != reservation.created_at
        {
            return Err(BranchError::BranchRootMismatch {
                branch_id: reservation.branch_id,
            }
            .into());
        }
        let history_key = Manifest::history_key(
            reservation.source_namespace.as_str(),
            root.source_generation.get(),
        );
        let history_bytes = self.store.get(&history_key).await?;
        let actual_digest =
            crate::namespace::ManifestDigest::new(Sha256::digest(&history_bytes).into());
        if actual_digest != root.source_manifest_sha256 {
            return Err(BranchError::ManifestDigestMismatch {
                generation: root.source_generation,
            }
            .into());
        }
        let source_manifest = Manifest::from_bytes_for_namespace(
            &history_bytes,
            reservation.source_namespace.as_str(),
        )?;
        if source_manifest.version() != root.source_generation.get()
            || source_manifest.namespace_incarnation()
                != Some(reservation.source_incarnation.as_uuid())
        {
            return Err(BranchError::ManifestDigestMismatch {
                generation: root.source_generation,
            }
            .into());
        }
        let resolved_index_config = target.index_config.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "branch target {} lost its resolved index configuration",
                target.name
            ))
        })?;
        let source_config_sha256 = source_data_plane_config_digest(target, resolved_index_config)?;
        if source_config_sha256 != root.source_config_sha256 {
            return Err(BranchError::BranchRootMismatch {
                branch_id: root.branch_id,
            }
            .into());
        }
        let candidate = self.build_candidate(
            &source_manifest,
            root.source_manifest_sha256,
            reservation,
            source_config_sha256,
        )?;
        if candidate.branch.root != *root {
            return Err(BranchError::BranchRootMismatch {
                branch_id: root.branch_id,
            }
            .into());
        }
        if target
            .branch_identity
            .as_ref()
            .is_some_and(|identity| identity != &candidate.branch.identity)
        {
            return Err(BranchError::IntentMismatch {
                target: reservation.target_namespace.clone(),
            }
            .into());
        }
        Ok(candidate)
    }

    async fn rebuild_rooted_candidate(
        &self,
        target: &NamespaceMetadata,
    ) -> Result<PreparedCandidate> {
        let reservation = match &target.creation_kind {
            NamespaceCreationKind::Fork(reservation) => reservation,
            NamespaceCreationKind::Root => {
                return Err(ZeppelinError::Serialization(format!(
                    "root namespace {} entered branch recovery",
                    target.name
                )))
            }
        };
        let (source_live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            reservation.source_namespace.as_str(),
            reservation.source_incarnation.as_uuid(),
        )
        .await?;
        let root = source_live
            .branch_roots()
            .get(&reservation.branch_id)
            .ok_or_else(|| BranchError::BranchRootMissing {
                branch_id: reservation.branch_id,
            })?;
        self.rebuild_candidate_from_root(target, root).await
    }

    async fn publish_and_mark_prepared(
        &self,
        target: &str,
        candidate: &PreparedCandidate,
    ) -> Result<()> {
        let target_identity = ArtifactOrigin {
            namespace: candidate.branch.identity.target_namespace.clone(),
            incarnation: candidate.branch.identity.target_incarnation.clone(),
        };
        Manifest::create_or_verify_generation_one(
            &self.store,
            &target_identity,
            &candidate.publication,
        )
        .await?;

        let mut marked_published = false;
        for _ in 0..MAX_PREPARE_ATTEMPTS {
            let (mut metadata, etag) = self
                .namespace_manager
                .read_creating_intent_strong(target)
                .await?;
            if metadata.branch_identity.as_ref() != Some(&candidate.branch.identity) {
                return Err(BranchError::IntentMismatch {
                    target: candidate.branch.identity.target_namespace.clone(),
                }
                .into());
            }
            let prepare = metadata.branch_prepare.as_ref().ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "rooted branch target {target} has no preparation milestone"
                ))
            })?;
            match prepare.stage {
                BranchPrepareStage::ManifestPublished => {
                    marked_published = true;
                    break;
                }
                BranchPrepareStage::Rooted => {
                    metadata.branch_prepare = Some(ForkPrepareIntent {
                        branch_id: candidate.branch.identity.branch_id,
                        target_incarnation: candidate.branch.identity.target_incarnation.clone(),
                        stage: BranchPrepareStage::ManifestPublished,
                        provisional: None,
                    });
                    metadata.updated_at = self.clock.now();
                    match self
                        .namespace_manager
                        .cas_update_creating_intent(&metadata, &etag)
                        .await
                    {
                        Ok(_) => {
                            marked_published = true;
                            break;
                        }
                        Err(ZeppelinError::ManifestConflict { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
                BranchPrepareStage::Reserved => {
                    return Err(BranchError::CreatingRecoveryRequired {
                        target: candidate.branch.identity.target_namespace.clone(),
                    }
                    .into())
                }
            }
        }
        if !marked_published {
            return Err(ZeppelinError::ManifestConflict {
                namespace: target.to_string(),
            });
        }
        self.manifest_cache.invalidate_at(target, self.clock.now());
        Ok(())
    }

    async fn verify_prepared_target(&self, target: &NamespaceMetadata) -> Result<PreparedBranch> {
        let prepare = target.branch_prepare.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "prepared branch {} has no preparation milestone",
                target.name
            ))
        })?;
        if prepare.stage != BranchPrepareStage::ManifestPublished {
            return Err(BranchError::CreatingRecoveryRequired {
                target: NamespaceId::parse(target.name.clone()).map_err(|_| {
                    ZeppelinError::Validation(format!(
                        "invalid prepared branch target: {}",
                        target.name
                    ))
                })?,
            }
            .into());
        }
        let candidate = self.rebuild_rooted_candidate(target).await?;
        let identity = target.branch_identity.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "prepared branch {} has no immutable identity",
                target.name
            ))
        })?;
        if identity != &candidate.branch.identity {
            return Err(BranchError::IntentMismatch {
                target: identity.target_namespace.clone(),
            }
            .into());
        }
        let live_key = Manifest::s3_key(&target.name);
        let live_bytes = self.store.get(&live_key).await?;
        if live_bytes != *candidate.publication.exact_bytes() {
            return Err(BranchError::ManifestDigestMismatch {
                generation: identity.target_generation,
            }
            .into());
        }
        let history_bytes = self
            .store
            .get(&Manifest::history_key(
                &target.name,
                identity.target_generation.get(),
            ))
            .await?;
        if history_bytes != live_bytes {
            return Err(BranchError::ManifestDigestMismatch {
                generation: identity.target_generation,
            }
            .into());
        }
        if candidate.publication.manifest().branch_lineage() != Some(&candidate.branch.lineage) {
            return Err(BranchError::BranchRootMismatch {
                branch_id: identity.branch_id,
            }
            .into());
        }
        Ok(candidate.branch)
    }

    async fn verify_active_branch(&self, metadata: &NamespaceMetadata) -> Result<()> {
        let reservation = match &metadata.creation_kind {
            NamespaceCreationKind::Fork(reservation) => reservation,
            NamespaceCreationKind::Root => return Ok(()),
        };
        let identity = metadata.branch_identity.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch {} has no immutable identity",
                metadata.name
            ))
        })?;
        if !identity.matches_reservation(reservation) {
            return Err(BranchError::IntentMismatch {
                target: reservation.target_namespace.clone(),
            }
            .into());
        }
        let (source_live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            reservation.source_namespace.as_str(),
            reservation.source_incarnation.as_uuid(),
        )
        .await?;
        let root = source_live
            .branch_roots()
            .get(&reservation.branch_id)
            .ok_or_else(|| BranchError::BranchRootMissing {
                branch_id: reservation.branch_id,
            })?;
        if root.source_generation != identity.source_generation
            || root.source_manifest_sha256 != identity.source_manifest_sha256
            || root.fork_view_sha256 != identity.fork_view_sha256
            || root.source_config_sha256 != identity.source_config_sha256
            || root.target_namespace != identity.target_namespace
            || root.target_incarnation != identity.target_incarnation
            || root.created_at != identity.created_at
        {
            return Err(BranchError::BranchRootMismatch {
                branch_id: reservation.branch_id,
            }
            .into());
        }

        let history_bytes = self
            .store
            .get(&Manifest::history_key(
                &metadata.name,
                identity.target_generation.get(),
            ))
            .await?;
        if crate::namespace::ManifestDigest::new(Sha256::digest(&history_bytes).into())
            != identity.target_manifest_sha256
        {
            return Err(BranchError::ManifestDigestMismatch {
                generation: identity.target_generation,
            }
            .into());
        }
        let generation_one = Manifest::from_bytes_for_namespace(&history_bytes, &metadata.name)?;
        let lineage = generation_one.branch_lineage().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch {} generation one has no lineage",
                metadata.name
            ))
        })?;
        if lineage.branch_id != identity.branch_id
            || lineage.parent_namespace != identity.source_namespace
            || lineage.parent_incarnation != identity.source_incarnation
            || lineage.fork_generation != identity.source_generation
            || lineage.fork_manifest_sha256 != identity.source_manifest_sha256
            || lineage.fork_view_sha256 != identity.fork_view_sha256
            || lineage.source_config_sha256 != identity.source_config_sha256
            || lineage.depth != identity.depth
            || lineage.created_at != identity.created_at
        {
            return Err(BranchError::BranchRootMismatch {
                branch_id: identity.branch_id,
            }
            .into());
        }
        let (live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            &metadata.name,
            identity.target_incarnation.as_uuid(),
        )
        .await?;
        if live.branch_lineage() != Some(lineage) {
            return Err(BranchError::BranchRootMismatch {
                branch_id: identity.branch_id,
            }
            .into());
        }
        Ok(())
    }

    /// Repair only already-authorized, non-visible branch preparation state.
    #[allow(dead_code)] // Phase 07 composes the production maintenance loop.
    pub(crate) async fn maintain(&self, budget: Duration) -> Result<BranchMaintenanceReport> {
        let started = Instant::now();
        let mut report = BranchMaintenanceReport::default();
        let mut seen_targets = HashSet::new();
        let prefixes = self.store.list_common_prefixes("").await?;
        for prefix in prefixes {
            if started.elapsed() >= budget {
                break;
            }
            let target_name = prefix.trim_end_matches('/');
            if !seen_targets.insert(target_name.to_string()) {
                continue;
            }
            let (metadata, _) = match self
                .namespace_manager
                .read_metadata_versioned(target_name)
                .await
            {
                Ok(value) => value,
                Err(ZeppelinError::NamespaceNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            let reservation = match &metadata.creation_kind {
                NamespaceCreationKind::Fork(reservation) => reservation.clone(),
                NamespaceCreationKind::Root => continue,
            };
            report.inspected += 1;
            match metadata.state {
                NamespaceState::Active => {
                    self.verify_active_branch(&metadata).await?;
                    report.active_verified += 1;
                    continue;
                }
                NamespaceState::Deleting => continue,
                NamespaceState::Creating => {}
            }
            let stage = metadata
                .branch_prepare
                .as_ref()
                .ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "creating fork {target_name} has no preparation milestone"
                    ))
                })?
                .stage;
            match stage {
                BranchPrepareStage::Reserved => {
                    let (root, parent_manifest_live) =
                        match Manifest::read_versioned_required_for_incarnation(
                            &self.store,
                            reservation.source_namespace.as_str(),
                            reservation.source_incarnation.as_uuid(),
                        )
                        .await
                        {
                            Ok((_, version)) if version.is_deletion_fenced() => (None, false),
                            Ok((manifest, _)) => (
                                manifest.branch_roots().get(&reservation.branch_id).cloned(),
                                true,
                            ),
                            Err(
                                ZeppelinError::NamespaceNotFound { .. }
                                | ZeppelinError::ManifestNotFound { .. }
                                | ZeppelinError::NotFound { .. },
                            ) => (None, false),
                            Err(error) => return Err(error),
                        };
                    let Some(root) = root else {
                        if !parent_manifest_live {
                            report.awaiting_authorized_cancellation += 1;
                            continue;
                        }
                        match self
                            .namespace_manager
                            .get_active_metadata_for_guarded_write(
                                reservation.source_namespace.as_str(),
                            )
                            .await
                        {
                            Ok(_) => report.awaiting_authenticated_retry += 1,
                            Err(
                                ZeppelinError::NamespaceDeleting { .. }
                                | ZeppelinError::NamespaceNotFound { .. },
                            ) => {
                                report.awaiting_authorized_cancellation += 1;
                            }
                            Err(error) => return Err(error),
                        }
                        continue;
                    };
                    let source_name = reservation.source_namespace.as_str();
                    let mut lease = self.lease_manager.acquire(source_name).await?;
                    let repair = async {
                        let (fresh, etag) = self
                            .namespace_manager
                            .read_creating_intent_strong(target_name)
                            .await?;
                        if fresh.creation_kind != NamespaceCreationKind::Fork(reservation.clone()) {
                            return Err(BranchError::IntentMismatch {
                                target: reservation.target_namespace.clone(),
                            }
                            .into());
                        }
                        if fresh.branch_prepare.as_ref().map(|prepare| prepare.stage)
                            != Some(BranchPrepareStage::Reserved)
                        {
                            return Ok(None);
                        }
                        let (source_live, source_version) =
                            Manifest::read_versioned_required_for_incarnation(
                                &self.store,
                                source_name,
                                reservation.source_incarnation.as_uuid(),
                            )
                            .await?;
                        if source_version.is_deletion_fenced() {
                            return Err(BranchError::SourceDeleting {
                                namespace: reservation.source_namespace.clone(),
                            }
                            .into());
                        }
                        let current_root = source_live
                            .branch_roots()
                            .get(&reservation.branch_id)
                            .ok_or_else(|| BranchError::BranchRootMissing {
                            branch_id: reservation.branch_id,
                        })?;
                        if current_root != &root {
                            return Err(BranchError::BranchRootMismatch {
                                branch_id: reservation.branch_id,
                            }
                            .into());
                        }
                        let candidate = self
                            .rebuild_candidate_from_root(&fresh, current_root)
                            .await?;
                        let renewed = self.lease_manager.renew(source_name, &lease).await?;
                        if !self.lease_manager.validate(&renewed) {
                            return Err(ZeppelinError::LeaseExpired {
                                namespace: source_name.to_string(),
                            });
                        }
                        lease = renewed;
                        let mut rooted = fresh;
                        rooted.branch_identity = Some(candidate.branch.identity.clone());
                        rooted.branch_prepare = Some(ForkPrepareIntent {
                            branch_id: reservation.branch_id,
                            target_incarnation: reservation.target_incarnation.clone(),
                            stage: BranchPrepareStage::Rooted,
                            provisional: None,
                        });
                        rooted.updated_at = self.clock.now();
                        self.namespace_manager
                            .cas_update_creating_intent(&rooted, &etag)
                            .await?;
                        Ok(Some(candidate))
                    }
                    .await;
                    if let Err(error) = self.lease_manager.release(source_name, &lease).await {
                        warn!(namespace = source_name, error = %error, "branch maintenance lease release failed (best-effort)");
                    }
                    let Some(candidate) = repair? else {
                        continue;
                    };
                    report.rooted_repaired += 1;
                    self.publish_and_mark_prepared(target_name, &candidate)
                        .await?;
                    report.manifests_published += 1;
                }
                BranchPrepareStage::Rooted => {
                    let candidate = self.rebuild_rooted_candidate(&metadata).await?;
                    self.publish_and_mark_prepared(target_name, &candidate)
                        .await?;
                    report.manifests_published += 1;
                }
                BranchPrepareStage::ManifestPublished => {
                    self.verify_prepared_target(&metadata).await?;
                    report.prepared_verified += 1;
                }
            }
        }
        Ok(report)
    }
}
