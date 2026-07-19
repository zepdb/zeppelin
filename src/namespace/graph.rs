//! Resumable namespace-fork lifecycle coordination.
//!
//! `NamespaceGraph` is the single owner of the cross-object fork protocol. It
//! reserves a non-visible target, freezes one exact source generation under the
//! source writer lease, publishes the direct-child root, and activates only
//! through a kernel-minted governance permit. The target metadata CAS remains
//! the sole visibility boundary.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tracing::warn;

use crate::cache::manifest_cache::ManifestCache;
use crate::config::{BranchingConfig, IndexingConfig};
use crate::error::{Result, ZeppelinError};
use crate::namespace::branch_root::{
    insert_branch_root_with_lease, remove_branch_root_with_lease, source_data_plane_config_digest,
    InsertBranchRootRequest, RemoveBranchRootRequest,
};
use crate::namespace::branching::deletion::{
    load_branch_visibility_removal, load_deletion_decision_evidence,
    persist_branch_visibility_removal, persist_deletion_decision_evidence, AuthorizedBranchList,
    AuthorizedNamespaceDelete, BranchVisibilityRemovalMarker, DeletionBoundary, DeletionDecision,
    DeletionGovernance, DeletionLifecycleAudit,
};
use crate::namespace::branching::activation::{
    AuthorizedForkNamespace, BranchActivationPermit, BranchActivationRecovery, ForkOutcome,
};
use crate::namespace::branching::{
    ActivationNonce, ArtifactOrigin, BranchDescriptor, BranchError, BranchLifecycleState,
    BranchMaintenanceReport, BranchPrepareStage, DisclosedBranchChild, ForkDataPlaneConfig,
    ForkIdentity, ForkPrepareIntent, ForkReservationIdentity, NamespaceCreationKind,
    NamespaceDeleteOutcome, PrepareForkOutcome, PrepareForkRequest, PreparedBranch,
};
use crate::namespace::manager::{
    BranchActivationRevocationOutcome, CompactionHealth, GovernedDeletionIdentity,
    NamespaceDeletionIntent, NamespaceDestructionRecord, NamespaceIndexConfig, NamespaceManager,
    NamespaceMetadata, NamespaceState, ReserveMetadataOutcome, RootReleaseState,
    VisibilityRemoval,
};
use crate::namespace::{
    BranchId, BranchRoot, ManifestGeneration, NamespaceId, NamespaceIncarnationId,
};
use crate::security::{RootReleaseAuditProgress, RootReleaseFailureClass, SecurityError};
use crate::storage::{CreateOnlyOutcome, NamespaceObjectKey, ZeppelinStore};
use crate::time::Clock;
use crate::wal::manifest::{BranchLineageSeed, PreparedManifestPublication, PreparedZeroCopyFork};
use crate::wal::{Lease, LeaseManager, Manifest};

const MAX_PREPARE_ATTEMPTS: usize = 16;
const NEVER_ACTIVE_CLEANUP_BUDGET: Duration = Duration::from_secs(25);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentRootObservation {
    Present,
    Absent,
}

enum CancellationParentObservation {
    Live { root: Option<BranchRoot> },
    PublicationImpossible,
}

enum BranchVisibilityResume {
    Metadata(NamespaceMetadata),
    Deleted,
}

struct LiveChildDisclosure {
    visible_children: Vec<DisclosedBranchChild>,
    has_additional_children: bool,
}

impl LiveChildDisclosure {
    fn hidden() -> Self {
        Self {
            visible_children: Vec::new(),
            has_additional_children: true,
        }
    }
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

    /// Prepare and activate one fork through a kernel-minted governance permit.
    ///
    /// The target first persists a nonce-bearing non-visible state. Governance
    /// then installs its policy guard and durable audit evidence. Only after a
    /// final permit and immutable-root verification does the graph CAS the
    /// target to `Active` with retained activation evidence.
    pub(crate) async fn fork(&self, request: AuthorizedForkNamespace) -> Result<ForkOutcome> {
        let (source, target, governance) = request.into_parts();
        if let Some(existing) = self.exact_active_fork(&source, &target).await? {
            return Ok(ForkOutcome::Existing(existing));
        }

        let prepared = self
            .prepare_fork(PrepareForkRequest {
                source,
                target: target.clone(),
            })
            .await?;
        let (branch, created) = match prepared {
            PrepareForkOutcome::Prepared(branch) => (branch, true),
            PrepareForkOutcome::ExistingPrepared(branch) => (branch, false),
        };
        if let Some(existing) = self
            .recover_pending_activation(&branch, governance.as_ref())
            .await?
        {
            return Ok(ForkOutcome::Existing(existing));
        }
        let nonce = ActivationNonce::new();
        self.namespace_manager
            .begin_branch_activation(&target, &branch.identity, nonce)
            .await?;

        let mut permit = match governance.begin(&branch, nonce).await {
            Ok(permit) => permit,
            Err(error) => {
                self.revoke_activation_without_guard(&branch, nonce).await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .settle_and_revalidate_activation(&branch, permit.as_mut())
            .await
        {
            self.revoke_and_abort_activation(&branch, nonce, permit)
                .await;
            return Err(error);
        }

        let evidence = permit.evidence().clone();
        match self
            .namespace_manager
            .commit_branch_activation(&target, &branch.identity, evidence)
            .await
        {
            Ok(_) => {
                if let Err(error) = permit.finalize().await {
                    warn!(
                        target = %target,
                        branch_id = %branch.identity.branch_id,
                        error = %error,
                        "branch activation committed with policy-guard cleanup pending"
                    );
                }
            }
            Err(error) => {
                let committed = self
                    .resolve_failed_activation_cas(&branch, nonce, permit)
                    .await?;
                if !committed {
                    return Err(error);
                }
            }
        }

        self.manifest_cache
            .invalidate_at(target.as_str(), self.clock.now());
        Ok(if created {
            ForkOutcome::Created(branch)
        } else {
            ForkOutcome::Existing(branch)
        })
    }

    async fn recover_pending_activation(
        &self,
        branch: &PreparedBranch,
        governance: &dyn crate::namespace::branching::activation::BranchActivationGovernance,
    ) -> Result<Option<PreparedBranch>> {
        let metadata = self
            .namespace_manager
            .read_metadata_versioned(branch.identity.target_namespace.as_str())
            .await?
            .0;
        if metadata.state == NamespaceState::Active {
            self.verify_active_branch(&metadata).await?;
            return self.load_active_branch(&metadata).await.map(Some);
        }
        if metadata.state == NamespaceState::Deleting {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: metadata.name,
            });
        }
        let Some(BranchPrepareStage::ActivationPending { nonce }) = metadata
            .branch_prepare
            .as_ref()
            .map(|prepare| prepare.stage)
        else {
            return Ok(None);
        };

        let retained = governance.retain_guard(branch, nonce).await?;
        match self
            .namespace_manager
            .revoke_branch_activation(
                &branch.identity.target_namespace,
                &branch.identity,
                nonce,
            )
            .await?
        {
            BranchActivationRevocationOutcome::ActivationCommitted => {
                if let Some(guard) = retained {
                    if let Err(error) = guard.finalize().await {
                        warn!(
                            target = %branch.identity.target_namespace,
                            branch_id = %branch.identity.branch_id,
                            error = %error,
                            "recovered committed activation retained policy-guard cleanup"
                        );
                    }
                }
                let metadata = self
                    .namespace_manager
                    .read_metadata_versioned(branch.identity.target_namespace.as_str())
                    .await?
                    .0;
                self.verify_active_branch(&metadata).await?;
                self.load_active_branch(&metadata).await.map(Some)
            }
            BranchActivationRevocationOutcome::Revoked
            | BranchActivationRevocationOutcome::AlreadyPrepared => {
                if let Some(guard) = retained {
                    guard.abort().await?;
                }
                Ok(None)
            }
        }
    }

    async fn settle_and_revalidate_activation(
        &self,
        branch: &PreparedBranch,
        permit: &mut dyn BranchActivationPermit,
    ) -> Result<()> {
        permit.settle_audit().await?;
        permit.revalidate().await?;
        let metadata = self
            .namespace_manager
            .read_metadata_versioned(branch.identity.target_namespace.as_str())
            .await?
            .0;
        let verified = self.verify_prepared_target(&metadata).await?;
        if verified != *branch {
            return Err(BranchError::IntentMismatch {
                target: branch.identity.target_namespace.clone(),
            }
            .into());
        }
        // Root/manifest verification performs remote reads. Renew and recheck
        // the exact authority once more so a slow object-store response cannot
        // carry an expired lease or credential across the visibility CAS.
        permit.revalidate().await?;
        Ok(())
    }

    async fn revoke_activation_without_guard(
        &self,
        branch: &PreparedBranch,
        nonce: ActivationNonce,
    ) {
        if let Err(error) = self
            .namespace_manager
            .revoke_branch_activation(
                &branch.identity.target_namespace,
                &branch.identity,
                nonce,
            )
            .await
        {
            warn!(
                target = %branch.identity.target_namespace,
                branch_id = %branch.identity.branch_id,
                error = %error,
                "failed to revoke unguarded branch activation nonce"
            );
        }
    }

    async fn revoke_and_abort_activation(
        &self,
        branch: &PreparedBranch,
        nonce: ActivationNonce,
        permit: Box<dyn BranchActivationPermit>,
    ) {
        match self
            .namespace_manager
            .revoke_branch_activation(
                &branch.identity.target_namespace,
                &branch.identity,
                nonce,
            )
            .await
        {
            Ok(BranchActivationRevocationOutcome::Revoked)
            | Ok(BranchActivationRevocationOutcome::AlreadyPrepared) => {
                if let Err(error) = permit.abort().await {
                    warn!(
                        target = %branch.identity.target_namespace,
                        branch_id = %branch.identity.branch_id,
                        error = %error,
                        "branch activation nonce revoked with policy-guard cleanup pending"
                    );
                }
            }
            Ok(BranchActivationRevocationOutcome::ActivationCommitted) => {
                if let Err(error) = permit.finalize().await {
                    warn!(
                        target = %branch.identity.target_namespace,
                        branch_id = %branch.identity.branch_id,
                        error = %error,
                        "committed branch activation retained an unresolved policy guard"
                    );
                }
            }
            Err(error) => {
                warn!(
                    target = %branch.identity.target_namespace,
                    branch_id = %branch.identity.branch_id,
                    error = %error,
                    "branch activation failure retained its policy guard because nonce revocation was not proved"
                );
            }
        }
    }

    async fn resolve_failed_activation_cas(
        &self,
        branch: &PreparedBranch,
        nonce: ActivationNonce,
        permit: Box<dyn BranchActivationPermit>,
    ) -> Result<bool> {
        match self
            .namespace_manager
            .revoke_branch_activation(
                &branch.identity.target_namespace,
                &branch.identity,
                nonce,
            )
            .await?
        {
            BranchActivationRevocationOutcome::ActivationCommitted => {
                if let Err(error) = permit.finalize().await {
                    warn!(
                        target = %branch.identity.target_namespace,
                        branch_id = %branch.identity.branch_id,
                        error = %error,
                        "lost activation response retained an unresolved policy guard"
                    );
                }
                Ok(true)
            }
            BranchActivationRevocationOutcome::Revoked
            | BranchActivationRevocationOutcome::AlreadyPrepared => {
                if let Err(error) = permit.abort().await {
                    warn!(
                        target = %branch.identity.target_namespace,
                        branch_id = %branch.identity.branch_id,
                        error = %error,
                        "failed activation CAS left a revoked target with policy-guard cleanup pending"
                    );
                }
                Ok(false)
            }
        }
    }

    async fn exact_active_fork(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<Option<PreparedBranch>> {
        let metadata = match self
            .namespace_manager
            .read_metadata_versioned(target.as_str())
            .await
        {
            Ok((metadata, _)) => metadata,
            Err(ZeppelinError::NamespaceNotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        match metadata.state {
            NamespaceState::Creating => return Ok(None),
            NamespaceState::Deleting => {
                return Err(ZeppelinError::NamespaceDeleting {
                    namespace: target.as_str().to_string(),
                })
            }
            NamespaceState::Active => {}
        }
        let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
            return Err(BranchError::TargetAlreadyExists {
                target: target.clone(),
            }
            .into());
        };
        if reservation.source_namespace != *source || reservation.target_namespace != *target {
            return Err(BranchError::IntentMismatch {
                target: target.clone(),
            }
            .into());
        }
        self.verify_active_branch(&metadata).await?;
        self.load_active_branch(&metadata).await.map(Some)
    }

    /// Delete through the graph guard, refusing to bypass live child roots.
    pub(crate) async fn delete(
        &self,
        request: AuthorizedNamespaceDelete,
    ) -> Result<NamespaceDeleteOutcome> {
        let AuthorizedNamespaceDelete {
            namespace,
            decision: requested_decision,
            governance,
            activation_recovery,
        } = request;
        let name = namespace.as_str();
        let (metadata, _) = self.namespace_manager.read_metadata_versioned(name).await?;
        if metadata.state == NamespaceState::Deleting {
            // A request retry is also a recovery worker. It must re-enter the
            // same strong preservation boundaries as background maintenance;
            // returning the cached lifecycle shape here would let a newly
            // published lock be skipped after tombstoning.
            return match self
                .resume_delete(&namespace, governance, Duration::ZERO)
                .await
            {
                Err(ZeppelinError::NamespaceNotFound { .. }) => {
                    // Another recovery worker may remove metadata after this
                    // request's authoritative tombstone read but before
                    // resume_delete performs its own read. Re-prove completion
                    // from the tombstone already observed instead of turning a
                    // successful lost response into a false 404.
                    self.confirm_missing_after_resume(&namespace, &metadata)
                        .await?;
                    Ok(NamespaceDeleteOutcome::Deleted)
                }
                outcome => outcome,
            };
        }
        if metadata.state == NamespaceState::Creating {
            return self
                .cancel_never_active_fork(
                    &namespace,
                    metadata,
                    requested_decision,
                    governance,
                    activation_recovery,
                )
                .await;
        }
        if metadata.state != NamespaceState::Active {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not active for governed deletion"
            )));
        }
        let (manifest, _) = Manifest::read_versioned_required(&self.store, name).await?;
        self.verify_active_branch(&metadata).await?;
        if !manifest.branch_roots().is_empty() {
            let disclosure = self.disclose_live_children(&manifest, governance.as_ref())?;
            return Err(self.live_child_error(&metadata, &namespace, disclosure));
        }

        let parent_root = self.deletion_parent_root(&metadata, &namespace).await?;
        let decision = match metadata.deletion_intent.as_ref() {
            Some(intent) => {
                if intent.parent_root != parent_root {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {namespace} deletion intent has a stale parent root"
                    )));
                }
                load_deletion_decision_evidence(&self.store, &intent.decision_evidence_ref).await?
            }
            None => {
                persist_deletion_decision_evidence(&self.store, &requested_decision)
                    .await
                    .map_err(Self::map_audit_evidence_error)?;
                requested_decision
            }
        };
        let intent_meta = self
            .namespace_manager
            .install_deletion_intent(name, decision.decision_evidence_ref.clone(), parent_root)
            .await?;
        let intent = intent_meta.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {namespace} did not persist its deletion intent"
            ))
        })?;
        let expected_intent = intent.clone();
        let lease = self.lease_manager.acquire(name).await?;
        let result = self
            .delete_active_under_lease(
                &namespace,
                &decision,
                governance,
                expected_intent,
                lease.clone(),
                true,
            )
            .await;
        if let Err(error) = self.lease_manager.release(name, &lease).await {
            warn!(
                namespace = %namespace,
                error = %error,
                "namespace deletion lease release failed (best-effort)"
            );
        }
        result
    }

    async fn cancel_never_active_fork(
        &self,
        namespace: &NamespaceId,
        initial: NamespaceMetadata,
        requested_decision: DeletionDecision,
        governance: Arc<dyn DeletionGovernance>,
        activation_recovery: Arc<dyn BranchActivationRecovery>,
    ) -> Result<NamespaceDeleteOutcome> {
        let reservation = Self::cancellation_reservation(namespace, &initial)?.clone();
        Self::require_preparation_open_or_cancelling(namespace, &initial)?;
        self.require_unlocked_boundary(
            governance.as_ref(),
            namespace,
            DeletionBoundary::CancellationIntent,
        )
        .await?;

        let decision = match initial.deletion_intent.as_ref() {
            Some(intent) => {
                load_deletion_decision_evidence(&self.store, &intent.decision_evidence_ref).await?
            }
            None => {
                persist_deletion_decision_evidence(&self.store, &requested_decision)
                    .await
                    .map_err(Self::map_audit_evidence_error)?;
                requested_decision
            }
        };

        for _ in 0..MAX_PREPARE_ATTEMPTS {
            match self.observe_cancellation_parent(&reservation).await? {
                CancellationParentObservation::PublicationImpossible => {
                    return self
                        .cancel_never_active_with_parent_proof(
                            namespace,
                            &reservation,
                            &decision,
                            governance,
                            activation_recovery,
                            None,
                            None,
                        )
                        .await;
                }
                CancellationParentObservation::Live { .. } => {}
            }

            let source = reservation.source_namespace.as_str();
            let lease = self.lease_manager.acquire(source).await?;
            let result = match self.observe_cancellation_parent(&reservation).await? {
                CancellationParentObservation::Live { root } => {
                    self.cancel_never_active_with_parent_proof(
                        namespace,
                        &reservation,
                        &decision,
                        Arc::clone(&governance),
                        Arc::clone(&activation_recovery),
                        Some(root),
                        Some(&lease),
                    )
                    .await
                }
                CancellationParentObservation::PublicationImpossible => {
                    Err(BranchError::SourceDeleting {
                        namespace: reservation.source_namespace.clone(),
                    }
                    .into())
                }
            };
            if let Err(error) = self.lease_manager.release(source, &lease).await {
                warn!(
                    namespace = source,
                    error = %error,
                    "never-active cancellation lease release failed (best-effort)"
                );
            }
            match result {
                Err(ZeppelinError::Branch(error))
                    if matches!(error.as_ref(), BranchError::SourceDeleting { .. }) =>
                {
                    continue;
                }
                result => return result,
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: reservation.source_namespace.to_string(),
        })
    }

    async fn cancel_never_active_with_parent_proof(
        &self,
        namespace: &NamespaceId,
        reservation: &ForkReservationIdentity,
        decision: &DeletionDecision,
        governance: Arc<dyn DeletionGovernance>,
        activation_recovery: Arc<dyn BranchActivationRecovery>,
        observed_root: Option<Option<BranchRoot>>,
        parent_lease: Option<&Lease>,
    ) -> Result<NamespaceDeleteOutcome> {
        let (metadata, target_etag) = self
            .namespace_manager
            .read_creating_intent_strong(namespace.as_str())
            .await?;
        if metadata.creation_kind != NamespaceCreationKind::Fork(reservation.clone()) {
            return Err(BranchError::IntentMismatch {
                target: namespace.clone(),
            }
            .into());
        }
        let current_root = observed_root.unwrap_or_default();
        let target_manifest_version = self
            .validate_never_active_cancellation_state(
                namespace,
                &metadata,
                reservation,
                current_root.as_ref(),
            )
            .await?;

        let metadata = self
            .install_never_active_cancellation_intent(
                namespace,
                metadata,
                &target_etag,
                current_root.as_ref(),
                decision,
            )
            .await?;
        let intent = metadata.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "never-active target {namespace} did not retain its cancellation intent"
            ))
        })?;
        let bound_decision =
            load_deletion_decision_evidence(&self.store, &intent.decision_evidence_ref).await?;
        self.validate_cancellation_root_observation(namespace, intent, current_root.as_ref())
            .await?;
        self.resolve_cancellation_activation_guard(
            namespace,
            &metadata,
            intent,
            activation_recovery.as_ref(),
        )
        .await?;

        let preservation_head = self
            .require_unlocked_boundary(
                governance.as_ref(),
                namespace,
                DeletionBoundary::CancellationIntent,
            )
            .await?;
        let evidence = self
            .ensure_cancellation_evidence(
                namespace,
                &metadata,
                intent,
                &bound_decision,
                target_manifest_version,
                preservation_head,
            )
            .await
            .map_err(Self::map_audit_evidence_error)?;

        self.remove_never_active_target_manifest(
            namespace,
            reservation,
            intent,
            &evidence,
            governance.as_ref(),
        )
        .await?;
        self.release_never_active_parent_root(
            reservation,
            intent,
            &metadata,
            &bound_decision,
            governance.as_ref(),
            parent_lease,
        )
        .await?;

        self.finish_never_active_cancellation(namespace, reservation, intent, governance.as_ref())
            .await
    }

    fn cancellation_reservation<'a>(
        namespace: &NamespaceId,
        metadata: &'a NamespaceMetadata,
    ) -> Result<&'a ForkReservationIdentity> {
        if metadata.state != NamespaceState::Creating {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not never-active"
            )));
        }
        match &metadata.creation_kind {
            NamespaceCreationKind::Fork(reservation)
                if reservation.target_namespace == *namespace
                    && metadata.incarnation_id.as_ref()
                        == Some(&reservation.target_incarnation) =>
            {
                Ok(reservation)
            }
            NamespaceCreationKind::Fork(_) => Err(BranchError::IntentMismatch {
                target: namespace.clone(),
            }
            .into()),
            NamespaceCreationKind::Root => Err(ZeppelinError::Validation(format!(
                "creating root namespace {namespace} cannot use fork cancellation"
            ))),
        }
    }

    fn require_preparation_open_or_cancelling(
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
    ) -> Result<()> {
        let reservation = Self::cancellation_reservation(namespace, metadata)?;
        let prepare = metadata.branch_prepare.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "creating fork {namespace} has no preparation milestone"
            ))
        })?;
        if prepare.branch_id != reservation.branch_id
            || prepare.target_incarnation != reservation.target_incarnation
        {
            return Err(BranchError::IntentMismatch {
                target: namespace.clone(),
            }
            .into());
        }
        if matches!(prepare.stage, BranchPrepareStage::ActivationPending { .. }) {
            // A pending activation may have a matching guard in the policy
            // head. The deletion graph cannot safely release that guard, so
            // it must leave both the target metadata and parent root intact
            // for the activation-governance recovery path.
            return Err(BranchError::CreatingRecoveryRequired {
                target: namespace.clone(),
            }
            .into());
        }
        if let Some(intent) = metadata.deletion_intent.as_ref() {
            if intent.incarnation != reservation.target_incarnation
                || intent.fenced_generation.is_some()
                || intent.visibility.is_some()
                || intent.root_release.is_some()
            {
                return Err(ZeppelinError::Validation(format!(
                    "never-active target {namespace} has an invalid cancellation intent"
                )));
            }
        }
        Ok(())
    }

    fn require_preparation_not_cancelled(metadata: &NamespaceMetadata) -> Result<()> {
        if metadata.deletion_intent.is_none() {
            return Ok(());
        }
        let target = NamespaceId::parse(metadata.name.clone()).map_err(|_| {
            ZeppelinError::Validation(format!(
                "invalid creating branch target name: {}",
                metadata.name
            ))
        })?;
        Err(BranchError::CancellationInProgress { target }.into())
    }

    async fn observe_cancellation_parent(
        &self,
        reservation: &ForkReservationIdentity,
    ) -> Result<CancellationParentObservation> {
        let source = reservation.source_namespace.as_str();
        let metadata = self.namespace_manager.read_metadata_versioned(source).await;
        let manifest = Manifest::read_versioned(&self.store, source).await?;

        match (metadata, manifest) {
            (Err(ZeppelinError::NamespaceNotFound { .. }), None) => {
                Ok(CancellationParentObservation::PublicationImpossible)
            }
            (Err(ZeppelinError::NamespaceNotFound { .. }), Some(_)) => {
                Err(ZeppelinError::Validation(format!(
                    "branch parent {source} has a live manifest without metadata"
                )))
            }
            (Err(error), _) => Err(error),
            (Ok((metadata, _)), manifest) => {
                if metadata.incarnation_id.as_ref() != Some(&reservation.source_incarnation) {
                    let current_incarnation =
                        metadata.incarnation_id.as_ref().ok_or_else(|| {
                            ZeppelinError::Serialization(format!(
                                "branch parent {source} omitted its current incarnation"
                            ))
                        })?;
                    return match manifest {
                        Some((manifest, _))
                            if manifest.namespace_incarnation()
                                == Some(current_incarnation.as_uuid())
                                && !manifest
                                    .branch_roots()
                                    .contains_key(&reservation.branch_id) =>
                        {
                            Ok(CancellationParentObservation::PublicationImpossible)
                        }
                        None if matches!(
                            metadata.state,
                            NamespaceState::Creating | NamespaceState::Deleting
                        ) =>
                        {
                            Ok(CancellationParentObservation::PublicationImpossible)
                        }
                        _ => Err(BranchError::SourceIncarnationChanged {
                            namespace: reservation.source_namespace.clone(),
                        }
                        .into()),
                    };
                }
                let Some((manifest, version)) = manifest else {
                    return match metadata.state {
                        NamespaceState::Deleting => {
                            Ok(CancellationParentObservation::PublicationImpossible)
                        }
                        NamespaceState::Creating | NamespaceState::Active => {
                            Err(ZeppelinError::Validation(format!(
                                "branch parent {source} metadata has no live manifest"
                            )))
                        }
                    };
                };
                if manifest.namespace_incarnation()
                    != Some(reservation.source_incarnation.as_uuid())
                {
                    return Err(BranchError::SourceIncarnationChanged {
                        namespace: reservation.source_namespace.clone(),
                    }
                    .into());
                }
                let root = manifest.branch_roots().get(&reservation.branch_id).cloned();
                if let Some(root) = root.as_ref() {
                    Self::validate_reservation_root(reservation, root)?;
                }
                if version.is_deletion_fenced() {
                    if root.is_some() {
                        return Err(BranchError::NamespaceHasLiveBranches {
                            namespace: source.to_string(),
                            visible_children: Vec::new(),
                            has_additional_children: true,
                        }
                        .into());
                    }
                    return Ok(CancellationParentObservation::PublicationImpossible);
                }
                match metadata.state {
                    NamespaceState::Active => Ok(CancellationParentObservation::Live { root }),
                    NamespaceState::Deleting => Err(ZeppelinError::Validation(format!(
                        "deleting branch parent {source} has an unfenced live manifest"
                    ))),
                    NamespaceState::Creating => Err(ZeppelinError::Validation(format!(
                        "branch parent {source} regressed to creating"
                    ))),
                }
            }
        }
    }

    fn validate_reservation_root(
        reservation: &ForkReservationIdentity,
        root: &BranchRoot,
    ) -> Result<()> {
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
        Ok(())
    }

    async fn install_never_active_cancellation_intent(
        &self,
        namespace: &NamespaceId,
        mut metadata: NamespaceMetadata,
        target_etag: &str,
        observed_root: Option<&BranchRoot>,
        decision: &DeletionDecision,
    ) -> Result<NamespaceMetadata> {
        if let Some(intent) = metadata.deletion_intent.as_ref() {
            self.validate_existing_cancellation_intent(namespace, &metadata, intent, observed_root)
                .await?;
            return Ok(metadata);
        }
        let incarnation = metadata.incarnation_id.clone().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "never-active target {namespace} omitted its incarnation"
            ))
        })?;
        metadata.deletion_intent = Some(NamespaceDeletionIntent {
            incarnation: incarnation.clone(),
            destruction_record_key: format!(
                "_audit/destruction/{}.json",
                incarnation.as_uuid().simple()
            ),
            decision_evidence_ref: decision.decision_evidence_ref.clone(),
            parent_root: observed_root.cloned(),
            fenced_generation: None,
            visibility: None,
            root_release: None,
        });
        metadata.updated_at = self.clock.now();
        match self
            .namespace_manager
            .cas_update_creating_intent(&metadata, target_etag)
            .await
        {
            Ok(_) => Ok(metadata),
            Err(ZeppelinError::ManifestConflict { .. }) => {
                let (latest, _) = self
                    .namespace_manager
                    .read_creating_intent_strong(namespace.as_str())
                    .await?;
                let intent = latest.deletion_intent.as_ref().ok_or_else(|| {
                    ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    }
                })?;
                self.validate_existing_cancellation_intent(
                    namespace,
                    &latest,
                    intent,
                    observed_root,
                )
                .await?;
                Ok(latest)
            }
            Err(error) => Err(error),
        }
    }

    async fn validate_existing_cancellation_intent(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &NamespaceDeletionIntent,
        observed_root: Option<&BranchRoot>,
    ) -> Result<()> {
        Self::require_preparation_open_or_cancelling(namespace, metadata)?;
        if metadata.incarnation_id.as_ref() != Some(&intent.incarnation)
            || intent.fenced_generation.is_some()
            || intent.visibility.is_some()
            || intent.root_release.is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "never-active target {namespace} has a malformed cancellation intent"
            )));
        }
        if let Some(root) = observed_root {
            if intent.parent_root.as_ref() != Some(root) {
                return Err(BranchError::BranchRootMismatch {
                    branch_id: root.branch_id,
                }
                .into());
            }
        } else if intent.parent_root.is_some() {
            match self.store.get(&intent.destruction_record_key).await {
                Ok(_) => {}
                Err(ZeppelinError::NotFound { .. }) => {
                    return Err(ZeppelinError::Validation(format!(
                        "never-active target {namespace} lost its parent root before cancellation evidence"
                    )))
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn validate_never_active_cancellation_state(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        reservation: &ForkReservationIdentity,
        observed_root: Option<&BranchRoot>,
    ) -> Result<u64> {
        Self::require_preparation_open_or_cancelling(namespace, metadata)?;
        let prepare = metadata.branch_prepare.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "creating fork {namespace} has no preparation milestone"
            ))
        })?;
        if let Some(root) = observed_root {
            Self::validate_reservation_root(reservation, root)?;
        }
        let retained_root = if let Some(root) = observed_root {
            Some(root.clone())
        } else if let Some(intent) = metadata.deletion_intent.as_ref() {
            match intent.parent_root.as_ref() {
                Some(root) => match self.store.get(&intent.destruction_record_key).await {
                    Ok(_) => Some(root.clone()),
                    Err(ZeppelinError::NotFound { .. }) => {
                        return Err(ZeppelinError::Validation(format!(
                            "never-active target {namespace} lost its parent root before immutable evidence"
                        )))
                    }
                    Err(error) => return Err(error),
                },
                None => None,
            }
        } else {
            None
        };

        let target_manifest = Manifest::read_versioned(&self.store, namespace.as_str()).await?;
        if target_manifest.is_none() {
            if let Some(intent) = metadata.deletion_intent.as_ref() {
                match self.store.get(&intent.destruction_record_key).await {
                    Ok(bytes) => {
                        let evidence = NamespaceDestructionRecord::from_bytes(&bytes)?;
                        let decision = load_deletion_decision_evidence(
                            &self.store,
                            &intent.decision_evidence_ref,
                        )
                        .await?;
                        self.validate_cancellation_evidence(
                            namespace, metadata, intent, &decision, &evidence, 0,
                        )?;
                        self.validate_post_evidence_cancellation_shape(
                            namespace,
                            metadata,
                            reservation,
                            retained_root.as_ref(),
                        )?;
                        return Ok(0);
                    }
                    Err(ZeppelinError::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        let candidate = match prepare.stage {
            BranchPrepareStage::Reserved => {
                if metadata.branch_identity.is_some() {
                    return Err(BranchError::IntentMismatch {
                        target: namespace.clone(),
                    }
                    .into());
                }
                match retained_root.as_ref() {
                    Some(root) => Some(self.rebuild_candidate_from_root(metadata, root).await?),
                    None => None,
                }
            }
            BranchPrepareStage::Rooted
            | BranchPrepareStage::ManifestPublished
            | BranchPrepareStage::ActivationPending { .. } => {
                let root = retained_root
                    .as_ref()
                    .ok_or(BranchError::BranchRootMissing {
                        branch_id: reservation.branch_id,
                    })?;
                let candidate = self.rebuild_candidate_from_root(metadata, root).await?;
                if metadata.branch_identity.as_ref() != Some(&candidate.branch.identity) {
                    return Err(BranchError::IntentMismatch {
                        target: namespace.clone(),
                    }
                    .into());
                }
                Some(candidate)
            }
        };

        match target_manifest {
            Some((manifest, _)) => {
                let candidate = candidate.as_ref().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "reserved target {namespace} published a manifest before rooting"
                    ))
                })?;
                if manifest.version() != 1
                    || manifest.namespace_incarnation()
                        != Some(reservation.target_incarnation.as_uuid())
                    || !manifest.branch_roots().is_empty()
                    || self
                        .store
                        .get(&Manifest::s3_key(namespace.as_str()))
                        .await?
                        != *candidate.publication.exact_bytes()
                {
                    return Err(BranchError::ManifestDigestMismatch {
                        generation: ManifestGeneration::new(1)?,
                    }
                    .into());
                }
                Ok(1)
            }
            None
                if matches!(
                    prepare.stage,
                    BranchPrepareStage::ManifestPublished
                        | BranchPrepareStage::ActivationPending { .. }
                ) =>
            {
                Err(ZeppelinError::Validation(format!(
                    "manifest-published target {namespace} has no live manifest or immutable cancellation evidence"
                )))
            }
            None => Ok(0),
        }
    }

    fn validate_post_evidence_cancellation_shape(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        reservation: &ForkReservationIdentity,
        retained_root: Option<&BranchRoot>,
    ) -> Result<()> {
        let prepare = metadata.branch_prepare.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "creating fork {namespace} has no preparation milestone"
            ))
        })?;
        if let Some(root) = retained_root {
            Self::validate_reservation_root(reservation, root)?;
        }
        match prepare.stage {
            BranchPrepareStage::Reserved => {
                if metadata.branch_identity.is_some() {
                    return Err(BranchError::IntentMismatch {
                        target: namespace.clone(),
                    }
                    .into());
                }
            }
            BranchPrepareStage::Rooted
            | BranchPrepareStage::ManifestPublished
            | BranchPrepareStage::ActivationPending { .. } => {
                let root = retained_root.ok_or(BranchError::BranchRootMissing {
                    branch_id: reservation.branch_id,
                })?;
                let identity = metadata.branch_identity.as_ref().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "rooted cancellation target {namespace} lost its fork identity"
                    ))
                })?;
                if !identity.matches_reservation(reservation)
                    || !identity.matches_root(root)
                    || identity.target_generation != ManifestGeneration::new(1)?
                {
                    return Err(BranchError::IntentMismatch {
                        target: namespace.clone(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    async fn validate_cancellation_root_observation(
        &self,
        namespace: &NamespaceId,
        intent: &NamespaceDeletionIntent,
        observed_root: Option<&BranchRoot>,
    ) -> Result<()> {
        match (intent.parent_root.as_ref(), observed_root) {
            (None, None) => Ok(()),
            (Some(expected), Some(actual)) if expected == actual => Ok(()),
            (Some(_), None) => match self.store.get(&intent.destruction_record_key).await {
                Ok(_) => Ok(()),
                Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::Validation(format!(
                    "never-active target {namespace} lost its root before immutable evidence"
                ))),
                Err(error) => Err(error),
            },
            (None, Some(actual)) | (Some(_), Some(actual)) => {
                Err(BranchError::BranchRootMismatch {
                    branch_id: actual.branch_id,
                }
                .into())
            }
        }
    }

    async fn ensure_cancellation_evidence(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &NamespaceDeletionIntent,
        decision: &DeletionDecision,
        observed_manifest_version: u64,
        preservation_head: crate::security::PreservationHeadProof,
    ) -> Result<NamespaceDestructionRecord> {
        match self.store.get(&intent.destruction_record_key).await {
            Ok(bytes) => {
                let evidence = NamespaceDestructionRecord::from_bytes(&bytes)?;
                self.validate_cancellation_evidence(
                    namespace,
                    metadata,
                    intent,
                    decision,
                    &evidence,
                    observed_manifest_version,
                )?;
                return Ok(evidence);
            }
            Err(ZeppelinError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        let (object_count, byte_count) = self
            .namespace_destruction_census(namespace.as_str())
            .await?;
        let evidence = NamespaceDestructionRecord {
            namespace: namespace.clone(),
            manifest_version_destroyed: observed_manifest_version,
            object_count,
            byte_count,
            actor: decision.actor.clone(),
            approver: decision.approver.clone(),
            decision_id: decision.decision_id,
            parent_root: intent.parent_root.clone(),
            incarnation: Some(intent.incarnation.clone()),
            preservation_head: Some(preservation_head),
            ts: self.clock.now(),
        };
        let bytes = evidence.to_bytes()?;
        match self
            .store
            .put_create_outcome(&intent.destruction_record_key, bytes)
            .await?
        {
            CreateOnlyOutcome::Created { .. } => {
                let (verified_count, verified_bytes) = self
                    .namespace_destruction_census(namespace.as_str())
                    .await?;
                if verified_count != object_count || verified_bytes != byte_count {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
                Ok(evidence)
            }
            CreateOnlyOutcome::AlreadyExists => {
                let existing = NamespaceDestructionRecord::from_bytes(
                    &self.store.get(&intent.destruction_record_key).await?,
                )?;
                self.validate_cancellation_evidence(
                    namespace,
                    metadata,
                    intent,
                    decision,
                    &existing,
                    observed_manifest_version,
                )?;
                Ok(existing)
            }
        }
    }

    fn validate_cancellation_evidence(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &NamespaceDeletionIntent,
        decision: &DeletionDecision,
        evidence: &NamespaceDestructionRecord,
        observed_manifest_version: u64,
    ) -> Result<()> {
        let prepare_stage = metadata
            .branch_prepare
            .as_ref()
            .map(|prepare| prepare.stage)
            .ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "never-active target {namespace} lost its preparation milestone"
                ))
            })?;
        let version_matches_stage = match prepare_stage {
            BranchPrepareStage::Reserved => evidence.manifest_version_destroyed == 0,
            BranchPrepareStage::Rooted => {
                matches!(evidence.manifest_version_destroyed, 0 | 1)
            }
            BranchPrepareStage::ManifestPublished
            | BranchPrepareStage::ActivationPending { .. } => {
                evidence.manifest_version_destroyed == 1
            }
        };
        let observed_matches = observed_manifest_version == 0
            || evidence.manifest_version_destroyed == observed_manifest_version;
        if metadata.state != NamespaceState::Creating
            || metadata.incarnation_id.as_ref() != Some(&intent.incarnation)
            || metadata.deletion_intent.as_ref() != Some(intent)
            || intent.fenced_generation.is_some()
            || intent.visibility.is_some()
            || intent.root_release.is_some()
            || evidence.namespace != *namespace
            || evidence.parent_root != intent.parent_root
            || evidence.incarnation.as_ref() != Some(&intent.incarnation)
            || evidence.actor != decision.actor
            || evidence.approver != decision.approver
            || evidence.decision_id != decision.decision_id
            || evidence.preservation_head.is_none()
            || !version_matches_stage
            || !observed_matches
        {
            return Err(ZeppelinError::Validation(format!(
                "never-active target {namespace} cancellation evidence does not match its durable intent"
            )));
        }
        Ok(())
    }

    async fn remove_never_active_target_manifest(
        &self,
        namespace: &NamespaceId,
        reservation: &ForkReservationIdentity,
        intent: &NamespaceDeletionIntent,
        evidence: &NamespaceDestructionRecord,
        governance: &dyn DeletionGovernance,
    ) -> Result<()> {
        self.require_unlocked_boundary(governance, namespace, DeletionBoundary::VisibilityRemoval)
            .await?;
        let (metadata, _) = self
            .namespace_manager
            .read_creating_intent_strong(namespace.as_str())
            .await?;
        if metadata.deletion_intent.as_ref() != Some(intent)
            || metadata.creation_kind != NamespaceCreationKind::Fork(reservation.clone())
        {
            return Err(BranchError::CancellationInProgress {
                target: namespace.clone(),
            }
            .into());
        }
        let Some((manifest, _)) = Manifest::read_versioned(&self.store, namespace.as_str()).await?
        else {
            return Ok(());
        };
        if evidence.manifest_version_destroyed != 1
            || manifest.version() != 1
            || manifest.namespace_incarnation() != Some(reservation.target_incarnation.as_uuid())
            || !manifest.branch_roots().is_empty()
        {
            return Err(BranchError::ManifestDigestMismatch {
                generation: ManifestGeneration::new(1)?,
            }
            .into());
        }
        let root = intent.parent_root.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "never-active target {namespace} published a manifest without a parent root"
            ))
        })?;
        let candidate = self.rebuild_candidate_from_root(&metadata, root).await?;
        if self
            .store
            .get(&Manifest::s3_key(namespace.as_str()))
            .await?
            != *candidate.publication.exact_bytes()
        {
            return Err(BranchError::ManifestDigestMismatch {
                generation: ManifestGeneration::new(1)?,
            }
            .into());
        }
        let manifest_key = Manifest::s3_key(namespace.as_str());
        if let Err(delete_error) = self.store.delete(&manifest_key).await {
            match Manifest::read_versioned(&self.store, namespace.as_str()).await {
                Ok(None) => {}
                Ok(Some(_)) => return Err(delete_error),
                Err(read_error) => return Err(read_error),
            }
        }
        self.manifest_cache
            .invalidate_at(namespace.as_str(), self.clock.now());
        Ok(())
    }

    async fn release_never_active_parent_root(
        &self,
        reservation: &ForkReservationIdentity,
        intent: &NamespaceDeletionIntent,
        metadata: &NamespaceMetadata,
        decision: &DeletionDecision,
        governance: &dyn DeletionGovernance,
        parent_lease: Option<&Lease>,
    ) -> Result<()> {
        let namespace = &reservation.target_namespace;
        let result: Result<bool> = async {
            self.require_unlocked_boundary(governance, namespace, DeletionBoundary::RootRelease)
                .await?;
            self.require_unlocked_boundary(
                governance,
                &reservation.source_namespace,
                DeletionBoundary::RootRelease,
            )
            .await?;
            let (latest, _) = self
                .namespace_manager
                .read_creating_intent_strong(namespace.as_str())
                .await?;
            let latest_intent = latest.deletion_intent.as_ref().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "never-active target {namespace} lost its cancellation intent before root release"
                ))
            })?;
            if latest_intent != intent {
                return Err(BranchError::CancellationInProgress {
                    target: namespace.clone(),
                }
                .into());
            }
            let evidence = NamespaceDestructionRecord::from_bytes(
                &self.store.get(&intent.destruction_record_key).await?,
            )?;
            self.validate_cancellation_evidence(
                namespace, metadata, intent, decision, &evidence, 0,
            )?;

            let parent = self.observe_cancellation_parent(reservation).await?;
            let converged = match (parent_lease, parent, intent.parent_root.as_ref()) {
                (Some(lease), CancellationParentObservation::Live { root }, Some(expected)) => {
                    match root {
                        Some(actual) if actual == *expected => {
                            remove_branch_root_with_lease(
                                &self.store,
                                &self.namespace_manager,
                                &self.lease_manager,
                                lease,
                                RemoveBranchRootRequest {
                                    source_namespace: reservation.source_namespace.clone(),
                                    expected_source_incarnation: reservation
                                        .source_incarnation
                                        .clone(),
                                    expected_root: expected.clone(),
                                },
                            )
                            .await?;
                            false
                        }
                        None => true,
                        Some(actual) => {
                            return Err(BranchError::BranchRootMismatch {
                                branch_id: actual.branch_id,
                            }
                            .into())
                        }
                    }
                }
                (Some(_), CancellationParentObservation::Live { root: None }, None) => false,
                (
                    Some(_),
                    CancellationParentObservation::Live { root: Some(actual) },
                    None,
                ) => {
                    return Err(BranchError::BranchRootMismatch {
                        branch_id: actual.branch_id,
                    }
                    .into())
                }
                (None, CancellationParentObservation::PublicationImpossible, None) => false,
                (None, CancellationParentObservation::PublicationImpossible, Some(_)) => true,
                (_, CancellationParentObservation::PublicationImpossible, Some(_)) => true,
                (_, CancellationParentObservation::PublicationImpossible, None) => false,
                (None, CancellationParentObservation::Live { .. }, _) => {
                    return Err(ZeppelinError::Validation(format!(
                        "branch parent {} became live without a cancellation lease",
                        reservation.source_namespace
                    )))
                }
            };

            self.confirm_cancellation_root_absent(reservation, intent)
                .await?;
            Ok(converged)
        }
        .await;
        let converged = match result {
            Ok(converged) => converged,
            Err(error) => {
                if intent.parent_root.is_some() {
                    self.settle_root_release_failure(
                        governance,
                        namespace,
                        &intent.decision_evidence_ref,
                        &error,
                    )
                    .await?;
                }
                return Err(error);
            }
        };
        if intent.parent_root.is_some() {
            self.settle_root_release_audit(
                governance,
                namespace,
                if converged {
                    RootReleaseAuditProgress::Converged
                } else {
                    RootReleaseAuditProgress::Released
                },
                &intent.decision_evidence_ref,
            )
            .await?;
        }
        Ok(())
    }

    async fn confirm_cancellation_root_absent(
        &self,
        reservation: &ForkReservationIdentity,
        _intent: &NamespaceDeletionIntent,
    ) -> Result<()> {
        match self.observe_cancellation_parent(reservation).await? {
            CancellationParentObservation::PublicationImpossible => Ok(()),
            CancellationParentObservation::Live { root: None } => Ok(()),
            CancellationParentObservation::Live { root: Some(root) } => {
                Err(BranchError::BranchRootMismatch {
                    branch_id: root.branch_id,
                }
                .into())
            }
        }
    }

    async fn finish_never_active_cancellation(
        &self,
        namespace: &NamespaceId,
        reservation: &ForkReservationIdentity,
        intent: &NamespaceDeletionIntent,
        governance: &dyn DeletionGovernance,
    ) -> Result<NamespaceDeleteOutcome> {
        self.require_unlocked_boundary(governance, namespace, DeletionBoundary::CleanupBatch)
            .await?;
        self.confirm_cancellation_root_absent(reservation, intent)
            .await?;
        let outcome = self
            .namespace_manager
            .cleanup_creating_cancellation_batch(
                namespace.as_str(),
                intent,
                NEVER_ACTIVE_CLEANUP_BUDGET,
            )
            .await?;
        if !outcome.complete {
            governance
                .settle_lifecycle_audit(DeletionLifecycleAudit::NamespaceDeleteCleanupIncomplete {
                    namespace: namespace.clone(),
                    remaining: 1,
                    decision_evidence_ref: intent.decision_evidence_ref.clone(),
                })
                .await?;
            return Ok(NamespaceDeleteOutcome::AlreadyDeleting);
        }

        self.require_unlocked_boundary(governance, namespace, DeletionBoundary::MetadataRemoval)
            .await?;
        self.confirm_cancellation_root_absent(reservation, intent)
            .await?;
        self.namespace_manager
            .remove_creating_cancellation_metadata(namespace.as_str(), intent)
            .await?;
        self.manifest_cache
            .invalidate_at(namespace.as_str(), self.clock.now());
        Ok(NamespaceDeleteOutcome::Deleted)
    }

    async fn delete_active_under_lease(
        &self,
        namespace: &NamespaceId,
        decision: &DeletionDecision,
        governance: Arc<dyn DeletionGovernance>,
        expected_intent: NamespaceDeletionIntent,
        lease: Lease,
        disclose_conflict: bool,
    ) -> Result<NamespaceDeleteOutcome> {
        let name = namespace.as_str();
        let (metadata, _) = self.namespace_manager.read_metadata_versioned(name).await?;
        if metadata.state == NamespaceState::Deleting {
            return Ok(Self::deletion_in_progress_outcome(&metadata));
        }
        if metadata.state != NamespaceState::Active
            || metadata.deletion_intent.as_ref() != Some(&expected_intent)
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} deletion intent changed under its writer lease"
            )));
        }
        let (manifest, _) = Manifest::read_versioned_required(&self.store, name).await?;
        self.verify_active_branch(&metadata).await?;
        if !manifest.branch_roots().is_empty() {
            self.namespace_manager
                .clear_unfenced_deletion_intent(name, &expected_intent.decision_evidence_ref)
                .await?;
            let disclosure = if disclose_conflict {
                self.disclose_live_children(&manifest, governance.as_ref())?
            } else {
                LiveChildDisclosure::hidden()
            };
            return Err(self.live_child_error(&metadata, namespace, disclosure));
        }

        let (guard, head_proof) = governance
            .preservation_boundary(namespace, DeletionBoundary::Fence)
            .await?;
        if guard.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        let fenced = match Manifest::fence_for_destruction_with_lease(
            &self.store,
            &self.lease_manager,
            &lease,
            name,
            &expected_intent.destruction_record_key,
        )
        .await
        {
            Ok((manifest, _renewed)) => manifest,
            Err(ZeppelinError::Branch(error))
                if matches!(error.as_ref(), BranchError::NamespaceHasLiveBranches { .. }) =>
            {
                self.namespace_manager
                    .clear_unfenced_deletion_intent(name, &expected_intent.decision_evidence_ref)
                    .await?;
                let (latest, _) = self.namespace_manager.read_metadata_versioned(name).await?;
                let disclosure = if disclose_conflict {
                    let (latest_manifest, _) =
                        Manifest::read_versioned_required(&self.store, name).await?;
                    self.disclose_live_children(&latest_manifest, governance.as_ref())?
                } else {
                    LiveChildDisclosure::hidden()
                };
                return Err(self.live_child_error(&latest, namespace, disclosure));
            }
            Err(error) => return Err(error),
        };
        // The fence makes this exact generation immutable to ordinary writers.
        // Prove every deferred delete is a target-owned immutable artifact
        // before binding destruction evidence to that generation. A foreign
        // source key must stop deletion before visibility or cleanup changes.
        fenced.validate_pending_deletes_are_local(name)?;
        self.namespace_manager
            .record_fenced_generation(
                name,
                &expected_intent.decision_evidence_ref,
                fenced.version(),
            )
            .await?;
        let (metadata, _) = self.namespace_manager.read_metadata_versioned(name).await?;
        let intent = metadata.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {namespace} lost its fenced deletion intent"
            ))
        })?;
        self.ensure_destruction_evidence(
            namespace,
            &metadata,
            intent,
            decision,
            fenced.version(),
            head_proof,
        )
        .await
        .map_err(Self::map_audit_evidence_error)?;

        let (guard, _) = governance
            .preservation_boundary(namespace, DeletionBoundary::Tombstone)
            .await?;
        if guard.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        self.namespace_manager
            .tombstone_with_intent(name, &expected_intent.decision_evidence_ref)
            .await?;
        let (guard, _) = governance
            .preservation_boundary(namespace, DeletionBoundary::VisibilityRemoval)
            .await?;
        if guard.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        self.namespace_manager
            .remove_governed_live_manifest(name)
            .await?;
        self.manifest_cache.invalidate_at(name, self.clock.now());

        let outcome = if let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind {
            let floor_secs = self.gc_horizon_floor_secs.ok_or_else(|| {
                ZeppelinError::Validation(
                    "branch deletion requires a checked GC horizon floor".to_string(),
                )
            })?;
            let visibility = persist_branch_visibility_removal(
                &self.store,
                namespace,
                reservation.branch_id,
                intent,
                Duration::from_secs(floor_secs),
            )
            .await?;
            self.namespace_manager
                .record_visibility_removal(name, visibility.clone())
                .await?;
            self.settle_root_release_audit(
                governance.as_ref(),
                namespace,
                RootReleaseAuditProgress::GracePending {
                    not_before: visibility.not_before,
                },
                &intent.decision_evidence_ref,
            )
            .await?;
            NamespaceDeleteOutcome::BranchGraceWait {
                not_before: visibility.not_before,
            }
        } else {
            NamespaceDeleteOutcome::AlreadyDeleting
        };
        Ok(outcome)
    }

    fn deletion_in_progress_outcome(metadata: &NamespaceMetadata) -> NamespaceDeleteOutcome {
        match &metadata.creation_kind {
            NamespaceCreationKind::Fork(_) => metadata
                .deletion_intent
                .as_ref()
                .and_then(|intent| intent.visibility.as_ref())
                .map_or(NamespaceDeleteOutcome::AlreadyDeleting, |visibility| {
                    NamespaceDeleteOutcome::BranchGraceWait {
                        not_before: visibility.not_before,
                    }
                }),
            NamespaceCreationKind::Root => NamespaceDeleteOutcome::AlreadyDeleting,
        }
    }

    fn disclose_live_children(
        &self,
        manifest: &Manifest,
        governance: &dyn DeletionGovernance,
    ) -> Result<LiveChildDisclosure> {
        let mut visible_children = Vec::new();
        let mut has_additional_children = false;
        for root in manifest.branch_roots().values() {
            if governance.disclose_child(&root.target_namespace)? {
                visible_children.push(DisclosedBranchChild {
                    namespace: root.target_namespace.clone(),
                    branch_id: root.branch_id,
                });
            } else {
                has_additional_children = true;
            }
        }
        visible_children.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then(left.branch_id.cmp(&right.branch_id))
        });
        if visible_children.len() > self.branching.max_children_per_namespace {
            visible_children.truncate(self.branching.max_children_per_namespace);
            has_additional_children = true;
        }
        Ok(LiveChildDisclosure {
            visible_children,
            has_additional_children,
        })
    }

    fn live_child_error(
        &self,
        metadata: &NamespaceMetadata,
        namespace: &NamespaceId,
        disclosure: LiveChildDisclosure,
    ) -> ZeppelinError {
        match &metadata.creation_kind {
            NamespaceCreationKind::Fork(reservation) => BranchError::BranchHasLiveChildren {
                branch_id: reservation.branch_id,
                visible_children: disclosure.visible_children,
                has_additional_children: disclosure.has_additional_children,
            }
            .into(),
            NamespaceCreationKind::Root => BranchError::NamespaceHasLiveBranches {
                namespace: namespace.to_string(),
                visible_children: disclosure.visible_children,
                has_additional_children: disclosure.has_additional_children,
            }
            .into(),
        }
    }

    async fn deletion_parent_root(
        &self,
        metadata: &NamespaceMetadata,
        namespace: &NamespaceId,
    ) -> Result<Option<BranchRoot>> {
        let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
            return Ok(None);
        };
        let identity =
            metadata
                .branch_identity
                .as_ref()
                .ok_or(BranchError::BranchRootMismatch {
                    branch_id: reservation.branch_id,
                })?;
        if identity.branch_id != reservation.branch_id
            || identity.target_namespace != *namespace
            || identity.target_incarnation != reservation.target_incarnation
        {
            return Err(BranchError::BranchRootMismatch {
                branch_id: reservation.branch_id,
            }
            .into());
        }
        let (parent_manifest, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            reservation.source_namespace.as_str(),
            reservation.source_incarnation.as_uuid(),
        )
        .await?;
        let root = parent_manifest
            .branch_roots()
            .get(&reservation.branch_id)
            .cloned()
            .ok_or(BranchError::BranchRootMissing {
                branch_id: reservation.branch_id,
            })?;
        if !identity.matches_root(&root) {
            return Err(BranchError::BranchRootMismatch {
                branch_id: reservation.branch_id,
            }
            .into());
        }
        Ok(Some(root))
    }

    async fn ensure_destruction_evidence(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &crate::namespace::manager::NamespaceDeletionIntent,
        decision: &DeletionDecision,
        fenced_generation: u64,
        preservation_head: crate::security::PreservationHeadProof,
    ) -> Result<NamespaceDestructionRecord> {
        let key = &intent.destruction_record_key;
        match self.store.get(key).await {
            Ok(bytes) => {
                let existing = NamespaceDestructionRecord::from_bytes(&bytes)?;
                self.validate_destruction_evidence(
                    namespace,
                    metadata,
                    intent,
                    Some(decision),
                    &existing,
                )?;
                return Ok(existing);
            }
            Err(ZeppelinError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }

        let (object_count, byte_count) = self
            .namespace_destruction_census(namespace.as_str())
            .await?;
        let record = NamespaceDestructionRecord {
            namespace: namespace.clone(),
            manifest_version_destroyed: fenced_generation,
            object_count,
            byte_count,
            actor: decision.actor.clone(),
            approver: decision.approver.clone(),
            decision_id: decision.decision_id,
            parent_root: intent.parent_root.clone(),
            incarnation: Some(intent.incarnation.clone()),
            preservation_head: Some(preservation_head),
            ts: self.clock.now(),
        };
        let body = record.to_bytes()?;
        match self.store.put_create_outcome(key, body).await? {
            CreateOnlyOutcome::Created { .. } => {
                let (verified_count, verified_bytes) = self
                    .namespace_destruction_census(namespace.as_str())
                    .await?;
                if verified_count != object_count || verified_bytes != byte_count {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: namespace.to_string(),
                    });
                }
                Ok(record)
            }
            CreateOnlyOutcome::AlreadyExists => {
                let existing = NamespaceDestructionRecord::from_bytes(&self.store.get(key).await?)?;
                self.validate_destruction_evidence(
                    namespace,
                    metadata,
                    intent,
                    Some(decision),
                    &existing,
                )?;
                Ok(existing)
            }
        }
    }

    fn validate_destruction_evidence(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &crate::namespace::manager::NamespaceDeletionIntent,
        decision: Option<&DeletionDecision>,
        evidence: &NamespaceDestructionRecord,
    ) -> Result<()> {
        let metadata_incarnation = metadata.incarnation_id.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "namespace {namespace} deletion metadata omitted its incarnation"
            ))
        })?;
        // Before graph-owned deletion decisions were introduced, the intent's
        // decision reference pointed at the destruction record itself. That
        // immutable record is the only durable actor/approval/decision binding
        // available to an upgraded reader; every other reference shape must
        // still resolve through the current decision-evidence envelope.
        let legacy_binding = intent.is_legacy_direct_evidence_binding();
        let fenced_generation_matches = intent.fenced_generation.map_or(legacy_binding, |value| {
            value == evidence.manifest_version_destroyed
        });
        let decision_matches = decision.map_or(legacy_binding, |decision| {
            evidence.actor == decision.actor
                && evidence.approver == decision.approver
                && evidence.decision_id == decision.decision_id
        });
        if metadata_incarnation != &intent.incarnation
            || metadata
                .destruction_record_key
                .as_ref()
                .is_some_and(|key| key != &intent.destruction_record_key)
            || !fenced_generation_matches
            || evidence.namespace != *namespace
            || evidence.parent_root != intent.parent_root
            || !evidence.protocol_fields_match(intent)
            || !decision_matches
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} destruction evidence does not match its durable deletion intent"
            )));
        }
        Ok(())
    }

    async fn load_bound_destruction_evidence(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &crate::namespace::manager::NamespaceDeletionIntent,
    ) -> Result<NamespaceDestructionRecord> {
        let evidence = NamespaceDestructionRecord::from_bytes(
            &self.store.get(&intent.destruction_record_key).await?,
        )?;
        let legacy_binding = intent.is_legacy_direct_evidence_binding();
        let decision = if legacy_binding {
            None
        } else {
            Some(load_deletion_decision_evidence(&self.store, &intent.decision_evidence_ref).await?)
        };
        self.validate_destruction_evidence(
            namespace,
            metadata,
            intent,
            decision.as_ref(),
            &evidence,
        )?;
        Ok(evidence)
    }

    async fn namespace_destruction_census(&self, namespace: &str) -> Result<(usize, u64)> {
        let objects = self
            .store
            .list_prefix_meta(&format!("{namespace}/"))
            .await?;
        for object in &objects {
            NamespaceObjectKey::classify(namespace, object.key.clone())?;
        }
        let byte_count = objects.iter().try_fold(0_u64, |total, object| {
            total.checked_add(object.size).ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {namespace} destruction census byte count overflowed"
                ))
            })
        })?;
        Ok((objects.len(), byte_count))
    }

    fn map_audit_evidence_error(error: ZeppelinError) -> ZeppelinError {
        match error {
            ZeppelinError::Storage(_) => crate::security::SecurityError::AuditUnavailable.into(),
            error => error,
        }
    }

    /// Resume one durable governed-deletion state machine.
    ///
    /// Request-spawned cleanup, periodic maintenance, and graph maintenance all
    /// enter here. Every destructive batch has its own strong preservation
    /// boundary, branch roots are released before target cleanup, and metadata
    /// remains the final target-owned object.
    pub(crate) async fn resume_delete(
        &self,
        namespace: &NamespaceId,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        let (mut metadata, _) = self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await?;

        if metadata.state == NamespaceState::Active {
            let intent = metadata.deletion_intent.clone().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {namespace} has no governed deletion intent to resume"
                ))
            })?;
            let decision =
                load_deletion_decision_evidence(&self.store, &intent.decision_evidence_ref).await?;
            let lease = self.lease_manager.acquire(namespace.as_str()).await?;
            let result = self
                .delete_active_under_lease(
                    namespace,
                    &decision,
                    Arc::clone(&governance),
                    intent,
                    lease.clone(),
                    false,
                )
                .await;
            if let Err(error) = self.lease_manager.release(namespace.as_str(), &lease).await {
                warn!(
                    namespace = %namespace,
                    error = %error,
                    "resumed namespace deletion lease release failed (best-effort)"
                );
            }
            result?;
            metadata = self
                .namespace_manager
                .read_metadata_versioned(namespace.as_str())
                .await?
                .0;
        }

        if metadata.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not in a resumable governed deletion state"
            )));
        }

        if metadata.deletion_intent.is_none() {
            return self
                .finish_legacy_deleting_cleanup(namespace, &metadata, governance, budget)
                .await;
        }

        self.resume_deleting(namespace, metadata, governance, budget)
            .await
    }

    pub(crate) async fn confirm_missing_after_resume(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
    ) -> Result<()> {
        self.confirm_missing_after_resume_with_visibility(namespace, metadata, None)
            .await
    }

    async fn confirm_missing_after_resume_with_visibility(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        published_visibility: Option<&VisibilityRemoval>,
    ) -> Result<()> {
        if let Some(intent) = metadata.deletion_intent.as_ref() {
            self.load_bound_destruction_evidence(namespace, metadata, intent)
                .await?;
            if let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind {
                if Self::root_release_is_final(intent) {
                    self.require_released_branch_root_absent(namespace, metadata, intent)
                        .await?;
                } else {
                    let visibility = match (intent.visibility.as_ref(), published_visibility) {
                        (Some(durable), Some(published)) if durable != published => {
                            return Err(ZeppelinError::Validation(format!(
                                "branch namespace {namespace} completed with conflicting visibility evidence"
                            )))
                        }
                        (Some(durable), _) => durable,
                        (None, Some(published)) => published,
                        (None, None) => {
                            return Err(ZeppelinError::Validation(format!(
                                "branch namespace {namespace} disappeared before visibility evidence became durable"
                            )))
                        }
                    };
                    self.require_branch_root_absent_after_grace(
                        namespace, metadata, intent, visibility,
                    )
                    .await?;
                    self.remove_completed_branch_visibility_marker(
                        namespace,
                        reservation,
                        intent,
                        visibility,
                    )
                    .await?;
                }
            }
        } else if matches!(metadata.creation_kind, NamespaceCreationKind::Fork(_)) {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} disappeared without a deletion intent"
            )));
        }

        let remaining = self
            .store
            .list_prefix_meta(&format!("{}/", namespace.as_str()))
            .await?;
        if !remaining.is_empty() {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} metadata disappeared before owned cleanup completed"
            )));
        }
        Ok(())
    }

    async fn finish_legacy_deleting_cleanup(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        if !matches!(metadata.creation_kind, NamespaceCreationKind::Root) {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} cannot resume without a governed deletion intent"
            )));
        }
        let expected_incarnation = metadata.incarnation_id.clone();
        let started = Instant::now();
        loop {
            self.require_unlocked_boundary(
                governance.as_ref(),
                namespace,
                DeletionBoundary::CleanupBatch,
            )
            .await?;
            let outcome = self
                .namespace_manager
                .cleanup_legacy_delete_batch(
                    namespace.as_str(),
                    expected_incarnation.as_ref(),
                    Duration::ZERO,
                )
                .await?;
            if outcome.complete {
                break;
            }
            if started.elapsed() >= budget {
                return Ok(NamespaceDeleteOutcome::AlreadyDeleting);
            }
        }
        self.require_unlocked_boundary(
            governance.as_ref(),
            namespace,
            DeletionBoundary::MetadataRemoval,
        )
        .await?;
        self.namespace_manager
            .remove_legacy_deletion_metadata(namespace.as_str(), expected_incarnation.as_ref())
            .await?;
        self.manifest_cache
            .invalidate_at(namespace.as_str(), self.clock.now());
        Ok(NamespaceDeleteOutcome::Deleted)
    }

    async fn resume_deleting(
        &self,
        namespace: &NamespaceId,
        mut metadata: NamespaceMetadata,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        let mut intent = metadata.deletion_intent.clone().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {namespace} deletion tombstone omitted its durable intent"
            ))
        })?;
        let mut evidence = self
            .load_bound_destruction_evidence(namespace, &metadata, &intent)
            .await?;

        if intent.fenced_generation.is_none() {
            if let Some(manifest) = Manifest::read(&self.store, namespace.as_str()).await? {
                manifest.require_destruction_fence(
                    namespace.as_str(),
                    &intent.destruction_record_key,
                    evidence.manifest_version_destroyed,
                )?;
            }
            metadata = self
                .namespace_manager
                .record_fenced_generation(
                    namespace.as_str(),
                    &intent.decision_evidence_ref,
                    evidence.manifest_version_destroyed,
                )
                .await?;
            intent = metadata.deletion_intent.clone().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {namespace} lost its migrated deletion intent"
                ))
            })?;
            evidence = self
                .load_bound_destruction_evidence(namespace, &metadata, &intent)
                .await?;
        }

        if let Some(manifest) = Manifest::read(&self.store, namespace.as_str()).await? {
            manifest.require_destruction_fence(
                namespace.as_str(),
                &intent.destruction_record_key,
                evidence.manifest_version_destroyed,
            )?;
            self.require_unlocked_boundary(
                governance.as_ref(),
                namespace,
                DeletionBoundary::VisibilityRemoval,
            )
            .await?;
            self.namespace_manager
                .remove_governed_live_manifest(namespace.as_str())
                .await?;
            self.manifest_cache
                .invalidate_at(namespace.as_str(), self.clock.now());
        }

        if matches!(metadata.creation_kind, NamespaceCreationKind::Fork(_)) {
            if Self::root_release_is_final(&intent) {
                // Owned cleanup is allowed to delete the visibility marker.
                // Once the root-release acknowledgement is durable, retries
                // must not recreate that marker and start a new grace window.
                self.require_released_branch_root_absent(namespace, &metadata, &intent)
                    .await?;
            } else {
                if intent.visibility.is_none() {
                    self.require_branch_root_retained(namespace, &metadata, &intent)
                        .await?;
                }
                metadata = match self
                    .ensure_branch_visibility_removal(namespace, &metadata)
                    .await?
                {
                    BranchVisibilityResume::Metadata(metadata) => metadata,
                    BranchVisibilityResume::Deleted => return Ok(NamespaceDeleteOutcome::Deleted),
                };
                intent = metadata.deletion_intent.clone().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "branch namespace {namespace} lost its deletion intent"
                    ))
                })?;
                if Self::root_release_is_final(&intent) {
                    self.require_released_branch_root_absent(namespace, &metadata, &intent)
                        .await?;
                } else {
                    let visibility = intent.visibility.as_ref().ok_or_else(|| {
                        ZeppelinError::Validation(format!(
                            "branch namespace {namespace} has no persisted visibility deadline"
                        ))
                    })?;
                    if self.clock.now() < visibility.not_before {
                        self.require_branch_root_retained(namespace, &metadata, &intent)
                            .await?;
                        self.settle_root_release_audit(
                            governance.as_ref(),
                            namespace,
                            RootReleaseAuditProgress::GracePending {
                                not_before: visibility.not_before,
                            },
                            &intent.decision_evidence_ref,
                        )
                        .await?;
                        return Ok(NamespaceDeleteOutcome::BranchGraceWait {
                            not_before: visibility.not_before,
                        });
                    }
                    self.release_branch_root(namespace, &metadata, Arc::clone(&governance))
                        .await?;
                    metadata = match self
                        .namespace_manager
                        .read_metadata_versioned(namespace.as_str())
                        .await
                    {
                        Ok((metadata, _)) => metadata,
                        Err(ZeppelinError::NamespaceNotFound { .. }) => {
                            self.confirm_missing_after_resume(namespace, &metadata)
                                .await?;
                            return Ok(NamespaceDeleteOutcome::Deleted);
                        }
                        Err(error) => return Err(error),
                    };
                    intent = metadata.deletion_intent.clone().ok_or_else(|| {
                        ZeppelinError::Validation(format!(
                            "branch namespace {namespace} lost its root-release acknowledgement"
                        ))
                    })?;
                    self.require_released_branch_root_absent(namespace, &metadata, &intent)
                        .await?;
                }
            }
        }

        self.finish_deleting_cleanup(namespace, &intent, governance, budget)
            .await
    }

    async fn ensure_branch_visibility_removal(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
    ) -> Result<BranchVisibilityResume> {
        if Manifest::read(&self.store, namespace.as_str())
            .await?
            .is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} visibility marker requires live manifest removal"
            )));
        }
        let current = match self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await
        {
            Ok((current, _)) => current,
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                self.confirm_missing_after_resume(namespace, metadata)
                    .await?;
                return Ok(BranchVisibilityResume::Deleted);
            }
            Err(error) => return Err(error),
        };
        self.require_same_branch_resume_identity(namespace, metadata, &current)?;
        let NamespaceCreationKind::Fork(reservation) = &current.creation_kind else {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not a branch target"
            )));
        };
        let intent = current.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no deletion intent"
            ))
        })?;
        self.load_bound_destruction_evidence(namespace, &current, intent)
            .await?;
        if Self::root_release_is_final(intent) {
            self.require_released_branch_root_absent(namespace, &current, intent)
                .await?;
            return Ok(BranchVisibilityResume::Metadata(current));
        }
        if intent.visibility.is_some() {
            match load_branch_visibility_removal(
                &self.store,
                namespace,
                reservation.branch_id,
                intent,
            )
            .await
            {
                Ok(_) => return Ok(BranchVisibilityResume::Metadata(current)),
                Err(ZeppelinError::NotFound { key })
                    if intent
                        .visibility
                        .as_ref()
                        .is_some_and(|visibility| visibility.marker_key == key) =>
                {
                    return self
                        .classify_missing_visibility_marker(namespace, &current)
                        .await;
                }
                Err(error) => return Err(error),
            }
        }
        self.require_branch_root_retained(namespace, &current, intent)
            .await?;
        let floor_secs = self.gc_horizon_floor_secs.ok_or_else(|| {
            ZeppelinError::Validation(
                "branch deletion requires a checked GC horizon floor".to_string(),
            )
        })?;
        let expected_marker_key = BranchVisibilityRemovalMarker::key(
            namespace,
            reservation.branch_id,
            intent.incarnation.clone(),
        );
        let visibility = match persist_branch_visibility_removal(
            &self.store,
            namespace,
            reservation.branch_id,
            intent,
            Duration::from_secs(floor_secs),
        )
        .await
        {
            Ok(visibility) => visibility,
            Err(ZeppelinError::NotFound { key }) if key == expected_marker_key => {
                return self
                    .classify_missing_visibility_marker(namespace, &current)
                    .await;
            }
            Err(error) => return Err(error),
        };
        match self
            .namespace_manager
            .record_visibility_removal(namespace.as_str(), visibility.clone())
            .await
        {
            Ok(()) => {}
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                self.confirm_missing_after_resume_with_visibility(
                    namespace,
                    &current,
                    Some(&visibility),
                )
                .await?;
                return Ok(BranchVisibilityResume::Deleted);
            }
            Err(error) => return Err(error),
        }
        match self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await
        {
            Ok((latest, _)) => {
                self.require_same_branch_resume_identity(namespace, &current, &latest)?;
                Ok(BranchVisibilityResume::Metadata(latest))
            }
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                self.confirm_missing_after_resume_with_visibility(
                    namespace,
                    &current,
                    Some(&visibility),
                )
                .await?;
                Ok(BranchVisibilityResume::Deleted)
            }
            Err(error) => Err(error),
        }
    }

    async fn classify_missing_visibility_marker(
        &self,
        namespace: &NamespaceId,
        observed: &NamespaceMetadata,
    ) -> Result<BranchVisibilityResume> {
        match self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await
        {
            Ok((current, _)) => {
                self.require_same_branch_resume_identity(namespace, observed, &current)?;
                let intent = current.deletion_intent.as_ref().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "branch namespace {namespace} lost its deletion intent at the visibility boundary"
                    ))
                })?;
                self.load_bound_destruction_evidence(namespace, &current, intent)
                    .await?;
                if Self::root_release_is_final(intent) {
                    self.require_released_branch_root_absent(namespace, &current, intent)
                        .await?;
                    Ok(BranchVisibilityResume::Metadata(current))
                } else {
                    Err(ZeppelinError::Serialization(format!(
                        "branch namespace {namespace} has pending root release but its durable visibility marker is missing"
                    )))
                }
            }
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                self.confirm_missing_after_resume(namespace, observed)
                    .await?;
                Ok(BranchVisibilityResume::Deleted)
            }
            Err(error) => Err(error),
        }
    }

    fn require_same_branch_resume_identity(
        &self,
        namespace: &NamespaceId,
        observed: &NamespaceMetadata,
        current: &NamespaceMetadata,
    ) -> Result<()> {
        let observed_intent = observed.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no observed deletion intent"
            ))
        })?;
        let current_intent = current.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} lost its deletion intent"
            ))
        })?;
        let mut observed_identity = observed_intent.clone();
        observed_identity.visibility = None;
        observed_identity.root_release = None;
        let mut current_identity = current_intent.clone();
        current_identity.visibility = None;
        current_identity.root_release = None;
        if current.state != NamespaceState::Deleting
            || current.name != observed.name
            || current.incarnation_id != observed.incarnation_id
            || current.creation_kind != observed.creation_kind
            || current.branch_identity != observed.branch_identity
            || current.destruction_record_key != observed.destruction_record_key
            || current_identity != observed_identity
        {
            return Err(ZeppelinError::Serialization(format!(
                "branch namespace {namespace} identity changed during concurrent deletion resume"
            )));
        }
        Ok(())
    }

    fn root_release_is_final(intent: &NamespaceDeletionIntent) -> bool {
        matches!(
            intent.root_release,
            Some(RootReleaseState::Released { .. } | RootReleaseState::Converged { .. })
        )
    }

    async fn require_branch_root_retained(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &NamespaceDeletionIntent,
    ) -> Result<()> {
        let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not a branch target"
            )));
        };
        let expected_root = intent.parent_root.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no exact parent root"
            ))
        })?;
        match self
            .observe_parent_root(reservation, expected_root, true)
            .await?
        {
            ParentRootObservation::Present => Ok(()),
            ParentRootObservation::Absent => Err(BranchError::BranchRootMissing {
                branch_id: expected_root.branch_id,
            }
            .into()),
        }
    }

    async fn observe_parent_root(
        &self,
        reservation: &ForkReservationIdentity,
        expected_root: &BranchRoot,
        require_active: bool,
    ) -> Result<ParentRootObservation> {
        let parent_metadata = match self
            .namespace_manager
            .read_metadata_versioned(reservation.source_namespace.as_str())
            .await
        {
            Ok((metadata, _)) => metadata,
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                if Manifest::read(&self.store, reservation.source_namespace.as_str())
                    .await?
                    .is_some()
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "branch source {} has a live manifest without metadata",
                        reservation.source_namespace
                    )));
                }
                return if require_active {
                    Err(BranchError::SourceIncarnationChanged {
                        namespace: reservation.source_namespace.clone(),
                    }
                    .into())
                } else {
                    Ok(ParentRootObservation::Absent)
                };
            }
            Err(error) => return Err(error),
        };
        if parent_metadata.incarnation_id.as_ref() != Some(&reservation.source_incarnation) {
            return Err(BranchError::SourceIncarnationChanged {
                namespace: reservation.source_namespace.clone(),
            }
            .into());
        }
        if require_active && parent_metadata.state != NamespaceState::Active {
            return Err(BranchError::SourceDeleting {
                namespace: reservation.source_namespace.clone(),
            }
            .into());
        }
        if !matches!(
            parent_metadata.state,
            NamespaceState::Active | NamespaceState::Deleting
        ) {
            return Err(ZeppelinError::Serialization(format!(
                "branch source {} has invalid lifecycle state {} during root release",
                reservation.source_namespace,
                parent_metadata.state.as_str()
            )));
        }

        let Some(parent_manifest) =
            Manifest::read(&self.store, reservation.source_namespace.as_str()).await?
        else {
            return if parent_metadata.state == NamespaceState::Deleting && !require_active {
                Ok(ParentRootObservation::Absent)
            } else {
                Err(ZeppelinError::Serialization(format!(
                    "active branch source {} has no live manifest",
                    reservation.source_namespace
                )))
            };
        };
        if parent_manifest.namespace_incarnation() != Some(reservation.source_incarnation.as_uuid())
        {
            return Err(BranchError::SourceIncarnationChanged {
                namespace: reservation.source_namespace.clone(),
            }
            .into());
        }
        match parent_manifest.branch_roots().get(&expected_root.branch_id) {
            Some(current) if current != expected_root => Err(BranchError::BranchRootMismatch {
                branch_id: expected_root.branch_id,
            }
            .into()),
            Some(_) if parent_metadata.state != NamespaceState::Active => {
                Err(BranchError::SourceDeleting {
                    namespace: reservation.source_namespace.clone(),
                }
                .into())
            }
            Some(_) => Ok(ParentRootObservation::Present),
            None => Ok(ParentRootObservation::Absent),
        }
    }

    async fn renew_parent_lease_and_require_root_absent(
        &self,
        reservation: &ForkReservationIdentity,
        expected_root: &BranchRoot,
        parent_lease: &Lease,
    ) -> Result<()> {
        let renewed = self
            .lease_manager
            .renew(reservation.source_namespace.as_str(), parent_lease)
            .await?;
        if !self.lease_manager.validate(&renewed) {
            return Err(ZeppelinError::LeaseExpired {
                namespace: reservation.source_namespace.to_string(),
            });
        }
        match self
            .observe_parent_root(reservation, expected_root, false)
            .await?
        {
            ParentRootObservation::Absent => Ok(()),
            ParentRootObservation::Present => Err(ZeppelinError::Serialization(format!(
                "branch root {} remained live after root-release mutation",
                expected_root.branch_id
            ))),
        }
    }

    async fn release_branch_root(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        governance: Arc<dyn DeletionGovernance>,
    ) -> Result<()> {
        let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not a branch target"
            )));
        };
        let intent = metadata.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no deletion intent"
            ))
        })?;
        let visibility = intent.visibility.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no persisted visibility removal"
            ))
        })?;
        if self.clock.now() < visibility.not_before {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} reader-safety grace has not elapsed"
            )));
        }
        let expected_root = intent.parent_root.clone().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no exact parent root"
            ))
        })?;
        let source_namespace = reservation.source_namespace.clone();
        let decision_evidence_ref = intent.decision_evidence_ref.clone();
        match self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await
        {
            Ok((current, _)) => {
                self.require_same_branch_resume_identity(namespace, metadata, &current)?;
                let current_intent = current.deletion_intent.as_ref().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "branch namespace {namespace} lost its deletion intent before root release"
                    ))
                })?;
                if Self::root_release_is_final(current_intent) {
                    self.require_released_branch_root_absent(namespace, &current, current_intent)
                        .await?;
                    return Ok(());
                }
            }
            Err(ZeppelinError::NamespaceNotFound { .. }) => {
                self.confirm_missing_after_resume(namespace, metadata)
                    .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        let parent_lease = match self.lease_manager.acquire(source_namespace.as_str()).await {
            Ok(lease) => lease,
            Err(error) => {
                self.settle_root_release_failure(
                    governance.as_ref(),
                    namespace,
                    &decision_evidence_ref,
                    &error,
                )
                .await?;
                return Err(error);
            }
        };
        let result = async {
            let mutation: Result<Option<(bool, NamespaceMetadata, String)>> = async {
                self.require_unlocked_boundary(
                    governance.as_ref(),
                    namespace,
                    DeletionBoundary::RootRelease,
                )
                .await?;
                self.require_unlocked_boundary(
                    governance.as_ref(),
                    &source_namespace,
                    DeletionBoundary::RootRelease,
                )
                .await?;

                let (current_target, current_target_etag) = self
                    .namespace_manager
                    .read_metadata_versioned(namespace.as_str())
                    .await?;
                let current_target_etag = current_target_etag
                    .filter(|etag| !etag.is_empty())
                    .ok_or_else(|| {
                        ZeppelinError::Serialization(format!(
                            "branch namespace {namespace} metadata has no ETag for root-release acknowledgement"
                        ))
                    })?;
                let current_intent = current_target.deletion_intent.as_ref().ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "branch namespace {namespace} lost its deletion intent before root release"
                    ))
                })?;
                self.require_same_branch_resume_identity(namespace, metadata, &current_target)?;
                if Self::root_release_is_final(current_intent) {
                    self.require_released_branch_root_absent(
                        namespace,
                        &current_target,
                        current_intent,
                    )
                    .await?;
                    return Ok(None);
                }
                if current_target.state != NamespaceState::Deleting
                    || current_target.incarnation_id != metadata.incarnation_id
                    || current_target.creation_kind != metadata.creation_kind
                    || current_target.branch_identity != metadata.branch_identity
                    || current_intent.visibility.as_ref() != Some(visibility)
                    || self.clock.now() < visibility.not_before
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "branch namespace {namespace} identity changed before root release"
                    )));
                }
                self.load_bound_destruction_evidence(namespace, &current_target, current_intent)
                    .await?;
                match load_branch_visibility_removal(
                    &self.store,
                    namespace,
                    reservation.branch_id,
                    current_intent,
                )
                .await
                {
                    Ok(_) => {}
                    Err(ZeppelinError::NotFound { key })
                        if current_intent
                            .visibility
                            .as_ref()
                            .is_some_and(|visibility| visibility.marker_key == key) =>
                    {
                        match self
                            .classify_missing_visibility_marker(namespace, &current_target)
                            .await?
                        {
                            BranchVisibilityResume::Metadata(_)
                            | BranchVisibilityResume::Deleted => return Ok(None),
                        }
                    }
                    Err(error) => return Err(error),
                }

                let root_was_present = matches!(
                    self.observe_parent_root(reservation, &expected_root, false)
                        .await?,
                    ParentRootObservation::Present
                );

                if root_was_present {
                    remove_branch_root_with_lease(
                        &self.store,
                        &self.namespace_manager,
                        &self.lease_manager,
                        &parent_lease,
                        RemoveBranchRootRequest {
                            source_namespace: source_namespace.clone(),
                            expected_source_incarnation: reservation.source_incarnation.clone(),
                            expected_root: expected_root.clone(),
                        },
                    )
                    .await?;
                }
                self.renew_parent_lease_and_require_root_absent(
                    reservation,
                    &expected_root,
                    &parent_lease,
                )
                .await?;
                Ok(Some((
                    root_was_present,
                    current_target,
                    current_target_etag,
                )))
            }
            .await;
            let (root_was_present, current_target, current_target_etag) = match mutation {
                Ok(Some(progress)) => progress,
                Ok(None) => return Ok(()),
                Err(error) => {
                    self.settle_root_release_failure(
                        governance.as_ref(),
                        namespace,
                        &decision_evidence_ref,
                        &error,
                    )
                    .await?;
                    return Err(error);
                }
            };
            self.settle_root_release_audit(
                governance.as_ref(),
                namespace,
                if root_was_present {
                    RootReleaseAuditProgress::Released
                } else {
                    RootReleaseAuditProgress::Converged
                },
                &decision_evidence_ref,
            )
            .await?;
            let release = if root_was_present {
                RootReleaseState::Released {
                    acked_at: self.clock.now(),
                }
            } else {
                RootReleaseState::Converged {
                    observed_at: self.clock.now(),
                }
            };
            if let Err(error) = self
                .namespace_manager
                .record_root_release(
                    namespace.as_str(),
                    &current_target,
                    &current_target_etag,
                    release,
                )
                .await
            {
                self.settle_root_release_failure(
                    governance.as_ref(),
                    namespace,
                    &decision_evidence_ref,
                    &error,
                )
                .await?;
                return Err(error);
            }
            Ok(())
        }
        .await;
        if let Err(error) = self
            .lease_manager
            .release(source_namespace.as_str(), &parent_lease)
            .await
        {
            warn!(
                namespace = %source_namespace,
                error = %error,
                "branch root-release lease release failed (best-effort)"
            );
        }
        result
    }

    async fn require_released_branch_root_absent(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &crate::namespace::manager::NamespaceDeletionIntent,
    ) -> Result<()> {
        if !matches!(
            intent.root_release,
            Some(RootReleaseState::Released { .. } | RootReleaseState::Converged { .. })
        ) {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} cleanup requires final root release"
            )));
        }
        let visibility = intent.visibility.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "branch namespace {namespace} final root release has no visibility deadline"
            ))
        })?;
        let release_time = match intent.root_release.as_ref() {
            Some(RootReleaseState::Released { acked_at }) => *acked_at,
            Some(RootReleaseState::Converged { observed_at }) => *observed_at,
            Some(RootReleaseState::Pending) | None => {
                return Err(ZeppelinError::Serialization(format!(
                    "branch namespace {namespace} cleanup has no final root-release acknowledgement"
                )))
            }
        };
        if release_time < visibility.not_before {
            return Err(ZeppelinError::Serialization(format!(
                "branch namespace {namespace} root-release acknowledgement predates its reader-safety deadline"
            )));
        }
        self.require_branch_root_absent_after_grace(namespace, metadata, intent, visibility)
            .await
    }

    async fn require_branch_root_absent_after_grace(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
        intent: &NamespaceDeletionIntent,
        visibility: &VisibilityRemoval,
    ) -> Result<()> {
        if self.clock.now() < visibility.not_before {
            return Err(ZeppelinError::Serialization(format!(
                "branch namespace {namespace} root release predates its reader-safety deadline"
            )));
        }
        let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} is not a branch target"
            )));
        };
        let expected_root = intent.parent_root.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "branch namespace {namespace} has no exact parent root"
            ))
        })?;
        // A final acknowledgement, or metadata-last completion observed by a
        // stale worker, authorizes this exact-root absence proof after grace.
        // The source may itself finish deletion after root release, so a missing
        // parent manifest means the released root is no longer live. If the
        // source was recreated, its current manifest remains authoritative: any
        // reuse of this branch ID fails closed instead of allowing cleanup
        // through an ambiguous root.
        let Some(parent_manifest) =
            Manifest::read(&self.store, reservation.source_namespace.as_str()).await?
        else {
            return Ok(());
        };
        if let Some(current) = parent_manifest.branch_roots().get(&expected_root.branch_id) {
            if current != expected_root {
                return Err(BranchError::BranchRootMismatch {
                    branch_id: expected_root.branch_id,
                }
                .into());
            }
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {namespace} root-release acknowledgement exists while the parent root is still live"
            )));
        }
        Ok(())
    }

    async fn remove_completed_branch_visibility_marker(
        &self,
        namespace: &NamespaceId,
        reservation: &ForkReservationIdentity,
        intent: &NamespaceDeletionIntent,
        visibility: &VisibilityRemoval,
    ) -> Result<()> {
        let expected_key = BranchVisibilityRemovalMarker::key(
            namespace,
            reservation.branch_id,
            intent.incarnation.clone(),
        );
        if visibility.marker_key != expected_key {
            return Err(ZeppelinError::Serialization(format!(
                "branch namespace {namespace} visibility marker key changed before completed cleanup"
            )));
        }
        match self.store.delete(&expected_key).await {
            Ok(()) => Ok(()),
            Err(ZeppelinError::NotFound { key }) if key == expected_key => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn finish_deleting_cleanup(
        &self,
        namespace: &NamespaceId,
        intent: &crate::namespace::manager::NamespaceDeletionIntent,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        let identity = GovernedDeletionIdentity::from_intent(intent)?;
        let started = Instant::now();
        loop {
            self.require_unlocked_boundary(
                governance.as_ref(),
                namespace,
                DeletionBoundary::CleanupBatch,
            )
            .await?;
            let outcome = self
                .namespace_manager
                .cleanup_governed_delete_batch(namespace.as_str(), &identity, Duration::ZERO)
                .await?;
            if outcome.complete {
                break;
            }
            governance
                .settle_lifecycle_audit(DeletionLifecycleAudit::NamespaceDeleteCleanupIncomplete {
                    namespace: namespace.clone(),
                    remaining: 1,
                    decision_evidence_ref: intent.decision_evidence_ref.clone(),
                })
                .await?;
            if started.elapsed() >= budget {
                return Ok(NamespaceDeleteOutcome::AlreadyDeleting);
            }
        }

        self.require_unlocked_boundary(
            governance.as_ref(),
            namespace,
            DeletionBoundary::MetadataRemoval,
        )
        .await?;
        let (latest, _) = self
            .namespace_manager
            .read_metadata_versioned(namespace.as_str())
            .await?;
        let latest_intent = latest.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {namespace} lost its deletion intent before metadata removal"
            ))
        })?;
        if latest_intent != intent || latest.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {namespace} deletion intent changed before metadata removal"
            )));
        }
        self.load_bound_destruction_evidence(namespace, &latest, latest_intent)
            .await?;
        if let Err(delete_error) = self
            .namespace_manager
            .remove_deletion_metadata(namespace.as_str(), &identity)
            .await
        {
            match self
                .namespace_manager
                .read_metadata_versioned(namespace.as_str())
                .await
            {
                Err(ZeppelinError::NamespaceNotFound { .. }) => {
                    self.confirm_missing_after_resume(namespace, &latest)
                        .await?;
                }
                Ok(_) => return Err(delete_error),
                Err(read_error) => return Err(read_error),
            }
        }
        self.manifest_cache
            .invalidate_at(namespace.as_str(), self.clock.now());
        Ok(NamespaceDeleteOutcome::Deleted)
    }

    async fn require_unlocked_boundary(
        &self,
        governance: &dyn DeletionGovernance,
        namespace: &NamespaceId,
        boundary: DeletionBoundary,
    ) -> Result<crate::security::PreservationHeadProof> {
        let (guard, proof) = governance
            .preservation_boundary(namespace, boundary)
            .await?;
        if guard.is_locked() {
            return Err(crate::security::SecurityError::PreservationLocked.into());
        }
        Ok(proof)
    }

    async fn settle_root_release_audit(
        &self,
        governance: &dyn DeletionGovernance,
        namespace: &NamespaceId,
        progress: RootReleaseAuditProgress,
        decision_evidence_ref: &str,
    ) -> Result<()> {
        governance
            .settle_lifecycle_audit(DeletionLifecycleAudit::NamespaceDeleteRootRelease {
                namespace: namespace.clone(),
                progress,
                decision_evidence_ref: decision_evidence_ref.to_string(),
            })
            .await
    }

    async fn settle_root_release_failure(
        &self,
        governance: &dyn DeletionGovernance,
        namespace: &NamespaceId,
        decision_evidence_ref: &str,
        error: &ZeppelinError,
    ) -> Result<()> {
        self.settle_root_release_audit(
            governance,
            namespace,
            RootReleaseAuditProgress::Failed {
                class: Self::root_release_failure_class(error),
            },
            decision_evidence_ref,
        )
        .await
    }

    fn root_release_failure_class(error: &ZeppelinError) -> RootReleaseFailureClass {
        match error {
            ZeppelinError::Security(SecurityError::PreservationLocked) => {
                RootReleaseFailureClass::PreservationBlocked
            }
            ZeppelinError::LeaseHeld { .. }
            | ZeppelinError::LeaseExpired { .. }
            | ZeppelinError::FencingTokenStale { .. } => {
                RootReleaseFailureClass::ParentLeaseUnavailable
            }
            ZeppelinError::Branch(_)
            | ZeppelinError::ChecksumMismatch { .. }
            | ZeppelinError::MalformedControlKey { .. }
            | ZeppelinError::ManifestNotFound { .. }
            | ZeppelinError::NamespaceNotFound { .. }
            | ZeppelinError::NotFound { .. }
            | ZeppelinError::Serialization(_) => RootReleaseFailureClass::IntegrityRejected,
            ZeppelinError::Storage(_) | ZeppelinError::StoragePath(_) => {
                RootReleaseFailureClass::StorageUnavailable
            }
            ZeppelinError::AuditSink(_)
            | ZeppelinError::Security(SecurityError::AuditUnavailable) => {
                RootReleaseFailureClass::AuditUnavailable
            }
            _ => RootReleaseFailureClass::MutationRejected,
        }
    }

    /// List only the direct children represented by the authoritative root map.
    #[cfg_attr(not(feature = "branching-test-support"), allow(dead_code))]
    pub(crate) async fn list_children(
        &self,
        request: AuthorizedBranchList,
    ) -> Result<Vec<BranchDescriptor>> {
        let (manifest, _) =
            Manifest::read_versioned_required(&self.store, request.source.as_str()).await?;
        let mut children = Vec::with_capacity(manifest.branch_roots().len());
        for root in manifest.branch_roots().values() {
            if !request.disclose_child(&root.target_namespace)? {
                continue;
            }
            let metadata = match self
                .namespace_manager
                .read_metadata_versioned(root.target_namespace.as_str())
                .await
            {
                Ok((metadata, _)) => metadata,
                Err(error) => {
                    warn!(
                        source = %request.source,
                        target = %root.target_namespace,
                        branch_id = %root.branch_id,
                        error = %error,
                        "authorized branch metadata read failed integrity validation"
                    );
                    return Err(BranchError::BranchIntegrity.into());
                }
            };
            let NamespaceCreationKind::Fork(reservation) = &metadata.creation_kind else {
                warn!(
                    source = %request.source,
                    target = %root.target_namespace,
                    branch_id = %root.branch_id,
                    "authorized branch target is not a fork"
                );
                return Err(BranchError::BranchIntegrity.into());
            };
            let reservation_matches = reservation.branch_id == root.branch_id
                && reservation.source_namespace == request.source
                && reservation.target_namespace == root.target_namespace
                && reservation.target_incarnation == root.target_incarnation
                && reservation.created_at == root.created_at;
            let active_identity_matches = match metadata.state {
                NamespaceState::Creating => {
                    metadata
                        .branch_prepare
                        .as_ref()
                        .is_some_and(|prepare| match prepare.stage {
                            BranchPrepareStage::Reserved => {
                                metadata.branch_identity.is_none() && prepare.provisional.is_some()
                            }
                            BranchPrepareStage::Rooted
                            | BranchPrepareStage::ManifestPublished
                            | BranchPrepareStage::ActivationPending { .. } => {
                                prepare.provisional.is_none()
                                    && metadata.branch_identity.as_ref().is_some_and(|identity| {
                                        identity.matches_reservation(reservation)
                                            && identity.matches_root(root)
                                    })
                            }
                        })
                }
                NamespaceState::Active | NamespaceState::Deleting => {
                    metadata.branch_identity.as_ref().is_some_and(|identity| {
                        identity.matches_reservation(reservation) && identity.matches_root(root)
                    })
                }
            };
            if !reservation_matches || !active_identity_matches {
                warn!(
                    source = %request.source,
                    target = %root.target_namespace,
                    branch_id = %root.branch_id,
                    "authorized branch target identity failed integrity validation"
                );
                return Err(BranchError::BranchIntegrity.into());
            }
            let state = match metadata.state {
                NamespaceState::Creating => BranchLifecycleState::Preparing,
                NamespaceState::Active => BranchLifecycleState::Active,
                NamespaceState::Deleting => BranchLifecycleState::Deleting,
            };
            children.push(BranchDescriptor {
                target: root.target_namespace.clone(),
                branch_id: root.branch_id,
                target_incarnation: root.target_incarnation.clone(),
                depth: reservation.depth,
                created_at: reservation.created_at,
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
        Self::require_preparation_not_cancelled(&target)?;

        #[cfg(feature = "branching-test-support")]
        if stop == PrepareStop::AfterReservation {
            let stage = target.branch_prepare.as_ref().map(|prepare| prepare.stage);
            if stage == Some(BranchPrepareStage::Reserved) {
                return Ok(None);
            }
        }

        if target.branch_prepare.as_ref().is_some_and(|prepare| {
            matches!(
                prepare.stage,
                BranchPrepareStage::ManifestPublished
                    | BranchPrepareStage::ActivationPending { .. }
            )
        }) {
            let prepared = self.verify_prepared_target(&target).await?;
            return Ok(Some(PrepareForkOutcome::ExistingPrepared(prepared)));
        }

        if target
            .branch_prepare
            .as_ref()
            .is_some_and(|prepare| prepare.stage == BranchPrepareStage::Rooted)
        {
            let source_name = request.source.as_str();
            let mut lease = self.lease_manager.acquire(source_name).await?;
            let publish = async {
                let current = self
                    .namespace_manager
                    .read_creating_intent_strong(&target.name)
                    .await?
                    .0;
                Self::require_preparation_not_cancelled(&current)?;
                let candidate = self.rebuild_rooted_candidate(&current).await?;
                self.publish_and_mark_prepared(&target.name, &candidate, &mut lease)
                    .await
            }
            .await;
            if let Err(error) = self.lease_manager.release(source_name, &lease).await {
                warn!(
                    namespace = source_name,
                    error = %error,
                    "fork source lease release failed (best-effort)"
                );
            }
            publish?;
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
        let prepared = async {
            let rooted = self
                .root_and_install_identity(&request, &mut lease, stop)
                .await?;
            let candidate = match rooted {
                RootedProgress::Candidate(candidate) => candidate,
                #[cfg(feature = "branching-test-support")]
                RootedProgress::StoppedAfterRoot => return Ok(None),
            };
            self.publish_and_mark_prepared(request.target.as_str(), &candidate, &mut lease)
                .await?;
            Ok::<_, ZeppelinError>(Some(candidate))
        }
        .await;
        if let Err(error) = self.lease_manager.release(source_name, &lease).await {
            warn!(
                namespace = source_name,
                error = %error,
                "fork source lease release failed (best-effort)"
            );
        }
        let Some(_candidate) = prepared? else {
            return Ok(None);
        };
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
            branch_activation: None,
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
            Self::require_preparation_not_cancelled(&target)?;
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
        lease: &mut Lease,
    ) -> Result<()> {
        let source = candidate.branch.identity.source_namespace.as_str();
        let renewed = self.lease_manager.renew(source, lease).await?;
        if !self.lease_manager.validate(&renewed) {
            return Err(ZeppelinError::LeaseExpired {
                namespace: source.to_string(),
            });
        }
        *lease = renewed;
        let (source_manifest, source_version) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            source,
            candidate.branch.identity.source_incarnation.as_uuid(),
        )
        .await?;
        if source_version.is_deletion_fenced() {
            return Err(BranchError::SourceDeleting {
                namespace: candidate.branch.identity.source_namespace.clone(),
            }
            .into());
        }
        if source_manifest
            .branch_roots()
            .get(&candidate.branch.identity.branch_id)
            != Some(&candidate.branch.root)
        {
            return Err(BranchError::BranchRootMismatch {
                branch_id: candidate.branch.identity.branch_id,
            }
            .into());
        }
        let (before_publication, _) = self
            .namespace_manager
            .read_creating_intent_strong(target)
            .await?;
        Self::require_preparation_not_cancelled(&before_publication)?;
        if before_publication.branch_identity.as_ref() != Some(&candidate.branch.identity) {
            return Err(BranchError::IntentMismatch {
                target: candidate.branch.identity.target_namespace.clone(),
            }
            .into());
        }
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
            Self::require_preparation_not_cancelled(&metadata)?;
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
                BranchPrepareStage::ManifestPublished
                | BranchPrepareStage::ActivationPending { .. } => {
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
        Self::require_preparation_not_cancelled(target)?;
        let prepare = target.branch_prepare.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "prepared branch {} has no preparation milestone",
                target.name
            ))
        })?;
        if !matches!(
            prepare.stage,
            BranchPrepareStage::ManifestPublished | BranchPrepareStage::ActivationPending { .. }
        ) {
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
        if !identity.matches_root(root) {
            return Err(BranchError::BranchRootMismatch {
                branch_id: reservation.branch_id,
            }
            .into());
        }

        // Generation-one history is retention data and may be pruned after
        // the branch advances. Branch successor validation makes lineage
        // immutable, so the signed, incarnation-bound live manifest is the
        // durable creation-proof carrier for an active branch.
        let (live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            &metadata.name,
            identity.target_incarnation.as_uuid(),
        )
        .await?;
        let lineage = live.branch_lineage().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch {} live manifest has no lineage",
                metadata.name
            ))
        })?;
        if !identity.matches_lineage(lineage) {
            return Err(BranchError::BranchRootMismatch {
                branch_id: identity.branch_id,
            }
            .into());
        }
        Ok(())
    }

    async fn load_active_branch(&self, metadata: &NamespaceMetadata) -> Result<PreparedBranch> {
        let identity = metadata.branch_identity.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch {} has no immutable identity",
                metadata.name
            ))
        })?;
        let (source_live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            identity.source_namespace.as_str(),
            identity.source_incarnation.as_uuid(),
        )
        .await?;
        let root = source_live
            .branch_roots()
            .get(&identity.branch_id)
            .ok_or_else(|| BranchError::BranchRootMissing {
                branch_id: identity.branch_id,
            })?;
        let (target_live, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            &metadata.name,
            identity.target_incarnation.as_uuid(),
        )
        .await?;
        let lineage = target_live.branch_lineage().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "active branch {} live manifest has no lineage",
                metadata.name
            ))
        })?;
        Ok(PreparedBranch {
            identity: identity.clone(),
            lineage: lineage.clone(),
            root: root.clone(),
        })
    }

    fn target_matches_child_root(
        parent: &NamespaceMetadata,
        target: &NamespaceMetadata,
        root: &BranchRoot,
    ) -> bool {
        let Some(parent_incarnation) = parent.incarnation_id.as_ref() else {
            return false;
        };
        let NamespaceCreationKind::Fork(reservation) = &target.creation_kind else {
            return false;
        };
        if target.name != root.target_namespace.as_str()
            || target.incarnation_id.as_ref() != Some(&root.target_incarnation)
            || reservation.branch_id != root.branch_id
            || reservation.source_namespace.as_str() != parent.name
            || &reservation.source_incarnation != parent_incarnation
            || reservation.target_namespace != root.target_namespace
            || reservation.target_incarnation != root.target_incarnation
        {
            return false;
        }

        match target.branch_identity.as_ref() {
            Some(identity) => {
                identity.matches_reservation(reservation) && identity.matches_root(root)
            }
            None => {
                target.state == NamespaceState::Creating
                    && target.branch_prepare.as_ref().is_some_and(|prepare| {
                        prepare.stage == BranchPrepareStage::Reserved
                            && prepare.branch_id == root.branch_id
                            && prepare.target_incarnation == root.target_incarnation
                    })
            }
        }
    }

    async fn confirm_child_root_failure(
        &self,
        parent: &NamespaceMetadata,
        expected_root: &BranchRoot,
        target_was_absent: bool,
    ) -> Result<()> {
        let parent_incarnation = parent.incarnation_id.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "branch-root parent {} has no namespace incarnation",
                parent.name
            ))
        })?;
        let (current_parent, _) = Manifest::read_versioned_required_for_incarnation(
            &self.store,
            &parent.name,
            parent_incarnation.as_uuid(),
        )
        .await?;
        match current_parent.branch_roots().get(&expected_root.branch_id) {
            None => Ok(()),
            Some(current_root) if current_root != expected_root => {
                Err(BranchError::BranchRootMismatch {
                    branch_id: expected_root.branch_id,
                }
                .into())
            }
            Some(_) if target_was_absent => Err(BranchError::OrphanBranchRoot {
                source_namespace: NamespaceId::parse(parent.name.clone()).map_err(|_| {
                    ZeppelinError::Validation(format!(
                        "invalid branch-root parent namespace: {}",
                        parent.name
                    ))
                })?,
                root: expected_root.clone(),
            }
            .into()),
            Some(_) => Err(BranchError::BranchRootMismatch {
                branch_id: expected_root.branch_id,
            }
            .into()),
        }
    }

    async fn verify_live_child_roots(
        &self,
        parent: &NamespaceMetadata,
        started: &Instant,
        budget: Duration,
    ) -> Result<bool> {
        if started.elapsed() >= budget {
            return Ok(false);
        }
        let (parent_manifest, _) = match parent.incarnation_id.as_ref() {
            Some(incarnation) => {
                Manifest::read_versioned_required_for_incarnation(
                    &self.store,
                    &parent.name,
                    incarnation.as_uuid(),
                )
                .await?
            }
            None => Manifest::read_versioned_required(&self.store, &parent.name).await?,
        };

        for root in parent_manifest.branch_roots().values() {
            if started.elapsed() >= budget {
                return Ok(false);
            }
            match self
                .namespace_manager
                .read_metadata_versioned(root.target_namespace.as_str())
                .await
            {
                Ok((target, _)) if Self::target_matches_child_root(parent, &target, root) => {}
                Ok(_) => {
                    self.confirm_child_root_failure(parent, root, false).await?;
                }
                Err(ZeppelinError::NamespaceNotFound { .. }) => {
                    self.confirm_child_root_failure(parent, root, true).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    async fn verify_active_graph_state(
        &self,
        metadata: &NamespaceMetadata,
        started: &Instant,
        budget: Duration,
    ) -> Result<bool> {
        if started.elapsed() >= budget {
            return Ok(false);
        }
        self.verify_active_branch(metadata).await?;
        self.verify_live_child_roots(metadata, started, budget)
            .await
    }

    /// Resume governed deletion and repair already-authorized branch state.
    #[allow(dead_code)] // Feature support exercises this seam before Phase 10 readiness wiring.
    pub(crate) async fn maintain(
        &self,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Result<BranchMaintenanceReport> {
        if budget.is_zero() {
            return Ok(BranchMaintenanceReport::default());
        }
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
            if started.elapsed() >= budget {
                break;
            }
            if metadata.state == NamespaceState::Deleting
                || (metadata.state == NamespaceState::Active && metadata.deletion_intent.is_some())
            {
                report.deletions_inspected += 1;
                let remaining = budget.saturating_sub(started.elapsed());
                match self
                    .resume_delete(
                        &NamespaceId::new(target_name.to_string())?,
                        Arc::clone(&governance),
                        remaining,
                    )
                    .await
                {
                    Ok(NamespaceDeleteOutcome::Deleted) => {
                        report.deletions_completed += 1;
                    }
                    Ok(NamespaceDeleteOutcome::AlreadyDeleting) => {
                        report.deletions_in_progress += 1;
                    }
                    Ok(NamespaceDeleteOutcome::BranchGraceWait { .. }) => {
                        report.deletions_in_progress += 1;
                        report.branch_grace_waiting += 1;
                    }
                    Err(ZeppelinError::NamespaceNotFound { .. }) => {
                        self.confirm_missing_after_resume(
                            &NamespaceId::new(target_name.to_string())?,
                            &metadata,
                        )
                        .await?;
                        report.deletions_completed += 1;
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }
            if metadata.state == NamespaceState::Active {
                if !self
                    .verify_active_graph_state(&metadata, &started, budget)
                    .await?
                {
                    break;
                }
                if matches!(metadata.creation_kind, NamespaceCreationKind::Fork(_)) {
                    report.active_verified += 1;
                }
                continue;
            }
            let reservation = match &metadata.creation_kind {
                NamespaceCreationKind::Fork(reservation) => reservation.clone(),
                NamespaceCreationKind::Root => continue,
            };
            report.inspected += 1;
            match metadata.state {
                NamespaceState::Active => continue,
                NamespaceState::Deleting => continue,
                NamespaceState::Creating => {}
            }
            if metadata.deletion_intent.is_some() {
                report.awaiting_authorized_cancellation += 1;
                continue;
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
                        Self::require_preparation_not_cancelled(&fresh)?;
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
                        self.publish_and_mark_prepared(target_name, &candidate, &mut lease)
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
                    let _ = candidate;
                    report.manifests_published += 1;
                }
                BranchPrepareStage::Rooted => {
                    let source_name = reservation.source_namespace.as_str();
                    let mut lease = self.lease_manager.acquire(source_name).await?;
                    let publish = async {
                        let current = self
                            .namespace_manager
                            .read_creating_intent_strong(target_name)
                            .await?
                            .0;
                        Self::require_preparation_not_cancelled(&current)?;
                        let candidate = self.rebuild_rooted_candidate(&current).await?;
                        self.publish_and_mark_prepared(target_name, &candidate, &mut lease)
                            .await
                    }
                    .await;
                    if let Err(error) = self.lease_manager.release(source_name, &lease).await {
                        warn!(namespace = source_name, error = %error, "branch maintenance lease release failed (best-effort)");
                    }
                    publish?;
                    report.manifests_published += 1;
                }
                BranchPrepareStage::ManifestPublished => {
                    self.verify_prepared_target(&metadata).await?;
                    report.prepared_verified += 1;
                }
                BranchPrepareStage::ActivationPending { .. } => {
                    // Maintenance may verify immutable preparation facts, but
                    // only an authenticated activation-governance adapter may
                    // resolve the policy guard or publish live visibility.
                    self.verify_prepared_target(&metadata).await?;
                    report.prepared_verified += 1;
                }
            }
        }
        Ok(report)
    }
}
