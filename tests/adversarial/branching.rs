//! Branch operations and lifecycle bookkeeping for the deterministic adversarial profile.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Logical branch lifecycle relevant to deletion/root-release oracles.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycle {
    Active,
    Deleting,
    Released,
}

/// Model violation raised when HTTP behavior contradicts the durable branch root.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchDeleteViolation {
    DuplicateBranchTarget {
        target_namespace: String,
    },
    UnknownBranchTarget {
        target_namespace: String,
    },
    BranchDeleteAcceptedWithoutRetainedRoot,
    RootReleasedBeforeReaderSafetyDeadline {
        observed_at: DateTime<Utc>,
        not_before: DateTime<Utc>,
    },
    SourceDeleteSucceededWithRetainedRoot {
        status: u16,
    },
    SourceDeleteDidNotConflictWithRetainedRoot {
        status: u16,
    },
    SourceDeleteConflictHadWrongCode {
        code: Option<String>,
    },
    SourceDeleteRejectedAfterRootRelease {
        status: u16,
    },
    RootReleaseObservedOutsideDeletion,
    MissingReaderSafetyDeadline,
    ActiveBranchMissingMatchingRoot,
    BranchIdentityMismatch {
        field: String,
        expected: String,
        observed: Option<String>,
    },
    ForkSnapshotChanged {
        namespace: String,
        expected_ids: BTreeSet<String>,
        observed_ids: BTreeSet<String>,
    },
    SourceTargetWritesCrossed {
        namespace: String,
        unexpected_ids: BTreeSet<String>,
    },
    ForeignArtifactDelete {
        key: String,
        target_namespace: String,
    },
    RootReleasedBeforeVisibilityRemoval,
    MaterializationReleasedRoot,
    RestartMaintenanceDidNotConverge,
    MergeOperationGenerated {
        kind: String,
    },
    BookkeepingOverflow,
}

/// Per-edge state held by the adversarial model while a branch is deleted.
///
/// Identity fields make stale-incarnation/root observations distinguishable in
/// replay artifacts. `root_retained` remains authoritative for source-delete
/// expectations: elapsed grace alone never permits the source delete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchDeleteBookkeeping {
    pub source_namespace: String,
    pub source_incarnation: String,
    pub target_namespace: String,
    pub target_incarnation: String,
    pub branch_id: String,
    pub fork_generation: u64,
    pub depth: u16,
    lifecycle: BranchLifecycle,
    root_retained: bool,
    reader_safety_not_before: Option<DateTime<Utc>>,
    expected_source_conflicts: u64,
    #[serde(default)]
    fork_snapshot_ids: BTreeSet<String>,
    #[serde(default)]
    expected_source_ids: BTreeSet<String>,
    #[serde(default)]
    expected_target_ids: BTreeSet<String>,
    #[serde(default)]
    source_only_ids: BTreeSet<String>,
    #[serde(default)]
    target_only_ids: BTreeSet<String>,
    #[serde(default)]
    matching_root_observed: bool,
    #[serde(default)]
    visibility_removed: bool,
    #[serde(default)]
    materialized: bool,
    #[serde(default)]
    restart_pending: bool,
}

impl BranchDeleteBookkeeping {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        source_namespace: impl Into<String>,
        source_incarnation: impl Into<String>,
        target_namespace: impl Into<String>,
        target_incarnation: impl Into<String>,
        branch_id: impl Into<String>,
        fork_generation: u64,
        depth: u16,
    ) -> Self {
        Self {
            source_namespace: source_namespace.into(),
            source_incarnation: source_incarnation.into(),
            target_namespace: target_namespace.into(),
            target_incarnation: target_incarnation.into(),
            branch_id: branch_id.into(),
            fork_generation,
            depth,
            lifecycle: BranchLifecycle::Active,
            root_retained: true,
            reader_safety_not_before: None,
            expected_source_conflicts: 0,
            fork_snapshot_ids: BTreeSet::new(),
            expected_source_ids: BTreeSet::new(),
            expected_target_ids: BTreeSet::new(),
            source_only_ids: BTreeSet::new(),
            target_only_ids: BTreeSet::new(),
            matching_root_observed: false,
            visibility_removed: false,
            materialized: false,
            restart_pending: false,
        }
    }

    #[must_use]
    pub fn with_fork_snapshot(mut self, ids: impl IntoIterator<Item = String>) -> Self {
        self.fork_snapshot_ids = ids.into_iter().collect();
        self.expected_source_ids = self.fork_snapshot_ids.clone();
        self.expected_target_ids = self.fork_snapshot_ids.clone();
        self
    }

    pub fn observe_matching_root(
        &mut self,
        source_incarnation: &str,
        target_incarnation: &str,
        branch_id: &str,
        fork_generation: u64,
        depth: u16,
    ) -> Result<(), BranchDeleteViolation> {
        for (field, expected, observed) in [
            (
                "source_incarnation",
                self.source_incarnation.clone(),
                source_incarnation.to_string(),
            ),
            (
                "target_incarnation",
                self.target_incarnation.clone(),
                target_incarnation.to_string(),
            ),
            ("branch_id", self.branch_id.clone(), branch_id.to_string()),
            (
                "fork_generation",
                self.fork_generation.to_string(),
                fork_generation.to_string(),
            ),
            ("depth", self.depth.to_string(), depth.to_string()),
        ] {
            if expected != observed {
                return Err(BranchDeleteViolation::BranchIdentityMismatch {
                    field: field.to_string(),
                    expected,
                    observed: Some(observed),
                });
            }
        }
        self.matching_root_observed = true;
        Ok(())
    }

    pub fn require_matching_root(&self) -> Result<(), BranchDeleteViolation> {
        if self.lifecycle == BranchLifecycle::Active && !self.matching_root_observed {
            return Err(BranchDeleteViolation::ActiveBranchMissingMatchingRoot);
        }
        Ok(())
    }

    pub fn observe_source_upserts(&mut self, ids: impl IntoIterator<Item = String>) {
        for id in ids {
            self.expected_source_ids.insert(id.clone());
            self.source_only_ids.insert(id);
        }
    }

    pub fn observe_target_upserts(&mut self, ids: impl IntoIterator<Item = String>) {
        for id in ids {
            self.expected_target_ids.insert(id.clone());
            self.target_only_ids.insert(id);
        }
    }

    pub fn observe_source_deletes<'a>(&mut self, ids: impl IntoIterator<Item = &'a str>) {
        for id in ids {
            self.expected_source_ids.remove(id);
            self.source_only_ids.remove(id);
        }
    }

    pub fn observe_target_deletes<'a>(&mut self, ids: impl IntoIterator<Item = &'a str>) {
        for id in ids {
            self.expected_target_ids.remove(id);
            self.target_only_ids.remove(id);
        }
    }

    pub fn observe_namespace_view(
        &self,
        namespace: &str,
        observed_ids: BTreeSet<String>,
    ) -> Result<(), BranchDeleteViolation> {
        let expected_ids = if namespace == self.source_namespace {
            &self.expected_source_ids
        } else if namespace == self.target_namespace {
            &self.expected_target_ids
        } else {
            return Ok(());
        };
        let crossed: BTreeSet<String> = if namespace == self.source_namespace {
            observed_ids
                .intersection(&self.target_only_ids)
                .cloned()
                .collect()
        } else {
            observed_ids
                .intersection(&self.source_only_ids)
                .cloned()
                .collect()
        };
        if !crossed.is_empty() {
            return Err(BranchDeleteViolation::SourceTargetWritesCrossed {
                namespace: namespace.to_string(),
                unexpected_ids: crossed,
            });
        }
        if &observed_ids != expected_ids {
            return Err(BranchDeleteViolation::ForkSnapshotChanged {
                namespace: namespace.to_string(),
                expected_ids: expected_ids.clone(),
                observed_ids,
            });
        }
        Ok(())
    }

    pub fn observe_target_delete_key(&self, key: &str) -> Result<(), BranchDeleteViolation> {
        let target_prefix = format!("{}/", self.target_namespace);
        if !key.starts_with(&target_prefix) {
            return Err(BranchDeleteViolation::ForeignArtifactDelete {
                key: key.to_string(),
                target_namespace: self.target_namespace.clone(),
            });
        }
        Ok(())
    }

    pub fn observe_materialized(
        &mut self,
        root_retained: bool,
    ) -> Result<(), BranchDeleteViolation> {
        if !root_retained {
            return Err(BranchDeleteViolation::MaterializationReleasedRoot);
        }
        self.materialized = true;
        self.root_retained = true;
        Ok(())
    }

    pub fn observe_restart(&mut self) {
        self.restart_pending = self.lifecycle == BranchLifecycle::Deleting;
    }

    pub fn observe_restart_maintenance(
        &mut self,
        converged: bool,
    ) -> Result<(), BranchDeleteViolation> {
        if self.restart_pending && !converged {
            return Err(BranchDeleteViolation::RestartMaintenanceDidNotConverge);
        }
        self.restart_pending = false;
        Ok(())
    }

    pub fn observe_generated_operation_kind(kind: &str) -> Result<(), BranchDeleteViolation> {
        if kind.contains("merge") || kind.contains("rebase") || kind.contains("diff_branch") {
            return Err(BranchDeleteViolation::MergeOperationGenerated {
                kind: kind.to_string(),
            });
        }
        Ok(())
    }

    pub fn begin_branch_delete_without_deadline(&mut self) {
        self.lifecycle = BranchLifecycle::Deleting;
        self.root_retained = true;
        self.visibility_removed = true;
    }

    pub fn observe_branch_delete_accepted(
        &mut self,
        not_before: DateTime<Utc>,
        root_retained: bool,
    ) -> Result<(), BranchDeleteViolation> {
        if !root_retained {
            return Err(BranchDeleteViolation::BranchDeleteAcceptedWithoutRetainedRoot);
        }
        self.lifecycle = BranchLifecycle::Deleting;
        self.root_retained = true;
        self.visibility_removed = true;
        self.reader_safety_not_before = Some(not_before);
        Ok(())
    }

    pub fn observe_branch_delete_progress(
        &mut self,
        observed_at: DateTime<Utc>,
        root_retained: bool,
    ) -> Result<(), BranchDeleteViolation> {
        if self.lifecycle != BranchLifecycle::Deleting {
            return Err(BranchDeleteViolation::RootReleaseObservedOutsideDeletion);
        }
        if root_retained {
            self.root_retained = true;
            return Ok(());
        }
        if !self.visibility_removed {
            return Err(BranchDeleteViolation::RootReleasedBeforeVisibilityRemoval);
        }
        let not_before = self
            .reader_safety_not_before
            .ok_or(BranchDeleteViolation::MissingReaderSafetyDeadline)?;
        if observed_at < not_before {
            return Err(
                BranchDeleteViolation::RootReleasedBeforeReaderSafetyDeadline {
                    observed_at,
                    not_before,
                },
            );
        }
        self.root_retained = false;
        self.lifecycle = BranchLifecycle::Released;
        Ok(())
    }

    pub fn observe_source_delete(
        &mut self,
        status: u16,
        code: Option<&str>,
        _observed_at: DateTime<Utc>,
    ) -> Result<(), BranchDeleteViolation> {
        if self.root_retained {
            if (200..300).contains(&status) {
                return Err(
                    BranchDeleteViolation::SourceDeleteSucceededWithRetainedRoot { status },
                );
            }
            if status != 409 {
                return Err(
                    BranchDeleteViolation::SourceDeleteDidNotConflictWithRetainedRoot { status },
                );
            }
            if code != Some("namespace_has_live_branches") {
                return Err(BranchDeleteViolation::SourceDeleteConflictHadWrongCode {
                    code: code.map(str::to_string),
                });
            }
            self.expected_source_conflicts = self
                .expected_source_conflicts
                .checked_add(1)
                .ok_or(BranchDeleteViolation::BookkeepingOverflow)?;
            return Ok(());
        }

        if !(200..300).contains(&status) {
            return Err(BranchDeleteViolation::SourceDeleteRejectedAfterRootRelease { status });
        }
        Ok(())
    }

    #[must_use]
    pub const fn lifecycle(&self) -> BranchLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub const fn expected_source_conflicts(&self) -> u64 {
        self.expected_source_conflicts
    }

    #[must_use]
    pub const fn root_retained(&self) -> bool {
        self.root_retained
    }

    #[must_use]
    pub const fn matching_root_observed(&self) -> bool {
        self.matching_root_observed
    }

    #[must_use]
    pub const fn materialized(&self) -> bool {
        self.materialized
    }
}

/// One real HTTP execution carried by the runner's replay-compatible record.
pub type BranchingDeleteOpRecord = super::ops::OpRecord;

/// Aggregate proof returned by the feature-gated two-seed smoke.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchingDeleteSmokeSummary {
    pub seeds_run: u64,
    pub failed_seeds: u64,
    pub delete_branch_ops: u64,
    pub delete_source_with_branches_ops: u64,
    pub expected_source_conflicts: u64,
}

#[cfg(feature = "branching-test-support")]
mod smoke {
    use std::collections::BTreeSet;
    use std::fs::{self, File};
    use std::io::{BufWriter, Write};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use chrono::{DateTime, Utc};
    use reqwest::Client;
    use serde::Serialize;
    use serde_json::json;
    use zeppelin::config::Config;
    use zeppelin::namespace::branching::test_support::{
        activate_fork_for_test, branch_control_snapshot,
    };
    use zeppelin::namespace::branching::{ForkIdentity, PrepareForkOutcome};
    use zeppelin::namespace::manager::NamespaceMetadata;
    use zeppelin::namespace::NamespaceId;
    use zeppelin::time::{Clock, TimeSource};
    use zeppelin::wal::Lease;

    use super::{
        BranchDeleteBookkeeping, BranchDeleteViolation, BranchingDeleteOpRecord,
        BranchingDeleteSmokeSummary,
    };
    use crate::adversarial::generator::{BranchingDeleteSchedule, BranchingDeleteWindow, Coverage};
    use crate::adversarial::model::Model;
    use crate::adversarial::ops::{ActorSel, BranchingOp, ExecutionMetadata, Op};
    use crate::adversarial::oracle;
    use crate::adversarial::runner::execute_branching_http;
    use crate::adversarial::{RunMode, RunnerEnv};
    use crate::common::harness::TestHarness;
    use crate::common::server::{
        client_with_bearer, start_test_server_full,
        start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    };

    const SETUP_SEAM: &str = "non-production branching-test-support::activate_fork_for_test";
    const ACTIVATION_BLOCKER: &str =
        "production HTTP fork currently prepares but does not activate the target";

    #[derive(Debug)]
    struct AdjustableClock(Mutex<DateTime<Utc>>);

    impl AdjustableClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self(Mutex::new(now))
        }

        fn set(&self, now: DateTime<Utc>) -> Result<(), String> {
            let mut current = self
                .0
                .lock()
                .map_err(|_| "branching smoke clock mutex poisoned".to_string())?;
            *current = now;
            Ok(())
        }
    }

    impl TimeSource for AdjustableClock {
        fn now(&self) -> DateTime<Utc> {
            *self
                .0
                .lock()
                .unwrap_or_else(|_| panic!("branching smoke clock mutex poisoned"))
        }
    }

    #[derive(Debug, Serialize)]
    struct SetupRecord<'a> {
        seed: u64,
        seam: &'a str,
        production_activation_blocker: &'a str,
        source_setup_lease_holder: String,
        source_setup_lease_fencing_token: u64,
        source_setup_lease_acquired_at: DateTime<Utc>,
        source_setup_lease_expires_at: DateTime<Utc>,
        server_clock_after_setup_lease_expiry: DateTime<Utc>,
        pre_grace_cleanup_drained_by_restart: bool,
        pre_grace_source_attempts: u8,
        elapsed_grace_source_attempts: u8,
        source_namespace: &'a str,
        target_namespace: &'a str,
        branch_id: String,
        source_incarnation: String,
        target_incarnation: String,
        fork_generation: u64,
        depth: u16,
    }

    struct SeedProof {
        operations: Vec<BranchingDeleteOpRecord>,
        expected_source_conflicts: u64,
    }

    pub(super) async fn run(env: RunnerEnv) -> Result<BranchingDeleteSmokeSummary, String> {
        if env.mode != RunMode::Deterministic {
            return Err("branching deletion smoke requires deterministic mode".to_string());
        }
        if env.profile.is_some() {
            return Err("branching deletion smoke does not accept a chaos profile".to_string());
        }
        if env.seeds.len() < 2 {
            return Err("branching deletion smoke requires at least two pinned seeds".to_string());
        }
        let distinct_seeds = env.seeds.iter().copied().collect::<BTreeSet<_>>();
        if distinct_seeds.len() != env.seeds.len() {
            return Err("branching deletion smoke pinned seeds must be unique".to_string());
        }

        let mut summary = BranchingDeleteSmokeSummary::default();
        for seed in &env.seeds {
            let proof = run_seed(*seed, &env)
                .await
                .map_err(|error| format!("branching deletion smoke seed {seed}: {error}"))?;
            summary.seeds_run = summary
                .seeds_run
                .checked_add(1)
                .ok_or_else(|| "branching smoke seed count overflowed".to_string())?;
            for record in proof.operations {
                match &record.op {
                    Op::Branching(BranchingOp::DeleteBranch { .. }) => {
                        summary.delete_branch_ops =
                            summary.delete_branch_ops.checked_add(1).ok_or_else(|| {
                                "delete-branch operation count overflowed".to_string()
                            })?;
                    }
                    Op::Branching(BranchingOp::DeleteSourceWithBranches { .. }) => {
                        summary.delete_source_with_branches_ops = summary
                            .delete_source_with_branches_ops
                            .checked_add(1)
                            .ok_or_else(|| {
                                "delete-source-with-branches operation count overflowed".to_string()
                            })?;
                    }
                    _ => {
                        return Err(format!(
                            "branching deletion smoke recorded non-deletion operation: {:?}",
                            record.op
                        ))
                    }
                }
            }
            summary.expected_source_conflicts = summary
                .expected_source_conflicts
                .checked_add(proof.expected_source_conflicts)
                .ok_or_else(|| "expected source-conflict count overflowed".to_string())?;
        }
        Ok(summary)
    }

    async fn run_seed(seed: u64, env: &RunnerEnv) -> Result<SeedProof, String> {
        let run_started = Instant::now();
        let harness = TestHarness::new().await;
        let source = format!("{}-branch-adv-{seed}-source", harness.prefix);
        let target = format!("{}-branch-adv-{seed}-target", harness.prefix);
        let schedule = BranchingDeleteSchedule::for_seed(seed);
        let config = branching_smoke_config()?;
        let adjustable_clock = Arc::new(AdjustableClock::new(Utc::now()));
        let clock = Clock::from_source(adjustable_clock.clone());
        let server = start_test_server_full(
            harness.store.clone(),
            Some(harness.prefix.clone()),
            config.clone(),
            false,
            Some(clock.clone()),
        )
        .await;
        let admin_bearer = server.admin_bearer.clone();
        let client = client_with_bearer(&server.admin_bearer);

        let create_response = client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&json!({
                "name": source.clone(),
                "dimensions": 4,
                "distance_metric": "cosine"
            }))
            .send()
            .await
            .map_err(|error| format!("source create request failed: {error}"))?;
        if create_response.status().as_u16() != 201 {
            let status = create_response.status();
            let body = create_response
                .text()
                .await
                .map_err(|error| format!("source create error response read failed: {error}"))?;
            return Err(format!("source create returned {status}: {body}"));
        }

        // Explicit test-only setup. Deletion requests below never bypass HTTP.
        let prepared = activate_fork_for_test(
            server.store.clone(),
            NamespaceId::new(source.clone())
                .map_err(|error| format!("invalid generated source namespace: {error}"))?,
            NamespaceId::new(target.clone())
                .map_err(|error| format!("invalid generated target namespace: {error}"))?,
            config.indexing.clone(),
            config.branching.clone(),
        )
        .await
        .map_err(|error| format!("{SETUP_SEAM} failed: {error}"))?;
        let identity = match prepared {
            PrepareForkOutcome::Prepared(branch) | PrepareForkOutcome::ExistingPrepared(branch) => {
                branch.identity
            }
        };
        // The feature-gated activation adapter composes its own system clock.
        // Read the source lease back from object storage, then move the
        // server's injected clock strictly past that authoritative expiry.
        // This distinguishes setup-clock skew from a deletion-path lease
        // conflict without sleeping or trusting process-local time.
        let source_setup_lease = read_authoritative_lease(&server.store, &source).await?;
        let server_clock_after_setup_lease_expiry = source_setup_lease
            .expires_at
            .checked_add_signed(chrono::Duration::seconds(1))
            .ok_or_else(|| {
                "source setup lease expiry could not advance by one second".to_string()
            })?;
        adjustable_clock.set(server_clock_after_setup_lease_expiry)?;
        let mut model = Model::default();
        model
            .track_branch_delete(model_from_identity(&identity))
            .map_err(|violation| {
                branch_oracle_error("branch model setup", 0, &target, violation)
            })?;
        let setup = SetupRecord {
            seed,
            seam: SETUP_SEAM,
            production_activation_blocker: ACTIVATION_BLOCKER,
            source_setup_lease_holder: source_setup_lease.holder_id,
            source_setup_lease_fencing_token: source_setup_lease.fencing_token,
            source_setup_lease_acquired_at: source_setup_lease.acquired_at,
            source_setup_lease_expires_at: source_setup_lease.expires_at,
            server_clock_after_setup_lease_expiry,
            pre_grace_cleanup_drained_by_restart: true,
            pre_grace_source_attempts: schedule.pre_grace_source_attempts,
            elapsed_grace_source_attempts: schedule.elapsed_grace_source_attempts,
            source_namespace: &source,
            target_namespace: &target,
            branch_id: identity.branch_id.to_string(),
            source_incarnation: identity.source_incarnation.to_string(),
            target_incarnation: identity.target_incarnation.to_string(),
            fork_generation: identity.source_generation.get(),
            depth: identity.depth,
        };

        let mut operations = Vec::new();
        let mut coverage = Coverage::default();
        let mut next_index = 0_u64;
        let initial_delete = Op::Branching(BranchingOp::DeleteBranch {
            actor: ActorSel::ADMIN,
            namespace: target.clone(),
        });
        let index = take_operation_index(&mut next_index)?;
        let initial_delete = execute_delete_operation(
            &client,
            &server.base_url,
            initial_delete,
            index,
            run_started,
        )
        .await?;
        require_status(&initial_delete, 202).map_err(|error| {
            format!(
                "{error}; source setup lease holder={} token={} acquired_at={} expires_at={}; injected_server_clock={}",
                setup.source_setup_lease_holder,
                setup.source_setup_lease_fencing_token,
                setup.source_setup_lease_acquired_at,
                setup.source_setup_lease_expires_at,
                setup.server_clock_after_setup_lease_expiry,
            )
        })?;
        let initial_delete_index = initial_delete.index;
        coverage.record(&initial_delete.op);
        operations.push(initial_delete);
        let not_before = read_reader_safety_deadline(&server.store, &target).await?;
        let retained = exact_root_retained(&server.store, &source, &identity).await?;
        model
            .observe_branch_delete_accepted(&target, not_before, retained)
            .map_err(|violation| {
                branch_oracle_error(
                    "initial branch delete",
                    initial_delete_index,
                    &target,
                    violation,
                )
            })?;

        // A successful namespace DELETE schedules a real post-audit cleanup
        // worker. Drain that pre-grace worker before moving the injected clock
        // to the deadline; otherwise the logical time jump can make the first
        // worker race the explicit retry over the same visibility marker. A
        // restart preserves S3 authority and the caller credential while
        // giving this sequential smoke a deterministic recovery boundary.
        model.observe_branch_restart(&target).map_err(|violation| {
            branch_oracle_error("pre-restart branch model", next_index, &target, violation)
        })?;
        server.shutdown().await;
        let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            harness.store.clone(),
            Some(harness.prefix.clone()),
            config.clone(),
            false,
            Some(clock.clone()),
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;
        let client = client_with_bearer(&server.admin_bearer);

        for source_delete_before_grace in
            schedule.source_delete_ops(&source, BranchingDeleteWindow::PreGrace)
        {
            let index = take_operation_index(&mut next_index)?;
            let retained = exact_root_retained(&server.store, &source, &identity).await?;
            model
                .observe_branch_delete_progress(&target, clock.now(), retained)
                .map_err(|violation| {
                    branch_oracle_error(
                        "pre-grace exact-root before source delete",
                        index,
                        &target,
                        violation,
                    )
                })?;
            let source_delete_before_grace = execute_delete_operation(
                &client,
                &server.base_url,
                source_delete_before_grace,
                index,
                run_started,
            )
            .await?;
            let retained = exact_root_retained(&server.store, &source, &identity).await?;
            model
                .observe_branch_delete_progress(&target, clock.now(), retained)
                .map_err(|violation| {
                    branch_oracle_error(
                        "pre-grace exact-root",
                        source_delete_before_grace.index,
                        &target,
                        violation,
                    )
                })?;
            model
                .observe_branch_source_delete(
                    &target,
                    source_delete_before_grace.status,
                    source_delete_before_grace.response["code"].as_str(),
                    clock.now(),
                )
                .map_err(|violation| {
                    branch_oracle_error(
                        "pre-grace source delete",
                        source_delete_before_grace.index,
                        &target,
                        violation,
                    )
                })?;
            coverage.record(&source_delete_before_grace.op);
            operations.push(source_delete_before_grace);
        }

        adjustable_clock.set(not_before)?;
        for source_delete_before_release in
            schedule.source_delete_ops(&source, BranchingDeleteWindow::ElapsedGraceBeforeRelease)
        {
            let index = take_operation_index(&mut next_index)?;
            let retained = exact_root_retained(&server.store, &source, &identity).await?;
            model
                .observe_branch_delete_progress(&target, clock.now(), retained)
                .map_err(|violation| {
                    branch_oracle_error(
                        "elapsed-grace exact-root before source delete",
                        index,
                        &target,
                        violation,
                    )
                })?;
            let source_delete_before_release = execute_delete_operation(
                &client,
                &server.base_url,
                source_delete_before_release,
                index,
                run_started,
            )
            .await?;
            let retained = exact_root_retained(&server.store, &source, &identity).await?;
            model
                .observe_branch_delete_progress(&target, clock.now(), retained)
                .map_err(|violation| {
                    branch_oracle_error(
                        "elapsed-grace exact-root",
                        source_delete_before_release.index,
                        &target,
                        violation,
                    )
                })?;
            model
                .observe_branch_source_delete(
                    &target,
                    source_delete_before_release.status,
                    source_delete_before_release.response["code"].as_str(),
                    clock.now(),
                )
                .map_err(|violation| {
                    branch_oracle_error(
                        "elapsed-grace pre-release source delete",
                        source_delete_before_release.index,
                        &target,
                        violation,
                    )
                })?;
            coverage.record(&source_delete_before_release.op);
            operations.push(source_delete_before_release);
        }

        let resumed_delete = Op::Branching(BranchingOp::DeleteBranch {
            actor: ActorSel::ADMIN,
            namespace: target.clone(),
        });
        let index = take_operation_index(&mut next_index)?;
        let resumed_delete = execute_delete_operation(
            &client,
            &server.base_url,
            resumed_delete,
            index,
            run_started,
        )
        .await?;
        // The public handler intentionally returns 202 for both the initial
        // grace wait and a retry that converges root release + cleanup.
        require_status(&resumed_delete, 202)?;
        let resumed_delete_index = resumed_delete.index;
        coverage.record(&resumed_delete.op);
        operations.push(resumed_delete);
        let retained = exact_root_retained(&server.store, &source, &identity).await?;
        model
            .observe_branch_delete_progress(&target, clock.now(), retained)
            .map_err(|violation| {
                branch_oracle_error(
                    "resumed branch delete",
                    resumed_delete_index,
                    &target,
                    violation,
                )
            })?;
        if retained {
            return Err(
                "resumed branch DELETE returned 202 but retained the exact parent root".to_string(),
            );
        }
        model
            .observe_branch_restart_maintenance(&target, true)
            .map_err(|violation| {
                branch_oracle_error(
                    "post-restart branch convergence",
                    resumed_delete_index,
                    &target,
                    violation,
                )
            })?;

        let source_delete_after_release = Op::Branching(BranchingOp::DeleteSourceWithBranches {
            actor: ActorSel::ADMIN,
            source: source.clone(),
        });
        let index = take_operation_index(&mut next_index)?;
        let source_delete_after_release = execute_delete_operation(
            &client,
            &server.base_url,
            source_delete_after_release,
            index,
            run_started,
        )
        .await?;
        require_status(&source_delete_after_release, 202)?;
        model
            .observe_branch_source_delete(
                &target,
                source_delete_after_release.status,
                source_delete_after_release.response["code"].as_str(),
                clock.now(),
            )
            .map_err(|violation| {
                branch_oracle_error(
                    "post-release source delete",
                    source_delete_after_release.index,
                    &target,
                    violation,
                )
            })?;
        coverage.record(&source_delete_after_release.op);
        operations.push(source_delete_after_release);

        let final_model = model
            .branch_delete_bookkeeping(&target)
            .map_err(|violation| {
                branch_oracle_error("branch final-model lookup", next_index, &target, violation)
            })?;
        write_seed_artifacts(env, seed, &setup, &operations, final_model, &coverage).await?;
        let expected_source_conflicts = final_model.expected_source_conflicts();
        server.shutdown().await;
        harness.cleanup_artifact_origin_namespace(&target).await;
        harness.cleanup_artifact_origin_namespace(&source).await;
        harness.cleanup().await;

        Ok(SeedProof {
            operations,
            expected_source_conflicts,
        })
    }

    fn branching_smoke_config() -> Result<Config, String> {
        let mut config = Config::default();
        config.branching.enabled = true;
        config.security.policy_refresh_secs = 3_600;
        config.security.set_cursor_hmac_key_hex("42".repeat(32));
        config.cache.manifest_cache_ttl_ms = 0;
        config.cache.namespace_registry_ttl_ms = 0;
        config.server.request_timeout_secs = 30;
        config.gc.compaction_upload_window_secs = 1;
        config.gc.skew_slop_secs = 0;
        config.gc.horizon_secs = 31;
        config.server.rate_limit_rps = 1_000_000;
        config.server.rate_limit_burst = 1_000_000;
        config.server.write_rate_limit_rps = 1_000_000;
        config.server.write_rate_limit_burst = 1_000_000;
        config
            .validate()
            .map_err(|error| format!("branching smoke config was invalid: {error}"))?;
        if config.gc_horizon_floor_secs() != Some(31) {
            return Err(format!(
                "branching smoke reader-safety floor drifted: {:?}",
                config.gc_horizon_floor_secs()
            ));
        }
        Ok(config)
    }

    fn model_from_identity(identity: &ForkIdentity) -> BranchDeleteBookkeeping {
        BranchDeleteBookkeeping::active(
            identity.source_namespace.to_string(),
            identity.source_incarnation.to_string(),
            identity.target_namespace.to_string(),
            identity.target_incarnation.to_string(),
            identity.branch_id.to_string(),
            identity.source_generation.get(),
            identity.depth,
        )
    }

    fn take_operation_index(next_index: &mut u64) -> Result<u64, String> {
        let index = *next_index;
        *next_index = index
            .checked_add(1)
            .ok_or_else(|| "branching smoke operation index overflowed".to_string())?;
        Ok(index)
    }

    fn branch_oracle_error(
        context: &str,
        op_index: u64,
        target: &str,
        violation: BranchDeleteViolation,
    ) -> String {
        let violation = oracle::branching_delete_violation(op_index, target, violation);
        format!("{context}: {:?}: {}", violation.id, violation.detail)
    }

    async fn execute_delete_operation(
        client: &Client,
        base_url: &str,
        operation: Op,
        index: u64,
        run_started: Instant,
    ) -> Result<BranchingDeleteOpRecord, String> {
        let branching_operation = match &operation {
            Op::Branching(
                operation @ (BranchingOp::DeleteBranch { .. }
                | BranchingOp::DeleteSourceWithBranches { .. }),
            ) => operation,
            other => {
                return Err(format!(
                    "deletion executor received non-deletion branching op: {other:?}"
                ))
            }
        };
        if operation.actor() != ActorSel::ADMIN {
            return Err(format!(
                "deletion smoke has no credential fixture for actor {}",
                operation.actor().0
            ));
        }
        let before = Instant::now();
        let (method, path, status, response) =
            execute_branching_http(client, base_url, branching_operation).await;
        let wall_ms = u64::try_from(run_started.elapsed().as_millis())
            .map_err(|_| "branching smoke wall time did not fit u64 milliseconds".to_string())?;
        let duration_ms = u64::try_from(before.elapsed().as_millis()).map_err(|_| {
            "branching smoke operation duration did not fit u64 milliseconds".to_string()
        })?;
        Ok(BranchingDeleteOpRecord {
            index,
            wall_ms,
            op: operation,
            method,
            path,
            status,
            response,
            outcome: if (200..300).contains(&status) {
                "applied".to_string()
            } else {
                "not_applied".to_string()
            },
            target_node: 0,
            execution: ExecutionMetadata::workload(),
            gen_after: None,
            duration_ms,
            violations: Vec::new(),
        })
    }

    fn require_status(record: &BranchingDeleteOpRecord, expected: u16) -> Result<(), String> {
        if record.status == expected {
            return Ok(());
        }
        Err(format!(
            "{} returned {}, expected {expected}: {}",
            record.op.kind(),
            record.status,
            record.response
        ))
    }

    async fn read_reader_safety_deadline(
        store: &zeppelin::storage::ZeppelinStore,
        target: &str,
    ) -> Result<DateTime<Utc>, String> {
        let key = NamespaceMetadata::s3_key(target);
        let bytes = store
            .get(&key)
            .await
            .map_err(|error| format!("deleting target metadata read failed: {error}"))?;
        let metadata = NamespaceMetadata::from_bytes(&bytes)
            .map_err(|error| format!("deleting target metadata decode failed: {error}"))?;
        metadata
            .deletion_intent
            .and_then(|intent| intent.visibility)
            .map(|visibility| visibility.not_before)
            .ok_or_else(|| "accepted branch delete did not persist reader-safety grace".to_string())
    }

    async fn read_authoritative_lease(
        store: &zeppelin::storage::ZeppelinStore,
        namespace: &str,
    ) -> Result<Lease, String> {
        let key = format!("{namespace}/lease.json");
        let bytes = store
            .get(&key)
            .await
            .map_err(|error| format!("authoritative setup lease read {key} failed: {error}"))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("authoritative setup lease decode {key} failed: {error}"))
    }

    async fn exact_root_retained(
        store: &zeppelin::storage::ZeppelinStore,
        source: &str,
        identity: &ForkIdentity,
    ) -> Result<bool, String> {
        let snapshot = branch_control_snapshot(store, source)
            .await
            .map_err(|error| format!("source branch-root observation failed: {error}"))?;
        Ok(snapshot
            .roots
            .iter()
            .any(|root| identity.matches_root(root)))
    }

    async fn write_seed_artifacts(
        env: &RunnerEnv,
        seed: u64,
        setup: &SetupRecord<'_>,
        operations: &[BranchingDeleteOpRecord],
        model: &BranchDeleteBookkeeping,
        coverage: &Coverage,
    ) -> Result<(), String> {
        let seed_dir = env
            .artifacts
            .join("branching-delete-smoke")
            .join(format!("seed-{seed}"));
        let setup_bytes = serde_json::to_vec_pretty(setup)
            .map_err(|error| format!("failed to encode branching smoke setup: {error}"))?;
        let model_bytes = serde_json::to_vec_pretty(model)
            .map_err(|error| format!("failed to encode branching smoke final model: {error}"))?;
        let coverage_bytes = serde_json::to_vec_pretty(coverage)
            .map_err(|error| format!("failed to encode branching smoke coverage: {error}"))?;
        let mut operation_bytes = Vec::new();
        for record in operations {
            let record = serde_json::to_vec(record)
                .map_err(|error| format!("failed to encode branching op record: {error}"))?;
            operation_bytes.extend_from_slice(&record);
            operation_bytes.push(b'\n');
        }

        tokio::task::spawn_blocking(move || {
            write_seed_artifact_files(
                seed_dir,
                setup_bytes,
                model_bytes,
                coverage_bytes,
                operation_bytes,
            )
        })
        .await
        .map_err(|error| format!("branching smoke artifact writer task failed: {error}"))?
    }

    fn write_seed_artifact_files(
        seed_dir: std::path::PathBuf,
        setup_bytes: Vec<u8>,
        model_bytes: Vec<u8>,
        coverage_bytes: Vec<u8>,
        operation_bytes: Vec<u8>,
    ) -> Result<(), String> {
        fs::create_dir_all(&seed_dir).map_err(|error| {
            format!(
                "failed to create branching smoke artifact dir {}: {error}",
                seed_dir.display()
            )
        })?;
        write_bytes(seed_dir.join("setup.json"), &setup_bytes)?;
        write_bytes(seed_dir.join("final-model.json"), &model_bytes)?;
        write_bytes(seed_dir.join("coverage.json"), &coverage_bytes)?;
        write_bytes(seed_dir.join("ops.jsonl"), &operation_bytes)
    }

    fn write_bytes(path: std::path::PathBuf, bytes: &[u8]) -> Result<(), String> {
        let file = File::create(&path).map_err(|error| {
            format!(
                "failed to create branching smoke artifact {}: {error}",
                path.display()
            )
        })?;
        let mut writer = BufWriter::new(file);
        writer.write_all(bytes).map_err(|error| {
            format!(
                "failed to write branching smoke artifact {}: {error}",
                path.display()
            )
        })?;
        writer.flush().map_err(|error| {
            format!(
                "failed to flush branching smoke artifact {}: {error}",
                path.display()
            )
        })
    }
}

/// Run active-branch deletion traces for every configured pinned seed.
///
/// The setup-only activation adapter is feature gated. Every recorded
/// `DeleteBranch` and `DeleteSourceWithBranches` operation crosses the real
/// authenticated HTTP namespace DELETE seam.
#[cfg(feature = "branching-test-support")]
pub async fn run_branching_delete_smoke(
    env: crate::adversarial::RunnerEnv,
) -> Result<BranchingDeleteSmokeSummary, String> {
    smoke::run(env).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{Duration, TimeZone, Utc};

    use super::{
        BranchDeleteBookkeeping, BranchDeleteViolation, BranchLifecycle, BranchingDeleteOpRecord,
    };
    use crate::adversarial::model::Model;
    use crate::adversarial::ops::{ActorSel, BranchingOp, ExecutionMetadata, Op};
    use crate::adversarial::oracle::{self, ViolationId};

    fn active_branch() -> BranchDeleteBookkeeping {
        BranchDeleteBookkeeping::active(
            "source",
            "source-incarnation",
            "target",
            "target-incarnation",
            "branch-id",
            7,
            1,
        )
    }

    #[test]
    fn core_model_rejects_missing_and_duplicate_branch_targets() {
        let mut model = Model::default();
        assert_eq!(
            model.branch_delete_bookkeeping("target"),
            Err(BranchDeleteViolation::UnknownBranchTarget {
                target_namespace: "target".to_string(),
            })
        );
        model
            .track_branch_delete(active_branch())
            .expect("first exact branch identity must install");
        assert_eq!(
            model.track_branch_delete(active_branch()),
            Err(BranchDeleteViolation::DuplicateBranchTarget {
                target_namespace: "target".to_string(),
            })
        );
    }

    #[test]
    fn source_delete_conflict_during_branch_grace_is_expected() {
        let mut model = active_branch();
        let deadline = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();

        model
            .observe_branch_delete_accepted(deadline, true)
            .expect("accepted branch delete must retain its root");
        model
            .observe_source_delete(
                409,
                Some("namespace_has_live_branches"),
                deadline - Duration::nanoseconds(1),
            )
            .expect("409 before grace/root release is expected bookkeeping");

        assert_eq!(model.lifecycle(), BranchLifecycle::Deleting);
        assert_eq!(model.expected_source_conflicts(), 1);
    }

    #[test]
    fn source_delete_success_before_root_release_is_a_violation() {
        let mut model = active_branch();
        let deadline = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        model
            .observe_branch_delete_accepted(deadline, true)
            .expect("accepted branch delete must retain its root");

        let violation = model
            .observe_source_delete(202, None, deadline - Duration::seconds(1))
            .expect_err("source success with a live root must violate the oracle");
        assert_eq!(
            violation,
            BranchDeleteViolation::SourceDeleteSucceededWithRetainedRoot { status: 202 }
        );
        let finding = oracle::branching_delete_violation(7, "target", violation);
        assert_eq!(finding.id, ViolationId::I30BranchingLifecycle);
    }

    #[test]
    fn elapsed_grace_without_root_release_still_requires_source_conflict() {
        let mut model = active_branch();
        let deadline = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        model
            .observe_branch_delete_accepted(deadline, true)
            .expect("accepted branch delete must retain its root");

        model
            .observe_source_delete(
                409,
                Some("namespace_has_live_branches"),
                deadline + Duration::seconds(30),
            )
            .expect("elapsed grace cannot substitute for exact root release");
        assert_eq!(model.expected_source_conflicts(), 1);
    }

    #[test]
    fn source_delete_succeeds_only_after_deadline_and_root_release() {
        let mut model = active_branch();
        let deadline = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        model
            .observe_branch_delete_accepted(deadline, true)
            .expect("accepted branch delete must retain its root");
        model
            .observe_source_delete(
                409,
                Some("namespace_has_live_branches"),
                deadline - Duration::nanoseconds(1),
            )
            .expect("source conflict must be expected while the root remains");
        model
            .observe_branch_delete_progress(deadline, false)
            .expect("root release at the persisted deadline must converge");
        model
            .observe_source_delete(202, None, deadline)
            .expect("source deletion may succeed after exact root release");

        assert_eq!(model.lifecycle(), BranchLifecycle::Released);
    }

    #[test]
    fn active_branch_without_matching_root_fires_i30() {
        assert_eq!(
            active_branch().require_matching_root(),
            Err(BranchDeleteViolation::ActiveBranchMissingMatchingRoot)
        );
    }

    #[test]
    fn fork_snapshot_stability_oracle_rejects_changed_target_view() {
        let branch = active_branch().with_fork_snapshot(["inherited".to_string()]);
        assert!(matches!(
            branch.observe_namespace_view("target", BTreeSet::new()),
            Err(BranchDeleteViolation::ForkSnapshotChanged { .. })
        ));
    }

    #[test]
    fn divergence_oracle_rejects_target_only_write_in_source() {
        let mut branch = active_branch().with_fork_snapshot(["inherited".to_string()]);
        branch.observe_target_upserts(["target-only".to_string()]);
        assert!(matches!(
            branch.observe_namespace_view(
                "source",
                ["inherited".to_string(), "target-only".to_string()]
                    .into_iter()
                    .collect(),
            ),
            Err(BranchDeleteViolation::SourceTargetWritesCrossed { .. })
        ));
    }

    #[test]
    fn foreign_delete_oracle_rejects_source_owned_key() {
        let branch = active_branch();
        assert!(matches!(
            branch.observe_target_delete_key("source/segments/foreign/cluster_0.bin"),
            Err(BranchDeleteViolation::ForeignArtifactDelete { .. })
        ));
        branch
            .observe_target_delete_key("target/segments/local/cluster_0.bin")
            .expect("target-owned cleanup must remain valid");
    }

    #[test]
    fn visibility_must_precede_root_release_and_materialization_keeps_root() {
        let deadline = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let mut branch = active_branch();
        branch.lifecycle = BranchLifecycle::Deleting;
        branch.reader_safety_not_before = Some(deadline);
        assert_eq!(
            branch.observe_branch_delete_progress(deadline, false),
            Err(BranchDeleteViolation::RootReleasedBeforeVisibilityRemoval)
        );

        let mut materializing = active_branch();
        assert_eq!(
            materializing.observe_materialized(false),
            Err(BranchDeleteViolation::MaterializationReleasedRoot)
        );
    }

    #[test]
    fn restart_and_no_merge_oracles_fail_closed() {
        let mut branch = active_branch();
        branch.begin_branch_delete_without_deadline();
        branch.observe_restart();
        assert_eq!(
            branch.observe_restart_maintenance(false),
            Err(BranchDeleteViolation::RestartMaintenanceDidNotConverge)
        );
        assert!(matches!(
            BranchDeleteBookkeeping::observe_generated_operation_kind("merge_namespace"),
            Err(BranchDeleteViolation::MergeOperationGenerated { .. })
        ));
    }

    #[test]
    fn operation_record_round_trips_the_live_branching_vocabulary() {
        let record = BranchingDeleteOpRecord {
            index: 3,
            wall_ms: 9,
            op: Op::Branching(BranchingOp::DeleteBranch {
                actor: ActorSel::ADMIN,
                namespace: "target".to_string(),
            }),
            method: "DELETE".to_string(),
            path: "/v1/namespaces/target".to_string(),
            status: 202,
            response: serde_json::json!({"state": "deleting"}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata::workload(),
            gen_after: None,
            duration_ms: 4,
            violations: Vec::new(),
        };

        let encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(encoded["op"]["Branching"]["kind"], "delete_branch");
        assert_eq!(encoded["op"]["Branching"]["namespace"], "target");
        let decoded: BranchingDeleteOpRecord = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.op.kind(), record.op.kind());
        assert_eq!(decoded.op.namespace(), record.op.namespace());

        let replay_dir = tempfile::tempdir().unwrap();
        let mut line = serde_json::to_vec(&record).unwrap();
        line.push(b'\n');
        std::fs::write(replay_dir.path().join("ops.jsonl"), line).unwrap();
        let replayed = crate::adversarial::artifacts::read_ops(replay_dir.path());
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].op.kind(), "delete_branch");
        assert_eq!(replayed[0].op.namespace(), "target");
    }
}
