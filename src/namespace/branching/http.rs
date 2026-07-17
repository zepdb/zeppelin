//! Stable, redacted HTTP contract vocabulary for namespace branches.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Strict create-fork request; live-head only in v1.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// Target namespace name and idempotency key.
    pub target: String,
}

/// Stable v1 branch mode.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchMode {
    CopyOnWrite,
}

/// Public branch lifecycle.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycle {
    Preparing,
    Active,
    Deleting,
}

/// Public branch health.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchHealth {
    AwaitingAuthenticatedRetry,
    Ready,
    DeletionInProgress,
}

/// Redacted target identity.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchTargetIdentity {
    pub namespace: String,
    pub incarnation: String,
}

/// Public direct-child descriptor.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchDescriptorResponse {
    pub branch_id: String,
    pub target: BranchTargetIdentity,
    pub mode: BranchMode,
    pub depth: u16,
    pub lifecycle: BranchLifecycle,
    pub health: BranchHealth,
    pub materialized: bool,
    pub created_at: DateTime<Utc>,
}

/// Direct-child list response without count or pagination oracle fields.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchListResponse {
    pub branches: Vec<BranchDescriptorResponse>,
}
