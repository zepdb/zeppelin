//! Strong physical-artifact origin identities.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::BranchError;
use crate::namespace::{NamespaceId, NamespaceIncarnationId};

/// Physical namespace lifetime that owns one immutable artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactOrigin {
    /// Validated namespace prefix containing the immutable object.
    pub namespace: NamespaceId,
    /// Exact lifetime of that namespace name.
    pub incarnation: NamespaceIncarnationId,
}

/// Compact index into a manifest's canonical physical-origin table.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ArtifactOriginIndex(u32);

impl ArtifactOriginIndex {
    /// Wrap one already range-checked table index.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Return the persisted numeric index.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// First pass of deterministic origin-table construction.
#[derive(Debug, Default)]
#[allow(dead_code)] // Frozen in phase 02; fork normalization adopts it in phase 05.
pub(crate) struct ArtifactOriginSetBuilder {
    origins: BTreeSet<ArtifactOrigin>,
}

#[allow(dead_code, clippy::result_large_err)]
// The builder reports the complete typed integrity context required by Phase 02.
impl ArtifactOriginSetBuilder {
    /// Collect one ultimate physical owner without assigning an unstable index.
    pub(crate) fn collect(&mut self, origin: ArtifactOrigin) -> Result<(), BranchError> {
        if origin.incarnation.is_nil() {
            return Err(BranchError::ArtifactOriginInvalid {
                manifest_namespace: origin.namespace.as_str().to_string(),
                manifest_incarnation: Some(origin.incarnation.clone()),
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: Some(origin),
                reason: "namespace incarnation is nil".to_string(),
            });
        }
        self.origins.insert(origin);
        Ok(())
    }

    /// Freeze sorted origins and assign their final stable indices.
    pub(crate) fn finish(self) -> Result<CanonicalArtifactOrigins, BranchError> {
        if self.origins.len() > u32::MAX as usize {
            return Err(BranchError::ArtifactOriginInvalid {
                manifest_namespace: "<unbound>".to_string(),
                manifest_incarnation: None,
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: None,
                reason: "origin table exceeds u32 address space".to_string(),
            });
        }

        let table: Vec<_> = self.origins.into_iter().collect();
        let mut indices = BTreeMap::new();
        for (index, origin) in table.iter().cloned().enumerate() {
            let index = u32::try_from(index).map_err(|_| BranchError::ArtifactOriginInvalid {
                manifest_namespace: "<unbound>".to_string(),
                manifest_incarnation: None,
                descriptor_kind: "manifest",
                descriptor_id: "artifact_origins".to_string(),
                offending_index: None,
                offending_key: None,
                expected_origin: Some(origin.clone()),
                reason: "origin table exceeds u32 address space".to_string(),
            })?;
            indices.insert(origin, ArtifactOriginIndex::new(index));
        }
        Ok(CanonicalArtifactOrigins { table, indices })
    }
}

/// Frozen deterministic table plus its origin-to-final-index map.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // Frozen in phase 02; fork normalization adopts it in phase 05.
pub(crate) struct CanonicalArtifactOrigins {
    pub(crate) table: Vec<ArtifactOrigin>,
    pub(crate) indices: BTreeMap<ArtifactOrigin, ArtifactOriginIndex>,
}
