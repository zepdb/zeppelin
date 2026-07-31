//! Typed ownership and family classification for namespace-scoped object keys.
//!
//! Namespace deletion and garbage collection must never infer ownership from a
//! loose string prefix.  This module accepts only the exact `<namespace>/`
//! prefix and the finite set of object families written by production code.
//! Unknown keys therefore stop destructive work instead of becoming deletion
//! candidates by default.

use crate::error::{Result, ZeppelinError};

/// Production object families allowed directly beneath one namespace prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceObjectFamily {
    /// `meta.json`: control-protocol state, never deferred, expanded, or branch-local.
    Metadata,
    /// `manifest.json`: control-protocol state, never deferred, expanded, or branch-local.
    Manifest,
    /// `lease.json`: lease-protocol state, never deferred, expanded, or branch-local.
    Lease,
    /// `manifests/`: retention-protocol state, never deferred, expanded, or branch-local.
    ManifestHistory,
    /// `snapshots/`: snapshot-protocol state, never deferred, expanded, or branch-local.
    Snapshot,
    /// `wal/`: manifest-referenced immutable data, deferred and branch-local.
    Wal,
    /// `input-wal/`: typed-input immutable data, deferred and branch-local.
    InputWal,
    /// `sources/`: checksum-addressed source data, deferred and branch-local.
    Source,
    /// `segments/`: manifest-referenced immutable data, deferred and branch-local.
    Segment,
    /// `late/state/`: content-addressed manifest late-state sections.
    LateSection,
    /// `late/matrix-fragments/`: immutable exact-scoring matrix fragments.
    MatrixFragment,
    /// `late/fde-fragments/`: immutable fixed-dimensional candidate fragments.
    FdeFragment,
    /// `late/transforms/`: immutable materialized FDE transforms.
    FdeTransform,
    /// `late/centering/`: immutable frozen centering means.
    Centering,
    /// `late/quarantine/`: immutable deterministic-failure evidence.
    Quarantine,
    /// `late/segments/`: immutable candidate, truth, and filter segment artifacts.
    LateSegment,
    /// `_staging/`: fenced staging roots, never deferred or branch-local.
    Staging,
    /// `_gc/`: GC-protocol state, never deferred, expanded, or branch-local.
    Gc,
    /// `_lifecycle/`: lifecycle-protocol state, never deferred, expanded, or branch-local.
    BranchVisibilityRemoved,
}

/// Which lifecycle owns reachability for one namespace object family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GcOwnership {
    /// A manifest or retained snapshot decides whether the immutable object is live.
    ManifestReferenced,
    /// A dedicated control-plane protocol owns creation and removal.
    ControlProtocol,
    /// The active fencing token temporarily roots in-flight compaction uploads.
    StagingProtocol,
}

impl NamespaceObjectFamily {
    /// Every production namespace object family, in stable registry order.
    pub(crate) const ALL: [Self; 19] = [
        Self::Metadata,
        Self::Manifest,
        Self::Lease,
        Self::ManifestHistory,
        Self::Snapshot,
        Self::Wal,
        Self::InputWal,
        Self::Source,
        Self::Segment,
        Self::LateSection,
        Self::MatrixFragment,
        Self::FdeFragment,
        Self::FdeTransform,
        Self::Centering,
        Self::Quarantine,
        Self::LateSegment,
        Self::Staging,
        Self::Gc,
        Self::BranchVisibilityRemoved,
    ];

    /// Whether a manifest may name this family in `pending_deletes`.
    ///
    /// Deferred deletion is restricted to immutable data artifacts. Control,
    /// history, snapshot, staging, GC, and lifecycle objects have separate
    /// ownership protocols and must never enter the artifact drain.
    #[must_use]
    pub(crate) const fn allows_deferred_delete(self) -> bool {
        match self {
            Self::Wal
            | Self::InputWal
            | Self::Source
            | Self::Segment
            | Self::LateSection
            | Self::MatrixFragment
            | Self::FdeFragment
            | Self::FdeTransform
            | Self::Centering
            | Self::Quarantine => true,
            Self::LateSegment => true,
            Self::Metadata
            | Self::Manifest
            | Self::Lease
            | Self::ManifestHistory
            | Self::Snapshot
            | Self::Staging
            | Self::Gc
            | Self::BranchVisibilityRemoved => false,
        }
    }

    /// Return the lifecycle that owns reachability and removal for this family.
    #[must_use]
    pub(crate) const fn gc_ownership(self) -> GcOwnership {
        match self {
            Self::Wal
            | Self::InputWal
            | Self::Source
            | Self::Segment
            | Self::LateSection
            | Self::MatrixFragment
            | Self::FdeFragment
            | Self::FdeTransform
            | Self::Centering
            | Self::Quarantine => GcOwnership::ManifestReferenced,
            Self::LateSegment => GcOwnership::ManifestReferenced,
            Self::Staging => GcOwnership::StagingProtocol,
            Self::Metadata
            | Self::Manifest
            | Self::Lease
            | Self::ManifestHistory
            | Self::Snapshot
            | Self::Gc
            | Self::BranchVisibilityRemoved => GcOwnership::ControlProtocol,
        }
    }

    /// Whether physical origins for this family participate in branch locality.
    #[must_use]
    pub(crate) const fn participates_in_branch_locality(self) -> bool {
        match self {
            Self::Wal
            | Self::InputWal
            | Self::Source
            | Self::Segment
            | Self::LateSection
            | Self::MatrixFragment
            | Self::FdeFragment
            | Self::FdeTransform
            | Self::Centering
            | Self::Quarantine => true,
            Self::LateSegment => true,
            Self::Metadata
            | Self::Manifest
            | Self::Lease
            | Self::ManifestHistory
            | Self::Snapshot
            | Self::Staging
            | Self::Gc
            | Self::BranchVisibilityRemoved => false,
        }
    }

    /// Return the namespace-relative exact key or descendant prefix.
    #[must_use]
    pub(crate) const fn relative_prefix(self) -> &'static str {
        match self {
            Self::Metadata => "meta.json",
            Self::Manifest => "manifest.json",
            Self::Lease => "lease.json",
            Self::ManifestHistory => "manifests/",
            Self::Snapshot => "snapshots/",
            Self::Wal => "wal/",
            Self::InputWal => "input-wal/",
            Self::Source => "sources/",
            Self::Segment => "segments/",
            Self::LateSection => "late/state/",
            Self::MatrixFragment => "late/matrix-fragments/",
            Self::FdeFragment => "late/fde-fragments/",
            Self::FdeTransform => "late/transforms/",
            Self::Centering => "late/centering/",
            Self::Quarantine => "late/quarantine/",
            Self::LateSegment => "late/segments/",
            Self::Staging => "_staging/",
            Self::Gc => "_gc/candidates.json",
            Self::BranchVisibilityRemoved => "_lifecycle/branch_visibility_removed/",
        }
    }

    /// Return the namespace-relative top-level family root.
    #[must_use]
    pub(crate) const fn relative_root_prefix(self) -> &'static str {
        match self {
            Self::Metadata => "meta.json",
            Self::Manifest => "manifest.json",
            Self::Lease => "lease.json",
            Self::ManifestHistory => "manifests/",
            Self::Snapshot => "snapshots/",
            Self::Wal => "wal/",
            Self::InputWal => "input-wal/",
            Self::Source => "sources/",
            Self::Segment => "segments/",
            Self::LateSection
            | Self::MatrixFragment
            | Self::FdeFragment
            | Self::FdeTransform
            | Self::Centering
            | Self::Quarantine => "late/",
            Self::LateSegment => "late/",
            Self::Staging => "_staging/",
            Self::Gc => "_gc/",
            Self::BranchVisibilityRemoved => "_lifecycle/",
        }
    }

    /// Build this family's exact namespace prefix or singleton key.
    #[must_use]
    pub(crate) fn namespace_prefix(self, namespace: &str) -> String {
        format!("{namespace}/{}", self.relative_prefix())
    }

    /// Whether this is the exact metadata tombstone retained during cleanup.
    #[must_use]
    pub(crate) const fn is_metadata(self) -> bool {
        match self {
            Self::Metadata => true,
            Self::Manifest
            | Self::Lease
            | Self::ManifestHistory
            | Self::Snapshot
            | Self::Wal
            | Self::InputWal
            | Self::Source
            | Self::Segment
            | Self::LateSection
            | Self::MatrixFragment
            | Self::FdeFragment
            | Self::FdeTransform
            | Self::Centering
            | Self::Quarantine
            | Self::LateSegment
            | Self::Staging
            | Self::Gc
            | Self::BranchVisibilityRemoved => false,
        }
    }
}

/// One object key proven to belong to an exact namespace and known family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceObjectKey {
    namespace: String,
    key: String,
    family: NamespaceObjectFamily,
}

impl NamespaceObjectKey {
    /// Classify one full key against an exact namespace owner.
    ///
    /// Keys outside `<namespace>/`, unknown top-level families, empty family
    /// descendants, and non-canonical reserved control keys are rejected as
    /// corruption. A successful result is safe to carry to an ownership-bound
    /// deletion primitive; reachability and lifecycle policy remain separate
    /// caller responsibilities.
    pub(crate) fn classify(namespace: &str, key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        let prefix = namespace_prefix(namespace)?;
        let suffix = key.strip_prefix(&prefix).ok_or_else(|| {
            malformed_namespace_key(
                key.clone(),
                format!("key is outside exact namespace prefix {prefix}"),
            )
        })?;

        let family = if suffix == NamespaceObjectFamily::Metadata.relative_prefix() {
            NamespaceObjectFamily::Metadata
        } else if suffix == NamespaceObjectFamily::Manifest.relative_prefix() {
            NamespaceObjectFamily::Manifest
        } else if suffix == NamespaceObjectFamily::Lease.relative_prefix() {
            NamespaceObjectFamily::Lease
        } else {
            classify_nested_family(&key, suffix)?
        };

        Ok(Self {
            namespace: namespace.to_string(),
            key,
            family,
        })
    }

    /// Return the exact namespace owner against which this key was classified.
    #[must_use]
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Return the recognized production family.
    #[must_use]
    pub(crate) const fn family(&self) -> NamespaceObjectFamily {
        self.family
    }

    /// Consume the proof wrapper and return its full object-store key.
    #[must_use]
    pub(crate) fn into_key(self) -> String {
        self.key
    }

    /// Whether this key may be carried by manifest `pending_deletes`.
    #[must_use]
    pub(crate) const fn allows_deferred_delete(&self) -> bool {
        self.family.allows_deferred_delete()
    }
}

/// Build the exact recursive-list prefix for one top-level namespace.
pub(crate) fn namespace_prefix(namespace: &str) -> Result<String> {
    if namespace.is_empty() || namespace.contains('/') {
        return Err(ZeppelinError::Validation(format!(
            "namespace object ownership requires one non-empty top-level segment: {namespace:?}"
        )));
    }
    Ok(format!("{namespace}/"))
}

fn classify_nested_family(key: &str, suffix: &str) -> Result<NamespaceObjectFamily> {
    if let Some(file_name) =
        suffix.strip_prefix(NamespaceObjectFamily::ManifestHistory.relative_prefix())
    {
        validate_manifest_history_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::ManifestHistory);
    }
    if let Some(file_name) = suffix.strip_prefix(NamespaceObjectFamily::Snapshot.relative_prefix())
    {
        validate_snapshot_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::Snapshot);
    }
    for (prefix, family) in [
        (
            NamespaceObjectFamily::Wal.relative_prefix(),
            NamespaceObjectFamily::Wal,
        ),
        (
            NamespaceObjectFamily::InputWal.relative_prefix(),
            NamespaceObjectFamily::InputWal,
        ),
        (
            NamespaceObjectFamily::Source.relative_prefix(),
            NamespaceObjectFamily::Source,
        ),
        (
            NamespaceObjectFamily::Segment.relative_prefix(),
            NamespaceObjectFamily::Segment,
        ),
        (
            NamespaceObjectFamily::LateSection.relative_prefix(),
            NamespaceObjectFamily::LateSection,
        ),
        (
            NamespaceObjectFamily::MatrixFragment.relative_prefix(),
            NamespaceObjectFamily::MatrixFragment,
        ),
        (
            NamespaceObjectFamily::FdeFragment.relative_prefix(),
            NamespaceObjectFamily::FdeFragment,
        ),
        (
            NamespaceObjectFamily::FdeTransform.relative_prefix(),
            NamespaceObjectFamily::FdeTransform,
        ),
        (
            NamespaceObjectFamily::Centering.relative_prefix(),
            NamespaceObjectFamily::Centering,
        ),
        (
            NamespaceObjectFamily::Quarantine.relative_prefix(),
            NamespaceObjectFamily::Quarantine,
        ),
        (
            NamespaceObjectFamily::LateSegment.relative_prefix(),
            NamespaceObjectFamily::LateSegment,
        ),
    ] {
        if let Some(descendant) = suffix.strip_prefix(prefix) {
            if descendant.is_empty() {
                return Err(malformed_namespace_key(
                    key,
                    format!("{prefix} requires a non-empty descendant key"),
                ));
            }
            return Ok(family);
        }
    }

    if let Some(file_name) = suffix.strip_prefix(NamespaceObjectFamily::Staging.relative_prefix()) {
        validate_staging_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::Staging);
    }
    if suffix == NamespaceObjectFamily::Gc.relative_prefix() {
        return Ok(NamespaceObjectFamily::Gc);
    }
    if suffix.starts_with(NamespaceObjectFamily::Gc.relative_root_prefix()) {
        return Err(malformed_namespace_family_key(
            "gc",
            key,
            "unrecognized reserved GC key",
        ));
    }
    if let Some(file_name) =
        suffix.strip_prefix(NamespaceObjectFamily::BranchVisibilityRemoved.relative_prefix())
    {
        validate_visibility_marker_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::BranchVisibilityRemoved);
    }
    if suffix.starts_with(NamespaceObjectFamily::BranchVisibilityRemoved.relative_root_prefix()) {
        return Err(malformed_namespace_family_key(
            "branch-visibility-removal",
            key,
            "unrecognized reserved lifecycle key",
        ));
    }

    Err(malformed_namespace_key(
        key,
        "key does not belong to a recognized namespace object family",
    ))
}

fn validate_manifest_history_file(key: &str, file_name: &str) -> Result<()> {
    let generation = file_name.strip_suffix(".msgpack").filter(|generation| {
        generation.len() == 20
            && generation.bytes().all(|byte| byte.is_ascii_digit())
            && generation.parse::<u64>().is_ok()
    });
    if generation.is_none() {
        return Err(malformed_namespace_family_key(
            "manifest-history",
            key,
            "manifest history key must be canonical <20-digit-generation>.msgpack",
        ));
    }
    Ok(())
}

fn validate_snapshot_file(key: &str, file_name: &str) -> Result<()> {
    let name = file_name.strip_suffix(".msgpack").filter(|name| {
        !name.is_empty()
            && name.len() <= 255
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    });
    if name.is_none() {
        return Err(malformed_namespace_family_key(
            "snapshot",
            key,
            "snapshot key must be canonical <safe-name>.msgpack",
        ));
    }
    Ok(())
}

fn validate_staging_file(key: &str, file_name: &str) -> Result<()> {
    let token = file_name
        .strip_suffix(".json")
        .filter(|token| !token.is_empty() && !token.contains('/'))
        .and_then(|token| token.parse::<u64>().ok())
        .filter(|token| format!("{token}.json") == file_name);
    if token.is_none() {
        return Err(malformed_namespace_family_key(
            "staging",
            key,
            "_staging key must be canonical <decimal-fencing-token>.json",
        ));
    }
    Ok(())
}

fn validate_visibility_marker_file(key: &str, file_name: &str) -> Result<()> {
    let Some(stem) = file_name.strip_suffix(".json") else {
        return Err(malformed_namespace_family_key(
            "branch-visibility-removal",
            key,
            "branch visibility marker must end with .json",
        ));
    };
    let Some((branch_id, incarnation)) = stem.split_once('.') else {
        return Err(malformed_namespace_family_key(
            "branch-visibility-removal",
            key,
            "branch visibility marker must contain branch and incarnation identities",
        ));
    };
    let branch_is_canonical =
        ulid::Ulid::from_string(branch_id).is_ok_and(|parsed| parsed.to_string() == branch_id);
    let incarnation_is_canonical = uuid::Uuid::parse_str(incarnation)
        .is_ok_and(|parsed| !parsed.is_nil() && parsed.simple().to_string() == incarnation);
    if !branch_is_canonical || !incarnation_is_canonical {
        return Err(malformed_namespace_family_key(
            "branch-visibility-removal",
            key,
            "branch visibility marker identities are not canonical",
        ));
    }
    Ok(())
}

fn malformed_namespace_key(key: impl Into<String>, reason: impl Into<String>) -> ZeppelinError {
    malformed_namespace_family_key("namespace-object", key, reason)
}

fn malformed_namespace_family_key(
    family: &'static str,
    key: impl Into<String>,
    reason: impl Into<String>,
) -> ZeppelinError {
    ZeppelinError::MalformedControlKey {
        family,
        key: key.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{GcOwnership, NamespaceObjectFamily, NamespaceObjectKey};

    const NS: &str = "target";

    #[test]
    fn classifier_covers_every_production_namespace_family() {
        let mut conformance_rows =
            include_str!("../../tests/fixtures/mmli2/phase3_family_conformance.tsv").lines();

        for family in NamespaceObjectFamily::ALL {
            let descendant = match family {
                NamespaceObjectFamily::Metadata
                | NamespaceObjectFamily::Manifest
                | NamespaceObjectFamily::Lease
                | NamespaceObjectFamily::Gc => "",
                NamespaceObjectFamily::ManifestHistory => "00000000000000000001.msgpack",
                NamespaceObjectFamily::Snapshot => "daily.msgpack",
                NamespaceObjectFamily::Wal => "fragment.wal",
                NamespaceObjectFamily::InputWal => "fragment.wal",
                NamespaceObjectFamily::Source => {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
                NamespaceObjectFamily::Segment => "segment/centroids.bin",
                NamespaceObjectFamily::LateSection => {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
                NamespaceObjectFamily::MatrixFragment
                | NamespaceObjectFamily::FdeFragment
                | NamespaceObjectFamily::FdeTransform
                | NamespaceObjectFamily::Centering
                | NamespaceObjectFamily::Quarantine => {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                }
                NamespaceObjectFamily::LateSegment => "segment/matrix_0.bin",
                NamespaceObjectFamily::Staging => "17.json",
                NamespaceObjectFamily::BranchVisibilityRemoved => {
                    "01ARZ3NDEKTSV4RRFFQ69G5FAV.1234567890abcdef1234567890abcdef.json"
                }
            };
            let key = format!("{}{descendant}", family.namespace_prefix(NS));
            let row = conformance_rows
                .next()
                .unwrap_or_else(|| panic!("missing counting conformance row for {family:?}"));
            let mut fields = row.split('\t');
            assert_eq!(fields.next(), Some(format!("{family:?}").as_str()));
            assert_eq!(fields.next(), Some(key.as_str()));
            assert!(
                fields.next().is_some_and(|class| !class.is_empty()),
                "{family:?} must declare a counting class"
            );
            assert_eq!(fields.next(), None, "{family:?} row has extra fields");

            let classified = NamespaceObjectKey::classify(NS, key.clone())
                .unwrap_or_else(|error| panic!("{key} should classify: {error}"));
            assert_eq!(classified.namespace(), NS);
            assert_eq!(classified.family(), family);
            assert_eq!(classified.into_key(), key);
        }

        assert_eq!(
            conformance_rows.next(),
            None,
            "counting conformance fixture has a family absent from ALL"
        );
    }

    #[test]
    fn only_immutable_data_families_allow_deferred_delete() {
        let wal = NamespaceObjectKey::classify(NS, "target/wal/f.wal")
            .unwrap_or_else(|error| panic!("WAL key should classify: {error}"));
        let segment = NamespaceObjectKey::classify(NS, "target/segments/s/file.bin")
            .unwrap_or_else(|error| panic!("segment key should classify: {error}"));
        let late = NamespaceObjectKey::classify(NS, "target/late/state/0123456789abcdef")
            .unwrap_or_else(|error| panic!("late section key should classify: {error}"));
        let input = NamespaceObjectKey::classify(NS, "target/input-wal/f.wal")
            .unwrap_or_else(|error| panic!("input WAL key should classify: {error}"));
        let source = NamespaceObjectKey::classify(NS, "target/sources/0123456789abcdef")
            .unwrap_or_else(|error| panic!("source key should classify: {error}"));
        let transform = NamespaceObjectKey::classify(NS, "target/late/transforms/0123456789abcdef")
            .unwrap_or_else(|error| panic!("FDE transform key should classify: {error}"));
        let lifecycle = NamespaceObjectKey::classify(
            NS,
            "target/_lifecycle/branch_visibility_removed/01ARZ3NDEKTSV4RRFFQ69G5FAV.1234567890abcdef1234567890abcdef.json",
        )
        .unwrap_or_else(|error| panic!("lifecycle key should classify: {error}"));

        assert!(wal.allows_deferred_delete());
        assert!(segment.allows_deferred_delete());
        assert!(late.allows_deferred_delete());
        assert!(input.allows_deferred_delete());
        assert!(source.allows_deferred_delete());
        assert!(transform.allows_deferred_delete());
        assert!(!lifecycle.allows_deferred_delete());
    }

    #[test]
    fn every_family_declares_gc_and_branch_locality_semantics() {
        for family in NamespaceObjectFamily::ALL {
            match family {
                NamespaceObjectFamily::Wal
                | NamespaceObjectFamily::InputWal
                | NamespaceObjectFamily::Source
                | NamespaceObjectFamily::Segment
                | NamespaceObjectFamily::LateSection
                | NamespaceObjectFamily::MatrixFragment
                | NamespaceObjectFamily::FdeFragment
                | NamespaceObjectFamily::FdeTransform
                | NamespaceObjectFamily::Centering
                | NamespaceObjectFamily::Quarantine
                | NamespaceObjectFamily::LateSegment => {
                    assert_eq!(family.gc_ownership(), GcOwnership::ManifestReferenced);
                    assert!(family.participates_in_branch_locality());
                }
                NamespaceObjectFamily::Staging => {
                    assert_eq!(family.gc_ownership(), GcOwnership::StagingProtocol);
                    assert!(!family.participates_in_branch_locality());
                }
                NamespaceObjectFamily::Metadata
                | NamespaceObjectFamily::Manifest
                | NamespaceObjectFamily::Lease
                | NamespaceObjectFamily::ManifestHistory
                | NamespaceObjectFamily::Snapshot
                | NamespaceObjectFamily::Gc
                | NamespaceObjectFamily::BranchVisibilityRemoved => {
                    assert_eq!(family.gc_ownership(), GcOwnership::ControlProtocol);
                    assert!(!family.participates_in_branch_locality());
                }
            }
        }
    }

    #[test]
    fn classifier_rejects_foreign_unknown_and_malformed_reserved_keys() {
        for key in [
            "source/wal/f.wal",
            "target/unknown.bin",
            "target/_gc/other.json",
            "target/_staging/017.json",
            "target/manifests/latest.msgpack",
            "target/manifests/99999999999999999999.msgpack",
            "target/snapshots/nested/name.msgpack",
            "target/_lifecycle/other/event.json",
            "target/_lifecycle/branch_visibility_removed/not-canonical.json",
            "target/_lifecycle/branch_visibility_removed/01ARZ3NDEKTSV4RRFFQ69G5FAV.00000000000000000000000000000000.json",
        ] {
            assert!(
                NamespaceObjectKey::classify(NS, key).is_err(),
                "{key} must fail closed"
            );
        }
    }
}
