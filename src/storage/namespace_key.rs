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
    /// The authoritative namespace lifecycle record at `meta.json`.
    Metadata,
    /// The live visibility boundary at `manifest.json`.
    Manifest,
    /// The single-writer lease at `lease.json`.
    Lease,
    /// Immutable manifest history beneath `manifests/`.
    ManifestHistory,
    /// Named snapshot pins beneath `snapshots/`.
    Snapshot,
    /// Immutable WAL fragments beneath `wal/`.
    Wal,
    /// Immutable index artifacts beneath `segments/`.
    Segment,
    /// Lease-scoped compaction roots beneath `_staging/`.
    Staging,
    /// The persisted GC candidate ledger beneath `_gc/`.
    Gc,
    /// Durable branch-visibility removal markers beneath `_lifecycle/`.
    BranchVisibilityRemoved,
}

impl NamespaceObjectFamily {
    /// Whether a manifest may name this family in `pending_deletes`.
    ///
    /// Deferred deletion is restricted to immutable data artifacts. Control,
    /// history, snapshot, staging, GC, and lifecycle objects have separate
    /// ownership protocols and must never enter the artifact drain.
    #[must_use]
    pub(crate) const fn allows_deferred_delete(self) -> bool {
        matches!(self, Self::Wal | Self::Segment)
    }

    /// Whether this is the exact metadata tombstone retained during cleanup.
    #[must_use]
    pub(crate) const fn is_metadata(self) -> bool {
        matches!(self, Self::Metadata)
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

        let family = match suffix {
            "meta.json" => NamespaceObjectFamily::Metadata,
            "manifest.json" => NamespaceObjectFamily::Manifest,
            "lease.json" => NamespaceObjectFamily::Lease,
            _ => classify_nested_family(&key, suffix)?,
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
    if let Some(file_name) = suffix.strip_prefix("manifests/") {
        validate_manifest_history_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::ManifestHistory);
    }
    if let Some(file_name) = suffix.strip_prefix("snapshots/") {
        validate_snapshot_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::Snapshot);
    }
    for (prefix, family) in [
        ("wal/", NamespaceObjectFamily::Wal),
        ("segments/", NamespaceObjectFamily::Segment),
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

    if let Some(file_name) = suffix.strip_prefix("_staging/") {
        validate_staging_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::Staging);
    }
    if suffix == "_gc/candidates.json" {
        return Ok(NamespaceObjectFamily::Gc);
    }
    if suffix.starts_with("_gc/") {
        return Err(malformed_namespace_family_key(
            "gc",
            key,
            "unrecognized reserved GC key",
        ));
    }
    if let Some(file_name) = suffix.strip_prefix("_lifecycle/branch_visibility_removed/") {
        validate_visibility_marker_file(key, file_name)?;
        return Ok(NamespaceObjectFamily::BranchVisibilityRemoved);
    }
    if suffix.starts_with("_lifecycle/") {
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
    use super::{NamespaceObjectFamily, NamespaceObjectKey};

    const NS: &str = "target";

    #[test]
    fn classifier_covers_every_production_namespace_family() {
        let keys = [
            ("target/meta.json", NamespaceObjectFamily::Metadata),
            ("target/manifest.json", NamespaceObjectFamily::Manifest),
            ("target/lease.json", NamespaceObjectFamily::Lease),
            (
                "target/manifests/00000000000000000001.msgpack",
                NamespaceObjectFamily::ManifestHistory,
            ),
            ("target/snapshots/daily.msgpack", NamespaceObjectFamily::Snapshot),
            ("target/wal/fragment.wal", NamespaceObjectFamily::Wal),
            (
                "target/segments/segment/centroids.bin",
                NamespaceObjectFamily::Segment,
            ),
            ("target/_staging/17.json", NamespaceObjectFamily::Staging),
            ("target/_gc/candidates.json", NamespaceObjectFamily::Gc),
            (
                "target/_lifecycle/branch_visibility_removed/01ARZ3NDEKTSV4RRFFQ69G5FAV.1234567890abcdef1234567890abcdef.json",
                NamespaceObjectFamily::BranchVisibilityRemoved,
            ),
        ];

        for (key, expected) in keys {
            let classified = NamespaceObjectKey::classify(NS, key)
                .unwrap_or_else(|error| panic!("{key} should classify: {error}"));
            assert_eq!(classified.namespace(), NS);
            assert_eq!(classified.family(), expected);
            assert_eq!(classified.into_key(), key);
        }
    }

    #[test]
    fn only_immutable_data_families_allow_deferred_delete() {
        let wal = NamespaceObjectKey::classify(NS, "target/wal/f.wal")
            .unwrap_or_else(|error| panic!("WAL key should classify: {error}"));
        let segment = NamespaceObjectKey::classify(NS, "target/segments/s/file.bin")
            .unwrap_or_else(|error| panic!("segment key should classify: {error}"));
        let lifecycle = NamespaceObjectKey::classify(
            NS,
            "target/_lifecycle/branch_visibility_removed/01ARZ3NDEKTSV4RRFFQ69G5FAV.1234567890abcdef1234567890abcdef.json",
        )
        .unwrap_or_else(|error| panic!("lifecycle key should classify: {error}"));

        assert!(wal.allows_deferred_delete());
        assert!(segment.allows_deferred_delete());
        assert!(!lifecycle.allows_deferred_delete());
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
