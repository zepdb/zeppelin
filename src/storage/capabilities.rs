//! Static per-substrate capability matrix for the storage seam.
//!
//! Zeppelin never asks "is this S3?" outside [`crate::storage::ZeppelinStore`]
//! construction — it asks what the configured substrate can do. The matrix
//! here is declared from verified `object_store` 0.11.2 behavior; the tracked
//! [multi-substrate storage evidence](https://github.com/zepdb/zeppelin/blob/main/docs/evidence/multi-substrate-storage.md#declared-capability-matrix)
//! records the matrix and validation scope. When `storage.fail_fast` is set,
//! the declaration is verified live at boot by
//! [`crate::storage::ZeppelinStore::verify_declared_capabilities`], so a
//! mis-declared or mis-deployed backend refuses to boot instead of silently
//! degrading a compare-and-swap into an overwrite.

use crate::config::StorageBackend;

/// Which token kind authorizes a conditional PUT on a substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasTokenKind {
    /// HTTP `If-Match` on the entity tag (S3, MinIO, Azure, InMemory).
    ETag,
    /// Substrate-native version identifier — the GCS object generation via
    /// `x-goog-if-generation-match`; GCS ignores ETags for conditional puts.
    BackendVersion,
}

/// What the configured substrate can do.
///
/// Derived statically from the backend variant at construction; verified live
/// at boot when `storage.fail_fast` is set. These are facts about the
/// substrate, not behavior choices — behavior (like
/// `PrefixDeleteMode`) is derived from them at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapabilities {
    /// Which token authorizes `PutMode::Update`, or `None` if conditional
    /// PUT is unsupported (Local).
    pub conditional_put: Option<CasTokenKind>,
    /// `PutMode::Create` (create-only PUT) enforced atomically.
    pub create_only_put: bool,
    /// LIST results carry an ETag comparable with GET's ETag after
    /// [`canonical_etag`] normalization. (Azure lists ETags unquoted while
    /// GETs quote them; comparisons must go through the canonical form.)
    pub list_etag_comparable: bool,
    /// Backend batches multi-key deletes natively (S3 DeleteObjects).
    pub native_batch_delete: bool,
    /// Server-side copy with atomic destination creation.
    pub copy_if_not_exists: bool,
    /// Deleting an absent key reports success (S3) rather than NotFound
    /// (GCS, Azure, Local, InMemory). The seam normalizes both shapes to
    /// "absence is success"; this flag records the raw substrate behavior
    /// the boot probe verifies.
    pub delete_absent_is_ok: bool,
    /// User metadata attributes survive conditional puts.
    pub user_metadata: bool,
    /// Metadata wire names must be valid identifiers — no hyphens (Azure's
    /// C#-identifier rule). The seam canonicalizes hyphens to underscores on
    /// write and back on read for such substrates; logical keys inside
    /// Zeppelin never change.
    pub user_metadata_identifier_names: bool,
}

impl StorageCapabilities {
    /// S3 and S3-compatible substrates (MinIO), with
    /// `S3ConditionalPut::ETagMatch` enabled by construction.
    #[must_use]
    pub const fn s3() -> Self {
        Self {
            conditional_put: Some(CasTokenKind::ETag),
            create_only_put: true,
            list_etag_comparable: true,
            native_batch_delete: true,
            copy_if_not_exists: true,
            delete_absent_is_ok: true,
            user_metadata: true,
            user_metadata_identifier_names: false,
        }
    }

    /// Google Cloud Storage: CAS keys on the object generation, no native
    /// batch delete, delete-of-absent reports NotFound.
    #[must_use]
    pub const fn gcs() -> Self {
        Self {
            conditional_put: Some(CasTokenKind::BackendVersion),
            create_only_put: true,
            list_etag_comparable: true,
            native_batch_delete: false,
            copy_if_not_exists: true,
            delete_absent_is_ok: false,
            user_metadata: true,
            user_metadata_identifier_names: false,
        }
    }

    /// Azure Blob Storage: ETag CAS, no native batch delete, delete-of-absent
    /// reports NotFound, metadata names must be valid C# identifiers.
    #[must_use]
    pub const fn azure() -> Self {
        Self {
            conditional_put: Some(CasTokenKind::ETag),
            create_only_put: true,
            list_etag_comparable: true,
            native_batch_delete: false,
            copy_if_not_exists: true,
            delete_absent_is_ok: false,
            user_metadata: true,
            user_metadata_identifier_names: true,
        }
    }

    /// Local filesystem (development/testing only): no conditional PUT at
    /// all — every CAS consumer fails loudly with `NotImplemented`.
    #[must_use]
    pub const fn local() -> Self {
        Self {
            conditional_put: None,
            create_only_put: true,
            list_etag_comparable: true,
            native_batch_delete: false,
            copy_if_not_exists: true,
            delete_absent_is_ok: false,
            user_metadata: false,
            user_metadata_identifier_names: false,
        }
    }

    /// `object_store::memory::InMemory`, what `TEST_BACKEND=memory` wraps.
    #[must_use]
    pub const fn in_memory() -> Self {
        Self {
            conditional_put: Some(CasTokenKind::ETag),
            create_only_put: true,
            list_etag_comparable: true,
            native_batch_delete: false,
            copy_if_not_exists: true,
            delete_absent_is_ok: false,
            user_metadata: true,
            user_metadata_identifier_names: false,
        }
    }

    /// The static matrix row for a configured backend variant.
    ///
    /// This is the config-time view (used by `Config::validate` and startup
    /// gates before a store exists); the constructed
    /// [`crate::storage::ZeppelinStore`] carries the same value.
    #[must_use]
    pub const fn for_backend(backend: StorageBackend) -> Self {
        match backend {
            StorageBackend::S3 => Self::s3(),
            StorageBackend::Gcs => Self::gcs(),
            StorageBackend::Azure => Self::azure(),
            StorageBackend::Local => Self::local(),
        }
    }
}

/// Canonical ETag form for cross-request comparisons.
///
/// Strips the RFC 9110 weak-validator prefix and one pair of surrounding
/// quotes. Needed because Azure returns the ETag unquoted in LIST responses
/// and quoted in GET/PUT responses (real-cloud behavior, mirrored by
/// Azurite); S3 and GCS quote both sides, so canonicalization is a no-op
/// for equality there. Comparisons between LIST- and GET-derived ETags must
/// use this form; the raw token is never rewritten or stored canonicalized.
#[must_use]
pub fn canonical_etag(etag: &str) -> &str {
    let etag = etag.strip_prefix("W/").unwrap_or(etag);
    etag.strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(etag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_matches_backend_variants() {
        assert_eq!(
            StorageCapabilities::for_backend(StorageBackend::S3),
            StorageCapabilities::s3()
        );
        assert_eq!(
            StorageCapabilities::for_backend(StorageBackend::Gcs),
            StorageCapabilities::gcs()
        );
        assert_eq!(
            StorageCapabilities::for_backend(StorageBackend::Azure),
            StorageCapabilities::azure()
        );
        assert_eq!(
            StorageCapabilities::for_backend(StorageBackend::Local),
            StorageCapabilities::local()
        );
    }

    #[test]
    fn cas_token_kinds_follow_substrate_protocols() {
        assert_eq!(
            StorageCapabilities::s3().conditional_put,
            Some(CasTokenKind::ETag)
        );
        assert_eq!(
            StorageCapabilities::gcs().conditional_put,
            Some(CasTokenKind::BackendVersion)
        );
        assert_eq!(
            StorageCapabilities::azure().conditional_put,
            Some(CasTokenKind::ETag)
        );
        assert_eq!(StorageCapabilities::local().conditional_put, None);
    }

    #[test]
    fn only_s3_batches_deletes_natively_and_tolerates_absent_deletes() {
        for (caps, is_s3) in [
            (StorageCapabilities::s3(), true),
            (StorageCapabilities::gcs(), false),
            (StorageCapabilities::azure(), false),
            (StorageCapabilities::local(), false),
            (StorageCapabilities::in_memory(), false),
        ] {
            assert_eq!(caps.native_batch_delete, is_s3);
            assert_eq!(caps.delete_absent_is_ok, is_s3);
        }
    }

    #[test]
    fn only_azure_requires_identifier_metadata_names() {
        for (caps, requires) in [
            (StorageCapabilities::s3(), false),
            (StorageCapabilities::gcs(), false),
            (StorageCapabilities::azure(), true),
            (StorageCapabilities::local(), false),
            (StorageCapabilities::in_memory(), false),
        ] {
            assert_eq!(caps.user_metadata_identifier_names, requires);
        }
    }

    #[test]
    fn canonical_etag_strips_quotes_and_weak_prefix_only() {
        assert_eq!(canonical_etag("\"abc\""), "abc");
        assert_eq!(canonical_etag("abc"), "abc");
        assert_eq!(canonical_etag("W/\"abc\""), "abc");
        // Unbalanced quotes are left alone rather than half-stripped.
        assert_eq!(canonical_etag("\"abc"), "\"abc");
        assert_eq!(canonical_etag("abc\""), "abc\"");
        assert_eq!(canonical_etag(""), "");
    }
}
