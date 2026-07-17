//! Namespace metadata, lifecycle, and CRUD boundary.
//!
//! [`crate::namespace::manager::NamespaceManager`] is the public entry point for
//! creating, discovering, updating, and deleting namespaces. The manager stores
//! authoritative metadata in S3 or MinIO, uses a disposable process-local
//! registry for read performance, and coordinates deletion through a durable
//! tombstone. It does not own WAL or segment visibility; the manifest layer
//! remains responsible for those artifacts.
//!
//! Start with [`crate::namespace::manager::NamespaceMetadata`] for the persisted
//! shape, then read [`crate::namespace::manager::NamespaceManager`] for the
//! lifecycle operations.

/// Namespace branching identities and fail-closed validation errors.
pub mod branching;
/// Namespace metadata and lifecycle implementation.
pub mod manager;
/// Strong namespace identities shared by metadata, security, and manifests.
pub mod types;

/// Public namespace manager used by startup, HTTP handlers, and maintenance.
pub use manager::NamespaceManager;
pub use types::{NamespaceId, NamespaceIncarnationId};

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{NamespaceId, NamespaceIncarnationId};

    #[test]
    fn identity_move_preserves_public_paths_and_wire_forms() {
        let namespace = NamespaceId::new("tenant-a").expect("valid namespace");
        let security_namespace: crate::security::NamespaceId = namespace.clone();
        assert_eq!(security_namespace.as_str(), "tenant-a");
        assert_eq!(serde_json::to_string(&namespace).unwrap(), r#""tenant-a""#);
        let decoded_namespace: crate::security::NamespaceId =
            serde_json::from_str(r#""tenant-a""#).unwrap();
        assert_eq!(decoded_namespace, namespace);
        assert!(matches!(
            NamespaceId::new("../tenant"),
            Err(crate::security::SecurityError::InvalidNamespaceId)
        ));

        let uuid = uuid::Uuid::from_u128(0x1234567890abcdef1234567890abcdef);
        let incarnation = NamespaceIncarnationId::from_uuid(uuid);
        let manager_incarnation: crate::namespace::manager::NamespaceIncarnationId =
            incarnation.clone();
        assert_eq!(manager_incarnation.as_uuid(), uuid);
        assert_eq!(incarnation.to_string(), uuid.to_string());
        assert_eq!(
            serde_json::to_string(&incarnation).unwrap(),
            format!(r#""{uuid}""#)
        );
        let decoded_incarnation: NamespaceIncarnationId =
            serde_json::from_str(&format!(r#""{uuid}""#)).unwrap();
        assert_eq!(decoded_incarnation, incarnation);
    }
}
