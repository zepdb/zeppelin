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

/// Namespace metadata and lifecycle implementation.
pub mod manager;

/// Public namespace manager used by startup, HTTP handlers, and maintenance.
pub use manager::NamespaceManager;
