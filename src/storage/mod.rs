//! Zeppelin's object-storage abstraction boundary.
//!
//! Higher-level domains import [`crate::storage::ZeppelinStore`] from this
//! module instead of depending on `object_store` directly. The implementation
//! in [`crate::storage::store`] builds S3/MinIO or local backends, performs full
//! and ranged reads, provides conditional writes used by manifests and leases,
//! and supports bounded cleanup. S3/MinIO remains authoritative; this module
//! does not make local cache state authoritative and an uploaded artifact is not
//! visible until a manifest references it.
//!
//! ```text
//! domain modules
//!      |
//!      | crate::storage::ZeppelinStore
//!      v
//! storage::store implementation
//!      |
//!      v
//! object_store backend -> S3 / MinIO / local test backend
//! ```
//!
//! Start with [`crate::storage::store::ZeppelinStore::from_config`] for backend
//! construction, then follow the read and conditional-write operations on
//! [`crate::storage::store::ZeppelinStore`].
//!
//! ## Rust concepts used here
//!
//! `pub use` re-exports the gateway and deletion result so callers depend on the
//! stable `crate::storage` boundary rather than the implementation file layout.
//! This resembles a Java package facade or a public C header: implementation can
//! stay in a submodule while the compiler checks which names are part of the
//! public API.

/// Implements backend construction and normalized storage operations.
pub mod store;

/// Re-exports the storage gateway and bounded-cleanup progress type.
pub use store::{DeletePrefixOutcome, ZeppelinStore};
