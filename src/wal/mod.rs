//! Durable write-ahead-log artifacts, visibility metadata, and write coordination.
//!
//! This subsystem is the boundary between accepted vector mutations and the
//! immutable indexed segments produced by [`crate::compaction`]. Write handlers
//! call [`crate::wal::writer::WalWriter`] to upload immutable
//! [`crate::wal::fragment::WalFragment`] objects and publish their references in
//! [`crate::wal::manifest::Manifest`]. Query execution and compaction use
//! [`crate::wal::reader::WalReader`] to replay the manifest-selected fragments.
//! [`crate::wal::lease::LeaseManager`] supplies time-bounded ownership and
//! fencing generations for long-running or multi-process writes.
//!
//! ```text
//! vector upserts / deletes
//!           |
//!           v
//! serialize + PUT immutable WAL fragment
//!           | object exists, but is not visible
//!           v
//! publish FragmentRef in authoritative manifest with CAS
//!           |
//!           +-----------------------+
//!           v                       v
//! query replays live WAL      compaction builds immutable segment
//!                                   |
//!                                   v
//!                    manifest CAS replaces fragment refs
//!                                   |
//!                                   v
//!                         reachability-aware cleanup
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::wal::fragment`] for the immutable operation batch and
//!    checksum/serialization contract.
//! 2. Read [`crate::wal::manifest`] for the authoritative visibility model and
//!    monotonic replay sequence.
//! 3. Read [`crate::wal::writer`] for upload-before-publication, group commit,
//!    fencing, and CAS.
//! 4. Read [`crate::wal::reader`] for ordered replay, cache use, and verified
//!    compaction/GC races.
//! 5. Read [`crate::wal::lease`] for acquisition, heartbeat renewal, takeover,
//!    and best-effort release.
//!
//! ## Invariants
//!
//! - WAL fragments and segments are write-once; the manifest changes references
//!   instead of mutating artifacts in place.
//! - S3/MinIO manifest state is authoritative. Memory and disk cache only bytes
//!   for already-selected immutable objects.
//! - Uploading a fragment does not expose it. Successful manifest publication is
//!   the visibility boundary.
//! - Replay follows manifest-assigned sequence numbers rather than wall clocks or
//!   ULID ordering.
//! - Where leases are used, fencing checks and ETag CAS are complementary stale-
//!   writer defenses. Best-effort release is cleanup, not a correctness barrier.
//!
//! ## Rust concepts used here
//!
//! The module exposes a small facade through `pub use` while retaining focused
//! implementation modules. This resembles Java package exports or C umbrella
//! headers, but Rust privacy is compiler-enforced at the module boundary. Domain
//! operations return [`crate::error::Result`] rather than exceptions or integer
//! status codes, and immutable payloads commonly use [`bytes::Bytes`] so async
//! layers can transfer shared buffers without copying their contents.

/// Defines immutable WAL fragment payloads, versioned serialization, and checksum validation.
pub mod fragment;
pub mod fragment_cache;
/// Coordinates namespace lease acquisition, renewal, fencing generations, and release.
pub mod lease;
/// Defines and publishes the authoritative namespace manifest and retained history.
pub mod manifest;
/// Reads manifest-selected fragments through checked, unchecked, and cached paths.
pub mod reader;
/// Uploads fragments and group-commits their references with fencing and CAS.
pub mod writer;

/// Re-exports the primary immutable WAL fragment type for crate consumers.
pub use fragment::WalFragment;
pub use fragment_cache::WalFragmentCache;
/// Re-exports namespace lease snapshots and their object-store manager.
pub use lease::{Lease, LeaseManager};
/// Re-exports the authoritative manifest and its object-store version token.
pub use manifest::{Manifest, ManifestVersion};
/// Re-exports the fragment reader and its explicit immutable-byte cache policy.
pub use reader::{FragmentCachePolicy, WalReader};
/// Re-exports the fragment uploader and manifest group-commit coordinator.
pub use writer::WalWriter;
