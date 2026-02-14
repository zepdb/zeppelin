/// Batched WAL writer for high-throughput ingestion.
pub mod batch_writer;
/// WAL fragment serialization and checksum validation.
pub mod fragment;
/// Namespace lease management (fencing tokens, expiry).
pub mod lease;
/// Manifest: single source of truth for fragments and segments.
pub mod manifest;
/// WAL fragment reader (list, read, read-unchecked).
pub mod reader;
/// WAL writer with per-namespace locking and CAS manifest updates.
pub mod writer;

pub use fragment::WalFragment;
pub use lease::{Lease, LeaseManager};
pub use manifest::{Manifest, ManifestVersion};
pub use reader::WalReader;
pub use writer::WalWriter;
