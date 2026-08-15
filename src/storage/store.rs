//! Object-storage construction and the only object-store API exposed to
//! Zeppelin domains.
//!
//! [`crate::storage::store::ZeppelinStore`] sits below manifests, WAL
//! management, compaction, indexing,
//! query execution, cache hydration, and namespace management. Those layers pass
//! object keys and immutable byte buffers into this wrapper; this file translates
//! them into [`object_store::ObjectStore`] operations, normalizes selected
//! errors into [`crate::error::ZeppelinError`], and records storage latency and
//! error metrics. Production code above this module must not call
//! `object_store` directly.
//!
//! S3 or MinIO remains authoritative. A successful
//! [`crate::storage::store::ZeppelinStore::put`] only
//! creates or replaces an object; it does **not** make a WAL fragment or segment
//! visible. Visibility is a separate manifest operation built from
//! [`crate::storage::store::ZeppelinStore::get_with_meta`] and
//! [`crate::storage::store::ZeppelinStore::put_if_match`]. The local
//! backend implements the same interface for local runs and tests, but does not
//! change that production authority model.
//!
//! ```text
//! WAL / manifest / compaction / query / cache / namespace layers
//!                              |
//!                              | object keys and Bytes
//!                              v
//!                       ZeppelinStore
//!                    /        |         \
//!          ordinary I/O   conditional I/O   discovery / cleanup
//!          get, put, head  ETag CAS, create  list, delete prefix
//!                    \        |         /
//!                              v
//!                  dyn ObjectStore implementation
//!                    /                     \
//!          S3 or MinIO (authority)       local filesystem
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::storage::store::ZeppelinStore`] and
//!    [`crate::storage::store::ZeppelinStore::from_config`] to see the
//!    backend boundary and S3 client policy.
//! 2. Read [`crate::storage::store::ZeppelinStore::get`],
//!    [`crate::storage::store::ZeppelinStore::get_range`], and
//!    [`crate::storage::store::ZeppelinStore::get_ranges`] for ordinary and
//!    query-path reads.
//! 3. Read [`crate::storage::store::ZeppelinStore::get_with_meta`],
//!    [`crate::storage::store::ZeppelinStore::get_if_none_match`],
//!    [`crate::storage::store::ZeppelinStore::put_if_match`],
//!    [`crate::storage::store::ZeppelinStore::put_create`], and
//!    [`crate::storage::store::ZeppelinStore::put_if_not_exists`] for conditional
//!    consistency operations.
//! 4. Finish with [`crate::storage::store::ZeppelinStore::delete_prefix_paged`]
//!    and [`crate::storage::store::DeletePrefixOutcome`] for bounded namespace
//!    cleanup.
//!
//! ## Invariants not to break
//!
//! - Higher layers use this wrapper rather than bypassing storage policy and
//!   instrumentation through the raw [`object_store::ObjectStore`].
//! - S3 conditional PUT support stays enabled; manifest publication and leases
//!   require an ETag check, not a read-then-unconditional-write sequence.
//! - Object existence is distinct from manifest visibility.
//! - Recursive listing and deletion reject an empty prefix where it could scan
//!   or erase the entire store accidentally.
//! - Range reads reject empty and reversed ranges before issuing remote I/O.
//! - Prefix deletion is allowed to make partial progress and reports whether
//!   the listing was completely consumed.
//!
//! ## Rust concepts used here
//!
//! The wrapper stores `Arc<dyn ObjectStore>`. `dyn ObjectStore` is trait-object
//! dispatch: the concrete S3, local, in-memory, or test-instrumented backend is
//! selected at runtime. `Arc` gives clones shared ownership of that backend and
//! keeps it alive across async tasks. A Java reader can think of an interface
//! reference held by a thread-safe reference-counted owner. In C this would
//! require a function table plus explicit lifetime and synchronization rules;
//! Rust checks the trait's `Send + Sync` requirements and drops the backend only
//! after the last `Arc` is gone.
//!
//! Payloads use [`bytes::Bytes`], whose clones share an immutable buffer instead
//! of copying every byte. Conditional operations use `Result`, `Option`, and
//! exhaustive `match` arms so callers must distinguish success, unchanged state,
//! conflicts, missing objects, and storage failures explicitly.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, BackoffConfig, ClientOptions, GetOptions, ObjectStore,
    PutMode, PutOptions, PutPayload, RetryConfig, UpdateVersion,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;
use tracing::{debug, instrument};

use crate::config::StorageConfig;
use crate::error::{Result, ZeppelinError};
use crate::storage::capabilities::{canonical_etag, CasTokenKind, StorageCapabilities};
use crate::storage::{namespace_prefix, NamespaceObjectFamily, NamespaceObjectKey};

/// Maximum number of exact keys accepted by one `delete_many` call.
///
/// The bound originates from S3's DeleteObjects request limit, and the seam
/// applies it as its chunk size on every substrate so batch shape, retry, and
/// progress semantics stay identical regardless of whether the backend
/// batches natively.
pub(crate) const DELETE_MANY_MAX_KEYS: usize = 1_000;

/// Hard process bound for transient successful-PUT hashes awaiting publication.
const CONTENT_HASH_CACHE_MAX_ENTRIES: usize = 65_536;

#[derive(Debug, Default)]
struct ContentHashCache {
    entries: BTreeMap<String, [u8; 32]>,
}

impl ContentHashCache {
    fn insert(&mut self, key: String, content_hash: [u8; 32]) {
        if !self.entries.contains_key(&key) && self.entries.len() >= CONTENT_HASH_CACHE_MAX_ENTRIES
        {
            // The cache is only a zero-readback optimization. Evicting one
            // unpublished hash cannot change authority: scoped ANN validation
            // fails loudly if the exact hash is unavailable when needed.
            if let Some(oldest_key) = self.entries.keys().next().cloned() {
                self.entries.remove(&oldest_key);
            }
        }
        self.entries.insert(key, content_hash);
    }
}

/// Shared object-storage gateway used by every Zeppelin domain layer.
///
/// Cloning this value clones the [`Arc`] handle, not the backend or its data.
/// All clones therefore use the same connection pool and underlying store.
/// The type deliberately exposes domain-oriented operations such as ETag
/// compare-and-swap and bounded prefix deletion rather than every
/// [`ObjectStore`] primitive.
///
/// # Examples
///
/// A WAL writer and a query executor may each own a cloned `ZeppelinStore`.
/// Both handles address the same S3 bucket, while Rust keeps the shared backend
/// alive until both components have been dropped.
#[derive(Clone)]
pub struct ZeppelinStore {
    /// Runtime-selected backend shared by all clones of this gateway.
    inner: Arc<dyn ObjectStore>,
    /// Declared capability matrix for the constructed substrate.
    capabilities: StorageCapabilities,
    /// Prefix-cleanup behavior selected when the backend is constructed.
    prefix_delete_mode: PrefixDeleteMode,
    /// Transient hashes of exact bodies accepted by local immutable PUTs.
    content_hashes: Arc<Mutex<ContentHashCache>>,
    /// Signing root and immutable signer-inventory view installed together by
    /// the live security composition root.
    object_signer: Arc<RwLock<ObjectSignerBinding>>,
}

/// Process-local signer backed by a public key published under `_security/signers/`.
pub(crate) trait ObjectSigner: Send + Sync {
    fn signer_node(&self) -> &str;
    fn sign(&self, message: &[u8]) -> Vec<u8>;
    fn publication_store(&self) -> ZeppelinStore;
}

/// Explicit view used to find the authoritative published signer inventory.
///
/// `CallerStore` is selected only for a fresh detached store with no installed
/// signer. Once a signer is installed, `Installed` records its publication
/// scope as a detached wrapper so verification never needs the live signer.
#[derive(Clone, Default)]
enum SignerInventoryView {
    #[default]
    CallerStore,
    Installed(ZeppelinStore),
}

/// Shared application-store signing state.
///
/// The signer is deliberately weak, while the inventory view is a detached
/// object-store wrapper with fresh signing state. Keeping them under one lock
/// makes same-node replacement update both views atomically.
#[derive(Default)]
struct ObjectSignerBinding {
    signer: Option<Weak<dyn ObjectSigner>>,
    inventory_view: SignerInventoryView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixDeleteMode {
    LegacyPerKeyUnordered32,
    NativeBatch,
}

/// Progress made by one bounded recursive prefix-deletion pass.
///
/// A caller resumes cleanup when [`Self::complete`] is `false`; it should not
/// infer that the remaining key set is unchanged because other actors may have
/// created or deleted objects between passes.
///
/// # Examples
///
/// `DeletePrefixOutcome { deleted: 1_000, complete: false }` means this pass
/// accepted 1,000 keys as deleted but stopped before consuming the entire
/// listing. A later pass should list the prefix again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePrefixOutcome {
    /// Number of selected keys deleted or already absent during this pass.
    pub deleted: usize,
    /// Whether the pass observed the end of the source listing before stopping.
    pub complete: bool,
}

/// Result of one atomic create-only object-store PUT.
///
/// Higher-level control-plane protocols use this neutral outcome instead of
/// inspecting backend-specific `object_store` errors outside the storage
/// boundary. A collision never overwrites the existing object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOnlyOutcome {
    /// This caller created the object. The backend may report no identity.
    Created {
        /// Backend-provided identity for the newly created object.
        version: Option<StorageVersion>,
    },
    /// The destination already existed and remains unchanged.
    AlreadyExists,
}

/// Result of one domain-neutral compare-and-swap PUT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionalPutOutcome {
    /// The observed identity matched and the replacement became authoritative.
    Updated {
        /// Backend-provided identity for the replacement object.
        version: Option<StorageVersion>,
    },
    /// Another writer changed the object first; no replacement occurred.
    Conflict,
}

/// One non-empty opaque backend identity observed for an object-store object.
///
/// Both forms are carried when a backend supplies both, because different
/// substrates key their conditional operations on different ones: S3 and Azure
/// require the ETag, while GCS requires the object generation and ignores ETags
/// for conditional puts entirely. Keeping only the preferred form would make a
/// GCS compare-and-swap inexpressible.
///
/// Absence is represented by `None` on the containing [`ListedObject`] or
/// return value, never by an all-empty token: [`StorageVersion::from_parts`] is
/// the only constructor and yields `None` when the backend supplied neither
/// form. Two unversioned observations therefore cannot compare equal as if they
/// authorized cache reuse.
///
/// This is a process-local concurrency token. It is never serialized into an
/// artifact and never compared across substrates.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageVersion {
    /// Entity tag supplied by the backend, when it supplied one.
    e_tag: Option<String>,
    /// Backend-specific version identifier (GCS generation, Azure version ID).
    backend_version: Option<String>,
}

/// User-defined object metadata carried through the storage boundary.
///
/// Domain modules use this owned map instead of depending on
/// `object_store::Attributes`. The values travel as S3 user-metadata headers
/// and therefore do not alter object bodies or object-store call counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectUserMetadata(BTreeMap<String, String>);

impl ObjectUserMetadata {
    /// Creates an empty metadata collection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts one owned user-metadata value.
    ///
    /// Logical keys are lowercase ASCII alphanumerics and hyphens only. The
    /// hyphen restriction is what keeps the Azure wire canonicalization
    /// (hyphen ↔ underscore, see [`Self::to_attributes`]) bijective — a
    /// logical key containing an underscore would collide with the wire form
    /// of its hyphenated sibling on read.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        debug_assert!(
            !key.is_empty()
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
            "user-metadata keys must be lowercase ASCII alphanumerics and hyphens, got {key:?}"
        );
        let _ = self.0.insert(key, value.into());
    }

    /// Returns one user-metadata value by its unprefixed key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Reads wire attributes back into logical keys.
    ///
    /// Wire underscores normalize to logical hyphens on every substrate: the
    /// identifier-only substrates (Azure) write the underscore form, and the
    /// [`Self::insert`] key alphabet (no underscores in logical keys) makes
    /// the mapping bijective, so hyphen-native substrates are unaffected.
    fn from_attributes(attributes: &Attributes) -> Self {
        let values = attributes
            .iter()
            .filter_map(|(attribute, value)| match attribute {
                Attribute::Metadata(key) => {
                    Some((key.replace('_', "-"), value.as_ref().to_string()))
                }
                _ => None,
            })
            .collect();
        Self(values)
    }

    /// Lowers logical keys to their substrate wire form.
    ///
    /// Azure metadata names must be valid C# identifiers — hyphens are
    /// illegal — so identifier-only substrates get the underscore form
    /// (`zeppelin-namespace-incarnation` → `zeppelin_namespace_incarnation`).
    /// Every other substrate writes the native hyphenated form unchanged:
    /// a single global wire form would break existing S3/GCS objects that
    /// already carry hyphenated names. Azure has no pre-canonicalization
    /// deployments, so no migration is needed there.
    fn to_attributes(&self, identifier_wire_names: bool) -> Attributes {
        self.0
            .iter()
            .map(|(key, value)| {
                let wire_key = if identifier_wire_names {
                    key.replace('-', "_")
                } else {
                    key.clone()
                };
                (
                    Attribute::Metadata(wire_key.into()),
                    AttributeValue::from(value.clone()),
                )
            })
            .collect()
    }
}

/// Body-adjacent metadata returned by one authoritative object GET.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReadMetadata {
    /// Backend identity of the exact body returned by the same request.
    pub version: Option<StorageVersion>,
    /// User-defined metadata headers attached to that object version.
    pub user_metadata: ObjectUserMetadata,
}

impl StorageVersion {
    /// Returns the ETag when the backend supplied one.
    ///
    /// Callers that need byte-identity against a LIST observation use this and
    /// treat its absence as "cannot validate", because only the ETag is
    /// comparable across a LIST and a GET on the backends Zeppelin supports.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    /// Returns the substrate-native version identifier, when the backend has one.
    ///
    /// GCS reports an object generation here and requires it for conditional
    /// puts; S3 and MinIO leave it absent.
    #[must_use]
    pub fn backend_version(&self) -> Option<&str> {
        self.backend_version.as_deref()
    }

    /// Borrows an observed token, or raises the loud error when there is none.
    ///
    /// Every conditional write is preceded by a read that either produced an
    /// identity or did not. This is the single place that turns "did not" into
    /// [`ZeppelinError::MissingVersionToken`], so no caller can quietly
    /// substitute an empty precondition and convert its compare-and-swap into an
    /// unconditional overwrite.
    ///
    /// # Errors
    ///
    /// [`ZeppelinError::MissingVersionToken`] when `version` is `None`.
    pub fn require<'a>(version: Option<&'a Self>, key: &str) -> Result<&'a Self> {
        version.ok_or_else(|| ZeppelinError::MissingVersionToken {
            key: key.to_string(),
        })
    }

    /// Builds a token from one backend observation, or `None` when it carries none.
    ///
    /// This is the only constructor, so the non-empty invariant holds
    /// everywhere. Empty strings count as absent. Returning `Option` rather than
    /// an all-empty token is what keeps an unversioned observation from
    /// comparing equal to another unversioned observation.
    #[must_use]
    pub fn from_parts(etag: Option<String>, backend_version: Option<String>) -> Option<Self> {
        let e_tag = etag.filter(|value| !value.is_empty());
        let backend_version = backend_version.filter(|value| !value.is_empty());
        if e_tag.is_none() && backend_version.is_none() {
            return None;
        }
        Some(Self {
            e_tag,
            backend_version,
        })
    }

    /// Lowers this token into the object-store precondition for a conditional put.
    ///
    /// Both fields pass through unchanged and the backend selects the one its
    /// protocol defines: S3 and Azure read `e_tag`, GCS reads `version`. A
    /// backend whose required field is absent fails loudly inside object_store
    /// (`MissingVersion` / `MissingETag`) rather than degrading to an
    /// unconditional write.
    fn to_update_version(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.backend_version.clone(),
        }
    }
}

/// Storage-owned metadata for one object returned by a recursive LIST.
///
/// Domain layers receive this type instead of depending directly on
/// `object_store::ObjectMeta`. The metadata is an observation from S3, not a
/// replacement for an authoritative body read when the version is absent or
/// has changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// Exact object key returned by the backend.
    pub key: String,
    /// Object payload size in bytes.
    pub size: u64,
    /// Backend-reported last modification timestamp.
    pub last_modified: DateTime<Utc>,
    /// Non-empty opaque identity, or `None` when this observation is unversioned.
    ///
    /// **Revalidation and comparison only — never pass a LIST-derived token
    /// to `put_if_match*`.** LIST responses carry an ETag but no
    /// substrate-native version, and on GCS the object generation is the only
    /// token that authorizes a conditional PUT, so a LIST-derived token would
    /// die with `MissingVersion` there. Every CAS uses a GET- or PUT-derived
    /// token (audited across all `put_if_match*` call sites,
    /// `tasks/multi-substrate/08-release-evidence.md`).
    pub version: Option<StorageVersion>,
}

/// Constructs backends and performs Zeppelin's normalized storage operations.
impl ZeppelinStore {
    /// Builds the configured storage backend and wraps it in the Zeppelin gateway.
    ///
    /// S3 configuration enables ETag conditional PUTs for manifest and lease
    /// compare-and-swap, atomic destination creation for clone copies, bounded
    /// retries, and a shared HTTP connection pool. The local backend creates its
    /// configured root directory when it does not already exist. GCS and Azure
    /// values are rejected because this constructor does not implement them.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed boot-time backend, bucket/path, endpoint, credential,
    ///   and transport settings. The returned store owns everything it needs;
    ///   it does not retain this borrow.
    ///
    /// # Returns
    ///
    /// A store whose concrete backend is hidden behind a shared
    /// [`ObjectStore`] trait object.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] when an S3 or local backend cannot be
    /// built or when the selected backend is unsupported. Local directory
    /// creation can also return a filesystem error. No alternate backend is
    /// selected after a failure.
    ///
    /// # Side Effects
    ///
    /// Building an S3 backend configures a client but does not contact the
    /// endpoint. Building a local backend may synchronously create directories
    /// on disk.
    ///
    /// # Consistency
    ///
    /// `S3ConditionalPut::ETagMatch` is essential: without it,
    /// [`Self::put_if_match`] cannot enforce the CAS half of manifest and lease
    /// correctness. Retry policy belongs to the backend, but conditional
    /// preconditions still decide whether a write is allowed.
    ///
    /// # Performance
    ///
    /// S3 requests use at most five configured retries, exponential backoff
    /// from 100 ms to a 5-second ceiling, a 15-second retry window, a
    /// 30-second request timeout, and up to 64 idle pooled connections per
    /// host. Local root creation performs blocking filesystem work during
    /// construction.
    ///
    /// # Examples
    ///
    /// ```text
    /// StorageBackend::S3 + bucket "vectors"
    ///         -> configured S3 client, no request yet
    ///
    /// StorageBackend::Local + missing root "/tmp/zeppelin"
    ///         -> directory is created, then a local ObjectStore is wrapped
    ///
    /// StorageBackend::Gcs
    ///         -> configuration error; Zeppelin does not fall back to local disk
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `match` must account for every [`crate::config::StorageBackend`]
    /// variant. The final `backend` arm rejects currently unsupported variants;
    /// it is an explicit error, not Java-style `null` or a C sentinel. Each
    /// builder call consumes and returns the builder, so rebinding `builder`
    /// models a checked construction pipeline without shared mutable state.
    pub fn from_config(config: &StorageConfig) -> Result<Self> {
        let store = Self::raw_backend_from_config(config)?;
        let capabilities = StorageCapabilities::for_backend(config.backend);
        let prefix_delete_mode = if capabilities.native_batch_delete {
            PrefixDeleteMode::NativeBatch
        } else {
            PrefixDeleteMode::LegacyPerKeyUnordered32
        };
        Ok(Self {
            inner: store,
            capabilities,
            prefix_delete_mode,
            content_hashes: Arc::new(Mutex::new(ContentHashCache::default())),
            object_signer: Arc::new(RwLock::new(ObjectSignerBinding::default())),
        })
    }

    /// Builds the raw `object_store` backend for a configuration.
    ///
    /// Crate-internal so instrumented test harnesses (the phase-9 counting
    /// bench) can wrap the exact production transport instead of carrying a
    /// second hand-built S3 client — `src/storage/` stays the only
    /// `object_store` importer.
    pub(crate) fn raw_backend_from_config(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
        let store: Arc<dyn ObjectStore> =
            match config.backend {
                crate::config::StorageBackend::S3 => {
                    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

                    if let Some(ref region) = config.s3_region {
                        builder = builder.with_region(region);
                    }
                    if let Some(ref endpoint) = config.s3_endpoint {
                        if !endpoint.is_empty() {
                            builder = builder
                                .with_endpoint(endpoint)
                                .with_virtual_hosted_style_request(false);
                        }
                    }
                    if let Some(ref key_id) = config.s3_access_key_id {
                        builder = builder.with_access_key_id(key_id);
                    }
                    if let Some(ref secret) = config.s3_secret_access_key {
                        builder = builder.with_secret_access_key(secret);
                    }
                    // Enable conditional PUT (ETag-based CAS) — required for
                    // manifest conflict detection and lease CAS operations.
                    builder = builder.with_conditional_put(S3ConditionalPut::ETagMatch);
                    // Enable atomic create semantics for server-side copy,
                    // used by restore-as-clone materialization.
                    builder = builder.with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
                    builder = builder
                        .with_retry(transport_retry_config())
                        .with_client_options(transport_client_options(config.s3_allow_http));

                    Arc::new(builder.build().map_err(|e| {
                        ZeppelinError::Config(format!("failed to build S3 store: {e}"))
                    })?)
                }
                crate::config::StorageBackend::Gcs => {
                    let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new()
                        .with_bucket_name(&config.bucket);
                    let endpoint = config
                        .gcs_endpoint
                        .as_deref()
                        .filter(|value| !value.is_empty());
                    let account_path = config
                        .gcs_service_account_path
                        .as_deref()
                        .filter(|value| !value.is_empty());
                    let account_key = config
                        .gcs_service_account_key
                        .as_deref()
                        .filter(|value| !value.is_empty());
                    // Config validation enforces that path and inline key are
                    // mutually exclusive. object_store 0.11.2 has no endpoint
                    // builder knob: a custom endpoint travels as gcs_base_url
                    // inside the service-account JSON, so an endpoint-bearing
                    // configuration synthesizes (no credentials — emulator,
                    // OAuth disabled) or augments (real credentials) that JSON.
                    match (endpoint, account_path, account_key) {
                        (None, Some(path), None) => {
                            builder = builder.with_service_account_path(path);
                        }
                        (None, None, Some(key)) => {
                            builder = builder.with_service_account_key(key);
                        }
                        (None, None, None) => {
                            // Ambient chain: GOOGLE_APPLICATION_CREDENTIALS or
                            // instance metadata, resolved by object_store at
                            // request time. Fails loudly there if absent.
                        }
                        (Some(endpoint), path, key) => {
                            let json = gcs_service_account_json_with_endpoint(endpoint, path, key)?;
                            builder = builder.with_service_account_key(json);
                        }
                        (None, Some(_), Some(_)) => {
                            return Err(ZeppelinError::Config(
                                "gcs_service_account_path and gcs_service_account_key \
                                 are mutually exclusive"
                                    .to_string(),
                            ));
                        }
                    }
                    // Conditional PUT needs no opt-in: PutMode::Update maps to
                    // x-goog-if-generation-match and PutMode::Create to
                    // generation-match 0; a token without a generation fails
                    // loudly inside object_store (MissingVersion), which is
                    // exactly the fail-loud behavior the seam wants.
                    // copy_if_not_exists is native. Plain HTTP rides on the
                    // configured endpoint's scheme (emulators only).
                    let allow_http = endpoint.is_some_and(|e| e.starts_with("http://"));
                    builder = builder
                        .with_retry(transport_retry_config())
                        .with_client_options(transport_client_options(allow_http));

                    Arc::new(builder.build().map_err(|e| {
                        ZeppelinError::Config(format!("failed to build GCS store: {e}"))
                    })?)
                }
                crate::config::StorageBackend::Local => {
                    let path = std::path::Path::new(&config.bucket);
                    if !path.exists() {
                        std::fs::create_dir_all(path)?;
                    }
                    Arc::new(
                        object_store::local::LocalFileSystem::new_with_prefix(path).map_err(
                            |e| ZeppelinError::Config(format!("failed to build local store: {e}")),
                        )?,
                    )
                }
                crate::config::StorageBackend::Azure => {
                    let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
                        .with_container_name(&config.bucket);
                    if config.azure_use_emulator {
                        // Azurite's well-known dev account and key; the
                        // endpoint defaults to http://127.0.0.1:10000 and can
                        // be overridden with AZURITE_BLOB_STORAGE_URL.
                        builder = builder.with_use_emulator(true);
                    }
                    if let Some(account) = config
                        .azure_account_name
                        .as_deref()
                        .filter(|value| !value.is_empty())
                    {
                        builder = builder.with_account(account);
                    }
                    if let Some(key) = config
                        .azure_access_key
                        .as_deref()
                        .filter(|value| !value.is_empty())
                    {
                        builder = builder.with_access_key(key);
                    }
                    if let Some(endpoint) = config
                        .azure_endpoint
                        .as_deref()
                        .filter(|value| !value.is_empty())
                    {
                        builder = builder.with_endpoint(endpoint.to_string());
                    }
                    // Conditional put needs no opt-in: PutMode::Update maps to
                    // If-Match and PutMode::Create to If-None-Match: *; a
                    // token without an ETag fails loudly inside object_store
                    // (MissingETag). copy_if_not_exists is native.
                    builder = builder
                        .with_retry(transport_retry_config())
                        .with_client_options(transport_client_options(config.azure_allow_http));

                    Arc::new(builder.build().map_err(|e| {
                        ZeppelinError::Config(format!("failed to build Azure store: {e}"))
                    })?)
                }
            };
        Ok(store)
    }

    /// Returns the declared capability matrix for the constructed substrate.
    ///
    /// Callers gate on capabilities, never on backend identity: "does this
    /// store support conditional PUT?" is answerable here without knowing or
    /// caring which vendor provides it.
    #[must_use]
    pub fn capabilities(&self) -> StorageCapabilities {
        self.capabilities
    }

    /// Verifies the declared capability matrix against live substrate behavior.
    ///
    /// The boot storage probe calls this when `storage.fail_fast` is set. One
    /// probe object is written under the reserved `__zeppelin_probe__/` prefix
    /// — outside every namespace family, so [`NamespaceObjectKey::classify`]
    /// fails closed on it by construction and GC can never touch it. The
    /// round-trip exercises create-only PUT, conditional PUT with a fresh and
    /// then a deliberately stale token, LIST-vs-GET ETag comparability, and
    /// delete-of-absent semantics, then removes the object.
    ///
    /// # Errors
    ///
    /// [`ZeppelinError::Config`] naming the capability whose observed behavior
    /// contradicts the declaration. A stale-token CAS that succeeds is the
    /// most important refusal: it is the mis-deployed-backend case (for
    /// example MinIO without conditional-PUT support) where every
    /// compare-and-swap in the system would silently become an overwrite.
    pub async fn verify_declared_capabilities(&self) -> Result<()> {
        let caps = self.capabilities;
        let probe_prefix = "__zeppelin_probe__";
        let key = format!("{probe_prefix}/{}", uuid::Uuid::new_v4());
        let mismatch = |capability: &str, detail: String| {
            ZeppelinError::Config(format!(
                "storage capability verification failed: declared {capability} \
                 but the substrate behaved otherwise ({detail})"
            ))
        };

        let created = self
            .put_create_outcome(&key, Bytes::from_static(b"zeppelin-capability-probe-1"))
            .await?;
        let CreateOnlyOutcome::Created { version } = created else {
            return Err(mismatch(
                "create_only_put",
                format!("probe key {key} already existed"),
            ));
        };
        if caps.create_only_put {
            let conflict = self
                .put_create_outcome(&key, Bytes::from_static(b"zeppelin-capability-probe-dup"))
                .await?;
            if conflict != CreateOnlyOutcome::AlreadyExists {
                return Err(mismatch(
                    "create_only_put",
                    "second create-only PUT overwrote the probe object".to_string(),
                ));
            }
        }

        let mut last_version = version;
        if let Some(token_kind) = caps.conditional_put {
            let token = last_version.clone().ok_or_else(|| {
                mismatch(
                    "conditional_put",
                    "create returned no version token".to_string(),
                )
            })?;
            match token_kind {
                CasTokenKind::ETag if token.etag().is_none() => {
                    return Err(mismatch(
                        "conditional_put = ETag",
                        "create returned a token without an ETag".to_string(),
                    ));
                }
                CasTokenKind::BackendVersion if token.backend_version().is_none() => {
                    return Err(mismatch(
                        "conditional_put = BackendVersion",
                        "create returned a token without a backend version".to_string(),
                    ));
                }
                _ => {}
            }

            let updated = self
                .put_if_match_outcome(
                    &key,
                    Bytes::from_static(b"zeppelin-capability-probe-2"),
                    &token,
                )
                .await?;
            let fresh = match updated {
                ConditionalPutOutcome::Updated {
                    version: Some(fresh),
                } if fresh != token => fresh,
                ConditionalPutOutcome::Updated { version } => {
                    return Err(mismatch(
                        "conditional_put",
                        format!(
                            "CAS with the current token returned no fresh identity \
                             (got {version:?})"
                        ),
                    ));
                }
                ConditionalPutOutcome::Conflict => {
                    return Err(mismatch(
                        "conditional_put",
                        "CAS with the current token was rejected".to_string(),
                    ));
                }
            };

            let stale = self
                .put_if_match_outcome(
                    &key,
                    Bytes::from_static(b"zeppelin-capability-probe-3"),
                    &token,
                )
                .await?;
            if stale != ConditionalPutOutcome::Conflict {
                return Err(mismatch(
                    "conditional_put",
                    "CAS with a STALE token succeeded — conditional preconditions \
                     are not enforced by this deployment"
                        .to_string(),
                ));
            }
            last_version = Some(fresh);
        }

        if caps.list_etag_comparable {
            let get_etag = match last_version.as_ref().and_then(StorageVersion::etag) {
                Some(etag) => etag.to_string(),
                None => {
                    let (_, read_version) = self.get_with_meta(&key).await?;
                    read_version
                        .as_ref()
                        .and_then(StorageVersion::etag)
                        .map(str::to_string)
                        .ok_or_else(|| {
                            mismatch(
                                "list_etag_comparable",
                                "GET returned no ETag to compare against".to_string(),
                            )
                        })?
                }
            };
            // Prefixes are path-segment based, so list the reserved parent
            // and find this boot's probe key among (possibly concurrent or
            // crash-leaked) siblings.
            let listed = self.list_prefix_meta(probe_prefix).await?;
            let listed_etag = listed
                .iter()
                .find(|object| object.key == key)
                .and_then(|object| object.version.as_ref())
                .and_then(StorageVersion::etag)
                .ok_or_else(|| {
                    mismatch(
                        "list_etag_comparable",
                        "LIST returned no ETag for the probe object".to_string(),
                    )
                })?;
            if canonical_etag(listed_etag) != canonical_etag(&get_etag) {
                return Err(mismatch(
                    "list_etag_comparable",
                    format!(
                        "LIST ETag {listed_etag:?} does not identify the version \
                         written last ({get_etag:?})"
                    ),
                ));
            }
        }

        self.delete(&key).await?;
        let raw_absent_delete = self.inner.delete(&Path::parse(&key)?).await;
        match (caps.delete_absent_is_ok, raw_absent_delete) {
            (true, Ok(())) => {}
            (false, Err(object_store::Error::NotFound { .. })) => {}
            (declared, observed) => {
                return Err(mismatch(
                    "delete_absent_is_ok",
                    format!("declared {declared}, observed {observed:?}"),
                ));
            }
        }
        Ok(())
    }

    /// Fails startup early when an explicitly configured storage endpoint is not reachable.
    ///
    /// The probe applies only to configurations that name a custom endpoint
    /// for their backend (today `s3_endpoint`; the GCS/Azure endpoint fields
    /// join it with their transports). No endpoint configured means nothing
    /// to probe — real-cloud DNS is exercised by the object-store-level boot
    /// probe instead. Hostnames are tested with an asynchronous TCP connection under
    /// a Tokio timeout. Numeric IPs use `connect_timeout` in a blocking worker;
    /// for loopback IPs, an occupied port is accepted after a bind check without
    /// verifying that the listener is an object store. This is a transport
    /// reachability check, not an authentication, bucket, or S3-protocol check.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed storage configuration containing the backend and
    ///   optional endpoint URL.
    /// - `timeout_duration`: Maximum connection duration. For loopback ports
    ///   already found to be occupied, the function returns before using it.
    ///
    /// # Returns
    ///
    /// `Ok(())` when no probe is applicable or the configured transport appears
    /// reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Config`] for a malformed endpoint, invalid port,
    /// refused or timed-out connection, or failure to join the blocking probe
    /// task. The function does not build a fallback endpoint.
    ///
    /// # Side Effects
    ///
    /// May bind a loopback address temporarily and may open one TCP connection.
    /// Numeric non-loopback probes spawn blocking work on Tokio's blocking pool.
    ///
    /// # Performance
    ///
    /// Performs no object-store request. At most one socket probe is attempted.
    ///
    /// # Examples
    ///
    /// ```text
    /// local backend                         -> no probe
    /// S3 with no custom endpoint            -> no probe; AWS resolution happens later
    /// S3 endpoint http://127.0.0.1:9000     -> checks whether the loopback port is occupied
    /// S3 endpoint http://minio:9000         -> resolves and connects within the timeout
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `let Some(endpoint) = ... else` narrows an optional value and returns
    /// early when it is absent. Rust then knows `endpoint` is a valid `&str` for
    /// the rest of the function. `spawn_blocking` moves the numeric socket
    /// address and timeout into work that may block, preventing that work from
    /// stalling an async executor thread; Java commonly uses a separate executor
    /// for the same reason, while C event loops require an equivalent worker
    /// handoff to be designed manually.
    pub async fn probe_configured_endpoint(
        config: &StorageConfig,
        timeout_duration: Duration,
    ) -> Result<()> {
        let Some(endpoint) = configured_probe_endpoint(config) else {
            return Ok(());
        };
        let (host, port) = endpoint_host_port(endpoint)?;
        if let Ok(ip) = host.parse::<IpAddr>() {
            let addr = SocketAddr::new(ip, port);
            if ip.is_loopback() {
                match std::net::TcpListener::bind(addr) {
                    Ok(listener) => {
                        drop(listener);
                        return Err(ZeppelinError::Config(format!(
                            "storage endpoint {endpoint} is unreachable at {host}:{port}: no listener on loopback port"
                        )));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => return Ok(()),
                    Err(_error) => {}
                }
            }
            match tokio::task::spawn_blocking(move || {
                std::net::TcpStream::connect_timeout(&addr, timeout_duration)
            })
            .await
            {
                Ok(Ok(_stream)) => Ok(()),
                Ok(Err(error)) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} is unreachable at {host}:{port}: {error}"
                ))),
                Err(error) => Err(ZeppelinError::Config(format!(
                    "storage endpoint probe task failed: {error}"
                ))),
            }
        } else {
            match tokio::time::timeout(
                timeout_duration,
                tokio::net::TcpStream::connect((host.as_str(), port)),
            )
            .await
            {
                Ok(Ok(_stream)) => Ok(()),
                Ok(Err(error)) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} is unreachable at {host}:{port}: {error}"
                ))),
                Err(_elapsed) => Err(ZeppelinError::Config(format!(
                    "storage endpoint {endpoint} did not accept a connection within {}s",
                    timeout_duration.as_secs()
                ))),
            }
        }
    }

    /// Wraps an already constructed backend, primarily for tests and instrumentation.
    ///
    /// # Parameters
    ///
    /// - `store`: Shared ownership of a backend implementing [`ObjectStore`].
    ///   The caller may retain another [`Arc`] to the same backend.
    ///
    /// # Returns
    ///
    /// A gateway that delegates to the supplied backend without changing its
    /// retry, consistency, or transport behavior.
    ///
    /// # Examples
    ///
    /// An integration test can wrap an in-memory store in a GET-counting
    /// implementation and pass its `Arc<dyn ObjectStore>` here. Production code
    /// normally uses [`Self::from_config`] instead; this constructor declares
    /// the `InMemory` capability column, which is what its test callers wrap.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::new_with_capabilities(store, StorageCapabilities::in_memory())
    }

    /// Wraps an already constructed backend under an explicit capability matrix.
    ///
    /// For wrappers around a non-`InMemory` backend (an instrumented MinIO
    /// store, a fault decorator over a real substrate) whose declared
    /// capabilities must match what the wrapped backend actually does.
    pub fn new_with_capabilities(
        store: Arc<dyn ObjectStore>,
        capabilities: StorageCapabilities,
    ) -> Self {
        let prefix_delete_mode = if capabilities.native_batch_delete {
            PrefixDeleteMode::NativeBatch
        } else {
            PrefixDeleteMode::LegacyPerKeyUnordered32
        };
        Self {
            inner: store,
            capabilities,
            prefix_delete_mode,
            content_hashes: Arc::new(Mutex::new(ContentHashCache::default())),
            object_signer: Arc::new(RwLock::new(ObjectSignerBinding::default())),
        }
    }

    /// Clone this gateway for authority-side reads without inheriting application signing.
    ///
    /// Long-lived policy, signer, and preservation caches need the same authoritative
    /// object-store view, but do not need application signing capability. The application
    /// slot is also weak, so neither view can retain those caches after their security root ends.
    /// The clone also resets any installed signer-inventory view, preventing a
    /// self-reference through the application store's shared binding.
    #[must_use]
    pub(crate) fn signer_detached_clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capabilities: self.capabilities,
            prefix_delete_mode: self.prefix_delete_mode,
            content_hashes: Arc::clone(&self.content_hashes),
            object_signer: Arc::new(RwLock::new(ObjectSignerBinding::default())),
        }
    }

    /// Install the one node signer used by durable audit anchors.
    ///
    /// The slot borrows the live security root. Reinstalling the same node
    /// rebinds it to a replacement root during crash recovery; a different
    /// live node remains a configuration error. Once the bound root ends, an
    /// installed signer becomes an explicit lifecycle error rather than keeping
    /// security caches alive through otherwise disposable store clones.
    pub(crate) fn install_object_signer(&self, signer: Arc<dyn ObjectSigner>) -> Result<()> {
        let mut binding = self
            .object_signer
            .write()
            .unwrap_or_else(|_| panic!("object signer lock poisoned"));
        if let Some(existing) = binding.signer.as_ref().and_then(Weak::upgrade) {
            if existing.signer_node() != signer.signer_node() {
                return Err(ZeppelinError::Config(
                    "object signer was initialized with different node key material".to_string(),
                ));
            }
        }
        let inventory_store = signer.publication_store().signer_detached_clone();
        binding.signer = Some(Arc::downgrade(&signer));
        binding.inventory_view = SignerInventoryView::Installed(inventory_store);
        Ok(())
    }

    fn resolve_object_signer(&self) -> Result<Option<Arc<dyn ObjectSigner>>> {
        let signer = self
            .object_signer
            .read()
            .unwrap_or_else(|_| panic!("object signer lock poisoned"))
            .signer
            .clone();
        signer
            .map(|signer| {
                signer.upgrade().ok_or_else(|| {
                    ZeppelinError::Config(
                        "object signer root ended while the application store remains live"
                            .to_string(),
                    )
                })
            })
            .transpose()
    }

    /// Return the explicit S3 view selected for published-signature verification.
    ///
    /// An installed signer contributes a detached copy of its publication view
    /// at installation time. That copy carries only object-store wrapper state,
    /// not the live signer, authority, or policy cache. A fresh detached store
    /// deliberately uses its own caller view rather than choosing an alternate
    /// store at verification time.
    #[must_use]
    pub(crate) fn signer_inventory_store(&self) -> Self {
        let inventory_view = self
            .object_signer
            .read()
            .unwrap_or_else(|_| panic!("object signer lock poisoned"))
            .inventory_view
            .clone();
        match inventory_view {
            SignerInventoryView::CallerStore => self.signer_detached_clone(),
            SignerInventoryView::Installed(store) => store,
        }
    }

    /// Return the SHA-256 computed from an exact body accepted by a prior local PUT.
    #[must_use]
    pub(crate) fn known_content_hash(&self, key: &str) -> Option<[u8; 32]> {
        self.content_hashes
            .lock()
            .unwrap_or_else(|_| panic!("content-hash cache lock poisoned"))
            .entries
            .get(key)
            .copied()
    }

    /// Consume transient hashes after the manifest that owns them is published.
    pub(crate) fn forget_known_content_hashes<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a String>,
    ) {
        let mut cache = self
            .content_hashes
            .lock()
            .unwrap_or_else(|_| panic!("content-hash cache lock poisoned"));
        for key in keys {
            cache.entries.remove(key);
        }
    }

    /// Sign one canonical domain payload with the published node key.
    pub(crate) fn object_signer_node(&self) -> Result<Option<String>> {
        Ok(self
            .resolve_object_signer()?
            .map(|signer| signer.signer_node().to_string()))
    }

    /// Sign one canonical domain payload with the published node key.
    pub(crate) fn sign_object(&self, message: &[u8]) -> Result<Option<(String, Vec<u8>)>> {
        Ok(self
            .resolve_object_signer()?
            .map(|signer| (signer.signer_node().to_string(), signer.sign(message))))
    }

    /// Returns a shared handle to the raw backend for test instrumentation.
    ///
    /// Production domain code must not use this escape hatch: bypassing
    /// `ZeppelinStore` also bypasses its error normalization, metrics, range
    /// validation, and conditional-operation vocabulary.
    ///
    /// # Returns
    ///
    /// A newly cloned [`Arc`] pointing at the same backend. Cloning increments
    /// the strong reference count; it does not clone the remote client or data.
    ///
    /// # Examples
    ///
    /// A test can obtain this handle, put a fault-injecting `ObjectStore` around
    /// it, and pass the wrapped backend to [`Self::new`].
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Arc::clone` makes shared ownership visible at the call site. It resembles
    /// copying a Java reference with an explicit atomic retain, or incrementing a
    /// C reference count, but Rust automatically releases it and prevents a
    /// dangling backend pointer.
    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    /// Writes a complete byte payload to an object key.
    ///
    /// This is an ordinary unconditional PUT. It is suitable for new immutable
    /// artifact keys, but the storage API itself does not prevent replacement if
    /// a caller reuses a key. Artifact visibility remains controlled by the
    /// manifest rather than by this successful upload.
    ///
    /// # Parameters
    ///
    /// - `key`: Object-store key, using object-key path rules rather than HTTP
    ///   URL-segment rules.
    /// - `data`: Owned immutable payload. Moving [`Bytes`] into this method is
    ///   cheap because it transfers a shared buffer descriptor.
    ///
    /// # Returns
    ///
    /// The backend-provided ETag after it atomically accepts the complete
    /// payload. Backends that omit an ETag return `None` without fabricating an
    /// identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the key cannot be parsed or the backend rejects or
    /// cannot complete the write. The function does not silently write elsewhere.
    ///
    /// # Side Effects
    ///
    /// Performs one logical object-store PUT and records successful operation
    /// latency. The configured S3 backend may retry transient failures internally.
    ///
    /// # Consistency
    ///
    /// The underlying [`ObjectStore`] contract publishes the payload atomically,
    /// so readers do not observe a partial object. A successful write alone does
    /// not publish the object through a namespace manifest.
    ///
    /// # Performance
    ///
    /// Uploads the entire payload and allocates no second application-level copy.
    ///
    /// # Examples
    ///
    /// A compactor uploads `namespaces/acme/segments/42.cvec`. The object now
    /// exists, but queries continue to use the previous segment set until a
    /// later conditional manifest update references it.
    #[instrument(skip(self, data), fields(key = key, size = data.len()))]
    pub async fn put(&self, key: &str, data: Bytes) -> Result<Option<String>> {
        let content_hash = Sha256::digest(&data).into();
        let result = self.put_result(key, data).await?;
        self.content_hashes
            .lock()
            .unwrap_or_else(|_| panic!("content-hash cache lock poisoned"))
            .insert(key.to_string(), content_hash);
        Ok(result.e_tag)
    }

    /// Creates an immutable object and refuses to replace an existing key.
    ///
    /// Unlike [`Self::put_if_not_exists`], this primitive is not coupled to
    /// namespace creation and therefore does not translate a collision into
    /// [`ZeppelinError::NamespaceAlreadyExists`]. Audit batches and other
    /// write-once control artifacts can inspect the original
    /// [`object_store::Error::AlreadyExists`] through
    /// [`ZeppelinError::Storage`] without inventing namespace context.
    ///
    /// # Parameters
    ///
    /// - `key`: Destination object key, which must not already exist.
    /// - `data`: Complete immutable payload to create.
    ///
    /// # Returns
    ///
    /// `Ok(())` only after the backend accepts the create-only PUT.
    ///
    /// # Errors
    ///
    /// Invalid keys remain [`ZeppelinError::StoragePath`]. Every backend
    /// failure remains [`ZeppelinError::Storage`], including an
    /// [`object_store::Error::AlreadyExists`] collision. No collision is
    /// reported as success and the existing bytes are never overwritten.
    ///
    /// # Side Effects
    ///
    /// Performs exactly one create-only PUT. A backend failure increments the
    /// PUT error counter; success records PUT latency.
    ///
    /// # Consistency
    ///
    /// `PutMode::Create` is an atomic backend precondition, not a racy HEAD
    /// followed by an unconditional PUT. S3 implements it with
    /// `If-None-Match: *` because conditional PUT support is enabled when the
    /// client is constructed.
    #[instrument(skip(self, data), fields(key = key, size = data.len()))]
    pub async fn put_create(&self, key: &str, data: Bytes) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        self.inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
            .map_err(|error| {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["put"])
                    .inc();
                ZeppelinError::Storage(error)
            })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "object-store put_create");
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Atomically creates an object and classifies an existing destination.
    ///
    /// This is the domain-neutral create primitive for authoritative
    /// control-plane heads and immutable artifacts. It performs exactly one
    /// `PutMode::Create` request and never translates a collision into a
    /// namespace-specific error.
    #[instrument(skip(self, data), fields(key = key, size = data.len()))]
    pub async fn put_create_outcome(&self, key: &str, data: Bytes) -> Result<CreateOnlyOutcome> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let outcome = match self
            .inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
        {
            Ok(result) => CreateOnlyOutcome::Created {
                version: StorageVersion::from_parts(result.e_tag, result.version),
            },
            Err(object_store::Error::AlreadyExists { .. }) => CreateOnlyOutcome::AlreadyExists,
            Err(error) => {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["put"])
                    .inc();
                return Err(ZeppelinError::Storage(error));
            }
        };
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "object-store put_create_outcome"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(outcome)
    }

    /// Writes one object and preserves the backend identity returned by PUT.
    ///
    /// This crate-visible variant performs the same single physical request and
    /// metrics work as [`Self::put`]. Maintenance code uses the returned opaque
    /// identity to update an already-observed inventory without an extra HEAD or
    /// LIST. An absent identity remains `None` and cannot authorize later reuse.
    pub(crate) async fn put_with_version(
        &self,
        key: &str,
        data: Bytes,
    ) -> Result<Option<StorageVersion>> {
        let result = self.put_result(key, data).await?;
        Ok(StorageVersion::from_parts(result.e_tag, result.version))
    }

    /// Performs the shared ordinary-PUT request and instrumentation once.
    async fn put_result(&self, key: &str, data: Bytes) -> Result<object_store::PutResult> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.put(&path, PutPayload::from(data)).await?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "object-store put");
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(result)
    }

    /// Downloads the complete object stored at a key.
    ///
    /// # Parameters
    ///
    /// - `key`: Key for an authoritative object or an immutable artifact already
    ///   discovered through authoritative metadata.
    ///
    /// # Returns
    ///
    /// Owned [`Bytes`] containing the full object body. The buffer may be shared
    /// cheaply by cloning it after return.
    ///
    /// # Errors
    ///
    /// Maps a missing object to [`ZeppelinError::NotFound`]. Invalid keys, request
    /// failures, and body-download failures remain explicit storage errors; no
    /// cached or empty value is substituted.
    ///
    /// # Side Effects
    ///
    /// Performs one logical full-object GET, increments the GET error counter
    /// when the initial request fails, and records latency on success.
    ///
    /// # Consistency
    ///
    /// This method reads the configured backend directly. Higher-level cache
    /// code may call it after a miss, but cache contents never override the
    /// authoritative bytes returned here.
    ///
    /// # Performance
    ///
    /// Transfers and retains the entire object in memory. Query code should use
    /// [`Self::get_range`] or [`Self::get_ranges`] when it needs only selected
    /// index regions.
    ///
    /// # Examples
    ///
    /// Reading an existing 4 KiB manifest returns all 4 KiB. If the referenced
    /// key is absent, the caller receives `NotFound` and must treat the missing
    /// authoritative state according to its own contract rather than continuing
    /// with an empty manifest.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get(&path).await.map_err(|e| {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            "object-store get"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(bytes)
    }

    /// Downloads one non-empty, half-open byte range from an object.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key to read.
    /// - `range`: Zero-based byte offsets `start..end`; `start` is included and
    ///   `end` is excluded. The range must contain at least one byte.
    ///
    /// # Returns
    ///
    /// [`Bytes`] containing exactly the range returned by the backend.
    ///
    /// # Errors
    ///
    /// Empty or reversed ranges are rejected before network I/O. A missing key
    /// becomes [`ZeppelinError::NotFound`]; invalid keys, out-of-bounds ranges,
    /// and transport failures remain storage errors.
    ///
    /// # Side Effects
    ///
    /// Performs one logical ranged GET and records GET error and duration
    /// metrics in the same way as [`Self::get`].
    ///
    /// # Performance
    ///
    /// Transfers only the selected region. This is a core query-path primitive
    /// for reading cluster or vector blocks without downloading a whole segment.
    ///
    /// # Examples
    ///
    /// For a segment object whose header occupies bytes `0..128`, requesting
    /// `0..128` returns those 128 bytes. Requests `128..128` and `200..100`
    /// fail locally and issue no object-store request.
    #[instrument(skip(self), fields(key = key, range_start = range.start, range_end = range.end))]
    pub async fn get_range(&self, key: &str, range: Range<usize>) -> Result<Bytes> {
        if range.start >= range.end {
            return Err(ZeppelinError::Storage(object_store::Error::Generic {
                store: "zeppelin",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "invalid empty or reversed range for {key}: {}..{}",
                        range.start, range.end
                    ),
                )),
            }));
        }

        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get_range(&path, range).await.map_err(|e| {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = result.len(),
            "object-store get_range"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(result)
    }

    /// Downloads multiple requested byte ranges from one object.
    ///
    /// The returned vector preserves the input range order. The underlying
    /// `object_store` implementation may coalesce nearby ranges into fewer
    /// physical requests; this wrapper exposes the logical result, not a fixed
    /// physical GET count.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key shared by all requested ranges.
    /// - `ranges`: Borrowed half-open byte ranges. This wrapper delegates range
    ///   validation to `object_store` for the batched form.
    ///
    /// # Returns
    ///
    /// One [`Bytes`] value per input range in corresponding order. An empty
    /// input produces an empty vector if the backend accepts it.
    ///
    /// # Errors
    ///
    /// Maps a missing key to [`ZeppelinError::NotFound`]. Invalid keys or ranges
    /// and backend failures are returned without a partial result vector.
    ///
    /// # Side Effects
    ///
    /// Issues the backend's multi-range read plan and records total returned
    /// bytes and logical GET latency. A failed initial request increments the
    /// GET error counter.
    ///
    /// # Performance
    ///
    /// Can reduce request overhead compared with independent range reads because
    /// `object_store` coalesces suitable ranges. It still retains every returned
    /// range in memory at once.
    ///
    /// # Examples
    ///
    /// A rerank stage can request vector regions `[0..128, 1024..1152]`. The
    /// result contains two buffers in that order even if the backend fetches one
    /// wider physical region and slices it internally.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&[Range<usize>]` is a borrowed slice: the method can inspect the caller's
    /// contiguous range array during the call but cannot resize, free, or retain
    /// it beyond the borrow. It resembles a Java array reference plus a read-only
    /// API, or `const Range *` with a length in C, with compiler-checked bounds
    /// and lifetime.
    #[instrument(skip(self, ranges), fields(key = key, ranges = ranges.len()))]
    pub async fn get_ranges(&self, key: &str, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get_ranges(&path, ranges).await.map_err(|e| {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let elapsed = start.elapsed();
        let size: usize = result.iter().map(Bytes::len).sum();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size,
            ranges = ranges.len(),
            "object-store get_ranges"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(result)
    }

    /// Downloads an object together with the backend version tag used for CAS.
    ///
    /// Higher layers use the returned ETag as the version they observed, then
    /// pass it to [`Self::put_if_match`] when publishing a replacement. The body
    /// and metadata come from the same backend GET result.
    ///
    /// # Parameters
    ///
    /// - `key`: Key of the object whose body and version metadata are required.
    ///
    /// # Returns
    ///
    /// A pair of full object bytes and an optional ETag. `None` means the backend
    /// supplied no ETag; callers requiring CAS cannot invent one.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Key parsing, request,
    /// and body-download failures are returned explicitly.
    ///
    /// # Side Effects
    ///
    /// Performs one full-object GET and records GET metrics on the same paths as
    /// [`Self::get`].
    ///
    /// # Consistency
    ///
    /// The ETag identifies the version returned by this request. A later
    /// [`Self::put_if_match`] uses it to prevent a stale caller from overwriting
    /// a newer authoritative manifest or lease.
    ///
    /// # Examples
    ///
    /// ```text
    /// read manifest bytes + ETag "v12"
    ///                 |
    ///                 v
    /// derive replacement manifest
    ///                 |
    ///                 v
    /// put_if_match(..., "v12") -> succeeds only if v12 is still current
    /// ```
    #[instrument(skip(self), fields(key = key))]
    pub async fn get_with_meta(&self, key: &str) -> Result<(Bytes, Option<StorageVersion>)> {
        let (bytes, metadata) = self.get_with_object_metadata(key).await?;
        Ok((bytes, metadata.version))
    }

    /// Downloads an object with its ETag and user-defined metadata headers.
    ///
    /// This is the typed storage seam for domain identities that must accompany
    /// an object body without becoming part of its serialized bytes. The body,
    /// ETag, and user metadata all come from the same authoritative GET.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get_with_object_metadata(&self, key: &str) -> Result<(Bytes, ObjectReadMetadata)> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = self.inner.get(&path).await.map_err(|e| {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["get"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let version =
            StorageVersion::from_parts(result.meta.e_tag.clone(), result.meta.version.clone());
        let user_metadata = ObjectUserMetadata::from_attributes(&result.attributes);
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            version = ?version,
            "object-store get_with_meta"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok((
            bytes,
            ObjectReadMetadata {
                version,
                user_metadata,
            },
        ))
    }

    /// Downloads an object only when its current ETag differs from a known version.
    ///
    /// This supports cache revalidation without treating the cache as
    /// authoritative. `NotModified` and `Precondition` responses are both
    /// interpreted as "the supplied version is still current" because supported
    /// S3-compatible stores use either representation for `If-None-Match`.
    ///
    /// # Parameters
    ///
    /// - `key`: Object to revalidate against authoritative storage.
    /// - `etag`: Version already held by the caller. The value is copied into the
    ///   request options and must use the backend's expected ETag representation.
    ///
    /// # Returns
    ///
    /// `None` when the object has not changed. `Some((bytes, next_etag))` contains
    /// the complete current object and its optional new ETag when it changed.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Other request, key,
    /// and body-download failures are returned explicitly and do not cause a
    /// caller to keep using cached data as if it had been revalidated.
    ///
    /// # Side Effects
    ///
    /// Performs one conditional full-object GET. Unchanged and changed responses
    /// both record successful duration; request failures increment the GET error
    /// counter.
    ///
    /// # Consistency
    ///
    /// `None` is evidence from the object store, not a cache-only decision. A
    /// caller may retain its cached bytes only after receiving that response.
    ///
    /// # Performance
    ///
    /// An unchanged response avoids transferring the object body. A changed
    /// response transfers and retains the complete body.
    ///
    /// # Examples
    ///
    /// A manifest cache holding ETag `v12` asks for the same key. If S3 still has
    /// `v12`, this returns `None`; if S3 has `v13`, it returns the `v13` bytes and
    /// ETag; if S3 is unavailable, it returns an error rather than declaring the
    /// cached `v12` authoritative.
    ///
    /// # Substrates without an ETag
    ///
    /// `If-None-Match` is ETag-defined on every substrate Zeppelin targets, so a
    /// token carrying only a backend version cannot express this request. Rather
    /// than fail, this method falls back to an unconditional
    /// [`Self::get_with_object_metadata`] and compares the returned token
    /// locally, returning `None` when it is unchanged. That is a full body
    /// transfer where a conditional GET would have transferred nothing: correct,
    /// more expensive, and visible in the GET duration metric.
    ///
    /// This is the **only** place in the storage seam where an absent token
    /// field degrades to a more expensive correct operation instead of raising
    /// an error, and it is deliberate: revalidation is a bandwidth optimization,
    /// not a correctness gate. Every conditional *write* raises instead, because
    /// there the missing token would cost correctness.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The nested `Option<(Bytes, Option<StorageVersion>)>` separates two
    /// independent facts: whether a body was transferred and whether that
    /// response carried a backend identity. Unlike `null`, Rust forces callers
    /// to handle each absence. The `match` also groups two concrete backend
    /// errors into the same deliberate unchanged outcome while preserving every
    /// other error.
    #[instrument(skip(self), fields(key = key))]
    pub async fn get_if_none_match(
        &self,
        key: &str,
        version: &StorageVersion,
    ) -> Result<Option<(Bytes, Option<StorageVersion>)>> {
        let Some(etag) = version.etag() else {
            return self.revalidate_without_etag(key, version).await;
        };
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = GetOptions {
            if_none_match: Some(etag.to_string()),
            ..GetOptions::default()
        };

        let result = match self.inner.get_opts(&path, options).await {
            Ok(result) => result,
            Err(
                object_store::Error::NotModified { .. } | object_store::Error::Precondition { .. },
            ) => {
                let elapsed = start.elapsed();
                debug!(
                    elapsed_ms = elapsed.as_millis(),
                    etag = %etag,
                    "object-store get_if_none_match not modified"
                );
                crate::metrics::STORAGE_OPERATION_DURATION
                    .with_label_values(&["get"])
                    .observe(elapsed.as_secs_f64());
                return Ok(None);
            }
            Err(e) => {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["get"])
                    .inc();
                return Err(match e {
                    object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                        key: path.to_string(),
                    },
                    other => ZeppelinError::Storage(other),
                });
            }
        };

        let next_version =
            StorageVersion::from_parts(result.meta.e_tag.clone(), result.meta.version.clone());
        let bytes = result.bytes().await?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            size = bytes.len(),
            version = ?next_version,
            "object-store get_if_none_match modified"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["get"])
            .observe(elapsed.as_secs_f64());
        Ok(Some((bytes, next_version)))
    }

    /// Revalidates by full GET when the observed token has no ETag to send.
    ///
    /// Used only by [`Self::get_if_none_match`]; see its documentation for why
    /// this path exists and why it is the sole degradation in the seam.
    async fn revalidate_without_etag(
        &self,
        key: &str,
        version: &StorageVersion,
    ) -> Result<Option<(Bytes, Option<StorageVersion>)>> {
        let (bytes, metadata) = self.get_with_object_metadata(key).await?;
        if metadata.version.as_ref() == Some(version) {
            debug!(key = key, "storage revalidate unchanged without etag");
            return Ok(None);
        }
        Ok(Some((bytes, metadata.version)))
    }

    /// Replaces an object only when its backend identity still matches.
    ///
    /// This is the storage half of compare-and-swap publication. Manifest and
    /// lease code first reads an object and its version token, derives a
    /// replacement, and then calls this method. A competing update changes the
    /// token and turns this request into an explicit conflict instead of a lost
    /// update.
    ///
    /// The token carries every identity form the backend reported and the
    /// backend selects the one its protocol requires — ETag on S3, MinIO and
    /// Azure, object generation on GCS. Taking `&StorageVersion` rather than a
    /// string makes the empty-token case unrepresentable: a caller that observed
    /// no identity holds `None` and cannot reach this method at all.
    ///
    /// # Parameters
    ///
    /// - `key`: Object key to replace conditionally.
    /// - `data`: Complete owned replacement payload.
    /// - `version`: Identity on which the caller based the replacement.
    /// - `namespace`: Domain namespace reported if the precondition loses a race.
    ///
    /// # Returns
    ///
    /// The new backend identity after the conditional replacement succeeds. A
    /// backend may legally report none, represented as `None`.
    ///
    /// # Errors
    ///
    /// A precondition failure becomes [`ZeppelinError::ManifestConflict`],
    /// telling the caller to reload and rebase. A backend that requires an
    /// identity form this token does not carry fails inside object_store and
    /// surfaces as a storage error rather than an unconditional write. Invalid
    /// keys and other backend failures remain storage errors. The old
    /// authoritative object remains current after a conflict.
    ///
    /// # Side Effects
    ///
    /// Performs one conditional PUT and records PUT latency on success. Non-CAS
    /// backend failures increment the PUT error counter.
    ///
    /// # Consistency
    ///
    /// S3 construction must retain `S3ConditionalPut::ETagMatch`. This method
    /// supplies CAS but does not perform a lease fencing check; manifest/lease
    /// callers are responsible for any additional protocol layer they require.
    ///
    /// # Performance
    ///
    /// Uploads one complete replacement object. The method performs no preceding
    /// read because the caller already supplies the observed identity, and no
    /// following read because it returns the new identity from the PUT result.
    ///
    /// # Examples
    ///
    /// ```text
    /// caller A and B both read ETag v12
    ///              |
    /// A PUT if v12 succeeds -> object becomes v13
    ///              |
    /// B PUT if v12 ---------> ManifestConflict; v13 is not overwritten
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Moving `Bytes` into the request transfers ownership of a small shared
    /// buffer handle; it does not imply a byte-for-byte clone. `map_err` converts
    /// the backend's error enum into Zeppelin's domain error while the `?`
    /// operator returns early on failure. Java would commonly throw and catch an
    /// exception here; C would need an explicit status plus disciplined cleanup.
    #[instrument(skip(self, data), fields(key = key))]
    pub async fn put_if_match(
        &self,
        key: &str,
        data: Bytes,
        version: &StorageVersion,
        namespace: &str,
    ) -> Result<Option<StorageVersion>> {
        self.put_if_match_with_user_metadata(
            key,
            data,
            version,
            namespace,
            &ObjectUserMetadata::new(),
        )
        .await
    }

    /// Replace an object only when its current identity matches, without
    /// attaching namespace-manifest semantics to a precondition loss.
    #[instrument(skip(self, data), fields(key = key))]
    pub async fn put_if_match_outcome(
        &self,
        key: &str,
        data: Bytes,
        version: &StorageVersion,
    ) -> Result<ConditionalPutOutcome> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Update(version.to_update_version()),
            ..PutOptions::default()
        };
        let outcome = match self
            .inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
        {
            Ok(result) => ConditionalPutOutcome::Updated {
                version: StorageVersion::from_parts(result.e_tag, result.version),
            },
            Err(object_store::Error::Precondition { .. }) => ConditionalPutOutcome::Conflict,
            Err(error) => {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["put"])
                    .inc();
                return Err(ZeppelinError::Storage(error));
            }
        };
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "object-store put_if_match_outcome"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(outcome)
    }

    /// Conditionally replaces an object while publishing user metadata.
    ///
    /// The metadata headers are part of the same atomic PUT as the body. No
    /// preliminary read or follow-up write is issued.
    #[instrument(skip(self, data, user_metadata), fields(key = key))]
    pub async fn put_if_match_with_user_metadata(
        &self,
        key: &str,
        data: Bytes,
        version: &StorageVersion,
        namespace: &str,
        user_metadata: &ObjectUserMetadata,
    ) -> Result<Option<StorageVersion>> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Update(version.to_update_version()),
            attributes: user_metadata
                .to_attributes(self.capabilities.user_metadata_identifier_names),
            ..PutOptions::default()
        };
        let result = self
            .inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
            .map_err(|e| match e {
                object_store::Error::Precondition { .. } => ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                },
                other => {
                    crate::metrics::STORAGE_ERRORS_TOTAL
                        .with_label_values(&["put"])
                        .inc();
                    ZeppelinError::Storage(other)
                }
            })?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "object-store put_if_match"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(StorageVersion::from_parts(result.e_tag, result.version))
    }

    /// Creates an object only when its key does not already exist.
    ///
    /// Namespace creation and first-time manifest publication use this operation
    /// to prevent a caller from replacing established authoritative state. For
    /// S3, `PutMode::Create` is implemented with `If-None-Match: *`.
    ///
    /// # Parameters
    ///
    /// - `key`: Destination object key.
    /// - `data`: Complete owned payload to create.
    /// - `namespace`: Domain name included in the already-exists error.
    ///
    /// # Returns
    ///
    /// `Ok(())` only when this call created the destination.
    ///
    /// # Errors
    ///
    /// An existing destination becomes
    /// [`ZeppelinError::NamespaceAlreadyExists`]. Invalid keys and other backend
    /// failures remain errors. The existing object's bytes are never replaced by
    /// the collision path.
    ///
    /// # Side Effects
    ///
    /// Performs one create-only PUT and records PUT latency on success. Backend
    /// failures other than an expected collision increment the PUT error metric.
    ///
    /// # Consistency
    ///
    /// This is an atomic create precondition supplied by the backend, not a
    /// racy `exists` check followed by [`Self::put`].
    ///
    /// # Examples
    ///
    /// The first request creating `acme/meta.json` succeeds. A concurrent request
    /// for the same namespace receives `NamespaceAlreadyExists`, and the first
    /// metadata object remains authoritative.
    #[instrument(skip(self, data), fields(key = key))]
    pub async fn put_if_not_exists(&self, key: &str, data: Bytes, namespace: &str) -> Result<()> {
        self.put_if_not_exists_with_user_metadata(key, data, namespace, &ObjectUserMetadata::new())
            .await
    }

    /// Creates an object and atomically attaches user-defined metadata.
    ///
    /// The create-only precondition, body, and metadata headers share one
    /// backend request, so a losing creator cannot replace any of them.
    #[instrument(skip(self, data, user_metadata), fields(key = key))]
    pub async fn put_if_not_exists_with_user_metadata(
        &self,
        key: &str,
        data: Bytes,
        namespace: &str,
        user_metadata: &ObjectUserMetadata,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            attributes: user_metadata
                .to_attributes(self.capabilities.user_metadata_identifier_names),
            ..PutOptions::default()
        };
        self.inner
            .put_opts(&path, PutPayload::from(data), options)
            .await
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { path, .. } => {
                    tracing::debug!(key = %path, "put_if_not_exists: object already exists");
                    ZeppelinError::NamespaceAlreadyExists {
                        namespace: namespace.to_string(),
                    }
                }
                other => {
                    crate::metrics::STORAGE_ERRORS_TOTAL
                        .with_label_values(&["put"])
                        .inc();
                    ZeppelinError::Storage(other)
                }
            })?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "object-store put_if_not_exists"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["put"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Copies an object within the backend without replacing an existing destination.
    ///
    /// Namespace clone materialization uses this to copy immutable artifacts
    /// while preserving collision safety at the target. The source remains in
    /// place; this is a copy, not a rename, and it does not publish a target
    /// manifest by itself.
    ///
    /// # Parameters
    ///
    /// - `from`: Existing source object key.
    /// - `to`: Destination key that must be absent.
    /// - `namespace`: Target namespace context retained by the public API. The
    ///   current implementation does not use it to rewrite backend errors.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the destination was created from the source.
    ///
    /// # Errors
    ///
    /// Returns a storage error if either key is invalid, the source is missing,
    /// the destination exists, or the backend cannot complete its configured
    /// copy-if-absent operation. A failed clone workflow may already have copied
    /// other keys and must clean them up at its orchestration layer.
    ///
    /// # Side Effects
    ///
    /// Performs a server-side copy operation within the configured store and
    /// records copy latency on success. Failures increment the copy error metric.
    ///
    /// # Consistency
    ///
    /// The destination precondition is delegated to the backend. S3 construction
    /// explicitly enables the multipart copy-if-not-exists strategy. This method
    /// neither deletes the source nor changes manifest visibility.
    ///
    /// # Examples
    ///
    /// Cloning a snapshot may copy source segment `segments/42.cvec` to a fresh
    /// target key. If that target key already exists, the copy fails rather than
    /// replacing an artifact that may belong to another clone attempt.
    #[instrument(skip(self), fields(from = from, to = to))]
    pub async fn copy_if_not_exists(&self, from: &str, to: &str, namespace: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let from_path = Path::parse(from)?;
        let to_path = Path::parse(to)?;
        self.inner
            .copy_if_not_exists(&from_path, &to_path)
            .await
            .map_err(|e| {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["copy"])
                    .inc();
                ZeppelinError::Storage(e)
            })?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "object-store copy_if_not_exists"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["copy"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Deletes one object key from the configured backend.
    ///
    /// Callers must establish that deletion is safe. This low-level method does
    /// not check manifest reachability, GC horizons, snapshots, or namespace
    /// tombstones.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key to remove.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the backend accepts the deletion.
    ///
    /// # Errors
    ///
    /// Invalid keys and delete failures are errors. Deleting an absent key is
    /// **success on every backend**: S3 reports success natively, and the seam
    /// normalizes the `NotFound` that GCS, Azure, Local, and InMemory report
    /// (per `StorageCapabilities::delete_absent_is_ok`) so GC's drain
    /// idempotency holds identically on all substrates. This is a single
    /// documented contract, not a fallback; the raw substrate behavior is
    /// still verified at boot by [`Self::verify_declared_capabilities`].
    ///
    /// # Side Effects
    ///
    /// Performs one logical DELETE and records delete latency on success.
    ///
    /// # Consistency
    ///
    /// Removing an object still referenced by an authoritative manifest violates
    /// Zeppelin's artifact invariant. Reachability-aware callers must update or
    /// inspect authoritative state before invoking this primitive.
    ///
    /// # Examples
    ///
    /// Garbage collection may delete an old segment only after proving no
    /// retained manifest or snapshot references it. Calling this method directly
    /// on the active segment would remove authoritative data and is unsafe.
    #[instrument(skip(self), fields(key = key))]
    pub async fn delete(&self, key: &str) -> Result<()> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        match self.inner.delete(&path).await {
            Ok(()) => {}
            // Absence-is-success normalization: only backends whose
            // capability row says deletes of absent keys surface NotFound
            // reach this arm; S3 never does, so its behavior is unchanged.
            Err(object_store::Error::NotFound { .. }) if !self.capabilities.delete_absent_is_ok => {
            }
            Err(object_store::Error::NotFound { path, .. }) => {
                return Err(ZeppelinError::NotFound {
                    key: path.to_string(),
                })
            }
            Err(other) => return Err(ZeppelinError::Storage(other)),
        }
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "object-store delete");
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["delete"])
            .observe(elapsed.as_secs_f64());
        Ok(())
    }

    /// Deletes one bounded batch of exact object keys.
    ///
    /// Every key is parsed and the batch is checked for duplicates before the
    /// backend sees any input. The 1,000-key limit matches one S3 DeleteObjects
    /// request in the pinned object-store backend; callers with more work must
    /// split it into explicit batches so retry and progress semantics remain
    /// visible.
    ///
    /// The result is successful only after the backend returns one successful
    /// or matching `NotFound` outcome for every unique input key, in input
    /// order. Any other per-key or request error is returned after the stream is
    /// drained. A short, long, reordered, or wrong-key result stream is a
    /// storage error rather than silent partial completion.
    #[instrument(skip(self, keys), fields(count = keys.len()))]
    pub async fn delete_many(&self, keys: Vec<String>) -> Result<usize> {
        if keys.len() > DELETE_MANY_MAX_KEYS {
            return Err(ZeppelinError::Validation(format!(
                "delete_many accepts at most {DELETE_MANY_MAX_KEYS} keys, got {}",
                keys.len()
            )));
        }

        let paths = keys
            .iter()
            .map(Path::parse)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let unique = paths.iter().cloned().collect::<BTreeSet<_>>();
        if unique.len() != paths.len() {
            return Err(ZeppelinError::Validation(
                "delete_many requires unique object keys".to_string(),
            ));
        }
        let expected = paths.len();
        if expected == 0 {
            return Ok(0);
        }

        let start = std::time::Instant::now();
        use futures::StreamExt;
        let input = futures::stream::iter(paths.iter().cloned().map(Ok)).boxed();
        let mut results = self.inner.delete_stream(input);
        let mut observed = 0usize;
        let mut first_error = None;
        while let Some(result) = results.next().await {
            let expected_path = paths.get(observed);
            match result {
                Ok(actual) if expected_path == Some(&actual) => {}
                Ok(actual) => {
                    first_error.get_or_insert_with(|| object_store::Error::Generic {
                        store: "ZeppelinStore",
                        source: Box::new(std::io::Error::other(format!(
                            "delete_many result {observed} returned unexpected key {actual}"
                        ))),
                    });
                }
                Err(object_store::Error::NotFound { path, .. })
                    if expected_path.is_some_and(|expected| {
                        // LocalFileSystem reports the absolute filesystem
                        // path rather than the object key; anchor the suffix
                        // on a separator so one key cannot satisfy another
                        // key's absence. Remote backends match exactly.
                        let expected = expected.as_ref();
                        path == expected || path.ends_with(&format!("/{expected}"))
                    }) => {}
                Err(object_store::Error::NotFound { path, source }) => {
                    first_error.get_or_insert(object_store::Error::NotFound { path, source });
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
            if let Some(next_observed) = observed.checked_add(1) {
                observed = next_observed;
            } else if first_error.is_none() {
                first_error = Some(object_store::Error::Generic {
                    store: "ZeppelinStore",
                    source: Box::new(std::io::Error::other(
                        "delete_many result count overflowed usize",
                    )),
                });
            }
        }

        if observed != expected && first_error.is_none() {
            first_error = Some(object_store::Error::Generic {
                store: "ZeppelinStore",
                source: Box::new(std::io::Error::other(format!(
                    "delete_many returned {observed} results for {expected} keys"
                ))),
            });
        }

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = expected,
            "object-store delete_many"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["delete_many"])
            .observe(elapsed.as_secs_f64());
        if let Some(error) = first_error {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["delete_many"])
                .inc();
            return Err(ZeppelinError::Storage(error));
        }

        Ok(expected)
    }

    /// Lists every object recursively beneath a non-empty prefix.
    ///
    /// The complete listing is materialized before return. Namespace discovery
    /// at the store root must use [`Self::list_common_prefixes`] instead, which
    /// avoids an unrestricted recursive scan.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty object-key prefix. Prefix matching follows
    ///   `object_store` path-segment semantics rather than raw string matching.
    ///
    /// # Returns
    ///
    /// Every matching object key. The backend does not guarantee ordering, so
    /// callers must sort when deterministic order matters.
    ///
    /// # Errors
    ///
    /// Invalid prefixes and any paginated listing failure return an error without
    /// exposing a partial key vector.
    ///
    /// # Panics
    ///
    /// An empty prefix panics in every build because unrestricted recursive root
    /// listing is forbidden. Use [`Self::list_common_prefixes`] for root namespace
    /// discovery.
    ///
    /// # Side Effects
    ///
    /// Performs a recursive object-store LIST, which may require multiple remote
    /// pages, and records total listing latency on success.
    ///
    /// # Performance
    ///
    /// Memory use is linear in the number of matching objects because both
    /// backend metadata and returned key strings are collected in memory.
    ///
    /// # Examples
    ///
    /// Listing `acme/segments/` can return every immutable segment object below
    /// that path. Listing `""` is rejected; root namespace discovery should ask
    /// [`Self::list_common_prefixes`] for immediate children instead.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_prefix_meta_inner(prefix)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    /// Lists and classifies every object owned by one exact namespace prefix.
    ///
    /// Unlike a raw prefix listing, this seam proves each returned key belongs
    /// to the requested namespace and one known production family. Unknown or
    /// malformed control keys fail the complete listing closed.
    pub(crate) async fn list_namespace_objects(
        &self,
        namespace: &str,
    ) -> Result<Vec<NamespaceObjectKey>> {
        let prefix = namespace_prefix(namespace)?;
        self.list_prefix(&prefix)
            .await?
            .into_iter()
            .map(|key| NamespaceObjectKey::classify(namespace, key))
            .collect()
    }

    /// Lists every object and its backend metadata beneath a non-empty prefix.
    ///
    /// This performs the same one logical recursive LIST as
    /// [`Self::list_prefix`] while preserving the object identity needed to
    /// validate disposable caches. Backend pagination may issue multiple remote
    /// requests. An absent ETag and backend version produces `version: None`;
    /// Zeppelin never invents a version token.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_prefix_meta(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        self.list_prefix_meta_inner(prefix).await
    }

    async fn list_prefix_meta_inner(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        assert!(
            !prefix.is_empty(),
            "recursive root listing must use list_common_prefixes"
        );

        let start = std::time::Instant::now();
        use futures::TryStreamExt;
        let path = Path::parse(prefix)?;
        let stream = self.inner.list(Some(&path));
        let objects: Vec<_> = stream.try_collect().await?;
        let objects = objects
            .into_iter()
            .map(|object| {
                let key = object.location.to_string();
                let size = u64::try_from(object.size).map_err(|_| {
                    ZeppelinError::Validation(format!(
                        "listed object {key} size does not fit in u64: {}",
                        object.size
                    ))
                })?;
                Ok(ListedObject {
                    key,
                    size,
                    last_modified: object.last_modified,
                    version: StorageVersion::from_parts(object.e_tag, object.version),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = objects.len(),
            "object-store list_prefix"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["list_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(objects)
    }

    /// Discovers unique immediate child prefixes without recursively returning all objects.
    ///
    /// In addition to backend-reported common prefixes, this method derives a
    /// child prefix from any returned object's first slash-delimited component.
    /// That normalization keeps root namespace discovery consistent across S3
    /// and local/test backends. Direct child objects without another slash do not
    /// themselves become prefixes.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Parent object-key prefix. The empty string intentionally means
    ///   the store root and is valid for namespace discovery.
    ///
    /// # Returns
    ///
    /// Lexicographically sorted, duplicate-free child prefixes, normally ending
    /// in `/` because they represent key hierarchy rather than concrete objects.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix cannot be parsed or the delimiter listing
    /// fails. No partial prefix set is returned.
    ///
    /// # Side Effects
    ///
    /// Performs one logical delimiter LIST and records its latency on success.
    ///
    /// # Performance
    ///
    /// Stores unique prefixes in a `BTreeSet`, using `O(n log n)` insertion to
    /// provide deterministic order without a separate sort.
    ///
    /// # Examples
    ///
    /// Given `acme/meta.json`, `acme/segments/1`, and `beta/meta.json`, listing
    /// immediate prefixes at `""` returns `acme/` and `beta/`, not every object
    /// below those namespaces.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `let Some(remainder) = ... else { continue; }` makes the successful
    /// optional parse path explicit and skips malformed/unrelated entries.
    /// `BTreeSet` owns inserted strings and combines uniqueness with sorted
    /// iteration. Java's `TreeSet<String>` is a close data-structure analogy; C
    /// needs a tree/set implementation plus explicit string ownership.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn list_common_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let start = std::time::Instant::now();
        let path = Path::parse(prefix)?;
        let result = self.inner.list_with_delimiter(Some(&path)).await?;
        let mut prefixes = std::collections::BTreeSet::new();
        for common_prefix in &result.common_prefixes {
            prefixes.insert(common_prefix.to_string());
        }
        for object in &result.objects {
            let key = object.location.to_string();
            let Some(remainder) = key.strip_prefix(prefix) else {
                continue;
            };
            let Some(delimiter_idx) = remainder.find('/') else {
                continue;
            };
            prefixes.insert(format!("{}{}", prefix, &remainder[..=delimiter_idx]));
        }
        let prefixes: Vec<String> = prefixes.into_iter().collect();
        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = prefixes.len(),
            "object-store list_common_prefixes"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["list_common_prefixes"])
            .observe(elapsed.as_secs_f64());
        Ok(prefixes)
    }

    /// Checks object existence without downloading its body.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key to test.
    ///
    /// # Returns
    ///
    /// `true` after a successful HEAD and `false` only when the backend reports
    /// `NotFound`.
    ///
    /// # Errors
    ///
    /// Invalid keys and all non-`NotFound` backend errors are returned. A timeout
    /// is not misreported as absence.
    ///
    /// # Side Effects
    ///
    /// Performs one metadata request, records existence-check latency, and
    /// increments the `exists` error counter for backend failures.
    ///
    /// # Consistency
    ///
    /// Existence does not imply manifest visibility. This method is useful for
    /// validation and cleanup, not for deciding which artifacts queries may read.
    ///
    /// # Examples
    ///
    /// A freshly uploaded but not yet published segment returns `true`; queries
    /// must still ignore it until the authoritative manifest references it.
    #[instrument(skip(self), fields(key = key))]
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let result = match self.inner.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => {
                crate::metrics::STORAGE_ERRORS_TOTAL
                    .with_label_values(&["exists"])
                    .inc();
                Err(ZeppelinError::Storage(e))
            }
        };
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "object-store exists");
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["exists"])
            .observe(elapsed.as_secs_f64());
        result
    }

    /// Loads object metadata without downloading the body.
    ///
    /// # Parameters
    ///
    /// - `key`: Exact object key whose metadata is required.
    ///
    /// # Returns
    ///
    /// Backend [`object_store::ObjectMeta`], including location, size, modified
    /// time, and available version identifiers.
    ///
    /// # Errors
    ///
    /// Missing objects become [`ZeppelinError::NotFound`]. Invalid keys and other
    /// backend failures remain errors; metadata is never synthesized.
    ///
    /// # Side Effects
    ///
    /// Performs one HEAD-style metadata request, records latency on success, and
    /// increments the HEAD error metric if the request fails.
    ///
    /// # Performance
    ///
    /// Avoids body transfer but still incurs an object-store roundtrip.
    ///
    /// # Examples
    ///
    /// Cache hydration can inspect a segment's size before scheduling a download.
    /// It receives metadata only; the segment bytes remain in object storage.
    #[instrument(skip(self), fields(key = key))]
    pub async fn head(&self, key: &str) -> Result<ListedObject> {
        let start = std::time::Instant::now();
        let path = Path::parse(key)?;
        let meta = self.inner.head(&path).await.map_err(|e| {
            crate::metrics::STORAGE_ERRORS_TOTAL
                .with_label_values(&["head"])
                .inc();
            match e {
                object_store::Error::NotFound { path, .. } => ZeppelinError::NotFound {
                    key: path.to_string(),
                },
                other => ZeppelinError::Storage(other),
            }
        })?;
        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "object-store head");
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["head"])
            .observe(elapsed.as_secs_f64());
        let size = u64::try_from(meta.size).map_err(|_| {
            ZeppelinError::Validation(format!(
                "object {key} size does not fit in u64: {}",
                meta.size
            ))
        })?;
        Ok(ListedObject {
            key: meta.location.to_string(),
            size,
            last_modified: meta.last_modified,
            version: StorageVersion::from_parts(meta.e_tag, meta.version),
        })
    }

    /// Deletes every object recursively under a non-empty prefix.
    ///
    /// This convenience method runs [`Self::delete_prefix_paged`] with no
    /// excluded key and an unlimited time budget. It is intended for cleanup
    /// only after the caller has established that every matching object is safe
    /// to remove.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty key prefix that bounds the deletion.
    ///
    /// # Returns
    ///
    /// Number of listed keys deleted or found already absent.
    ///
    /// # Errors
    ///
    /// Empty prefixes, listing failures, malformed keys, and delete failures are
    /// returned. Some matching objects may already be gone when an error occurs.
    ///
    /// # Panics
    ///
    /// In debug builds, an empty prefix triggers the guard in
    /// [`Self::delete_prefix_paged`].
    ///
    /// # Side Effects
    ///
    /// Lists and deletes all matching objects in bounded backend batches with no
    /// overall time budget.
    ///
    /// # Consistency
    ///
    /// This method does not perform reachability analysis. Namespace deletion and
    /// GC must preserve tombstones, manifests, snapshots, and safety horizons in
    /// their own orchestration before using it.
    ///
    /// # Examples
    ///
    /// A test cleanup may remove its random namespace prefix after the test has
    /// finished. Production namespace deletion uses the paged form when it must
    /// preserve `meta.json` as a tombstone during incremental cleanup.
    #[instrument(skip(self), fields(prefix = prefix))]
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let outcome = self
            .delete_prefix_paged(prefix, None, Duration::MAX)
            .await?;
        Ok(outcome.deleted)
    }

    /// Deletes a bounded amount of a prefix without materializing its full key list.
    ///
    /// The listing is streamed, exact excluded keys are skipped, and selected
    /// objects are deleted in chunks of at most 1,000. Configured S3 stores and
    /// the dedicated perf harness use [`Self::delete_many`]; generic decorated
    /// stores retain the original 32-way per-key path so fault semantics do not
    /// silently change. The time budget is checked after each full chunk, so it
    /// is a coarse stopping signal rather than a deadline: a pass can exceed the
    /// budget while finishing a chunk. A final partial chunk is deleted only
    /// when the listing reaches its end.
    ///
    /// ```text
    /// stream prefix listing
    ///          |
    ///          +-- exact excluded key --> keep it
    ///          |
    ///          v
    /// collect 1,000 keys --> configured S3: one delete_many batch
    ///                    \-> generic decorator: 32-way per-key deletes
    ///          |                         |
    ///          | budget exhausted       | delete error
    ///          v                         v
    /// complete = false             return error; partial work may exist
    ///          |
    ///          | listing exhausted
    ///          v
    /// delete final partial chunk --> complete = true
    /// ```
    ///
    /// # Parameters
    ///
    /// - `prefix`: Non-empty key prefix that bounds both listing and deletion.
    /// - `exclude`: Optional exact key to preserve. Descendants or similarly
    ///   named keys are not excluded.
    /// - `budget`: Approximate elapsed-time budget checked between full chunks.
    ///   `Duration::MAX` means effectively unbounded; a zero budget can still
    ///   delete a full chunk, and a listing shorter than one chunk is completed.
    ///
    /// # Returns
    ///
    /// [`DeletePrefixOutcome`] with the accepted delete count and whether the
    /// pass observed the end of the listing. `complete = false` is conservative:
    /// a later pass must list current state again even if the previous chunk
    /// happened to contain the final object.
    ///
    /// # Errors
    ///
    /// Empty prefixes are validation errors. Listing, key parsing, and delete
    /// failures return an error, possibly after other objects were deleted. A
    /// key that disappeared between listing and deletion is treated as already
    /// deleted and counted.
    ///
    /// # Panics
    ///
    /// In debug builds, an empty prefix triggers `debug_assert!` before the
    /// release-mode validation error path.
    ///
    /// # Side Effects
    ///
    /// Performs a recursive LIST and zero or more DELETEs. Namespace deletion
    /// passes `meta.json` as `exclude` so the authoritative tombstone remains
    /// present while other data is removed.
    ///
    /// # Consistency
    ///
    /// The listing is not a transaction or snapshot. Concurrent creation may
    /// require another pass. Concurrent disappearance is idempotent because
    /// `NotFound` counts as successful cleanup. The method never removes the
    /// exact excluded key.
    ///
    /// # Performance
    ///
    /// Memory is bounded to one 1,000-key chunk plus backend listing buffers.
    /// Chunks run sequentially so the budget can be checked between them. The
    /// configured S3 adapter maps each chunk to one DeleteObjects request.
    ///
    /// # Examples
    ///
    /// A namespace deletion pass lists `acme/`, skips `acme/meta.json`, deletes
    /// 1,000 data objects, and then notices its budget has elapsed. It returns
    /// `{ deleted: 1000, complete: false }`; the tombstone remains, and a later
    /// pass resumes by listing `acme/` again.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `Option<&str>` expresses "no excluded key" without a nullable pointer.
    /// `std::mem::take(&mut chunk)` moves the full vector into async deletion and
    /// leaves a new empty vector in its place, avoiding a clone of 1,000 strings.
    /// Java would typically swap list references; C would transfer the buffer and
    /// manually reset the original owner. Rust makes the moved vector unusable in
    /// the listing loop, preventing double free or accidental reuse.
    #[instrument(skip(self), fields(prefix = prefix, exclude = exclude.unwrap_or("<none>")))]
    pub async fn delete_prefix_paged(
        &self,
        prefix: &str,
        exclude: Option<&str>,
        budget: Duration,
    ) -> Result<DeletePrefixOutcome> {
        debug_assert!(
            !prefix.is_empty(),
            "recursive root deletion is never allowed"
        );
        if prefix.is_empty() {
            return Err(ZeppelinError::Validation(
                "delete_prefix requires a non-empty prefix".to_string(),
            ));
        }

        let start = std::time::Instant::now();
        use futures::TryStreamExt;

        let path = Path::parse(prefix)?;
        let mut listed = self.inner.list(Some(&path));
        let mut chunk = Vec::with_capacity(1000);
        let mut deleted = 0usize;
        let mut complete = true;

        while let Some(object) = listed.try_next().await? {
            let key = object.location.to_string();
            if exclude == Some(key.as_str()) {
                continue;
            }
            chunk.push(key);
            if chunk.len() == 1000 {
                deleted += self.delete_prefix_chunk(std::mem::take(&mut chunk)).await?;
                if start.elapsed() >= budget {
                    complete = false;
                    break;
                }
            }
        }

        if complete && !chunk.is_empty() {
            deleted += self.delete_prefix_chunk(chunk).await?;
        }

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            count = deleted,
            complete,
            "object-store delete_prefix"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["delete_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(DeletePrefixOutcome { deleted, complete })
    }

    /// Deletes one bounded batch of known objects owned by an exact namespace.
    ///
    /// Every listed key crosses [`NamespaceObjectKey::classify`] before it can
    /// enter a DELETE chunk. The exact `meta.json` key is always retained as the
    /// lifecycle tombstone. A still-live `manifest.json`, or any unknown,
    /// foreign, or malformed key, stops the pass; previously completed chunks
    /// may already have made safe partial progress. The live manifest itself is
    /// never submitted to DELETE.
    /// Chunk size, budget behavior, native S3 batching, and decorated-store
    /// per-key fault semantics match [`Self::delete_prefix_paged`].
    pub(crate) async fn delete_namespace_objects_paged(
        &self,
        namespace: &str,
        budget: Duration,
    ) -> Result<DeletePrefixOutcome> {
        let prefix = namespace_prefix(namespace)?;
        let start = std::time::Instant::now();
        use futures::TryStreamExt;

        let path = Path::parse(&prefix)?;
        let mut listed = self.inner.list(Some(&path));
        let mut chunk = Vec::with_capacity(DELETE_MANY_MAX_KEYS);
        let mut deleted = 0usize;
        let mut complete = true;

        while let Some(object) = listed.try_next().await? {
            let owned = NamespaceObjectKey::classify(namespace, object.location.to_string())?;
            match owned.family() {
                NamespaceObjectFamily::Metadata => continue,
                NamespaceObjectFamily::Manifest => {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {namespace} cleanup requires manifest removal before object deletion"
                    )));
                }
                _ => {}
            }
            debug_assert_eq!(owned.namespace(), namespace);
            chunk.push(owned.into_key());
            if chunk.len() == DELETE_MANY_MAX_KEYS {
                deleted += self.delete_prefix_chunk(std::mem::take(&mut chunk)).await?;
                if start.elapsed() >= budget {
                    complete = false;
                    break;
                }
            }
        }

        if complete && !chunk.is_empty() {
            deleted += self.delete_prefix_chunk(chunk).await?;
        }

        let elapsed = start.elapsed();
        debug!(
            namespace,
            elapsed_ms = elapsed.as_millis(),
            count = deleted,
            complete,
            "object-store delete namespace objects"
        );
        crate::metrics::STORAGE_OPERATION_DURATION
            .with_label_values(&["delete_prefix"])
            .observe(elapsed.as_secs_f64());
        Ok(DeletePrefixOutcome { deleted, complete })
    }

    async fn delete_prefix_chunk(&self, keys: Vec<String>) -> Result<usize> {
        match self.prefix_delete_mode {
            PrefixDeleteMode::NativeBatch => self.delete_many(keys).await,
            PrefixDeleteMode::LegacyPerKeyUnordered32 => self.delete_key_chunk(keys).await,
        }
    }

    /// Preserves the established behavior of generic fault-instrumented stores.
    ///
    /// Instrumentation layers created through [`Self::new`] may attach
    /// independent semantics to every per-key DELETE. They therefore retain the
    /// original 32-way unordered scheduling and first-error cancellation path.
    /// Configured S3 and the dedicated perf harness instead select
    /// [`PrefixDeleteMode::NativeBatch`].
    async fn delete_key_chunk(&self, keys: Vec<String>) -> Result<usize> {
        use futures::StreamExt;

        let count = keys.len();
        let inner = Arc::clone(&self.inner);
        let mut deletes = futures::stream::iter(keys.into_iter().map(move |key| {
            let inner = Arc::clone(&inner);
            async move {
                let path = Path::parse(&key)?;
                match inner.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(ZeppelinError::Storage(error)),
                }
                Ok::<_, ZeppelinError>(())
            }
        }))
        .buffer_unordered(32);

        while let Some(result) = deletes.next().await {
            result?;
        }

        Ok(count)
    }
}

/// Extracts a probe host and port from the endpoint forms accepted at startup.
///
/// Explicit `http://` and `https://` schemes select ports 80 and 443 when no
/// port is present; an endpoint without a scheme defaults to 443. The helper
/// ignores a path and user-info prefix and supports bracketed IPv6 literals.
/// Raw IPv6 literals must be bracketed so their colons are not mistaken for the
/// host/port separator.
///
/// # Parameters
///
/// - `endpoint`: Borrowed configured endpoint text.
///
/// # Returns
///
/// An owned host string and resolved port suitable for the TCP startup probe.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] when the authority is empty, a bracketed
/// IPv6 host has no closing bracket, a host is empty, or a port is not a valid
/// `u16`.
///
/// # Examples
///
/// `http://127.0.0.1:9000/path` becomes `(127.0.0.1, 9000)`,
/// `https://minio.internal` becomes `(minio.internal, 443)`, and
/// `http://[::1]:9000` becomes `(::1, 9000)`.
///
/// # Rust Notes for Java/C Engineers
///
/// The function returns an owned `String` because temporary slices such as
/// `without_scheme` borrow the input and cannot outlive it. `strip_prefix`,
/// `split_once`, and `transpose` combine `Option` and `Result` without nullable
/// pointers or out-parameters: absence can choose a default, while invalid text
/// still returns an error.
/// Returns the explicitly configured substrate endpoint eligible for the TCP
/// boot probe, if any.
///
/// This is config-shape dispatch (which field names the endpoint for the
/// selected backend), not a capability question. Backends without an endpoint
/// field configured have nothing to TCP-probe; real-cloud reachability is
/// exercised by the object-store-level boot probe instead. The GCS and Azure
/// endpoint fields join this match together with their transports.
fn configured_probe_endpoint(config: &StorageConfig) -> Option<&str> {
    let endpoint = match config.backend {
        crate::config::StorageBackend::S3 => config.s3_endpoint.as_deref(),
        crate::config::StorageBackend::Gcs => config.gcs_endpoint.as_deref(),
        crate::config::StorageBackend::Azure => config.azure_endpoint.as_deref(),
        crate::config::StorageBackend::Local => None,
    };
    endpoint.filter(|value| !value.is_empty())
}

/// Tuned retry policy shared by every remote transport.
///
/// Origin: S3 returns 503 SlowDown and partition-level throttling that
/// routinely needs multi-second backoff; a budget of two retries inside a
/// two-second window surfaced routine throttling to callers as hard errors.
/// Five retries with a 5 s backoff ceiling stay well inside the 30 s request
/// timeout. GCS 429/503 behaves comparably and reuses it unchanged;
/// per-substrate retuning is explicitly a follow-up track.
fn transport_retry_config() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            base: 2.0,
        },
        max_retries: 5,
        retry_timeout: Duration::from_secs(15),
    }
}

/// Connection pool tuning shared by every remote transport: increased idle
/// connections and timeouts prevent the 28% sustained throughput degradation
/// observed in Run-007.
fn transport_client_options(allow_http: bool) -> ClientOptions {
    ClientOptions::new()
        .with_allow_http(allow_http)
        .with_pool_max_idle_per_host(64)
        .with_timeout(std::time::Duration::from_secs(30))
        .with_connect_timeout(std::time::Duration::from_secs(2))
        .with_pool_idle_timeout(std::time::Duration::from_secs(90))
}

/// Builds the service-account JSON that carries a custom GCS endpoint.
///
/// `object_store` 0.11.2 reads `gcs_base_url` (and `disable_oauth`) only from
/// the service-account document — there is no endpoint knob on the builder.
/// Without credentials this synthesizes the emulator document (OAuth
/// disabled); with credentials it augments the operator's document with
/// `gcs_base_url`, leaving authentication untouched.
fn gcs_service_account_json_with_endpoint(
    endpoint: &str,
    account_path: Option<&str>,
    account_key: Option<&str>,
) -> Result<String> {
    let mut document: serde_json::Value = match (account_path, account_key) {
        (Some(path), _) => {
            let raw = std::fs::read_to_string(path).map_err(|error| {
                ZeppelinError::Config(format!(
                    "failed to read gcs_service_account_path {path}: {error}"
                ))
            })?;
            serde_json::from_str(&raw).map_err(|error| {
                ZeppelinError::Config(format!(
                    "gcs_service_account_path {path} is not valid JSON: {error}"
                ))
            })?
        }
        (None, Some(key)) => serde_json::from_str(key).map_err(|error| {
            ZeppelinError::Config(format!(
                "gcs_service_account_key is not valid JSON: {error}"
            ))
        })?,
        (None, None) => serde_json::json!({
            "disable_oauth": true,
            "client_email": "",
            "private_key": "",
            "private_key_id": "",
        }),
    };
    let Some(map) = document.as_object_mut() else {
        return Err(ZeppelinError::Config(
            "GCS service-account document must be a JSON object".to_string(),
        ));
    };
    map.insert(
        "gcs_base_url".to_string(),
        serde_json::Value::String(endpoint.to_string()),
    );
    Ok(document.to_string())
}

fn endpoint_host_port(endpoint: &str) -> Result<(String, u16)> {
    let (default_port, without_scheme) = if let Some(rest) = endpoint.strip_prefix("http://") {
        (80, rest)
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        (443, rest)
    } else {
        (443, endpoint)
    };
    let authority = without_scheme
        .split('/')
        .next()
        .filter(|authority| !authority.is_empty())
        .ok_or_else(|| {
            ZeppelinError::Config(format!("invalid object-store endpoint URL: {endpoint}"))
        })?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);

    if let Some(rest) = authority.strip_prefix('[') {
        let Some((host, after_host)) = rest.split_once(']') else {
            return Err(ZeppelinError::Config(format!(
                "invalid bracketed S3 endpoint host: {endpoint}"
            )));
        };
        let port = after_host
            .strip_prefix(':')
            .map(parse_endpoint_port)
            .transpose()?
            .unwrap_or(default_port);
        return Ok((host.to_string(), port));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => (host, parse_endpoint_port(port)?),
        Some((_host, _port)) => {
            return Err(ZeppelinError::Config(format!(
                "invalid object-store endpoint host: {endpoint}"
            )));
        }
        None => (authority, default_port),
    };
    Ok((host.to_string(), port))
}

/// Parses a decimal endpoint port into the socket API's unsigned 16-bit type.
///
/// # Parameters
///
/// - `port`: Borrowed decimal text after the endpoint's final colon.
///
/// # Returns
///
/// A port in `0..=65535`. This helper validates numeric width but does not reject
/// port zero.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] with the original text and parse error when
/// the value is non-numeric, signed, or outside the `u16` range.
///
/// # Examples
///
/// `"9000"` returns `9000`; `"70000"` and `"minio"` return configuration
/// errors before any socket is opened.
fn parse_endpoint_port(port: &str) -> Result<u16> {
    port.parse::<u16>().map_err(|error| {
        ZeppelinError::Config(format!(
            "invalid object-store endpoint port {port}: {error}"
        ))
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// The Azure metadata-name canonicalization is bijective: logical
    /// hyphenated keys lower to underscore wire names on identifier-only
    /// substrates and read back unchanged, while hyphen-native substrates
    /// round-trip the native form. Pinned at the seam so the incarnation key
    /// can never silently change shape.
    #[test]
    fn user_metadata_wire_names_canonicalize_per_substrate() {
        let mut metadata = ObjectUserMetadata::new();
        metadata.insert("zeppelin-namespace-incarnation", "incarnation-07");

        let native = metadata.to_attributes(false);
        assert!(native
            .get(&Attribute::Metadata(
                "zeppelin-namespace-incarnation".into()
            ))
            .is_some());
        assert_eq!(ObjectUserMetadata::from_attributes(&native), metadata);

        let identifier = metadata.to_attributes(true);
        assert!(identifier
            .get(&Attribute::Metadata(
                "zeppelin_namespace_incarnation".into()
            ))
            .is_some());
        assert!(identifier
            .get(&Attribute::Metadata(
                "zeppelin-namespace-incarnation".into()
            ))
            .is_none());
        assert_eq!(ObjectUserMetadata::from_attributes(&identifier), metadata);
    }

    struct TestObjectSigner {
        signer_node: String,
        signature_tag: String,
        publication_store: ZeppelinStore,
    }

    impl ObjectSigner for TestObjectSigner {
        fn signer_node(&self) -> &str {
            &self.signer_node
        }

        fn sign(&self, message: &[u8]) -> Vec<u8> {
            [self.signature_tag.as_bytes(), message].concat()
        }

        fn publication_store(&self) -> ZeppelinStore {
            self.publication_store.clone()
        }
    }

    fn test_object_signer(store: &ZeppelinStore, signer_node: &str) -> Arc<dyn ObjectSigner> {
        test_object_signer_with_signature_tag(store, signer_node, signer_node)
    }

    fn test_object_signer_with_signature_tag(
        store: &ZeppelinStore,
        signer_node: &str,
        signature_tag: &str,
    ) -> Arc<dyn ObjectSigner> {
        Arc::new(TestObjectSigner {
            signer_node: signer_node.to_string(),
            signature_tag: signature_tag.to_string(),
            publication_store: store.signer_detached_clone(),
        })
    }

    /// A create-only write reports the original collision and preserves the first body.
    #[tokio::test]
    async fn put_create_is_immutable_and_keeps_generic_storage_error() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key = "_audit/2026-07-13/node/batch.jsonl";

        store
            .put_create(key, Bytes::from_static(b"first\n"))
            .await
            .unwrap();
        let collision = store.put_create(key, Bytes::from_static(b"second\n")).await;

        assert!(matches!(
            collision,
            Err(ZeppelinError::Storage(
                object_store::Error::AlreadyExists { .. }
            ))
        ));
        assert_eq!(
            store.get(key).await.unwrap(),
            Bytes::from_static(b"first\n")
        );
    }

    /// An observation carrying no usable identity is `None`, never an empty token.
    ///
    /// This is the invariant the whole seam rests on: because `from_parts` is the
    /// only constructor and refuses to build an all-empty value, `&StorageVersion`
    /// is proof that a real identity was observed, and two unversioned
    /// observations cannot compare equal as if they authorized reuse.
    #[test]
    fn storage_version_is_absent_rather_than_empty() {
        assert_eq!(StorageVersion::from_parts(None, None), None);
        assert_eq!(
            StorageVersion::from_parts(Some(String::new()), Some(String::new())),
            None
        );

        let etag_only = StorageVersion::from_parts(Some("\"abc\"".to_string()), None)
            .expect("an ETag alone is a usable identity");
        assert_eq!(etag_only.etag(), Some("\"abc\""));
        assert_eq!(etag_only.backend_version(), None);

        let generation_only =
            StorageVersion::from_parts(Some(String::new()), Some("17".to_string()))
                .expect("a generation alone is a usable identity");
        assert_eq!(generation_only.etag(), None);
        assert_eq!(generation_only.backend_version(), Some("17"));

        assert_ne!(etag_only, generation_only);
    }

    /// Both identity forms reach the backend precondition unchanged.
    ///
    /// S3 and Azure read `e_tag` while GCS reads `version`; dropping either here
    /// is what would make a GCS compare-and-swap inexpressible.
    #[test]
    fn both_identity_forms_travel_into_the_backend_precondition() {
        let version =
            StorageVersion::from_parts(Some("\"abc\"".to_string()), Some("17".to_string()))
                .expect("both forms present");
        let update = version.to_update_version();

        assert_eq!(update.e_tag.as_deref(), Some("\"abc\""));
        assert_eq!(update.version.as_deref(), Some("17"));
    }

    /// A conditional write with nothing observed fails loudly instead of degrading.
    #[test]
    fn require_turns_an_unversioned_observation_into_a_loud_error() {
        let observed = StorageVersion::from_parts(Some("\"abc\"".to_string()), None);
        assert!(StorageVersion::require(observed.as_ref(), "ns/manifest.json").is_ok());

        let error = StorageVersion::require(None, "ns/manifest.json")
            .expect_err("an absent identity must not authorize a conditional write");
        assert!(matches!(
            error,
            ZeppelinError::MissingVersionToken { ref key } if key == "ns/manifest.json"
        ));
    }

    /// The identity a conditional PUT reports is directly usable as the next
    /// precondition, which is what lets lease acquire and renew skip a re-read.
    #[tokio::test]
    async fn a_returned_put_token_is_accepted_by_the_next_conditional_put() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key = "ns/manifest.json";

        let created = store
            .put_with_version(key, Bytes::from_static(b"v1"))
            .await
            .unwrap()
            .expect("the in-memory backend reports an identity");
        let after_first = store
            .put_if_match(key, Bytes::from_static(b"v2"), &created, "ns")
            .await
            .unwrap()
            .expect("a successful conditional PUT reports the identity it installed");

        // The superseded identity must now lose, and only the reported one wins.
        let stale = store
            .put_if_match(key, Bytes::from_static(b"v3"), &created, "ns")
            .await;
        assert!(matches!(stale, Err(ZeppelinError::ManifestConflict { .. })));

        store
            .put_if_match(key, Bytes::from_static(b"v3"), &after_first, "ns")
            .await
            .unwrap();
        assert_eq!(store.get(key).await.unwrap(), Bytes::from_static(b"v3"));
    }

    #[test]
    fn transient_content_hash_cache_is_hard_bounded_and_consumable() {
        let mut cache = ContentHashCache::default();
        for index in 0..=CONTENT_HASH_CACHE_MAX_ENTRIES {
            cache.insert(format!("artifact-{index:08}"), [index as u8; 32]);
        }
        assert_eq!(cache.entries.len(), CONTENT_HASH_CACHE_MAX_ENTRIES);

        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        store
            .content_hashes
            .lock()
            .unwrap()
            .insert("published".to_string(), [7; 32]);
        assert_eq!(store.known_content_hash("published"), Some([7; 32]));
        store.forget_known_content_hashes([&"published".to_string()]);
        assert_eq!(store.known_content_hash("published"), None);
    }

    #[test]
    fn live_object_signer_rejects_a_different_node_and_expired_root_can_be_replaced() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let first = test_object_signer(&store, "node-one");
        let replacement = test_object_signer(&store, "node-two");
        store
            .install_object_signer(Arc::clone(&first))
            .expect("first signer must install");

        let conflict = store
            .install_object_signer(Arc::clone(&replacement))
            .expect_err("a live different signer must be rejected");
        assert!(
            conflict.to_string().contains("different node key material"),
            "unexpected conflict: {conflict}"
        );

        drop(first);
        store
            .install_object_signer(Arc::clone(&replacement))
            .expect("an expired signer root must allow replacement");
        assert_eq!(
            store
                .object_signer_node()
                .expect("replacement signer is live"),
            Some("node-two".to_string())
        );
    }

    #[tokio::test]
    async fn same_node_rebinds_signer_and_inventory_view_before_the_old_root_drops() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let old_inventory = ZeppelinStore::new(Arc::new(InMemory::new()));
        let replacement_inventory = ZeppelinStore::new(Arc::new(InMemory::new()));
        old_inventory
            .put_create("old-inventory", Bytes::from_static(b"old"))
            .await
            .expect("old inventory fixture must write");
        replacement_inventory
            .put_create("replacement-inventory", Bytes::from_static(b"replacement"))
            .await
            .expect("replacement inventory fixture must write");
        let old = test_object_signer_with_signature_tag(&old_inventory, "node-one", "old:");
        let replacement = test_object_signer_with_signature_tag(
            &replacement_inventory,
            "node-one",
            "replacement:",
        );
        store
            .install_object_signer(Arc::clone(&old))
            .expect("original signer must install");
        store
            .install_object_signer(Arc::clone(&replacement))
            .expect("same-node replacement must rebind before the original drops");

        drop(old);
        assert_eq!(
            store
                .sign_object(b"canonical payload")
                .expect("replacement root must remain live"),
            Some((
                "node-one".to_string(),
                b"replacement:canonical payload".to_vec()
            ))
        );
        assert_eq!(
            store
                .signer_inventory_store()
                .get("replacement-inventory")
                .await
                .expect("same-node replacement must rebind its inventory view"),
            Bytes::from_static(b"replacement")
        );
    }

    #[test]
    fn expired_object_signer_fails_loudly_instead_of_silently_unsigned() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let signer = test_object_signer(&store, "node-one");
        store
            .install_object_signer(Arc::clone(&signer))
            .expect("signer must install");
        drop(signer);

        let error = store
            .sign_object(b"canonical payload")
            .expect_err("an installed signer whose root ended must fail loudly");
        assert!(
            error
                .to_string()
                .contains("object signer root ended while the application store remains live"),
            "unexpected signer lifecycle error: {error}"
        );
    }

    #[tokio::test]
    async fn signer_detached_clone_resets_the_installed_inventory_view() {
        let application = ZeppelinStore::new(Arc::new(InMemory::new()));
        let publication = ZeppelinStore::new(Arc::new(InMemory::new()));
        application
            .put_create("caller-view", Bytes::from_static(b"caller"))
            .await
            .expect("caller fixture must write");
        publication
            .put_create("published-view", Bytes::from_static(b"published"))
            .await
            .expect("publication fixture must write");
        let signer = test_object_signer(&publication, "node-one");
        application
            .install_object_signer(Arc::clone(&signer))
            .expect("signer must install its publication view");

        assert_eq!(
            application
                .signer_inventory_store()
                .get("published-view")
                .await
                .expect("application must retain the installed inventory view"),
            Bytes::from_static(b"published")
        );

        let detached = application.signer_detached_clone();
        assert_eq!(
            detached
                .object_signer_node()
                .expect("detached clone must not resolve an application signer"),
            None
        );
        assert_eq!(
            detached
                .signer_inventory_store()
                .get("caller-view")
                .await
                .expect("detached clone must use its explicit caller view"),
            Bytes::from_static(b"caller")
        );
    }
}
