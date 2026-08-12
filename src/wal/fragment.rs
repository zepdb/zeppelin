//! Defines the immutable write-ahead-log artifact stored for one write batch.
//!
//! This module sits between the write API and object storage. The
//! [`crate::wal::writer::WalWriter`] moves upserts and delete tombstones into a
//! [`crate::wal::fragment::WalFragment`], serializes it, and uploads it through
//! the storage layer. [`crate::wal::reader::WalReader`] reverses that process
//! for query execution and compaction. This file defines the artifact's bytes
//! and integrity check; it does not perform I/O, assign manifest sequence
//! numbers, or decide which uploaded fragments are visible.
//!
//! A fragment object can exist in S3 or MinIO without being part of the logical
//! database. Only a successful publication of a reference in the authoritative
//! [`crate::wal::manifest::Manifest`] makes it visible to readers. Once
//! uploaded, a fragment is treated as write-once. Replacement means writing a
//! new fragment and publishing a new manifest, never editing the old object.
//!
//! ```text
//! caller-owned upserts and deletes
//!              |
//!              | try_new: reject overlap, assign ULID, checksum payload
//!              v
//!       in-memory WalFragment
//!              |
//!              | to_bytes: version byte + MessagePack
//!              v
//!       immutable object in S3/MinIO ---- object exists, not visible yet
//!              |
//!              | writer publishes a fragment reference with manifest CAS
//!              v
//!       visible uncompacted WAL state
//!              |
//!              | reader decodes; query scans or compaction builds a segment
//!              v
//!       query result / immutable segment
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::wal::fragment::WalFragment`] for the persisted data
//!    model and operation-ordering invariant.
//! 2. Read [`crate::wal::fragment::WalFragment::try_new`] and its private
//!    `compute_checksum` helper for construction and deterministic integrity
//!    data.
//! 3. Read [`crate::wal::fragment::WalFragment::to_bytes`] and
//!    [`crate::wal::fragment::WalFragment::from_bytes`] for the current and
//!    legacy wire formats.
//! 4. Treat [`crate::wal::fragment::WalFragment::from_bytes_unchecked`] as an
//!    explicit trust boundary: it preserves Rust memory safety but skips the
//!    payload-integrity decision.
//!
//! ## Invariants
//!
//! - An ID must not appear in both the upsert and delete lists of a fragment
//!   created through the constructors. The lists can still contain duplicates
//!   within themselves; this type does not perform deduplication.
//! - The checksum covers the ordered upserts and deletes, with attribute keys
//!   canonicalized for stable hashing. It deliberately does not cover the
//!   fragment ULID or the checksum field itself.
//! - Persisted data uses a self-describing format because
//!   [`crate::types::AttributeValue`] is a Serde `untagged` enum and
//!   [`crate::types::VectorEntry`] contains a conditionally omitted field.
//! - A valid checksum detects accidental payload changes; xxHash is not an
//!   authentication mechanism and does not defend against a party that can
//!   replace both the data and its checksum.
//!
//! ## Rust concepts used here
//!
//! The constructors take ownership of the input [`Vec`] values. This is unlike
//! passing Java collection references: after a successful move the caller
//! cannot keep using the original bindings. It resembles transferring an
//! owning pointer in C, but Rust enforces the transfer and automatically drops
//! the allocations with the fragment. The result does not deep-clone the input;
//! checksum serialization only encodes a temporary byte representation.
//!
//! Checksum computation temporarily borrows slices and builds sorted maps of
//! references. The borrowed values resemble `const` pointers in C, with the
//! additional guarantees that they are non-null and cannot outlive the input.
//! Serde derives generate format code at compile time rather than relying on
//! Java-style runtime reflection.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, ZeppelinError};
use crate::types::{VectorEntry, VectorId};

/// Prefix that identifies the current MessagePack WAL-fragment encoding.
///
/// A serialized current-format object begins with `0x01`, followed by one
/// complete MessagePack value. Legacy JSON objects have no version prefix and
/// are recognized by their opening `{` byte in
/// [`WalFragment::from_bytes`]. Changing this value would require preserving a
/// decoder for already persisted objects.
const WAL_FORMAT_MSGPACK: u8 = 0x01;

/// One immutable batch of vector upserts and delete tombstones in the WAL.
///
/// The writer stores the complete value as one object and then publishes a
/// separate manifest reference. Query and compaction code process published
/// fragments in manifest sequence order; the time component of [`Ulid`] does
/// not determine which write wins.
///
/// The fields are public for serialization and downstream processing, so Rust
/// does not make post-construction mutation impossible in memory. Callers must
/// treat a fragment as immutable after computing its checksum, and especially
/// after uploading it. Mutating payload fields without recomputing the checksum
/// creates bytes that [`WalFragment::from_bytes`] will reject.
///
/// # Examples
///
/// A write that upserts `product-42` and tombstones `product-17` becomes one
/// fragment. The object is not queryable merely because it was uploaded; its
/// reference must first be added to the namespace manifest.
///
/// # Rust Notes for Java/C Engineers
///
/// `Clone` is explicit and potentially expensive here: cloning a fragment
/// allocates and clones its vector entries, coordinate buffers, attributes, and
/// delete IDs. Borrow `&WalFragment` when shared read-only access is enough.
/// The Serde derives generate owned encoding and decoding implementations, so a
/// decoded fragment does not borrow from the input byte slice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalFragment {
    /// Unique object identity used in the WAL key and manifest reference.
    ///
    /// A constructor generates this ULID, but manifest sequence numbers—not
    /// ULID timestamp order—define replay and last-write-wins order. The ULID is
    /// not part of [`WalFragment::checksum`].
    pub id: Ulid,
    /// Ordered, owned vector records to insert or replace.
    ///
    /// The constructor rejects an ID that also appears in [`Self::deletes`],
    /// but it does not reject repeated IDs within this vector. Namespace
    /// dimension and attribute validation happen at higher boundaries.
    pub vectors: Vec<VectorEntry>,
    /// Ordered, owned tombstones for logical vector IDs.
    ///
    /// A tombstone suppresses an older record when fragments are replayed in
    /// manifest order. Repeated IDs within this list are retained, while an ID
    /// that also occurs in [`Self::vectors`] is rejected by the constructors.
    pub deletes: Vec<VectorId>,
    /// xxHash3-64 digest of the canonicalized vectors-and-deletes payload.
    ///
    /// Attribute-map insertion order does not affect this value. Vector order,
    /// delete order, attribute presence, IDs, coordinates, and attribute values
    /// do. The fragment [`Self::id`] is intentionally outside the digest.
    pub checksum: u64,
}

impl WalFragment {
    /// Creates a fragment and treats overlapping upsert/delete IDs as a caller bug.
    ///
    /// This is the infallible convenience boundary for callers that have
    /// already established the operation invariant. Use
    /// [`WalFragment::try_new`] when overlap can come from untrusted or
    /// user-controlled input.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Owned upserts to move into the new fragment. This function
    ///   does not validate vector dimensions or attribute limits.
    /// - `deletes`: Owned tombstone IDs to move into the new fragment.
    ///
    /// # Returns
    ///
    /// A new owned fragment with a fresh ULID and a checksum over the supplied
    /// operations. Creating it performs no object-store I/O and does not make
    /// the operations visible.
    ///
    /// # Panics
    ///
    /// Panics when any vector ID also appears in `deletes`. It would also panic
    /// if Serde unexpectedly failed to encode the internal canonical checksum
    /// representation; the current representation contains no fallible custom
    /// serializer.
    ///
    /// # Performance
    ///
    /// Has the same linear scan, attribute sorting, serialization, and hashing
    /// cost as [`WalFragment::try_new`]. The input vectors are moved, not cloned.
    ///
    /// # Examples
    ///
    /// A trusted internal caller can create a fragment containing an upsert for
    /// `product-42` and a delete for `product-17`. Passing `product-42` in both
    /// lists panics rather than choosing an ambiguous intra-fragment order.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Rust normally uses [`Result`](std::result::Result) for expected failures.
    /// This wrapper deliberately converts validation failure into a panic via
    /// `expect`, closer to a failed Java assertion or a fatal C precondition.
    /// The underlying [`WalFragment::try_new`] keeps validation recoverable.
    #[allow(clippy::expect_used)]
    pub fn new(vectors: Vec<VectorEntry>, deletes: Vec<VectorId>) -> Self {
        Self::try_new(vectors, deletes)
            .expect("WalFragment::new called with overlapping vector IDs in upserts and deletes")
    }

    /// Creates a fragment after rejecting IDs with conflicting operations.
    ///
    /// The constructor makes one deterministic payload checksum and assigns a
    /// fresh ULID. It checks only the intra-fragment upsert/delete intersection;
    /// higher layers remain responsible for namespace dimension, finite-number,
    /// ID, and attribute validation.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Owned vector records to move into the upsert list.
    /// - `deletes`: Owned vector IDs to move into the tombstone list.
    ///
    /// # Returns
    ///
    /// A new owned fragment with the original list ordering, a fresh ULID, and
    /// a checksum. No remote artifact or manifest entry is created.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] naming the first vector whose ID
    /// also occurs in `deletes`. No object-store work or other external partial
    /// state exists when validation fails.
    ///
    /// # Panics
    ///
    /// Panics only if Serde unexpectedly cannot encode the internal canonical
    /// checksum representation. The current representation has no custom
    /// serializer that is expected to fail.
    ///
    /// # Consistency
    ///
    /// Construction alone does not publish a write. The writer must upload the
    /// immutable bytes and then win the authoritative manifest CAS before the
    /// fragment becomes visible.
    ///
    /// # Performance
    ///
    /// Builds a hash set over delete IDs, scans every vector, sorts each
    /// attribute map for canonical checksum input, serializes that input to a
    /// temporary JSON buffer, and hashes the buffer. The input collections are
    /// moved into the result without a deep clone.
    ///
    /// # Examples
    ///
    /// ```text
    /// upserts = [product-42], deletes = [product-17] -> new fragment
    /// upserts = [product-42], deletes = [product-42] -> Validation error
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The function owns both `Vec` arguments, so a successful call transfers
    /// their heap buffers into the fragment. During validation, the temporary
    /// `HashSet<&str>` borrows ID text from `deletes`; it allocates hash-table
    /// buckets but does not clone each string. Rust's lifetime checking ensures
    /// those borrowed strings cannot escape after `deletes` moves into the
    /// returned fragment.
    pub fn try_new(
        vectors: Vec<VectorEntry>,
        deletes: Vec<VectorId>,
    ) -> std::result::Result<Self, ZeppelinError> {
        use std::collections::HashSet;

        let delete_set: HashSet<&str> = deletes.iter().map(|id| id.as_str()).collect();
        for vec in &vectors {
            if delete_set.contains(vec.id.as_str()) {
                return Err(ZeppelinError::Validation(format!(
                    "vector ID '{}' appears in both upserts and deletes within the same fragment",
                    vec.id
                )));
            }
        }

        let id = Ulid::new();
        let checksum = Self::compute_checksum(&vectors, &deletes);
        Ok(Self {
            id,
            vectors,
            deletes,
            checksum,
        })
    }

    /// Computes a deterministic integrity digest for an ordered operation batch.
    ///
    /// Attribute maps are collected into sorted [`BTreeMap`](std::collections::BTreeMap)
    /// views before JSON serialization. This removes randomized `HashMap`
    /// iteration order from the digest while retaining the ordering of vectors
    /// and deletes. JSON is used as the canonical intermediate because the
    /// nested [`crate::types::AttributeValue`] representation is untagged and
    /// therefore requires a self-describing serializer.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed, ordered upserts whose IDs, coordinates, attribute
    ///   presence, keys, and values contribute to the digest.
    /// - `deletes`: Borrowed, ordered tombstone IDs that contribute to the
    ///   digest after the vector data.
    ///
    /// # Returns
    ///
    /// A 64-bit xxHash3 digest. The value does not include a fragment ULID and
    /// is intended to detect accidental changes, not malicious replacement.
    ///
    /// # Panics
    ///
    /// Panics if Serde cannot serialize the canonical borrowed representation.
    /// The current data types have no serializer expected to produce that
    /// error, so failure indicates a broken internal assumption.
    ///
    /// # Performance
    ///
    /// Allocates one canonical vector, one sorted map per present attribute map,
    /// and one JSON byte buffer. Attribute sorting costs `O(a log a)` per vector
    /// with `a` fields; serialization and xxHash are linear in payload size.
    ///
    /// # Examples
    ///
    /// Two `VectorEntry` values with identical attributes inserted into their
    /// hash maps in different orders produce the same checksum. Reordering the
    /// vector list or changing one coordinate changes the checksum input.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The canonical tuples hold references such as `&str`, `&[f32]`, and
    /// `&AttributeValue`; they do not deep-copy record data. This resembles a
    /// Java list of references or a C array of `const` pointers, but Rust proves
    /// that every reference remains valid until serialization finishes. The
    /// temporary vectors, tree nodes, and JSON bytes are then freed by RAII.
    fn compute_checksum(vectors: &[VectorEntry], deletes: &[VectorId]) -> u64 {
        use crate::types::AttributeValue;
        use std::collections::BTreeMap;

        #[allow(clippy::type_complexity)]
        let canonical: Vec<(&str, &[f32], Option<BTreeMap<&String, &AttributeValue>>)> = vectors
            .iter()
            .map(|v| {
                let attrs = v
                    .attributes
                    .as_ref()
                    .map(|a| a.iter().collect::<BTreeMap<_, _>>());
                (v.id.as_str(), v.values.as_slice(), attrs)
            })
            .collect();
        #[allow(clippy::expect_used)]
        let payload = serde_json::to_vec(&(&canonical, deletes)).expect(
            "checksum payload is slices, strings, and string-keyed BTreeMaps, which serde_json always serializes",
        );
        xxh3_64(&payload)
    }

    /// Recomputes the payload digest and rejects a checksum mismatch.
    ///
    /// Validation covers the ordered vectors and deletes but not the fragment
    /// ULID. It detects accidental corruption after construction or storage; it
    /// does not authenticate an object against deliberate rewriting.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the stored checksum equals the digest of the
    /// current payload.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::ChecksumMismatch`] with the recomputed value as
    /// `expected` and the stored field as `actual` when they differ. The method
    /// has no external side effects before returning the error.
    ///
    /// # Panics
    ///
    /// Panics if canonical JSON serialization unexpectedly fails, matching the
    /// invariant documented on the private checksum helper.
    ///
    /// # Performance
    ///
    /// Repeats canonical sorting, JSON serialization, and hashing over the
    /// complete operation payload. It performs no object-store requests.
    ///
    /// # Examples
    ///
    /// A freshly constructed fragment validates successfully. If a caller then
    /// changes one coordinate through the public `vectors` field without
    /// updating `checksum`, validation returns `ChecksumMismatch`.
    pub fn validate_checksum(&self) -> Result<()> {
        let expected = Self::compute_checksum(&self.vectors, &self.deletes);
        if self.checksum != expected {
            return Err(ZeppelinError::ChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }
        Ok(())
    }

    /// Encodes the complete fragment in the current versioned wire format.
    ///
    /// The output is `[0x01][MessagePack payload]`. MessagePack is
    /// self-describing, which is required by the nested untagged attribute enum
    /// and conditionally omitted attributes field. The encoded payload includes
    /// the ULID, operations, and already stored checksum.
    ///
    /// This method does not recompute or validate [`WalFragment::checksum`]. A
    /// caller that mutates public payload fields after construction can encode
    /// inconsistent bytes that the checked decoder later rejects.
    ///
    /// # Returns
    ///
    /// Owned [`Bytes`] containing the one-byte format marker followed by the
    /// complete MessagePack representation.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] if MessagePack cannot encode
    /// the fragment. No object-store write or other external partial work has
    /// happened when this method fails.
    ///
    /// # Side Effects
    ///
    /// Performs no I/O. The WAL writer separately uploads the returned bytes.
    ///
    /// # Consistency
    ///
    /// Producing bytes does not make a fragment visible. Visibility begins only
    /// when the authoritative manifest successfully references the uploaded
    /// immutable object.
    ///
    /// # Performance
    ///
    /// MessagePack first allocates its payload vector. This method then
    /// allocates the prefixed vector and copies the payload once; converting the
    /// final `Vec<u8>` into [`Bytes`] transfers that allocation without another
    /// byte-for-byte copy.
    ///
    /// # Examples
    ///
    /// A fragment with two upserts becomes one byte string whose first byte is
    /// `0x01`. The writer can PUT those bytes under [`WalFragment::s3_key`], but
    /// readers still ignore the object until a manifest publication succeeds.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&self` is a shared borrow, so encoding cannot consume the fragment or
    /// mutate it. [`Bytes`] is an owned, reference-counted byte buffer; here it
    /// takes over the final `Vec` allocation. Cloning that `Bytes` later shares
    /// the buffer by incrementing a reference count rather than copying all
    /// bytes, unlike cloning this `WalFragment`.
    pub fn to_bytes(&self) -> Result<Bytes> {
        // Serialize directly after the marker byte: one buffer, no second
        // copy of a potentially multi-megabyte fragment.
        let mut data = vec![WAL_FORMAT_MSGPACK];
        rmp_serde::encode::write(&mut data, self)
            .map_err(|e| ZeppelinError::Serialization(format!("msgpack serialize: {e}")))?;
        Ok(Bytes::from(data))
    }

    /// Decodes a current or legacy fragment and verifies its payload checksum.
    ///
    /// A leading `0x01` selects versioned MessagePack. A leading `{` selects
    /// legacy unprefixed JSON. For any other first byte, compatibility decoding
    /// tries MessagePack after skipping that byte and then tries the entire
    /// slice as unprefixed MessagePack. The unknown-prefix branch does **not**
    /// attempt JSON.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed complete object bytes. The returned fragment owns its
    ///   decoded fields and does not retain this slice.
    ///
    /// # Returns
    ///
    /// A fully owned fragment whose serialized structure decoded successfully
    /// and whose vectors-and-deletes checksum matches.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] for empty input or malformed
    /// MessagePack, a JSON error for malformed recognized legacy JSON, or
    /// [`ZeppelinError::ChecksumMismatch`] for a structurally valid fragment
    /// whose payload no longer matches its stored checksum. No partial fragment
    /// is returned.
    ///
    /// # Consistency
    ///
    /// Checksum success establishes payload integrity, not database visibility.
    /// The caller must still have obtained the key from the authoritative
    /// manifest when using the fragment for a query or compaction snapshot.
    /// Because the checksum excludes `id`, callers also rely on the manifest
    /// reference and object key to identify the expected fragment.
    ///
    /// # Performance
    ///
    /// Decoding allocates owned IDs, vectors, and attributes. Validation then
    /// performs a second full payload pass with canonical maps, a temporary JSON
    /// buffer, and xxHash computation.
    ///
    /// # Examples
    ///
    /// Bytes returned by [`WalFragment::to_bytes`] select the `0x01` branch and
    /// round-trip to an equivalent owned fragment. If one payload byte changes,
    /// decoding either rejects malformed MessagePack or reports a checksum
    /// mismatch; it never substitutes an empty fragment.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The exhaustive `match` makes each recognized first-byte case explicit,
    /// similar to a Java `switch` or C `switch`. The `?` operator returns the
    /// first decoding or checksum error to the caller while preserving its
    /// concrete error variant; it is structured early-return, not an exception.
    /// Borrowing `&[u8]` avoids copying the input, while Serde allocates the
    /// independent owned result.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ZeppelinError::Serialization(
                "empty WAL fragment data".into(),
            ));
        }

        let fragment: Self = match data[0] {
            WAL_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..])
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
            // Legacy JSON has no version byte, so preserve the full slice.
            b'{' => serde_json::from_slice(data)?,
            // Compatibility path for unknown or absent MessagePack markers.
            _ => rmp_serde::from_slice(&data[1..])
                .or_else(|_| rmp_serde::from_slice(data))
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
        };
        fragment.validate_checksum()?;
        Ok(fragment)
    }

    /// Decodes a fragment while deliberately skipping payload-integrity validation.
    ///
    /// Format detection and structural deserialization are identical to
    /// [`WalFragment::from_bytes`], but the stored checksum is trusted rather
    /// than recomputed. Query, vector-fetch, tombstone, and compaction paths use
    /// this fast path for immutable artifacts produced by the controlled writer.
    /// Callers must not use it for bytes whose provenance or continued
    /// immutability is uncertain.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed complete fragment bytes from a trusted immutable
    ///   object. The decoded result owns its data.
    ///
    /// # Returns
    ///
    /// A fully owned structurally decoded fragment. Its `checksum` field may not
    /// match its operations because this method does not check it.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Serialization`] for empty or malformed
    /// MessagePack and a JSON error for malformed recognized legacy JSON.
    /// Structurally valid corruption is not an error here.
    ///
    /// # Consistency
    ///
    /// This method trusts the immutability and provenance of the object; it does
    /// not establish them. It also does not establish manifest visibility. A
    /// caller must select fragment references from its authoritative manifest
    /// snapshot before decoding their objects.
    ///
    /// # Performance
    ///
    /// Performs one deserialization pass and skips the canonical map sorting,
    /// JSON allocation, and xxHash pass required by checked decoding.
    ///
    /// # Examples
    ///
    /// For a writer-produced immutable object, this returns the same logical
    /// fragment as [`WalFragment::from_bytes`] with less CPU work. If a payload
    /// is changed but remains valid MessagePack, this method can return `Ok`
    /// while the checked method returns `ChecksumMismatch`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// “Unchecked” here is a domain-integrity warning, not Rust's `unsafe`
    /// keyword. The compiler still enforces memory safety and valid lifetimes;
    /// only the application-level checksum postcondition is skipped. This is
    /// closer to a Java or C API whose name documents a precondition than to an
    /// unsafe memory operation.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ZeppelinError::Serialization(
                "empty WAL fragment data".into(),
            ));
        }

        let fragment: Self = match data[0] {
            WAL_FORMAT_MSGPACK => rmp_serde::from_slice(&data[1..])
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
            b'{' => serde_json::from_slice(data)?,
            _ => rmp_serde::from_slice(&data[1..])
                .or_else(|_| rmp_serde::from_slice(data))
                .map_err(|e| ZeppelinError::Serialization(format!("msgpack deserialize: {e}")))?,
        };
        Ok(fragment)
    }

    /// Builds the object-store key for a namespace's fragment ULID.
    ///
    /// This is a pure naming helper. It neither checks that the namespace is
    /// valid nor accesses S3/MinIO; callers pass the returned key through the
    /// storage abstraction.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Borrowed, already validated namespace name. This is an
    ///   object-key component, not a URL path segment.
    /// - `id`: Borrowed fragment identity recorded in a manifest reference.
    ///
    /// # Returns
    ///
    /// An owned key in the form `<namespace>/wal/<ulid>.wal`.
    ///
    /// # Performance
    ///
    /// Allocates one [`String`] containing the formatted key and performs no
    /// network request.
    ///
    /// # Examples
    ///
    /// Namespace `catalog` and fragment ULID `01ABC...` map to
    /// `catalog/wal/01ABC....wal`. Creating that string does not create the
    /// object or publish it in the manifest.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Both inputs are borrowed, so the helper neither clones the namespace nor
    /// takes ownership of the ULID. The returned `String` owns its allocation,
    /// like a newly created Java `String`; unlike a C formatting buffer, its
    /// length and cleanup remain automatic.
    pub fn s3_key(namespace: &str, id: &Ulid) -> String {
        format!("{namespace}/wal/{id}.wal")
    }

    /// Counts the upsert and delete commands stored in this fragment.
    ///
    /// # Returns
    ///
    /// The sum of `vectors.len()` and `deletes.len()`. This is an operation
    /// count, not a count of unique IDs or currently live vectors; duplicates
    /// within either list are counted separately.
    ///
    /// # Performance
    ///
    /// Runs in constant time because each [`Vec`] stores its length.
    ///
    /// # Examples
    ///
    /// A fragment with three upserts and two tombstones reports five
    /// operations, even if two upserts use the same vector ID.
    pub fn operation_count(&self) -> usize {
        self.vectors.len() + self.deletes.len()
    }
}
