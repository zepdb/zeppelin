//! Reads immutable WAL fragments selected by authoritative manifest state.
//!
//! Query execution and compaction enter through
//! [`crate::wal::reader::WalReader`] after obtaining
//! [`crate::wal::manifest::FragmentRef`] values from a
//! [`crate::wal::manifest::Manifest`]. This module turns those references into
//! decoded [`crate::wal::fragment::WalFragment`] values, optionally using
//! [`crate::cache::DiskCache`] for immutable bytes. It does not decide which
//! fragments are visible: the manifest supplied or read by the caller owns that
//! decision.
//!
//! ```text
//! authoritative manifest refs (ordered by sequence number)
//!                         |
//!                         v
//!             concurrent cache lookups
//!                 | hit          | miss
//!                 |              v
//!                 |       authoritative S3 GET
//!                 |              |
//!                 `-------> fragment bytes
//!                                |
//!                                v
//!                     decode + required checksum
//!                                |
//!              missing object? --+-- complete ordered results
//!                     |
//!                     v
//!       re-read manifest from S3
//!          | still live -> fail loud
//!          ` compacted away -> verified GC-race skip
//! ```
//!
//! Direct helpers such as [`crate::wal::reader::WalReader::read_fragment`] and
//! [`crate::wal::reader::WalReader::list_fragment_keys`] address storage objects without checking
//! manifest visibility. They are diagnostic/building-block APIs and can observe
//! an uploaded orphan or an artifact not yet published. Visibility-sensitive
//! callers should begin with manifest references.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::wal::reader::WalReader::read_uncompacted_fragments`] for the simplest
//!    manifest-driven read.
//! 2. Read [`crate::wal::reader::WalReader::read_fragments_from_refs`] and its unchecked counterpart
//!    for query and compaction batch behavior.
//! 3. Read the private `read_fragment_bytes` helper for the cache-to-S3 path.
//! 4. Finish with the private `finish_fragment_results` helper for the fail-loud
//!    missing-object rule and the narrowly verified compaction/GC race.
//!
//! ## Invariants
//!
//! - Manifest reference order, not ULID timestamp order, determines WAL replay.
//! - Cache contents can supply immutable bytes but cannot make an unreferenced
//!   fragment visible or excuse an authoritative read failure.
//! - A missing fragment still referenced by a fresh manifest is data loss and
//!   remains an error. Only an exact ref absent from a fresh manifest is skipped.
//! - Every consumed fragment read validates its checksum. Historical method
//!   names containing `unchecked` remain compatibility aliases only.
//!
//! ## Rust concepts used here
//!
//! Batch methods borrow `&[FragmentRef]`, so they do not take or copy the
//! caller's reference vector. Java would pass a collection reference and C a
//! pointer plus length; Rust's slice additionally guarantees a valid contiguous
//! view for the duration of the async call. [`futures::future::join_all`] owns
//! all per-fragment futures and preserves their input order while polling them
//! concurrently. Optional cache access uses
//! `Option<&Arc<crate::cache::DiskCache>>`: the `Arc` shares cache ownership
//! across tasks, while this reader only borrows the handle and performs no
//! reference-count clone on each call.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, instrument, warn};
use ulid::Ulid;

use crate::cache::DiskCache;
use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

use super::fragment::WalFragment;
use super::manifest::{FragmentRef, Manifest};

/// Object-store-backed reader for immutable WAL fragments.
///
/// The reader owns a cheap store handle and keeps no manifest or fragment state
/// between calls. Any supplied cache is borrowed per operation, so one reader
/// can serve cached and uncached callers without changing its authority model.
pub struct WalReader {
    /// Shared handle used for authoritative manifest and fragment operations.
    store: ZeppelinStore,
}

impl WalReader {
    /// Creates a stateless reader backed by the configured object store.
    ///
    /// # Parameters
    ///
    /// - `store`: Store abstraction used for all authoritative reads and LISTs.
    ///
    /// # Returns
    ///
    /// A reader that performs no I/O until a read method is called.
    ///
    /// # Examples
    ///
    /// A server can construct one reader during startup and share the owning
    /// server state across namespace requests.
    pub fn new(store: ZeppelinStore) -> Self {
        Self { store }
    }

    /// Lists stored `.wal` object keys beneath a namespace's WAL prefix.
    ///
    /// This is an inventory operation, not a visibility read. It may include
    /// orphans, not-yet-published fragments, or objects retained during cleanup.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace prefix whose `wal/` subtree should be listed.
    ///
    /// # Returns
    ///
    /// All listed keys ending in `.wal`. Backend order is preserved and is not
    /// guaranteed to be deterministic or replay-safe.
    ///
    /// # Errors
    ///
    /// Returns validation, path, or object-store listing failures. A partial
    /// listing is never returned.
    ///
    /// # Side Effects
    ///
    /// Performs a recursive object-store LIST under `<namespace>/wal/`.
    ///
    /// # Performance
    ///
    /// May consume multiple remote listing pages and materializes every matching
    /// key before filtering non-WAL suffixes.
    ///
    /// # Examples
    ///
    /// If the prefix contains two fragment objects and a temporary marker, the
    /// result contains only the two `.wal` keys. Callers must consult a manifest
    /// before treating either fragment as visible.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn list_fragment_keys(&self, namespace: &str) -> Result<Vec<String>> {
        let prefix = format!("{namespace}/wal/");
        let keys = self.store.list_prefix(&prefix).await?;
        Ok(keys.into_iter().filter(|k| k.ends_with(".wal")).collect())
    }

    /// Reads and checksum-validates one fragment addressed directly by ULID.
    ///
    /// The method does not consult the manifest, so successfully reading the
    /// object proves existence and integrity, not visibility.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace that owns the WAL object key.
    /// - `fragment_id`: ULID used to derive the immutable `.wal` key.
    ///
    /// # Returns
    ///
    /// An owned decoded fragment after checksum validation.
    ///
    /// # Errors
    ///
    /// Returns missing-object, storage, decoding, or checksum-mismatch errors.
    /// No empty fragment or cached fallback is substituted.
    ///
    /// # Performance
    ///
    /// Performs one full object-store GET and checksum computation over the
    /// decoded vectors and deletes.
    ///
    /// # Examples
    ///
    /// A diagnostic tool with fragment ID `01H...` reads
    /// `<namespace>/wal/01H....wal`. Corruption returns an error even when the
    /// bytes can otherwise be decoded.
    #[instrument(skip(self), fields(namespace = namespace, fragment_id = %fragment_id))]
    pub async fn read_fragment(&self, namespace: &str, fragment_id: &Ulid) -> Result<WalFragment> {
        let data = self
            .read_fragment_bytes(namespace, fragment_id, None)
            .await?;
        Self::validate_fragment_identity(WalFragment::from_bytes(&data)?, fragment_id)
    }

    /// Reads every currently uncompacted fragment in authoritative manifest order.
    ///
    /// Manifest order reflects monotonic sequence-number assignment and is not
    /// reconstructed from ULID timestamps, which can be affected by clock skew.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose current manifest and referenced WAL
    ///   fragments should be read.
    ///
    /// # Returns
    ///
    /// Checksum-validated fragments in manifest order. A missing manifest is
    /// treated as an empty fragment set by this convenience API.
    ///
    /// # Errors
    ///
    /// Returns manifest read/decoding failures and any fragment read, decoding,
    /// or integrity failure. A concurrently compacted-and-deleted fragment is
    /// skipped only after `finish_fragment_results` re-reads the manifest
    /// and verifies that the exact ref is no longer live.
    ///
    /// # Side Effects
    ///
    /// Performs one manifest GET, followed by concurrent full-fragment GETs. A
    /// missing fragment can cause one additional fresh manifest GET and a GC-race
    /// metric increment.
    ///
    /// # Examples
    ///
    /// A manifest containing sequence `40` then `41` returns those fragments in
    /// that order even if their ULID timestamps sort differently. A namespace
    /// with no manifest returns an empty vector.
    #[instrument(skip(self), fields(namespace = namespace))]
    pub async fn read_uncompacted_fragments(&self, namespace: &str) -> Result<Vec<WalFragment>> {
        let manifest = Manifest::read(&self.store, namespace).await?;
        let manifest = match manifest {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        let refs = manifest.uncompacted_fragments().to_vec();
        self.read_fragments_from_refs(namespace, &refs, None).await
    }

    /// Reads and checksum-validates specific refs while preserving input order.
    ///
    /// All reads are started together. A `NotFound` is tolerated only when a
    /// fresh authoritative manifest proves compaction removed that exact ref;
    /// other failures remain fail-loud and no partial success vector is returned.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace used to derive object keys and revalidate any
    ///   missing ref.
    /// - `refs`: Borrowed manifest refs in the caller's required replay order.
    /// - `cache`: Optional shared tiered cache for immutable fragment bytes.
    ///
    /// # Returns
    ///
    /// Decoded fragments in `refs` order, minus only refs verified absent from a
    /// fresh manifest after a compaction/GC race.
    ///
    /// # Errors
    ///
    /// Returns cache-miss S3 failures, decode/checksum failures, or `NotFound`
    /// for a fragment still referenced by fresh authoritative state. Other
    /// fragment futures may already have completed when an error is returned.
    ///
    /// # Side Effects
    ///
    /// Reads cache/S3 concurrently, populates the cache best-effort on misses,
    /// and may re-read the manifest and increment the GC-race metric.
    ///
    /// # Performance
    ///
    /// Creates one future per ref and waits for all with `join_all`; peak decoded
    /// memory is proportional to the complete requested batch. Cache hits avoid
    /// S3 GETs, while misses perform one full-object GET each.
    ///
    /// # Examples
    ///
    /// Refs `[12, 13, 14]` return fragments in that order. If `13` disappears and
    /// a fresh manifest no longer references it, the result is `[12, 14]`; if the
    /// fresh manifest still references `13`, the whole call fails.
    #[instrument(skip(self, refs, cache), fields(namespace = namespace, ref_count = refs.len(), cache_enabled = cache.is_some()))]
    pub async fn read_fragments_from_refs(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<Vec<WalFragment>> {
        // Parallel prefetch all fragments concurrently.
        let results = futures::future::join_all(
            refs.iter()
                .map(|fref| self.read_fragment_with_cache(namespace, &fref.id, cache)),
        )
        .await;

        let fragments = self
            .finish_fragment_results(namespace, refs, results)
            .await?;

        debug!(fragment_count = fragments.len(), "read fragments from refs");

        Ok(fragments)
    }

    /// Reads and checksum-validates one directly addressed fragment.
    ///
    /// This compatibility alias now validates the checksum because immutable
    /// upload does not prove that later object-store reads preserved the bytes.
    /// Like [`Self::read_fragment`], it does not consult manifest visibility.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace that owns the fragment object.
    /// - `fragment_id`: ULID used to derive its key.
    ///
    /// # Returns
    ///
    /// The decoded, checksum-validated fragment.
    ///
    /// # Errors
    ///
    /// Returns storage, decoding, and checksum-mismatch errors.
    ///
    /// # Performance
    ///
    /// Performs one full-object GET and avoids the checksum's serialization and
    /// hashing work.
    ///
    /// # Examples
    ///
    /// Compaction may use this historical entry point after selecting a
    /// fragment; corruption still fails loud.
    #[instrument(skip(self), fields(namespace = namespace, fragment_id = %fragment_id))]
    pub async fn read_fragment_unchecked(
        &self,
        namespace: &str,
        fragment_id: &Ulid,
    ) -> Result<WalFragment> {
        self.read_fragment(namespace, fragment_id).await
    }

    /// Reads specific refs in input order with checksum validation.
    ///
    /// Missing-object revalidation, cache behavior, and ordering match
    /// [`Self::read_fragments_from_refs`]. The historical name remains for API
    /// compatibility; integrity behavior is now identical.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace used for keys and fresh-manifest verification.
    /// - `refs`: Borrowed refs in required replay order.
    /// - `cache`: Optional immutable-byte cache.
    ///
    /// # Returns
    ///
    /// Checksum-validated fragments in input order, excluding only verified
    /// GC-race misses.
    ///
    /// # Errors
    ///
    /// Returns storage, decoding, checksum, or still-referenced `NotFound`
    /// failures.
    ///
    /// # Performance
    ///
    /// Runs cache/S3 reads concurrently. Memory remains proportional to the
    /// full batch.
    ///
    /// # Examples
    ///
    /// A compaction snapshot loads selected immutable refs in manifest order
    /// and rejects any payload whose checksum changed after upload.
    #[instrument(skip(self, refs, cache), fields(namespace = namespace, ref_count = refs.len(), cache_enabled = cache.is_some()))]
    pub async fn read_fragments_from_refs_unchecked(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<Vec<WalFragment>> {
        let results = futures::future::join_all(
            refs.iter()
                .map(|fref| self.read_fragment_unchecked_with_cache(namespace, &fref.id, cache)),
        )
        .await;

        let fragments = self
            .finish_fragment_results(namespace, refs, results)
            .await?;

        debug!(
            fragment_count = fragments.len(),
            "read fragments from refs (unchecked)"
        );

        Ok(fragments)
    }

    /// Computes the effective tombstone IDs from delete-bearing refs only.
    ///
    /// Eventual queries use this to hide deleted segment results without fetching
    /// delete-free WAL fragments or scoring WAL vectors. Refs are processed in
    /// manifest order: a delete inserts its ID, and an upsert in a fetched
    /// delete-bearing fragment removes an older tombstone for the same ID.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace used for fragment reads and race verification.
    /// - `refs`: Manifest-ordered refs from which only `delete_count > 0` entries
    ///   are selected.
    /// - `cache`: Optional shared immutable-byte cache.
    ///
    /// # Returns
    ///
    /// IDs whose last relevant operation among the fetched delete-bearing
    /// fragments is a delete. An empty set means no selected fragment leaves an
    /// effective tombstone.
    ///
    /// # Errors
    ///
    /// Propagates batch-read failures, including checksum mismatches and a
    /// missing fragment that remains referenced by a fresh manifest.
    ///
    /// # Side Effects
    ///
    /// Performs no I/O when no ref advertises deletes. Otherwise it uses the
    /// normal cache/S3 path and emits tombstone-read diagnostics.
    ///
    /// # Performance
    ///
    /// Avoids fetching every delete-free fragment. CPU and memory are linear in
    /// the operations within selected fragments plus the number of effective IDs.
    ///
    /// # Examples
    ///
    /// For fetched operations `delete A`, then `upsert A + delete B`, the result
    /// is `{B}`. An upsert in a delete-free fragment is intentionally not fetched
    /// by this eventual-consistency fast path because WAL vectors are not being
    /// scored there.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`HashSet`] models idempotent tombstone state: inserting an existing ID or
    /// removing an absent ID is harmless. Java's `HashSet` is similar. In C this
    /// requires an explicit hash-table library and ownership policy for strings;
    /// Rust's set owns each cloned [`String`] and frees it automatically when the
    /// set drops.
    #[instrument(skip(self, refs, cache), fields(namespace = namespace, ref_count = refs.len(), cache_enabled = cache.is_some()))]
    pub async fn read_delete_ids_from_refs_unchecked(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<HashSet<String>> {
        let delete_refs: Vec<FragmentRef> = refs
            .iter()
            .filter(|fref| fref.delete_count > 0)
            .cloned()
            .collect();

        if delete_refs.is_empty() {
            debug!(
                tombstone_fragment_count = 0,
                deleted_ids = 0,
                "read WAL tombstones from refs"
            );
            return Ok(HashSet::new());
        }

        let fragments = self
            .read_fragments_from_refs_unchecked(namespace, &delete_refs, cache)
            .await?;
        let mut deleted_ids = HashSet::new();
        for fragment in &fragments {
            for del_id in &fragment.deletes {
                deleted_ids.insert(del_id.clone());
            }
            for vec in &fragment.vectors {
                deleted_ids.remove(&vec.id);
            }
        }

        debug!(
            tombstone_fragment_count = fragments.len(),
            deleted_ids = deleted_ids.len(),
            "read WAL tombstones from refs"
        );

        Ok(deleted_ids)
    }

    /// Reads cached-or-authoritative bytes and decodes them with checksum validation.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace owning the object.
    /// - `fragment_id`: Fragment identity used for storage and cache keys.
    /// - `cache`: Optional borrowed cache shared by the caller.
    ///
    /// # Returns
    ///
    /// A decoded, checksum-validated fragment.
    ///
    /// # Errors
    ///
    /// Propagates authoritative read, decoding, and integrity failures.
    ///
    /// # Examples
    ///
    /// A query cache hit avoids S3 but still validates the decoded fragment's
    /// checksum before returning it.
    async fn read_fragment_with_cache(
        &self,
        namespace: &str,
        fragment_id: &Ulid,
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<WalFragment> {
        let data = self
            .read_fragment_bytes(namespace, fragment_id, cache)
            .await?;
        let result = WalFragment::from_bytes(&data)
            .and_then(|fragment| Self::validate_fragment_identity(fragment, fragment_id));
        if result.is_err() {
            if let Some(cache) = cache {
                let cache_key = Self::fragment_cache_key(fragment_id);
                if let Err(error) = cache.invalidate(&cache_key).await {
                    warn!(
                        namespace,
                        fragment_id = %fragment_id,
                        error = %error,
                        "failed to evict corrupt WAL fragment cache entry"
                    );
                }
            }
        }
        result
    }

    /// Reads cached-or-authoritative bytes and validates their checksum.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace owning the object.
    /// - `fragment_id`: Fragment identity used for storage and cache keys.
    /// - `cache`: Optional borrowed cache shared by the caller.
    ///
    /// # Returns
    ///
    /// A decoded, checksum-validated fragment.
    ///
    /// # Errors
    ///
    /// Propagates authoritative read, decoding, and checksum failures.
    ///
    /// # Examples
    ///
    /// Compatibility callers use this wrapper, but immutable write-path
    /// validation never substitutes for validating bytes read back from S3.
    async fn read_fragment_unchecked_with_cache(
        &self,
        namespace: &str,
        fragment_id: &Ulid,
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<WalFragment> {
        self.read_fragment_with_cache(namespace, fragment_id, cache)
            .await
    }

    fn validate_fragment_identity(
        fragment: WalFragment,
        expected_id: &Ulid,
    ) -> Result<WalFragment> {
        if fragment.id != *expected_id {
            return Err(ZeppelinError::Serialization(format!(
                "WAL fragment id mismatch: key requested {expected_id}, payload contained {}",
                fragment.id
            )));
        }
        Ok(fragment)
    }

    /// Resolves immutable fragment bytes from cache first and S3 after a miss.
    ///
    /// The cache may satisfy a read because fragment objects are immutable. It
    /// does not decide visibility: the caller must already have selected the ID
    /// through the appropriate manifest contract.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace used in the authoritative S3 key.
    /// - `fragment_id`: ULID used for both object and cache keys.
    /// - `cache`: Optional tiered cache. `None` forces a direct S3 GET.
    ///
    /// # Returns
    ///
    /// Shared [`bytes::Bytes`] containing the complete serialized fragment.
    ///
    /// # Errors
    ///
    /// Without a usable cache entry, propagates the S3 `NotFound` or storage
    /// error. Cache-write failures after a successful GET are warned and
    /// suppressed because the requested bytes are already available.
    ///
    /// # Side Effects
    ///
    /// May update cache hit/miss diagnostics, perform one full S3 GET, populate
    /// memory and disk cache tiers, or log a best-effort cache-fill failure.
    ///
    /// # Consistency
    ///
    /// A cache hit is safe only because the key names a write-once fragment.
    /// A miss never becomes empty data: authoritative read failure propagates.
    /// Cache-fill failure is the only intentional degradation and changes cost,
    /// not returned data or visibility.
    ///
    /// # Performance
    ///
    /// Memory hits are cheapest, disk hits allocate a read buffer and promote it,
    /// and misses perform a full-object GET followed by a disk/memory fill.
    ///
    /// # Examples
    ///
    /// ```text
    /// cache hit  -> return immutable bytes; zero S3 GETs
    /// cache miss -> S3 GET succeeds -> attempt cache fill -> return same bytes
    /// cache miss -> S3 GET fails    -> return error; do not fabricate bytes
    /// ```
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `let Some(cache) = cache else` exhaustively handles the optional borrowed
    /// cache and exits early for the direct path. The returned [`bytes::Bytes`]
    /// owns a reference-counted immutable buffer; cloning it is closer to sharing
    /// a Java immutable buffer view than copying a C byte array.
    async fn read_fragment_bytes(
        &self,
        namespace: &str,
        fragment_id: &Ulid,
        cache: Option<&Arc<DiskCache>>,
    ) -> Result<bytes::Bytes> {
        let s3_key = WalFragment::s3_key(namespace, fragment_id);
        let Some(cache) = cache else {
            return self.store.get(&s3_key).await;
        };

        // A cache HIT serves the immutable fragment without S3. On a MISS we
        // fetch from S3 (the consumed read — its failure must propagate) and
        // then populate the cache BEST-EFFORT: caching is an optimization, so a
        // cache-write failure (e.g. disk full, or a torn-down cache dir) must
        // never fail a query that already has the bytes. Degrade to
        // served-from-S3-uncached, log, and move on.
        let cache_key = Self::fragment_cache_key(fragment_id);
        if let Some(data) = cache.get(&cache_key).await {
            return Ok(data);
        }
        let data = self.store.get(&s3_key).await?;
        if let Err(e) = cache.put(&cache_key, &data).await {
            warn!(
                namespace = namespace,
                fragment_id = %fragment_id,
                error = %e,
                "WAL fragment cache write failed; serving from S3 uncached"
            );
        }
        Ok(data)
    }

    /// Builds the cache-relative key for one globally unique fragment ID.
    ///
    /// # Parameters
    ///
    /// - `fragment_id`: ULID whose randomness makes namespace-independent cache
    ///   key collisions negligibly unlikely.
    ///
    /// # Returns
    ///
    /// An owned key under the `wal_fragments/` cache namespace.
    ///
    /// # Examples
    ///
    /// ID `01H...` becomes `wal_fragments/01H....wal`.
    #[must_use]
    fn fragment_cache_key(fragment_id: &Ulid) -> String {
        format!("wal_fragments/{fragment_id}.wal")
    }

    /// Reconciles ordered fragment results and verifies every missing-object race.
    ///
    /// Non-`NotFound` errors fail immediately after all futures have completed.
    /// If any object was missing, this method re-reads authoritative manifest
    /// state once. A missing ID still present there is returned as data loss;
    /// only IDs removed by concurrent compaction are skipped and counted.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose fresh manifest arbitrates missing refs.
    /// - `refs`: Original ordered refs corresponding one-for-one with `results`.
    /// - `results`: Completed fragment reads in the same order as `refs`.
    ///
    /// # Returns
    ///
    /// Successful fragments in their original relative order, omitting only
    /// verified compacted-away missing refs.
    ///
    /// # Errors
    ///
    /// Returns the first non-`NotFound` read error, a manifest read/decode error,
    /// or the missing key for any ID still referenced by a fresh manifest. If the
    /// fresh manifest itself is absent, the first missing-key error is returned.
    ///
    /// # Panics
    ///
    /// A `NotFound` result beyond the end of `refs` would panic while indexing;
    /// private callers prevent this by building both sequences from the same
    /// `join_all` input.
    ///
    /// # Side Effects
    ///
    /// Missing results trigger one authoritative manifest GET. Each verified
    /// compaction/GC skip emits a warning and increments the namespace-labeled
    /// `WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL` metric.
    ///
    /// # Consistency
    ///
    /// This is the narrow exception to fail-loud consumed reads. It never treats
    /// age, ULID order, or a stale cache as proof of removal; only absence from a
    /// fresh authoritative uncompacted-ref set permits a skip.
    ///
    /// # Examples
    ///
    /// If old refs `[A, B]` are being read while compaction publishes a manifest
    /// containing neither and GC removes `A`, the missing `A` can be skipped. If
    /// the fresh manifest still contains `A`, the method returns `NotFound`.
    async fn finish_fragment_results(
        &self,
        namespace: &str,
        refs: &[FragmentRef],
        results: Vec<Result<WalFragment>>,
    ) -> Result<Vec<WalFragment>> {
        let mut fragments = Vec::new();
        let mut missing = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(fragment) => fragments.push(fragment),
                Err(ZeppelinError::NotFound { key }) => {
                    missing.push((refs[i].id, key));
                }
                Err(e) => return Err(e),
            }
        }

        if missing.is_empty() {
            return Ok(fragments);
        }

        let fresh_manifest = Manifest::read(&self.store, namespace).await?;
        let Some(fresh_manifest) = fresh_manifest else {
            let (_, key) = missing.remove(0);
            return Err(ZeppelinError::NotFound { key });
        };
        let live_fragment_ids: HashSet<Ulid> = fresh_manifest
            .uncompacted_fragments()
            .iter()
            .map(|fref| fref.id)
            .collect();

        for (fragment_id, key) in &missing {
            if live_fragment_ids.contains(fragment_id) {
                return Err(ZeppelinError::NotFound { key: key.clone() });
            }
        }

        for (fragment_id, key) in missing {
            warn!(
                namespace = %namespace,
                fragment_id = %fragment_id,
                key = %key,
                "WAL fragment not found; fresh manifest no longer references it, treating as compaction GC race"
            );
            crate::metrics::WAL_FRAGMENT_GC_RACE_SKIPPED_TOTAL
                .with_label_values(&[namespace])
                .inc();
        }

        Ok(fragments)
    }
}
