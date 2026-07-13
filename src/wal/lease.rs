//! Coordinates time-bounded namespace ownership through an object-store lease.
//!
//! Writers and compactors enter this module through
//! [`crate::wal::lease::LeaseManager::acquire`], keep long-running work alive
//! with [`crate::wal::lease::LeaseManager::renew`], and finish with
//! [`crate::wal::lease::LeaseManager::release`]. The authoritative lease is the
//! JSON object at `<namespace>/lease.json`; the
//! [`crate::wal::lease::Lease`] returned to a caller is only a snapshot of that
//! object.
//!
//! A lease alone does not make a write safe. The monotonically increasing
//! [`crate::wal::lease::Lease::fencing_token`] must travel into fenced manifest publication such as
//! [`crate::wal::writer::WalWriter::append_with_lease`]. Publication then uses
//! both a fencing check and an ETag compare-and-swap (CAS): the token rejects a
//! known zombie, while CAS closes the race between checking the token and
//! replacing the manifest.
//!
//! ```text
//! holder A acquires token 7
//!          |
//!          | lease expires
//!          v
//! holder B CAS-takes over with token 8
//!          |
//!          v
//! manifest records token 8 with ETag CAS
//!          |
//!          +---- A presents token 7 -> rejected as stale
//!          `---- B presents token 8 -> may publish if CAS also succeeds
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::wal::lease::Lease`] to understand the persisted
//!    ownership record and its process-local ETag.
//! 2. Read [`crate::wal::lease::LeaseManager::acquire`] for first acquisition
//!    and expired-lease takeover.
//! 3. Read [`crate::wal::lease::LeaseManager::renew`] for the heartbeat path.
//! 4. Finish with [`crate::wal::lease::LeaseManager::release`] and
//!    [`crate::wal::lease::LeaseManager::validate`] to see why cleanup and local
//!    time checks are not fencing substitutes.
//!
//! ## Invariants and limits
//!
//! - Takeover preserves and increments the previous token. Deleting the lease
//!   object resets a later first acquisition to token `1`.
//! - An expired holder must be able to leave its cleanup path even after another
//!   process takes over; release is deliberately best-effort.
//! - Wall-clock expiry and holder identity are necessary coordination signals,
//!   but correctness-sensitive writes still require fencing plus manifest CAS.
//! - Initial creation is an unconditional PUT and relies on Zeppelin's v1
//!   single-writer-per-namespace operating rule. Expired-object takeover and
//!   renewal use ETag CAS. Renewal treats the returned ETag as an optimistic
//!   precondition; one authoritative read classifies a conflict before one
//!   bounded retry.
//!
//! ## Rust concepts used here
//!
//! [`crate::wal::lease::LeaseManager`] owns a clonable
//! [`crate::storage::ZeppelinStore`] handle and
//! borrows `&self` across async object-store calls. Java would normally share a
//! client reference and C would pass a client pointer; Rust additionally proves
//! that the manager remains alive for the whole future.
//! [`crate::error::Result`] and `?`
//! provide explicit error propagation instead of Java exceptions or C status
//! codes. The serialized [`crate::wal::lease::Lease`] uses `serde`, while its ETag is marked
//! `#[serde(skip)]` because that value belongs to the current GET response, not
//! to durable lease data.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::time::Clock;

/// A process's snapshot of the time-bounded write lease for one namespace.
///
/// The serialized fields are stored in `<namespace>/lease.json`. The private
/// `etag` field
/// is filled from object-store metadata after a read and is intentionally not
/// persisted. Possessing this value does not by itself prove current ownership:
/// another holder may take over after expiration, so callers must renew and use
/// [`Self::fencing_token`] on consistency-sensitive manifest writes.
///
/// # Examples
///
/// A compactor may receive token `12` with an expiry 30 seconds in the future.
/// If another compactor later takes over with token `13`, the first snapshot
/// still contains `12`; fenced publication rejects it rather than trusting the
/// stale in-memory value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    /// Stable process or worker identity written as the current holder.
    pub holder_id: String,
    /// Namespace-local generation carried into fenced manifest writes.
    ///
    /// Takeover increments this value so work started by an older holder can be
    /// distinguished from work started by the current holder.
    pub fencing_token: u64,
    /// Wall-clock time at which this lease record was most recently built.
    ///
    /// Renewal rebuilds the record, so this is the current acquisition or
    /// renewal time rather than necessarily the first time the holder acquired
    /// the namespace.
    pub acquired_at: DateTime<Utc>,
    /// Wall-clock instant after which another manager may attempt takeover.
    pub expires_at: DateTime<Utc>,
    /// ETag observed when this snapshot was read from object storage.
    ///
    /// This process-local concurrency token is excluded from JSON. Renewal uses
    /// it only as an optimistic CAS precondition and replaces it with the ETag
    /// returned by each successful conditional PUT. A conflict never trusts the
    /// memo: it is classified through one fresh authoritative read.
    #[serde(skip)]
    pub(crate) etag: String,
}

/// Acquires, renews, and best-effort releases namespace lease objects.
///
/// Each manager represents one `holder_id` and grants leases with one configured
/// duration. The manager talks only through [`crate::storage::ZeppelinStore`];
/// it has no local authority. Returned [`Lease`] snapshots carry a disposable
/// ETag memo, but object-store CAS and conflict classification remain the
/// ownership authority.
///
/// The manager coordinates writers, but stale-write prevention is completed by
/// passing the returned token into a manifest operation that performs both the
/// fencing check and CAS.
pub struct LeaseManager {
    /// Shared handle to the authoritative object-store abstraction.
    store: ZeppelinStore,
    /// Identity this manager writes into acquired and renewed lease records.
    holder_id: String,
    /// Amount of wall-clock time granted by each acquisition or renewal.
    lease_duration: Duration,
    /// Explicit wall-clock source shared with the process's other components.
    clock: Clock,
}

/// Builds the object-store key for a namespace's authoritative lease record.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix already validated by the calling layer.
///
/// # Returns
///
/// An owned key ending in `lease.json`.
///
/// # Examples
///
/// Namespace `catalog` maps to `catalog/lease.json`. This is an object key, not
/// an HTTP path segment.
fn lease_key(namespace: &str) -> String {
    format!("{namespace}/lease.json")
}

impl LeaseManager {
    /// Creates a manager for one holder identity and lease duration.
    ///
    /// Construction performs no object-store I/O and does not acquire a lease.
    ///
    /// # Parameters
    ///
    /// - `store`: Handle used for all authoritative lease GETs and PUTs.
    /// - `holder_id`: Process or worker identity persisted in leases created by
    ///   this manager.
    /// - `lease_duration`: Wall-clock lifetime granted by acquisition and each
    ///   renewal. Conversion to Chrono's duration is checked later by
    ///   `build_lease`.
    ///
    /// # Returns
    ///
    /// A manager that is ready to acquire namespaces but holds none yet.
    ///
    /// # Examples
    ///
    /// A compactor can construct one manager for `node-a` with a 30-second
    /// duration, then use that manager for different namespace keys.
    pub fn new(store: ZeppelinStore, holder_id: String, lease_duration: Duration) -> Self {
        Self::with_clock(store, holder_id, lease_duration, Clock::system())
    }

    /// Creates a manager with an explicitly selected wall-clock source.
    #[must_use]
    pub fn with_clock(
        store: ZeppelinStore,
        holder_id: String,
        lease_duration: Duration,
        clock: Clock,
    ) -> Self {
        Self {
            store,
            holder_id,
            lease_duration,
            clock,
        }
    }

    /// Returns the duration granted by this manager's acquisitions and renewals.
    ///
    /// Heartbeats such as [`crate::compaction::background`] derive their renewal
    /// interval from this value so scheduling remains tied to the actual grant.
    ///
    /// # Returns
    ///
    /// A copied [`Duration`]; reading it performs no I/O or allocation.
    ///
    /// # Examples
    ///
    /// A 30-second lease can be renewed every 10 seconds by choosing one third
    /// of this returned duration.
    #[must_use]
    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    /// Borrows the wall clock used for lease timestamps and expiry checks.
    #[must_use]
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Acquires a namespace lease or takes over its expired record.
    ///
    /// A missing lease object is created with token `1`. An unexpired object is
    /// rejected with [`ZeppelinError::LeaseHeld`]. An expired object is replaced
    /// conditionally using its ETag and a token one greater than the previous
    /// value.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose `<namespace>/lease.json` object should be
    ///   acquired.
    ///
    /// # Returns
    ///
    /// The lease bytes re-read after the successful PUT, including the ETag
    /// returned by that GET.
    ///
    /// # Errors
    ///
    /// Returns storage or JSON errors when the authoritative object cannot be
    /// read, written, or decoded. Returns [`ZeppelinError::LeaseHeld`] when an
    /// unexpired holder exists or when expired-lease CAS loses a takeover race.
    /// A successful PUT followed by a failed re-read leaves the lease changed in
    /// object storage even though this caller receives an error.
    ///
    /// # Side Effects
    ///
    /// Performs one GET first. First acquisition then performs an unconditional
    /// PUT and a GET; takeover performs a conditional PUT and a GET. It also
    /// emits structured acquisition diagnostics.
    ///
    /// # Consistency
    ///
    /// Expired takeover uses ETag CAS, so two contenders based on the same lease
    /// cannot both replace it. Initial creation is not conditional and therefore
    /// relies on Zeppelin's v1 single-writer-per-namespace operating rule. The
    /// returned token still must be used with fenced manifest CAS; lease
    /// acquisition by itself does not publish or protect data artifacts.
    ///
    /// # Performance
    ///
    /// Uses two sequential object-store reads and one full lease-object write on
    /// success. Lease JSON is small, so latency is dominated by remote roundtrips.
    ///
    /// # Examples
    ///
    /// ```text
    /// no lease object       -> PUT token 1 -> re-read token 1 + ETag
    /// live token 4          -> LeaseHeld; object unchanged
    /// expired token 4, v20  -> PUT-if-v20 token 5 -> re-read token 5 + ETag
    /// expired token 4 race  -> loser maps the CAS conflict to LeaseHeld
    /// ```
    #[instrument(skip(self), fields(namespace = namespace, holder = %self.holder_id))]
    pub async fn acquire(&self, namespace: &str) -> Result<Lease> {
        let key = lease_key(namespace);

        match self.store.get_with_meta(&key).await {
            Err(ZeppelinError::NotFound { .. }) => {
                // No existing lease — create the first one.
                let lease = self.build_lease(1);
                let data = serde_json::to_vec_pretty(&lease)?;
                self.store.put(&key, Bytes::from(data)).await?;

                // Re-read to capture the ETag for future CAS operations.
                let (data, etag) = self.store.get_with_meta(&key).await?;
                let mut lease: Lease = serde_json::from_slice(&data)?;
                lease.etag = etag.unwrap_or_default();
                debug!(fencing_token = lease.fencing_token, "lease acquired (new)");
                Ok(lease)
            }
            Ok((data, etag)) => {
                let existing: Lease = serde_json::from_slice(&data)?;

                if existing.expires_at > self.clock.now() {
                    // Lease is still valid — reject.
                    return Err(ZeppelinError::LeaseHeld {
                        namespace: namespace.to_string(),
                        holder: existing.holder_id,
                    });
                }

                // Lease expired — takeover via CAS.
                let new_token = existing.fencing_token + 1;
                let lease = self.build_lease(new_token);
                let data = Bytes::from(serde_json::to_vec_pretty(&lease)?);
                let etag_str = etag.unwrap_or_default();

                self.store
                    .put_if_match(&key, data, &etag_str, namespace)
                    .await
                    .map_err(|e| match e {
                        ZeppelinError::ManifestConflict { .. } => ZeppelinError::LeaseHeld {
                            namespace: namespace.to_string(),
                            holder: "unknown (race)".to_string(),
                        },
                        other => other,
                    })?;

                // Re-read to capture the new ETag.
                let (data, new_etag) = self.store.get_with_meta(&key).await?;
                let mut lease: Lease = serde_json::from_slice(&data)?;
                lease.etag = new_etag.unwrap_or_default();
                debug!(
                    fencing_token = lease.fencing_token,
                    "lease acquired (takeover)"
                );
                Ok(lease)
            }
            Err(e) => Err(e),
        }
    }

    /// Replaces a still-owned lease record with the same token and a fresh expiry.
    ///
    /// An ETag-bearing snapshot first attempts one conditional PUT directly.
    /// CAS success proves the object has not changed since that snapshot. A
    /// conflict performs one authoritative read, rejects a different holder or
    /// token, and retries once only when the body still names this owner. An
    /// ETag-less snapshot takes the cold read-validate-CAS path. Wall-clock
    /// expiry itself is not rejected: an expired record may be renewed if no
    /// takeover has changed its identity or token.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose lease should be renewed.
    /// - `lease`: Previously acquired or renewed snapshot whose fencing token
    ///   must still match authoritative state.
    ///
    /// # Returns
    ///
    /// A [`Lease`] with the same fencing token, refreshed timestamps, and the
    /// ETag returned by the successful CAS. A backend that omits the PUT ETag
    /// causes one fallback read to recover it.
    ///
    /// # Errors
    ///
    /// A missing object, different holder, different token, or second CAS
    /// conflict becomes [`ZeppelinError::LeaseExpired`]. JSON and unrelated
    /// storage errors propagate. If a conditional write succeeds without an
    /// ETag and the fallback GET fails, the remote expiry may already be
    /// extended even though no renewed snapshot is returned.
    ///
    /// # Side Effects
    ///
    /// Warm success performs one conditional PUT and no GET. A fast-path
    /// conflict adds exactly one classification GET and at most one retry. The
    /// cold path performs one GET plus one conditional PUT. A successful PUT
    /// whose response omits its ETag adds one fallback GET. Renewal rewrites
    /// `acquired_at` as well as `expires_at` but does not increment the token.
    ///
    /// # Consistency
    ///
    /// The memo is never ownership evidence by itself: it is only a CAS
    /// precondition. A failed CAS is resolved from S3, and a second change fails
    /// closed instead of looping. A heartbeat must still stop
    /// correctness-sensitive work when it can no longer prove renewal before
    /// the last confirmed expiry.
    ///
    /// # Examples
    ///
    /// Holder `node-a` renews token `7` with one CAS, extending its expiry while
    /// keeping token `7`. If `node-b` has already taken over with token `8`, the
    /// failed CAS plus one GET returns `LeaseExpired` and never revives
    /// `node-a`'s ownership.
    #[instrument(skip(self, lease), fields(namespace = namespace, holder = %self.holder_id))]
    pub async fn renew(&self, namespace: &str, lease: &Lease) -> Result<Lease> {
        let key = lease_key(namespace);

        if lease.holder_id != self.holder_id {
            return Err(Self::expired(namespace));
        }

        let mut renewed = self.build_lease(lease.fencing_token);
        let data = Bytes::from(serde_json::to_vec_pretty(&renewed)?);

        if !lease.etag.is_empty() {
            match self
                .store
                .put_if_match(&key, data.clone(), &lease.etag, namespace)
                .await
            {
                Ok(Some(new_etag)) => {
                    renewed.etag = new_etag;
                    debug!(
                        fencing_token = renewed.fencing_token,
                        fast_path = true,
                        "lease renewed"
                    );
                    return Ok(renewed);
                }
                Ok(None) => {
                    let renewed = self
                        .read_owned_lease(&key, namespace, lease.fencing_token)
                        .await?;
                    debug!(
                        fencing_token = renewed.fencing_token,
                        fast_path = true,
                        "lease renewed"
                    );
                    return Ok(renewed);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    let current = self
                        .read_owned_lease(&key, namespace, lease.fencing_token)
                        .await?;
                    match self
                        .store
                        .put_if_match(&key, data, &current.etag, namespace)
                        .await
                    {
                        Ok(Some(new_etag)) => {
                            renewed.etag = new_etag;
                            debug!(
                                fencing_token = renewed.fencing_token,
                                fast_path = false,
                                "lease renewed after ETag classification"
                            );
                            return Ok(renewed);
                        }
                        Ok(None) => {
                            let renewed = self
                                .read_owned_lease(&key, namespace, lease.fencing_token)
                                .await?;
                            debug!(
                                fencing_token = renewed.fencing_token,
                                fast_path = false,
                                "lease renewed after ETag classification"
                            );
                            return Ok(renewed);
                        }
                        Err(ZeppelinError::ManifestConflict { .. }) => {
                            return Err(Self::expired(namespace));
                        }
                        Err(ZeppelinError::Storage(object_store::Error::NotFound { .. })) => {
                            return Err(Self::expired(namespace));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(ZeppelinError::Storage(object_store::Error::NotFound { .. })) => {
                    return Err(Self::expired(namespace));
                }
                Err(error) => return Err(error),
            }
        }

        let current = self
            .read_owned_lease(&key, namespace, lease.fencing_token)
            .await?;
        match self
            .store
            .put_if_match(&key, data, &current.etag, namespace)
            .await?
        {
            Some(new_etag) => {
                renewed.etag = new_etag;
                debug!(
                    fencing_token = renewed.fencing_token,
                    fast_path = false,
                    "lease renewed"
                );
                Ok(renewed)
            }
            None => {
                let renewed = self
                    .read_owned_lease(&key, namespace, lease.fencing_token)
                    .await?;
                debug!(
                    fencing_token = renewed.fencing_token,
                    fast_path = false,
                    "lease renewed"
                );
                Ok(renewed)
            }
        }
    }

    async fn read_owned_lease(
        &self,
        key: &str,
        namespace: &str,
        fencing_token: u64,
    ) -> Result<Lease> {
        let (data, etag) = match self.store.get_with_meta(key).await {
            Ok(value) => value,
            Err(ZeppelinError::NotFound { .. }) => return Err(Self::expired(namespace)),
            Err(error) => return Err(error),
        };
        let mut current: Lease = serde_json::from_slice(&data)?;
        if current.holder_id != self.holder_id || current.fencing_token != fencing_token {
            return Err(Self::expired(namespace));
        }
        current.etag = etag.unwrap_or_default();
        Ok(current)
    }

    fn expired(namespace: &str) -> ZeppelinError {
        ZeppelinError::LeaseExpired {
            namespace: namespace.to_string(),
        }
    }

    /// Marks this manager's current lease as expired on a best-effort basis.
    ///
    /// Release never deletes the object, because preserving its token lets the
    /// next acquisition increment the namespace generation. A missing object or
    /// a record owned by another holder is already released from this caller's
    /// perspective. Storage failures while reading or writing are warned and
    /// suppressed so cleanup cannot deadlock a worker that has lost its lease.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose lease cleanup is being attempted.
    /// - `lease`: Snapshot identifying the holder and fencing token this caller
    ///   intends to release.
    ///
    /// # Returns
    ///
    /// `Ok(())` after marking the matching record expired, or after deciding
    /// cleanup is unnecessary or failed best-effort.
    ///
    /// # Errors
    ///
    /// Lease GET/PUT failures and missing objects are swallowed, but malformed
    /// lease JSON still returns a serialization error because the current record
    /// cannot be safely interpreted.
    ///
    /// # Side Effects
    ///
    /// Performs one GET. When holder and token still match, performs one
    /// unconditional PUT with an expiry one second in the past. Failures are
    /// logged as warnings rather than promoted to the caller.
    ///
    /// # Consistency
    ///
    /// Holder/token comparison prevents an already-observed takeover from being
    /// released by the old holder. The subsequent expiry write is intentionally
    /// cleanup, not the write-safety boundary; correctness comes from fencing
    /// and manifest CAS even if release loses a race or fails. This operation is
    /// why an expired process can always move on instead of waiting for a lease
    /// it no longer owns.
    ///
    /// # Examples
    ///
    /// If holder A still owns token `3`, release leaves token `3` in place but
    /// expires it so the next acquisition receives token `4`. If holder B has
    /// already taken over with token `4`, A returns `Ok(())` without modifying
    /// B's observed record.
    #[instrument(skip(self, lease), fields(namespace = namespace, holder = %self.holder_id))]
    pub async fn release(&self, namespace: &str, lease: &Lease) -> Result<()> {
        let key = lease_key(namespace);

        match self.store.get_with_meta(&key).await {
            Ok((data, _etag)) => {
                let current: Lease = serde_json::from_slice(&data)?;

                if current.holder_id != self.holder_id
                    || current.fencing_token != lease.fencing_token
                {
                    // Lease was taken over — best-effort, not an error.
                    debug!("lease already taken over, skipping release");
                    return Ok(());
                }

                // We still hold it — mark as expired (preserves fencing token
                // so the next acquire increments from it, not from 1).
                let mut released = current;
                released.expires_at = self.clock.now() - chrono::Duration::seconds(1);
                let release_data = Bytes::from(serde_json::to_vec_pretty(&released)?);
                match self.store.put(&key, release_data).await {
                    Ok(_) => {
                        debug!("lease released");
                        Ok(())
                    }
                    Err(e) => {
                        warn!(error = %e, "lease release failed (best-effort)");
                        Ok(())
                    }
                }
            }
            Err(ZeppelinError::NotFound { .. }) => {
                // Already gone.
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "failed to read lease for release (best-effort)");
                Ok(())
            }
        }
    }

    /// Checks whether a lease snapshot names this manager and is unexpired locally.
    ///
    /// This is a cheap local preflight only. It performs no object-store read and
    /// cannot discover a takeover that happened after the snapshot was returned.
    ///
    /// # Parameters
    ///
    /// - `lease`: Snapshot to compare with this manager's holder identity and the
    ///   current process wall clock.
    ///
    /// # Returns
    ///
    /// `true` only when `expires_at` is in the future and `holder_id` matches;
    /// otherwise `false`.
    ///
    /// # Consistency
    ///
    /// Never use this result instead of renewal or fenced manifest CAS. It says
    /// nothing about the current authoritative lease object.
    ///
    /// # Examples
    ///
    /// A snapshot for `node-a` that expires in five seconds validates for
    /// `node-a`. The same snapshot fails for `node-b`, and it fails for both
    /// after its wall-clock expiry.
    pub fn validate(&self, lease: &Lease) -> bool {
        lease.expires_at > self.clock.now() && lease.holder_id == self.holder_id
    }

    /// Builds an unpersisted lease record with current wall-clock timestamps.
    ///
    /// # Parameters
    ///
    /// - `fencing_token`: Generation to place in the new record. The caller is
    ///   responsible for choosing `1`, preserving a renewal token, or
    ///   incrementing an expired record.
    ///
    /// # Returns
    ///
    /// An owned lease with this manager's holder identity, an empty process-local
    /// ETag, and expiry `lease_duration` after construction time.
    ///
    /// # Panics
    ///
    /// Panics when `lease_duration` cannot be represented by Chrono. This is a
    /// configuration/programming error rather than a recoverable lease state.
    ///
    /// # Examples
    ///
    /// Building token `9` for a 30-second manager produces a record for this
    /// holder whose expiry is approximately 30 seconds after `acquired_at`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The holder string is cloned because the returned [`Lease`] owns its
    /// serialized identity independently of the manager. Unlike copying a Java
    /// reference or a C pointer, cloning a Rust [`String`] allocates and copies
    /// its UTF-8 bytes. The `expect` is an explicit invariant assertion after a
    /// checked standard-to-Chrono duration conversion.
    fn build_lease(&self, fencing_token: u64) -> Lease {
        let now = self.clock.now();
        #[allow(clippy::expect_used)]
        let expires_at = now
            + chrono::Duration::from_std(self.lease_duration)
                .expect("lease_duration out of range for chrono");
        Lease {
            holder_id: self.holder_id.clone(),
            fencing_token,
            acquired_at: now,
            expires_at,
            etag: String::new(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::time::TimeSource;

    #[derive(Debug)]
    struct AdjustableTimeSource {
        now_ms: AtomicI64,
    }

    impl AdjustableTimeSource {
        fn new(now: DateTime<Utc>) -> Self {
            Self {
                now_ms: AtomicI64::new(now.timestamp_millis()),
            }
        }

        fn jump(&self, delta: chrono::Duration) {
            self.now_ms
                .fetch_add(delta.num_milliseconds(), Ordering::SeqCst);
        }
    }

    impl TimeSource for AdjustableTimeSource {
        fn now(&self) -> DateTime<Utc> {
            DateTime::from_timestamp_millis(self.now_ms.load(Ordering::SeqCst))
                .expect("adjustable lease-test timestamp must be representable")
        }
    }

    #[tokio::test]
    async fn lease_safety_under_backward_jump() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let source = Arc::new(AdjustableTimeSource::new(Utc::now()));
        let clock = Clock::from_source(source.clone());
        let holder_a = LeaseManager::with_clock(
            store.clone(),
            "holder-a".to_string(),
            Duration::from_secs(10),
            clock.clone(),
        );
        let holder_b = LeaseManager::with_clock(
            store,
            "holder-b".to_string(),
            Duration::from_secs(10),
            clock,
        );

        let first = holder_a.acquire("clock-lease").await.unwrap();
        source.jump(chrono::Duration::seconds(-30));
        let blocked = holder_b.acquire("clock-lease").await.unwrap_err();
        assert!(matches!(blocked, ZeppelinError::LeaseHeld { .. }));

        source.jump(chrono::Duration::seconds(45));
        let takeover = holder_b.acquire("clock-lease").await.unwrap();
        assert_eq!(takeover.fencing_token, first.fencing_token + 1);
    }
}
