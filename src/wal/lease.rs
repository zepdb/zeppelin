use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, instrument, warn};

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

/// A lease granting exclusive write access to a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    /// ID of the process that holds this lease.
    pub holder_id: String,
    /// Monotonically increasing fencing token to prevent zombie writes.
    pub fencing_token: u64,
    /// When the lease was acquired.
    pub acquired_at: DateTime<Utc>,
    /// When the lease expires (wall clock).
    pub expires_at: DateTime<Utc>,
    /// ETag of the lease.json object — used for CAS on lease operations.
    #[serde(skip)]
    pub(crate) etag: String,
}

/// Manages namespace leases on S3.
///
/// Each namespace has at most one active lease (`{namespace}/lease.json`).
/// Writers and compactors acquire a lease before writing to a namespace.
/// The lease carries a monotonically increasing fencing token that prevents
/// zombie writers from committing stale data.
pub struct LeaseManager {
    store: ZeppelinStore,
    holder_id: String,
    lease_duration: Duration,
}

/// S3 key for a namespace's lease object.
fn lease_key(namespace: &str) -> String {
    format!("{namespace}/lease.json")
}

impl LeaseManager {
    /// Create a new lease manager for the given store and holder.
    pub fn new(store: ZeppelinStore, holder_id: String, lease_duration: Duration) -> Self {
        Self {
            store,
            holder_id,
            lease_duration,
        }
    }

    /// Acquire the namespace lease.
    ///
    /// - If no lease exists: creates one with `fencing_token = 1`.
    /// - If a lease exists but is expired: takes over with `fencing_token + 1` (CAS).
    /// - If a lease exists and is valid: returns `LeaseHeld`.
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

                if existing.expires_at > Utc::now() {
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

    /// Renew an existing lease. Token stays the same, expiry extends.
    ///
    /// Returns `LeaseExpired` if the lease was taken over by another holder.
    #[instrument(skip(self, lease), fields(namespace = namespace, holder = %self.holder_id))]
    pub async fn renew(&self, namespace: &str, lease: &Lease) -> Result<Lease> {
        let key = lease_key(namespace);
        let (data, etag) = self.store.get_with_meta(&key).await?;
        let current: Lease = serde_json::from_slice(&data)?;

        if current.holder_id != self.holder_id || current.fencing_token != lease.fencing_token {
            return Err(ZeppelinError::LeaseExpired {
                namespace: namespace.to_string(),
            });
        }

        // Same token, extended expiry.
        let renewed = self.build_lease(lease.fencing_token);
        let data = Bytes::from(serde_json::to_vec_pretty(&renewed)?);
        let etag_str = etag.unwrap_or_default();
        self.store
            .put_if_match(&key, data, &etag_str, namespace)
            .await?;

        let (data, new_etag) = self.store.get_with_meta(&key).await?;
        let mut renewed: Lease = serde_json::from_slice(&data)?;
        renewed.etag = new_etag.unwrap_or_default();
        debug!(fencing_token = renewed.fencing_token, "lease renewed");
        Ok(renewed)
    }

    /// Release the lease. **Best-effort** — if the lease was taken over
    /// (expired and re-acquired by another process), this returns `Ok(())`
    /// rather than blocking or returning a hard error.
    ///
    /// This satisfies the TLA+ constraint from Bug #9 (deadlock on release
    /// after expiry): a process must never get stuck trying to release a
    /// lease it no longer holds.
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
                released.expires_at = Utc::now() - chrono::Duration::seconds(1);
                let release_data = Bytes::from(serde_json::to_vec_pretty(&released)?);
                match self.store.put(&key, release_data).await {
                    Ok(()) => {
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

    /// Check if a lease is still valid (not expired by wall clock, held by us).
    pub fn validate(&self, lease: &Lease) -> bool {
        lease.expires_at > Utc::now() && lease.holder_id == self.holder_id
    }

    /// Build a new Lease with the given token and current timestamps.
    fn build_lease(&self, fencing_token: u64) -> Lease {
        let now = Utc::now();
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
