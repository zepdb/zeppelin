//! Fenced publication coordination for the global security-policy head.
//!
//! Policy snapshots are immutable, but the small head selecting the active
//! snapshot is mutable. Branch activation must linearize against that head, so
//! every head writer shares one global create-only/CAS-only lease. The lease is
//! not sufficient by itself: a holder first claims its fencing token into the
//! policy head, then every semantic or guard mutation CASes the exact claimed
//! head ETag.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::activation::{BranchActivationAttempt, BranchActivationTarget};
use crate::namespace::branching::ActivationNonce;
use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};
use crate::storage::{ConditionalPutOutcome, CreateOnlyOutcome, StorageVersion, ZeppelinStore};
use crate::time::Clock;

use super::{PolicyVersion, SecurityError};

/// Authoritative object holding the one global policy-publication lease.
pub const POLICY_PUBLICATION_LEASE_KEY: &str = "_security/leases/policy-publication.json";

/// Maximum number of crash-recoverable branch activation guards in one head.
pub const MAX_PENDING_BRANCH_ACTIVATIONS: usize = 1_024;

/// Longest accepted lifetime for one persisted branch activation guard.
pub const MAX_PENDING_BRANCH_ACTIVATION_LIFETIME_SECS: i64 = 600;

const DEFAULT_POLICY_PUBLICATION_LEASE_SECS: u64 = 30;
const MAX_POLICY_PUBLICATION_LEASE_SECS: u64 = 300;
const POLICY_LEASE_CAS_ATTEMPTS: usize = 5;

/// Monotonic control-plane revision of the mutable policy head.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PolicyControlRevision(u64);

impl PolicyControlRevision {
    /// Revision used by policy heads written before the publication interlock.
    pub const INITIAL: Self = Self(0);

    /// Construct an observed revision.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the numeric revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance exactly once, rejecting wraparound.
    pub fn next(self) -> std::result::Result<Self, SecurityError> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            SecurityError::InvalidPolicy("policy control revision overflow".to_string())
        })
    }
}

/// Nonzero generation of the global policy-publication lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PolicyLeaseFencingToken(u64);

impl PolicyLeaseFencingToken {
    /// First token persisted by a create-only lease acquisition.
    pub const INITIAL: Self = Self(1);

    /// Construct a persisted token, rejecting the non-authoritative zero value.
    pub fn new(value: u64) -> std::result::Result<Self, SecurityError> {
        if value == 0 {
            Err(SecurityError::InvalidPolicy(
                "policy-publication fencing token must be nonzero".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }

    /// Return the numeric fencing generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance on release or expired takeover, rejecting wraparound.
    pub fn next(self) -> std::result::Result<Self, SecurityError> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            SecurityError::InvalidPolicy("policy-publication fencing token overflow".to_string())
        })
    }
}

impl<'de> Deserialize<'de> for PolicyLeaseFencingToken {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// SHA-256 over the canonical semantic fields of one policy head.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PolicyHeadDigest([u8; 32]);

impl PolicyHeadDigest {
    /// Wrap one exact SHA-256 result.
    #[must_use]
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Borrow the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for PolicyHeadDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PolicyHeadDigest([REDACTED])")
    }
}

impl Serialize for PolicyHeadDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_digest(self.0))
    }
}

impl<'de> Deserialize<'de> for PolicyHeadDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        decode_digest(&encoded).map(Self).map_err(de::Error::custom)
    }
}

/// Durable pre-activation guard covered by the policy head's control CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingBranchActivation {
    branch_id: BranchId,
    target_namespace: NamespaceId,
    target_incarnation: NamespaceIncarnationId,
    activation_nonce: ActivationNonce,
    policy_version: PolicyVersion,
    policy_head_digest: PolicyHeadDigest,
    lease_fencing_token: PolicyLeaseFencingToken,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl PendingBranchActivation {
    /// Build a complete persisted guard. PolicyStore additionally checks its
    /// head identity and fencing token against the exact claimed head.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        branch_id: BranchId,
        target_namespace: NamespaceId,
        target_incarnation: NamespaceIncarnationId,
        activation_nonce: ActivationNonce,
        policy_version: PolicyVersion,
        policy_head_digest: PolicyHeadDigest,
        lease_fencing_token: PolicyLeaseFencingToken,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> std::result::Result<Self, SecurityError> {
        let guard = Self {
            branch_id,
            target_namespace,
            target_incarnation,
            activation_nonce,
            policy_version,
            policy_head_digest,
            lease_fencing_token,
            created_at,
            expires_at,
        };
        guard.validate()?;
        Ok(guard)
    }

    pub(crate) fn validate(&self) -> std::result::Result<(), SecurityError> {
        let zero_ulid = Ulid::from(0_u128);
        if self.branch_id.get() == zero_ulid
            || self.activation_nonce.get() == zero_ulid
            || self.target_incarnation.is_nil()
            || NamespaceId::parse(self.target_namespace.as_str().to_string()).is_err()
            || self.policy_version == PolicyVersion::BOOT
        {
            return Err(SecurityError::InvalidPolicy(
                "invalid pending branch activation identity".to_string(),
            ));
        }
        let lifetime = self.expires_at.signed_duration_since(self.created_at);
        if lifetime <= ChronoDuration::zero()
            || lifetime > ChronoDuration::seconds(MAX_PENDING_BRANCH_ACTIVATION_LIFETIME_SECS)
        {
            return Err(SecurityError::InvalidPolicy(
                "pending branch activation lifetime is out of bounds".to_string(),
            ));
        }
        Ok(())
    }

    /// Stable branch edge used as the policy-head map key.
    #[must_use]
    pub const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    /// Exact target namespace named by the non-visible activation attempt.
    #[must_use]
    pub fn target_namespace(&self) -> &NamespaceId {
        &self.target_namespace
    }

    /// Exact target lifetime named by the activation attempt.
    #[must_use]
    pub fn target_incarnation(&self) -> &NamespaceIncarnationId {
        &self.target_incarnation
    }

    /// One-shot target metadata fence.
    #[must_use]
    pub const fn activation_nonce(&self) -> ActivationNonce {
        self.activation_nonce
    }

    /// Exact target identity independently usable by background recovery.
    #[must_use]
    pub(crate) fn target(&self) -> BranchActivationTarget {
        BranchActivationTarget::new(
            self.branch_id,
            self.target_namespace.clone(),
            self.target_incarnation.clone(),
        )
    }

    /// Exact target and nonce covered by this persisted guard.
    #[must_use]
    pub(crate) fn attempt(&self) -> BranchActivationAttempt {
        BranchActivationAttempt::new(self.target(), self.activation_nonce)
    }

    /// Semantic policy version re-proved before guard insertion.
    #[must_use]
    pub const fn policy_version(&self) -> PolicyVersion {
        self.policy_version
    }

    /// Canonical semantic policy-head identity re-proved before insertion.
    #[must_use]
    pub const fn policy_head_digest(&self) -> PolicyHeadDigest {
        self.policy_head_digest
    }

    /// Publication lease generation that inserted this guard.
    #[must_use]
    pub const fn lease_fencing_token(&self) -> PolicyLeaseFencingToken {
        self.lease_fencing_token
    }

    /// Trusted guard creation time.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Hard expiry after which target nonce recovery is required.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PolicyPublicationLeaseState {
    Held,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyPublicationLeaseRecord {
    holder_id: Ulid,
    fencing_token: PolicyLeaseFencingToken,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    state: PolicyPublicationLeaseState,
}

impl PolicyPublicationLeaseRecord {
    fn validate(&self) -> std::result::Result<(), SecurityError> {
        if self.holder_id == Ulid::from(0_u128) {
            return Err(SecurityError::InvalidPolicy(
                "policy-publication lease holder must be nonzero".to_string(),
            ));
        }
        let lifetime = self.expires_at.signed_duration_since(self.issued_at);
        match self.state {
            PolicyPublicationLeaseState::Held
                if lifetime > ChronoDuration::zero()
                    && lifetime
                        <= ChronoDuration::seconds(MAX_POLICY_PUBLICATION_LEASE_SECS as i64) =>
            {
                Ok(())
            }
            PolicyPublicationLeaseState::Released if lifetime == ChronoDuration::zero() => Ok(()),
            PolicyPublicationLeaseState::Held | PolicyPublicationLeaseState::Released => {
                Err(SecurityError::InvalidPolicy(
                    "policy-publication lease timestamps are invalid".to_string(),
                ))
            }
        }
    }
}

/// Owned, identity-bearing observation of one acquired publication lease.
#[derive(Debug)]
pub struct PolicyPublicationLeaseClaim {
    record: PolicyPublicationLeaseRecord,
    version: StorageVersion,
}

impl PolicyPublicationLeaseClaim {
    /// Lease generation that must be claimed into the policy head.
    #[must_use]
    pub const fn fencing_token(&self) -> PolicyLeaseFencingToken {
        self.record.fencing_token
    }

    /// Last authoritative lease issue/renew time.
    #[must_use]
    pub const fn issued_at(&self) -> DateTime<Utc> {
        self.record.issued_at
    }

    /// Time after which takeover is permitted.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.record.expires_at
    }
}

/// S3-backed manager for the dedicated global policy-publication lease.
#[derive(Clone)]
pub struct PolicyPublicationLease {
    store: ZeppelinStore,
    holder_id: Ulid,
    lease_duration: Duration,
    clock: Clock,
}

impl PolicyPublicationLease {
    /// Construct one process holder with the production lease lifetime.
    #[must_use]
    pub fn new(store: ZeppelinStore) -> Self {
        Self {
            store,
            holder_id: Ulid::new(),
            lease_duration: Duration::from_secs(DEFAULT_POLICY_PUBLICATION_LEASE_SECS),
            clock: Clock::system(),
        }
    }

    /// Construct an explicitly identified holder and clock.
    pub fn with_clock(
        store: ZeppelinStore,
        holder_id: Ulid,
        lease_duration: Duration,
        clock: Clock,
    ) -> std::result::Result<Self, SecurityError> {
        if holder_id == Ulid::from(0_u128)
            || lease_duration.is_zero()
            || lease_duration > Duration::from_secs(MAX_POLICY_PUBLICATION_LEASE_SECS)
        {
            return Err(SecurityError::InvalidPolicy(
                "invalid policy-publication lease configuration".to_string(),
            ));
        }
        Ok(Self {
            store,
            holder_id,
            lease_duration,
            clock,
        })
    }

    /// Acquire a missing, released, or expired lease using create-only/CAS only.
    pub async fn acquire(&self) -> Result<PolicyPublicationLeaseClaim> {
        for _attempt in 0..POLICY_LEASE_CAS_ATTEMPTS {
            let now = self.clock.now();
            match self.store.get_with_meta(POLICY_PUBLICATION_LEASE_KEY).await {
                Err(ZeppelinError::NotFound { .. }) => {
                    let record = self.held_record(PolicyLeaseFencingToken::INITIAL, now)?;
                    let body = encode_record(&record)?;
                    match self
                        .store
                        .put_create_outcome(POLICY_PUBLICATION_LEASE_KEY, body)
                        .await?
                    {
                        CreateOnlyOutcome::Created { version } => {
                            return self.claim_after_write(record, version).await;
                        }
                        CreateOnlyOutcome::AlreadyExists => continue,
                    }
                }
                Ok((body, observed)) => {
                    let current = decode_record(&body)?;
                    if current.state == PolicyPublicationLeaseState::Held
                        && current.expires_at > now
                    {
                        return Err(SecurityError::PolicyConflict.into());
                    }
                    let expected = required_version(observed)?;
                    let record = self.held_record(current.fencing_token.next()?, now)?;
                    match self
                        .store
                        .put_if_match_outcome(
                            POLICY_PUBLICATION_LEASE_KEY,
                            encode_record(&record)?,
                            &expected,
                        )
                        .await?
                    {
                        ConditionalPutOutcome::Conflict => continue,
                        ConditionalPutOutcome::Updated { version } => {
                            return self.claim_after_write(record, version).await;
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(SecurityError::PolicyConflict.into())
    }

    /// Renew the exact still-live claim by ETag CAS without changing its token.
    pub async fn renew(
        &self,
        claim: &PolicyPublicationLeaseClaim,
    ) -> Result<PolicyPublicationLeaseClaim> {
        self.require_owned(claim)?;
        let now = self.clock.now();
        if claim.record.expires_at <= now {
            return Err(SecurityError::PolicyConflict.into());
        }
        let record = self.held_record(claim.record.fencing_token, now)?;
        match self
            .store
            .put_if_match_outcome(
                POLICY_PUBLICATION_LEASE_KEY,
                encode_record(&record)?,
                &claim.version,
            )
            .await?
        {
            ConditionalPutOutcome::Conflict => Err(SecurityError::PolicyConflict.into()),
            ConditionalPutOutcome::Updated { version } => {
                self.claim_after_write(record, version).await
            }
        }
    }

    /// Best-effort-compatible release: one exact ETag CAS to a released body.
    /// The lease object is never deleted and is never overwritten unconditionally.
    pub async fn release(&self, claim: &PolicyPublicationLeaseClaim) -> Result<()> {
        self.require_owned(claim)?;
        let now = self.clock.now();
        let released = PolicyPublicationLeaseRecord {
            holder_id: self.holder_id,
            fencing_token: claim.record.fencing_token,
            issued_at: now,
            expires_at: now,
            state: PolicyPublicationLeaseState::Released,
        };
        released.validate()?;
        match self
            .store
            .put_if_match_outcome(
                POLICY_PUBLICATION_LEASE_KEY,
                encode_record(&released)?,
                &claim.version,
            )
            .await?
        {
            ConditionalPutOutcome::Updated { .. } => Ok(()),
            ConditionalPutOutcome::Conflict => Err(SecurityError::PolicyConflict.into()),
        }
    }

    fn held_record(
        &self,
        fencing_token: PolicyLeaseFencingToken,
        issued_at: DateTime<Utc>,
    ) -> std::result::Result<PolicyPublicationLeaseRecord, SecurityError> {
        let duration = ChronoDuration::from_std(self.lease_duration).map_err(|_| {
            SecurityError::InvalidPolicy(
                "policy-publication lease duration is unrepresentable".to_string(),
            )
        })?;
        let expires_at = issued_at.checked_add_signed(duration).ok_or_else(|| {
            SecurityError::InvalidPolicy("policy-publication lease expiry overflow".to_string())
        })?;
        let record = PolicyPublicationLeaseRecord {
            holder_id: self.holder_id,
            fencing_token,
            issued_at,
            expires_at,
            state: PolicyPublicationLeaseState::Held,
        };
        record.validate()?;
        Ok(record)
    }

    async fn claim_after_write(
        &self,
        expected: PolicyPublicationLeaseRecord,
        written: Option<StorageVersion>,
    ) -> Result<PolicyPublicationLeaseClaim> {
        let version = match written {
            Some(version) => version,
            None => {
                let (body, observed) = self
                    .store
                    .get_with_meta(POLICY_PUBLICATION_LEASE_KEY)
                    .await?;
                let decoded = decode_record(&body)?;
                if decoded != expected {
                    return Err(SecurityError::PolicyConflict.into());
                }
                required_version(observed)?
            }
        };
        Ok(PolicyPublicationLeaseClaim {
            record: expected,
            version,
        })
    }

    fn require_owned(
        &self,
        claim: &PolicyPublicationLeaseClaim,
    ) -> std::result::Result<(), SecurityError> {
        claim.record.validate()?;
        if claim.record.state != PolicyPublicationLeaseState::Held
            || claim.record.holder_id != self.holder_id
        {
            return Err(SecurityError::PolicyConflict);
        }
        Ok(())
    }
}

/// Permit retained across target activation and exact guard finalization.
#[derive(Debug)]
pub struct PolicyActivationGuardPermit {
    pub(crate) guard: PendingBranchActivation,
    pub(crate) lease_claim: PolicyPublicationLeaseClaim,
    pub(crate) head_version: StorageVersion,
    pub(crate) control_revision: PolicyControlRevision,
}

impl PolicyActivationGuardPermit {
    /// Exact persisted guard installed before target visibility.
    #[must_use]
    pub fn guard(&self) -> &PendingBranchActivation {
        &self.guard
    }

    /// Policy version used by the activation proof.
    #[must_use]
    pub const fn policy_version(&self) -> PolicyVersion {
        self.guard.policy_version
    }

    /// Canonical semantic policy-head digest used by the proof.
    #[must_use]
    pub const fn policy_head_digest(&self) -> PolicyHeadDigest {
        self.guard.policy_head_digest
    }

    /// Control revision after the guard insertion CAS.
    #[must_use]
    pub const fn control_revision(&self) -> PolicyControlRevision {
        self.control_revision
    }

    /// Lease generation that remains retained by this permit.
    #[must_use]
    pub const fn lease_fencing_token(&self) -> PolicyLeaseFencingToken {
        self.guard.lease_fencing_token
    }

    /// Exact target metadata nonce that activation/cancellation must CAS.
    #[must_use]
    pub const fn activation_nonce(&self) -> ActivationNonce {
        self.guard.activation_nonce
    }

    /// Exact target and nonce retained by this permit.
    #[must_use]
    pub(crate) fn attempt(&self) -> BranchActivationAttempt {
        self.guard.attempt()
    }
}

fn required_version(version: Option<StorageVersion>) -> Result<StorageVersion> {
    version.ok_or_else(|| SecurityError::PolicyHeadMissingEtag.into())
}

fn encode_record(record: &PolicyPublicationLeaseRecord) -> Result<Bytes> {
    record.validate()?;
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            SecurityError::InvalidPolicy(format!(
                "policy-publication lease encoding failed: {error}"
            ))
            .into()
        })
}

fn decode_record(body: &[u8]) -> Result<PolicyPublicationLeaseRecord> {
    let record: PolicyPublicationLeaseRecord = serde_json::from_slice(body).map_err(|error| {
        SecurityError::InvalidPolicy(format!("policy-publication lease JSON is invalid: {error}"))
    })?;
    record.validate()?;
    Ok(record)
}

fn encode_digest(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn decode_digest(encoded: &str) -> std::result::Result<[u8; 32], &'static str> {
    if encoded.len() != 64 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("policy head digest must be 64 hexadecimal characters");
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk)
            .map_err(|_| "policy head digest must contain ASCII hexadecimal")?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| "policy head digest must contain hexadecimal")?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::time::TimeSource;

    use super::*;

    #[derive(Debug)]
    struct AdjustableClock(Mutex<DateTime<Utc>>);

    impl AdjustableClock {
        fn new(now: DateTime<Utc>) -> Self {
            Self(Mutex::new(now))
        }

        fn advance(&self, duration: ChronoDuration) {
            let mut now = self
                .0
                .lock()
                .unwrap_or_else(|_| panic!("policy lease test clock poisoned"));
            *now += duration;
        }
    }

    impl TimeSource for AdjustableClock {
        fn now(&self) -> DateTime<Utc> {
            *self
                .0
                .lock()
                .unwrap_or_else(|_| panic!("policy lease test clock poisoned"))
        }
    }

    /// Build a store whose backend actually implements ETag compare-and-swap.
    ///
    /// `object_store`'s `LocalFileSystem` returns `NotImplemented` for
    /// `PutMode::Update`, so a local-backed store fails these cases before
    /// they can assert anything about lease acquisition or release. The
    /// in-memory backend supports conditional writes and therefore exercises
    /// the exact create-only and CAS semantics this lease depends on.
    fn cas_store() -> ZeppelinStore {
        ZeppelinStore::new(Arc::new(object_store::memory::InMemory::new()))
    }

    #[test]
    fn persisted_fencing_tokens_are_nonzero_and_monotonic() {
        assert!(PolicyLeaseFencingToken::new(0).is_err());
        let first = PolicyLeaseFencingToken::new(1).expect("token one is persisted");
        assert_eq!(first.get(), 1);
        assert_eq!(first.next().expect("token two").get(), 2);
    }

    #[tokio::test]
    async fn missing_acquisition_is_create_only_and_release_keeps_a_cas_record() {
        let store = cas_store();
        let now = Utc::now();
        let clock = Clock::from_source(Arc::new(AdjustableClock::new(now)));
        let first = PolicyPublicationLease::with_clock(
            store.clone(),
            Ulid::new(),
            Duration::from_secs(30),
            clock.clone(),
        )
        .expect("first lease manager");
        let second = PolicyPublicationLease::with_clock(
            store.clone(),
            Ulid::new(),
            Duration::from_secs(30),
            clock,
        )
        .expect("second lease manager");

        let (left, right) = tokio::join!(first.acquire(), second.acquire());
        let (winner, loser) = match (left, right) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            state => panic!("exactly one create-only acquisition must win: {state:?}"),
        };
        assert!(matches!(
            loser,
            ZeppelinError::Security(SecurityError::PolicyConflict)
        ));

        let winner_manager = if winner.record.holder_id == first.holder_id {
            &first
        } else {
            &second
        };
        winner_manager
            .release(&winner)
            .await
            .expect("exact release CAS");
        let released_bytes = store
            .get(POLICY_PUBLICATION_LEASE_KEY)
            .await
            .expect("release must retain the lease object");
        let released = decode_record(&released_bytes).expect("released record must validate");
        assert_eq!(released.state, PolicyPublicationLeaseState::Released);
    }

    #[tokio::test]
    async fn expired_takeover_increments_token_and_stale_release_cannot_overwrite() {
        let store = cas_store();
        let source = Arc::new(AdjustableClock::new(Utc::now()));
        let clock = Clock::from_source(source.clone());
        let first = PolicyPublicationLease::with_clock(
            store.clone(),
            Ulid::new(),
            Duration::from_secs(30),
            clock.clone(),
        )
        .expect("first lease manager");
        let second =
            PolicyPublicationLease::with_clock(store, Ulid::new(), Duration::from_secs(30), clock)
                .expect("second lease manager");
        let stale = first.acquire().await.expect("first acquisition");
        assert_eq!(stale.fencing_token().get(), 1);

        source.advance(ChronoDuration::seconds(31));
        let takeover = second.acquire().await.expect("expired takeover");
        assert_eq!(takeover.fencing_token().get(), 2);
        let stale_release = first
            .release(&stale)
            .await
            .expect_err("stale release ETag must conflict");
        assert!(matches!(
            stale_release,
            ZeppelinError::Security(SecurityError::PolicyConflict)
        ));
        second
            .renew(&takeover)
            .await
            .expect("takeover remains authoritative");
    }
}
