//! Short-lived Ed25519 credentials that can only narrow current parent grants.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
#[cfg(unix)]
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration as StdDuration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::task::AbortHandle;
use ulid::Ulid;

use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::storage::store::ObjectSigner;
use crate::storage::{CreateOnlyOutcome, ZeppelinStore};
use crate::time::Clock;
use crate::types::Filter;

use super::policy::validate_policy_filter;
use super::policy_cache::{CachedPolicy, PolicyCache};
use super::{
    Action, ApiKeyId, AuthnFailure, NamespaceId, PolicyVersion, Principal, PrincipalId,
    SecurityError,
};

const TOKEN_PREFIX: &str = "zpt1_";
const SIGNER_PREFIX: &str = "_security/signers/";
const SIGNER_SLOT_PREFIX: &str = "_security/signer-slots/";
const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_PURPOSE_BYTES: usize = 512;
const MAX_DELEGATED_NAMESPACES: usize = 64;
const MAX_NARROWING_BYTES: usize = 12 * 1024;
const MAX_PUBLISHED_SIGNERS: usize = 32;

/// Validated narrowing carried by one delegated credential.
#[derive(Debug, Clone, Serialize)]
pub struct DelegationNarrowing {
    actions: Vec<Action>,
    namespaces: Vec<NamespaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mandatory_filter: Option<Filter>,
    purpose: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegationNarrowingWire {
    actions: Vec<Action>,
    namespaces: Vec<NamespaceId>,
    #[serde(default)]
    mandatory_filter: Option<Filter>,
    purpose: String,
}

impl<'de> Deserialize<'de> for DelegationNarrowing {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = DelegationNarrowingWire::deserialize(deserializer)?;
        Self::new(
            wire.actions,
            wire.namespaces,
            wire.mandatory_filter,
            wire.purpose,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl PartialEq for DelegationNarrowing {
    fn eq(&self, other: &Self) -> bool {
        let left_filter = serde_json::to_value(&self.mandatory_filter)
            .unwrap_or_else(|error| panic!("delegation filter serialization failed: {error}"));
        let right_filter = serde_json::to_value(&other.mandatory_filter)
            .unwrap_or_else(|error| panic!("delegation filter serialization failed: {error}"));
        self.actions == other.actions
            && self.namespaces == other.namespaces
            && self.purpose == other.purpose
            && left_filter == right_filter
    }
}

impl Eq for DelegationNarrowing {}

impl DelegationNarrowing {
    /// Construct and canonicalize one requested narrowed authority set.
    pub fn new(
        mut actions: Vec<Action>,
        mut namespaces: Vec<NamespaceId>,
        mandatory_filter: Option<Filter>,
        purpose: String,
    ) -> Result<Self, SecurityError> {
        if actions.len() > Action::ALL.len() || namespaces.len() > MAX_DELEGATED_NAMESPACES {
            return Err(SecurityError::InvalidPolicyRequest(format!(
                "delegation narrowing may name at most {} actions and {MAX_DELEGATED_NAMESPACES} namespaces",
                Action::ALL.len()
            )));
        }
        if actions.iter().any(|action| !action.is_delegatable()) {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegation actions must target namespace-bound resources".to_string(),
            ));
        }
        let action_count = actions.len();
        actions.sort_unstable();
        actions.dedup();
        let namespace_count = namespaces.len();
        namespaces.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        namespaces.dedup();
        if actions.len() != action_count || namespaces.len() != namespace_count {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegated actions and namespaces must be unique".to_string(),
            ));
        }
        if actions.is_empty() || namespaces.is_empty() {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegated actions and namespaces must not be empty".to_string(),
            ));
        }
        if purpose.trim().is_empty() || purpose.len() > MAX_PURPOSE_BYTES {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegation purpose must be nonempty and at most 512 bytes".to_string(),
            ));
        }
        if let Some(filter) = &mandatory_filter {
            validate_policy_filter(filter).map_err(SecurityError::InvalidPolicyRequest)?;
        }
        let narrowed = Self {
            actions,
            namespaces,
            mandatory_filter,
            purpose,
        };
        let encoded = serde_json::to_vec(&narrowed).map_err(|error| {
            SecurityError::InvalidPolicyRequest(format!(
                "delegation narrowing serialization failed: {error}"
            ))
        })?;
        if encoded.len() > MAX_NARROWING_BYTES {
            return Err(SecurityError::InvalidPolicyRequest(format!(
                "delegation narrowing exceeds {MAX_NARROWING_BYTES} encoded bytes"
            )));
        }
        Ok(narrowed)
    }

    /// Borrow the explicit action ceiling.
    #[must_use]
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    /// Borrow the exact namespace ceiling.
    #[must_use]
    pub fn namespaces(&self) -> &[NamespaceId] {
        &self.namespaces
    }

    /// Borrow the agent-supplied extra mandatory filter.
    #[must_use]
    pub const fn mandatory_filter(&self) -> Option<&Filter> {
        self.mandatory_filter.as_ref()
    }

    /// Borrow the mandatory human-readable purpose.
    #[must_use]
    pub fn purpose(&self) -> &str {
        &self.purpose
    }

    /// Return whether this signed ceiling names the exact action and namespace.
    #[must_use]
    pub fn allows(&self, action: Action, namespace: &NamespaceId) -> bool {
        self.actions.contains(&action) && self.namespaces.contains(namespace)
    }

    /// Intersect this signed ceiling with the parent's current authority.
    #[must_use]
    pub fn effective_allows(
        &self,
        action: Action,
        namespace: &NamespaceId,
        parent_allows: bool,
    ) -> bool {
        parent_allows && self.allows(action, namespace)
    }
}

/// Verified claims attached to an authenticated delegated principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DelegationContext {
    token_id: String,
    parent_principal: PrincipalId,
    parent_api_key_id: ApiKeyId,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    narrowed: DelegationNarrowing,
    signer_node: String,
}

impl DelegationContext {
    /// Borrow the token-scoped rate-limit and audit identity.
    #[must_use]
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Borrow the current-policy parent whose authority is intersected at use.
    #[must_use]
    pub const fn parent_principal(&self) -> &PrincipalId {
        &self.parent_principal
    }

    /// Borrow the exact parent credential whose continued validity bounds this token.
    #[must_use]
    pub const fn parent_api_key_id(&self) -> &ApiKeyId {
        &self.parent_api_key_id
    }

    /// Borrow the structural narrowing ceiling.
    #[must_use]
    pub const fn narrowed(&self) -> &DelegationNarrowing {
        &self.narrowed
    }

    /// Return the signed expiry instant.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegatedTokenPayload {
    token_id: String,
    parent_principal: PrincipalId,
    parent_api_key_id: ApiKeyId,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    narrowed: DelegationNarrowing,
    signer_node: String,
}

impl From<DelegatedTokenPayload> for DelegationContext {
    fn from(payload: DelegatedTokenPayload) -> Self {
        Self {
            token_id: payload.token_id,
            parent_principal: payload.parent_principal,
            parent_api_key_id: payload.parent_api_key_id,
            issued_at: payload.issued_at,
            expires_at: payload.expires_at,
            narrowed: payload.narrowed,
            signer_node: payload.signer_node,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignerDocument {
    node_id: String,
    public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignerSlot {
    slot: u8,
    node_id: String,
}

/// One-time token response produced after narrowing validation.
pub struct IssuedDelegatedToken {
    token_id: String,
    token: String,
    expires_at: DateTime<Utc>,
}

impl fmt::Debug for IssuedDelegatedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedDelegatedToken")
            .field("token_id", &self.token_id)
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl IssuedDelegatedToken {
    /// Borrow the stable token identifier.
    #[must_use]
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Borrow the complete bearer returned exactly once.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Return the signed expiry instant.
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

/// Node-local signer paired with a disposable cache of S3-published public keys.
pub(crate) struct DelegationAuthority {
    signing_key: SigningKey,
    signer_node: String,
    max_ttl: StdDuration,
    verifier: Arc<DelegationVerifier>,
}

/// Published node signer used by signed audit anchors when delegation itself
/// is not licensed (for example enforced audit in `open_unsafe` mode).
pub(crate) struct PublishedObjectSigner {
    signing_key: SigningKey,
    signer_node: String,
    publication_store: ZeppelinStore,
}

impl PublishedObjectSigner {
    pub(crate) async fn compose(
        store: ZeppelinStore,
        key_path: PathBuf,
    ) -> ZeppelinResult<Arc<Self>> {
        let signing_key = tokio::task::spawn_blocking(move || read_signing_key(&key_path))
            .await
            .map_err(|error| {
                ZeppelinError::Config(format!(
                    "audit signing-key task failed during boot: {error}"
                ))
            })??;
        let signer_node = signer_node_id(&signing_key.verifying_key());
        publish_signer(&store, &signer_node, &signing_key.verifying_key()).await?;
        claim_signer_slot(&store, &signer_node).await?;
        let signer = Arc::new(Self {
            signing_key,
            signer_node,
            publication_store: store,
        });
        Ok(signer)
    }
}

impl ObjectSigner for PublishedObjectSigner {
    fn signer_node(&self) -> &str {
        &self.signer_node
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.signing_key.sign(message).to_bytes().to_vec()
    }

    fn publication_store(&self) -> ZeppelinStore {
        self.publication_store.clone()
    }
}

impl DelegationAuthority {
    pub(crate) async fn compose(
        store: ZeppelinStore,
        policy_cache: Arc<PolicyCache>,
        clock: Clock,
        key_path: PathBuf,
        max_ttl_secs: u64,
        refresh_interval: StdDuration,
    ) -> ZeppelinResult<Arc<Self>> {
        let signing_key = tokio::task::spawn_blocking(move || read_signing_key(&key_path))
            .await
            .map_err(|error| {
                ZeppelinError::Config(format!(
                    "delegation signing-key task failed during boot: {error}"
                ))
            })??;
        let signer_node = signer_node_id(&signing_key.verifying_key());
        publish_signer(&store, &signer_node, &signing_key.verifying_key()).await?;
        claim_signer_slot(&store, &signer_node).await?;
        let signers = load_signers(&store).await?;
        let signer_cache = SignerCache::start(store.clone(), signers, refresh_interval)?;
        let monotonic_wall = MonotonicWall::new(clock.now());
        let verifier = Arc::new(DelegationVerifier {
            signer_cache,
            policy_cache,
            clock,
            max_ttl: StdDuration::from_secs(max_ttl_secs),
            monotonic_wall: Mutex::new(monotonic_wall),
        });
        let authority = Arc::new(Self {
            signing_key,
            signer_node,
            max_ttl: StdDuration::from_secs(max_ttl_secs),
            verifier,
        });
        Ok(authority)
    }

    pub(crate) fn verifier(&self) -> Arc<DelegationVerifier> {
        Arc::clone(&self.verifier)
    }

    pub(crate) async fn refresh_signers_once(&self) -> ZeppelinResult<()> {
        self.verifier.signer_cache.refresh_once().await
    }

    pub(crate) fn mint(
        &self,
        parent_principal: PrincipalId,
        parent_api_key_id: ApiKeyId,
        narrowed: DelegationNarrowing,
        requested_ttl_secs: u64,
        observed_now: DateTime<Utc>,
    ) -> Result<IssuedDelegatedToken, SecurityError> {
        if requested_ttl_secs == 0 || StdDuration::from_secs(requested_ttl_secs) > self.max_ttl {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegated token lifetime exceeds the configured maximum".to_string(),
            ));
        }
        let requested_ttl_secs = i64::try_from(requested_ttl_secs).map_err(|_| {
            SecurityError::InvalidPolicyRequest(
                "delegated token lifetime is out of range".to_string(),
            )
        })?;
        let now = self.verifier.effective_now(observed_now);
        let expires_at = now
            .checked_add_signed(Duration::seconds(requested_ttl_secs))
            .ok_or_else(|| {
                SecurityError::InvalidPolicyRequest(
                    "delegated token expiry is out of range".to_string(),
                )
            })?;
        let payload = DelegatedTokenPayload {
            token_id: format!("zdt1_{}", Ulid::new()),
            parent_principal,
            parent_api_key_id,
            issued_at: now,
            expires_at,
            narrowed,
            signer_node: self.signer_node.clone(),
        };
        let payload_bytes = serde_json::to_vec(&payload).map_err(|error| {
            SecurityError::InvalidPolicyRequest(format!(
                "delegated token payload serialization failed: {error}"
            ))
        })?;
        let signature = self.signing_key.sign(&payload_bytes);
        let token = format!(
            "{TOKEN_PREFIX}{}.{}",
            URL_SAFE_NO_PAD.encode(&payload_bytes),
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        );
        if token.len() > MAX_TOKEN_BYTES {
            return Err(SecurityError::InvalidPolicyRequest(
                "delegated token payload exceeds the maximum encoded size".to_string(),
            ));
        }
        Ok(IssuedDelegatedToken {
            token_id: payload.token_id,
            token,
            expires_at,
        })
    }
}

impl ObjectSigner for DelegationAuthority {
    fn signer_node(&self) -> &str {
        &self.signer_node
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        self.signing_key.sign(message).to_bytes().to_vec()
    }

    fn publication_store(&self) -> ZeppelinStore {
        self.verifier.signer_cache.store.clone()
    }
}

/// Synchronous request-path verification over bounded, refreshed signer keys.
pub(crate) struct DelegationVerifier {
    signer_cache: Arc<SignerCache>,
    policy_cache: Arc<PolicyCache>,
    clock: Clock,
    max_ttl: StdDuration,
    monotonic_wall: Mutex<MonotonicWall>,
}

struct MonotonicWall {
    wall: DateTime<Utc>,
    observed_at: Instant,
}

impl MonotonicWall {
    fn new(wall: DateTime<Utc>) -> Self {
        Self {
            wall,
            observed_at: Instant::now(),
        }
    }

    fn observe(&mut self, wall: DateTime<Utc>) -> DateTime<Utc> {
        let elapsed = Duration::from_std(self.observed_at.elapsed())
            .unwrap_or_else(|error| panic!("delegation monotonic elapsed time overflow: {error}"));
        let monotonic_now = self
            .wall
            .checked_add_signed(elapsed)
            .unwrap_or_else(|| panic!("delegation monotonic wall-clock overflow"));
        if wall > monotonic_now {
            self.wall = wall;
            self.observed_at = Instant::now();
            wall
        } else {
            monotonic_now
        }
    }
}

impl DelegationVerifier {
    fn effective_now(&self, observed_now: DateTime<Utc>) -> DateTime<Utc> {
        self.monotonic_wall
            .lock()
            .unwrap_or_else(|_| panic!("delegation monotonic wall-clock lock poisoned"))
            .observe(observed_now)
    }

    pub(crate) fn verify_authorization(
        &self,
        authorization: &str,
    ) -> (Result<Principal, AuthnFailure>, PolicyVersion, bool) {
        let current = self.policy_cache.current();
        let policy_version = current.policy.version();
        let policy_fresh = self.policy_cache.is_fresh(&current);
        let result = self.verify_authorization_against(authorization, &current, policy_fresh);
        (result, policy_version, policy_fresh)
    }

    fn verify_authorization_against(
        &self,
        authorization: &str,
        current: &CachedPolicy,
        policy_fresh: bool,
    ) -> Result<Principal, AuthnFailure> {
        let token = authorization
            .strip_prefix("Bearer ")
            .and_then(|value| value.strip_prefix(TOKEN_PREFIX))
            .ok_or(AuthnFailure::CredentialUnknown)?;
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthnFailure::CredentialUnknown);
        }
        let (payload_text, signature_text) = token
            .split_once('.')
            .filter(|(_, signature)| !signature.contains('.'))
            .ok_or(AuthnFailure::CredentialUnknown)?;
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_text)
            .map_err(|_| AuthnFailure::CredentialUnknown)?;
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature_text)
            .map_err(|_| AuthnFailure::CredentialUnknown)?;
        let signature =
            Signature::from_slice(&signature_bytes).map_err(|_| AuthnFailure::CredentialUnknown)?;
        let payload: DelegatedTokenPayload =
            serde_json::from_slice(&payload_bytes).map_err(|_| AuthnFailure::CredentialUnknown)?;
        let signer = self
            .signer_cache
            .signer(&payload.signer_node)
            .ok_or(AuthnFailure::CredentialUnknown)?;
        signer
            .verify(&payload_bytes, &signature)
            .map_err(|_| AuthnFailure::CredentialUnknown)?;
        validate_payload_shape(&payload)?;

        let now = self.effective_now(self.clock.now());
        let signed_ttl = payload
            .expires_at
            .signed_duration_since(payload.issued_at)
            .to_std()
            .map_err(|_| AuthnFailure::CredentialUnknown)?;
        if signed_ttl > self.max_ttl {
            return Err(AuthnFailure::CredentialUnknown);
        }
        if now < payload.issued_at || now >= payload.expires_at {
            return Err(AuthnFailure::CredentialExpired);
        }

        if !policy_fresh {
            return Err(AuthnFailure::CredentialUnknown);
        }
        if !current.policy.delegated_parent_credential_is_active(
            &payload.parent_principal,
            &payload.parent_api_key_id,
            now,
        ) {
            return Err(AuthnFailure::CredentialUnknown);
        }
        Ok(Principal::delegated(payload.into()))
    }
}

/// S3-authoritative, disposable public-key cache with the policy freshness bound.
struct SignerCache {
    store: ZeppelinStore,
    signers: RwLock<HashMap<String, VerifyingKey>>,
    last_confirmed: Mutex<Instant>,
    refresh_interval: StdDuration,
    refresh_abort: OnceLock<AbortHandle>,
}

impl SignerCache {
    fn start(
        store: ZeppelinStore,
        signers: HashMap<String, VerifyingKey>,
        refresh_interval: StdDuration,
    ) -> ZeppelinResult<Arc<Self>> {
        if refresh_interval.is_zero() {
            return Err(ZeppelinError::Config(
                "delegation signer refresh interval must be greater than zero".to_string(),
            ));
        }
        let cache = Arc::new(Self {
            store,
            signers: RwLock::new(signers),
            last_confirmed: Mutex::new(Instant::now()),
            refresh_interval,
            refresh_abort: OnceLock::new(),
        });
        let task = tokio::spawn(signer_refresh_loop(
            Arc::downgrade(&cache),
            refresh_interval,
        ));
        cache.refresh_abort.set(task.abort_handle()).map_err(|_| {
            ZeppelinError::Config(
                "delegation signer refresh runtime initialized more than once".to_string(),
            )
        })?;
        Ok(cache)
    }

    fn signer(&self, node_id: &str) -> Option<VerifyingKey> {
        if self
            .last_confirmed
            .lock()
            .unwrap_or_else(|_| panic!("delegation signer freshness lock poisoned"))
            .elapsed()
            > self.refresh_interval.saturating_mul(2)
        {
            return None;
        }
        self.signers
            .read()
            .unwrap_or_else(|_| panic!("delegation signer cache lock poisoned"))
            .get(node_id)
            .copied()
    }

    async fn refresh_once(&self) -> ZeppelinResult<()> {
        let signers = load_signers(&self.store).await?;
        *self
            .signers
            .write()
            .unwrap_or_else(|_| panic!("delegation signer cache lock poisoned")) = signers;
        *self
            .last_confirmed
            .lock()
            .unwrap_or_else(|_| panic!("delegation signer freshness lock poisoned")) =
            Instant::now();
        Ok(())
    }
}

impl Drop for SignerCache {
    fn drop(&mut self) {
        if let Some(abort) = self.refresh_abort.get() {
            abort.abort();
        }
    }
}

async fn signer_refresh_loop(cache: Weak<SignerCache>, refresh_interval: StdDuration) {
    loop {
        tokio::time::sleep(refresh_interval).await;
        let Some(cache) = cache.upgrade() else {
            return;
        };
        if let Err(error) = cache.refresh_once().await {
            tracing::warn!(error = %error, "delegation signer refresh failed");
        }
    }
}

fn validate_payload_shape(payload: &DelegatedTokenPayload) -> Result<(), AuthnFailure> {
    let token_ulid = payload
        .token_id
        .strip_prefix("zdt1_")
        .and_then(|value| Ulid::from_string(value).ok())
        .filter(|value| format!("zdt1_{value}") == payload.token_id);
    let canonical_narrowing = DelegationNarrowing::new(
        payload.narrowed.actions.clone(),
        payload.narrowed.namespaces.clone(),
        payload.narrowed.mandatory_filter.clone(),
        payload.narrowed.purpose.clone(),
    )
    .map_err(|_| AuthnFailure::CredentialUnknown)?;
    if token_ulid.is_none()
        || payload.signer_node.is_empty()
        || payload.expires_at <= payload.issued_at
        || canonical_narrowing != payload.narrowed
    {
        return Err(AuthnFailure::CredentialUnknown);
    }
    let unique_actions = payload
        .narrowed
        .actions
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let unique_namespaces = payload
        .narrowed
        .namespaces
        .iter()
        .map(NamespaceId::as_str)
        .collect::<BTreeSet<_>>();
    if unique_actions.len() != payload.narrowed.actions.len()
        || unique_namespaces.len() != payload.narrowed.namespaces.len()
    {
        return Err(AuthnFailure::CredentialUnknown);
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_signing_key(_path: &Path) -> ZeppelinResult<SigningKey> {
    Err(ZeppelinError::Config(
        "delegation signing-key permission enforcement is unsupported on this platform".to_string(),
    ))
}

#[cfg(unix)]
fn read_signing_key(path: &Path) -> ZeppelinResult<SigningKey> {
    if path.as_os_str().is_empty() {
        return Err(SecurityError::MissingDelegationSigningKey.into());
    }
    let file = std::fs::File::open(path).map_err(|error| {
        ZeppelinError::Config(format!(
            "delegation signing key open failed for {}: {error}",
            path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ZeppelinError::Config(format!(
            "delegation signing key metadata failed for {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() != 64 {
        return Err(SecurityError::InvalidDelegationSigningKey.into());
    }
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(SecurityError::DelegationSigningKeyPermissions.into());
    }
    let mut text = Vec::with_capacity(65);
    file.take(65).read_to_end(&mut text).map_err(|error| {
        ZeppelinError::Config(format!(
            "delegation signing key read failed for {}: {error}",
            path.display()
        ))
    })?;
    if text.len() != 64 || !text.iter().all(u8::is_ascii_hexdigit) {
        return Err(SecurityError::InvalidDelegationSigningKey.into());
    }
    let mut seed = [0_u8; 32];
    for (index, output) in seed.iter_mut().enumerate() {
        let pair = std::str::from_utf8(&text[index * 2..index * 2 + 2])
            .map_err(|_| SecurityError::InvalidDelegationSigningKey)?;
        *output =
            u8::from_str_radix(pair, 16).map_err(|_| SecurityError::InvalidDelegationSigningKey)?;
    }
    Ok(SigningKey::from_bytes(&seed))
}

fn signer_node_id(public_key: &VerifyingKey) -> String {
    let digest = Sha256::digest(public_key.as_bytes());
    format!("zsn1_{}", URL_SAFE_NO_PAD.encode(&digest[..18]))
}

async fn claim_signer_slot(store: &ZeppelinStore, node_id: &str) -> ZeppelinResult<()> {
    for slot in 0..MAX_PUBLISHED_SIGNERS {
        let slot = u8::try_from(slot).map_err(|_| {
            ZeppelinError::Config("delegation signer slot index overflowed".to_string())
        })?;
        let document = SignerSlot {
            slot,
            node_id: node_id.to_string(),
        };
        let body = serde_json::to_vec(&document).map_err(|error| {
            ZeppelinError::Config(format!(
                "delegation signer slot serialization failed: {error}"
            ))
        })?;
        let key = signer_slot_key(slot);
        match store
            .put_create_outcome(&key, Bytes::from(body.clone()))
            .await?
        {
            CreateOnlyOutcome::Created { .. } => return Ok(()),
            CreateOnlyOutcome::AlreadyExists => {
                let existing = store.get(&key).await?;
                let existing: SignerSlot = serde_json::from_slice(&existing).map_err(|error| {
                    ZeppelinError::Config(format!(
                        "invalid delegated-token signer slot {key}: {error}"
                    ))
                })?;
                validate_signer_slot(&key, &existing)?;
                if existing.node_id == node_id {
                    return Ok(());
                }
            }
        }
    }
    Err(ZeppelinError::Config(format!(
        "delegation signer inventory is full; retire an old signer before adding {node_id}"
    )))
}

fn signer_slot_key(slot: u8) -> String {
    format!("{SIGNER_SLOT_PREFIX}{slot:02}.json")
}

fn validate_signer_slot(key: &str, document: &SignerSlot) -> ZeppelinResult<()> {
    if usize::from(document.slot) >= MAX_PUBLISHED_SIGNERS
        || key != signer_slot_key(document.slot)
        || !document.node_id.starts_with("zsn1_")
    {
        return Err(ZeppelinError::Config(format!(
            "invalid delegated-token signer slot document: {key}"
        )));
    }
    Ok(())
}

async fn publish_signer(
    store: &ZeppelinStore,
    node_id: &str,
    public_key: &VerifyingKey,
) -> ZeppelinResult<()> {
    let document = SignerDocument {
        node_id: node_id.to_string(),
        public_key: URL_SAFE_NO_PAD.encode(public_key.as_bytes()),
    };
    let body = serde_json::to_vec(&document).map_err(|error| {
        ZeppelinError::Config(format!("delegation signer serialization failed: {error}"))
    })?;
    let key = format!("{SIGNER_PREFIX}{node_id}.json");
    match store
        .put_create_outcome(&key, Bytes::from(body.clone()))
        .await?
    {
        CreateOnlyOutcome::Created { .. } => Ok(()),
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&key).await?;
            if existing.as_ref() == body.as_slice() {
                Ok(())
            } else {
                Err(SecurityError::DelegationSignerCollision.into())
            }
        }
    }
}

async fn load_signers(store: &ZeppelinStore) -> ZeppelinResult<HashMap<String, VerifyingKey>> {
    let signers = load_signers_allow_empty(store).await?;
    if signers.is_empty() {
        return Err(SecurityError::InvalidDelegationSigner.into());
    }
    Ok(signers)
}

pub(crate) async fn verify_published_signature(
    store: &ZeppelinStore,
    node_id: &str,
    message: &[u8],
    signature: &[u8],
) -> ZeppelinResult<bool> {
    if !node_id.starts_with("zsn1_") {
        return Ok(false);
    }
    let publication_store = store
        .signer_publication_store()
        .unwrap_or_else(|| store.clone());
    let signers = load_signers_allow_empty(&publication_store).await?;
    let Some(verifying_key) = signers.get(node_id) else {
        return Ok(false);
    };
    let Ok(signature) = Signature::from_slice(signature) else {
        return Ok(false);
    };
    Ok(verifying_key.verify(message, &signature).is_ok())
}

async fn load_signers_allow_empty(
    store: &ZeppelinStore,
) -> ZeppelinResult<HashMap<String, VerifyingKey>> {
    let slot_keys = store.list_prefix(SIGNER_SLOT_PREFIX).await?;
    if slot_keys.len() > MAX_PUBLISHED_SIGNERS {
        return Err(ZeppelinError::Config(format!(
            "delegation signer inventory exceeds the {MAX_PUBLISHED_SIGNERS}-entry limit"
        )));
    }
    let mut signers = HashMap::new();
    for slot_key in slot_keys {
        let slot_body = store.get(&slot_key).await?;
        let slot: SignerSlot = serde_json::from_slice(&slot_body).map_err(|error| {
            ZeppelinError::Config(format!(
                "invalid delegated-token signer slot {slot_key}: {error}"
            ))
        })?;
        validate_signer_slot(&slot_key, &slot)?;
        let key = format!("{SIGNER_PREFIX}{}.json", slot.node_id);
        let body = store.get(&key).await?;
        let document: SignerDocument = serde_json::from_slice(&body).map_err(|error| {
            ZeppelinError::Config(format!("invalid delegated-token signer {key}: {error}"))
        })?;
        if document.node_id != slot.node_id {
            return Err(ZeppelinError::Config(format!(
                "delegated-token signer slot {slot_key} does not match signer document {key}"
            )));
        }
        if key != format!("{SIGNER_PREFIX}{}.json", document.node_id) {
            return Err(ZeppelinError::Config(format!(
                "delegated-token signer key does not match document: {key}"
            )));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(&document.public_key)
            .map_err(|_| SecurityError::InvalidDelegationSigner)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| SecurityError::InvalidDelegationSigner)?;
        let key =
            VerifyingKey::from_bytes(&bytes).map_err(|_| SecurityError::InvalidDelegationSigner)?;
        if document.node_id != signer_node_id(&key) {
            return Err(SecurityError::InvalidDelegationSigner.into());
        }
        if signers.insert(document.node_id, key).is_some() {
            return Err(SecurityError::InvalidDelegationSigner.into());
        }
    }
    Ok(signers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace(index: usize) -> NamespaceId {
        NamespaceId::new(format!("delegation-unit-{index}"))
            .unwrap_or_else(|error| panic!("unit namespace must validate: {error}"))
    }

    #[test]
    fn narrowing_rejects_global_actions_invalid_filters_and_oversized_scope() {
        assert!(matches!(
            DelegationNarrowing::new(
                vec![Action::SecurityAdminWrite],
                vec![namespace(0)],
                None,
                "must remain namespace-bound".to_string(),
            ),
            Err(SecurityError::InvalidPolicyRequest(_))
        ));
        assert!(matches!(
            DelegationNarrowing::new(
                vec![Action::Query],
                vec![namespace(0)],
                Some(Filter::And {
                    filters: Vec::new()
                }),
                "invalid empty filter".to_string(),
            ),
            Err(SecurityError::InvalidPolicyRequest(_))
        ));
        assert!(matches!(
            DelegationNarrowing::new(
                vec![Action::Query],
                (0..=MAX_DELEGATED_NAMESPACES).map(namespace).collect(),
                None,
                "too many namespaces".to_string(),
            ),
            Err(SecurityError::InvalidPolicyRequest(_))
        ));
    }

    #[test]
    fn narrowing_deserialization_cannot_bypass_canonical_constructor() {
        let duplicate = serde_json::json!({
            "actions": ["Query", "Query"],
            "namespaces": ["delegation-unit-0"],
            "purpose": "duplicates remain invalid"
        });
        assert!(serde_json::from_value::<DelegationNarrowing>(duplicate).is_err());
    }

    #[test]
    fn issued_token_debug_is_redacted() {
        let issued = IssuedDelegatedToken {
            token_id: "zdt1_01J00000000000000000000000".to_string(),
            token: "zpt1_secret-bearing-material.signature".to_string(),
            expires_at: Utc::now(),
        };
        let debug = format!("{issued:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-bearing-material"));
    }

    #[test]
    fn monotonic_wall_is_fixed_size_under_high_cardinality_observation() {
        let origin = Utc::now();
        let mut wall = MonotonicWall::new(origin);
        let size = std::mem::size_of_val(&wall);
        let mut prior = origin;
        for index in 0..10_000 {
            let observed = origin - Duration::milliseconds(index);
            let effective = wall.observe(observed);
            assert!(effective >= prior);
            prior = effective;
        }
        assert_eq!(std::mem::size_of_val(&wall), size);
    }

    #[cfg(unix)]
    #[test]
    fn signing_key_reader_rejects_noncanonical_bytes_and_non_files() {
        use std::os::unix::fs::PermissionsExt;

        let file = tempfile::NamedTempFile::new()
            .unwrap_or_else(|error| panic!("signing key fixture: {error}"));
        std::fs::write(file.path(), format!("{}\n", "11".repeat(32)))
            .unwrap_or_else(|error| panic!("whitespace key fixture write: {error}"));
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))
            .unwrap_or_else(|error| panic!("whitespace key fixture permissions: {error}"));
        assert!(read_signing_key(file.path()).is_err());

        let directory = tempfile::tempdir()
            .unwrap_or_else(|error| panic!("signing key directory fixture: {error}"));
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o600))
            .unwrap_or_else(|error| panic!("signing key directory permissions: {error}"));
        assert!(read_signing_key(directory.path()).is_err());
    }
}
