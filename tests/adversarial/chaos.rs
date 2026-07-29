use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use zeppelin::storage::ZeppelinStore;

use super::faults::{ContractClass, FaultContract, ProtectedAssumption};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoreOp {
    Put,
    Get,
    Head,
    Delete,
    List,
    Copy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultMode {
    FailNthMatch { n: u32 },
    FailFirstK { k: u32 },
    Latency { ms: u64 },
    SilentDrop,
    PostCommitError { n: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaultSite {
    pub id: String,
    pub op: StoreOp,
    pub key_substring: String,
    pub mode: FaultMode,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaultPlan {
    pub sites: Vec<FaultSite>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiredFault {
    pub site_id: String,
    pub key: String,
    pub call_ordinal: u64,
    pub wall_ms: u64,
    pub mode: FaultMode,
}

#[derive(Clone, Debug)]
pub struct ChaosHandle {
    enabled: Arc<AtomicBool>,
    fired: Arc<Mutex<Vec<FiredFault>>>,
}

impl ChaosHandle {
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    #[must_use]
    pub fn fired(&self) -> Vec<FiredFault> {
        self.fired
            .lock()
            .expect("chaos fired-fault mutex poisoned")
            .clone()
    }
}

#[derive(Debug)]
struct RuntimeSite {
    site: FaultSite,
    matches: AtomicU64,
    fired: AtomicU64,
}

#[derive(Debug)]
pub struct ChaosStore {
    inner: Arc<dyn ObjectStore>,
    enabled: Arc<AtomicBool>,
    sites: Vec<RuntimeSite>,
    fired: Arc<Mutex<Vec<FiredFault>>>,
    started: Instant,
}

#[derive(Debug, Clone)]
enum FaultAction {
    Fail,
    Latency(u64),
    SilentDrop,
    PostCommit(PostCommitAction),
}

#[derive(Debug, Clone)]
struct PostCommitAction {
    site_id: String,
    call: u64,
    mode: FaultMode,
}

impl FaultPlan {
    /// Returns contract metadata for every enabled legacy-chaos site.
    ///
    /// Default generation is restricted to supported-v1 failure, latency, and
    /// post-commit ambiguity. Hand-authored/replayed `SilentDrop` plans remain
    /// available, but are labeled as provider-contract-abuse research.
    #[must_use]
    pub fn contracts(&self) -> Vec<FaultContract> {
        self.sites
            .iter()
            .filter(|site| site.enabled)
            .map(FaultSite::contract)
            .collect()
    }

    #[must_use]
    pub fn for_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed_fa17_cafe_babe);
        let sites = fault_catalog()
            .into_iter()
            .map(|(id, op, key_substring)| {
                let enabled = rng.gen_bool(0.25);
                let mode = match rng.gen_range(0..4) {
                    0 => FaultMode::FailNthMatch {
                        n: rng.gen_range(1..=3),
                    },
                    1 => FaultMode::FailFirstK {
                        k: rng.gen_range(1..=2),
                    },
                    2 => FaultMode::Latency {
                        ms: rng.gen_range(10..=75),
                    },
                    _ if matches!(op, StoreOp::Put | StoreOp::Delete | StoreOp::Copy) => {
                        FaultMode::PostCommitError {
                            n: rng.gen_range(1..=2),
                        }
                    }
                    _ => FaultMode::Latency {
                        ms: rng.gen_range(10..=75),
                    },
                };
                FaultSite {
                    id: id.to_string(),
                    op,
                    key_substring: key_substring.to_string(),
                    mode,
                    enabled,
                }
            })
            .collect();
        Self { sites }
    }

    #[must_use]
    pub fn lost_write_selftest() -> Self {
        Self {
            sites: vec![FaultSite {
                id: "chaos-lost-write".to_string(),
                op: StoreOp::Put,
                key_substring: ".wal".to_string(),
                mode: FaultMode::SilentDrop,
                enabled: true,
            }],
        }
    }

    #[must_use]
    pub fn post_commit_selftest(manifest_call: u32) -> Self {
        Self {
            sites: vec![FaultSite {
                id: "post-commit-lost-write".to_string(),
                op: StoreOp::Put,
                key_substring: "manifest.json".to_string(),
                mode: FaultMode::PostCommitError { n: manifest_call },
                enabled: true,
            }],
        }
    }
}

impl FaultSite {
    #[must_use]
    fn contract(&self) -> FaultContract {
        let (contract_class, violated_assumptions) = if matches!(self.mode, FaultMode::SilentDrop) {
            let assumption = if matches!(self.op, StoreOp::Put | StoreOp::Delete | StoreOp::Copy) {
                ProtectedAssumption::A1
            } else {
                ProtectedAssumption::A2
            };
            (ContractClass::ProviderContractAbuse, vec![assumption])
        } else {
            (ContractClass::SupportedV1, Vec::new())
        };
        FaultContract {
            event_id: self.id.clone(),
            contract_class,
            violated_assumptions,
        }
    }
}

fn fault_catalog() -> [(&'static str, StoreOp, &'static str); 10] {
    [
        ("put-manifest", StoreOp::Put, "manifest.json"),
        ("put-wal", StoreOp::Put, ".wal"),
        ("put-segment", StoreOp::Put, "segments/"),
        ("get-cluster", StoreOp::Get, "cluster_"),
        ("get-centroids", StoreOp::Get, "centroids"),
        ("get-bootstrap", StoreOp::Get, "bootstrap.bin"),
        ("get-sketch", StoreOp::Get, "coarse_sketch.bin"),
        ("copy-clone", StoreOp::Copy, "segments/"),
        ("delete-gc", StoreOp::Delete, "segments/"),
        ("list-gc", StoreOp::List, "/"),
    ]
}

pub fn chaos_store(store: &ZeppelinStore, plan: FaultPlan) -> (ZeppelinStore, ChaosHandle) {
    let enabled = Arc::new(AtomicBool::new(true));
    let fired = Arc::new(Mutex::new(Vec::new()));
    let sites = plan
        .sites
        .into_iter()
        .map(|site| RuntimeSite {
            site,
            matches: AtomicU64::new(0),
            fired: AtomicU64::new(0),
        })
        .collect();
    let chaos = ChaosStore {
        inner: store.inner(),
        enabled: Arc::clone(&enabled),
        sites,
        fired: Arc::clone(&fired),
        started: Instant::now(),
    };
    (
        ZeppelinStore::new(Arc::new(chaos)),
        ChaosHandle { enabled, fired },
    )
}

impl ChaosStore {
    fn action(&self, op: StoreOp, key: &str) -> Option<FaultAction> {
        if !self.enabled.load(Ordering::SeqCst) {
            return None;
        }
        for runtime in &self.sites {
            let site = &runtime.site;
            if !site.enabled || site.op != op || !key.contains(&site.key_substring) {
                continue;
            }
            let call = runtime.matches.fetch_add(1, Ordering::SeqCst) + 1;
            let action = match site.mode {
                FaultMode::FailNthMatch { n } => {
                    if call == u64::from(n)
                        && runtime
                            .fired
                            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        Some(FaultAction::Fail)
                    } else {
                        None
                    }
                }
                FaultMode::FailFirstK { k } => {
                    if call <= u64::from(k) {
                        runtime.fired.fetch_add(1, Ordering::SeqCst);
                        Some(FaultAction::Fail)
                    } else {
                        None
                    }
                }
                FaultMode::Latency { ms } => {
                    runtime.fired.fetch_add(1, Ordering::SeqCst);
                    Some(FaultAction::Latency(ms))
                }
                FaultMode::SilentDrop => {
                    if runtime
                        .fired
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        Some(FaultAction::SilentDrop)
                    } else {
                        None
                    }
                }
                FaultMode::PostCommitError { n } => {
                    if !matches!(op, StoreOp::Put | StoreOp::Delete | StoreOp::Copy) {
                        None
                    } else if call == u64::from(n)
                        && runtime
                            .fired
                            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        Some(FaultAction::PostCommit(PostCommitAction {
                            site_id: site.id.clone(),
                            call,
                            mode: site.mode.clone(),
                        }))
                    } else {
                        None
                    }
                }
            };
            if let Some(action) = action {
                if !matches!(action, FaultAction::PostCommit(_)) {
                    self.record(site, key, call);
                }
                return Some(action);
            }
        }
        None
    }

    fn record(&self, site: &FaultSite, key: &str, call: u64) {
        self.record_parts(&site.id, &site.mode, key, call);
    }

    fn record_post_commit(&self, action: &PostCommitAction, key: &str) {
        self.record_parts(&action.site_id, &action.mode, key, action.call);
    }

    fn record_parts(&self, site_id: &str, mode: &FaultMode, key: &str, call: u64) {
        self.fired
            .lock()
            .expect("chaos fired-fault mutex poisoned")
            .push(FiredFault {
                site_id: site_id.to_string(),
                key: key.to_string(),
                call_ordinal: call,
                wall_ms: self.started.elapsed().as_millis() as u64,
                mode: mode.clone(),
            });
    }
}

fn injected_error(key: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "adversarial_chaos",
        source: Box::new(std::io::Error::other(format!(
            "injected chaos fault for {key}"
        ))),
    }
}

async fn apply_action(action: FaultAction, key: &str) -> OsResult<Option<PutResult>> {
    match action {
        FaultAction::Fail => Err(injected_error(key)),
        FaultAction::Latency(ms) => {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            Ok(None)
        }
        FaultAction::SilentDrop => Ok(Some(PutResult {
            e_tag: Some("chaos-silent-drop".to_string()),
            version: None,
        })),
        FaultAction::PostCommit(_) => {
            panic!("post-commit actions must be applied after the inner mutation")
        }
    }
}

impl fmt::Display for ChaosStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChaosStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ChaosStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let key = location.to_string();
        if let Some(action) = self.action(StoreOp::Put, &key) {
            if let FaultAction::PostCommit(post_commit) = action {
                let result = self.inner.put_opts(location, payload, opts).await;
                return match result {
                    Ok(_) => {
                        self.record_post_commit(&post_commit, &key);
                        Err(injected_error(&key))
                    }
                    Err(error) => Err(error),
                };
            }
            if let Some(result) = apply_action(action, &key).await? {
                return Ok(result);
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let key = location.to_string();
        if let Some(action) = self.action(StoreOp::Get, &key) {
            let _ = apply_action(action, &key).await?;
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let key = location.to_string();
        if let Some(action) = self.action(StoreOp::Head, &key) {
            if !matches!(action, FaultAction::SilentDrop | FaultAction::PostCommit(_)) {
                let _ = apply_action(action, &key).await?;
            }
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let key = location.to_string();
        if let Some(action) = self.action(StoreOp::Delete, &key) {
            if let FaultAction::PostCommit(post_commit) = action {
                let result = self.inner.delete(location).await;
                return match result {
                    Ok(()) => {
                        self.record_post_commit(&post_commit, &key);
                        Err(injected_error(&key))
                    }
                    Err(error) => Err(error),
                };
            }
            let _ = apply_action(action, &key).await?;
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        if let Some(action) = self.action(StoreOp::List, &key) {
            match action {
                FaultAction::Fail => {
                    return stream::once(async move { Err(injected_error(&key)) }).boxed();
                }
                FaultAction::Latency(ms) => {
                    let mut inner = Some(self.inner.list(prefix));
                    return stream::once(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    })
                    .flat_map(move |()| inner.take().expect("delayed list stream reused"))
                    .boxed();
                }
                FaultAction::SilentDrop => {}
                FaultAction::PostCommit(_) => {}
            }
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        if let Some(action) = self.action(StoreOp::List, &key) {
            let _ = apply_action(action, &key).await?;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.action(StoreOp::Copy, &key) {
            if let FaultAction::PostCommit(post_commit) = action {
                let result = self.inner.copy(from, to).await;
                return match result {
                    Ok(()) => {
                        self.record_post_commit(&post_commit, &key);
                        Err(injected_error(&key))
                    }
                    Err(error) => Err(error),
                };
            }
            let _ = apply_action(action, &key).await?;
        }
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.action(StoreOp::Copy, &key) {
            if let FaultAction::PostCommit(post_commit) = action {
                let result = self.inner.copy_if_not_exists(from, to).await;
                return match result {
                    Ok(()) => {
                        self.record_post_commit(&post_commit, &key);
                        Err(injected_error(&key))
                    }
                    Err(error) => Err(error),
                };
            }
            let _ = apply_action(action, &key).await?;
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    use super::*;

    #[test]
    fn fault_catalog_covers_bootstrap_and_standalone_sketch_gets() {
        let catalog = fault_catalog();
        assert!(catalog.contains(&("get-bootstrap", StoreOp::Get, "bootstrap.bin")));
        assert!(catalog.contains(&("get-sketch", StoreOp::Get, "coarse_sketch.bin")));
    }

    #[test]
    fn generated_fault_plans_stay_within_supported_v1_contract() {
        for seed in 0..4_096 {
            let plan = FaultPlan::for_seed(seed);
            assert!(
                plan.sites
                    .iter()
                    .all(|site| !matches!(site.mode, FaultMode::SilentDrop)),
                "default LegacyChaos seed {seed} generated a SilentDrop"
            );
            assert!(
                plan.contracts().iter().all(|contract| {
                    contract.contract_class
                        == crate::adversarial::faults::ContractClass::SupportedV1
                }),
                "default LegacyChaos seed {seed} escaped the supported-v1 contract"
            );
        }
    }

    #[tokio::test]
    async fn post_commit_error_persists_put_and_fires_once() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let plan = FaultPlan {
            sites: vec![FaultSite {
                id: "post-commit-put".to_string(),
                op: StoreOp::Put,
                key_substring: ".wal".to_string(),
                mode: FaultMode::PostCommitError { n: 1 },
                enabled: true,
            }],
        };
        let (faulted, handle) = chaos_store(&inner, plan);

        let first = faulted
            .put("ns/first.wal", Bytes::from_static(b"durable"))
            .await;
        assert!(
            first.is_err(),
            "caller must lose the successful acknowledgement"
        );
        assert_eq!(
            inner.get("ns/first.wal").await.unwrap(),
            Bytes::from_static(b"durable"),
            "post-commit failure must happen after the inner write"
        );

        faulted
            .put("ns/second.wal", Bytes::from_static(b"acked"))
            .await
            .expect("latched fault must not fire twice");
        assert_eq!(handle.fired().len(), 1);
    }

    #[tokio::test]
    async fn disabled_chaos_does_not_consume_fault_ordinal_before_enable() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let plan = FaultPlan {
            sites: vec![FaultSite {
                id: "first-put".to_string(),
                op: StoreOp::Put,
                key_substring: "/".to_string(),
                mode: FaultMode::FailNthMatch { n: 1 },
                enabled: true,
            }],
        };
        let (faulted, handle) = chaos_store(&inner, plan);
        handle.disable();

        faulted
            .put("bootstrap/record", Bytes::from_static(b"ready"))
            .await
            .expect("disabled chaos must not fault harness bootstrap");
        assert!(handle.fired().is_empty());

        handle.enable();
        let result = faulted
            .put("workload/record", Bytes::from_static(b"fault"))
            .await;
        assert!(
            result.is_err(),
            "the first workload match must retain ordinal one"
        );
        assert_eq!(handle.fired().len(), 1);
    }
}
