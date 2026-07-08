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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoreOp {
    Put,
    Get,
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
}

impl FaultPlan {
    #[must_use]
    pub fn for_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed_fa17_cafe_babe);
        let sites = fault_catalog()
            .into_iter()
            .map(|(id, op, key_substring)| {
                let enabled = rng.gen_bool(0.25);
                let mode = match rng.gen_range(0..3) {
                    0 => FaultMode::FailNthMatch {
                        n: rng.gen_range(1..=3),
                    },
                    1 => FaultMode::FailFirstK {
                        k: rng.gen_range(1..=2),
                    },
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
}

fn fault_catalog() -> [(&'static str, StoreOp, &'static str); 8] {
    [
        ("put-manifest", StoreOp::Put, "manifest.json"),
        ("put-wal", StoreOp::Put, ".wal"),
        ("put-segment", StoreOp::Put, "segments/"),
        ("get-cluster", StoreOp::Get, "cluster_"),
        ("get-centroids", StoreOp::Get, "centroids"),
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
            };
            if let Some(action) = action {
                self.record(site, key, call);
                return Some(action);
            }
        }
        None
    }

    fn record(&self, site: &FaultSite, key: &str, call: u64) {
        self.fired
            .lock()
            .expect("chaos fired-fault mutex poisoned")
            .push(FiredFault {
                site_id: site.id.clone(),
                key: key.to_string(),
                call_ordinal: call,
                wall_ms: self.started.elapsed().as_millis() as u64,
                mode: site.mode.clone(),
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
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let key = location.to_string();
        if let Some(action) = self.action(StoreOp::Delete, &key) {
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
            let _ = apply_action(action, &key).await?;
        }
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.action(StoreOp::Copy, &key) {
            let _ = apply_action(action, &key).await?;
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}
