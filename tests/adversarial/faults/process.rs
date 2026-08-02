use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::adversarial::chaos::StoreOp;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CrashPoint {
    WalFragmentPut,
    ManifestCas,
    SegmentPut,
    StagingSideObjectPut,
    StagingDrop,
    CloneCopy { nth: u32 },
    NamespaceDeleteBatch { nth: u32 },
    SnapshotPut,
    HydrationGet,
    LateSegmentArtifactPut,
    LateSectionPut,
}

impl CrashPoint {
    #[must_use]
    pub fn selector(self) -> (StoreOp, &'static str, u32) {
        match self {
            Self::WalFragmentPut => (StoreOp::Put, ".wal", 1),
            Self::ManifestCas => (StoreOp::Put, "manifest.json", 1),
            Self::SegmentPut => (StoreOp::Put, "segments/", 1),
            Self::StagingSideObjectPut => (StoreOp::Put, "/_staging/", 1),
            Self::StagingDrop => (StoreOp::Delete, "/_staging/", 1),
            Self::CloneCopy { nth } => (StoreOp::Copy, "segments/", nth),
            Self::NamespaceDeleteBatch { nth } => (StoreOp::Delete, "/", nth),
            Self::SnapshotPut => (StoreOp::Put, "/snapshots/", 1),
            Self::HydrationGet => (StoreOp::Get, "cluster_", 1),
            Self::LateSegmentArtifactPut => (StoreOp::Put, "late/segments/", 1),
            Self::LateSectionPut => (StoreOp::Put, "late/state/", 1),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TriggerPosition {
    Pre,
    Post,
}

#[derive(Debug, Clone)]
pub struct CrashRequest {
    pub event_id: String,
    pub op_index: u64,
    pub point: CrashPoint,
    pub position: TriggerPosition,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct ProcessController {
    pub crash_requested: Arc<Notify>,
    pub crash_armed: Arc<AtomicBool>,
    pub park_token: CancellationToken,
    request: Arc<Mutex<Option<CrashRequest>>>,
}

impl ProcessController {
    #[must_use]
    pub fn new() -> Self {
        Self {
            crash_requested: Arc::new(Notify::new()),
            crash_armed: Arc::new(AtomicBool::new(true)),
            park_token: CancellationToken::new(),
            request: Arc::new(Mutex::new(None)),
        }
    }

    pub fn request_crash(&self, request: CrashRequest) {
        if !self.crash_armed.swap(false, Ordering::SeqCst) {
            return;
        }
        let mut slot = self
            .request
            .lock()
            .expect("process crash request mutex poisoned");
        assert!(
            slot.is_none(),
            "process crash request slot already occupied"
        );
        *slot = Some(request);
        drop(slot);
        self.crash_requested.notify_one();
    }

    #[must_use]
    pub fn take_request(&self) -> CrashRequest {
        self.try_take_request()
            .expect("process crash notification had no request")
    }

    #[must_use]
    pub fn try_take_request(&self) -> Option<CrashRequest> {
        self.request
            .lock()
            .expect("process crash request mutex poisoned")
            .take()
    }
}

impl Default for ProcessController {
    fn default() -> Self {
        Self::new()
    }
}
