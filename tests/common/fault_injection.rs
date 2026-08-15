//! Fault-injecting `ObjectStore` wrappers for storage failure tests.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::NamedSnapshot;

/// Controller for a toggleable matching-GET fault.
#[derive(Clone, Debug)]
pub struct GetFailureHandle {
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

impl GetFailureHandle {
    /// Begin failing every matching GET.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Stop injecting failures so recovery can be verified.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Return the exact number of failures injected so far.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that can fail all matching GET requests.
#[derive(Debug)]
pub struct ToggleGetFailureStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

/// Wrap a store in a disabled matching-GET fault that a test can toggle.
pub fn toggle_get_failure_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, GetFailureHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let wrapper = ToggleGetFailureStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        failures_injected: Arc::clone(&failures_injected),
    };
    (
        store.rewrap(Arc::new(wrapper)),
        GetFailureHandle {
            enabled,
            failures_injected,
        },
    )
}

/// Controller for a toggleable matching CAS-precondition fault.
#[derive(Clone, Debug)]
pub struct CasPreconditionFailureHandle {
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
}

impl CasPreconditionFailureHandle {
    /// Begin rejecting every matching ETag-update PUT as a CAS conflict.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Stop rejecting matching CAS writes so recovery can be verified.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Return the exact number of precondition failures injected so far.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that can reject matching ETag-update PUTs.
#[derive(Debug)]
pub struct ToggleCasPreconditionFailureStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    failures_injected: Arc<AtomicUsize>,
    enable_get_on_failure: Option<Arc<AtomicBool>>,
}

/// Controller for synchronizing the first two enabled matching CAS writes.
#[derive(Clone, Debug)]
pub struct CasPairBarrierHandle {
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    arrived: Arc<tokio::sync::Notify>,
    conflicts: Arc<AtomicUsize>,
}

impl CasPairBarrierHandle {
    /// Arm the two-writer barrier after single-writer setup mutations finish.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Return how many matching CAS calls reached the armed wrapper.
    #[must_use]
    pub fn arrivals(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }

    /// Wait until at least `expected` matching CAS calls reach the wrapper.
    pub async fn wait_until_arrivals(&self, expected: usize) {
        loop {
            let notified = self.arrived.notified();
            if self.arrivals.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    }

    /// Return how many inner CAS calls lost with an ETag precondition error.
    #[must_use]
    pub fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that makes two CAS writers race from the same base.
#[derive(Debug)]
pub struct CasPairBarrierStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    payload_needles: Option<(Vec<u8>, Vec<u8>)>,
    winner_payload_needle: Option<Vec<u8>>,
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    arrived: Arc<tokio::sync::Notify>,
    conflicts: Arc<AtomicUsize>,
    barrier: Arc<tokio::sync::Barrier>,
    winner_done: Arc<AtomicBool>,
    winner_done_notify: Arc<tokio::sync::Notify>,
}

/// Controller for a one-shot matching CAS publication pause.
#[derive(Clone, Debug)]
pub struct PauseCasHandle {
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

impl PauseCasHandle {
    /// Wait until the first matching CAS has reached the publication boundary.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused CAS to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

/// Controller for an explicitly armed one-shot matching CAS pause.
#[derive(Clone, Debug)]
pub struct ArmedPauseCasHandle {
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

impl ArmedPauseCasHandle {
    /// Pause the next matching ETag-update PUT before it reaches storage.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Wait until the armed matching CAS reaches the publication boundary.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused CAS to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

/// Object-store decorator that pauses the first matching ETag-update PUT.
#[derive(Debug)]
pub struct PauseCasStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    payload_needle: Option<Vec<u8>>,
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Controller for a one-shot matching GET pause.
#[derive(Clone, Debug)]
pub struct PauseGetHandle {
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

impl PauseGetHandle {
    /// Wait until the first matching GET is blocked before reaching storage.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused GET to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

/// Object-store decorator that pauses the first matching full-object GET.
#[derive(Debug)]
pub struct PauseGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Object-store decorator that pauses after the first matching GET snapshots storage.
#[derive(Debug)]
pub struct PauseAfterGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Controller for a repeatedly armed pause after matching GETs snapshot storage.
#[derive(Clone, Debug)]
pub struct RepeatedPauseAfterGetHandle {
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

impl RepeatedPauseAfterGetHandle {
    /// Begin pausing every matching GET after it has captured its result.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Stop pausing new matching GETs.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Wait until at least `expected` matching GET snapshots are paused.
    pub async fn wait_until_arrivals(&self, expected: usize) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    }

    /// Release one paused matching GET.
    pub fn release_next(&self) {
        self.release.add_permits(1);
    }

    /// Return the exact number of matching GET snapshots paused so far.
    #[must_use]
    pub fn arrivals(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that repeatedly pauses after matching GETs snapshot storage.
#[derive(Debug)]
pub struct RepeatedPauseAfterGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Controller for an explicitly armed one-shot matching GET pause.
///
/// Unlike [`PauseGetHandle`], this starts disarmed so a test can complete
/// server bootstrap before pinning a background refresh at the storage
/// boundary.
#[derive(Clone, Debug)]
pub struct ArmedPauseGetHandle {
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

impl ArmedPauseGetHandle {
    /// Pause the next matching GET before it reaches storage.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Wait until the armed matching GET is blocked before reaching storage.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused GET to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }

    /// Wait until the paused GET future has exited for any reason.
    pub async fn wait_until_exited(&self) {
        loop {
            let notified = self.exit_notify.notified();
            if self.exited.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    /// Return whether the paused GET was cancelled before it could reach S3.
    #[must_use]
    pub fn was_cancelled_before_storage(&self) -> bool {
        self.cancelled_before_storage.load(Ordering::SeqCst)
    }

    /// Return whether the paused GET future has exited.
    #[must_use]
    pub fn has_exited(&self) -> bool {
        self.exited.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that pauses the next explicitly armed matching GET.
#[derive(Debug)]
pub struct ArmedPauseGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

struct ArmedPauseGetFlight {
    reached_storage: bool,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

impl Drop for ArmedPauseGetFlight {
    fn drop(&mut self) {
        if !self.reached_storage {
            self.cancelled_before_storage.store(true, Ordering::SeqCst);
        }
        self.exited.store(true, Ordering::SeqCst);
        self.exit_notify.notify_waiters();
    }
}

/// Controller for an explicitly armed one-shot matching COPY pause.
#[derive(Clone, Debug)]
pub struct ArmedPauseCopyHandle {
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

impl ArmedPauseCopyHandle {
    /// Pause the next matching COPY before it reaches storage.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Wait until the armed matching COPY is blocked before reaching storage.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused COPY to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }

    /// Wait until the paused COPY future has exited for any reason.
    pub async fn wait_until_exited(&self) {
        loop {
            let notified = self.exit_notify.notified();
            if self.exited.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    /// Return whether the paused COPY was cancelled before it could reach S3.
    #[must_use]
    pub fn was_cancelled_before_storage(&self) -> bool {
        self.cancelled_before_storage.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that pauses the next explicitly armed matching COPY.
#[derive(Debug)]
pub struct ArmedPauseCopyStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    armed: Arc<AtomicBool>,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

struct ArmedPauseCopyFlight {
    reached_storage: bool,
    cancelled_before_storage: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
    exit_notify: Arc<tokio::sync::Notify>,
}

impl Drop for ArmedPauseCopyFlight {
    fn drop(&mut self) {
        if !self.reached_storage {
            self.cancelled_before_storage.store(true, Ordering::SeqCst);
        }
        self.exited.store(true, Ordering::SeqCst);
        self.exit_notify.notify_waiters();
    }
}

/// Controller for a one-shot matching create-only PUT pause.
#[derive(Clone, Debug)]
pub struct PauseCreateHandle {
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

impl PauseCreateHandle {
    /// Wait until the first matching create is blocked before reaching storage.
    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.entered.notified();
            if self.arrivals.load(Ordering::SeqCst) != 0 {
                return;
            }
            notified.await;
        }
    }

    /// Allow the paused create to reach the authoritative backend.
    pub fn release(&self) {
        self.release.add_permits(1);
    }
}

/// Object-store decorator that pauses the first matching create-only PUT.
#[derive(Debug)]
pub struct PauseCreateStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    arrivals: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

/// Controller for inspecting create-only calls that reached a matched key.
#[derive(Clone, Debug)]
pub struct CreateObservationHandle {
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
}

impl CreateObservationHandle {
    /// Return how many matching create-only calls reached the wrapper.
    #[must_use]
    pub fn arrivals(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }

    /// Return how many matching create-only calls lost to an existing object.
    #[must_use]
    pub fn conflicts(&self) -> usize {
        self.conflicts.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that counts create-only calls reaching one key.
#[derive(Debug)]
pub struct CreateObservationStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    arrivals: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
}

/// Wrap a store with a disabled deterministic two-CAS synchronization point.
pub fn synchronize_cas_pair_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CasPairBarrierHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let conflicts = Arc::new(AtomicUsize::new(0));
    let winner_done = Arc::new(AtomicBool::new(false));
    let winner_done_notify = Arc::new(tokio::sync::Notify::new());
    let wrapper = CasPairBarrierStore {
        inner: store.inner(),
        needle: needle.into(),
        payload_needles: None,
        winner_payload_needle: None,
        enabled: Arc::clone(&enabled),
        arrivals: Arc::clone(&arrivals),
        arrived: Arc::clone(&arrived),
        conflicts: Arc::clone(&conflicts),
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
        winner_done,
        winner_done_notify,
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasPairBarrierHandle {
            enabled,
            arrivals,
            arrived,
            conflicts,
        },
    )
}

/// Synchronize two exact CAS payload families and force one family to publish first.
///
/// Both writers reach the barrier with their already-read ETags before either
/// request reaches S3. The payload containing `winner_payload_needle` then
/// publishes first, and the other request is released only after that write
/// returns. This gives race tests both linearization orderings without sleeps.
pub fn synchronize_cas_pair_matching_payloads_with_winner(
    store: &ZeppelinStore,
    key_needle: impl Into<String>,
    first_payload_needle: impl Into<Vec<u8>>,
    second_payload_needle: impl Into<Vec<u8>>,
    winner_payload_needle: impl Into<Vec<u8>>,
) -> (ZeppelinStore, CasPairBarrierHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let conflicts = Arc::new(AtomicUsize::new(0));
    let winner_done = Arc::new(AtomicBool::new(false));
    let winner_done_notify = Arc::new(tokio::sync::Notify::new());
    let wrapper = CasPairBarrierStore {
        inner: store.inner(),
        needle: key_needle.into(),
        payload_needles: Some((first_payload_needle.into(), second_payload_needle.into())),
        winner_payload_needle: Some(winner_payload_needle.into()),
        enabled: Arc::clone(&enabled),
        arrivals: Arc::clone(&arrivals),
        arrived: Arc::clone(&arrived),
        conflicts: Arc::clone(&conflicts),
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
        winner_done,
        winner_done_notify,
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasPairBarrierHandle {
            enabled,
            arrivals,
            arrived,
            conflicts,
        },
    )
}

/// Wrap a store with a one-shot pause before the first matching CAS reaches S3.
pub fn pause_first_cas_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PauseCasHandle) {
    let armed = Arc::new(AtomicBool::new(true));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseCasStore {
        inner: store.inner(),
        needle: needle.into(),
        payload_needle: None,
        armed,
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        PauseCasHandle {
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with an initially disarmed one-shot pause before a matching
/// ETag-update PUT reaches S3.
pub fn pause_next_cas_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, ArmedPauseCasHandle) {
    let armed = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseCasStore {
        inner: store.inner(),
        needle: needle.into(),
        payload_needle: None,
        armed: Arc::clone(&armed),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        ArmedPauseCasHandle {
            armed,
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with an initially disarmed one-shot pause before a matching
/// ETag-update PUT whose payload contains the exact byte sequence.
pub fn pause_next_cas_matching_payload(
    store: &ZeppelinStore,
    needle: impl Into<String>,
    payload_needle: impl Into<Vec<u8>>,
) -> (ZeppelinStore, ArmedPauseCasHandle) {
    let armed = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseCasStore {
        inner: store.inner(),
        needle: needle.into(),
        payload_needle: Some(payload_needle.into()),
        armed: Arc::clone(&armed),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        ArmedPauseCasHandle {
            armed,
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with a one-shot pause before the first matching GET reaches S3.
pub fn pause_first_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PauseGetHandle) {
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseGetStore {
        inner: store.inner(),
        needle: needle.into(),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        PauseGetHandle {
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with a one-shot pause after the first matching GET reaches S3.
pub fn pause_first_after_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PauseGetHandle) {
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseAfterGetStore {
        inner: store.inner(),
        needle: needle.into(),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        PauseGetHandle {
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with a disabled repeatable pause after matching GET snapshots.
///
/// Tests can deterministically publish a newer object while a caller retains
/// the older returned bytes, then release that exact stale observation without
/// timing sleeps.
pub fn pause_repeatedly_after_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, RepeatedPauseAfterGetHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = RepeatedPauseAfterGetStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        RepeatedPauseAfterGetHandle {
            enabled,
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with an initially disarmed one-shot pause before a matching
/// GET reaches S3.
pub fn pause_next_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, ArmedPauseGetHandle) {
    let armed = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let cancelled_before_storage = Arc::new(AtomicBool::new(false));
    let exited = Arc::new(AtomicBool::new(false));
    let exit_notify = Arc::new(tokio::sync::Notify::new());
    let wrapper = ArmedPauseGetStore {
        inner: store.inner(),
        needle: needle.into(),
        armed: Arc::clone(&armed),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        cancelled_before_storage: Arc::clone(&cancelled_before_storage),
        exited: Arc::clone(&exited),
        exit_notify: Arc::clone(&exit_notify),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        ArmedPauseGetHandle {
            armed,
            arrivals,
            entered,
            release,
            cancelled_before_storage,
            exited,
            exit_notify,
        },
    )
}

/// Wrap a store with an initially disarmed one-shot pause before a matching
/// COPY reaches S3.
pub fn pause_next_copy_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, ArmedPauseCopyHandle) {
    let armed = Arc::new(AtomicBool::new(false));
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let cancelled_before_storage = Arc::new(AtomicBool::new(false));
    let exited = Arc::new(AtomicBool::new(false));
    let exit_notify = Arc::new(tokio::sync::Notify::new());
    let wrapper = ArmedPauseCopyStore {
        inner: store.inner(),
        needle: needle.into(),
        armed: Arc::clone(&armed),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        cancelled_before_storage: Arc::clone(&cancelled_before_storage),
        exited: Arc::clone(&exited),
        exit_notify: Arc::clone(&exit_notify),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        ArmedPauseCopyHandle {
            armed,
            arrivals,
            entered,
            release,
            cancelled_before_storage,
            exited,
            exit_notify,
        },
    )
}

/// Wrap a store with a one-shot pause before the first matching create reaches S3.
pub fn pause_first_create_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PauseCreateHandle) {
    let arrivals = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let wrapper = PauseCreateStore {
        inner: store.inner(),
        needle: needle.into(),
        arrivals: Arc::clone(&arrivals),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        PauseCreateHandle {
            arrivals,
            entered,
            release,
        },
    )
}

/// Wrap a store with a create-only observation point counting arrivals and conflicts.
///
/// Bootstrap publication leasing lets only the lease winner issue the matched
/// create-PUT; concurrent losers read the winner's published object instead of
/// arriving with a second create. Tests assert that leasing semantics through
/// the returned handle instead of synchronizing two arrivals.
pub fn observe_create_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CreateObservationHandle) {
    let arrivals = Arc::new(AtomicUsize::new(0));
    let conflicts = Arc::new(AtomicUsize::new(0));
    let wrapper = CreateObservationStore {
        inner: store.inner(),
        needle: needle.into(),
        arrivals: Arc::clone(&arrivals),
        conflicts: Arc::clone(&conflicts),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CreateObservationHandle {
            arrivals,
            conflicts,
        },
    )
}

/// Wrap a store in a disabled matching CAS-precondition fault that a test can toggle.
pub fn toggle_cas_precondition_failure_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CasPreconditionFailureHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let wrapper = ToggleCasPreconditionFailureStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        failures_injected: Arc::clone(&failures_injected),
        enable_get_on_failure: None,
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasPreconditionFailureHandle {
            enabled,
            failures_injected,
        },
    )
}

/// Fail the first policy CAS attempt, then fail the following matching reload GET.
pub fn fail_get_after_cas_conflict_matching(
    store: &ZeppelinStore,
    cas_needle: impl Into<String>,
    get_needle: impl Into<String>,
) -> (
    ZeppelinStore,
    CasPreconditionFailureHandle,
    GetFailureHandle,
) {
    let get_enabled = Arc::new(AtomicBool::new(false));
    let get_failures = Arc::new(AtomicUsize::new(0));
    let get_store = ToggleGetFailureStore {
        inner: store.inner(),
        needle: get_needle.into(),
        enabled: Arc::clone(&get_enabled),
        failures_injected: Arc::clone(&get_failures),
    };
    let cas_enabled = Arc::new(AtomicBool::new(true));
    let cas_failures = Arc::new(AtomicUsize::new(0));
    let cas_store = ToggleCasPreconditionFailureStore {
        inner: Arc::new(get_store),
        needle: cas_needle.into(),
        enabled: Arc::clone(&cas_enabled),
        failures_injected: Arc::clone(&cas_failures),
        enable_get_on_failure: Some(Arc::clone(&get_enabled)),
    };
    (
        ZeppelinStore::new(Arc::new(cas_store)),
        CasPreconditionFailureHandle {
            enabled: cas_enabled,
            failures_injected: cas_failures,
        },
        GetFailureHandle {
            enabled: get_enabled,
            failures_injected: get_failures,
        },
    )
}

/// Shared handle for inspecting an injected fail-once PUT rule.
#[derive(Clone, Debug)]
pub struct PutFailureHandle {
    failures_injected: Arc<AtomicUsize>,
}

impl PutFailureHandle {
    /// Number of PUT failures injected by the wrapped store.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails the first PUT whose key contains `needle`.
#[derive(Debug)]
pub struct FailPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailPutOnceStore {
    /// Wrap an existing store, failing the first matching PUT.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a fail-once PUT layer.
pub fn fail_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// Object-store decorator that fails the first matching GET after a successful
/// matching conditional PUT. This pins post-CAS reread hazards.
#[derive(Debug)]
pub struct FailGetAfterSuccessfulCasStore {
    inner: Arc<dyn ObjectStore>,
    cas_needle: String,
    get_needle: String,
    armed: AtomicBool,
    failures_injected: Arc<AtomicUsize>,
}

impl FailGetAfterSuccessfulCasStore {
    /// Wrap a store and arm one GET failure only after the matching CAS commits.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        cas_needle: impl Into<String>,
        get_needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                cas_needle: cas_needle.into(),
                get_needle: get_needle.into(),
                armed: AtomicBool::new(false),
                failures_injected,
            },
            handle,
        )
    }
}

/// Fail one preservation-head GET that occurs after a successful head CAS.
pub fn fail_get_after_successful_cas_matching(
    store: &ZeppelinStore,
    cas_needle: impl Into<String>,
    get_needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) =
        FailGetAfterSuccessfulCasStore::wrap(store.inner(), cas_needle, get_needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// Controller for one enabled CAS-ETag reconciliation failure.
#[derive(Clone, Debug)]
pub struct CasEtagReconciliationFailureHandle {
    enabled: Arc<AtomicBool>,
    etags_stripped: Arc<AtomicUsize>,
    failures_injected: Arc<AtomicUsize>,
}

impl CasEtagReconciliationFailureHandle {
    /// Arm the fault after startup has established the authoritative head.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Return the number of successful CAS responses whose ETag was removed.
    #[must_use]
    pub fn etags_stripped(&self) -> usize {
        self.etags_stripped.load(Ordering::SeqCst)
    }

    /// Return the number of reconciliation GET failures injected.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::SeqCst)
    }
}

/// Object-store decorator that removes one successful matching CAS ETag and
/// fails the exact-key GET used to reconcile that committed write.
#[derive(Debug)]
pub struct FailCasEtagReconciliationStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    enabled: Arc<AtomicBool>,
    remaining: AtomicUsize,
    armed: AtomicBool,
    etags_stripped: Arc<AtomicUsize>,
    failures_injected: Arc<AtomicUsize>,
}

/// Wrap a store in a disabled one-shot CAS-ETag reconciliation fault.
pub fn fail_cas_etag_reconciliation_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CasEtagReconciliationFailureHandle) {
    let enabled = Arc::new(AtomicBool::new(false));
    let etags_stripped = Arc::new(AtomicUsize::new(0));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let wrapper = FailCasEtagReconciliationStore {
        inner: store.inner(),
        needle: needle.into(),
        enabled: Arc::clone(&enabled),
        remaining: AtomicUsize::new(1),
        armed: AtomicBool::new(false),
        etags_stripped: Arc::clone(&etags_stripped),
        failures_injected: Arc::clone(&failures_injected),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        CasEtagReconciliationFailureHandle {
            enabled,
            etags_stripped,
            failures_injected,
        },
    )
}

/// Object-store decorator that fails the first matching DELETE.
#[derive(Debug)]
pub struct FailDeleteOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

/// Snapshot handle for every exact object key submitted to DELETE.
#[derive(Clone, Debug)]
pub struct DeleteRecorderHandle {
    deleted_keys: Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl DeleteRecorderHandle {
    /// Return DELETE attempts in the order they reached the storage boundary.
    #[must_use]
    pub async fn deleted_keys(&self) -> Vec<String> {
        self.deleted_keys.lock().await.clone()
    }

    /// Discard setup operations before observing the behavior under test.
    pub async fn reset(&self) {
        self.deleted_keys.lock().await.clear();
    }
}

/// Object-store decorator that records every exact DELETE attempt.
#[derive(Debug)]
pub struct RecordDeleteStore {
    inner: Arc<dyn ObjectStore>,
    deleted_keys: Arc<tokio::sync::Mutex<Vec<String>>>,
}

/// Wrap a store with a deterministic operation recorder for DELETE calls.
pub fn record_delete_operations(store: &ZeppelinStore) -> (ZeppelinStore, DeleteRecorderHandle) {
    let deleted_keys = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let wrapper = RecordDeleteStore {
        inner: store.inner(),
        deleted_keys: Arc::clone(&deleted_keys),
    };
    (
        ZeppelinStore::new(Arc::new(wrapper)),
        DeleteRecorderHandle { deleted_keys },
    )
}

impl FailDeleteOnceStore {
    /// Wrap an existing store, failing one matching DELETE before it acts.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }
}

/// Wrap a `ZeppelinStore` in a fail-once DELETE layer.
pub fn fail_delete_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailDeleteOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// Object-store decorator that loses the first successful matching DELETE reply.
#[derive(Debug)]
pub struct FailAfterDeleteOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailAfterDeleteOnceStore {
    /// Wrap an existing store and fail after one matching DELETE commits.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }
}

/// Wrap a store so one matching DELETE commits but its acknowledgement is lost.
pub fn fail_after_delete_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailAfterDeleteOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// `ObjectStore` decorator that delays every DELETE whose key contains `needle`.
#[derive(Debug)]
pub struct DelayDeleteStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    delay: Duration,
}

impl DelayDeleteStore {
    /// Wrap an existing store, delaying matching DELETEs before they begin.
    pub fn wrap(inner: Arc<dyn ObjectStore>, needle: impl Into<String>, delay: Duration) -> Self {
        Self {
            inner,
            needle: needle.into(),
            delay,
        }
    }
}

/// Wrap a `ZeppelinStore` in a matching-DELETE delay layer.
pub fn delay_delete_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
    delay: Duration,
) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(DelayDeleteStore::wrap(
        store.inner(),
        needle,
        delay,
    )))
}

/// `ObjectStore` decorator that delays every GET whose key contains `needle`.
#[derive(Debug)]
pub struct DelayGetStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    delay: Duration,
}

impl DelayGetStore {
    /// Wrap an existing store, delaying matching GETs before they begin.
    pub fn wrap(inner: Arc<dyn ObjectStore>, needle: impl Into<String>, delay: Duration) -> Self {
        Self {
            inner,
            needle: needle.into(),
            delay,
        }
    }
}

/// Wrap a `ZeppelinStore` in a matching-GET delay layer.
pub fn delay_get_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
    delay: Duration,
) -> ZeppelinStore {
    store.rewrap(Arc::new(DelayGetStore::wrap(store.inner(), needle, delay)))
}

/// `ObjectStore` decorator that reports one matching PUT as failed only after
/// the wrapped store has committed it.
#[derive(Debug)]
pub struct FailAfterPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailAfterPutOnceStore {
    /// Wrap an existing store, losing the first successful matching PUT reply.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a layer that loses one successful PUT reply.
pub fn fail_after_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (failing, handle) = FailAfterPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

/// `ObjectStore` decorator that acknowledges one matching PUT after writing
/// its payload to a sibling key instead of the requested destination.
#[derive(Debug)]
pub struct MisdirectPutOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl MisdirectPutOnceStore {
    /// Wrap an existing store, misdirecting the first matching PUT.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, PutFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = PutFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_misdirect(&self, location: &Path) -> bool {
        location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a one-shot misdirected PUT layer.
pub fn misdirect_put_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, PutFailureHandle) {
    let (misdirecting, handle) = MisdirectPutOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(misdirecting)), handle)
}

/// Shared handle for inspecting an injected fail-once COPY rule.
#[derive(Clone, Debug)]
pub struct CopyFailureHandle {
    failures_injected: Arc<AtomicUsize>,
}

impl CopyFailureHandle {
    /// Number of COPY failures injected by the wrapped store.
    #[must_use]
    pub fn failures_injected(&self) -> usize {
        self.failures_injected.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails the first copy whose source or
/// destination key contains `needle`.
#[derive(Debug)]
pub struct FailCopyOnceStore {
    inner: Arc<dyn ObjectStore>,
    needle: String,
    remaining: AtomicUsize,
    failures_injected: Arc<AtomicUsize>,
}

impl FailCopyOnceStore {
    /// Wrap an existing store, failing the first matching copy.
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        needle: impl Into<String>,
    ) -> (Self, CopyFailureHandle) {
        let failures_injected = Arc::new(AtomicUsize::new(0));
        let handle = CopyFailureHandle {
            failures_injected: Arc::clone(&failures_injected),
        };
        (
            Self {
                inner,
                needle: needle.into(),
                remaining: AtomicUsize::new(1),
                failures_injected,
            },
            handle,
        )
    }

    fn should_fail(&self, from: &Path, to: &Path) -> bool {
        (from.as_ref().contains(&self.needle) || to.as_ref().contains(&self.needle))
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }
}

/// Wrap a `ZeppelinStore` in a fail-once COPY layer.
pub fn fail_copy_once_matching(
    store: &ZeppelinStore,
    needle: impl Into<String>,
) -> (ZeppelinStore, CopyFailureHandle) {
    let (failing, handle) = FailCopyOnceStore::wrap(store.inner(), needle);
    (ZeppelinStore::new(Arc::new(failing)), handle)
}

impl fmt::Display for FailCopyOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailCopyOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailCopyOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        if self.should_fail(from, to) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_copy_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected copy failure from {from} to {to}"
                ))),
            });
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[derive(Clone, Debug)]
struct ExpectedSnapshot {
    namespace: String,
    generation: u64,
    name_prefix: String,
}

/// Shared handle for asserting a snapshot pin exists during clone copy.
#[derive(Clone, Debug)]
pub struct SnapshotOnCopyHandle {
    expected: Arc<Mutex<Option<ExpectedSnapshot>>>,
    observations: Arc<AtomicUsize>,
}

impl SnapshotOnCopyHandle {
    /// Expect copy operations to observe a snapshot pin for `generation`.
    pub fn expect_snapshot(
        &self,
        namespace: impl Into<String>,
        generation: u64,
        name_prefix: impl Into<String>,
    ) {
        *self.expected.lock().expect("snapshot expectation poisoned") = Some(ExpectedSnapshot {
            namespace: namespace.into(),
            generation,
            name_prefix: name_prefix.into(),
        });
    }

    /// Number of copy operations that observed the expected snapshot pin.
    #[must_use]
    pub fn observations(&self) -> usize {
        self.observations.load(Ordering::Relaxed)
    }
}

/// `ObjectStore` decorator that fails a copy if the expected snapshot pin is
/// not present while the copy starts.
#[derive(Debug)]
pub struct AssertSnapshotOnCopyStore {
    inner: Arc<dyn ObjectStore>,
    expected: Arc<Mutex<Option<ExpectedSnapshot>>>,
    observations: Arc<AtomicUsize>,
}

impl AssertSnapshotOnCopyStore {
    /// Wrap an existing store and return the assertion handle.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> (Self, SnapshotOnCopyHandle) {
        let expected = Arc::new(Mutex::new(None));
        let observations = Arc::new(AtomicUsize::new(0));
        let handle = SnapshotOnCopyHandle {
            expected: Arc::clone(&expected),
            observations: Arc::clone(&observations),
        };
        (
            Self {
                inner,
                expected,
                observations,
            },
            handle,
        )
    }
}

/// Wrap a `ZeppelinStore` in a snapshot-asserting COPY layer.
pub fn assert_snapshot_on_copy(store: &ZeppelinStore) -> (ZeppelinStore, SnapshotOnCopyHandle) {
    let (asserting, handle) = AssertSnapshotOnCopyStore::wrap(store.inner());
    (ZeppelinStore::new(Arc::new(asserting)), handle)
}

impl fmt::Display for AssertSnapshotOnCopyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AssertSnapshotOnCopyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for AssertSnapshotOnCopyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let expected = self
            .expected
            .lock()
            .expect("snapshot expectation poisoned")
            .clone();
        if let Some(expected) = expected {
            let store = ZeppelinStore::new(Arc::clone(&self.inner));
            let snapshots = NamedSnapshot::list(&store, &expected.namespace)
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "assert_snapshot_on_copy",
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            let found = snapshots.iter().any(|snapshot| {
                snapshot.generation == expected.generation
                    && snapshot.name.starts_with(&expected.name_prefix)
            });
            if !found {
                return Err(object_store::Error::Generic {
                    store: "assert_snapshot_on_copy",
                    source: Box::new(std::io::Error::other(format!(
                        "missing snapshot pin for {} generation {} during copy from {from} to {to}",
                        expected.namespace, expected.generation
                    ))),
                });
            }
            self.observations.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for FailPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailPutOnceStore({})", self.inner)
    }
}

impl fmt::Display for FailGetAfterSuccessfulCasStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailGetAfterSuccessfulCasStore({})", self.inner)
    }
}

impl fmt::Display for FailCasEtagReconciliationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailCasEtagReconciliationStore({})", self.inner)
    }
}

impl fmt::Display for FailDeleteOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailDeleteOnceStore({})", self.inner)
    }
}

impl fmt::Display for FailAfterDeleteOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailAfterDeleteOnceStore({})", self.inner)
    }
}

impl fmt::Display for ToggleGetFailureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToggleGetFailureStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ToggleGetFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        if self.enabled.load(Ordering::SeqCst) && location.as_ref().contains(&self.needle) {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "toggle_get_failure",
                source: Box::new(std::io::Error::other(format!(
                    "injected get failure for {location}"
                ))),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for ToggleCasPreconditionFailureStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToggleCasPreconditionFailureStore({})", self.inner)
    }
}

impl fmt::Display for CasPairBarrierStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CasPairBarrierStore({})", self.inner)
    }
}

impl fmt::Display for PauseCasStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PauseCasStore({})", self.inner)
    }
}

impl fmt::Display for PauseGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PauseGetStore({})", self.inner)
    }
}

impl fmt::Display for PauseAfterGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PauseAfterGetStore({})", self.inner)
    }
}

impl fmt::Display for RepeatedPauseAfterGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RepeatedPauseAfterGetStore({})", self.inner)
    }
}

impl fmt::Display for ArmedPauseGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArmedPauseGetStore({})", self.inner)
    }
}

impl fmt::Display for ArmedPauseCopyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArmedPauseCopyStore({})", self.inner)
    }
}

impl fmt::Display for PauseCreateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PauseCreateStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PauseCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let payload_matches = self
            .payload_needle
            .as_ref()
            .is_none_or(|needle| put_payload_contains(&payload, needle));
        let should_pause = location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Update(_))
            && payload_matches
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        if should_pause {
            self.arrivals.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            let permit = self
                .release
                .acquire()
                .await
                .expect("pause CAS semaphore must remain open");
            permit.forget();
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for PauseGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        let should_pause = location.as_ref().contains(&self.needle)
            && self.arrivals.fetch_add(1, Ordering::SeqCst) == 0;
        if should_pause {
            self.entered.notify_waiters();
            let permit = self
                .release
                .acquire()
                .await
                .expect("pause GET semaphore must remain open");
            permit.forget();
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for PauseAfterGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        let should_pause = location.as_ref().contains(&self.needle)
            && self.arrivals.fetch_add(1, Ordering::SeqCst) == 0;
        let result = self.inner.get_opts(location, options).await;
        if should_pause {
            self.entered.notify_waiters();
            let permit = self
                .release
                .acquire()
                .await
                .expect("pause-after GET semaphore must remain open");
            permit.forget();
        }
        result
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for RepeatedPauseAfterGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        let should_pause =
            self.enabled.load(Ordering::SeqCst) && location.as_ref().contains(&self.needle);
        let result = self.inner.get_opts(location, options).await;
        if should_pause {
            self.arrivals.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            let permit = self
                .release
                .acquire()
                .await
                .expect("repeated pause-after GET semaphore must remain open");
            permit.forget();
        }
        result
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for ArmedPauseGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        let should_pause = location.as_ref().contains(&self.needle)
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        if should_pause {
            self.arrivals.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            let mut flight = ArmedPauseGetFlight {
                reached_storage: false,
                cancelled_before_storage: Arc::clone(&self.cancelled_before_storage),
                exited: Arc::clone(&self.exited),
                exit_notify: Arc::clone(&self.exit_notify),
            };
            let permit = self
                .release
                .acquire()
                .await
                .expect("armed pause GET semaphore must remain open");
            permit.forget();
            flight.reached_storage = true;
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for ArmedPauseCopyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let should_pause = (from.as_ref().contains(&self.needle)
            || to.as_ref().contains(&self.needle))
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
        if should_pause {
            self.arrivals.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            let mut flight = ArmedPauseCopyFlight {
                reached_storage: false,
                cancelled_before_storage: Arc::clone(&self.cancelled_before_storage),
                exited: Arc::clone(&self.exited),
                exit_notify: Arc::clone(&self.exit_notify),
            };
            let permit = self
                .release
                .acquire()
                .await
                .expect("armed pause COPY semaphore must remain open");
            permit.forget();
            flight.reached_storage = true;
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for PauseCreateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let should_pause = location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Create)
            && self.arrivals.fetch_add(1, Ordering::SeqCst) == 0;
        if should_pause {
            self.entered.notify_waiters();
            let permit = self
                .release
                .acquire()
                .await
                .expect("pause create semaphore must remain open");
            permit.forget();
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for CreateObservationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CreateObservationStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CasPairBarrierStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let payload_matches = self
            .payload_needles
            .as_ref()
            .map_or(true, |(first, second)| {
                put_payload_contains(&payload, first) || put_payload_contains(&payload, second)
            });
        let synchronize = self.enabled.load(Ordering::SeqCst)
            && location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Update(_))
            && payload_matches;
        let ordered_winner = synchronize
            && self
                .winner_payload_needle
                .as_ref()
                .is_some_and(|needle| put_payload_contains(&payload, needle));
        if synchronize {
            let arrival = self.arrivals.fetch_add(1, Ordering::SeqCst);
            self.arrived.notify_waiters();
            if arrival < 2 {
                self.barrier.wait().await;
            }
            if self.winner_payload_needle.is_some() && !ordered_winner {
                loop {
                    let notified = self.winner_done_notify.notified();
                    if self.winner_done.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
            }
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if ordered_winner {
            self.winner_done.store(true, Ordering::SeqCst);
            self.winner_done_notify.notify_waiters();
        }
        if synchronize && matches!(&result, Err(object_store::Error::Precondition { .. })) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn put_payload_contains(payload: &PutPayload, needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    let body = payload
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect::<Vec<_>>();
    body.windows(needle.len()).any(|window| window == needle)
}

#[async_trait]
impl ObjectStore for CreateObservationStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let observe =
            location.as_ref().contains(&self.needle) && matches!(&opts.mode, PutMode::Create);
        if observe {
            self.arrivals.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if observe && matches!(&result, Err(object_store::Error::AlreadyExists { .. })) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for ToggleCasPreconditionFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.enabled.load(Ordering::SeqCst)
            && location.as_ref().contains(&self.needle)
            && matches!(&opts.mode, PutMode::Update(_))
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            if let Some(enabled) = &self.enable_get_on_failure {
                enabled.store(true, Ordering::SeqCst);
            }
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: Box::new(std::io::Error::other(format!(
                    "injected CAS precondition failure for {location}"
                ))),
            });
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for DelayDeleteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayDeleteStore({})", self.inner)
    }
}

impl fmt::Display for DelayGetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayGetStore({})", self.inner)
    }
}

impl fmt::Display for FailAfterPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailAfterPutOnceStore({})", self.inner)
    }
}

impl fmt::Display for MisdirectPutOnceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MisdirectPutOnceStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MisdirectPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.should_misdirect(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            let redirected = Path::from(format!("{location}.misdirected"));
            let mut redirected_opts = opts;
            redirected_opts.mode = PutMode::Overwrite;
            return self
                .inner
                .put_opts(&redirected, payload, redirected_opts)
                .await;
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for FailAfterPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        if self.should_fail(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_after_put_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected lost acknowledgement after put for {location}"
                ))),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for FailPutOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.should_fail(location) {
            self.failures_injected.fetch_add(1, Ordering::Relaxed);
            return Err(object_store::Error::Generic {
                store: "fail_put_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected put failure for {location}"
                ))),
            });
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for FailGetAfterSuccessfulCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let arm = location.as_ref().contains(&self.cas_needle)
            && matches!(&opts.mode, PutMode::Update(_));
        let result = self.inner.put_opts(location, payload, opts).await;
        if arm && result.is_ok() {
            self.armed.store(true, Ordering::SeqCst);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if location.as_ref().contains(&self.get_needle)
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "fail_get_after_successful_cas",
                source: Box::new(std::io::Error::other(format!(
                    "injected post-CAS GET failure for {location}"
                ))),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for FailCasEtagReconciliationStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let matching_update =
            location.as_ref().contains(&self.needle) && matches!(&opts.mode, PutMode::Update(_));
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        if matching_update
            && self.enabled.load(Ordering::SeqCst)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            result.e_tag = None;
            self.etags_stripped.fetch_add(1, Ordering::SeqCst);
            self.armed.store(true, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if location.as_ref().contains(&self.needle)
            && self
                .armed
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "fail_cas_etag_reconciliation",
                source: Box::new(std::io::Error::other(format!(
                    "injected CAS ETag reconciliation GET failure for {location}"
                ))),
            });
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for RecordDeleteStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.deleted_keys.lock().await.push(location.to_string());
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for RecordDeleteStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RecordDeleteStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FailDeleteOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        if location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "fail_delete_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected delete failure for {location}"
                ))),
            });
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for FailAfterDeleteOnceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await?;
        if location.as_ref().contains(&self.needle)
            && self
                .remaining
                .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.failures_injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "fail_after_delete_once",
                source: Box::new(std::io::Error::other(format!(
                    "injected lost acknowledgement after delete for {location}"
                ))),
            });
        }
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for DelayDeleteStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        if location.as_ref().contains(&self.needle) {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[async_trait]
impl ObjectStore for DelayGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
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
        if location.as_ref().contains(&self.needle) {
            tokio::time::sleep(self.delay).await;
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use zeppelin::storage::{ConditionalPutOutcome, ZeppelinStore};

    use super::toggle_cas_precondition_failure_matching;

    /// The CAS matchers key on `PutMode::Update(_)`, not on which token field
    /// is populated — a generation-style token (backend version, no ETag)
    /// must trigger injection exactly like an ETag token, or GCS-substrate
    /// fault tests would silently stop injecting.
    #[tokio::test]
    async fn cas_matchers_trigger_for_backend_version_tokens() {
        let base = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key = "catalog/manifest.json";
        base.put(key, Bytes::from_static(b"v1"))
            .await
            .expect("seed CAS object");
        let (faulted, handle) = toggle_cas_precondition_failure_matching(&base, "catalog/");
        handle.enable();

        let generation_token =
            zeppelin::storage::StorageVersion::from_parts(None, Some("12345".to_string()))
                .expect("a backend-version-only token is constructible");
        assert_eq!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v2"), &generation_token)
                .await
                .expect("injected precondition must be a typed CAS conflict"),
            ConditionalPutOutcome::Conflict
        );
        assert_eq!(handle.failures_injected(), 1);
        assert_eq!(
            faulted.get(key).await.expect("read after conflict"),
            b"v1"[..]
        );
    }

    #[tokio::test]
    async fn matching_cas_precondition_failure_can_be_enabled_and_recovered() {
        let base = ZeppelinStore::new(Arc::new(InMemory::new()));
        let key = "_security/heads/policy.json";
        base.put(key, Bytes::from_static(b"v1"))
            .await
            .expect("seed CAS object");
        let (_, initial_etag) = base
            .get_with_meta(key)
            .await
            .expect("read initial CAS version");
        let initial_etag = initial_etag.expect("in-memory store must return an ETag");
        let (faulted, handle) = toggle_cas_precondition_failure_matching(&base, "_security/heads/");

        assert!(matches!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v2"), &initial_etag)
                .await
                .expect("disabled fault must forward CAS"),
            ConditionalPutOutcome::Updated { .. }
        ));

        let (_, current_etag) = faulted
            .get_with_meta(key)
            .await
            .expect("read forwarded CAS version");
        let current_etag = current_etag.expect("in-memory store must return an ETag");
        handle.enable();
        assert_eq!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v3"), &current_etag)
                .await
                .expect("injected precondition must be a typed CAS conflict"),
            ConditionalPutOutcome::Conflict
        );
        assert_eq!(handle.failures_injected(), 1);
        assert_eq!(
            faulted.get(key).await.expect("read after conflict"),
            b"v2"[..]
        );

        handle.disable();
        assert!(matches!(
            faulted
                .put_if_match_outcome(key, Bytes::from_static(b"v3"), &current_etag)
                .await
                .expect("disabled fault must recover"),
            ConditionalPutOutcome::Updated { .. }
        ));
        assert_eq!(handle.failures_injected(), 1);
        assert_eq!(
            faulted.get(key).await.expect("read recovered body"),
            b"v3"[..]
        );
    }
}
