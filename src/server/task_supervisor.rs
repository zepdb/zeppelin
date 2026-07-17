//! Owned lifecycle tracking for request-spawned authoritative mutation work.
//!
//! Manual compaction and post-audit namespace cleanup outlive the HTTP handler
//! that accepted them, but they must not outlive a retired server node. This
//! supervisor owns those task handles so graceful shutdown joins accepted work
//! and crash retirement aborts then joins it before a replacement starts.
//!
//! It intentionally does not own request-body draining or cache-only hydration:
//! those tasks do not own authoritative mutation capability. The adversarial
//! runner disables hydration, while production hydration remains independently
//! lifecycle-scoped to its queue handle.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::task::JoinSet;

/// Failure while retiring request-spawned authoritative work.
#[derive(Debug, Error)]
pub enum ServerTaskSupervisorError {
    /// One or more accepted tasks did not finish before the graceful deadline.
    #[error("timed out waiting for request-spawned authoritative tasks: {failures}")]
    DeadlineExceeded {
        /// Join failures observed while cancelling the remaining tasks.
        failures: String,
    },

    /// An accepted task panicked or otherwise failed before retirement completed.
    #[error("request-spawned authoritative task failed during shutdown: {failures}")]
    TaskFailed {
        /// Joined task failures, preserved for operator diagnostics.
        failures: String,
    },
}

#[derive(Default)]
struct SupervisorState {
    accepting: bool,
    tasks: JoinSet<&'static str>,
}

/// Shared owner for authoritative tasks detached from accepted HTTP requests.
#[derive(Clone)]
pub struct ServerTaskSupervisor {
    state: Arc<Mutex<SupervisorState>>,
}

impl ServerTaskSupervisor {
    /// Construct an accepting supervisor with no tracked tasks.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SupervisorState {
                accepting: true,
                tasks: JoinSet::new(),
            })),
        }
    }

    /// Spawn one request-originated authoritative mutation task under this owner.
    ///
    /// # Panics
    ///
    /// Panics when shutdown has begun. A task accepted after its server owner
    /// retired would violate the replacement boundary rather than being safely
    /// detached.
    pub fn spawn<F>(&self, name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|_| panic!("server task supervisor lock poisoned"));
        Self::reap_completed(&mut state);
        assert!(
            state.accepting,
            "server task supervisor retired before spawning {name}"
        );
        state.tasks.spawn(async move {
            future.await;
            name
        });
    }

    /// Stop admission and join all accepted tasks without cancelling them.
    pub async fn join(&self) -> Result<(), ServerTaskSupervisorError> {
        self.join_inner(None).await
    }

    /// Stop admission and join accepted tasks until `timeout` expires.
    ///
    /// Expiry is loud: the remaining authoritative tasks are aborted and
    /// joined before this returns an error, so none can leak into a later
    /// replacement lifecycle. The timeout bounds cooperative graceful waiting;
    /// the mandatory post-timeout cancellation barrier can take longer when a
    /// task is executing non-yielding work.
    pub async fn join_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(), ServerTaskSupervisorError> {
        self.join_until(tokio::time::Instant::now() + timeout).await
    }

    /// Stop admission and join accepted tasks using one caller-owned deadline.
    ///
    /// Process shutdown passes one absolute deadline through its request and
    /// background stages so each stage consumes the same configured graceful
    /// budget rather than resetting it.
    pub async fn join_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ServerTaskSupervisorError> {
        self.join_inner(Some(deadline)).await
    }

    async fn join_inner(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), ServerTaskSupervisorError> {
        let mut tasks = self.close_and_take();
        let mut failures = Vec::new();
        loop {
            let result = match deadline {
                Some(deadline) => {
                    tokio::select! {
                        result = tasks.join_next() => result,
                        _ = tokio::time::sleep_until(deadline) => {
                            tasks.abort_all();
                            while let Some(result) = tasks.join_next().await {
                                if let Err(error) = result {
                                    if !error.is_cancelled() {
                                        failures.push(error.to_string());
                                    }
                                }
                            }
                            return Err(ServerTaskSupervisorError::DeadlineExceeded {
                                failures: if failures.is_empty() {
                                    "remaining tasks were cancelled".to_string()
                                } else {
                                    failures.join("; ")
                                },
                            });
                        }
                    }
                }
                None => tasks.join_next().await,
            };
            let Some(result) = result else {
                break;
            };
            if let Err(error) = result {
                failures.push(error.to_string());
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ServerTaskSupervisorError::TaskFailed {
                failures: failures.join("; "),
            })
        }
    }

    /// Stop admission, abort all accepted tasks, and join every task.
    pub async fn abort_and_join(&self) -> Result<(), ServerTaskSupervisorError> {
        let mut tasks = self.close_and_take();
        tasks.abort_all();
        let mut failures = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(_) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ServerTaskSupervisorError::TaskFailed {
                failures: failures.join("; "),
            })
        }
    }

    /// Stop admission and abort tasks without awaiting their final cancellation.
    ///
    /// This is only a synchronous drop-path fallback. Ordered server retirement
    /// must use [`Self::abort_and_join`] instead.
    pub fn abort(&self) {
        let mut tasks = self.close_and_take();
        tasks.abort_all();
    }

    fn close_and_take(&self) -> JoinSet<&'static str> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|_| panic!("server task supervisor lock poisoned"));
        state.accepting = false;
        std::mem::take(&mut state.tasks)
    }

    fn reap_completed(state: &mut SupervisorState) {
        while let Some(result) = state.tasks.try_join_next() {
            if let Err(error) = result {
                panic!("request-spawned authoritative task failed before shutdown: {error}");
            }
        }
    }
}

impl Default for ServerTaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    struct DropMark(Arc<AtomicBool>);

    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn abort_and_join_cancels_a_paused_task_before_returning() {
        let supervisor = ServerTaskSupervisor::new();
        let (entered_tx, entered_rx) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_by_task = Arc::clone(&cancelled);
        supervisor.spawn("paused test task", async move {
            let _mark = DropMark(cancelled_by_task);
            entered_tx
                .send(())
                .expect("paused task entry receiver must remain live");
            std::future::pending::<()>().await;
        });
        entered_rx
            .await
            .expect("paused task must start before crash retirement");

        supervisor
            .abort_and_join()
            .await
            .expect("crash retirement must cancel and join tracked task");
        assert!(
            cancelled.load(Ordering::SeqCst),
            "abort_and_join returned before the paused task was cancelled"
        );
    }

    #[tokio::test]
    async fn join_completes_an_accepted_task() {
        let supervisor = ServerTaskSupervisor::new();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_by_task = Arc::clone(&completed);
        supervisor.spawn("completed test task", async move {
            completed_by_task.store(true, Ordering::SeqCst);
        });

        supervisor
            .join()
            .await
            .expect("graceful shutdown must join tracked task");
        assert!(
            completed.load(Ordering::SeqCst),
            "graceful join returned before the accepted task completed"
        );
    }

    #[tokio::test]
    async fn join_with_timeout_aborts_and_joins_a_stalled_task() {
        let supervisor = ServerTaskSupervisor::new();
        let (entered_tx, entered_rx) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_by_task = Arc::clone(&cancelled);
        supervisor.spawn("timed out test task", async move {
            let _mark = DropMark(cancelled_by_task);
            entered_tx
                .send(())
                .expect("timed out task entry receiver must remain live");
            std::future::pending::<()>().await;
        });
        entered_rx
            .await
            .expect("timed out task must start before graceful retirement");

        let error = supervisor
            .join_with_timeout(Duration::from_millis(20))
            .await
            .expect_err("stalled task must exhaust the graceful deadline");
        assert!(
            error.to_string().contains("timed out"),
            "timeout failure must identify the exhausted graceful deadline: {error}"
        );
        assert!(
            cancelled.load(Ordering::SeqCst),
            "timeout returned before the stalled task was aborted and joined"
        );
    }
}
