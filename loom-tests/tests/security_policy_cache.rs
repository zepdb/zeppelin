//! Loom model for the Phase 3 security-policy cache protocol.
//!
//! Run with:
//! `RUSTFLAGS="--cfg loom" cargo test --manifest-path loom-tests/Cargo.toml --test security_policy_cache -- --nocapture`

#[cfg(loom)]
mod policy_cache_model {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::mpsc;
    use loom::sync::{Arc, RwLock};
    use loom::thread;

    const FRESHNESS_BOUND_TICKS: usize = 2;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PolicyObservation {
        head_version: usize,
        snapshot_version: usize,
        key_revoked: bool,
        confirmed_at: usize,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Authorization {
        Allow,
        DenyRevoked,
        DenyStale,
    }

    #[derive(Debug, Clone, Copy)]
    struct HeadConfirmation {
        version: usize,
        observed_at: usize,
    }

    struct AuthoritativeHead {
        version: AtomicUsize,
    }

    struct PolicyCacheModel {
        current: RwLock<Arc<PolicyObservation>>,
    }

    impl PolicyCacheModel {
        fn new() -> Self {
            Self {
                current: RwLock::new(Arc::new(PolicyObservation {
                    head_version: 1,
                    snapshot_version: 1,
                    key_revoked: false,
                    confirmed_at: 0,
                })),
            }
        }

        fn install(&self, version: usize, key_revoked: bool, cas_completed_at: usize) {
            let next = Arc::new(PolicyObservation {
                head_version: version,
                snapshot_version: version,
                key_revoked,
                confirmed_at: cas_completed_at,
            });
            let mut current = self.current.write().unwrap();
            if current.snapshot_version < version {
                *current = next;
            }
        }

        fn observe(&self) -> PolicyObservation {
            **self.current.read().unwrap()
        }

        fn begin_conditional_refresh(
            &self,
            head: &AuthoritativeHead,
            observed_at: usize,
        ) -> Option<HeadConfirmation> {
            let current = self.observe();
            let authoritative_version = head.version.load(Ordering::SeqCst);
            (authoritative_version == current.head_version).then_some(HeadConfirmation {
                version: authoritative_version,
                observed_at,
            })
        }

        fn finish_unchanged_refresh(&self, confirmation: HeadConfirmation) {
            let mut current = self.current.write().unwrap();
            if current.head_version == confirmation.version {
                *current = Arc::new(PolicyObservation {
                    confirmed_at: confirmation.observed_at,
                    ..**current
                });
            }
        }

        fn begin_changed_refresh(
            &self,
            head: &AuthoritativeHead,
            observed_at: usize,
        ) -> Option<HeadConfirmation> {
            let current = self.observe();
            let authoritative_version = head.version.load(Ordering::SeqCst);
            (authoritative_version != current.head_version).then_some(HeadConfirmation {
                version: authoritative_version,
                observed_at,
            })
        }

        fn finish_changed_snapshot_load(&self, confirmation: HeadConfirmation, key_revoked: bool) {
            let next = Arc::new(PolicyObservation {
                head_version: confirmation.version,
                snapshot_version: confirmation.version,
                key_revoked,
                confirmed_at: confirmation.observed_at,
            });
            let mut current = self.current.write().unwrap();
            if current.snapshot_version < confirmation.version {
                *current = next;
            }
        }

        fn authorize(&self, now: usize) -> Authorization {
            let current = self.observe();
            if now.saturating_sub(current.confirmed_at) > FRESHNESS_BOUND_TICKS {
                Authorization::DenyStale
            } else if current.key_revoked {
                Authorization::DenyRevoked
            } else {
                Authorization::Allow
            }
        }
    }

    impl AuthoritativeHead {
        fn new() -> Self {
            Self {
                version: AtomicUsize::new(1),
            }
        }

        fn publish(&self, version: usize) {
            self.version.store(version, Ordering::SeqCst);
        }
    }

    #[test]
    fn snapshot_swap_is_atomic_during_authorize_and_revoke_write_through() {
        loom::model(|| {
            let cache = Arc::new(PolicyCacheModel::new());

            let writer_cache = Arc::clone(&cache);
            let writer = thread::spawn(move || writer_cache.install(2, true, 0));

            let reader_cache = Arc::clone(&cache);
            let reader = thread::spawn(move || reader_cache.observe());

            writer.join().unwrap();
            let observed = reader.join().unwrap();

            assert_eq!(
                observed.head_version, observed.snapshot_version,
                "authorization observed a torn policy head/snapshot"
            );
            if observed.snapshot_version == 2 {
                assert!(
                    observed.key_revoked,
                    "authorization observed the revoked snapshot as open"
                );
            }
        });
    }

    #[test]
    fn completed_old_head_read_cannot_reopen_past_freshness_bound() {
        loom::model(|| {
            let cache = Arc::new(PolicyCacheModel::new());
            let head = Arc::new(AuthoritativeHead::new());
            let now = Arc::new(AtomicUsize::new(0));
            let (observed_tx, observed_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();

            let refresh_cache = Arc::clone(&cache);
            let refresh_head = Arc::clone(&head);
            let refresh_now = Arc::clone(&now);
            let refresh = thread::spawn(move || {
                let confirmation = refresh_cache
                    .begin_conditional_refresh(&refresh_head, refresh_now.load(Ordering::SeqCst))
                    .expect("initial head must be unchanged");
                observed_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                refresh_cache.finish_unchanged_refresh(confirmation);
            });

            observed_rx.recv().unwrap();
            head.publish(2);
            now.store(FRESHNESS_BOUND_TICKS + 1, Ordering::SeqCst);
            resume_tx.send(()).unwrap();

            let authorize_cache = Arc::clone(&cache);
            let authorize_now = Arc::clone(&now);
            let authorize = thread::spawn(move || {
                authorize_cache.authorize(authorize_now.load(Ordering::SeqCst))
            });

            refresh.join().unwrap();
            assert_eq!(authorize.join().unwrap(), Authorization::DenyStale);
        });
    }

    #[test]
    fn delayed_changed_snapshot_load_cannot_reopen_past_freshness_bound() {
        loom::model(|| {
            let cache = Arc::new(PolicyCacheModel::new());
            let head = Arc::new(AuthoritativeHead::new());
            let now = Arc::new(AtomicUsize::new(0));
            let (observed_tx, observed_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();

            head.publish(2);

            let refresh_cache = Arc::clone(&cache);
            let refresh_head = Arc::clone(&head);
            let refresh_now = Arc::clone(&now);
            let refresh = thread::spawn(move || {
                let confirmation = refresh_cache
                    .begin_changed_refresh(&refresh_head, refresh_now.load(Ordering::SeqCst))
                    .expect("version 2 head must differ from cached version 1");
                observed_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                refresh_cache.finish_changed_snapshot_load(confirmation, false);
            });

            observed_rx.recv().unwrap();
            head.publish(3);
            now.store(FRESHNESS_BOUND_TICKS + 1, Ordering::SeqCst);
            resume_tx.send(()).unwrap();

            let authorize_cache = Arc::clone(&cache);
            let authorize_now = Arc::clone(&now);
            let authorize = thread::spawn(move || {
                authorize_cache.authorize(authorize_now.load(Ordering::SeqCst))
            });

            refresh.join().unwrap();
            assert_eq!(authorize.join().unwrap(), Authorization::DenyStale);
        });
    }

    #[test]
    fn delayed_write_through_install_uses_cas_completion_origin() {
        loom::model(|| {
            let cache = Arc::new(PolicyCacheModel::new());
            let head = Arc::new(AuthoritativeHead::new());
            let now = Arc::new(AtomicUsize::new(0));
            let (published_tx, published_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();

            let writer_cache = Arc::clone(&cache);
            let writer_head = Arc::clone(&head);
            let writer_now = Arc::clone(&now);
            let writer = thread::spawn(move || {
                writer_head.publish(2);
                let cas_completed_at = writer_now.load(Ordering::SeqCst);
                published_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                writer_cache.install(2, false, cas_completed_at);
            });

            published_rx.recv().unwrap();
            head.publish(3);
            now.store(FRESHNESS_BOUND_TICKS + 1, Ordering::SeqCst);

            let authorize_cache = Arc::clone(&cache);
            let authorize_now = Arc::clone(&now);
            let authorize = thread::spawn(move || {
                authorize_cache.authorize(authorize_now.load(Ordering::SeqCst))
            });
            resume_tx.send(()).unwrap();

            writer.join().unwrap();
            assert_eq!(authorize.join().unwrap(), Authorization::DenyStale);
        });
    }
}

#[cfg(not(loom))]
#[test]
fn security_policy_cache_models_require_cfg_loom() {}
