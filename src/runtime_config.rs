//! Process-local, runtime-mutable query configuration.
//!
//! This module lets the administrative configuration API change a small set of
//! query defaults without restarting Zeppelin.
//! [`RuntimeQueryConfig`][crate::runtime_config::RuntimeQueryConfig] owns the
//! current immutable [`QueryKnobs`][crate::runtime_config::QueryKnobs] snapshot; query handlers borrow a cheap
//! reference-counted snapshot and continue using it even if a later update
//! publishes a replacement.
//!
//! Boot-time [`Config`][crate::config::Config] remains authoritative for validation bounds. Updates in
//! this module are process-local operational settings: they do not write S3,
//! publish a manifest, or persist across a restart.
//!
//! ## Reading map
//!
//! 1. [`QueryKnobs`][crate::runtime_config::QueryKnobs] is the complete snapshot
//!    consumed by query execution.
//! 2. [`QueryKnobsPatch`][crate::runtime_config::QueryKnobsPatch] is the partial
//!    administrative API payload.
//! 3. [`QueryKnobBounds`][crate::runtime_config::QueryKnobBounds] captures limits
//!    fixed at process startup.
//! 4. [`RuntimeQueryConfig::snapshot`][crate::runtime_config::RuntimeQueryConfig::snapshot]
//!    serves readers, while
//!    [`RuntimeQueryConfig::update`][crate::runtime_config::RuntimeQueryConfig::update]
//!    validates and publishes replacements.
//!
//! ## Snapshot lifecycle
//!
//! ```text
//! boot-time Config
//!        |
//!        v
//! Arc<QueryKnobs> behind RwLock
//!        |
//!        +---- snapshot() ----> query A keeps old Arc
//!        |
//!        +---- update() ------> replace current Arc
//!                                |
//!                                +---- query B sees new values
//!
//! query A remains valid until it drops its old Arc
//! ```
//!
//! ## Rust concepts used here
//!
//! `Arc<T>` provides shared ownership of an immutable snapshot. It is similar
//! to handing Java callers the same immutable object reference, but Rust tracks
//! the reference count explicitly and frees the value when the final owner is
//! dropped. In C, this pattern would require a reference-counted allocation and
//! disciplined retain/release calls. `RwLock` serializes replacement of the
//! current `Arc`; readers clone the `Arc`, release the lock, and need no lock
//! while using the snapshot.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::{rerank_coalesce_gap_bytes_for_profile, Config, CostLatencyProfile};
use crate::error::{Result, ZeppelinError};

/// Immutable snapshot of runtime-mutable query knobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryKnobs {
    /// Maximum gap, in bytes, between rerank f32 ranges merged into one GET.
    pub rerank_coalesce_gap_bytes: usize,
    /// Default IVF clusters to probe when a query omits `nprobe`.
    pub default_nprobe: usize,
    /// Default result count when a query omits `top_k`.
    pub default_top_k: usize,
    /// Maximum clusters for BM25 full-scan fallback; 0 disables the breaker.
    pub bm25_max_full_scan_clusters: usize,
    /// Maximum vectors for BM25 full-scan fallback; 0 disables the breaker.
    pub bm25_max_full_scan_vectors: usize,
}

/// Partial administrative update for runtime query knobs.
///
/// Every field is optional so one request can change only the intended knobs.
/// Deserialization rejects unknown fields rather than silently ignoring a
/// misspelling.
///
/// # Rust Notes for Java/C Engineers
///
/// `Option<T>` distinguishes “the request omitted this field” (`None`) from an
/// explicit value (`Some(value)`). Java commonly uses nullable fields or a
/// separate builder; C commonly pairs each value with a presence flag. Rust's
/// enum forces callers to handle both cases explicitly.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryKnobsPatch {
    /// Explicit rerank coalesce gap in bytes; 0 means only overlapping ranges merge.
    #[serde(default)]
    pub rerank_coalesce_gap_bytes: Option<usize>,
    /// Preset profile that resolves to a rerank coalesce gap.
    #[serde(default)]
    pub cost_latency_profile: Option<CostLatencyProfile>,
    /// Default IVF clusters to probe when a query omits `nprobe`.
    #[serde(default)]
    pub default_nprobe: Option<usize>,
    /// Default result count when a query omits `top_k`.
    #[serde(default)]
    pub default_top_k: Option<usize>,
    /// Maximum clusters for BM25 full-scan fallback; 0 disables the breaker.
    #[serde(default)]
    pub bm25_max_full_scan_clusters: Option<usize>,
    /// Maximum vectors for BM25 full-scan fallback; 0 disables the breaker.
    #[serde(default)]
    pub bm25_max_full_scan_vectors: Option<usize>,
}

/// Immutable bounds for runtime query knob validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryKnobBounds {
    /// Maximum accepted default `nprobe`.
    pub max_nprobe: usize,
    /// Maximum accepted default `top_k`.
    pub max_top_k: usize,
}

impl QueryKnobBounds {
    /// Builds immutable validation bounds from boot-time configuration.
    ///
    /// The runtime API may change defaults, but it may not expand the maximum
    /// work allowed by the process configuration.
    ///
    /// # Parameters
    ///
    /// - `config`: Validated startup configuration containing the maximum IVF
    ///   probe count and maximum result count.
    ///
    /// # Returns
    ///
    /// Returns the two upper bounds required by [`RuntimeQueryConfig::update`].
    ///
    /// # Examples
    ///
    /// If startup allows at most 64 probes and 100 results, runtime updates may
    /// choose smaller positive defaults but cannot raise either ceiling.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            max_nprobe: config.indexing.max_nprobe,
            max_top_k: config.server.max_top_k,
        }
    }
}

/// Runtime holder for query configuration snapshots.
pub struct RuntimeQueryConfig {
    /// Replaceable current snapshot protected only during pointer access.
    inner: RwLock<Arc<QueryKnobs>>,
}

impl RuntimeQueryConfig {
    /// Seeds the first runtime snapshot from boot-time configuration.
    ///
    /// The explicit query configuration and the selected cost/latency profile
    /// have already been reconciled by [`Config::effective_rerank_coalesce_gap_bytes`].
    ///
    /// # Parameters
    ///
    /// - `config`: Validated process configuration used to initialize all
    ///   runtime-mutable query defaults.
    ///
    /// # Returns
    ///
    /// Returns a process-local holder containing one immutable snapshot.
    ///
    /// # Examples
    ///
    /// With `default_nprobe = 16`, a query that omits `nprobe` initially uses
    /// 16 until an administrator publishes a different valid default.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            inner: RwLock::new(Arc::new(QueryKnobs {
                rerank_coalesce_gap_bytes: config.effective_rerank_coalesce_gap_bytes(),
                default_nprobe: config.indexing.default_nprobe,
                default_top_k: config.server.default_top_k,
                bm25_max_full_scan_clusters: config.indexing.bm25_max_full_scan_clusters,
                bm25_max_full_scan_vectors: config.indexing.bm25_max_full_scan_vectors,
            })),
        }
    }

    /// Returns the current immutable query-knob snapshot.
    ///
    /// This is one atomic read-lock acquire plus one `Arc` clone. Writes are
    /// admin-API-only and rare; this is the std-only ArcSwap idiom.
    ///
    /// # Returns
    ///
    /// Returns shared ownership of the snapshot that was current while the
    /// read lock was held. A concurrent later update cannot mutate this value.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the lock by panicking while publishing
    /// an update. Zeppelin fails loudly instead of silently serving an
    /// untrustworthy configuration state.
    ///
    /// # Performance
    ///
    /// Acquires one process-local read lock and increments one `Arc` reference
    /// count. Using the returned snapshot requires no further locking.
    ///
    /// # Examples
    ///
    /// A query may retain a snapshot with `default_top_k = 10` while an
    /// administrator changes the current default to 20. That in-flight query
    /// continues consistently with 10; the next query observes 20.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Cloning an `Arc<QueryKnobs>` does not clone `QueryKnobs`; it only creates
    /// another shared owner of the same immutable allocation. Java references
    /// are also cheap to copy but garbage-collected. C would need an explicit
    /// retain/release protocol to keep the allocation alive safely.
    #[must_use]
    pub fn snapshot(&self) -> Arc<QueryKnobs> {
        match self.inner.read() {
            Ok(guard) => guard.clone(),
            Err(_) => panic!("runtime query config lock poisoned"),
        }
    }

    /// Validates a partial update, publishes it, and returns the new snapshot.
    ///
    /// Omitted fields retain their current values. A direct rerank coalescing
    /// gap and a named cost/latency profile are mutually exclusive because both
    /// choose the same effective setting.
    ///
    /// # Parameters
    ///
    /// - `patch`: Administrative changes; each `None` field means “leave the
    ///   current value unchanged.”
    /// - `bounds`: Boot-time limits that runtime defaults must continue to
    ///   respect.
    ///
    /// # Returns
    ///
    /// Returns shared ownership of the newly published immutable snapshot.
    /// Existing snapshots remain valid and unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] when mutually exclusive settings
    /// are supplied or when `default_nprobe` or `default_top_k` is zero or above
    /// its boot-time maximum. Returns [`ZeppelinError::Config`] if a previous
    /// panic poisoned the write lock. Validation failures publish nothing.
    ///
    /// # Side Effects
    ///
    /// Replaces the process-local current snapshot, logs every changed field,
    /// and updates the rerank-gap Prometheus gauge. It does not write object
    /// storage or a namespace manifest.
    ///
    /// # Consistency
    ///
    /// All fields become visible together through a single `Arc` replacement;
    /// readers cannot observe a half-applied patch. Readers that already hold a
    /// snapshot deliberately retain the older, internally consistent values.
    ///
    /// # Performance
    ///
    /// Acquires one process-local write lock, clones the small snapshot, and
    /// allocates one replacement `Arc`. This is an infrequent administration
    /// path rather than a per-query operation.
    ///
    /// # Examples
    ///
    /// A patch containing only `default_nprobe = 32` preserves every other
    /// field. If the startup maximum is at least 32, subsequent snapshots use
    /// 32; if the maximum is 16, the function returns validation error and the
    /// current snapshot remains unchanged.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method implements copy-on-write manually: clone the plain data,
    /// modify the clone, wrap it in a new `Arc`, then swap the shared pointer.
    /// This resembles replacing an immutable configuration object held by an
    /// `AtomicReference` in Java. In C, callers would need synchronization plus
    /// reference counting to avoid freeing a snapshot still used by a query.
    pub fn update(
        &self,
        patch: QueryKnobsPatch,
        bounds: &QueryKnobBounds,
    ) -> Result<Arc<QueryKnobs>> {
        if patch.rerank_coalesce_gap_bytes.is_some() && patch.cost_latency_profile.is_some() {
            return Err(ZeppelinError::Validation(
                "rerank_coalesce_gap_bytes and cost_latency_profile are mutually exclusive".into(),
            ));
        }

        if let Some(default_nprobe) = patch.default_nprobe {
            if default_nprobe == 0 || default_nprobe > bounds.max_nprobe {
                return Err(ZeppelinError::Validation(format!(
                    "default_nprobe must be between 1 and {}",
                    bounds.max_nprobe
                )));
            }
        }
        if let Some(default_top_k) = patch.default_top_k {
            if default_top_k == 0 || default_top_k > bounds.max_top_k {
                return Err(ZeppelinError::Validation(format!(
                    "default_top_k must be between 1 and {}",
                    bounds.max_top_k
                )));
            }
        }

        let mut guard = self
            .inner
            .write()
            .map_err(|_| ZeppelinError::Config("runtime query config lock poisoned".to_string()))?;
        let old = guard.as_ref();
        let mut new = old.clone();

        if let Some(gap) = patch.rerank_coalesce_gap_bytes {
            new.rerank_coalesce_gap_bytes = gap;
        } else if let Some(profile) = patch.cost_latency_profile {
            new.rerank_coalesce_gap_bytes = rerank_coalesce_gap_bytes_for_profile(profile);
        }
        if let Some(default_nprobe) = patch.default_nprobe {
            new.default_nprobe = default_nprobe;
        }
        if let Some(default_top_k) = patch.default_top_k {
            new.default_top_k = default_top_k;
        }
        if let Some(limit) = patch.bm25_max_full_scan_clusters {
            new.bm25_max_full_scan_clusters = limit;
        }
        if let Some(limit) = patch.bm25_max_full_scan_vectors {
            new.bm25_max_full_scan_vectors = limit;
        }

        let new_snapshot = Arc::new(new);
        log_changes(old, new_snapshot.as_ref());
        *guard = new_snapshot.clone();
        crate::metrics::RERANK_COALESCE_GAP_BYTES
            .set(i64::try_from(new_snapshot.rerank_coalesce_gap_bytes).unwrap_or(i64::MAX));
        Ok(new_snapshot)
    }
}

/// Logs each query knob whose value changed between two snapshots.
///
/// # Parameters
///
/// - `old`: Snapshot that was current before an update.
/// - `new`: Fully validated replacement snapshot.
///
/// # Returns
///
/// Returns unit after checking all supported fields.
///
/// # Side Effects
///
/// Emits one structured `tracing` event per changed field. It does not publish
/// the new snapshot or update metrics.
///
/// # Examples
///
/// Changing only `default_top_k` produces one event containing the knob name,
/// old value, and new value; unchanged settings produce no events.
fn log_changes(old: &QueryKnobs, new: &QueryKnobs) {
    if old.rerank_coalesce_gap_bytes != new.rerank_coalesce_gap_bytes {
        info!(
            knob = "rerank_coalesce_gap_bytes",
            old_value = old.rerank_coalesce_gap_bytes,
            new_value = new.rerank_coalesce_gap_bytes,
            "runtime query knob updated"
        );
    }
    if old.default_nprobe != new.default_nprobe {
        info!(
            knob = "default_nprobe",
            old_value = old.default_nprobe,
            new_value = new.default_nprobe,
            "runtime query knob updated"
        );
    }
    if old.default_top_k != new.default_top_k {
        info!(
            knob = "default_top_k",
            old_value = old.default_top_k,
            new_value = new.default_top_k,
            "runtime query knob updated"
        );
    }
    if old.bm25_max_full_scan_clusters != new.bm25_max_full_scan_clusters {
        info!(
            knob = "bm25_max_full_scan_clusters",
            old_value = old.bm25_max_full_scan_clusters,
            new_value = new.bm25_max_full_scan_clusters,
            "runtime query knob updated"
        );
    }
    if old.bm25_max_full_scan_vectors != new.bm25_max_full_scan_vectors {
        info!(
            knob = "bm25_max_full_scan_vectors",
            old_value = old.bm25_max_full_scan_vectors,
            new_value = new.bm25_max_full_scan_vectors,
            "runtime query knob updated"
        );
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    //! Unit tests for runtime snapshot isolation, patch validation, and serde
    //! strictness.
    //!
    //! These tests use only process-local configuration; they do not require
    //! object storage or start the HTTP server.

    use super::*;
    use crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES;

    /// Builds a deterministic runtime holder and matching validation bounds.
    ///
    /// # Returns
    ///
    /// Returns a holder seeded with representative defaults and the bounds
    /// derived from the same boot-time configuration.
    ///
    /// # Examples
    ///
    /// Tests use the returned values to validate updates against a maximum of
    /// 64 IVF probes and 100 results.
    fn runtime_config() -> (RuntimeQueryConfig, QueryKnobBounds) {
        let mut config = Config::default();
        config.query.rerank_coalesce_gap_bytes = Some(DEFAULT_RERANK_COALESCE_GAP_BYTES);
        config.indexing.default_nprobe = 16;
        config.indexing.max_nprobe = 64;
        config.server.default_top_k = 10;
        config.server.max_top_k = 100;
        config.indexing.bm25_max_full_scan_clusters = 500;
        config.indexing.bm25_max_full_scan_vectors = 100_000;
        (
            RuntimeQueryConfig::from_config(&config),
            QueryKnobBounds::from_config(&config),
        )
    }

    #[test]
    /// Verifies that publishing a replacement does not mutate retained `Arc`s.
    ///
    /// The old snapshot remains at 16 probes while the returned and current
    /// snapshots advance to 32.
    fn snapshot_semantics_preserve_old_arc_after_update() {
        let (runtime, bounds) = runtime_config();
        let old = runtime.snapshot();

        let new = runtime
            .update(
                QueryKnobsPatch {
                    default_nprobe: Some(32),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            )
            .unwrap();

        assert_eq!(old.default_nprobe, 16);
        assert_eq!(new.default_nprobe, 32);
        assert_eq!(runtime.snapshot().default_nprobe, 32);
    }

    #[test]
    /// Verifies that invalid defaults and conflicting gap selectors fail closed.
    ///
    /// Each rejected patch demonstrates that zero, an excessive maximum, or
    /// two competing rerank-gap selectors cannot change runtime state.
    fn update_validation_rejects_out_of_bounds_defaults_and_conflicting_gap() {
        let (runtime, bounds) = runtime_config();

        assert!(matches!(
            runtime.update(
                QueryKnobsPatch {
                    default_nprobe: Some(0),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            ),
            Err(ZeppelinError::Validation(_))
        ));
        assert!(matches!(
            runtime.update(
                QueryKnobsPatch {
                    default_nprobe: Some(bounds.max_nprobe + 1),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            ),
            Err(ZeppelinError::Validation(_))
        ));
        assert!(matches!(
            runtime.update(
                QueryKnobsPatch {
                    default_top_k: Some(0),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            ),
            Err(ZeppelinError::Validation(_))
        ));
        assert!(matches!(
            runtime.update(
                QueryKnobsPatch {
                    default_top_k: Some(bounds.max_top_k + 1),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            ),
            Err(ZeppelinError::Validation(_))
        ));
        assert!(matches!(
            runtime.update(
                QueryKnobsPatch {
                    rerank_coalesce_gap_bytes: Some(0),
                    cost_latency_profile: Some(CostLatencyProfile::LowLatency),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            ),
            Err(ZeppelinError::Validation(_))
        ));
    }

    #[test]
    /// Verifies that misspelled administrative fields are rejected.
    ///
    /// Serde's `deny_unknown_fields` prevents a successful response that would
    /// otherwise leave an intended setting unchanged.
    fn patch_denies_unknown_fields() {
        let err = serde_json::from_str::<QueryKnobsPatch>(r#"{"unknown":1}"#).unwrap_err();

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    /// Verifies that a named cost/latency profile resolves to its concrete gap.
    ///
    /// Applying the low-latency profile produces the expected 128 KiB
    /// coalescing threshold in the published snapshot.
    fn profile_patch_updates_gap_to_mapped_value() {
        let (runtime, bounds) = runtime_config();

        let updated = runtime
            .update(
                QueryKnobsPatch {
                    cost_latency_profile: Some(CostLatencyProfile::LowLatency),
                    ..QueryKnobsPatch::default()
                },
                &bounds,
            )
            .unwrap();

        assert_eq!(updated.rerank_coalesce_gap_bytes, 128 * 1024);
    }
}
