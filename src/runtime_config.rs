//! Runtime-mutable query configuration.

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

/// Partial update for runtime query knobs.
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
    /// Build query knob bounds from boot-time configuration.
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
    inner: RwLock<Arc<QueryKnobs>>,
}

impl RuntimeQueryConfig {
    /// Seed runtime query configuration from boot-time config.
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

    /// Return the current immutable query knob snapshot.
    ///
    /// This is one atomic read-lock acquire plus one `Arc` clone. Writes are
    /// admin-API-only and rare; this is the std-only ArcSwap idiom.
    #[must_use]
    pub fn snapshot(&self) -> Arc<QueryKnobs> {
        match self.inner.read() {
            Ok(guard) => guard.clone(),
            Err(_) => panic!("runtime query config lock poisoned"),
        }
    }

    /// Apply a validated partial update and return the new snapshot.
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
    use super::*;
    use crate::config::DEFAULT_RERANK_COALESCE_GAP_BYTES;

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
    fn patch_denies_unknown_fields() {
        let err = serde_json::from_str::<QueryKnobsPatch>(r#"{"unknown":1}"#).unwrap_err();

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
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
