//! Pure hardware-to-configuration tuning rules.
//!
//! [`tune`] converts one selected instance, cache device, data shape, query
//! choice, and safety interval set into [`TunedKnobs`]. It does not read the
//! environment, generate secrets, access object storage, or write files. The
//! separate emitter renders these values and validates them through the real
//! configuration loader.

use std::path::PathBuf;

use thiserror::Error;

use crate::index::quantization::QuantizationType;

use super::advisor::{estimated_object_storage_gb, node_aggregate_mbps, DataShape};
use super::catalog::{Cloud, InstanceSku};
use super::rows::Quantization;

const MIB: usize = 1024 * 1024;
const DEFAULT_MANIFEST_CACHE_TTL_MS: u64 = 500;
const DEFAULT_NAMESPACE_REGISTRY_TTL_MS: u64 = 5_000;
const DEFAULT_COMPACTION_UPLOAD_WINDOW_SECS: u64 = 300;
const DEFAULT_GC_SKEW_SLOP_SECS: u64 = 5;
const DEFAULT_BM25_FULL_SCAN_VECTORS: usize = 100_000;
const REAL_STORE_RERANK_GAP_BYTES: usize = 2 * MIB;

/// A tuning request that cannot produce a valid production configuration.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TunerError {
    /// One caller-supplied value violates the tuning contract.
    #[error("invalid tuning request: {reason}")]
    InvalidRequest {
        /// Field-specific explanation.
        reason: String,
    },
    /// The sizing arithmetic cannot be represented by the configuration type.
    #[error("tuning arithmetic overflowed while computing {field}")]
    ArithmeticOverflow {
        /// Derived value that overflowed.
        field: &'static str,
    },
    /// The requested advisor quantization has no production config spelling.
    #[error("quantization {quantization} cannot be emitted as a production index config")]
    UnsupportedQuantization {
        /// Stable advisor quantization label.
        quantization: &'static str,
    },
}

/// Security posture to render into the generated configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityChoice {
    /// Require authentication and generate a fresh cursor HMAC key.
    Enforced,
    /// Deliberately permit anonymous local-development access.
    OpenUnsafe,
}

impl SecurityChoice {
    /// Stable TOML spelling.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Enforced => "enforced",
            Self::OpenUnsafe => "open_unsafe",
        }
    }
}

/// Selected disposable cache device on every query replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheSelection {
    /// Local NVMe whose cost is included in the instance price.
    Nvme {
        /// Usable advertised capacity in decimal GB.
        capacity_gb: u64,
    },
    /// Separately attached network block volume.
    Block {
        /// Provider tier name retained in generated comments.
        tier: String,
        /// Provisioned capacity per node in decimal GB.
        volume_gb: u64,
    },
}

impl CacheSelection {
    /// Stable operator-facing label.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Nvme { capacity_gb } => format!("nvme:{capacity_gb}GB"),
            Self::Block { tier, volume_gb } => format!("{tier}:{volume_gb}GB"),
        }
    }

    fn capacity_gb(&self) -> u64 {
        match self {
            Self::Nvme { capacity_gb } => *capacity_gb,
            Self::Block { volume_gb, .. } => *volume_gb,
        }
    }
}

/// Intervals that jointly determine the minimum safe GC horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SafetyIntervals {
    /// Manifest-cache TTL in milliseconds.
    pub manifest_cache_ttl_ms: u64,
    /// Positive namespace-registry cache TTL in milliseconds.
    pub namespace_registry_ttl_ms: u64,
    /// Minimum request timeout in seconds; the p99-derived timeout may raise it.
    pub minimum_request_timeout_secs: u64,
    /// Maximum upload-before-publication window in seconds.
    pub compaction_upload_window_secs: u64,
    /// Clock-skew allowance in seconds.
    pub skew_slop_secs: u64,
}

impl Default for SafetyIntervals {
    fn default() -> Self {
        Self {
            manifest_cache_ttl_ms: DEFAULT_MANIFEST_CACHE_TTL_MS,
            namespace_registry_ttl_ms: DEFAULT_NAMESPACE_REGISTRY_TTL_MS,
            minimum_request_timeout_secs: 30,
            compaction_upload_window_secs: DEFAULT_COMPACTION_UPLOAD_WINDOW_SECS,
            skew_slop_secs: DEFAULT_GC_SKEW_SLOP_SECS,
        }
    }
}

/// Complete inputs for one pure tuning pass.
#[derive(Debug, Clone)]
pub struct TuningRequest<'a> {
    /// Selected cloud.
    pub cloud: Cloud,
    /// Selected provider region.
    pub region: &'a str,
    /// Selected catalog instance.
    pub instance: &'a InstanceSku,
    /// Number of identical query replicas.
    pub replicas: usize,
    /// Source-of-truth bucket or container name.
    pub bucket: &'a str,
    /// Customer data shape.
    pub shape: DataShape,
    /// Stored-vector quantization.
    pub quantization: Quantization,
    /// Explicit IVF probe floor.
    pub nprobe: usize,
    /// Cache device on every replica.
    pub cache: CacheSelection,
    /// Predicted p99 latency in milliseconds for this exact hardware choice.
    pub predicted_p99_ms: f64,
    /// Selected security posture.
    pub security: SecurityChoice,
    /// GC reader-safety interval inputs.
    pub safety: SafetyIntervals,
}

/// Fully derived values consumed by the validated TOML emitter.
#[derive(Debug, Clone)]
pub struct TunedKnobs {
    pub(super) cloud: Cloud,
    pub(super) region: String,
    pub(super) instance_name: String,
    pub(super) replicas: usize,
    pub(super) bucket: String,
    pub(super) shape: DataShape,
    pub(super) cache: CacheSelection,
    pub(super) security: SecurityChoice,
    pub(super) quantization: QuantizationType,
    pub(super) pq_m: usize,
    pub(super) nlist: usize,
    pub(super) nprobe: usize,
    pub(super) max_nprobe: usize,
    pub(super) predicted_p99_ms: f64,
    pub(super) dataset_storage_gb: f64,
    pub(super) aggregate_mbps_per_node: f64,
    pub(super) max_concurrent_queries: usize,
    pub(super) max_request_body_mb: usize,
    pub(super) request_timeout_secs: u64,
    pub(super) cache_dir: PathBuf,
    pub(super) cache_max_size_gb: u64,
    pub(super) memory_cache_max_mb: usize,
    pub(super) wal_fragment_cache_max_mb: usize,
    pub(super) decoded_artifact_cache_max_mb: usize,
    pub(super) manifest_cache_ttl_ms: u64,
    pub(super) namespace_registry_ttl_ms: u64,
    pub(super) hydration_parallelism: usize,
    pub(super) hydration_max_segment_fraction: f64,
    pub(super) bm25_max_full_scan_vectors: usize,
    pub(super) max_wal_bytes_before_compact: u64,
    pub(super) rerank_coalesce_gap_bytes: usize,
    pub(super) gc_horizon_floor_secs: u64,
    pub(super) gc_horizon_secs: u64,
    pub(super) compaction_upload_window_secs: u64,
    pub(super) skew_slop_secs: u64,
    pub(super) compaction_workers: usize,
}

impl TunedKnobs {
    /// Canonical nlist derived through [`crate::config::IndexingConfig`].
    #[must_use]
    pub fn nlist(&self) -> usize {
        self.nlist
    }

    /// Tuned in-memory cluster-cache capacity in MB.
    #[must_use]
    pub fn memory_cache_max_mb(&self) -> usize {
        self.memory_cache_max_mb
    }

    /// Recomputed minimum safe GC horizon in seconds.
    #[must_use]
    pub fn gc_horizon_floor_secs(&self) -> u64 {
        self.gc_horizon_floor_secs
    }

    /// Emitted GC horizon in seconds.
    #[must_use]
    pub fn gc_horizon_secs(&self) -> u64 {
        self.gc_horizon_secs
    }

    /// Request timeout used as one GC-floor input.
    #[must_use]
    pub fn request_timeout_secs(&self) -> u64 {
        self.request_timeout_secs
    }
}

/// Derives production configuration values from one selected plan row.
///
/// # Errors
///
/// Returns [`TunerError`] for zero/invalid inputs, unsupported one-bit RaBitQ,
/// an instance unavailable in the selected region, or checked-arithmetic
/// overflow. No input is repaired or silently substituted.
pub fn tune(request: &TuningRequest<'_>) -> Result<TunedKnobs, TunerError> {
    validate_request(request)?;
    let indexing = crate::config::IndexingConfig::default();
    let nlist = indexing.effective_num_centroids(request.shape.vectors);
    if request.nprobe > nlist {
        return Err(invalid(format!(
            "nprobe {} exceeds canonical nlist {nlist}",
            request.nprobe
        )));
    }
    let (quantization, pq_m) = production_quantization(request.quantization, request.shape.dims)?;
    let dataset_storage_gb = estimated_object_storage_gb(request.shape, request.quantization);
    if !dataset_storage_gb.is_finite() || dataset_storage_gb <= 0.0 {
        return Err(invalid("estimated dataset size is not finite and positive"));
    }

    let mem_mb = (request.instance.mem_gb * 1024.0).floor() as usize;
    let wal_fragment_cache_max_mb = (mem_mb / 16).clamp(128, 2_048);
    let decoded_artifact_cache_max_mb = (mem_mb / 32).clamp(64, 1_024);
    let centroid_bytes = nlist
        .checked_mul(request.shape.dims)
        .and_then(|value| value.checked_mul(std::mem::size_of::<f32>()))
        .ok_or(TunerError::ArithmeticOverflow {
            field: "centroid residency",
        })?;
    let centroid_mb = centroid_bytes.div_ceil(MIB);
    let headroom_mb = 2_048usize
        .checked_add(wal_fragment_cache_max_mb)
        .and_then(|value| value.checked_add(decoded_artifact_cache_max_mb))
        .and_then(|value| value.checked_add(centroid_mb.saturating_mul(4)))
        .ok_or(TunerError::ArithmeticOverflow {
            field: "memory headroom",
        })?;
    let memory_cache_max_mb = mem_mb.saturating_sub(headroom_mb).clamp(256, mem_mb / 2);

    let cache_capacity_gb = request.cache.capacity_gb();
    let cache_max_size_gb = match request.cache {
        CacheSelection::Nvme { .. } => ((cache_capacity_gb as f64 * 0.80).floor() as u64).max(1),
        CacheSelection::Block { .. } => ((cache_capacity_gb as f64 * 0.90).floor() as u64).max(1),
    };
    let cache_dir = match request.cache {
        CacheSelection::Nvme { .. } => PathBuf::from("/mnt/zeppelin-cache"),
        CacheSelection::Block { .. } => PathBuf::from("/var/cache/zeppelin"),
    };
    let hydration_parallelism = (request.instance.vcpus / 2).clamp(4, 16);
    let hydration_max_segment_fraction = if cache_max_size_gb as f64 >= 2.0 * dataset_storage_gb {
        0.8
    } else {
        0.5
    };

    let p99_timeout_secs = (3.0 * request.predicted_p99_ms / 1_000.0).ceil() as u64;
    let request_timeout_secs = p99_timeout_secs
        .max(30)
        .max(request.safety.minimum_request_timeout_secs);
    let gc_horizon_floor_secs = gc_floor(request.safety, request_timeout_secs)?;
    let gc_horizon_secs = gc_horizon_floor_secs.max(900);
    let max_wal_mb = (mem_mb / 8).clamp(64, 512);
    let max_wal_bytes_before_compact =
        max_wal_mb
            .checked_mul(MIB)
            .ok_or(TunerError::ArithmeticOverflow {
                field: "compaction WAL bytes",
            })? as u64;

    Ok(TunedKnobs {
        cloud: request.cloud,
        region: request.region.to_string(),
        instance_name: request.instance.name.clone(),
        replicas: request.replicas,
        bucket: request.bucket.to_string(),
        shape: request.shape,
        cache: request.cache.clone(),
        security: request.security,
        quantization,
        pq_m,
        nlist,
        nprobe: request.nprobe,
        max_nprobe: request.nprobe.max(256),
        predicted_p99_ms: request.predicted_p99_ms,
        dataset_storage_gb,
        aggregate_mbps_per_node: node_aggregate_mbps(request.instance),
        max_concurrent_queries: request.instance.vcpus.saturating_mul(8).clamp(32, 64),
        max_request_body_mb: (mem_mb / 8).clamp(1, 512),
        request_timeout_secs,
        cache_dir,
        cache_max_size_gb,
        memory_cache_max_mb,
        wal_fragment_cache_max_mb,
        decoded_artifact_cache_max_mb,
        manifest_cache_ttl_ms: request.safety.manifest_cache_ttl_ms,
        namespace_registry_ttl_ms: request.safety.namespace_registry_ttl_ms,
        hydration_parallelism,
        hydration_max_segment_fraction,
        bm25_max_full_scan_vectors: if request.shape.fts {
            DEFAULT_BM25_FULL_SCAN_VECTORS.max(request.shape.vectors / 100)
        } else {
            DEFAULT_BM25_FULL_SCAN_VECTORS
        },
        max_wal_bytes_before_compact,
        rerank_coalesce_gap_bytes: REAL_STORE_RERANK_GAP_BYTES,
        gc_horizon_floor_secs,
        gc_horizon_secs,
        compaction_upload_window_secs: request.safety.compaction_upload_window_secs,
        skew_slop_secs: request.safety.skew_slop_secs,
        compaction_workers: (request.instance.vcpus / 4).max(1),
    })
}

fn validate_request(request: &TuningRequest<'_>) -> Result<(), TunerError> {
    if request.region.is_empty() {
        return Err(invalid("region must not be empty"));
    }
    if request.bucket.is_empty() {
        return Err(invalid("bucket must not be empty"));
    }
    if request.shape.vectors == 0 || request.shape.dims == 0 {
        return Err(invalid("vectors and dims must be nonzero"));
    }
    if request.replicas == 0 {
        return Err(invalid("replicas must be nonzero"));
    }
    if request.nprobe == 0 {
        return Err(invalid("nprobe must be nonzero"));
    }
    if request.instance.price_in(request.region).is_none() {
        return Err(invalid(format!(
            "instance {} has no price in {}",
            request.instance.name, request.region
        )));
    }
    if !request.predicted_p99_ms.is_finite() || request.predicted_p99_ms <= 0.0 {
        return Err(invalid("predicted p99 must be finite and positive"));
    }
    if request.cache.capacity_gb() == 0 {
        return Err(invalid("cache capacity must be nonzero"));
    }
    if let CacheSelection::Block { tier, .. } = &request.cache {
        if tier.is_empty() {
            return Err(invalid("block-storage tier must not be empty"));
        }
    }
    if request.safety.minimum_request_timeout_secs == 0 {
        return Err(invalid("minimum request timeout must be nonzero"));
    }
    if request.safety.compaction_upload_window_secs == 0 {
        return Err(invalid("compaction upload window must be nonzero"));
    }
    Ok(())
}

fn production_quantization(
    quantization: Quantization,
    dims: usize,
) -> Result<(QuantizationType, usize), TunerError> {
    match quantization {
        Quantization::F32 => Ok((QuantizationType::None, 8)),
        Quantization::Sq8 => Ok((QuantizationType::Scalar, 8)),
        Quantization::RabitqTwoBit => Ok((QuantizationType::TwoBit, 8)),
        Quantization::RabitqOneBit => Err(TunerError::UnsupportedQuantization {
            quantization: quantization.label(),
        }),
        Quantization::Pq { m } if m > 0 && dims % m == 0 => Ok((QuantizationType::Product, m)),
        Quantization::Pq { .. } => Err(invalid(
            "PQ subquantizers must be nonzero and divide dimensions",
        )),
    }
}

fn gc_floor(safety: SafetyIntervals, request_timeout_secs: u64) -> Result<u64, TunerError> {
    safety
        .namespace_registry_ttl_ms
        .div_ceil(1_000)
        .checked_add(safety.manifest_cache_ttl_ms.div_ceil(1_000))
        .and_then(|value| value.checked_add(request_timeout_secs))
        .and_then(|value| value.checked_add(safety.compaction_upload_window_secs))
        .and_then(|value| value.checked_add(safety.skew_slop_secs))
        .ok_or(TunerError::ArithmeticOverflow {
            field: "GC horizon floor",
        })
}

fn invalid(reason: impl Into<String>) -> TunerError {
    TunerError::InvalidRequest {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog::Catalog;
    use super::*;

    #[test]
    fn tuning_rules_follow_the_documented_aws_shape() {
        let catalog = Catalog::embedded();
        let instance = catalog
            .instance(Cloud::Aws, "i4i.2xlarge")
            .unwrap_or_else(|| panic!("missing i4i.2xlarge fixture"));
        let knobs = tune(&TuningRequest {
            cloud: Cloud::Aws,
            region: "us-east-1",
            instance,
            replicas: 3,
            bucket: "example-bucket",
            shape: DataShape {
                vectors: 21_000_000,
                dims: 768,
                filters: true,
                fts: true,
            },
            quantization: Quantization::RabitqTwoBit,
            nprobe: 256,
            cache: CacheSelection::Nvme {
                capacity_gb: instance.nvme_gb,
            },
            predicted_p99_ms: 250.0,
            security: SecurityChoice::Enforced,
            safety: SafetyIntervals::default(),
        })
        .unwrap_or_else(|error| panic!("tuning failed: {error}"));
        assert_eq!(knobs.nlist(), 4_096);
        assert_eq!(knobs.max_concurrent_queries, 64);
        assert_eq!(knobs.cache_max_size_gb, 1_500);
        assert_eq!(knobs.request_timeout_secs(), 30);
        assert!(knobs.gc_horizon_secs() >= knobs.gc_horizon_floor_secs());
        assert_eq!(knobs.rerank_coalesce_gap_bytes, 2 * MIB);
    }

    #[test]
    fn one_bit_rabitq_is_not_a_production_config_state() {
        let catalog = Catalog::embedded();
        let instance = catalog
            .instance(Cloud::Aws, "m7i.xlarge")
            .unwrap_or_else(|| panic!("missing m7i.xlarge fixture"));
        let error = tune(&TuningRequest {
            cloud: Cloud::Aws,
            region: "us-east-1",
            instance,
            replicas: 1,
            bucket: "example-bucket",
            shape: DataShape {
                vectors: 1_000_000,
                dims: 768,
                filters: false,
                fts: false,
            },
            quantization: Quantization::RabitqOneBit,
            nprobe: 32,
            cache: CacheSelection::Block {
                tier: "gp3".to_string(),
                volume_gb: 100,
            },
            predicted_p99_ms: 250.0,
            security: SecurityChoice::OpenUnsafe,
            safety: SafetyIntervals::default(),
        })
        .expect_err("one-bit tuning must fail");
        assert!(matches!(error, TunerError::UnsupportedQuantization { .. }));
    }
}
