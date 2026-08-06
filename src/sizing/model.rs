//! Analytic per-query cost, throughput, latency, and dollar prediction.
//!
//! [`predict`] converts modeled per-query GET/PUT counters and serial GET
//! stages into QPS, latency percentiles, and cost under a [`Profile`]. The
//! math is a verbatim promotion of the Tier 2 perf-contract model and is
//! calibrated by that suite against two measured ground-truth fixtures
//! (GT-A QPS residual <= 10%, GT-B QPS residual <= 20%); changing any
//! formula here re-opens that validation.
//!
//! [`CalibratedShapeModel`] carries the constants fitted from an
//! instrumented small-shape run and scales them to arbitrary target shapes
//! (`vectors x dims`, `nlist`, `nprobe`, stored row bytes). The fit itself
//! lives in the perf-contract test tree because it consumes test-only
//! instrumentation; this module only consumes its serialized output.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::profiles::Profile;

const BYTES_PER_MB: f64 = 1_000_000.0;
const BYTES_PER_GIB: f64 = 1_073_741_824.0;

/// Modeled per-query operation and byte totals for one artifact class.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModeledClassStats {
    /// GET operations per query.
    pub get_ops: f64,
    /// GET bytes per query.
    pub get_bytes: f64,
    /// PUT operations per query.
    pub put_ops: f64,
    /// PUT bytes per query.
    pub put_bytes: f64,
}

/// One serial GET stage: operations that can proceed concurrently.
#[derive(Debug, Clone)]
pub struct ModeledStage {
    /// Concurrent operations in this stage.
    pub ops: f64,
    /// Total bytes transferred by this stage.
    pub bytes: f64,
}

/// Complete model input: per-class counters, serial stages, and clients.
#[derive(Debug, Clone)]
pub struct ModelInput {
    /// Per-artifact-class GET/PUT totals for one query.
    pub classes: BTreeMap<String, ModeledClassStats>,
    /// Serial GET stages on the query critical path.
    pub stages: Vec<ModeledStage>,
    /// Closed-loop client count driving the load.
    pub clients: usize,
}

/// Predicted throughput, latency, and cost for one query shape.
#[derive(Debug, Clone, Copy)]
pub struct Prediction {
    /// GET bytes per query across all classes.
    pub bytes_per_query: f64,
    /// GET operations per query across all classes.
    pub gets_per_query: f64,
    /// p50 service time in milliseconds (CPU + TTFB + transfer).
    pub service_time_ms: f64,
    /// Aggregate-bandwidth throughput ceiling in queries per second.
    pub qps_bw_cap: f64,
    /// Closed-loop throughput in queries per second.
    pub qps_closed: f64,
    /// Predicted throughput: the minimum of the two ceilings.
    pub qps: f64,
    /// Mean latency from Little's closed-loop identity, milliseconds.
    pub mean_latency_ms: f64,
    /// p50 latency in milliseconds.
    pub p50_ms: f64,
    /// p99 latency in milliseconds (coarse, +/-50%).
    pub p99_ms: f64,
    /// Object-store request plus egress cost per query in dollars.
    pub request_cost: f64,
    /// Amortized node cost per query in dollars.
    pub node_cost: f64,
    /// Total cost per query in dollars.
    pub total_cost: f64,
    /// Which ceiling produced the predicted QPS.
    pub bottleneck: Bottleneck,
}

/// The binding constraint behind a [`Prediction`]'s QPS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bottleneck {
    /// Aggregate object-store bandwidth capped throughput.
    Bandwidth,
    /// Closed-loop client service time capped throughput.
    ClosedLoop,
}

impl Bottleneck {
    /// Stable lowercase label used in rendered tables.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Bandwidth => "bandwidth",
            Self::ClosedLoop => "closed-loop",
        }
    }
}

/// Per-class totals of one fitted serial GET stage.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibratedStageClass {
    /// GET operations contributed by this class in this stage.
    pub ops: f64,
    /// GET bytes contributed by this class in this stage.
    pub bytes: f64,
}

/// One fitted serial GET stage, keyed by artifact class.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibratedStage {
    /// Per-class operation and byte totals for this stage.
    pub classes: BTreeMap<String, CalibratedStageClass>,
}

/// Shape-scaling constants fitted from one instrumented small-shape run.
///
/// The perf-contract Tier 2 suite fits these from a measured
/// `shape_small` scenario and validates the scaled predictions against a
/// held-out `shape_medium` run (<= 10% per-class residual). The advisor
/// consumes a serialized snapshot of the fit; it never fits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibratedShapeModel {
    /// Human-readable provenance of the fit, e.g. the source shape and nprobe.
    pub fitted_from: String,
    /// Date the snapshot was captured, `YYYY-MM-DD`.
    pub snapshot_date: String,
    /// Per-class counters measured on the source shape.
    pub source_classes: BTreeMap<String, ModeledClassStats>,
    /// Serial GET stages measured on the source shape.
    pub source_stages: Vec<CalibratedStage>,
    /// Bytes of per-row overhead observed in coarse (quantized) reads.
    pub coarse_overhead_per_row: f64,
    /// Bytes of per-row overhead observed in rerank (full-precision) reads.
    pub rerank_overhead_per_row: f64,
    /// Average logical clusters covered by one coarse GET (object grouping).
    pub clusters_per_coarse_get: f64,
    /// Rerank GET operations observed per query.
    pub rerank_get_ops: f64,
    /// Bytes of per-probe overhead used by real-dataset targets.
    pub target_overhead_per_probe: f64,
}

impl CalibratedShapeModel {
    /// Parses a snapshot from strict TOML text.
    ///
    /// # Panics
    ///
    /// Panics when the document does not parse strictly or carries no
    /// cluster counters, which would make every prediction meaningless.
    #[must_use]
    pub fn from_toml_str(source: &str) -> Self {
        let model = toml::from_str::<Self>(source)
            .unwrap_or_else(|error| panic!("invalid shape-model snapshot: {error}"));
        assert!(
            model.source_classes.contains_key("cluster"),
            "shape source omitted cluster counters"
        );
        assert!(
            !model.source_stages.is_empty(),
            "shape source produced no GET stages"
        );
        model
    }

    /// Serializes the snapshot as TOML text.
    ///
    /// # Panics
    ///
    /// Panics when serialization fails, which cannot happen for a value
    /// that was constructed or parsed by this module.
    #[must_use]
    pub fn to_toml_string(&self) -> String {
        toml::to_string_pretty(self)
            .unwrap_or_else(|error| panic!("failed to serialize shape-model snapshot: {error}"))
    }

    /// Scales the fitted constants to a synthetic dataset shape, modeling
    /// deterministic density-group expansion for grouped cluster objects.
    ///
    /// # Panics
    ///
    /// Panics on a zero or inconsistent shape parameter, or when the fitted
    /// template does not carry exactly the coarse and rerank cluster stages.
    #[must_use]
    pub fn predict_synthetic(
        &self,
        vectors: usize,
        dims: usize,
        nlist: usize,
        nprobe: usize,
        row_bytes: usize,
        clients: usize,
    ) -> ModelInput {
        assert!(vectors > 0, "shape-model vectors must be nonzero");
        assert!(dims > 0, "shape-model dims must be nonzero");
        assert!(nlist > 0, "shape-model nlist must be nonzero");
        assert!(
            nprobe > 0 && nprobe <= nlist,
            "shape-model nprobe outside nlist"
        );
        assert!(row_bytes > 0, "shape-model row bytes must be nonzero");
        assert!(clients > 0, "shape-model clients must be nonzero");

        let mut classes = self.source_classes.clone();
        let rows_per_cluster = vectors as f64 / nlist as f64;
        let group_count = (nlist as f64 / self.clusters_per_coarse_get).ceil();
        let coarse_ops = distinct_group_coverage(group_count, nprobe);
        let expanded_clusters = (coarse_ops * self.clusters_per_coarse_get)
            .round()
            .min(nlist as f64);
        let coarse_bytes = expanded_clusters
            * rows_per_cluster
            * (row_bytes as f64 + self.coarse_overhead_per_row);
        let rerank_bytes = rows_per_cluster * (dims as f64 * 4.0 + self.rerank_overhead_per_row);
        let cluster_ops = coarse_ops + self.rerank_get_ops;
        let cluster_bytes = coarse_bytes + rerank_bytes;
        let cluster = classes
            .get_mut("cluster")
            .unwrap_or_else(|| panic!("shape source omitted cluster counters"));
        cluster.get_ops = cluster_ops;
        cluster.get_bytes = cluster_bytes;

        let stages = self
            .source_stages
            .iter()
            .scan(0usize, |cluster_stage, stage| {
                let mut ops = 0.0;
                let mut bytes = 0.0;
                for (class, totals) in &stage.classes {
                    if class == "cluster" {
                        match *cluster_stage {
                            0 => {
                                ops += coarse_ops;
                                bytes += coarse_bytes;
                            }
                            1 => {
                                ops += self.rerank_get_ops;
                                bytes += rerank_bytes;
                            }
                            _ => panic!("shape template contains more than two cluster stages"),
                        }
                        *cluster_stage += 1;
                    } else {
                        ops += totals.ops;
                        bytes += totals.bytes;
                    }
                }
                Some(ModeledStage { ops, bytes })
            })
            .collect();
        ModelInput {
            classes,
            stages,
            clients,
        }
    }

    /// Scales the fitted constants to a real-dataset target using the
    /// closed-form `nprobe * vectors / nlist * row_bytes` payload equation
    /// plus the fitted per-probe overhead.
    ///
    /// # Panics
    ///
    /// Panics on a zero or inconsistent shape parameter, or when the fitted
    /// template omitted cluster counters.
    #[must_use]
    pub fn predict_target(
        &self,
        vectors: usize,
        dims: usize,
        nlist: usize,
        nprobe: usize,
        row_bytes: usize,
        clients: usize,
    ) -> ModelInput {
        assert!(vectors > 0, "shape-model vectors must be nonzero");
        assert!(dims > 0, "shape-model dims must be nonzero");
        assert!(nlist > 0, "shape-model nlist must be nonzero");
        assert!(
            nprobe > 0 && nprobe <= nlist,
            "shape-model nprobe outside nlist"
        );
        assert!(row_bytes > 0, "shape-model row bytes must be nonzero");
        assert!(clients > 0, "shape-model clients must be nonzero");
        let mut classes = self.source_classes.clone();
        let cluster_ops = nprobe as f64;
        let cluster_bytes = cluster_ops * vectors as f64 / nlist as f64 * row_bytes as f64
            + cluster_ops * self.target_overhead_per_probe;
        let cluster = classes
            .get_mut("cluster")
            .unwrap_or_else(|| panic!("shape source omitted cluster counters"));
        cluster.get_ops = cluster_ops;
        cluster.get_bytes = cluster_bytes;

        let source_cluster = self.source_classes["cluster"];
        let stages = self
            .source_stages
            .iter()
            .map(|stage| {
                let mut ops = 0.0;
                let mut bytes = 0.0;
                for (class, totals) in &stage.classes {
                    if class == "cluster" {
                        ops += totals.ops / source_cluster.get_ops * cluster_ops;
                        bytes += totals.bytes / source_cluster.get_bytes * cluster_bytes;
                    } else {
                        ops += totals.ops;
                        bytes += totals.bytes;
                    }
                }
                ModeledStage { ops, bytes }
            })
            .collect();
        ModelInput {
            classes,
            stages,
            clients,
        }
    }
}

/// Predict throughput, latency, and cost from measured stages and a profile.
///
/// For each stage `i`, latency is
/// `ttfb + stage_bytes / min(per_conn_MBps * stage_ops, aggregate_share)`.
/// Service time adds profile CPU time to all stages. Closed-loop throughput is
/// `clients / service_time`; bandwidth throughput is
/// `aggregate_MBps * nodes / bytes_per_query`; predicted QPS is their minimum.
/// Mean latency follows Little's closed-loop identity `clients / QPS`. p50 uses
/// p50 TTFB and p99 uses p99 TTFB with the same transfer term. Cost adds GET,
/// PUT, egress, and amortized node cost per query exactly as specified in the
/// Phase 3 plan.
///
/// # Panics
///
/// Panics on an empty or non-positive input: zero clients, no stages, a
/// stage without operations, negative stage bytes, zero total GET bytes, or
/// a profile that produces a non-finite QPS.
#[must_use]
pub fn predict(input: &ModelInput, profile: &Profile) -> Prediction {
    assert!(input.clients > 0, "prediction clients must be nonzero");
    assert!(
        !input.stages.is_empty(),
        "prediction requires at least one stage"
    );
    let aggregate_mbps = profile.storage.agg_mbps_per_node * profile.node.count as f64;
    let mut stage_transfer_ms = 0.0;
    for stage in &input.stages {
        assert!(stage.ops > 0.0, "prediction stage ops must be positive");
        assert!(
            stage.bytes >= 0.0,
            "prediction stage bytes must be nonnegative"
        );
        let available_mbps = (profile.storage.per_conn_mbps * stage.ops).min(aggregate_mbps);
        stage_transfer_ms += stage.bytes / BYTES_PER_MB / available_mbps * 1_000.0;
    }
    let p50_ms = profile.node.cpu_ms_per_query
        + input.stages.len() as f64 * profile.storage.ttfb_ms.p50
        + stage_transfer_ms;
    let p99_ms = profile.node.cpu_ms_per_query
        + input.stages.len() as f64 * profile.storage.ttfb_ms.p99
        + stage_transfer_ms;
    let bytes_per_query = input
        .classes
        .values()
        .map(|stats| stats.get_bytes)
        .sum::<f64>();
    let gets_per_query = input
        .classes
        .values()
        .map(|stats| stats.get_ops)
        .sum::<f64>();
    assert!(
        bytes_per_query > 0.0,
        "prediction requires positive GET bytes"
    );
    let qps_bw_cap = aggregate_mbps / (bytes_per_query / BYTES_PER_MB);
    let qps_closed = input.clients as f64 / (p50_ms / 1_000.0);
    let qps = qps_bw_cap.min(qps_closed);
    assert!(
        qps.is_finite() && qps > 0.0,
        "prediction produced invalid QPS"
    );
    let mean_latency_ms = input.clients as f64 / qps * 1_000.0;
    let request_cost = input
        .classes
        .values()
        .map(|stats| {
            stats.get_ops * profile.storage.price.get_per_req
                + stats.put_ops * profile.storage.price.put_per_req
                + stats.get_bytes / BYTES_PER_GIB * profile.storage.price.egress_per_gb
        })
        .sum::<f64>();
    let node_cost = profile.node.count as f64 * profile.node.price_hr / (qps * 3_600.0);
    Prediction {
        bytes_per_query,
        gets_per_query,
        service_time_ms: p50_ms,
        qps_bw_cap,
        qps_closed,
        qps,
        mean_latency_ms,
        p50_ms,
        p99_ms,
        request_cost,
        node_cost,
        total_cost: request_cost + node_cost,
        bottleneck: if qps_bw_cap <= qps_closed {
            Bottleneck::Bandwidth
        } else {
            Bottleneck::ClosedLoop
        },
    }
}

/// Expected logical clusters covered when `nprobe` probes hit grouped
/// cluster objects that each pack up to three logical clusters.
///
/// Mirrors `DEFAULT_MAX_CLUSTERS_PER_OBJECT = 3` in
/// `crate::index::ivf_flat::build`; a change there invalidates this model.
#[must_use]
pub fn grouped_cluster_coverage(nlist: usize, nprobe: usize) -> f64 {
    const MAX_CLUSTERS_PER_OBJECT: f64 = 3.0;
    let miss_probability = 1.0 - MAX_CLUSTERS_PER_OBJECT / nlist as f64;
    (nlist as f64 * (1.0 - miss_probability.powi(nprobe as i32))).round()
}

/// Expected distinct groups touched when `nprobe` draws land uniformly on
/// `group_count` groups.
#[must_use]
pub fn distinct_group_coverage(group_count: f64, nprobe: usize) -> f64 {
    let miss_probability = 1.0 - 1.0 / group_count;
    (group_count * (1.0 - miss_probability.powi(nprobe as i32))).round()
}

#[cfg(test)]
mod tests {
    use super::super::profiles::load_profile_from_str;
    use super::*;

    const LOCAL_PROFILE: &str = r#"
name = "unit-minio-local"

[storage]
per_conn_MBps = 377.0
agg_MBps_per_node = 410.0

[storage.ttfb_ms]
p50 = 1.0
p99 = 5.0

[storage.price]
get_per_req = 0.0
put_per_req = 0.0
egress_per_gb = 0.0

[node]
count = 1
vcpus = 8
mem_gb = 16
price_hr = 0.0
cpu_ms_per_query = 0.0

[client]
closed_loop_clients = 8
"#;

    #[test]
    fn bandwidth_cap_inflates_closed_loop_mean_latency() {
        let profile = load_profile_from_str("unit-minio-local", LOCAL_PROFILE);
        let input = ModelInput {
            classes: BTreeMap::from([(
                "cluster".to_string(),
                ModeledClassStats {
                    get_ops: 1.0,
                    get_bytes: 410.0 * BYTES_PER_MB,
                    ..ModeledClassStats::default()
                },
            )]),
            stages: vec![ModeledStage {
                ops: 1.0,
                bytes: 410.0 * BYTES_PER_MB,
            }],
            clients: 8,
        };
        let prediction = predict(&input, &profile);
        assert_eq!(prediction.bottleneck, Bottleneck::Bandwidth);
        assert!((prediction.qps - 1.0).abs() < 1e-9);
        assert!((prediction.mean_latency_ms - 8_000.0).abs() < 1e-9);
    }

    #[test]
    fn snapshot_round_trips_through_toml() {
        let model = CalibratedShapeModel {
            fitted_from: "unit 4096x64 nprobe=4".to_string(),
            snapshot_date: "2026-08-05".to_string(),
            source_classes: BTreeMap::from([(
                "cluster".to_string(),
                ModeledClassStats {
                    get_ops: 4.0,
                    get_bytes: 1_000_000.0,
                    put_ops: 0.0,
                    put_bytes: 0.0,
                },
            )]),
            source_stages: vec![
                CalibratedStage {
                    classes: BTreeMap::from([(
                        "cluster".to_string(),
                        CalibratedStageClass {
                            ops: 3.0,
                            bytes: 900_000.0,
                        },
                    )]),
                },
                CalibratedStage {
                    classes: BTreeMap::from([(
                        "cluster".to_string(),
                        CalibratedStageClass {
                            ops: 1.0,
                            bytes: 100_000.0,
                        },
                    )]),
                },
            ],
            coarse_overhead_per_row: 12.0,
            rerank_overhead_per_row: 4.0,
            clusters_per_coarse_get: 3.0,
            rerank_get_ops: 1.0,
            target_overhead_per_probe: 512.0,
        };
        let text = model.to_toml_string();
        let reparsed = CalibratedShapeModel::from_toml_str(&text);
        assert_eq!(reparsed.fitted_from, model.fitted_from);
        assert_eq!(reparsed.source_stages.len(), 2);
        assert!(
            (reparsed.target_overhead_per_probe - model.target_overhead_per_probe).abs() < 1e-12
        );
    }

    #[test]
    fn target_prediction_scales_cluster_bytes_by_the_closed_form() {
        let text = CalibratedShapeModel {
            fitted_from: "unit".to_string(),
            snapshot_date: "2026-08-05".to_string(),
            source_classes: BTreeMap::from([(
                "cluster".to_string(),
                ModeledClassStats {
                    get_ops: 4.0,
                    get_bytes: 1_000_000.0,
                    put_ops: 0.0,
                    put_bytes: 0.0,
                },
            )]),
            source_stages: vec![CalibratedStage {
                classes: BTreeMap::from([(
                    "cluster".to_string(),
                    CalibratedStageClass {
                        ops: 4.0,
                        bytes: 1_000_000.0,
                    },
                )]),
            }],
            coarse_overhead_per_row: 0.0,
            rerank_overhead_per_row: 0.0,
            clusters_per_coarse_get: 1.0,
            rerank_get_ops: 0.0,
            target_overhead_per_probe: 0.0,
        }
        .to_toml_string();
        let model = CalibratedShapeModel::from_toml_str(&text);
        let input = model.predict_target(1_000_000, 768, 1_000, 32, 200, 8);
        let cluster = input.classes["cluster"];
        assert!((cluster.get_ops - 32.0).abs() < 1e-9);
        // nprobe * vectors / nlist * row_bytes = 32 * 1000 * 200
        assert!((cluster.get_bytes - 6_400_000.0).abs() < 1e-6);
    }
}
