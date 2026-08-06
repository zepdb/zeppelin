//! Tier 2 analytic prediction, calibration, validation, and what-if tables.
//!
//! The prediction math (`predict`, `CalibratedShapeModel`) lives in
//! `zeppelin::sizing::model`; this module re-exports it and keeps the parts
//! that consume test-only instrumentation: fitting the shape model from a
//! measured repeat, ground-truth validation, CPU calibration, and the
//! frozen what-if report rendering.

use std::collections::BTreeMap;

use crate::common::counting::ClassStats;
use zeppelin::config::IndexingConfig;
use zeppelin::sizing::model::{CalibratedShapeModel, CalibratedStage, CalibratedStageClass};

// Re-exported for API continuity: perf-contract code keeps importing the
// prediction type family from this module even though it now lives in the lib.
#[allow(unused_imports)]
pub use zeppelin::sizing::model::{
    predict, Bottleneck, ModelInput, ModeledClassStats, ModeledStage, Prediction,
};

use super::dataset::{DatasetSpec, SHAPE_MEDIUM, SHAPE_SMALL};
use super::depth::{DepthTracker, SpanKind};
use super::ground_truth::{load_gt_a, load_gt_b, GroundTruthA, GroundTruthB};
use super::profiles::{load_profile, selected_profiles, Profile, WhatIfProfile};
use super::report::RunArtifacts;
use super::scenario::{run_scenario, RepeatCounters, ScenarioOutcome};
use super::{require_minio, scenarios, PerfEnv};

const BYTES_PER_MB: f64 = 1_000_000.0;
const GT_A_QPS_TOLERANCE: f64 = 0.10;
const GT_B_QPS_TOLERANCE: f64 = 0.20;
const GT_B_MEAN_TOLERANCE: f64 = 0.25;
const SHAPE_TOLERANCE: f64 = 0.10;

#[derive(Debug, Clone)]
struct ValidationSummary {
    gt_a_qps_residual: f64,
    gt_b_qps_residual: f64,
    gt_b_mean_residual: f64,
    shape_residuals: Vec<ShapeResidual>,
    calibration_note: String,
}

#[derive(Debug, Clone)]
struct ShapeResidual {
    class: String,
    get_ops: f64,
    get_bytes: f64,
    put_ops: f64,
    put_bytes: f64,
}

/// Execute the Phase 3 measurements, hard validation, and deterministic tables.
pub async fn run_predict_entry() {
    require_minio();
    let env = PerfEnv::from_env();
    assert!(!env.capture, "predict cannot run in capture mode");
    let selected = selected_profiles();

    let labels = vec!["shape_small".to_string(), "shape_medium".to_string()];
    let mut artifact_env = env.clone();
    artifact_env.scenarios.clone_from(&labels);
    let artifacts = RunArtifacts::create(&artifact_env, "predict", &labels);

    let small = measure_shape("shape_small", SHAPE_SMALL.clone()).await;
    artifacts.write_scenario("shape_small", &small, &[]);
    let medium = measure_shape("shape_medium", SHAPE_MEDIUM.clone()).await;
    artifacts.write_scenario("shape_medium", &medium, &[]);
    let report = artifacts.write_report();

    let small_repeat = only_repeat(&small);
    let model = fit_shape_model(&SHAPE_SMALL, 4, small_repeat);
    let validations = validate_model(&model, &medium);
    let whatif = render_whatif(&model, &selected, &validations);
    let path = artifacts.write_whatif(&whatif);
    let snapshot_path = path.with_file_name("shape_model.toml");
    std::fs::write(&snapshot_path, model.to_toml_string())
        .unwrap_or_else(|error| panic!("failed to write shape-model snapshot: {error}"));
    println!("performance prediction report: {}", report.display());
    println!("performance what-if table: {}", path.display());
    println!(
        "calibrated shape-model snapshot: {}",
        snapshot_path.display()
    );
}

async fn measure_shape(name: &str, shape: DatasetSpec) -> ScenarioOutcome {
    let spec = scenarios::standard_shape(name, shape);
    run_scenario(&spec, None).await
}

fn only_repeat(outcome: &ScenarioOutcome) -> &RepeatCounters {
    assert_eq!(
        outcome.per_repeat.len(),
        1,
        "shape measurement must produce exactly one repeat"
    );
    &outcome.per_repeat[0]
}

/// Fit the shape-scaling constants from one instrumented small-shape repeat.
///
/// This is the calibration seam that keeps `CalibratedShapeModel` honest: it
/// consumes the test-tree depth/counting instrumentation and produces the
/// serializable constants the library model scales from.
pub fn fit_shape_model(
    source: &DatasetSpec,
    source_nprobe: usize,
    repeat: &RepeatCounters,
) -> CalibratedShapeModel {
    let source_classes = modeled_classes(&repeat.classes);
    let cluster = source_classes
        .get("cluster")
        .copied()
        .expect("shape source omitted cluster counters");
    assert!(cluster.get_ops > 0.0, "shape source made no cluster GETs");
    let source_stages = DepthTracker::stages(
        &repeat.spans,
        &[SpanKind::Get, SpanKind::Head],
        Some(repeat.response_cutoff_us),
    );
    assert!(
        !source_stages.is_empty(),
        "shape source produced no GET stages"
    );
    let cluster_stages = source_stages
        .iter()
        .filter_map(|stage| stage.classes.get("cluster"))
        .collect::<Vec<_>>();
    assert_eq!(
        cluster_stages.len(),
        2,
        "shape source must expose coarse and rerank cluster stages"
    );
    let rows_per_cluster = source.vectors as f64 / source.nlist as f64;
    let expanded_clusters =
        zeppelin::sizing::model::grouped_cluster_coverage(source.nlist, source_nprobe);
    let coarse = cluster_stages[0];
    let rerank = cluster_stages[1];
    let coarse_payload = expanded_clusters * rows_per_cluster * source.dims as f64;
    assert!(
        coarse.bytes as f64 >= coarse_payload,
        "coarse stage bytes are below the closed-form row payload"
    );
    let coarse_overhead_per_row =
        coarse.bytes as f64 / expanded_clusters / rows_per_cluster - source.dims as f64;
    let rerank_payload = rows_per_cluster * source.dims as f64 * 4.0;
    assert!(
        rerank.bytes as f64 >= rerank_payload,
        "rerank stage bytes are below one full-precision cluster"
    );
    let rerank_overhead_per_row = (rerank.bytes as f64 - rerank_payload) / rows_per_cluster;
    let target_payload = source_nprobe as f64 * rows_per_cluster * source.dims as f64;
    let target_overhead_per_probe = (cluster.get_bytes - target_payload) / source_nprobe as f64;
    let clusters_per_coarse_get = expanded_clusters / coarse.ops as f64;
    let rerank_get_ops = rerank.ops as f64;
    CalibratedShapeModel {
        fitted_from: format!(
            "{}x{} nlist={} nprobe={}",
            source.vectors, source.dims, source.nlist, source_nprobe
        ),
        snapshot_date: chrono::Utc::now().date_naive().to_string(),
        source_classes,
        source_stages: source_stages
            .iter()
            .map(|stage| CalibratedStage {
                classes: stage
                    .classes
                    .iter()
                    .map(|(class, totals)| {
                        (
                            class.clone(),
                            CalibratedStageClass {
                                ops: totals.ops as f64,
                                bytes: totals.bytes as f64,
                            },
                        )
                    })
                    .collect(),
            })
            .collect(),
        coarse_overhead_per_row,
        rerank_overhead_per_row,
        clusters_per_coarse_get,
        rerank_get_ops,
        target_overhead_per_probe,
    }
}

/// Convert one Tier 1 serial repeat into the exact Tier 2 model input used by
/// latency cross-validation.
#[must_use]
pub fn model_input_from_repeat(repeat: &RepeatCounters, clients: usize) -> ModelInput {
    assert!(clients > 0, "prediction clients must be nonzero");
    let stages = DepthTracker::stages(
        &repeat.spans,
        &[SpanKind::Get, SpanKind::Head],
        Some(repeat.response_cutoff_us),
    )
    .into_iter()
    .map(|stage| ModeledStage {
        ops: stage.ops as f64,
        bytes: stage.bytes as f64,
    })
    .collect::<Vec<_>>();
    assert!(
        !stages.is_empty(),
        "latency prediction requires at least one measured GET/HEAD stage"
    );
    ModelInput {
        classes: modeled_classes(&repeat.classes),
        stages,
        clients,
    }
}

fn validate_model(model: &CalibratedShapeModel, medium: &ScenarioOutcome) -> ValidationSummary {
    let gt_a = load_gt_a();
    let gt_b = load_gt_b();
    assert_eq!(gt_a.name, "dbpedia100k-accepted-line");
    let mut minio = load_profile("minio-local-docker");
    assert_eq!(gt_a.profile, minio.name, "GT-A profile mismatch");
    assert_eq!(gt_b.profile, minio.name, "GT-B profile mismatch");
    let calibration_note = calibrate_cpu(&mut minio, &gt_a);

    let gt_a_input = gt_a_input(&gt_a);
    let gt_a_prediction = predict(&gt_a_input, &minio);
    let gt_a_qps_residual = relative_error(gt_a_prediction.qps, gt_a.measured_qps);
    assert!(
        gt_a_qps_residual <= GT_A_QPS_TOLERANCE,
        "GT-A QPS residual {:.2}% exceeds {:.0}%: predicted {:.3}, measured {:.3}",
        gt_a_qps_residual * 100.0,
        GT_A_QPS_TOLERANCE * 100.0,
        gt_a_prediction.qps,
        gt_a.measured_qps
    );

    let gt_b_input = model.predict_target(
        gt_b.vectors,
        gt_b.dims,
        gt_b.nlist,
        gt_b.nprobe,
        gt_b.dims,
        gt_b.clients,
    );
    let gt_b_prediction = predict(&gt_b_input, &minio);
    let gt_b_qps_residual = relative_error(gt_b_prediction.qps, gt_b.measured_rps);
    let gt_b_mean_residual = relative_error(
        gt_b_prediction.mean_latency_ms,
        gt_b.mean_latency_s * 1_000.0,
    );
    assert!(
        gt_b_qps_residual <= GT_B_QPS_TOLERANCE,
        "GT-B QPS residual {:.2}% exceeds {:.0}%: predicted {:.3}, measured {:.3}",
        gt_b_qps_residual * 100.0,
        GT_B_QPS_TOLERANCE * 100.0,
        gt_b_prediction.qps,
        gt_b.measured_rps
    );
    assert!(
        gt_b_mean_residual <= GT_B_MEAN_TOLERANCE,
        "GT-B mean residual {:.2}% exceeds {:.0}%: predicted {:.3} ms, measured {:.3} ms",
        gt_b_mean_residual * 100.0,
        GT_B_MEAN_TOLERANCE * 100.0,
        gt_b_prediction.mean_latency_ms,
        gt_b.mean_latency_s * 1_000.0
    );
    let closed_loop_identity = gt_b.clients as f64 / gt_b.mean_latency_s;
    assert!(
        relative_error(closed_loop_identity, gt_b.measured_rps) <= 0.02,
        "GT-B fixture violates its closed-loop identity"
    );

    let medium_nprobe =
        IndexingConfig::default().effective_default_nprobe_with_floor(SHAPE_MEDIUM.nlist, 4);
    let medium_prediction = model.predict_synthetic(
        SHAPE_MEDIUM.vectors,
        SHAPE_MEDIUM.dims,
        SHAPE_MEDIUM.nlist,
        medium_nprobe,
        SHAPE_MEDIUM.dims,
        8,
    );
    let shape_residuals =
        compare_shape_classes(&medium_prediction.classes, &only_repeat(medium).classes);
    for residual in &shape_residuals {
        for (metric, value) in [
            ("get_ops", residual.get_ops),
            ("get_bytes", residual.get_bytes),
            ("put_ops", residual.put_ops),
            ("put_bytes", residual.put_bytes),
        ] {
            assert!(
                value <= SHAPE_TOLERANCE,
                "shape scaling {}.{metric} residual {:.2}% exceeds {:.0}%",
                residual.class,
                value * 100.0,
                SHAPE_TOLERANCE * 100.0
            );
        }
    }
    println!(
        "GT-A residual={:.2}% GT-B qps={:.2}% mean={:.2}%",
        gt_a_qps_residual * 100.0,
        gt_b_qps_residual * 100.0,
        gt_b_mean_residual * 100.0
    );
    ValidationSummary {
        gt_a_qps_residual,
        gt_b_qps_residual,
        gt_b_mean_residual,
        shape_residuals,
        calibration_note,
    }
}

fn calibrate_cpu(profile: &mut Profile, gt: &GroundTruthA) -> String {
    profile.node.cpu_ms_per_query = 0.0;
    let zero_cpu = predict(&gt_a_input(gt), profile);
    if zero_cpu.qps_bw_cap <= gt.measured_qps {
        profile.node.cpu_ms_per_query = 0.0;
        return format!(
            "GT-A is bandwidth-capped at {:.3} QPS below measured {:.3}; CPU is non-identifiable, so the minimum nonnegative calibration (0 ms/query) is used",
            zero_cpu.qps_bw_cap, gt.measured_qps
        );
    }
    let stage_ms = zero_cpu.service_time_ms;
    let target_service_ms = gt.clients as f64 / gt.measured_qps * 1_000.0;
    profile.node.cpu_ms_per_query = (target_service_ms - stage_ms).max(0.0);
    format!(
        "GT-A identifies CPU at {:.3} ms/query from closed-loop service time",
        profile.node.cpu_ms_per_query
    )
}

fn gt_a_input(gt: &GroundTruthA) -> ModelInput {
    let bytes = gt.bytes_per_query_mb * BYTES_PER_MB;
    ModelInput {
        classes: BTreeMap::from([(
            "cluster".to_string(),
            ModeledClassStats {
                get_ops: gt.gets_per_query,
                get_bytes: bytes,
                ..ModeledClassStats::default()
            },
        )]),
        stages: vec![ModeledStage {
            ops: gt.gets_per_query,
            bytes,
        }],
        clients: gt.clients,
    }
}

fn compare_shape_classes(
    predicted: &BTreeMap<String, ModeledClassStats>,
    actual: &BTreeMap<String, ClassStats>,
) -> Vec<ShapeResidual> {
    actual
        .iter()
        .map(|(class, actual)| {
            let predicted = predicted
                .get(class)
                .copied()
                .unwrap_or_else(|| panic!("shape prediction omitted class {class}"));
            ShapeResidual {
                class: class.clone(),
                get_ops: metric_error(predicted.get_ops, actual.get_ops as f64),
                get_bytes: metric_error(predicted.get_bytes, actual.get_bytes as f64),
                put_ops: metric_error(predicted.put_ops, actual.put_ops as f64),
                put_bytes: metric_error(predicted.put_bytes, actual.put_bytes as f64),
            }
        })
        .collect()
}

fn metric_error(predicted: f64, actual: f64) -> f64 {
    if actual == 0.0 {
        assert_eq!(
            predicted, 0.0,
            "zero actual metric requires exact zero prediction"
        );
        0.0
    } else {
        relative_error(predicted, actual)
    }
}

fn render_whatif(
    model: &CalibratedShapeModel,
    profiles: &[Profile],
    validation: &ValidationSummary,
) -> String {
    let mut out = String::new();
    out.push_str("# Tier 2 - Analytic What-If Predictions\n\n");
    out.push_str("**PREDICTION - unvalidated at this scale.** ");
    out.push_str(&format!(
        "Validated points: GT-A QPS residual {:.2}%, GT-B QPS residual {:.2}%, GT-B mean-latency residual {:.2}%. p99 predictions are coarse +/-50%.\n\n",
        validation.gt_a_qps_residual * 100.0,
        validation.gt_b_qps_residual * 100.0,
        validation.gt_b_mean_residual * 100.0
    ));
    out.push_str(&format!("- calibration: {}\n", validation.calibration_note));
    out.push_str("- GT-B caveat: bytes/query were not measured; shape-model counters validate closed-loop and latency mechanics, while GT-A anchors bytes to QPS.\n");
    out.push_str("- shape domain: the held-out synthetic gate models deterministic density-group expansion and rerank ranges; real-dataset targets use the specified nprobe * N/nlist * row_bytes equation plus the small-shape per-probe overhead.\n");
    out.push_str(&format!(
        "- selected profiles: {}\n\n",
        profiles
            .iter()
            .map(|profile| format!("`{}`", profile.name))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    out.push_str("## Shape-Scaling Residuals\n\n");
    out.push_str("Fit: `shape_small`; held out: `shape_medium`.\n\n");
    out.push_str("| class | GET ops | GET bytes | PUT ops | PUT bytes |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    for residual in &validation.shape_residuals {
        out.push_str(&format!(
            "| `{}` | {:.2}% | {:.2}% | {:.2}% | {:.2}% |\n",
            residual.class,
            residual.get_ops * 100.0,
            residual.get_bytes * 100.0,
            residual.put_ops * 100.0,
            residual.put_bytes * 100.0
        ));
    }
    out.push('\n');

    for profile in profiles {
        let Some(target) = &profile.whatif else {
            continue;
        };
        render_profile_table(&mut out, model, profile, target);
    }
    out
}

fn render_profile_table(
    out: &mut String,
    model: &CalibratedShapeModel,
    profile: &Profile,
    target: &WhatIfProfile,
) {
    out.push_str(&format!("## `{}`\n\n", profile.name));
    out.push_str(&format!(
        "Shape: {} vectors x {} dimensions, nlist {}, {} node(s). EXPECTED rows are pending July10Quant measurement.\n\n",
        target.vectors, target.dims, target.nlist, profile.node.count
    ));
    out.push_str("| quantization | nprobe | bytes/query MB | GETs/query | QPS | p50 ms | p99 ms | request $/q | node $/q | total $/q | bottleneck |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
    for quantization in &target.quantization {
        for nprobe in &target.nprobe {
            let input = model.predict_target(
                target.vectors,
                target.dims,
                target.nlist,
                *nprobe,
                target.row_bytes[quantization],
                profile.client.closed_loop_clients,
            );
            let prediction = predict(&input, profile);
            debug_assert!(prediction.qps <= prediction.qps_closed);
            let expected = if target.expected_quantization.contains(quantization) {
                " (EXPECTED)"
            } else {
                ""
            };
            out.push_str(&format!(
                "| `{quantization}{expected}` | {nprobe} | {:.3} | {:.2} | {:.2} | {:.2} | {:.2} | {:.9} | {:.9} | {:.9} | {} |\n",
                prediction.bytes_per_query / BYTES_PER_MB,
                prediction.gets_per_query,
                prediction.qps,
                prediction.p50_ms,
                prediction.p99_ms,
                prediction.request_cost,
                prediction.node_cost,
                prediction.total_cost,
                prediction.bottleneck.name()
            ));
        }
    }
    out.push('\n');
    if profile.name == "s3-3node-wikidpr" {
        out.push_str("Reference lines: Qdrant 111.9 QPS; Elasticsearch DiskBBQ 32.4 QPS. Recall parity is not predicted and must be established by the benchmark campaign.\n\n");
    }
}

fn modeled_classes(classes: &BTreeMap<String, ClassStats>) -> BTreeMap<String, ModeledClassStats> {
    classes
        .iter()
        .map(|(class, stats)| {
            (
                class.clone(),
                ModeledClassStats {
                    get_ops: stats.get_ops as f64,
                    get_bytes: stats.get_bytes as f64,
                    put_ops: stats.put_ops as f64,
                    put_bytes: stats.put_bytes as f64,
                },
            )
        })
        .collect()
}

fn relative_error(predicted: f64, actual: f64) -> f64 {
    assert!(actual > 0.0, "relative-error actual value must be positive");
    (predicted - actual).abs() / actual
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_schema_and_closed_loop_identity_are_pinned() {
        let gt_b: GroundTruthB = load_gt_b();
        let identity = gt_b.clients as f64 / gt_b.mean_latency_s;
        assert!(relative_error(identity, gt_b.measured_rps) <= 0.02);
        assert!(gt_b.p95_latency_s < gt_b.p99_latency_s);
        assert!(gt_b.mean_precision > 0.8);
        assert_eq!(gt_b.dataset, "glove-100-angular");
        assert_eq!(gt_b.name, "glove-100-angular-search-0");
    }

    #[test]
    fn source_shape_is_retained_by_the_model() {
        assert_eq!(SHAPE_SMALL.vectors, 4096);
        assert_eq!(SHAPE_SMALL.dims, 64);
    }
}
