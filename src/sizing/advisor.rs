//! Deterministic deployment planning over the embedded cloud catalog.
//!
//! [`plan`] enumerates instance, cache-device, quantization, and probe-count
//! combinations for one cloud region. Each combination is converted into the
//! canonical sizing [`Profile`], predicted by the frozen
//! [`CalibratedShapeModel`], post-capped by node CPU capacity, priced for a
//! 730-hour month, checked against caller constraints, and sorted by monthly
//! cost. Nothing in this module discovers live prices or silently substitutes
//! a region: the supplied [`Catalog`] and its snapshot date are authoritative.

use crate::config::IndexingConfig;

use super::catalog::{Arch, BlockStorageSku, Catalog, Cloud, InstanceSku, ObjectStoreSku};
use super::model::{predict, Bottleneck, CalibratedShapeModel, Prediction};
use super::profiles::{ClientProfile, NodeProfile, Profile, StoragePrice, StorageProfile};
use super::rows::{row_bytes, Quantization};

use thiserror::Error;

const HOURS_PER_MONTH: f64 = 730.0;
const SECONDS_PER_MONTH: f64 = HOURS_PER_MONTH * 3_600.0;
const GB_BYTES: f64 = 1_000_000_000.0;
const GBPS_TO_MBPS: f64 = 125.0;
const CPU_MS_PER_QUERY: f64 = 8.0;
const SERVICE_AGGREGATE_CEILING_MBPS: f64 = 100_000.0;
const BLOCK_VOLUME_SLACK: f64 = 1.20;

/// A request that cannot be evaluated against the supplied catalog.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdvisorError {
    /// A request field violates the planner's strict input contract.
    #[error("invalid plan request: {reason}")]
    InvalidRequest {
        /// Field-specific explanation.
        reason: String,
    },
    /// The catalog has no object-store price sheet for this cloud/region.
    #[error("catalog has no {cloud} object-store price sheet for region {region}")]
    UnsupportedRegion {
        /// Stable lowercase cloud label.
        cloud: String,
        /// Requested provider region.
        region: String,
    },
}

/// Dataset traits that affect prediction and at-rest capacity.
#[derive(Debug, Clone, Copy)]
pub struct DataShape {
    /// Number of logical vectors in the namespace.
    pub vectors: usize,
    /// Dimensions in each vector.
    pub dims: usize,
    /// Whether attribute filters and their bitmap sidecars are required.
    pub filters: bool,
    /// Whether the full-text sidecar is required.
    pub fts: bool,
}

/// CPU-architecture constraint applied before candidate enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchFilter {
    /// Consider both x86-64 and Arm instances.
    Any,
    /// Consider x86-64 instances only.
    X86_64,
    /// Consider Arm64 instances only.
    Arm64,
}

impl ArchFilter {
    fn accepts(self, arch: Arch) -> bool {
        matches!(
            (self, arch),
            (Self::Any, _) | (Self::X86_64, Arch::X86_64) | (Self::Arm64, Arch::Arm64)
        )
    }
}

/// Complete set of inputs for one advisor run.
#[derive(Debug, Clone)]
pub struct PlanRequest {
    /// Cloud whose instances and prices should be considered.
    pub cloud: Cloud,
    /// Provider region, using the provider's canonical spelling.
    pub region: String,
    /// Customer data shape.
    pub shape: DataShape,
    /// Number of identical stateless query nodes.
    pub replicas: usize,
    /// Optional minimum aggregate throughput requirement.
    pub min_qps: Option<f64>,
    /// Optional maximum p99 latency requirement in milliseconds.
    pub max_p99_ms: Option<f64>,
    /// Optional all-in monthly budget in USD.
    pub max_monthly_usd: Option<f64>,
    /// CPU-architecture filter.
    pub arch: ArchFilter,
    /// Closed-loop clients used by the calibrated model.
    pub clients: usize,
    /// Quantization variants to enumerate.
    pub quantizations: Vec<Quantization>,
    /// Probe counts to enumerate.
    pub nprobes: Vec<usize>,
}

/// Cache device attached to each query node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheDevicePlan {
    /// Provider-managed local instance-store NVMe included in node price.
    Nvme {
        /// Advertised local capacity in decimal GB.
        capacity_gb: u64,
    },
    /// Separately billed network block storage.
    Block {
        /// Catalog tier name.
        tier: String,
        /// Recommended capacity per node in decimal GB.
        volume_gb: u64,
        /// Provisioned IOPS per node.
        iops: u64,
        /// Provisioned throughput per node in decimal MB/s.
        throughput_mbps: u64,
    },
}

impl CacheDevicePlan {
    /// Stable human-readable label used in tables and deterministic tie-breaks.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Nvme { capacity_gb } => format!("nvme:{capacity_gb}GB"),
            Self::Block {
                tier, volume_gb, ..
            } => format!("{tier}:{volume_gb}GB"),
        }
    }
}

/// Binding advisor-level throughput constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisorBottleneck {
    /// Object-store/service aggregate bandwidth.
    Bandwidth,
    /// Closed-loop client count and service time.
    ClosedLoop,
    /// Baseline network bandwidth of the chosen node shape.
    NodeNic,
    /// Engineered CPU-service capacity of the chosen node fleet.
    Cpu,
}

impl AdvisorBottleneck {
    /// Stable lowercase label used in rendered output.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Bandwidth => "bandwidth",
            Self::ClosedLoop => "closed-loop",
            Self::NodeNic => "node-nic",
            Self::Cpu => "cpu",
        }
    }
}

/// All components of the all-in 730-hour monthly estimate.
#[derive(Debug, Clone, Copy)]
pub struct MonthlyCost {
    /// Compute-node on-demand cost.
    pub nodes: f64,
    /// Attached block-storage cost; zero for included NVMe.
    pub block_storage: f64,
    /// Source-of-truth object-storage capacity cost.
    pub object_storage: f64,
    /// Object-store request and same-region transfer cost at the billed QPS.
    pub requests: f64,
    /// Sum of every monthly component.
    pub total: f64,
}

/// One viable, constraint-satisfying deployment combination.
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    /// Provider instance SKU.
    pub instance: InstanceSku,
    /// Cache device on every replica.
    pub cache: CacheDevicePlan,
    /// Stored-vector quantization.
    pub quantization: Quantization,
    /// Explicit IVF probe count.
    pub nprobe: usize,
    /// Raw calibrated analytic-model result.
    pub prediction: Prediction,
    /// Aggregate QPS after the advisor's CPU post-cap.
    pub qps: f64,
    /// CPU-only aggregate QPS ceiling.
    pub qps_cpu_cap: f64,
    /// Constraint that binds [`Self::qps`].
    pub bottleneck: AdvisorBottleneck,
    /// Object-store capacity estimate in decimal GB.
    pub object_storage_gb: f64,
    /// Queries used to amortize the monthly estimate.
    pub queries_per_month: f64,
    /// Monthly cost breakdown.
    pub monthly: MonthlyCost,
    /// All-in monthly cost divided by billed queries.
    pub cost_per_query: f64,
}

/// One enumerated combination excluded by caller constraints.
#[derive(Debug, Clone)]
pub struct RejectedCandidate {
    /// Provider instance SKU name.
    pub instance: String,
    /// Stable cache-device label.
    pub cache: String,
    /// Quantization label.
    pub quantization: String,
    /// Explicit IVF probe count.
    pub nprobe: usize,
    /// Every failed constraint, in stable order.
    pub reasons: Vec<String>,
}

/// Deterministic result of one complete catalog sweep.
#[derive(Debug, Clone)]
pub struct PlanReport {
    /// Catalog price snapshot used by this run.
    pub catalog_snapshot_date: String,
    /// MinIO model-calibration snapshot used by this run.
    pub calibration_snapshot_date: String,
    /// Canonical scale-aware IVF cluster count.
    pub nlist: usize,
    /// Viable candidates sorted by monthly cost, then cost/query and identity.
    pub candidates: Vec<RankedCandidate>,
    /// Enumerated candidates that failed one or more constraints.
    pub rejected: Vec<RejectedCandidate>,
    /// Engineered assumptions that are not calibrated measurements.
    pub assumptions: Vec<String>,
}

/// Runs the planner against caller-supplied catalog and calibration snapshots.
///
/// Supplying the snapshots explicitly keeps tests and offline audits fully
/// deterministic. Runtime callers normally use [`plan_embedded`].
///
/// # Errors
///
/// Returns [`AdvisorError::InvalidRequest`] for zero, non-finite, duplicate,
/// or otherwise inconsistent inputs, and [`AdvisorError::UnsupportedRegion`]
/// when the catalog has no object-store price sheet for the requested region.
pub fn plan(
    catalog: &Catalog,
    model: &CalibratedShapeModel,
    request: &PlanRequest,
) -> Result<PlanReport, AdvisorError> {
    validate_request(request)?;
    let store = catalog
        .object_store_for(request.cloud, &request.region)
        .ok_or_else(|| AdvisorError::UnsupportedRegion {
            cloud: request.cloud.name().to_string(),
            region: request.region.clone(),
        })?;
    let cloud_catalog = catalog
        .clouds
        .get(&request.cloud)
        .unwrap_or_else(|| panic!("validated catalog omitted {}", request.cloud.name()));
    let nlist = IndexingConfig::default().effective_num_centroids(request.shape.vectors);
    let mut candidates = Vec::new();
    let mut rejected = Vec::new();

    for instance in cloud_catalog
        .instances
        .iter()
        .filter(|instance| request.arch.accepts(instance.arch))
    {
        let Some(instance_price_hr) = instance.price_in(&request.region) else {
            continue;
        };
        for &quantization in &request.quantizations {
            let storage_gb = estimated_object_storage_gb(request.shape, quantization);
            let mut caches = cache_plans(instance, &cloud_catalog.block_storage, storage_gb);
            caches.sort_by_key(CacheDevicePlan::label);
            for cache in caches {
                let block_cost_per_node =
                    cache_monthly_cost(&cache, &cloud_catalog.block_storage, &request.region);
                for &nprobe in &request.nprobes {
                    if nprobe > nlist {
                        rejected.push(RejectedCandidate {
                            instance: instance.name.clone(),
                            cache: cache.label(),
                            quantization: quantization.label().to_string(),
                            nprobe,
                            reasons: vec![format!(
                                "nprobe {nprobe} exceeds canonical nlist {nlist}"
                            )],
                        });
                        continue;
                    }
                    let profile = candidate_profile(request, instance, instance_price_hr, store);
                    let prediction =
                        predict_for_profile(model, &profile, request.shape, quantization, nprobe);
                    let qps_cpu_cap = request.replicas as f64 * instance.vcpus as f64 * 1_000.0
                        / CPU_MS_PER_QUERY;
                    let qps = prediction.qps.min(qps_cpu_cap);
                    let bottleneck = advisor_bottleneck(instance, &prediction, qps_cpu_cap);
                    let billed_qps = request.min_qps.unwrap_or(qps);
                    let queries_per_month = billed_qps * SECONDS_PER_MONTH;
                    let nodes = request.replicas as f64 * instance_price_hr * HOURS_PER_MONTH;
                    let block_storage = request.replicas as f64 * block_cost_per_node;
                    let object_storage = storage_gb * store.storage_gb_month;
                    let requests = queries_per_month * prediction.request_cost;
                    let total = nodes + block_storage + object_storage + requests;
                    let monthly = MonthlyCost {
                        nodes,
                        block_storage,
                        object_storage,
                        requests,
                        total,
                    };
                    let cost_per_query = total / queries_per_month;
                    let reasons = rejection_reasons(request, qps, prediction.p99_ms, total);
                    if reasons.is_empty() {
                        candidates.push(RankedCandidate {
                            instance: instance.clone(),
                            cache: cache.clone(),
                            quantization,
                            nprobe,
                            prediction,
                            qps,
                            qps_cpu_cap,
                            bottleneck,
                            object_storage_gb: storage_gb,
                            queries_per_month,
                            monthly,
                            cost_per_query,
                        });
                    } else {
                        rejected.push(RejectedCandidate {
                            instance: instance.name.clone(),
                            cache: cache.label(),
                            quantization: quantization.label().to_string(),
                            nprobe,
                            reasons,
                        });
                    }
                }
            }
        }
    }

    candidates.sort_by(|left, right| {
        left.monthly
            .total
            .total_cmp(&right.monthly.total)
            .then_with(|| left.cost_per_query.total_cmp(&right.cost_per_query))
            .then_with(|| left.instance.name.cmp(&right.instance.name))
            .then_with(|| left.cache.label().cmp(&right.cache.label()))
            .then_with(|| left.quantization.label().cmp(right.quantization.label()))
            .then_with(|| left.nprobe.cmp(&right.nprobe))
    });
    rejected.sort_by(|left, right| {
        left.instance
            .cmp(&right.instance)
            .then_with(|| left.cache.cmp(&right.cache))
            .then_with(|| left.quantization.cmp(&right.quantization))
            .then_with(|| left.nprobe.cmp(&right.nprobe))
    });

    Ok(PlanReport {
        catalog_snapshot_date: catalog.snapshot_date().to_string(),
        calibration_snapshot_date: model.snapshot_date.clone(),
        nlist,
        candidates,
        rejected,
        assumptions: vec![
            "[E] service aggregate ceiling is 100000 MB/s per node, capped by baseline NIC"
                .to_string(),
            "[E] CPU capacity assumes 8 ms of CPU service per query".to_string(),
            "[E] at-rest bytes include quantized rows plus f32 rerank rows, 15% slack, 35% for FTS, and 10% for filters when selected".to_string(),
            "[E] block-cache volumes are 1.2x estimated at-rest bytes; IOPS are provisioned at 4 per MB/s within tier limits".to_string(),
            "[E] monthly estimates use 730 hours and request volume at required QPS, or predicted QPS when unconstrained".to_string(),
        ],
    })
}

/// Runs [`plan`] with the catalog and model snapshots compiled into Zeppelin.
///
/// # Errors
///
/// Returns the same strict input and region errors as [`plan`].
pub fn plan_embedded(request: &PlanRequest) -> Result<PlanReport, AdvisorError> {
    plan(
        &Catalog::embedded(),
        &CalibratedShapeModel::embedded(),
        request,
    )
}

/// Scales a shape through the advisor's canonical nlist/row-byte seam and
/// evaluates it with an explicit profile.
///
/// This is public so calibration audits can compare the advisor path with a
/// direct [`predict`] call without involving catalog candidate enumeration.
#[must_use]
pub fn predict_for_profile(
    model: &CalibratedShapeModel,
    profile: &Profile,
    shape: DataShape,
    quantization: Quantization,
    nprobe: usize,
) -> Prediction {
    let nlist = IndexingConfig::default().effective_num_centroids(shape.vectors);
    let input = model.predict_target(
        shape.vectors,
        shape.dims,
        nlist,
        nprobe,
        row_bytes(quantization, shape.dims),
        profile.client.closed_loop_clients,
    );
    predict(&input, profile)
}

fn validate_request(request: &PlanRequest) -> Result<(), AdvisorError> {
    let invalid = |reason: &str| AdvisorError::InvalidRequest {
        reason: reason.to_string(),
    };
    if request.region.is_empty() {
        return Err(invalid("region must not be empty"));
    }
    if request.shape.vectors == 0 {
        return Err(invalid("vectors must be nonzero"));
    }
    if request.shape.dims == 0 {
        return Err(invalid("dims must be nonzero"));
    }
    if request.replicas == 0 {
        return Err(invalid("replicas must be nonzero"));
    }
    if request.clients == 0 {
        return Err(invalid("clients must be nonzero"));
    }
    if request.quantizations.is_empty() {
        return Err(invalid("quantization sweep must not be empty"));
    }
    if request.nprobes.is_empty() {
        return Err(invalid("nprobe sweep must not be empty"));
    }
    if request.nprobes.contains(&0) {
        return Err(invalid("nprobe values must be nonzero"));
    }
    for (name, value) in [
        ("qps", request.min_qps),
        ("p99-ms", request.max_p99_ms),
        ("budget-month", request.max_monthly_usd),
    ] {
        if value.is_some_and(|number| !number.is_finite() || number <= 0.0) {
            return Err(invalid(&format!("{name} must be finite and positive")));
        }
    }
    let mut nprobes = request.nprobes.clone();
    nprobes.sort_unstable();
    nprobes.dedup();
    if nprobes.len() != request.nprobes.len() {
        return Err(invalid("nprobe sweep contains duplicates"));
    }
    let mut quantization_labels = request
        .quantizations
        .iter()
        .map(|quantization| quantization.label())
        .collect::<Vec<_>>();
    quantization_labels.sort_unstable();
    quantization_labels.dedup();
    if quantization_labels.len() != request.quantizations.len() {
        return Err(invalid("quantization sweep contains duplicates"));
    }
    Ok(())
}

fn candidate_profile(
    request: &PlanRequest,
    instance: &InstanceSku,
    instance_price_hr: f64,
    store: &ObjectStoreSku,
) -> Profile {
    Profile {
        name: format!(
            "advisor-{}-{}-{}",
            request.cloud.name(),
            request.region,
            instance.name
        ),
        storage: StorageProfile {
            ttfb_ms: store.ttfb_ms,
            per_conn_mbps: store.per_conn_mbps,
            agg_mbps_per_node: node_aggregate_mbps(instance),
            price: StoragePrice {
                get_per_req: store.get_per_req(),
                put_per_req: store.put_per_req(),
                egress_per_gb: store.egress_same_region_per_gb,
            },
        },
        node: NodeProfile {
            count: request.replicas,
            vcpus: instance.vcpus,
            mem_gb: instance.mem_gb.ceil() as usize,
            price_hr: instance_price_hr,
            cpu_ms_per_query: CPU_MS_PER_QUERY,
        },
        client: ClientProfile {
            closed_loop_clients: request.clients,
        },
        whatif: None,
    }
}

/// Estimates source-of-truth storage in decimal GB for one data shape.
///
/// The estimate includes the chosen coarse row, a full-precision rerank row,
/// optional FTS and bitmap sidecars, and the advisor's 15% immutable-artifact
/// slack. It is shared with the config tuner so cache sizing cannot drift from
/// plan pricing.
#[must_use]
pub fn estimated_object_storage_gb(shape: DataShape, quantization: Quantization) -> f64 {
    let mut bytes = shape.vectors as f64
        * (row_bytes(quantization, shape.dims) as f64 + (shape.dims * 4) as f64);
    if shape.fts {
        bytes *= 1.35;
    }
    if shape.filters {
        bytes *= 1.10;
    }
    bytes *= 1.15;
    bytes / GB_BYTES
}

/// Converts an instance's baseline network rating into the aggregate
/// per-node storage bandwidth used by the advisor profile.
#[must_use]
pub fn node_aggregate_mbps(instance: &InstanceSku) -> f64 {
    (instance.network_baseline_gbps * GBPS_TO_MBPS).min(SERVICE_AGGREGATE_CEILING_MBPS)
}

fn cache_plans(
    instance: &InstanceSku,
    tiers: &[BlockStorageSku],
    storage_gb: f64,
) -> Vec<CacheDevicePlan> {
    let mut plans = Vec::with_capacity(tiers.len() + usize::from(instance.nvme_gb > 0));
    if instance.nvme_gb > 0 {
        plans.push(CacheDevicePlan::Nvme {
            capacity_gb: instance.nvme_gb,
        });
    }
    let volume_gb = (storage_gb * BLOCK_VOLUME_SLACK).ceil() as u64;
    let node_mbps = (instance.network_baseline_gbps * GBPS_TO_MBPS).ceil() as u64;
    for tier in tiers {
        let throughput_mbps = node_mbps
            .max(tier.included_throughput_mbps)
            .min(tier.max_throughput_mbps);
        let iops = throughput_mbps
            .saturating_mul(4)
            .max(tier.included_iops)
            .min(tier.max_iops);
        plans.push(CacheDevicePlan::Block {
            tier: tier.name.clone(),
            volume_gb: volume_gb.max(1),
            iops,
            throughput_mbps,
        });
    }
    plans
}

fn cache_monthly_cost(cache: &CacheDevicePlan, tiers: &[BlockStorageSku], region: &str) -> f64 {
    let CacheDevicePlan::Block {
        tier,
        volume_gb,
        iops,
        throughput_mbps,
    } = cache
    else {
        return 0.0;
    };
    let sku = tiers
        .iter()
        .find(|sku| sku.name == *tier)
        .unwrap_or_else(|| panic!("cache plan names missing block tier {tier}"));
    let extra_iops = iops.saturating_sub(sku.included_iops);
    let extra_throughput = throughput_mbps.saturating_sub(sku.included_throughput_mbps);
    let base = *volume_gb as f64 * sku.price_gb_month
        + extra_iops as f64 * sku.price_per_iops_month
        + extra_throughput as f64 * sku.price_per_throughput_mbps_month;
    base * sku.price_multiplier.get(region).copied().unwrap_or(1.0)
}

fn advisor_bottleneck(
    instance: &InstanceSku,
    prediction: &Prediction,
    qps_cpu_cap: f64,
) -> AdvisorBottleneck {
    if qps_cpu_cap < prediction.qps {
        return AdvisorBottleneck::Cpu;
    }
    match prediction.bottleneck {
        Bottleneck::ClosedLoop => AdvisorBottleneck::ClosedLoop,
        Bottleneck::Bandwidth
            if instance.network_baseline_gbps * GBPS_TO_MBPS < SERVICE_AGGREGATE_CEILING_MBPS =>
        {
            AdvisorBottleneck::NodeNic
        }
        Bottleneck::Bandwidth => AdvisorBottleneck::Bandwidth,
    }
}

fn rejection_reasons(
    request: &PlanRequest,
    qps: f64,
    p99_ms: f64,
    monthly_usd: f64,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(minimum) = request.min_qps.filter(|minimum| qps < *minimum) {
        reasons.push(format!("QPS {qps:.2} below required {minimum:.2}"));
    }
    if let Some(maximum) = request.max_p99_ms.filter(|maximum| p99_ms > *maximum) {
        reasons.push(format!("p99 {p99_ms:.2} ms exceeds {maximum:.2} ms"));
    }
    if let Some(maximum) = request
        .max_monthly_usd
        .filter(|maximum| monthly_usd > *maximum)
    {
        reasons.push(format!(
            "monthly cost ${monthly_usd:.2} exceeds ${maximum:.2}"
        ));
    }
    reasons
}

#[cfg(test)]
mod tests {
    use super::super::profiles::load_profile_from_str;
    use super::*;

    fn sample_request() -> PlanRequest {
        PlanRequest {
            cloud: Cloud::Aws,
            region: "us-east-1".to_string(),
            shape: DataShape {
                vectors: 21_000_000,
                dims: 768,
                filters: false,
                fts: false,
            },
            replicas: 3,
            min_qps: None,
            max_p99_ms: None,
            max_monthly_usd: None,
            arch: ArchFilter::Any,
            clients: 8,
            quantizations: vec![Quantization::RabitqTwoBit],
            nprobes: vec![32, 64, 256],
        }
    }

    fn signature(report: &PlanReport) -> Vec<String> {
        report
            .candidates
            .iter()
            .map(|candidate| {
                format!(
                    "{}|{}|{}|{}|{:.12}|{:.12}",
                    candidate.instance.name,
                    candidate.cache.label(),
                    candidate.quantization.label(),
                    candidate.nprobe,
                    candidate.qps,
                    candidate.monthly.total
                )
            })
            .collect()
    }

    #[test]
    fn plan_is_deterministic_and_uses_canonical_nlist() {
        let catalog = Catalog::embedded();
        let model = CalibratedShapeModel::embedded();
        let request = sample_request();
        let first = plan(&catalog, &model, &request)
            .unwrap_or_else(|error| panic!("first plan failed: {error}"));
        let second = plan(&catalog, &model, &request)
            .unwrap_or_else(|error| panic!("second plan failed: {error}"));
        assert_eq!(first.nlist, 4_096);
        assert!(!first.candidates.is_empty());
        assert_eq!(signature(&first), signature(&second));
    }

    #[test]
    fn impossible_constraints_reject_every_combination_with_reasons() {
        let mut request = sample_request();
        request.min_qps = Some(1_000_000_000.0);
        request.max_monthly_usd = Some(0.01);
        let report = plan_embedded(&request)
            .unwrap_or_else(|error| panic!("constraint plan failed: {error}"));
        assert!(report.candidates.is_empty());
        assert!(!report.rejected.is_empty());
        assert!(report.rejected.iter().all(|candidate| {
            candidate
                .reasons
                .iter()
                .any(|reason| reason.starts_with("QPS "))
                && candidate
                    .reasons
                    .iter()
                    .any(|reason| reason.starts_with("monthly cost "))
        }));
    }

    #[test]
    fn shipped_s3_profile_matches_direct_prediction_through_advisor_path() {
        let profile = load_profile_from_str(
            "s3-3node-wikidpr",
            include_str!("../../tests/perf_contract/profiles/s3-3node-wikidpr.toml"),
        );
        let model = CalibratedShapeModel::embedded();
        let shape = DataShape {
            vectors: 21_000_000,
            dims: 768,
            filters: false,
            fts: false,
        };
        let through_advisor =
            predict_for_profile(&model, &profile, shape, Quantization::RabitqTwoBit, 256);
        let direct_input = model.predict_target(21_000_000, 768, 4_096, 256, 200, 8);
        let direct = predict(&direct_input, &profile);
        for (left, right) in [
            (through_advisor.bytes_per_query, direct.bytes_per_query),
            (through_advisor.gets_per_query, direct.gets_per_query),
            (through_advisor.qps, direct.qps),
            (through_advisor.p50_ms, direct.p50_ms),
            (through_advisor.p99_ms, direct.p99_ms),
            (through_advisor.total_cost, direct.total_cost),
        ] {
            assert!((left - right).abs() < 1e-12, "{left} != {right}");
        }
        assert_eq!(through_advisor.bottleneck, direct.bottleneck);
        assert!((through_advisor.qps - 96.18).abs() < 0.01);
        assert!((through_advisor.p99_ms - 218.18).abs() < 0.01);
    }
}
