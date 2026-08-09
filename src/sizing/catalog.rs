//! Embedded cloud hardware and object-store pricing catalog.
//!
//! The catalog is a curated, snapshot-dated dataset compiled into the
//! binary via `include_str!`: instance SKUs (specs, network baselines, and
//! per-region on-demand prices), block-storage tiers, object-store price
//! sheets per region group, and a small cross-region RTT matrix. It is
//! refreshed by `scripts/refresh_cloud_catalog.py`; the advisor prints the
//! snapshot date with every prediction so staleness is always visible.
//!
//! Prices are USD. S3 Express One Zone is deliberately out of scope —
//! standard object-store tiers only.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::profiles::Percentiles;

/// The three supported clouds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cloud {
    /// Amazon Web Services.
    Aws,
    /// Google Cloud Platform.
    Gcp,
    /// Microsoft Azure.
    Azure,
}

impl Cloud {
    /// Stable lowercase label matching the TOML encoding.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Gcp => "gcp",
            Self::Azure => "azure",
        }
    }

    /// Parses the lowercase label used on the CLI and in RTT keys.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "aws" => Some(Self::Aws),
            "gcp" => Some(Self::Gcp),
            "azure" => Some(Self::Azure),
            _ => None,
        }
    }
}

/// CPU architecture of an instance SKU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Arch {
    /// x86-64 (Intel/AMD).
    #[serde(rename = "x86_64")]
    X86_64,
    /// 64-bit Arm (Graviton, Ampere, Cobalt).
    #[serde(rename = "arm64")]
    Arm64,
}

/// Snapshot provenance carried by every catalog file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogMeta {
    /// Which cloud a per-cloud file describes; absent for shared files.
    #[serde(default)]
    pub cloud: Option<Cloud>,
    /// Date the prices were captured, `YYYY-MM-DD`.
    pub snapshot_date: String,
    /// Price currency; always USD today.
    pub currency: String,
    /// Source URLs the snapshot was compiled from.
    pub sources: Vec<String>,
}

/// One instance SKU: shape, network, and per-region on-demand price.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceSku {
    /// Vendor SKU name, e.g. `m7i.2xlarge`, `n2-standard-8`, `D8s_v5`.
    pub name: String,
    /// Family grouping used for filtering, e.g. `m7i`.
    pub family: String,
    /// CPU architecture.
    pub arch: Arch,
    /// vCPU count.
    pub vcpus: usize,
    /// Memory in GiB.
    pub mem_gb: f64,
    /// Local NVMe/instance-store capacity in GB; 0 when none.
    pub nvme_gb: u64,
    /// Sustained (baseline) network bandwidth in Gbps — the honest number.
    pub network_baseline_gbps: f64,
    /// Burst network bandwidth in Gbps (vendor "up to").
    pub network_burst_gbps: f64,
    /// On-demand hourly USD price per region.
    pub price_hr: BTreeMap<String, f64>,
}

impl InstanceSku {
    /// On-demand hourly price in `region`, if the snapshot covers it.
    #[must_use]
    pub fn price_in(&self, region: &str) -> Option<f64> {
        self.price_hr.get(region).copied()
    }
}

/// One block-storage tier (EBS/PD/managed disk) and its price components.
///
/// Fixed-size tiers (Azure Premium SSD P-levels) are approximated as an
/// effective $/GB-month in the data files, with the approximation noted in
/// the file comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockStorageSku {
    /// Tier name, e.g. `gp3`, `io2`, `pd-ssd`, `premium-ssd-v2`.
    pub name: String,
    /// Capacity price in $/GB-month (us-east-1-equivalent base).
    pub price_gb_month: f64,
    /// IOPS included with the volume at no extra charge.
    #[serde(default)]
    pub included_iops: u64,
    /// Throughput (MBps) included at no extra charge.
    #[serde(default)]
    pub included_throughput_mbps: u64,
    /// Price per provisioned IOPS-month above the included amount.
    #[serde(default)]
    pub price_per_iops_month: f64,
    /// Price per provisioned MBps-month above the included amount.
    #[serde(default)]
    pub price_per_throughput_mbps_month: f64,
    /// Maximum IOPS one volume supports.
    pub max_iops: u64,
    /// Maximum throughput (MBps) one volume supports.
    pub max_throughput_mbps: u64,
    /// Regional price multipliers relative to the base price; 1.0 assumed
    /// for regions not listed.
    #[serde(default)]
    pub price_multiplier: BTreeMap<String, f64>,
}

/// One object-store price sheet shared by a group of regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreSku {
    /// Which cloud serves this store.
    pub cloud: Cloud,
    /// Service tier, e.g. `s3-standard`, `gcs-standard`, `blob-hot-lrs`.
    pub service: String,
    /// Regions this price sheet applies to.
    pub regions: Vec<String>,
    /// Storage price in $/GB-month (first tier).
    pub storage_gb_month: f64,
    /// GET/read request price per 1,000 requests.
    pub get_per_1k: f64,
    /// PUT/write request price per 1,000 requests.
    pub put_per_1k: f64,
    /// Same-region egress to compute, $/GB (0 on all three clouds).
    pub egress_same_region_per_gb: f64,
    /// Cross-region transfer price, $/GB.
    pub egress_cross_region_per_gb: f64,
    /// Time-to-first-byte percentiles from same-region compute, ms.
    pub ttfb_ms: Percentiles,
    /// Sustained per-connection throughput in decimal MB/s.
    pub per_conn_mbps: f64,
}

impl ObjectStoreSku {
    /// Request price per single GET, the unit the sizing model uses.
    #[must_use]
    pub fn get_per_req(&self) -> f64 {
        self.get_per_1k / 1_000.0
    }

    /// Request price per single PUT, the unit the sizing model uses.
    #[must_use]
    pub fn put_per_req(&self) -> f64 {
        self.put_per_1k / 1_000.0
    }
}

/// One measured/curated median RTT between two cloud regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RttEntry {
    /// Origin endpoint as `cloud:region`, e.g. `aws:us-east-1`.
    pub from: String,
    /// Destination endpoint as `cloud:region`.
    pub to: String,
    /// Median round-trip time in milliseconds.
    pub rtt_ms: f64,
}

/// One cloud's instance and block-storage inventory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudCatalog {
    /// Snapshot provenance for this file.
    pub meta: CatalogMeta,
    /// Instance SKUs.
    #[serde(rename = "instance")]
    pub instances: Vec<InstanceSku>,
    /// Block-storage tiers.
    #[serde(rename = "block_storage")]
    pub block_storage: Vec<BlockStorageSku>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectStoresFile {
    meta: CatalogMeta,
    #[serde(rename = "object_store")]
    object_stores: Vec<ObjectStoreSku>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RttFile {
    meta: CatalogMeta,
    #[serde(rename = "rtt")]
    entries: Vec<RttEntry>,
}

/// The full embedded catalog across all three clouds.
#[derive(Debug, Clone)]
pub struct Catalog {
    /// Per-cloud instance and block-storage inventories.
    pub clouds: BTreeMap<Cloud, CloudCatalog>,
    /// Provenance for the shared object-store price sheets.
    pub object_store_meta: CatalogMeta,
    /// Object-store price sheets.
    pub object_stores: Vec<ObjectStoreSku>,
    /// Provenance for the curated cross-region RTT matrix.
    pub rtt_meta: CatalogMeta,
    /// Cross-region RTT matrix.
    pub rtt: Vec<RttEntry>,
}

fn validate_meta(label: &str, meta: &CatalogMeta) {
    assert!(
        chrono::NaiveDate::parse_from_str(&meta.snapshot_date, "%Y-%m-%d").is_ok(),
        "{label} snapshot_date {:?} is not YYYY-MM-DD",
        meta.snapshot_date
    );
    assert_eq!(meta.currency, "USD", "{label} prices must be USD");
    assert!(!meta.sources.is_empty(), "{label} catalog lists no sources");
    for source in &meta.sources {
        assert!(
            source.starts_with("https://"),
            "{label} catalog source {source:?} is not an HTTPS URL"
        );
    }
}

impl Catalog {
    /// Loads and validates the catalog compiled into the binary.
    ///
    /// # Panics
    ///
    /// Panics when any embedded file fails strict parsing or validation —
    /// a corrupt catalog must never silently price a deployment.
    #[must_use]
    pub fn embedded() -> Self {
        let catalog = Self::from_sources(
            include_str!("catalog_data/aws.toml"),
            include_str!("catalog_data/gcp.toml"),
            include_str!("catalog_data/azure.toml"),
            include_str!("catalog_data/object_stores.toml"),
            include_str!("catalog_data/rtt.toml"),
        );
        catalog.validate();
        catalog
    }

    /// Parses the five catalog documents without validating cross-file
    /// invariants; [`Self::validate`] completes the contract.
    ///
    /// # Panics
    ///
    /// Panics when any document fails strict parsing or a per-cloud file
    /// declares the wrong cloud.
    #[must_use]
    pub fn from_sources(aws: &str, gcp: &str, azure: &str, object_stores: &str, rtt: &str) -> Self {
        let mut clouds = BTreeMap::new();
        for (cloud, source, label) in [
            (Cloud::Aws, aws, "aws.toml"),
            (Cloud::Gcp, gcp, "gcp.toml"),
            (Cloud::Azure, azure, "azure.toml"),
        ] {
            let parsed: CloudCatalog = toml::from_str(source)
                .unwrap_or_else(|error| panic!("invalid catalog file {label}: {error}"));
            assert_eq!(
                parsed.meta.cloud,
                Some(cloud),
                "catalog file {label} declares the wrong cloud"
            );
            clouds.insert(cloud, parsed);
        }
        let stores: ObjectStoresFile = toml::from_str(object_stores)
            .unwrap_or_else(|error| panic!("invalid catalog file object_stores.toml: {error}"));
        let rtt: RttFile = toml::from_str(rtt)
            .unwrap_or_else(|error| panic!("invalid catalog file rtt.toml: {error}"));
        Self {
            clouds,
            object_store_meta: stores.meta,
            object_stores: stores.object_stores,
            rtt_meta: rtt.meta,
            rtt: rtt.entries,
        }
    }

    /// Enforces every cross-entry invariant the advisor relies on.
    ///
    /// # Panics
    ///
    /// Panics on the first violated invariant with an entry-specific
    /// message.
    pub fn validate(&self) {
        validate_meta("object_stores", &self.object_store_meta);
        validate_meta("rtt", &self.rtt_meta);
        assert_eq!(
            self.object_store_meta.cloud, None,
            "object_stores meta must not name one cloud"
        );
        assert_eq!(
            self.rtt_meta.cloud, None,
            "rtt meta must not name one cloud"
        );
        assert_eq!(
            self.rtt_meta.snapshot_date, self.object_store_meta.snapshot_date,
            "rtt and object-store snapshots must have the same date"
        );

        for (cloud, catalog) in &self.clouds {
            validate_meta(cloud.name(), &catalog.meta);
            assert_eq!(
                catalog.meta.snapshot_date,
                self.object_store_meta.snapshot_date,
                "{} and object-store snapshots must have the same date",
                cloud.name()
            );
            assert!(
                !catalog.instances.is_empty(),
                "{} catalog has no instances",
                cloud.name()
            );
            let mut instance_names = BTreeSet::new();
            for instance in &catalog.instances {
                let name = &instance.name;
                assert!(
                    instance_names.insert(name),
                    "{} catalog repeats instance {name}",
                    cloud.name()
                );
                assert!(instance.vcpus > 0, "{name}: vcpus must be nonzero");
                assert!(
                    instance.mem_gb.is_finite() && instance.mem_gb > 0.0,
                    "{name}: mem_gb must be positive"
                );
                assert!(
                    instance.network_baseline_gbps.is_finite()
                        && instance.network_baseline_gbps > 0.0,
                    "{name}: network baseline must be positive"
                );
                assert!(
                    instance.network_burst_gbps >= instance.network_baseline_gbps,
                    "{name}: burst bandwidth below baseline"
                );
                assert!(!instance.price_hr.is_empty(), "{name}: no region prices");
                for (region, price) in &instance.price_hr {
                    assert!(
                        price.is_finite() && *price > 0.0,
                        "{name}: non-positive price in {region}"
                    );
                    assert!(
                        self.object_store_for(*cloud, region).is_some(),
                        "{name}: region {region} has no object-store price sheet"
                    );
                }
            }
            let mut storage_names = BTreeSet::new();
            for tier in &catalog.block_storage {
                let name = &tier.name;
                assert!(
                    storage_names.insert(name),
                    "{} catalog repeats block-storage tier {name}",
                    cloud.name()
                );
                assert!(
                    tier.price_gb_month.is_finite() && tier.price_gb_month > 0.0,
                    "{name}: capacity price must be positive"
                );
                assert!(tier.max_iops > 0, "{name}: max_iops must be nonzero");
                assert!(
                    tier.max_throughput_mbps > 0,
                    "{name}: max throughput must be nonzero"
                );
                for value in [
                    tier.price_per_iops_month,
                    tier.price_per_throughput_mbps_month,
                ] {
                    assert!(
                        value.is_finite() && value >= 0.0,
                        "{name}: negative price component"
                    );
                }
                for (region, multiplier) in &tier.price_multiplier {
                    assert!(
                        multiplier.is_finite() && *multiplier > 0.0,
                        "{name}: non-positive multiplier for {region}"
                    );
                }
            }
        }
        let mut store_regions = BTreeSet::new();
        for store in &self.object_stores {
            let label = format!("{}/{}", store.cloud.name(), store.service);
            assert!(!store.regions.is_empty(), "{label}: no regions");
            for region in &store.regions {
                assert!(
                    store_regions.insert((store.cloud, region)),
                    "{} region {region} has more than one object-store price sheet",
                    store.cloud.name()
                );
            }
            for value in [
                store.storage_gb_month,
                store.get_per_1k,
                store.put_per_1k,
                store.egress_same_region_per_gb,
                store.egress_cross_region_per_gb,
            ] {
                assert!(
                    value.is_finite() && value >= 0.0,
                    "{label}: negative price component"
                );
            }
            assert!(
                store.ttfb_ms.p50 > 0.0 && store.ttfb_ms.p99 >= store.ttfb_ms.p50,
                "{label}: invalid TTFB percentiles"
            );
            assert!(
                store.per_conn_mbps.is_finite() && store.per_conn_mbps > 0.0,
                "{label}: per-connection throughput must be positive"
            );
        }
        let mut rtt_pairs = BTreeSet::new();
        for entry in &self.rtt {
            for endpoint in [&entry.from, &entry.to] {
                let (cloud_name, region) = endpoint
                    .split_once(':')
                    .unwrap_or_else(|| panic!("rtt endpoint {endpoint:?} is not cloud:region"));
                let cloud = Cloud::parse(cloud_name)
                    .unwrap_or_else(|| panic!("rtt endpoint {endpoint:?} names an unknown cloud"));
                assert!(
                    self.object_store_for(cloud, region).is_some(),
                    "rtt endpoint {endpoint:?} has no object-store price sheet"
                );
            }
            let pair = if entry.from < entry.to {
                (&entry.from, &entry.to)
            } else {
                (&entry.to, &entry.from)
            };
            assert!(
                rtt_pairs.insert(pair),
                "duplicate RTT pair {} <-> {}",
                entry.from,
                entry.to
            );
            assert!(
                entry.rtt_ms.is_finite() && entry.rtt_ms > 0.0,
                "rtt {} -> {} must be positive",
                entry.from,
                entry.to
            );
        }
    }

    /// Date shared by every validated embedded catalog document.
    #[must_use]
    pub fn snapshot_date(&self) -> &str {
        &self.object_store_meta.snapshot_date
    }

    /// The object-store price sheet serving `region` on `cloud`, if any.
    #[must_use]
    pub fn object_store_for(&self, cloud: Cloud, region: &str) -> Option<&ObjectStoreSku> {
        self.object_stores
            .iter()
            .find(|store| store.cloud == cloud && store.regions.iter().any(|r| r == region))
    }

    /// The named instance SKU on `cloud`, if the snapshot carries it.
    #[must_use]
    pub fn instance(&self, cloud: Cloud, name: &str) -> Option<&InstanceSku> {
        self.clouds
            .get(&cloud)?
            .instances
            .iter()
            .find(|instance| instance.name == name)
    }

    /// The named block-storage tier on `cloud`, if the snapshot carries it.
    #[must_use]
    pub fn block_storage(&self, cloud: Cloud, name: &str) -> Option<&BlockStorageSku> {
        self.clouds
            .get(&cloud)?
            .block_storage
            .iter()
            .find(|tier| tier.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_catalog_parses_and_validates() {
        let catalog = Catalog::embedded();
        assert_eq!(catalog.clouds.len(), 3);
        assert!(!catalog.object_stores.is_empty());
        assert!(!catalog.rtt.is_empty());
    }

    #[test]
    fn s3_standard_get_price_matches_the_shipped_perf_profile() {
        // tests/perf_contract/profiles/s3-3node-wikidpr.toml pins
        // get_per_req = 4.0e-7; the catalog must agree with the profile
        // the Tier 2 model was validated against.
        let catalog = Catalog::embedded();
        let store = catalog
            .object_store_for(Cloud::Aws, "us-east-1")
            .unwrap_or_else(|| panic!("us-east-1 missing from the S3 catalog"));
        assert!((store.get_per_req() - 4.0e-7).abs() < 1e-12);
    }

    #[test]
    fn every_cloud_has_an_nvme_option_or_block_storage() {
        // The advisor requires a cache-device story on every cloud.
        let catalog = Catalog::embedded();
        for (cloud, entry) in &catalog.clouds {
            let has_nvme = entry.instances.iter().any(|i| i.nvme_gb > 0);
            assert!(
                has_nvme || !entry.block_storage.is_empty(),
                "{} offers neither NVMe instances nor block storage",
                cloud.name()
            );
        }
    }

    #[test]
    fn embedded_scope_and_region_coverage_are_pinned() {
        let catalog = Catalog::embedded();
        let expected = [
            (
                Cloud::Aws,
                40,
                [
                    "c7g", "c7i", "i3en", "i4i", "im4gn", "m6i", "m7g", "m7i", "r7g", "r7i",
                ]
                .as_slice(),
            ),
            (Cloud::Gcp, 21, ["c3", "c3d", "n2", "n2d", "z3"].as_slice()),
            (
                Cloud::Azure,
                17,
                ["Ddsv5", "Dsv5", "Edsv5", "Esv5", "Lsv3"].as_slice(),
            ),
        ];
        for (cloud, count, families) in expected {
            let cloud_catalog = &catalog.clouds[&cloud];
            assert_eq!(cloud_catalog.instances.len(), count);
            let actual = cloud_catalog
                .instances
                .iter()
                .map(|instance| instance.family.as_str())
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, families.iter().copied().collect());
            assert!(
                cloud_catalog
                    .instances
                    .iter()
                    .all(|instance| instance.price_hr.len() == 6),
                "{} has incomplete regional prices",
                cloud.name()
            );
        }

        let gcp = &catalog.clouds[&Cloud::Gcp];
        assert!(gcp
            .instances
            .iter()
            .filter(|instance| instance.family == "z3")
            .all(|instance| instance.name.ends_with("lssd") && instance.nvme_gb >= 3_000));
    }

    #[test]
    fn every_catalog_document_uses_one_snapshot_date() {
        let catalog = Catalog::embedded();
        assert_eq!(catalog.snapshot_date(), "2026-08-05");
        assert!(catalog
            .clouds
            .values()
            .all(|cloud| cloud.meta.snapshot_date == catalog.snapshot_date()));
        assert_eq!(catalog.rtt_meta.snapshot_date, catalog.snapshot_date());
    }

    #[test]
    fn wrong_cloud_declaration_is_rejected() {
        let aws = include_str!("catalog_data/aws.toml");
        let gcp = include_str!("catalog_data/gcp.toml");
        let azure = include_str!("catalog_data/azure.toml");
        let stores = include_str!("catalog_data/object_stores.toml");
        let rtt = include_str!("catalog_data/rtt.toml");
        let result = std::panic::catch_unwind(|| {
            // gcp.toml handed to the aws slot must be rejected.
            let _ = Catalog::from_sources(gcp, aws, azure, stores, rtt);
        });
        assert!(result.is_err(), "cloud mismatch was not detected");
    }
}
