//! End-to-end validation matrix for advisor-emitted `zeppelin.toml` files.

use std::process::Command;

use proptest::prelude::*;
use zeppelin::config::Config;
use zeppelin::sizing::advisor::DataShape;
use zeppelin::sizing::catalog::{Catalog, Cloud, InstanceSku};
use zeppelin::sizing::emit::{render_and_validate, write_validated_config};
use zeppelin::sizing::rows::Quantization;
use zeppelin::sizing::tuner::{
    tune, CacheSelection, SafetyIntervals, SecurityChoice, TunedKnobs, TuningRequest,
};

const CHILD_CONFIG_ENV: &str = "ZEPPELIN_ADVISOR_TEST_CONFIG_PATH";

fn tune_fixture(
    cloud: Cloud,
    instance: &InstanceSku,
    cache: CacheSelection,
    security: SecurityChoice,
    fts: bool,
    safety: SafetyIntervals,
) -> TunedKnobs {
    let region = instance
        .price_hr
        .keys()
        .next()
        .unwrap_or_else(|| panic!("{} has no region prices", instance.name));
    tune(&TuningRequest {
        cloud,
        region,
        instance,
        replicas: 3,
        bucket: "advisor-grid-bucket",
        shape: DataShape {
            vectors: 1_000_000,
            dims: 768,
            filters: true,
            fts,
        },
        quantization: Quantization::RabitqTwoBit,
        nprobe: 32,
        cache,
        predicted_p99_ms: 250.0,
        security,
        safety,
    })
    .unwrap_or_else(|error| panic!("failed to tune {}: {error}", instance.name))
}

fn open_unsafe_fixture(safety: SafetyIntervals) -> TunedKnobs {
    let catalog = Catalog::embedded();
    let instance = catalog
        .instance(Cloud::Aws, "m7i.xlarge")
        .unwrap_or_else(|| panic!("missing m7i.xlarge fixture"));
    tune_fixture(
        Cloud::Aws,
        instance,
        CacheSelection::Block {
            tier: "gp3".to_string(),
            volume_gb: 750,
        },
        SecurityChoice::OpenUnsafe,
        false,
        safety,
    )
}

#[test]
fn every_catalog_instance_cache_security_and_fts_shape_round_trips() {
    let catalog = Catalog::embedded();
    let mut rendered_count = 0usize;
    for (cloud, cloud_catalog) in &catalog.clouds {
        for instance in &cloud_catalog.instances {
            // The pure renderer supports both cache layouts. The CLI separately
            // refuses `--cache-device nvme` when a catalog SKU has no local NVMe.
            for cache in [
                CacheSelection::Nvme { capacity_gb: 750 },
                CacheSelection::Block {
                    tier: "gp3".to_string(),
                    volume_gb: 750,
                },
            ] {
                for security in [SecurityChoice::Enforced, SecurityChoice::OpenUnsafe] {
                    for fts in [false, true] {
                        let knobs = tune_fixture(
                            *cloud,
                            instance,
                            cache.clone(),
                            security,
                            fts,
                            SafetyIntervals::default(),
                        );
                        let rendered = render_and_validate(&knobs).unwrap_or_else(|error| {
                            panic!(
                                "render failed for {}/{}/{}/{security:?}/fts={fts}: {error}",
                                cloud.name(),
                                instance.name,
                                cache.label()
                            )
                        });
                        assert_eq!(
                            rendered
                                .config()
                                .indexing
                                .effective_num_centroids(1_000_000),
                            knobs.nlist()
                        );
                        rendered_count += 1;
                    }
                }
            }
        }
    }
    assert_eq!(rendered_count, (40 + 21 + 17) * 2 * 2 * 2);
}

/// GCP and Azure emits pass full validation, carry their per-cloud storage
/// fields, and pin the transport gap: `ZeppelinStore::from_config` still
/// rejects the emitted backend until the GCS/Azure transports land
/// (multi-substrate plans 05/06 flip this assertion to construct-success).
#[test]
fn gcp_and_azure_emits_carry_storage_fields_and_pin_transport_gap() {
    let catalog = Catalog::embedded();
    for (cloud, expected_lines, transport_gap) in [
        (
            Cloud::Gcp,
            &["backend = \"gcs\"", "# gcs_service_account_path ="][..],
            "unsupported storage backend: gcs",
        ),
        (
            Cloud::Azure,
            &[
                "backend = \"azure\"",
                "azure_account_name = \"REPLACE-WITH-YOUR-STORAGE-ACCOUNT\"",
            ][..],
            "unsupported storage backend: azure",
        ),
    ] {
        let instance = catalog
            .clouds
            .get(&cloud)
            .and_then(|cloud_catalog| cloud_catalog.instances.first())
            .unwrap_or_else(|| panic!("catalog has no {} instances", cloud.name()));
        let knobs = tune_fixture(
            cloud,
            instance,
            CacheSelection::Block {
                tier: "gp3".to_string(),
                volume_gb: 750,
            },
            SecurityChoice::OpenUnsafe,
            false,
            SafetyIntervals::default(),
        );
        let rendered = render_and_validate(&knobs)
            .unwrap_or_else(|error| panic!("{} emit failed validation: {error}", cloud.name()));
        for line in expected_lines {
            assert!(
                rendered.text().contains(line),
                "{} emit must contain {line:?}",
                cloud.name()
            );
        }
        let error = match zeppelin::storage::ZeppelinStore::from_config(&rendered.config().storage)
        {
            Ok(_) => panic!(
                "{} transport arm has not landed yet; construction must fail loudly",
                cloud.name()
            ),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(transport_gap),
            "{} construction error must name the unsupported backend, got: {error}",
            cloud.name()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn emitted_horizon_covers_randomized_reader_safety_floor(
        manifest_ttl_ms in 0u64..120_000,
        registry_ttl_ms in 0u64..120_000,
        minimum_request_secs in 1u64..3_600,
        upload_window_secs in 1u64..3_600,
        skew_slop_secs in 0u64..300,
    ) {
        let safety = SafetyIntervals {
            manifest_cache_ttl_ms: manifest_ttl_ms,
            namespace_registry_ttl_ms: registry_ttl_ms,
            minimum_request_timeout_secs: minimum_request_secs,
            compaction_upload_window_secs: upload_window_secs,
            skew_slop_secs,
        };
        let knobs = open_unsafe_fixture(safety);
        let rendered = render_and_validate(&knobs)
            .unwrap_or_else(|error| panic!("randomized render failed: {error}"));
        let expected_floor = registry_ttl_ms.div_ceil(1_000)
            + manifest_ttl_ms.div_ceil(1_000)
            + knobs.request_timeout_secs()
            + upload_window_secs
            + skew_slop_secs;
        prop_assert_eq!(knobs.gc_horizon_floor_secs(), expected_floor);
        prop_assert!(knobs.gc_horizon_secs() >= expected_floor);
        prop_assert_eq!(rendered.config().gc_horizon_floor_secs(), Some(expected_floor));
        prop_assert!(rendered.config().gc.horizon_secs >= expected_floor);
    }
}

#[test]
fn renderer_never_co_emits_mutually_exclusive_rerank_knobs() {
    let knobs = open_unsafe_fixture(SafetyIntervals::default());
    let rendered =
        render_and_validate(&knobs).unwrap_or_else(|error| panic!("render failed: {error}"));
    let document: toml::Value = toml::from_str(rendered.text())
        .unwrap_or_else(|error| panic!("rendered TOML did not parse: {error}"));
    let query = document["query"]
        .as_table()
        .unwrap_or_else(|| panic!("rendered query table is absent"));
    assert!(query.contains_key("rerank_coalesce_gap_bytes"));
    assert!(!query.contains_key("cost_latency_profile"));
}

#[test]
fn emitted_file_loads_through_real_config_path_with_scrubbed_environment() {
    let knobs = open_unsafe_fixture(SafetyIntervals::default());
    let rendered =
        render_and_validate(&knobs).unwrap_or_else(|error| panic!("render failed: {error}"));
    let directory =
        tempfile::tempdir().unwrap_or_else(|error| panic!("failed to create tempdir: {error}"));
    let path = directory.path().join("zeppelin.toml");
    write_validated_config(&path, &rendered, false)
        .unwrap_or_else(|error| panic!("failed to write test config: {error}"));

    let executable = std::env::current_exe()
        .unwrap_or_else(|error| panic!("failed to locate current test binary: {error}"));
    let status = Command::new(executable)
        .env_clear()
        .env(CHILD_CONFIG_ENV, &path)
        .arg("--exact")
        .arg("real_config_loader_child")
        .arg("--nocapture")
        .status()
        .unwrap_or_else(|error| panic!("failed to launch scrubbed child: {error}"));
    assert!(status.success(), "scrubbed Config::load child failed");
}

#[test]
fn real_config_loader_child() {
    let Ok(path) = std::env::var(CHILD_CONFIG_ENV) else {
        return;
    };
    let config = Config::load(Some(&path))
        .unwrap_or_else(|error| panic!("real Config::load rejected emitted file: {error}"));
    assert_eq!(config.storage.bucket, "advisor-grid-bucket");
    assert_eq!(
        config.effective_rerank_coalesce_gap_bytes(),
        2 * 1024 * 1024
    );
}
