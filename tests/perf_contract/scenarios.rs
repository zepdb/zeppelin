//! Phase-1 performance-contract scenario catalog.

use serde_json::json;

use super::contract::{
    CacheState as ContractCacheState, Consistency, ContractSpec, MeasureSpec, Quantization,
};
use super::dataset::{AttrShape, FtsShape};
use super::scenario::{CacheState, MeasureOp, ScenarioSpec, ServerKnobs, Step};

const DEFAULT_QUERY_NPROBE: usize = 4;
const DEFAULT_UPSERT_NPROBE: usize = 1;
const DEFAULT_MEMORY_CACHE_MAX_MB: usize = 256;
const DEFAULT_MAX_WAL_FRAGMENTS_BEFORE_COMPACT: usize = 64;

/// Build one of the three Phase-1 runner scenarios from its loaded contract.
#[must_use]
pub fn build(contract: &ContractSpec, env_repeats: usize) -> ScenarioSpec {
    assert_eq!(
        contract.schema_version, 1,
        "unsupported performance-contract schema version: {}",
        contract.schema_version
    );
    assert!(
        matches!(
            contract.scenario.as_str(),
            "warm_query_strong" | "cold_query_strong" | "upsert_single"
        ),
        "unknown Phase-1 performance-contract scenario: {}",
        contract.scenario
    );
    assert!(
        matches!(&contract.dataset.attrs, AttrShape::None),
        "Phase-1 scenarios do not support attribute dataset shapes"
    );
    assert!(
        matches!(&contract.dataset.fts, FtsShape::None),
        "Phase-1 scenarios do not support FTS dataset shapes"
    );
    assert!(
        env_repeats > 0,
        "performance-contract repeats must be greater than zero"
    );

    let measure = measure_op(&contract.run.measure);

    let nprobe = contract
        .server_config
        .nprobe
        .unwrap_or_else(|| default_nprobe(&measure));
    assert!(
        nprobe > 0,
        "performance-contract nprobe must be greater than zero"
    );

    let cache_state = match &contract.run.cache_state {
        ContractCacheState::Cold => CacheState::Cold,
        ContractCacheState::Warm => CacheState::Warm {
            prime: vec![Step::Measure],
        },
        ContractCacheState::WarmHydrated => CacheState::WarmHydrated,
    };
    let repeats = if matches!(&cache_state, CacheState::Cold) {
        1
    } else {
        env_repeats
    };

    let quantization = match contract.ns_config.as_ref() {
        Some(config) => match &config.quantization {
            Quantization::Sq8 => "scalar",
        },
        None => "scalar",
    };
    let dataset = contract.dataset.clone();
    let ns_config = json!({
        "dimensions": dataset.dims,
        "distance_metric": "euclidean",
        "full_text_search": {},
        "index_config": {
            "nlist": dataset.nlist,
            "quantization": quantization,
            "pq_m": 1,
            "hierarchical": false,
            "fts_index": false,
            "bitmap_index": false,
        }
    });

    ScenarioSpec {
        name: contract.scenario.clone(),
        dataset,
        ns_config,
        server_config: ServerKnobs {
            nprobe,
            manifest_cache_ttl_ms: contract.server_config.manifest_cache_ttl_ms,
            memory_cache_max_mb: contract
                .server_config
                .memory_cache_max_mb
                .unwrap_or(DEFAULT_MEMORY_CACHE_MAX_MB),
            max_wal_fragments_before_compact: contract
                .server_config
                .max_wal_fragments_before_compact
                .unwrap_or(DEFAULT_MAX_WAL_FRAGMENTS_BEFORE_COMPACT),
        },
        cache_state,
        measure,
        repeats,
    }
}

fn measure_op(measure: &MeasureSpec) -> MeasureOp {
    match measure {
        MeasureSpec::Query {
            consistency,
            top_k,
            query_index,
        } => {
            let consistency = match consistency {
                Consistency::Strong => "strong",
                Consistency::Eventual => {
                    panic!("eventual consistency scenarios require Phase 2")
                }
            };
            MeasureOp::Query {
                consistency,
                top_k: *top_k,
                query_index: *query_index,
            }
        }
        MeasureSpec::Upsert { batch } => MeasureOp::Upsert { batch: *batch },
    }
}

fn default_nprobe(measure: &MeasureOp) -> usize {
    match measure {
        MeasureOp::Query { .. } => DEFAULT_QUERY_NPROBE,
        MeasureOp::Upsert { .. } => DEFAULT_UPSERT_NPROBE,
    }
}
