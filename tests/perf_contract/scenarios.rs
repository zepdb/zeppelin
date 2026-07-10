//! Closed performance-contract scenario catalog.

use serde_json::json;

use super::contract::{
    CacheState as ContractCacheState, Consistency, ContractSpec, MeasureSpec, Quantization,
};
use super::dataset::FtsShape;
use super::scenario::{CacheState, MeasureOp, ScenarioSpec, ServerKnobs, SetupPlan, Step};

const DEFAULT_QUERY_NPROBE: usize = 4;
const DEFAULT_WRITE_NPROBE: usize = 1;
const DEFAULT_MEMORY_CACHE_MAX_MB: usize = 256;
const DEFAULT_MAX_WAL_FRAGMENTS_BEFORE_COMPACT: usize = 64;

/// Build one closed-catalog runner scenario from its checked-in contract.
#[must_use]
pub fn build(contract: &ContractSpec, env_repeats: usize) -> ScenarioSpec {
    assert_eq!(
        contract.schema_version, 1,
        "unsupported performance-contract schema version: {}",
        contract.schema_version
    );
    assert!(
        super::ALL_SCENARIOS.contains(&contract.scenario.as_str()),
        "unknown performance-contract scenario: {}",
        contract.scenario
    );
    assert!(
        env_repeats > 0,
        "performance-contract repeats must be greater than zero"
    );

    let measure = measure_op(&contract.run.measure);
    let setup = setup_plan(&contract.scenario, &measure);
    let nprobe = contract
        .server_config
        .nprobe
        .unwrap_or_else(|| default_nprobe(&measure));
    assert!(
        nprobe > 0,
        "performance-contract nprobe must be greater than zero"
    );

    let namespace_options = contract.ns_config.as_ref();
    let bitmap_index = namespace_options.is_some_and(|config| config.bitmap_index);
    let cache_state = cache_state(&contract.run.cache_state, &setup, &measure, bitmap_index);
    let repeats = if matches!(&cache_state, CacheState::Cold | CacheState::WarmHydrated)
        || matches!(
            &measure,
            MeasureOp::Compact { .. } | MeasureOp::Gc | MeasureOp::Hydrate { .. }
        ) {
        1
    } else {
        env_repeats
    };

    let quantization = match namespace_options.map(|config| &config.quantization) {
        Some(Quantization::Sq8) | None => "scalar",
    };
    let fts_index = namespace_options.is_some_and(|config| config.fts_index);
    let dataset = contract.dataset.clone();
    let full_text_search = match &dataset.fts {
        FtsShape::None => json!({}),
        FtsShape::Vocab { .. } => json!({
            "content": {
                "stemming": true,
                "remove_stopwords": true
            }
        }),
    };
    let ns_config = json!({
        "dimensions": dataset.dims,
        "distance_metric": "euclidean",
        "full_text_search": full_text_search,
        "index_config": {
            "nlist": dataset.nlist,
            "quantization": quantization,
            "pq_m": 1,
            "hierarchical": false,
            "fts_index": fts_index,
            "bitmap_index": bitmap_index,
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
            bitmap_index,
            fts_index,
            hydration_enabled: matches!(&measure, MeasureOp::Hydrate { .. }),
        },
        setup,
        cache_state,
        measure,
        repeats,
    }
}

fn cache_state(
    contract: &ContractCacheState,
    setup: &SetupPlan,
    measure: &MeasureOp,
    bitmap_index: bool,
) -> CacheState {
    match contract {
        ContractCacheState::Cold => CacheState::Cold,
        ContractCacheState::Warm => {
            let prime = if matches!(measure, MeasureOp::Compact { .. } | MeasureOp::Gc) {
                Vec::new()
            } else if bitmap_index
                && matches!(
                    measure,
                    MeasureOp::Query {
                        filter: Some(_),
                        ..
                    }
                )
            {
                vec![Step::Measure]
            } else if matches!(measure, MeasureOp::FtsQuery { .. }) {
                vec![Step::Measure]
            } else if matches!(
                setup,
                SetupPlan::EventualDeletes { .. } | SetupPlan::AsOfRetainedGeneration
            ) || matches!(
                measure,
                MeasureOp::Query {
                    filter: Some(_),
                    ..
                } | MeasureOp::HybridQuery { .. }
                    | MeasureOp::QueryPages { .. }
                    | MeasureOp::Fetch { .. }
            ) {
                vec![Step::Query]
            } else {
                vec![Step::Measure]
            };
            CacheState::Warm { prime }
        }
        ContractCacheState::WarmHydrated => CacheState::WarmHydrated,
    }
}

fn setup_plan(name: &str, measure: &MeasureOp) -> SetupPlan {
    match name {
        "warm_query_eventual" => SetupPlan::EventualDeletes { fragments: 2 },
        "as_of_query" => SetupPlan::AsOfRetainedGeneration,
        "compaction_cycle" => SetupPlan::CompactionFull {
            fragments: measured_fragments(measure),
        },
        "compaction_incremental" => SetupPlan::CompactionIncremental {
            fragments: measured_fragments(measure),
        },
        "gc_cycle" => SetupPlan::GcNothingEligible,
        "hydration" => SetupPlan::Hydration,
        _ => SetupPlan::Compacted,
    }
}

fn measured_fragments(measure: &MeasureOp) -> usize {
    match measure {
        MeasureOp::Compact { fragments, .. } => *fragments,
        _ => panic!("compaction setup requires a compact measure"),
    }
}

fn measure_op(measure: &MeasureSpec) -> MeasureOp {
    match measure {
        MeasureSpec::Query {
            consistency,
            top_k,
            query_index,
            filter,
        } => MeasureOp::Query {
            consistency: consistency_name(consistency),
            top_k: *top_k,
            query_index: *query_index,
            filter: filter
                .as_ref()
                .map(|filter| (filter.field.clone(), filter.value.clone())),
        },
        MeasureSpec::FtsQuery {
            consistency,
            top_k,
            field,
            query,
        } => MeasureOp::FtsQuery {
            consistency: consistency_name(consistency),
            top_k: *top_k,
            field: field.clone(),
            query: query.clone(),
        },
        MeasureSpec::HybridQuery {
            consistency,
            top_k,
            candidate_k,
            query_index,
            field,
            query,
        } => MeasureOp::HybridQuery {
            consistency: consistency_name(consistency),
            top_k: *top_k,
            candidate_k: *candidate_k,
            query_index: *query_index,
            field: field.clone(),
            query: query.clone(),
        },
        MeasureSpec::AsOfQuery {
            consistency,
            top_k,
            query_index,
        } => MeasureOp::AsOfQuery {
            consistency: consistency_name(consistency),
            top_k: *top_k,
            query_index: *query_index,
        },
        MeasureSpec::QueryPages {
            consistency,
            pages,
            page_size,
            query_index,
        } => MeasureOp::QueryPages {
            consistency: consistency_name(consistency),
            pages: *pages,
            page_size: *page_size,
            query_index: *query_index,
        },
        MeasureSpec::Fetch { consistency, ids } => MeasureOp::Fetch {
            consistency: consistency_name(consistency),
            ids: *ids,
        },
        MeasureSpec::Upsert { batch } => MeasureOp::Upsert { batch: *batch },
        MeasureSpec::Delete { count } => MeasureOp::Delete { count: *count },
        MeasureSpec::Compact {
            fragments,
            incremental,
        } => MeasureOp::Compact {
            fragments: *fragments,
            incremental: *incremental,
        },
        MeasureSpec::Gc => MeasureOp::Gc,
        MeasureSpec::Hydrate { attempts } => MeasureOp::Hydrate {
            attempts: *attempts,
        },
    }
}

fn consistency_name(consistency: &Consistency) -> &'static str {
    match consistency {
        Consistency::Strong => "strong",
        Consistency::Eventual => "eventual",
    }
}

fn default_nprobe(measure: &MeasureOp) -> usize {
    match measure {
        MeasureOp::Query { .. }
        | MeasureOp::HybridQuery { .. }
        | MeasureOp::AsOfQuery { .. }
        | MeasureOp::QueryPages { .. } => DEFAULT_QUERY_NPROBE,
        MeasureOp::FtsQuery { .. }
        | MeasureOp::Fetch { .. }
        | MeasureOp::Upsert { .. }
        | MeasureOp::Delete { .. }
        | MeasureOp::Compact { .. }
        | MeasureOp::Gc
        | MeasureOp::Hydrate { .. } => DEFAULT_WRITE_NPROBE,
    }
}
