//! Production security hot-path measurement for the frozen secured query.

use std::hint::black_box;
use std::time::Instant;

use axum::http::Method;
use serde::Serialize;
use zeppelin::config::{Config, SecurityMode};
use zeppelin::security::{
    classify_route, Action, CredentialAdapter, Decision, DelegationNarrowing, GrantActions,
    GrantDefinition, GrantScope, NamespaceId, RequestContext, Resource, RouteClass, SecurityKernel,
    WriteConstraints,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::Clock;

use crate::common::server::bearer_headers;

use super::contract::{load_contract, SecurityAssertionSpec};
use super::scenario::RepeatCounters;

const SAMPLES: usize = 101;
const OPERATIONS_PER_SAMPLE: usize = 1_024;
const WARMUP_OPERATIONS: usize = 4_096;
const QUERY_ROUTE: &str = "/v1/namespaces/:ns/query";

/// Measured security CPU cost and object-store delta for one scenario run.
#[derive(Debug, Clone, Serialize)]
pub struct SecurityMeasurement {
    pub security_mode: &'static str,
    pub credential_kind: &'static str,
    pub baseline_scenario: String,
    pub samples: usize,
    pub operations_per_sample: usize,
    pub baseline_loop_p50_ns: u64,
    pub authn_authz_p50_ns: u64,
    pub authn_authz_p50_delta_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegated_authn_authz_p50_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegated_authn_authz_p50_delta_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paired_query_samples: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_filtered_query_p50_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_filtered_query_p50_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_p50_regression_basis_points: Option<u64>,
    pub added_get_ops: i64,
    pub added_put_ops: i64,
}

/// Direct latency comparison for equivalent caller-owned and policy-owned filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PairedQueryLatency {
    pub samples: usize,
    pub caller_filtered_p50_ns: u64,
    pub policy_filtered_p50_ns: u64,
    pub regression_basis_points: u64,
}

/// Inputs for one security-contract CPU and storage-parity measurement.
pub struct SecurityMeasureInput<'a> {
    pub config: &'a Config,
    pub measured_bearer: &'a str,
    pub namespace: &'a str,
    pub repeats: &'a [RepeatCounters],
    pub assertions: &'a SecurityAssertionSpec,
    pub security_store: &'a ZeppelinStore,
    pub clock: Clock,
    pub paired_query: Option<PairedQueryLatency>,
}

impl PairedQueryLatency {
    /// Build the comparison from two directly observed HTTP latency distributions.
    #[must_use]
    pub fn from_observed_ns(mut caller_filtered: Vec<u64>, mut policy_filtered: Vec<u64>) -> Self {
        assert!(
            !caller_filtered.is_empty(),
            "paired query latency requires observed samples"
        );
        assert_eq!(
            caller_filtered.len(),
            policy_filtered.len(),
            "paired query latency distributions must have equal sample counts"
        );
        caller_filtered.sort_unstable();
        policy_filtered.sort_unstable();
        let samples = caller_filtered.len();
        let caller_filtered_p50_ns = caller_filtered[samples / 2];
        let policy_filtered_p50_ns = policy_filtered[samples / 2];
        assert!(
            caller_filtered_p50_ns > 0,
            "caller-filtered query p50 must be nonzero"
        );
        let regression_ns = policy_filtered_p50_ns.saturating_sub(caller_filtered_p50_ns);
        let regression_basis_points = match u64::try_from(
            u128::from(regression_ns)
                .saturating_mul(10_000)
                .div_ceil(u128::from(caller_filtered_p50_ns)),
        ) {
            Ok(value) => value,
            Err(error) => panic!("paired query regression exceeded u64 basis points: {error}"),
        };
        Self {
            samples,
            caller_filtered_p50_ns,
            policy_filtered_p50_ns,
            regression_basis_points,
        }
    }
}

/// Measure the same production adapter, route map, and kernel used by HTTP.
#[must_use]
pub async fn measure(input: SecurityMeasureInput<'_>) -> SecurityMeasurement {
    let SecurityMeasureInput {
        config,
        measured_bearer,
        namespace,
        repeats,
        assertions,
        security_store,
        clock,
        paired_query,
    } = input;
    assert_eq!(
        config.security.mode,
        SecurityMode::Enforced,
        "secured_query must boot the production server in enforced mode"
    );
    assert!(
        !repeats.is_empty(),
        "secured_query must produce object-store counters"
    );

    let delegation_key = tempfile::NamedTempFile::new()
        .unwrap_or_else(|error| panic!("delegated performance signing key failed: {error}"));
    std::fs::write(delegation_key.path(), "71".repeat(32))
        .unwrap_or_else(|error| panic!("delegated performance signing key write failed: {error}"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            delegation_key.path(),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap_or_else(|error| {
            panic!("delegated performance signing key permissions failed: {error}")
        });
    }
    let mut security_config = config.security.clone();
    security_config.token_signing_key_path = delegation_key.path().to_string_lossy().into_owned();
    // A licensed kernel spawns a policy refresh loop, and this measurement runs
    // hundreds of thousands of authenticate calls across many seconds of wall
    // clock. At the 5s default a refresh lands mid-sample and the credential
    // under measurement stops resolving, so the run dies with
    // `CredentialUnknown` partway through. Background I/O has no place in a CPU
    // benchmark regardless; the branching census pins it the same way
    // (`branching.rs:204`).
    security_config.policy_refresh_secs = 3_600;
    security_config.rbac = true;
    let (kernel, adapter) =
        SecurityKernel::compose(security_store.clone(), &security_config, clock.clone())
            .await
            .unwrap_or_else(|error| {
                panic!("secured performance security authority must load: {error}")
            });
    let headers = bearer_headers(measured_bearer);
    let now = clock.now();
    let context = RequestContext::at("secured-query-perf", now);
    let resource = Resource::Namespace(NamespaceId::new(namespace.to_string()).unwrap_or_else(
        |error| panic!("performance namespace must be a valid security resource: {error}"),
    ));

    let preflight_principal = adapter.authenticate(&headers, now).unwrap_or_else(|error| {
        panic!("measured performance principal must authenticate: {error}")
    });
    let RouteClass::Protected(query_action) = classify_route(&Method::POST, QUERY_ROUTE, false)
        .unwrap_or_else(|| panic!("query route must stay mapped"))
    else {
        panic!("query route must stay protected");
    };
    let preflight = kernel.authorize(&preflight_principal, query_action, &resource, &context);
    let Decision::Allow(preflight) = preflight else {
        panic!("measured performance principal must be authorized for query");
    };
    let expected_filter =
        assertions
            .mandatory_filter
            .as_ref()
            .map_or(serde_json::Value::Null, |filter| {
                serde_json::json!({
                    "op": "eq",
                    "field": filter.field,
                    "value": filter.value,
                })
            });
    let actual_filter = serde_json::to_value(&preflight.mandatory_filter)
        .unwrap_or_else(|error| panic!("compiled mandatory filter must serialize: {error}"));
    assert_eq!(
        actual_filter, expected_filter,
        "measured performance principal must carry the contract's mandatory filter"
    );

    for _ in 0..WARMUP_OPERATIONS {
        let principal = adapter
            .authenticate(black_box(&headers), now)
            .expect("performance administrator must authenticate");
        let RouteClass::Protected(action) =
            classify_route(black_box(&Method::POST), black_box(QUERY_ROUTE), false)
                .expect("query route must stay centrally mapped")
        else {
            panic!("query route must stay protected");
        };
        let decision = kernel.authorize(&principal, action, &resource, &context);
        assert!(matches!(decision, Decision::Allow(_)));
        black_box(decision);
    }

    let mut baseline_samples = Vec::with_capacity(SAMPLES);
    let mut secured_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let (baseline, secured) = if sample % 2 == 0 {
            (
                measure_baseline(&headers, &resource, &context),
                measure_secured(
                    &headers,
                    now,
                    adapter.as_ref(),
                    kernel.as_ref(),
                    &resource,
                    &context,
                ),
            )
        } else {
            let secured = measure_secured(
                &headers,
                now,
                adapter.as_ref(),
                kernel.as_ref(),
                &resource,
                &context,
            );
            let baseline = measure_baseline(&headers, &resource, &context);
            (baseline, secured)
        };
        baseline_samples.push(baseline);
        secured_samples.push(secured);
    }

    let baseline_loop_p50_ns = median(&mut baseline_samples);
    let authn_authz_p50_ns = median(&mut secured_samples);
    let authn_authz_p50_delta_ns = authn_authz_p50_ns.saturating_sub(baseline_loop_p50_ns);

    let (delegated_authn_authz_p50_ns, delegated_authn_authz_p50_delta_ns) =
        if assertions.delegated_authn_authz_p50_delta_ns_max.is_some() {
            kernel
                .add_grant(
                    &preflight_principal,
                    GrantDefinition::new(
                        preflight_principal.id.clone(),
                        GrantScope::Global,
                        GrantActions::Selected {
                            actions: vec![Action::CredentialDelegate],
                        },
                        None,
                        None,
                        WriteConstraints::none(),
                        Vec::new(),
                    ),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("delegated performance grant publication failed: {error}")
                });
            let narrowed = DelegationNarrowing::new(
                vec![query_action],
                vec![
                    NamespaceId::new(namespace.to_string()).unwrap_or_else(|error| {
                        panic!("delegated performance namespace must validate: {error}")
                    }),
                ],
                None,
                "Phase 7 token verification performance gate".to_string(),
            )
            .unwrap_or_else(|error| panic!("delegated performance narrowing failed: {error}"));
            let (token, _) = kernel
                .mint_delegated_token(&preflight_principal, narrowed, 300, now)
                .unwrap_or_else(|error| panic!("delegated performance mint failed: {error}"));
            let token_headers = bearer_headers(token.token());
            for _ in 0..WARMUP_OPERATIONS {
                let principal = adapter
                    .authenticate(black_box(&token_headers), now)
                    .expect("delegated performance token must authenticate");
                let decision = kernel.authorize(&principal, query_action, &resource, &context);
                assert!(matches!(decision, Decision::Allow(_)));
                black_box(decision);
            }
            let mut delegated_baseline = Vec::with_capacity(SAMPLES);
            let mut delegated_secured = Vec::with_capacity(SAMPLES);
            for sample in 0..SAMPLES {
                let (baseline, secured) = if sample % 2 == 0 {
                    (
                        measure_baseline(&token_headers, &resource, &context),
                        measure_secured(
                            &token_headers,
                            now,
                            adapter.as_ref(),
                            kernel.as_ref(),
                            &resource,
                            &context,
                        ),
                    )
                } else {
                    let secured = measure_secured(
                        &token_headers,
                        now,
                        adapter.as_ref(),
                        kernel.as_ref(),
                        &resource,
                        &context,
                    );
                    let baseline = measure_baseline(&token_headers, &resource, &context);
                    (baseline, secured)
                };
                delegated_baseline.push(baseline);
                delegated_secured.push(secured);
            }
            let baseline = median(&mut delegated_baseline);
            let secured = median(&mut delegated_secured);
            (Some(secured), Some(secured.saturating_sub(baseline)))
        } else {
            (None, None)
        };

    assert_eq!(
        paired_query.is_some(),
        assertions.mandatory_filter.is_some(),
        "mandatory-filter contracts require a direct paired query measurement"
    );

    let baseline = load_contract(&assertions.baseline_scenario).unwrap_or_else(|error| {
        panic!(
            "failed to load secured_query object-store baseline {:?}: {error}",
            assertions.baseline_scenario
        )
    });
    let baseline_get_ops = baseline.assertions.gets["total"];
    let baseline_put_ops = baseline.assertions.puts["total"];
    let measured = &repeats[0];

    SecurityMeasurement {
        security_mode: "enforced",
        credential_kind: "api_key",
        baseline_scenario: assertions.baseline_scenario.clone(),
        samples: SAMPLES,
        operations_per_sample: OPERATIONS_PER_SAMPLE,
        baseline_loop_p50_ns,
        authn_authz_p50_ns,
        authn_authz_p50_delta_ns,
        delegated_authn_authz_p50_ns,
        delegated_authn_authz_p50_delta_ns,
        paired_query_samples: paired_query.map(|measurement| measurement.samples),
        caller_filtered_query_p50_ns: paired_query
            .map(|measurement| measurement.caller_filtered_p50_ns),
        policy_filtered_query_p50_ns: paired_query
            .map(|measurement| measurement.policy_filtered_p50_ns),
        query_p50_regression_basis_points: paired_query
            .map(|measurement| measurement.regression_basis_points),
        added_get_ops: added_delta(measured.totals.get_ops, baseline_get_ops),
        added_put_ops: added_delta(measured.totals.put_ops, baseline_put_ops),
    }
}

fn measure_baseline(
    headers: &axum::http::HeaderMap,
    resource: &Resource,
    context: &RequestContext,
) -> u64 {
    let started = Instant::now();
    for _ in 0..OPERATIONS_PER_SAMPLE {
        black_box((headers, resource, context, &Method::POST, QUERY_ROUTE));
    }
    per_operation_ns(started)
}

fn measure_secured(
    headers: &axum::http::HeaderMap,
    now: chrono::DateTime<chrono::Utc>,
    adapter: &dyn CredentialAdapter,
    kernel: &zeppelin::security::SecurityKernel,
    resource: &Resource,
    context: &RequestContext,
) -> u64 {
    let started = Instant::now();
    for _ in 0..OPERATIONS_PER_SAMPLE {
        let principal = adapter
            .authenticate(black_box(headers), now)
            .expect("performance administrator must authenticate");
        let RouteClass::Protected(action) =
            classify_route(black_box(&Method::POST), black_box(QUERY_ROUTE), false)
                .expect("query route must stay centrally mapped")
        else {
            panic!("query route must stay protected");
        };
        black_box(kernel.authorize(&principal, action, resource, context));
    }
    per_operation_ns(started)
}

fn per_operation_ns(started: Instant) -> u64 {
    let elapsed = started.elapsed().as_nanos();
    let per_operation = elapsed / OPERATIONS_PER_SAMPLE as u128;
    u64::try_from(per_operation).expect("security measurement exceeded u64 nanoseconds")
}

fn median(samples: &mut [u64]) -> u64 {
    assert_eq!(samples.len(), SAMPLES, "security sample count drifted");
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn added_delta(actual: u64, baseline: u64) -> i64 {
    i64::try_from(actual.saturating_sub(baseline))
        .expect("added object-store operation count exceeded i64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_query_latency_uses_both_observed_distributions() {
        let measurement =
            PairedQueryLatency::from_observed_ns(vec![1_000, 900, 1_100], vec![1_060, 950, 1_200]);

        assert_eq!(measurement.caller_filtered_p50_ns, 1_000);
        assert_eq!(measurement.policy_filtered_p50_ns, 1_060);
        assert_eq!(measurement.regression_basis_points, 600);
    }

    #[test]
    fn added_delta_does_not_treat_a_cost_reduction_as_security_overhead() {
        assert_eq!(added_delta(9, 14), 0);
        assert_eq!(added_delta(14, 14), 0);
        assert_eq!(added_delta(17, 14), 3);
    }
}
