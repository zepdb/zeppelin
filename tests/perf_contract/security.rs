//! Production security hot-path measurement for the frozen secured query.

use std::hint::black_box;
use std::time::Instant;

use axum::http::Method;
use serde::Serialize;
use sha2::{Digest, Sha256};
use zeppelin::config::{ApiKeyConfig, Config, SecurityMode};
use zeppelin::security::{
    classify_route, ApiKeyAdapter, CredentialAdapter, Decision, NamespaceId, RequestContext,
    Resource, RouteClass, SecurityKernel,
};

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
    pub added_get_ops: i64,
    pub added_put_ops: i64,
}

/// Measure the same production adapter, route map, and kernel used by HTTP.
#[must_use]
pub fn measure(
    config: &Config,
    admin_bearer: &str,
    namespace: &str,
    repeats: &[RepeatCounters],
    assertions: &SecurityAssertionSpec,
) -> SecurityMeasurement {
    assert_eq!(
        config.security.mode,
        SecurityMode::Enforced,
        "secured_query must boot the production server in enforced mode"
    );
    assert!(
        !repeats.is_empty(),
        "secured_query must produce object-store counters"
    );

    let security_config = config_for_bearer(config, admin_bearer);
    let kernel = SecurityKernel::from_config(&security_config.security)
        .expect("secured_query security kernel must compile");
    let adapter = ApiKeyAdapter::from_config(&security_config.security)
        .expect("secured_query credential adapter must compile");
    let headers = bearer_headers(admin_bearer);
    let now = chrono::Utc::now();
    let context = RequestContext::at("secured-query-perf", now);
    let resource = Resource::Namespace(
        NamespaceId::new(namespace.to_string())
            .expect("performance namespace must be a valid security resource"),
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
                measure_secured(&headers, now, &adapter, &kernel, &resource, &context),
            )
        } else {
            let secured = measure_secured(&headers, now, &adapter, &kernel, &resource, &context);
            let baseline = measure_baseline(&headers, &resource, &context);
            (baseline, secured)
        };
        baseline_samples.push(baseline);
        secured_samples.push(secured);
    }

    let baseline_loop_p50_ns = median(&mut baseline_samples);
    let authn_authz_p50_ns = median(&mut secured_samples);
    let authn_authz_p50_delta_ns = authn_authz_p50_ns.saturating_sub(baseline_loop_p50_ns);

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
        added_get_ops: signed_delta(measured.totals.get_ops, baseline_get_ops),
        added_put_ops: signed_delta(measured.totals.put_ops, baseline_put_ops),
    }
}

fn config_for_bearer(config: &Config, bearer: &str) -> Config {
    let (key_id, secret) = bearer
        .split_once('.')
        .expect("server admin bearer must contain one key/secret separator");
    assert!(
        !secret.contains('.'),
        "server admin bearer must contain one key/secret separator"
    );
    let mut configured = config.clone();
    configured.security.mode = SecurityMode::Enforced;
    configured.security.api_keys = vec![ApiKeyConfig {
        key_id: key_id.to_string(),
        name: "secured-query-admin".to_string(),
        sha256_hex: format!("{:x}", Sha256::digest(secret.as_bytes())),
        actions: vec!["*".to_string()],
        namespaces: vec!["*".to_string()],
        expires_at: None,
    }];
    configured
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

fn signed_delta(actual: u64, baseline: u64) -> i64 {
    let actual = i128::from(actual);
    let baseline = i128::from(baseline);
    i64::try_from(actual - baseline).expect("object-store operation delta exceeded i64")
}
