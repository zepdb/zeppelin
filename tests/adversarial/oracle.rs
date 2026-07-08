use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeppelin::types::{AttributeValue, ConsistencyLevel};

use super::model::{model_distance, Model, ModelRecord, OracleMutation};
use super::ops::{GeneratedQuery, Op, OpRecord, QueryOracleClass};
use super::RunMode;

pub const SCORE_ABS_EPS: f32 = 1e-5;
pub const SCORE_REL_EPS: f32 = 1e-4;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ViolationId {
    I1StrongExact,
    I2DeletedNeverReturned,
    I4FetchExact,
    I11ErrorEnvelope,
    I12StructuralSanity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Violation {
    pub id: ViolationId,
    pub op_index: u64,
    pub namespace: String,
    pub detail: String,
    pub evidence: serde_json::Value,
}

pub trait Invariant {
    fn id(&self) -> ViolationId;
    fn check_op(&self, model: &Model, rec: &OpRecord, mode: RunMode) -> Vec<Violation>;
}

#[must_use]
pub fn check_op(
    model: &Model,
    rec: &OpRecord,
    mode: RunMode,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    violations.extend(check_i11_error_envelope(rec));
    violations.extend(check_expected_error(rec));
    if (200..300).contains(&rec.status) {
        violations.extend(check_i2_deleted_never_returned(model, rec));
        violations.extend(check_i4_fetch_exact(model, rec, mode));
        violations.extend(check_i12_structural_sanity(rec));
        violations.extend(check_i1_strong_exact(model, rec, mode, mutation));
    }
    violations
}

/// I1 — Strong exact top-k
///
/// Deterministic; `ExactAnn`; strong. Let `V = {id in live : filter
/// matches}`. Phase 1 has no filters, so `V = live`. The model computes scalar
/// distance and uses the tie-group epsilon rule: `results.len() == k`; every
/// returned id is in `V`; every returned id has `d(id) <= kth + eps`; every id
/// with `d(id) < kth - eps` is returned; returned scores match scalar distance
/// within eps; scores are ascending; requested attributes match exactly.
fn check_i1_strong_exact(
    model: &Model,
    rec: &OpRecord,
    mode: RunMode,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    if mode != RunMode::Deterministic {
        return Vec::new();
    }
    let Op::Query { ns, q, .. } = &rec.op else {
        return Vec::new();
    };
    let QueryOracleClass::ExactAnn {
        top_k,
        consistency,
        filter,
    } = &q.class
    else {
        return Vec::new();
    };
    if *consistency != ConsistencyLevel::Strong {
        return Vec::new();
    }
    assert!(filter.is_none(), "filters land in Phase 2");
    let Some(ns_model) = model.namespaces.get(ns) else {
        return vec![violation(
            ViolationId::I1StrongExact,
            rec,
            ns,
            "query response for unknown namespace",
            serde_json::json!({ "namespace": ns }),
        )];
    };
    let query = query_vector(q);
    let Ok(results) = query_results(&rec.response) else {
        return vec![violation(
            ViolationId::I1StrongExact,
            rec,
            ns,
            "query response did not contain parseable results",
            rec.response.clone(),
        )];
    };

    let mut expected: Vec<(String, f32, &ModelRecord)> = ns_model
        .live
        .iter()
        .map(|(id, record)| {
            (
                id.clone(),
                oracle_distance(
                    mutation,
                    id,
                    ns_model.live.keys().next(),
                    ns_model.spec.metric,
                    &query,
                    &record.values,
                ),
                record,
            )
        })
        .collect();
    expected.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    let k = (*top_k).min(expected.len());
    let mut violations = Vec::new();
    if results.len() != k {
        violations.push(violation(
            ViolationId::I1StrongExact,
            rec,
            ns,
            "result length did not match exact top-k",
            serde_json::json!({ "expected_len": k, "actual_len": results.len() }),
        ));
    }
    if k == 0 {
        return violations;
    }

    let kth = expected[k - 1].1;
    let eps = score_eps(kth);
    let returned_ids: BTreeSet<&str> = results.iter().map(|result| result.id.as_str()).collect();
    if returned_ids.len() != results.len() {
        violations.push(violation(
            ViolationId::I1StrongExact,
            rec,
            ns,
            "query returned duplicate ids",
            serde_json::json!({ "results": results }),
        ));
    }

    for result in &results {
        let Some((_, expected_score, expected_record)) =
            expected.iter().find(|(id, _, _)| id == &result.id)
        else {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query returned id outside model live set",
                serde_json::json!({ "id": result.id }),
            ));
            continue;
        };
        if *expected_score > kth + eps {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query returned id outside kth tie group",
                serde_json::json!({
                    "id": result.id,
                    "score": expected_score,
                    "kth": kth,
                    "eps": eps
                }),
            ));
        }
        if !score_close(result.score, *expected_score) {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query score did not match model distance",
                serde_json::json!({
                    "id": result.id,
                    "actual": result.score,
                    "expected": expected_score
                }),
            ));
        }
        if include_attributes(q) && result.attributes != expected_record.attributes {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query attributes did not match model",
                serde_json::json!({
                    "id": result.id,
                    "actual": result.attributes,
                    "expected": expected_record.attributes
                }),
            ));
        }
    }

    for (id, score, _) in &expected {
        if *score >= kth - eps {
            continue;
        }
        if !returned_ids.contains(id.as_str()) {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query omitted id below kth tie boundary",
                serde_json::json!({ "id": id, "score": score, "kth": kth, "eps": eps }),
            ));
        }
    }

    for pair in results.windows(2) {
        if pair[0].score > pair[1].score + score_eps(pair[1].score) {
            violations.push(violation(
                ViolationId::I1StrongExact,
                rec,
                ns,
                "query scores were not ascending",
                serde_json::json!({ "left": pair[0], "right": pair[1] }),
            ));
        }
    }

    violations
}

/// I2 — Deleted ids never returned
///
/// All modes/classes/consistencies. An id whose delete was acked and never
/// re-upserted since must not appear in any query or fetch result issued after
/// the delete is visible. Deterministic mode has zero manifest-cache TTL, so
/// the check applies immediately.
fn check_i2_deleted_never_returned(model: &Model, rec: &OpRecord) -> Vec<Violation> {
    let (ns, ids) = match &rec.op {
        Op::Query { ns, .. } => {
            let Ok(results) = query_results(&rec.response) else {
                return Vec::new();
            };
            (
                ns.as_str(),
                results
                    .into_iter()
                    .map(|result| result.id)
                    .collect::<Vec<_>>(),
            )
        }
        Op::FetchVectors { ns, .. } => {
            let Ok(response) = fetch_response(&rec.response) else {
                return Vec::new();
            };
            (
                ns.as_str(),
                response
                    .results
                    .into_iter()
                    .map(|result| result.id)
                    .collect::<Vec<_>>(),
            )
        }
        _ => return Vec::new(),
    };
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    ids.into_iter()
        .filter(|id| ns_model.deleted_ever.contains(id))
        .map(|id| {
            violation(
                ViolationId::I2DeletedNeverReturned,
                rec,
                ns,
                "deleted id was returned",
                serde_json::json!({ "id": id }),
            )
        })
        .collect()
}

/// I4 — Fetch exact
///
/// Strong fetch returns `results = requested intersect live` in request-relative
/// order, values byte-equal, attributes equal; `missing` is the rest.
fn check_i4_fetch_exact(model: &Model, rec: &OpRecord, _mode: RunMode) -> Vec<Violation> {
    let Op::FetchVectors {
        ns,
        ids,
        consistency,
    } = &rec.op
    else {
        return Vec::new();
    };
    if *consistency != ConsistencyLevel::Strong {
        return Vec::new();
    }
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let Ok(actual) = fetch_response(&rec.response) else {
        return vec![violation(
            ViolationId::I4FetchExact,
            rec,
            ns,
            "fetch response did not contain parseable results/missing",
            rec.response.clone(),
        )];
    };

    let mut expected_results = Vec::new();
    let mut expected_missing = Vec::new();
    for id in ids {
        if let Some(record) = ns_model.live.get(id) {
            expected_results.push((id.clone(), record));
        } else {
            expected_missing.push(id.clone());
        }
    }

    let mut violations = Vec::new();
    if actual.results.len() != expected_results.len() {
        violations.push(violation(
            ViolationId::I4FetchExact,
            rec,
            ns,
            "fetch result length mismatch",
            serde_json::json!({
                "expected_len": expected_results.len(),
                "actual_len": actual.results.len()
            }),
        ));
    }
    for (idx, (expected_id, expected_record)) in expected_results.iter().enumerate() {
        let Some(actual_record) = actual.results.get(idx) else {
            continue;
        };
        if &actual_record.id != expected_id {
            violations.push(violation(
                ViolationId::I4FetchExact,
                rec,
                ns,
                "fetch result order/id mismatch",
                serde_json::json!({
                    "index": idx,
                    "expected": expected_id,
                    "actual": actual_record.id
                }),
            ));
            continue;
        }
        if actual_record.values.as_ref() != Some(&expected_record.values) {
            violations.push(violation(
                ViolationId::I4FetchExact,
                rec,
                ns,
                "fetch values mismatch",
                serde_json::json!({
                    "id": expected_id,
                    "expected": expected_record.values,
                    "actual": actual_record.values
                }),
            ));
        }
        if actual_record.attributes != expected_record.attributes {
            violations.push(violation(
                ViolationId::I4FetchExact,
                rec,
                ns,
                "fetch attributes mismatch",
                serde_json::json!({
                    "id": expected_id,
                    "expected": expected_record.attributes,
                    "actual": actual_record.attributes
                }),
            ));
        }
    }
    if actual.missing != expected_missing {
        violations.push(violation(
            ViolationId::I4FetchExact,
            rec,
            ns,
            "fetch missing set/order mismatch",
            serde_json::json!({
                "expected": expected_missing,
                "actual": actual.missing
            }),
        ));
    }

    violations
}

/// I11 — Stable error envelope
///
/// Every non-2xx response body must parse as `{code in known table, error,
/// status == HTTP status, retryable, request_id}`. Exact-error query classes
/// additionally assert the expected status and code.
fn check_i11_error_envelope(rec: &OpRecord) -> Vec<Violation> {
    if (200..300).contains(&rec.status) {
        return Vec::new();
    }
    let Some(object) = rec.response.as_object() else {
        return vec![violation(
            ViolationId::I11ErrorEnvelope,
            rec,
            rec.op.namespace(),
            "error response was not a JSON object",
            rec.response.clone(),
        )];
    };
    let code = object.get("code").and_then(serde_json::Value::as_str);
    let status = object.get("status").and_then(serde_json::Value::as_u64);
    let retryable = object.get("retryable").and_then(serde_json::Value::as_bool);
    let error = object.get("error").and_then(serde_json::Value::as_str);
    let request_id = object.get("request_id").and_then(serde_json::Value::as_str);
    if code.is_none()
        || !KNOWN_ERROR_CODES.contains(&code.unwrap())
        || status != Some(u64::from(rec.status))
        || retryable.is_none()
        || error.is_none_or(str::is_empty)
        || request_id.is_none_or(str::is_empty)
    {
        return vec![violation(
            ViolationId::I11ErrorEnvelope,
            rec,
            rec.op.namespace(),
            "error response did not match the canonical envelope",
            serde_json::json!({
                "status": rec.status,
                "body": rec.response,
            }),
        )];
    }
    Vec::new()
}

fn check_expected_error(rec: &OpRecord) -> Vec<Violation> {
    let Op::Query { ns, q, .. } = &rec.op else {
        return Vec::new();
    };
    let QueryOracleClass::ExpectError { status, code } = &q.class else {
        return Vec::new();
    };
    let actual_code = rec.response.get("code").and_then(serde_json::Value::as_str);
    if rec.status == *status && actual_code == Some(code.as_str()) {
        return Vec::new();
    }
    vec![violation(
        ViolationId::I11ErrorEnvelope,
        rec,
        ns,
        "ExpectError query returned unexpected status/code",
        serde_json::json!({
            "expected_status": status,
            "expected_code": code,
            "actual_status": rec.status,
            "actual_code": actual_code,
        }),
    )]
}

/// I12 — Structural sanity
///
/// Every successful query response has no NaN/infinite scores, no duplicate
/// ids, and `results.len() <= top_k`.
fn check_i12_structural_sanity(rec: &OpRecord) -> Vec<Violation> {
    let Op::Query { ns, q, .. } = &rec.op else {
        return Vec::new();
    };
    let Ok(results) = query_results(&rec.response) else {
        return Vec::new();
    };
    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    for result in &results {
        if !result.score.is_finite() {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "query returned non-finite score",
                serde_json::json!({ "id": result.id, "score": result.score }),
            ));
        }
        if !seen.insert(result.id.clone()) {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "query returned duplicate id",
                serde_json::json!({ "id": result.id }),
            ));
        }
    }
    if let Some(top_k) = q.top_k() {
        if results.len() > top_k {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "query returned more results than top_k",
                serde_json::json!({ "top_k": top_k, "actual_len": results.len() }),
            ));
        }
    }
    violations
}

#[must_use]
pub fn score_close(actual: f32, expected: f32) -> bool {
    (actual - expected).abs() <= score_eps(expected)
}

fn score_eps(score: f32) -> f32 {
    SCORE_ABS_EPS + SCORE_REL_EPS * score.abs()
}

fn oracle_distance(
    mutation: Option<OracleMutation>,
    id: &str,
    first_id: Option<&String>,
    metric: zeppelin::types::DistanceMetric,
    query: &[f32],
    values: &[f32],
) -> f32 {
    let base = model_distance(metric, query, values);
    if mutation == Some(OracleMutation::SkewScore) && first_id.is_some_and(|first| first == id) {
        base + (SCORE_ABS_EPS * 10.0)
    } else {
        base
    }
}

fn query_vector(q: &GeneratedQuery) -> Vec<f32> {
    if let Some(vector) = q.body.get("vector").and_then(serde_json::Value::as_array) {
        return vector
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .unwrap_or_else(|| panic!("query vector contained non-number: {}", q.body))
                    as f32
            })
            .collect();
    }
    q.body
        .get("sources")
        .and_then(serde_json::Value::as_array)
        .and_then(|sources| sources.first())
        .and_then(|source| source.get("vector"))
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("ExactAnn query body missing vector: {}", q.body))
        .iter()
        .map(|value| {
            value
                .as_f64()
                .unwrap_or_else(|| panic!("query vector contained non-number: {}", q.body))
                as f32
        })
        .collect()
}

fn include_attributes(q: &GeneratedQuery) -> bool {
    q.body
        .get("include_attributes")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

fn query_results(response: &serde_json::Value) -> Result<Vec<WireSearchResult>, serde_json::Error> {
    serde_json::from_value(response["results"].clone())
}

fn fetch_response(response: &serde_json::Value) -> Result<WireFetchResponse, serde_json::Error> {
    serde_json::from_value(response.clone())
}

fn violation(
    id: ViolationId,
    rec: &OpRecord,
    namespace: &str,
    detail: &str,
    evidence: serde_json::Value,
) -> Violation {
    Violation {
        id,
        op_index: rec.index,
        namespace: namespace.to_string(),
        detail: detail.to_string(),
        evidence,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireSearchResult {
    id: String,
    score: f32,
    #[serde(default)]
    attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireFetchResponse {
    results: Vec<WireFetchRecord>,
    missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireFetchRecord {
    id: String,
    #[serde(default)]
    values: Option<Vec<f32>>,
    #[serde(default)]
    attributes: Option<HashMap<String, AttributeValue>>,
}

const KNOWN_ERROR_CODES: &[&str] = &[
    "INTERNAL_DATA_MISSING",
    "STORAGE_ERROR",
    "INTERNAL_ERROR",
    "DATA_CORRUPTION",
    "NAMESPACE_NOT_FOUND",
    "CONFLICT_RETRY",
    "NAMESPACE_ALREADY_EXISTS",
    "SNAPSHOT_ALREADY_EXISTS",
    "SNAPSHOT_NOT_FOUND",
    "POINT_IN_TIME_NOT_RETAINED",
    "NAMESPACE_DELETING",
    "DIMENSION_MISMATCH",
    "VECTOR_NOT_FOUND",
    "VALIDATION_ERROR",
    "PAYLOAD_TOO_LARGE",
    "NOT_IMPLEMENTED",
    "HYDRATION_DISABLED",
    "FTS_FIELD_NOT_CONFIGURED",
    "INDEX_UNAVAILABLE",
    "CONCURRENCY_LIMIT",
    "RATE_LIMITED",
];
