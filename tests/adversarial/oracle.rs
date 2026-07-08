use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeppelin::index::filter::evaluate_filter;
use zeppelin::types::{AttributeValue, ConsistencyLevel, Filter};

use super::model::{model_distance, Model, ModelRecord, NsModel, OracleMutation};
use super::ops::{AsOfTarget, GeneratedQuery, Op, OpRecord, QueryOracleClass};
use super::RunMode;

pub const SCORE_ABS_EPS: f32 = 1e-5;
pub const SCORE_REL_EPS: f32 = 1e-4;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ViolationId {
    I1StrongExact,
    I2DeletedNeverReturned,
    I3EventualExact,
    I4FetchExact,
    I5BatchEquivalent,
    I6PaginationEquivalent,
    I7FtsMembership,
    I8AsOfExact,
    I9Clone,
    I10FailedValidationNoWal,
    I11ErrorEnvelope,
    I12StructuralSanity,
    I13ProbeSandwich,
    I14S3Reachability,
    I15ManifestLineage,
    I16Quiescence,
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
    if mode == RunMode::Deterministic {
        violations.extend(check_i10_failed_validation_no_wal(rec));
    }
    if (200..300).contains(&rec.status) {
        violations.extend(check_i2_deleted_never_returned(model, rec));
        violations.extend(check_i4_fetch_exact(model, rec, mode));
        violations.extend(check_i12_structural_sanity(model, rec));
        if mode == RunMode::Deterministic {
            violations.extend(check_i1_strong_exact(model, rec, mode, mutation));
            violations.extend(check_i8_as_of_exact(model, rec, mode, mutation));
            violations.extend(check_i3_membership(model, rec));
            violations.extend(check_i5_batch_equivalence(rec));
            violations.extend(check_i6_pagination_equivalence(rec));
        }
        violations.extend(check_i7_fts_membership(model, rec));
    }
    violations
}

/// I1/I3 — Exact top-k
///
/// Deterministic; `ExactAnn`. Strong uses `live`; eventual uses
/// `compacted_live \ wal_tombstones`. Filters are evaluated through the
/// production filter evaluator, with `attributes: None` treated as no match.
fn check_i1_strong_exact(
    model: &Model,
    rec: &OpRecord,
    mode: RunMode,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    if mode != RunMode::Deterministic {
        return Vec::new();
    }
    let Op::Query { ns, q, as_of } = &rec.op else {
        return Vec::new();
    };
    if as_of.is_some() {
        return Vec::new();
    }
    let QueryOracleClass::ExactAnn {
        top_k,
        consistency,
        filter,
    } = &q.class
    else {
        return Vec::new();
    };
    let violation_id = exact_violation_id(*consistency);
    let Some(ns_model) = model.namespaces.get(ns) else {
        return vec![violation(
            violation_id,
            rec,
            ns,
            "query response for unknown namespace",
            serde_json::json!({ "namespace": ns }),
        )];
    };
    let query = query_vector(q);
    let Ok(results) = query_results(&rec.response) else {
        return vec![violation(
            violation_id,
            rec,
            ns,
            "query response did not contain parseable results",
            rec.response.clone(),
        )];
    };

    let first_visible = first_visible_id(ns_model, *consistency);
    let mut expected: Vec<(String, f32, &ModelRecord)> = ns_model
        .visible_records(*consistency)
        .into_iter()
        .filter(|(id, record)| {
            record_matches_filter(mutation, id, first_visible, record, filter.as_ref())
        })
        .map(|(id, record)| {
            (
                id.clone(),
                oracle_distance(
                    mutation,
                    id,
                    first_visible,
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
            violation_id,
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
            violation_id,
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
                violation_id,
                rec,
                ns,
                "query returned id outside model live set",
                serde_json::json!({ "id": result.id }),
            ));
            continue;
        };
        if *expected_score > kth + eps {
            violations.push(violation(
                violation_id,
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
                violation_id,
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
        if include_attributes(q)
            && !attributes_equal(&result.attributes, &expected_record.attributes)
        {
            violations.push(violation(
                violation_id,
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
                violation_id,
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
                violation_id,
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
        Op::Query {
            ns, as_of: None, ..
        } => (ns.as_str(), response_ids(&rec.response)),
        Op::Query { as_of: Some(_), .. } => return Vec::new(),
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
        Op::BatchQuery { ns, .. } => (
            ns.as_str(),
            rec.response["batch"]["results"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|entry| entry.get("ok").and_then(serde_json::Value::as_bool) == Some(true))
                .flat_map(|entry| response_ids(&entry["response"]))
                .collect::<Vec<_>>(),
        ),
        Op::PaginateAll { ns, .. } => (
            ns.as_str(),
            rec.response["pages"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|page| response_ids(&page["body"]))
                .collect::<Vec<_>>(),
        ),
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

/// I4/I3 — Fetch exact
///
/// Strong fetch uses `live`; eventual fetch uses
/// `compacted_live \ wal_tombstones`.
fn check_i4_fetch_exact(model: &Model, rec: &OpRecord, _mode: RunMode) -> Vec<Violation> {
    let Op::FetchVectors {
        ns,
        ids,
        consistency,
    } = &rec.op
    else {
        return Vec::new();
    };
    let violation_id = fetch_violation_id(*consistency);
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let Ok(actual) = fetch_response(&rec.response) else {
        return vec![violation(
            violation_id,
            rec,
            ns,
            "fetch response did not contain parseable results/missing",
            rec.response.clone(),
        )];
    };

    let mut expected_results = Vec::new();
    let mut expected_missing = Vec::new();
    for id in ids {
        if let Some(record) = ns_model.visible_get(id, *consistency) {
            expected_results.push((id.clone(), record));
        } else {
            expected_missing.push(id.clone());
        }
    }

    let mut violations = Vec::new();
    if actual.results.len() != expected_results.len() {
        violations.push(violation(
            violation_id,
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
                violation_id,
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
                violation_id,
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
        if !attributes_equal(&actual_record.attributes, &expected_record.attributes) {
            violations.push(violation(
                violation_id,
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
            violation_id,
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

/// I8 — Point-in-time query exactness
///
/// Successful `as_of` generation and snapshot reads are checked against the
/// model checkpoint pinned by that target. Non-retained targets are covered by
/// the expected-error query class and the canonical error envelope check.
fn check_i8_as_of_exact(
    model: &Model,
    rec: &OpRecord,
    mode: RunMode,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    if mode != RunMode::Deterministic {
        return Vec::new();
    }
    let Op::Query {
        ns,
        q,
        as_of: Some(as_of),
    } = &rec.op
    else {
        return Vec::new();
    };
    let Some(ns_model) = model.namespaces.get(ns) else {
        return vec![violation(
            ViolationId::I8AsOfExact,
            rec,
            ns,
            "as_of query response for unknown namespace",
            serde_json::json!({ "namespace": ns }),
        )];
    };
    let Some((generation, checkpoint)) = checkpoint_for_as_of(ns_model, as_of) else {
        return Vec::new();
    };

    match &q.class {
        QueryOracleClass::ExactAnn {
            top_k,
            consistency: ConsistencyLevel::Strong,
            filter,
        } => check_as_of_exact_ann(
            rec, ns, q, ns_model, generation, checkpoint, *top_k, filter, mutation,
        ),
        QueryOracleClass::Membership {
            consistency: ConsistencyLevel::Strong,
        } => {
            let visible = checkpoint.keys().cloned().collect::<BTreeSet<_>>();
            response_ids(&rec.response)
                .into_iter()
                .filter(|id| !visible.contains(id))
                .map(|id| {
                    violation(
                        ViolationId::I8AsOfExact,
                        rec,
                        ns,
                        "as_of membership query returned id outside checkpoint",
                        serde_json::json!({
                            "id": id,
                            "generation": generation,
                        }),
                    )
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn check_as_of_exact_ann(
    rec: &OpRecord,
    ns: &str,
    q: &GeneratedQuery,
    ns_model: &NsModel,
    generation: u64,
    checkpoint: &BTreeMap<String, ModelRecord>,
    top_k: usize,
    filter: &Option<Filter>,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    let query = query_vector(q);
    let Ok(results) = query_results(&rec.response) else {
        return vec![violation(
            ViolationId::I8AsOfExact,
            rec,
            ns,
            "as_of query response did not contain parseable results",
            rec.response.clone(),
        )];
    };
    let first_visible = checkpoint.keys().next();
    let mut expected: Vec<(String, f32, &ModelRecord)> = checkpoint
        .iter()
        .filter(|(id, record)| {
            record_matches_filter(mutation, id, first_visible, record, filter.as_ref())
        })
        .map(|(id, record)| {
            (
                id.clone(),
                oracle_distance(
                    mutation,
                    id,
                    first_visible,
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

    let k = top_k.min(expected.len());
    let mut violations = Vec::new();
    if results.len() != k {
        violations.push(violation(
            ViolationId::I8AsOfExact,
            rec,
            ns,
            "as_of result length did not match checkpoint top-k",
            serde_json::json!({
                "generation": generation,
                "expected_len": k,
                "actual_len": results.len(),
            }),
        ));
    }
    if k == 0 {
        return violations;
    }

    let kth = expected[k - 1].1;
    let eps = score_eps(kth);
    let returned_ids = results
        .iter()
        .map(|result| result.id.as_str())
        .collect::<BTreeSet<_>>();
    for result in &results {
        let Some((_, expected_score, expected_record)) =
            expected.iter().find(|(id, _, _)| id == &result.id)
        else {
            violations.push(violation(
                ViolationId::I8AsOfExact,
                rec,
                ns,
                "as_of query returned id outside checkpoint",
                serde_json::json!({
                    "id": result.id,
                    "generation": generation,
                }),
            ));
            continue;
        };
        if *expected_score > kth + eps {
            violations.push(violation(
                ViolationId::I8AsOfExact,
                rec,
                ns,
                "as_of query returned id outside kth tie group",
                serde_json::json!({
                    "id": result.id,
                    "score": expected_score,
                    "kth": kth,
                    "eps": eps,
                    "generation": generation,
                }),
            ));
        }
        if !score_close(result.score, *expected_score) {
            violations.push(violation(
                ViolationId::I8AsOfExact,
                rec,
                ns,
                "as_of query score did not match checkpoint distance",
                serde_json::json!({
                    "id": result.id,
                    "actual": result.score,
                    "expected": expected_score,
                    "generation": generation,
                }),
            ));
        }
        if include_attributes(q)
            && !attributes_equal(&result.attributes, &expected_record.attributes)
        {
            violations.push(violation(
                ViolationId::I8AsOfExact,
                rec,
                ns,
                "as_of query attributes did not match checkpoint",
                serde_json::json!({
                    "id": result.id,
                    "actual": result.attributes,
                    "expected": expected_record.attributes,
                    "generation": generation,
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
                ViolationId::I8AsOfExact,
                rec,
                ns,
                "as_of query omitted id below kth tie boundary",
                serde_json::json!({
                    "id": id,
                    "score": score,
                    "kth": kth,
                    "eps": eps,
                    "generation": generation,
                }),
            ));
        }
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
    let expected = match &rec.op {
        Op::Query { ns, q, .. } => {
            let QueryOracleClass::ExpectError { status, code } = &q.class else {
                return Vec::new();
            };
            (ns.as_str(), *status, code.as_str())
        }
        Op::InvalidProbe { ns, probe } => {
            (ns.as_str(), probe.expected_status(), probe.expected_code())
        }
        _ => return Vec::new(),
    };
    let actual_code = rec.response.get("code").and_then(serde_json::Value::as_str);
    if rec.status == expected.1 && actual_code == Some(expected.2) {
        return Vec::new();
    }
    vec![violation(
        ViolationId::I11ErrorEnvelope,
        rec,
        expected.0,
        "operation returned unexpected status/code",
        serde_json::json!({
            "expected_status": expected.1,
            "expected_code": expected.2,
            "actual_status": rec.status,
            "actual_code": actual_code,
        }),
    )]
}

fn check_i10_failed_validation_no_wal(rec: &OpRecord) -> Vec<Violation> {
    let Op::InvalidProbe { ns, probe } = &rec.op else {
        return Vec::new();
    };
    if !probe.is_write_shaped() || !(400..500).contains(&rec.status) {
        return Vec::new();
    }
    let before = &rec.response["compact_status_before"];
    let after = &rec.response["compact_status_after"];
    let fields = ["manifest_generation", "uncompacted_fragments"];
    let mut violations = Vec::new();
    for field in fields {
        if before.get(field) != after.get(field) {
            violations.push(violation(
                ViolationId::I10FailedValidationNoWal,
                rec,
                ns,
                "failed validation changed compact status",
                serde_json::json!({
                    "field": field,
                    "before": before.get(field),
                    "after": after.get(field),
                    "probe": probe,
                }),
            ));
        }
    }
    violations
}

/// I12 — Structural sanity
///
/// Every successful query response has no NaN/infinite scores, no duplicate
/// ids, and `results.len() <= top_k`.
fn check_i12_structural_sanity(model: &Model, rec: &OpRecord) -> Vec<Violation> {
    match &rec.op {
        Op::Query { ns, q, .. } => check_query_structural(model, rec, ns, q, &rec.response),
        Op::BatchQuery { ns, qs } => rec.response["batch"]["results"]
            .as_array()
            .into_iter()
            .flatten()
            .zip(qs.iter())
            .filter(|(entry, _)| entry.get("ok").and_then(serde_json::Value::as_bool) == Some(true))
            .flat_map(|(entry, q)| check_query_structural(model, rec, ns, q, &entry["response"]))
            .collect(),
        Op::PaginateAll { ns, q, .. } => {
            let mut violations = Vec::new();
            for page in rec.response["pages"].as_array().into_iter().flatten() {
                if page.get("status").and_then(serde_json::Value::as_u64) == Some(200) {
                    violations.extend(check_query_structural(model, rec, ns, q, &page["body"]));
                }
            }
            violations
        }
        _ => Vec::new(),
    }
}

fn check_query_structural(
    model: &Model,
    rec: &OpRecord,
    ns: &str,
    q: &GeneratedQuery,
    response: &serde_json::Value,
) -> Vec<Violation> {
    let Ok(results) = query_results(response) else {
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
    violations.extend(check_groups_structural(rec, ns, q, response));
    violations.extend(check_facets_structural(model, rec, ns, response));
    violations.extend(check_explain_structural(rec, ns, q, response));
    violations.extend(check_debug_structural(rec, ns, q, response));
    violations
}

fn check_groups_structural(
    rec: &OpRecord,
    ns: &str,
    q: &GeneratedQuery,
    response: &serde_json::Value,
) -> Vec<Violation> {
    let Some(groups) = response.get("groups").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    let max_per_group = q
        .body
        .get("grouping")
        .and_then(|grouping| grouping.get("max_per_group"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(u64::MAX) as usize;
    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    for group in groups {
        let results = group["results"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if results.len() > max_per_group {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "group exceeded max_per_group",
                serde_json::json!({ "group": group, "max_per_group": max_per_group }),
            ));
        }
        for result in results {
            if let Some(id) = result.get("id").and_then(serde_json::Value::as_str) {
                if !seen.insert(id.to_string()) {
                    violations.push(violation(
                        ViolationId::I12StructuralSanity,
                        rec,
                        ns,
                        "grouped query returned duplicate id across groups",
                        serde_json::json!({ "id": id }),
                    ));
                }
            }
        }
    }
    violations
}

fn check_facets_structural(
    model: &Model,
    rec: &OpRecord,
    ns: &str,
    response: &serde_json::Value,
) -> Vec<Violation> {
    let Some(facets) = response
        .get("facets")
        .and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let totals = facet_totals(ns_model);
    let mut violations = Vec::new();
    for (field, counts) in facets {
        let Some(counts) = counts.as_object() else {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "facet counts were not an object",
                serde_json::json!({ "field": field, "counts": counts }),
            ));
            continue;
        };
        for (value, count) in counts {
            let actual = count.as_u64().unwrap_or(u64::MAX);
            let upper = totals
                .get(field)
                .and_then(|field_counts| field_counts.get(value))
                .copied()
                .unwrap_or(0);
            if actual > upper {
                violations.push(violation(
                    ViolationId::I12StructuralSanity,
                    rec,
                    ns,
                    "facet count exceeded model total",
                    serde_json::json!({
                        "field": field,
                        "value": value,
                        "actual": actual,
                        "upper": upper,
                    }),
                ));
            }
        }
    }
    violations
}

fn check_explain_structural(
    rec: &OpRecord,
    ns: &str,
    q: &GeneratedQuery,
    response: &serde_json::Value,
) -> Vec<Violation> {
    if q.body.get("explain").is_none() {
        return Vec::new();
    }
    let mut violations = Vec::new();
    let explain = &response["explain"];
    if explain.get("plan").is_none() {
        violations.push(violation(
            ViolationId::I12StructuralSanity,
            rec,
            ns,
            "explain response omitted plan",
            response.clone(),
        ));
        return violations;
    }
    if requested_explain_full(q) {
        let response_ids = response_ids(response);
        let explain_ids = explain["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("id").and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        if response_ids != explain_ids {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "full explain provenance ids did not match results",
                serde_json::json!({
                    "response_ids": response_ids,
                    "explain_ids": explain_ids,
                }),
            ));
        }
    }
    violations
}

fn check_debug_structural(
    rec: &OpRecord,
    ns: &str,
    q: &GeneratedQuery,
    response: &serde_json::Value,
) -> Vec<Violation> {
    if q.body.get("debug").and_then(serde_json::Value::as_bool) != Some(true) {
        return Vec::new();
    }
    let Some(debug) = response.get("debug").and_then(serde_json::Value::as_object) else {
        return vec![violation(
            ViolationId::I12StructuralSanity,
            rec,
            ns,
            "debug response omitted debug object",
            response.clone(),
        )];
    };
    let expected = consistency_str(q.consistency().unwrap_or(ConsistencyLevel::Strong));
    if debug
        .get("consistency_effective")
        .and_then(serde_json::Value::as_str)
        != Some(expected)
    {
        return vec![violation(
            ViolationId::I12StructuralSanity,
            rec,
            ns,
            "debug consistency_effective did not match request",
            serde_json::json!({
                "expected": expected,
                "debug": debug,
            }),
        )];
    }
    Vec::new()
}

fn check_i3_membership(model: &Model, rec: &OpRecord) -> Vec<Violation> {
    let Op::Query { ns, q, as_of } = &rec.op else {
        return Vec::new();
    };
    if as_of.is_some() {
        return Vec::new();
    }
    let QueryOracleClass::Membership { consistency } = q.class else {
        return Vec::new();
    };
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let visible = ns_model
        .visible_records(consistency)
        .into_iter()
        .map(|(id, _)| id.clone())
        .collect::<BTreeSet<_>>();
    response_ids(&rec.response)
        .into_iter()
        .filter(|id| !visible.contains(id))
        .map(|id| {
            violation(
                exact_violation_id(consistency),
                rec,
                ns,
                "membership query returned id outside visible set",
                serde_json::json!({ "id": id, "consistency": consistency }),
            )
        })
        .collect()
}

fn check_i5_batch_equivalence(rec: &OpRecord) -> Vec<Violation> {
    let Op::BatchQuery { ns, qs } = &rec.op else {
        return Vec::new();
    };
    let Some(batch_entries) = rec.response["batch"]["results"].as_array() else {
        return vec![violation(
            ViolationId::I5BatchEquivalent,
            rec,
            ns,
            "batch response omitted results array",
            rec.response.clone(),
        )];
    };
    let Some(individual) = rec.response["individual"].as_array() else {
        return vec![violation(
            ViolationId::I5BatchEquivalent,
            rec,
            ns,
            "batch record omitted individual responses",
            rec.response.clone(),
        )];
    };
    let mut violations = Vec::new();
    if batch_entries.len() != qs.len() || individual.len() != qs.len() {
        violations.push(violation(
            ViolationId::I5BatchEquivalent,
            rec,
            ns,
            "batch/individual response length mismatch",
            serde_json::json!({
                "queries": qs.len(),
                "batch": batch_entries.len(),
                "individual": individual.len(),
            }),
        ));
        return violations;
    }
    for (idx, ((entry, single), q)) in batch_entries
        .iter()
        .zip(individual.iter())
        .zip(qs.iter())
        .enumerate()
    {
        match &q.class {
            QueryOracleClass::ExpectError { status, code } => {
                let ok = entry.get("ok").and_then(serde_json::Value::as_bool);
                let batch_status = entry["error"]["status"].as_u64();
                let batch_code = entry["error"]["code"].as_str();
                let single_status = single["status"].as_u64();
                let single_code = single["body"]["code"].as_str();
                if ok != Some(false)
                    || batch_status != Some(u64::from(*status))
                    || batch_code != Some(code.as_str())
                    || single_status != Some(u64::from(*status))
                    || single_code != Some(code.as_str())
                {
                    violations.push(violation(
                        ViolationId::I5BatchEquivalent,
                        rec,
                        ns,
                        "batch error entry did not match individual error",
                        serde_json::json!({
                            "index": idx,
                            "expected_status": status,
                            "expected_code": code,
                            "entry": entry,
                            "single": single,
                        }),
                    ));
                }
            }
            _ => {
                if entry.get("ok").and_then(serde_json::Value::as_bool) != Some(true)
                    || single["status"]
                        .as_u64()
                        .is_none_or(|status| !(200..300).contains(&status))
                    || !responses_equivalent(&entry["response"], &single["body"])
                {
                    violations.push(violation(
                        ViolationId::I5BatchEquivalent,
                        rec,
                        ns,
                        "batch success entry did not match individual query",
                        serde_json::json!({
                            "index": idx,
                            "entry": entry,
                            "single": single,
                        }),
                    ));
                }
            }
        }
    }
    violations
}

fn check_i6_pagination_equivalence(rec: &OpRecord) -> Vec<Violation> {
    let Op::PaginateAll { ns, .. } = &rec.op else {
        return Vec::new();
    };
    let pages = rec.response["pages"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let big = &rec.response["big"];
    let mut violations = Vec::new();
    if pages.is_empty()
        || big["status"]
            .as_u64()
            .is_none_or(|status| !(200..300).contains(&status))
    {
        violations.push(violation(
            ViolationId::I6PaginationEquivalent,
            rec,
            ns,
            "pagination did not record successful pages and big query",
            rec.response.clone(),
        ));
        return violations;
    }
    let mut paged_results = Vec::new();
    for page in pages {
        if page["status"]
            .as_u64()
            .is_none_or(|status| !(200..300).contains(&status))
        {
            violations.push(violation(
                ViolationId::I6PaginationEquivalent,
                rec,
                ns,
                "pagination page failed",
                page.clone(),
            ));
            return violations;
        }
        if let Ok(results) = query_results(&page["body"]) {
            paged_results.extend(results);
        }
    }
    let Ok(big_results) = query_results(&big["body"]) else {
        return vec![violation(
            ViolationId::I6PaginationEquivalent,
            rec,
            ns,
            "big pagination query response was not parseable",
            big.clone(),
        )];
    };
    let paged_ids = paged_results
        .iter()
        .map(|result| result.id.clone())
        .collect::<Vec<_>>();
    let big_ids = big_results
        .iter()
        .map(|result| result.id.clone())
        .collect::<Vec<_>>();
    if paged_ids != big_ids {
        violations.push(violation(
            ViolationId::I6PaginationEquivalent,
            rec,
            ns,
            "paged ids did not match big query ids",
            serde_json::json!({ "paged": paged_ids, "big": big_ids }),
        ));
    }
    let unique = paged_results
        .iter()
        .map(|result| result.id.clone())
        .collect::<BTreeSet<_>>();
    if unique.len() != paged_results.len() {
        violations.push(violation(
            ViolationId::I6PaginationEquivalent,
            rec,
            ns,
            "paged query returned duplicate id across pages",
            serde_json::json!({
                "paged": paged_results
                    .iter()
                    .map(|result| result.id.clone())
                    .collect::<Vec<_>>()
            }),
        ));
    }
    for pair in paged_results.windows(2) {
        if pair[0].score > pair[1].score + score_eps(pair[1].score) {
            violations.push(violation(
                ViolationId::I6PaginationEquivalent,
                rec,
                ns,
                "paged scores were not ascending across page boundaries",
                serde_json::json!({ "left": pair[0], "right": pair[1] }),
            ));
        }
    }
    if pages
        .last()
        .and_then(|page| page["body"].get("next_cursor"))
        .is_some()
    {
        violations.push(violation(
            ViolationId::I6PaginationEquivalent,
            rec,
            ns,
            "terminal page still had a next_cursor",
            pages.last().cloned().unwrap_or_default(),
        ));
    }
    violations
}

fn check_i7_fts_membership(model: &Model, rec: &OpRecord) -> Vec<Violation> {
    let Op::Query { ns, q, as_of } = &rec.op else {
        return Vec::new();
    };
    if as_of.is_some() {
        return Vec::new();
    }
    let query_words = bm25_query_words(&q.body);
    if query_words.is_empty() {
        return Vec::new();
    }
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let mut violations = Vec::new();
    for id in response_ids(&rec.response) {
        let Some(record) =
            ns_model.visible_get(&id, q.consistency().unwrap_or(ConsistencyLevel::Strong))
        else {
            violations.push(violation(
                ViolationId::I7FtsMembership,
                rec,
                ns,
                "FTS query returned id outside visible set",
                serde_json::json!({ "id": id }),
            ));
            continue;
        };
        let Some(text) = record
            .attributes
            .as_ref()
            .and_then(|attrs| attrs.get("body"))
            .and_then(attribute_string)
        else {
            violations.push(violation(
                ViolationId::I7FtsMembership,
                rec,
                ns,
                "FTS query returned id without text body",
                serde_json::json!({ "id": id }),
            ));
            continue;
        };
        let lower = text.to_ascii_lowercase();
        if !query_words.iter().any(|word| lower.contains(word)) {
            violations.push(violation(
                ViolationId::I7FtsMembership,
                rec,
                ns,
                "FTS query returned id whose body had none of the query words",
                serde_json::json!({
                    "id": id,
                    "body": lower,
                    "query_words": query_words,
                }),
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

trait VisibleRecords {
    fn visible_records(&self, consistency: ConsistencyLevel) -> Vec<(&String, &ModelRecord)>;
    fn visible_get(&self, id: &str, consistency: ConsistencyLevel) -> Option<&ModelRecord>;
}

impl VisibleRecords for NsModel {
    fn visible_records(&self, consistency: ConsistencyLevel) -> Vec<(&String, &ModelRecord)> {
        match consistency {
            ConsistencyLevel::Strong => self.live.iter().collect(),
            ConsistencyLevel::Eventual => self
                .compacted_live
                .iter()
                .filter(|(id, _)| !self.wal_tombstones.contains(*id))
                .collect(),
        }
    }

    fn visible_get(&self, id: &str, consistency: ConsistencyLevel) -> Option<&ModelRecord> {
        match consistency {
            ConsistencyLevel::Strong => self.live.get(id),
            ConsistencyLevel::Eventual => {
                if self.wal_tombstones.contains(id) {
                    None
                } else {
                    self.compacted_live.get(id)
                }
            }
        }
    }
}

fn exact_violation_id(consistency: ConsistencyLevel) -> ViolationId {
    match consistency {
        ConsistencyLevel::Strong => ViolationId::I1StrongExact,
        ConsistencyLevel::Eventual => ViolationId::I3EventualExact,
    }
}

fn fetch_violation_id(consistency: ConsistencyLevel) -> ViolationId {
    match consistency {
        ConsistencyLevel::Strong => ViolationId::I4FetchExact,
        ConsistencyLevel::Eventual => ViolationId::I3EventualExact,
    }
}

fn consistency_str(consistency: ConsistencyLevel) -> &'static str {
    match consistency {
        ConsistencyLevel::Strong => "strong",
        ConsistencyLevel::Eventual => "eventual",
    }
}

fn first_visible_id(ns_model: &NsModel, consistency: ConsistencyLevel) -> Option<&String> {
    ns_model
        .visible_records(consistency)
        .first()
        .map(|(id, _)| *id)
}

fn checkpoint_for_as_of<'a>(
    ns_model: &'a NsModel,
    as_of: &AsOfTarget,
) -> Option<(u64, &'a BTreeMap<String, ModelRecord>)> {
    let generation = match as_of {
        AsOfTarget::Generation(generation) => *generation,
        AsOfTarget::Snapshot(name) => *ns_model.snapshots.get(name)?,
        AsOfTarget::Timestamp(_) => return None,
    };
    ns_model
        .checkpoints
        .get(&generation)
        .map(|checkpoint| (generation, checkpoint))
}

fn record_matches_filter(
    mutation: Option<OracleMutation>,
    id: &str,
    first_id: Option<&String>,
    record: &ModelRecord,
    filter: Option<&Filter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let base = record
        .attributes
        .as_ref()
        .is_some_and(|attributes| evaluate_filter(filter, attributes));
    if mutation == Some(OracleMutation::FilterSkew) && first_id.is_some_and(|first| first == id) {
        !base
    } else {
        base
    }
}

fn response_ids(response: &serde_json::Value) -> Vec<String> {
    let mut ids = query_results(response)
        .unwrap_or_default()
        .into_iter()
        .map(|result| result.id)
        .collect::<Vec<_>>();
    ids.extend(
        response["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|group| group["results"].as_array().into_iter().flatten())
            .filter_map(|result| result.get("id").and_then(serde_json::Value::as_str))
            .map(str::to_string),
    );
    ids
}

fn responses_equivalent(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    let Ok(left_results) = query_results(left) else {
        return false;
    };
    let Ok(right_results) = query_results(right) else {
        return false;
    };
    if left_results.len() != right_results.len() {
        return false;
    }
    left_results
        .iter()
        .zip(right_results.iter())
        .all(|(left, right)| left.id == right.id && score_close(left.score, right.score))
}

fn facet_totals(ns_model: &NsModel) -> BTreeMap<String, BTreeMap<String, u64>> {
    let mut totals = BTreeMap::new();
    for record in ns_model.live.values() {
        let Some(attributes) = record.attributes.as_ref() else {
            continue;
        };
        for (field, value) in attributes {
            let field_counts = totals.entry(field.clone()).or_insert_with(BTreeMap::new);
            for key in facet_keys(value) {
                *field_counts.entry(key).or_default() += 1;
            }
        }
    }
    totals
}

fn facet_keys(value: &AttributeValue) -> Vec<String> {
    match value {
        AttributeValue::String(value) => vec![value.clone()],
        AttributeValue::Integer(value) => vec![value.to_string()],
        AttributeValue::Float(value) => vec![value.to_string()],
        AttributeValue::Bool(value) => vec![value.to_string()],
        AttributeValue::StringList(values) => values.clone(),
        AttributeValue::IntegerList(values) => values.iter().map(ToString::to_string).collect(),
        AttributeValue::FloatList(values) => values.iter().map(ToString::to_string).collect(),
    }
}

fn requested_explain_full(q: &GeneratedQuery) -> bool {
    match q.body.get("explain") {
        Some(serde_json::Value::String(value)) => value == "full",
        Some(serde_json::Value::Object(object)) => object
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|mode| mode == "full"),
        _ => false,
    }
}

fn bm25_query_words(body: &serde_json::Value) -> Vec<String> {
    let mut words = Vec::new();
    if let Some(rank_by) = body.get("rank_by") {
        collect_rank_by_words(rank_by, &mut words);
    }
    if let Some(sources) = body.get("sources").and_then(serde_json::Value::as_array) {
        for source in sources {
            if source.get("type").and_then(serde_json::Value::as_str) == Some("bm25") {
                if let Some(rank_by) = source.get("rank_by") {
                    collect_rank_by_words(rank_by, &mut words);
                }
            }
        }
    }
    words.sort();
    words.dedup();
    words
}

fn collect_rank_by_words(rank_by: &serde_json::Value, out: &mut Vec<String>) {
    let Some(array) = rank_by.as_array() else {
        return;
    };
    if array.len() == 3
        && array[1]
            .as_str()
            .is_some_and(|algo| algo.eq_ignore_ascii_case("bm25"))
    {
        if let Some(query) = array[2].as_str() {
            out.extend(
                query
                    .split_whitespace()
                    .map(|word| word.to_ascii_lowercase()),
            );
        }
        return;
    }
    match array.first().and_then(serde_json::Value::as_str) {
        Some("Sum" | "sum" | "Max" | "max") => {
            for child in array
                .get(1)
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                collect_rank_by_words(child, out);
            }
        }
        Some("Product" | "product") => {
            if let Some(child) = array.get(2) {
                collect_rank_by_words(child, out);
            }
        }
        _ => {}
    }
}

fn attribute_string(value: &AttributeValue) -> Option<&str> {
    match value {
        AttributeValue::String(value) => Some(value.as_str()),
        _ => None,
    }
}

fn attributes_equal(
    left: &Option<HashMap<String, AttributeValue>>,
    right: &Option<HashMap<String, AttributeValue>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) if left.len() == right.len() => {
            left.iter().all(|(key, left_value)| {
                right
                    .get(key)
                    .is_some_and(|right_value| attribute_values_equal(left_value, right_value))
            })
        }
        _ => false,
    }
}

fn attribute_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::String(left), AttributeValue::String(right)) => left == right,
        (AttributeValue::Integer(left), AttributeValue::Integer(right)) => left == right,
        (AttributeValue::Float(left), AttributeValue::Float(right)) => (left - right).abs() <= 1e-9,
        (AttributeValue::Bool(left), AttributeValue::Bool(right)) => left == right,
        (AttributeValue::StringList(left), AttributeValue::StringList(right)) => left == right,
        (AttributeValue::IntegerList(left), AttributeValue::IntegerList(right)) => left == right,
        (AttributeValue::FloatList(left), AttributeValue::FloatList(right))
            if left.len() == right.len() =>
        {
            left.iter()
                .zip(right.iter())
                .all(|(left, right)| (left - right).abs() <= 1e-9)
        }
        _ => false,
    }
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
