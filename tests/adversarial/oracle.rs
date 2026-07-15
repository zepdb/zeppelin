use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeppelin::index::filter::evaluate_filter;
use zeppelin::types::{AttributeValue, ConsistencyLevel, Filter};

use super::model::{
    model_distance, IndetEffect, Model, ModelRecord, NsIndeterminate, NsModel, OracleMutation,
};
use super::ops::{AsOfTarget, GeneratedQuery, MaintenanceKind, Op, OpRecord, QueryOracleClass};
use super::security_program::{
    check_i22_authz_decision, check_i23_tenant_leak, check_i27_constraint_drop, ExpectedDecision,
    SecurityFinding,
};
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
    I17SketchPublication,
    I18IndeterminateResolution,
    I19CrashRecovery,
    I20CorruptionSurfaced,
    I21FencingViolation,
    I22AuthzDecision,
    I23TenantLeak,
    I24RevocationFreshness,
    I25AuditEvidence,
    I26SecurityStateSanity,
    I27ConstraintDrop,
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
    violations.extend(check_expected_error(model, rec, mode));
    violations.extend(check_security_operation(model, rec, mutation));
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
            violations.extend(check_i13_probe_sandwich(rec));
        }
        violations.extend(check_i7_fts_membership(model, rec));
    }
    violations
}

fn check_security_operation(
    model: &Model,
    rec: &OpRecord,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    if !model.security.enabled() {
        return Vec::new();
    }
    let mut findings = Vec::new();
    if !rec.outcome.starts_with("ambiguous:")
        && rec.response.get("_adversarial_store_fault").is_none()
        && !(rec.status == 429 && rec.response["request_id"] == "adversarial-drop-request")
    {
        if let Some(mut expected) = model.security.expected_decision(&rec.op, rec.index) {
            if mutation == Some(OracleMutation::GrantModelDesync)
                && matches!(rec.op, Op::AuditBarrierOp { .. })
            {
                expected = match expected {
                    ExpectedDecision::Forbidden => ExpectedDecision::Allow,
                    _ => ExpectedDecision::Forbidden,
                };
            }
            if mutation == Some(OracleMutation::DelegationParentDesync)
                && matches!(rec.op, Op::RevokeParentThenUseToken { .. })
            {
                expected = ExpectedDecision::Allow;
            }
            if mutation == Some(OracleMutation::DelegationNarrowingBypass)
                && matches!(rec.op, Op::TokenExceedScopeProbe { .. })
            {
                expected = ExpectedDecision::Allow;
            }
            if let Some(finding) = check_i22_authz_decision(expected, rec.status) {
                findings.push(finding);
            }
        }
    }

    if let Op::UseToken { token, target_ns } | Op::TokenExceedScopeProbe { token, target_ns } =
        &rec.op
    {
        let visible = model
            .security
            .expected_token_visible_ids(model, *token, target_ns);
        let observed = security_response_ids(&rec.response);
        if let Some(finding) = check_i23_tenant_leak(&visible, &observed) {
            findings.push(finding);
        }
        if matches!(rec.op, Op::UseToken { .. }) {
            if let Some(finding) = check_i27_constraint_drop(&visible, &observed) {
                findings.push(finding);
            }
        }
    }

    if let Op::TenantBoundaryProbe {
        actor, target_ns, ..
    } = &rec.op
    {
        let visible = model.security.expected_visible_ids(model, rec.op.actor());
        let mut observed = security_response_ids(&rec.response);
        if mutation == Some(OracleMutation::LeakedIdSuppression) {
            observed.insert("mutation-outsider-id".to_string());
        }
        if let Some(finding) = check_i23_tenant_leak(&visible, &observed) {
            findings.push(finding);
        }
        let own_namespace = model
            .security
            .config
            .as_ref()
            .and_then(|config| config.tenant_namespace(*actor));
        if own_namespace != Some(target_ns.as_str()) {
            let aggregate_leaks = security_response_aggregate_leaks(&rec.response);
            if !aggregate_leaks.is_empty() {
                findings.push(SecurityFinding {
                    id: ViolationId::I23TenantLeak,
                    detail: "cross-tenant response exposed non-empty aggregate data".to_string(),
                    evidence: serde_json::json!({
                        "target_namespace": target_ns,
                        "aggregate_leaks": aggregate_leaks,
                    }),
                });
            }
        }
    }

    findings
        .into_iter()
        .map(|finding| security_finding_violation(rec, finding))
        .collect()
}

fn security_response_aggregate_leaks(value: &serde_json::Value) -> BTreeSet<String> {
    fn collect(value: &serde_json::Value, path: &str, leaks: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    collect(value, &format!("{path}/{index}"), leaks);
                }
            }
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    let child = format!("{path}/{key}");
                    if matches!(key.as_str(), "count" | "total" | "total_count")
                        && value.as_u64().is_some_and(|count| count > 0)
                    {
                        leaks.insert(child.clone());
                    }
                    if matches!(key.as_str(), "facets" | "groups" | "buckets")
                        && match value {
                            serde_json::Value::Array(values) => !values.is_empty(),
                            serde_json::Value::Object(values) => !values.is_empty(),
                            _ => false,
                        }
                    {
                        leaks.insert(child.clone());
                    }
                    collect(value, &child, leaks);
                }
            }
            _ => {}
        }
    }
    let mut leaks = BTreeSet::new();
    collect(value, "", &mut leaks);
    leaks
}

pub(crate) fn security_response_ids(value: &serde_json::Value) -> BTreeSet<String> {
    fn collect(value: &serde_json::Value, ids: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, ids);
                }
            }
            serde_json::Value::Object(object) => {
                if let Some(id) = object.get("id").and_then(serde_json::Value::as_str) {
                    ids.insert(id.to_string());
                }
                for value in object.values() {
                    collect(value, ids);
                }
            }
            _ => {}
        }
    }
    let mut ids = BTreeSet::new();
    collect(value, &mut ids);
    ids
}

fn security_finding_violation(rec: &OpRecord, finding: SecurityFinding) -> Violation {
    Violation {
        id: finding.id,
        op_index: rec.index,
        namespace: rec.op.namespace().to_string(),
        detail: finding.detail,
        evidence: finding.evidence,
    }
}

pub struct CorruptionContext<'a> {
    pub tainted_keys: &'a BTreeSet<String>,
    pub fault_window_active: bool,
}

#[must_use]
pub fn check_op_with_faults(
    model: &Model,
    rec: &OpRecord,
    mode: RunMode,
    mutation: Option<OracleMutation>,
    corruption: Option<&CorruptionContext<'_>>,
) -> Vec<Violation> {
    let mut violations = check_op(model, rec, mode, mutation);
    if let Some(corruption) = corruption {
        violations.extend(check_i20_corruption_surfaced(
            model, rec, mutation, corruption,
        ));
    }
    violations
}

fn check_i20_corruption_surfaced(
    model: &Model,
    rec: &OpRecord,
    mutation: Option<OracleMutation>,
    corruption: &CorruptionContext<'_>,
) -> Vec<Violation> {
    if !(200..300).contains(&rec.status) || corruption.tainted_keys.is_empty() {
        return Vec::new();
    }
    let namespace = rec.op.namespace();
    let Some(ns_model) = model.namespaces.get(namespace) else {
        return Vec::new();
    };
    if !ns_model.spec.is_exact()
        || !ns_model.indeterminate.is_empty()
        || !ns_model.indeterminate_ns.is_empty()
    {
        // TODO tighten once per-record read sets can exclude ambiguous state.
        return Vec::new();
    }

    let divergences = match &rec.op {
        Op::FetchVectors { .. } => check_i4_fetch_exact(model, rec, RunMode::Deterministic),
        Op::Query { q, as_of: None, .. }
            if matches!(q.class, QueryOracleClass::ExactAnn { .. }) =>
        {
            check_i1_strong_exact(model, rec, RunMode::Deterministic, mutation)
        }
        Op::Query {
            q, as_of: Some(_), ..
        } if matches!(q.class, QueryOracleClass::ExactAnn { .. }) => {
            check_i8_as_of_exact(model, rec, RunMode::Deterministic, mutation)
        }
        _ => {
            // TODO tighten for composite operations once their read sets are exact.
            Vec::new()
        }
    };
    if divergences.is_empty() {
        return Vec::new();
    }

    vec![violation(
        ViolationId::I20CorruptionSurfaced,
        rec,
        namespace,
        "successful response diverged while corrupted storage was plausibly consulted",
        serde_json::json!({
            "tainted_keys": corruption.tainted_keys,
            "fault_window_active": corruption.fault_window_active,
            "exact_divergences": divergences,
        }),
    )]
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
    let Op::Query { ns, q, as_of, .. } = &rec.op else {
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
    let Ok(mut results) = query_results(&rec.response) else {
        return vec![violation(
            violation_id,
            rec,
            ns,
            "query response did not contain parseable results",
            rec.response.clone(),
        )];
    };
    results.retain(|result| !ns_model.indeterminate.contains_key(&result.id));

    let first_visible = first_visible_id(ns_model, *consistency);
    let mut expected: Vec<(String, f32, &ModelRecord)> = ns_model
        .visible_records(*consistency)
        .into_iter()
        .filter(|(id, _)| !ns_model.indeterminate.contains_key(*id))
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
        .filter(|id| ns_model.deleted_ever.contains(id) && !ns_model.indeterminate.contains_key(id))
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
        ..
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
    if !ns_model.indeterminate.is_empty() {
        return check_indeterminate_fetch(rec, ns, ids, *consistency, ns_model, &actual);
    }

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

fn check_indeterminate_fetch(
    rec: &OpRecord,
    ns: &str,
    ids: &[String],
    consistency: ConsistencyLevel,
    ns_model: &NsModel,
    actual: &WireFetchResponse,
) -> Vec<Violation> {
    let violation_id = fetch_violation_id(consistency);
    let actual_by_id = actual
        .results
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect::<BTreeMap<_, _>>();
    let missing = actual
        .missing
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let requested = ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut violations = Vec::new();

    for record in &actual.results {
        if !requested.contains(record.id.as_str()) {
            violations.push(violation(
                violation_id,
                rec,
                ns,
                "fetch returned an id that was not requested",
                serde_json::json!({ "id": record.id }),
            ));
        }
    }

    for id in ids {
        if let Some(pending) = ns_model.indeterminate.get(id) {
            let old = ns_model.visible_get(id, consistency);
            let actual_record = actual_by_id.get(id.as_str()).copied();
            let accepted = match (&pending.effect, actual_record) {
                (IndetEffect::MaybeUpserted(candidate), Some(actual_record)) => {
                    wire_record_matches(actual_record, candidate)
                        || old.is_some_and(|old| wire_record_matches(actual_record, old))
                }
                (IndetEffect::MaybeUpserted(_), None) => {
                    old.is_none() && missing.contains(id.as_str())
                }
                (IndetEffect::MaybeDeleted, Some(actual_record)) => {
                    old.is_some_and(|old| wire_record_matches(actual_record, old))
                }
                (IndetEffect::MaybeDeleted, None) => missing.contains(id.as_str()),
            };
            if !accepted {
                violations.push(violation(
                    violation_id,
                    rec,
                    ns,
                    "indeterminate fetch matched neither old nor new state",
                    serde_json::json!({
                        "id": id,
                        "pending": pending,
                        "actual": actual_record,
                        "missing": missing.contains(id.as_str()),
                    }),
                ));
            }
            continue;
        }

        match (
            ns_model.visible_get(id, consistency),
            actual_by_id.get(id.as_str()),
        ) {
            (Some(expected), Some(actual_record))
                if wire_record_matches(actual_record, expected) => {}
            (Some(expected), actual_record) => violations.push(violation(
                violation_id,
                rec,
                ns,
                "determinate fetch value diverged while another id was indeterminate",
                serde_json::json!({ "id": id, "expected": expected, "actual": actual_record }),
            )),
            (None, None) if missing.contains(id.as_str()) => {}
            (None, actual_record) => violations.push(violation(
                violation_id,
                rec,
                ns,
                "determinate missing id diverged while another id was indeterminate",
                serde_json::json!({ "id": id, "actual": actual_record }),
            )),
        }
    }
    violations
}

fn wire_record_matches(actual: &WireFetchRecord, expected: &ModelRecord) -> bool {
    actual.values.as_ref() == Some(&expected.values)
        && attributes_equal(&actual.attributes, &expected.attributes)
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
        ..
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
    let nested = nested_error_envelopes(rec);
    if rec.outcome.starts_with("ambiguous:")
        && nested.is_empty()
        && rec.response.get("code").is_none()
    {
        return Vec::new();
    }
    if nested.is_empty() {
        return check_error_envelope_body(rec, "$", rec.status, &rec.response)
            .into_iter()
            .collect();
    }
    nested
        .into_iter()
        .filter_map(|nested| {
            check_error_envelope_body(rec, &nested.path, nested.status, nested.body)
        })
        .collect()
}

struct NestedErrorEnvelope<'a> {
    path: String,
    status: u16,
    body: &'a serde_json::Value,
}

fn nested_error_envelopes(rec: &OpRecord) -> Vec<NestedErrorEnvelope<'_>> {
    let mut nested = Vec::new();
    match &rec.op {
        Op::BatchQuery { .. } => {
            push_nested_error(&mut nested, "batch", &rec.response["batch"]);
            for (idx, individual) in rec.response["individual"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                push_nested_error(&mut nested, &format!("individual[{idx}]"), individual);
            }
        }
        Op::PaginateAll { .. } => {
            for (idx, page) in rec.response["pages"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                push_nested_error(&mut nested, &format!("pages[{idx}]"), page);
            }
            push_nested_error(&mut nested, "big", &rec.response["big"]);
        }
        Op::ExportProbe { .. } => {
            push_nested_error(&mut nested, "fetch", &rec.response["fetch"]);
            push_nested_error(&mut nested, "snapshot", &rec.response["snapshot"]);
        }
        _ => {}
    }
    nested
}

fn push_nested_error<'a>(
    nested: &mut Vec<NestedErrorEnvelope<'a>>,
    path: &str,
    wrapper: &'a serde_json::Value,
) {
    let Some(status) = wrapper
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
    else {
        return;
    };
    if (200..300).contains(&status) {
        return;
    }
    nested.push(NestedErrorEnvelope {
        path: path.to_string(),
        status,
        body: wrapper.get("body").unwrap_or(wrapper),
    });
}

fn check_error_envelope_body(
    rec: &OpRecord,
    response_path: &str,
    status_code: u16,
    body: &serde_json::Value,
) -> Option<Violation> {
    let Some(object) = body.as_object() else {
        return Some(violation(
            ViolationId::I11ErrorEnvelope,
            rec,
            rec.op.namespace(),
            "error response was not a JSON object",
            serde_json::json!({
                "status": status_code,
                "response_path": response_path,
                "body": body,
            }),
        ));
    };
    let code = object.get("code").and_then(serde_json::Value::as_str);
    let status = object.get("status").and_then(serde_json::Value::as_u64);
    let retryable = object.get("retryable").and_then(serde_json::Value::as_bool);
    let error = object.get("error").and_then(serde_json::Value::as_str);
    let request_id = object.get("request_id").and_then(serde_json::Value::as_str);
    if code.is_none()
        || !KNOWN_ERROR_CODES.contains(&code.unwrap())
        || status != Some(u64::from(status_code))
        || retryable.is_none()
        || error.is_none_or(str::is_empty)
        || request_id.is_none_or(str::is_empty)
    {
        return Some(violation(
            ViolationId::I11ErrorEnvelope,
            rec,
            rec.op.namespace(),
            "error response did not match the canonical envelope",
            serde_json::json!({
                "status": status_code,
                "response_path": response_path,
                "body": body,
            }),
        ));
    }
    None
}

fn check_expected_error(model: &Model, rec: &OpRecord, mode: RunMode) -> Vec<Violation> {
    let expected = match &rec.op {
        Op::Query { ns, q, .. } => {
            let QueryOracleClass::ExpectError { status, code } = &q.class else {
                return Vec::new();
            };
            (ns.as_str(), *status, code.as_str())
        }
        Op::InvalidProbe { ns, probe, .. } => {
            (ns.as_str(), probe.expected_status(), probe.expected_code())
        }
        _ => return Vec::new(),
    };
    let actual_code = rec.response.get("code").and_then(serde_json::Value::as_str);
    if mode == RunMode::Chaos
        && rec.status >= 500
        && matches!(actual_code, Some("STORAGE_ERROR" | "INTERNAL_DATA_MISSING"))
        && rec.response.get("_adversarial_store_fault") == Some(&serde_json::Value::Bool(true))
    {
        return Vec::new();
    }
    if mode == RunMode::Chaos
        && rec.status == 404
        && actual_code == Some("NAMESPACE_NOT_FOUND")
        && model
            .namespaces
            .get(rec.op.namespace())
            .is_some_and(|ns_model| {
                ns_model.indeterminate_ns.iter().any(|pending| {
                    matches!(
                        pending,
                        NsIndeterminate::MaybeCreatedNs | NsIndeterminate::MaybeDeletedNs
                    )
                })
            })
    {
        return Vec::new();
    }
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
    let Op::InvalidProbe { ns, probe, .. } = &rec.op else {
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
        Op::BatchQuery { ns, qs, .. } => rec.response["batch"]["results"]
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
    let has_field_grouping = field_grouping_requested(q);
    if !has_field_grouping {
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
    if field_grouping_requested(q) {
        if let Some(top_k) = q.top_k() {
            if groups.len() > top_k {
                violations.push(violation(
                    ViolationId::I12StructuralSanity,
                    rec,
                    ns,
                    "grouped query returned more groups than top_k",
                    serde_json::json!({ "top_k": top_k, "actual_groups": groups.len() }),
                ));
            }
        }
        let flat_ids = query_results(response)
            .unwrap_or_default()
            .into_iter()
            .map(|result| result.id)
            .collect::<Vec<_>>();
        let grouped_ids = groups
            .iter()
            .flat_map(|group| group["results"].as_array().into_iter().flatten())
            .filter_map(|result| result.get("id").and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        if flat_ids != grouped_ids {
            violations.push(violation(
                ViolationId::I12StructuralSanity,
                rec,
                ns,
                "grouped query flat results did not match group concatenation",
                serde_json::json!({
                    "flat_ids": flat_ids,
                    "grouped_ids": grouped_ids,
                }),
            ));
        }
    }
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
    let Op::Query { ns, q, as_of, .. } = &rec.op else {
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
        .filter(|id| !ns_model.indeterminate.contains_key(id))
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
    let Op::BatchQuery { ns, qs, .. } = &rec.op else {
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
                let entry_ok = entry.get("ok").and_then(serde_json::Value::as_bool);
                let single_status = single["status"].as_u64();
                if entry_ok == Some(false)
                    || single_status.is_some_and(|status| !(200..300).contains(&status))
                {
                    if !batch_error_equivalent(entry, single) {
                        violations.push(violation(
                            ViolationId::I5BatchEquivalent,
                            rec,
                            ns,
                            "batch error entry did not match individual query",
                            serde_json::json!({
                                "index": idx,
                                "entry": entry,
                                "single": single,
                            }),
                        ));
                    }
                    continue;
                }
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

fn batch_error_equivalent(entry: &serde_json::Value, single: &serde_json::Value) -> bool {
    if entry.get("ok").and_then(serde_json::Value::as_bool) != Some(false) {
        return false;
    }
    let batch_error = &entry["error"];
    let single_body = &single["body"];
    let Some(batch_status) = batch_error["status"].as_u64() else {
        return false;
    };
    let Some(single_status) = single["status"].as_u64() else {
        return false;
    };
    batch_status == single_status
        && single_body["status"].as_u64() == Some(batch_status)
        && batch_error["code"].as_str() == single_body["code"].as_str()
        && batch_error["error"].as_str() == single_body["error"].as_str()
        && batch_error["retryable"].as_bool() == single_body["retryable"].as_bool()
}

fn check_i6_pagination_equivalence(rec: &OpRecord) -> Vec<Violation> {
    let Op::PaginateAll { ns, q, .. } = &rec.op else {
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
    // I6's full page-walk-vs-big-query equivalence is only an exact ANN
    // contract while the query reads the same WAL/segment shape throughout.
    // Membership/approximate ANN queries intentionally only promise visible
    // members, and compaction can move exact rows between WAL and SQ8 segments
    // mid-walk, changing score bits embedded in cursor markers. In those
    // cases I6 still checks cursor structure: successful pages, no duplicate
    // ids, ascending page-boundary scores, and terminal cursor exhaustion.
    let require_big_equivalence = matches!(q.class, QueryOracleClass::ExactAnn { .. })
        && !pagination_scan_shape_changed(pages, big);
    if require_big_equivalence && paged_ids != big_ids {
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

fn pagination_scan_shape_changed(pages: &[serde_json::Value], big: &serde_json::Value) -> bool {
    let mut first = None;
    for body in pages
        .iter()
        .map(|page| &page["body"])
        .chain(std::iter::once(&big["body"]))
    {
        let Some(shape) = query_scan_shape(body) else {
            continue;
        };
        match first {
            Some(first) if first != shape => return true,
            Some(_) => {}
            None => first = Some(shape),
        }
    }
    false
}

fn query_scan_shape(body: &serde_json::Value) -> Option<(u64, u64)> {
    Some((
        body.get("scanned_fragments")?.as_u64()?,
        body.get("scanned_segments")?.as_u64()?,
    ))
}

fn check_i7_fts_membership(model: &Model, rec: &OpRecord) -> Vec<Violation> {
    let Op::Query { ns, q, as_of, .. } = &rec.op else {
        return Vec::new();
    };
    if as_of.is_some() {
        return Vec::new();
    }
    let query_words = bm25_query_words(&q.body);
    if query_words.is_empty() {
        return Vec::new();
    }
    let enforce_text_membership = is_pure_bm25_query(&q.body);
    let Some(ns_model) = model.namespaces.get(ns) else {
        return Vec::new();
    };
    let mut violations = Vec::new();
    for id in response_ids(&rec.response) {
        if ns_model.indeterminate.contains_key(&id) {
            continue;
        }
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
        if !enforce_text_membership {
            continue;
        }
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

fn is_pure_bm25_query(body: &serde_json::Value) -> bool {
    match body.get("sources").and_then(serde_json::Value::as_array) {
        Some(sources) => {
            !sources.is_empty()
                && sources.iter().all(|source| {
                    source.get("type").and_then(serde_json::Value::as_str) == Some("bm25")
                })
        }
        None => body.get("rank_by").is_some(),
    }
}

fn check_i13_probe_sandwich(rec: &OpRecord) -> Vec<Violation> {
    let Op::ProbeSandwich {
        ns, maintenance, ..
    } = &rec.op
    else {
        return Vec::new();
    };
    let before = &rec.response["before"];
    let after = &rec.response["after"];
    let recorded_maintenance = &rec.response["maintenance"];
    let mut violations = Vec::new();
    if before.is_null() || after.is_null() || recorded_maintenance.is_null() {
        violations.push(violation(
            ViolationId::I13ProbeSandwich,
            rec,
            ns,
            "probe sandwich response omitted before/maintenance/after records",
            rec.response.clone(),
        ));
        return violations;
    }

    let before_generation = before
        .get("manifest_generation")
        .and_then(serde_json::Value::as_u64);
    let after_generation = after
        .get("manifest_generation")
        .and_then(serde_json::Value::as_u64);
    if before_generation.is_none() || after_generation.is_none() {
        violations.push(violation(
            ViolationId::I13ProbeSandwich,
            rec,
            ns,
            "probe sandwich compact-status records omitted manifest_generation",
            serde_json::json!({ "before": before, "after": after }),
        ));
    } else if after_generation < before_generation {
        violations.push(violation(
            ViolationId::I13ProbeSandwich,
            rec,
            ns,
            "probe sandwich moved manifest generation backwards",
            serde_json::json!({
                "before_generation": before_generation,
                "after_generation": after_generation,
            }),
        ));
    }

    match maintenance {
        MaintenanceKind::CompactInline | MaintenanceKind::CompactEndpoint => {
            let ready = after.get("ready").and_then(serde_json::Value::as_bool);
            let uncompacted = after
                .get("uncompacted_fragments")
                .and_then(serde_json::Value::as_u64);
            if ready != Some(true) || uncompacted != Some(0) {
                violations.push(violation(
                    ViolationId::I13ProbeSandwich,
                    rec,
                    ns,
                    "compaction sandwich did not end compact-ready",
                    serde_json::json!({ "after": after, "maintenance": maintenance }),
                ));
            }
        }
        MaintenanceKind::GcCycle | MaintenanceKind::Hydrate => {
            for field in ["ready", "uncompacted_fragments"] {
                if before.get(field) != after.get(field) {
                    violations.push(violation(
                        ViolationId::I13ProbeSandwich,
                        rec,
                        ns,
                        "non-compaction sandwich changed compact-readiness fields",
                        serde_json::json!({
                            "field": field,
                            "before": before.get(field),
                            "after": after.get(field),
                            "maintenance": maintenance,
                        }),
                    ));
                }
            }
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
    let mut possible_ids = BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
    for (id, record) in &ns_model.live {
        add_possible_facet_values(&mut possible_ids, id, record);
    }
    for (id, pending) in &ns_model.indeterminate {
        if let IndetEffect::MaybeUpserted(candidate) = &pending.effect {
            add_possible_facet_values(&mut possible_ids, id, candidate);
        }
    }
    possible_ids
        .into_iter()
        .map(|(field, values)| {
            (
                field,
                values
                    .into_iter()
                    .map(|(value, ids)| (value, ids.len() as u64))
                    .collect(),
            )
        })
        .collect()
}

fn add_possible_facet_values(
    possible_ids: &mut BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    id: &str,
    record: &ModelRecord,
) {
    let Some(attributes) = record.attributes.as_ref() else {
        return;
    };
    for (field, value) in attributes {
        let field_values = possible_ids.entry(field.clone()).or_default();
        for key in facet_keys(value) {
            field_values.entry(key).or_default().insert(id.to_string());
        }
    }
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

fn field_grouping_requested(q: &GeneratedQuery) -> bool {
    q.body
        .get("grouping")
        .and_then(|grouping| grouping.get("type"))
        .and_then(serde_json::Value::as_str)
        == Some("field")
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
    if matches!(
        mutation,
        Some(OracleMutation::SkewScore | OracleMutation::SwallowCorruption)
    ) && first_id.is_some_and(|first| first == id)
    {
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
    "unauthenticated",
    "credential_expired",
    "credential_unknown",
    "forbidden",
    "namespace_not_granted",
    "security_stale",
    "obligation_unsatisfied",
    "constraint_violation",
    "cursor_policy_stale",
    "invalid_namespace",
    "invalid_snapshot",
    "invalid_security_request",
    "security_conflict",
    "security_entity_exists",
    "security_entity_not_found",
    "audit_unavailable",
    "unmapped_route",
    "security_internal",
];

#[cfg(test)]
mod tests {
    use serde_json::json;
    use zeppelin::index::quantization::QuantizationType;
    use zeppelin::types::DistanceMetric;

    use crate::adversarial::ops::{ActorSel, NamespaceSpec};

    use super::*;

    const NS: &str = "ns";

    fn namespace_spec() -> NamespaceSpec {
        NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Euclidean,
            quantization: QuantizationType::None,
            num_centroids: 1,
            fts_fields: vec!["body".to_string()],
            bitmap: false,
        }
    }

    fn model_with_record(id: &str, attributes: Option<HashMap<String, AttributeValue>>) -> Model {
        let mut ns_model = NsModel::new(namespace_spec(), 1);
        ns_model.live.insert(
            id.to_string(),
            ModelRecord {
                values: vec![0.0, 0.0],
                attributes,
            },
        );
        Model {
            namespaces: BTreeMap::from([(NS.to_string(), ns_model)]),
            ..Model::default()
        }
    }

    fn fetch_record(values: [f32; 2]) -> OpRecord {
        OpRecord {
            index: 17,
            wall_ms: 0,
            op: Op::FetchVectors {
                actor: ActorSel::ADMIN,
                ns: NS.to_string(),
                ids: vec!["row".to_string()],
                consistency: ConsistencyLevel::Strong,
            },
            method: "POST".to_string(),
            path: format!("/v1/namespaces/{NS}/vectors/fetch"),
            status: 200,
            response: json!({
                "results": [{
                    "id": "row",
                    "values": values,
                    "attributes": null
                }],
                "missing": []
            }),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: Default::default(),
            gen_after: Some(1),
            duration_ms: 1,
            violations: Vec::new(),
        }
    }

    fn query_record(body: serde_json::Value) -> OpRecord {
        query_record_with_response(
            body,
            json!({
                "results": [{
                    "id": "row",
                    "score": 1.0,
                    "attributes": null
                }]
            }),
        )
    }

    fn query_record_with_response(
        body: serde_json::Value,
        response: serde_json::Value,
    ) -> OpRecord {
        OpRecord {
            index: 1,
            wall_ms: 0,
            op: Op::Query {
                actor: ActorSel::ADMIN,
                ns: NS.to_string(),
                q: GeneratedQuery {
                    body,
                    class: QueryOracleClass::Membership {
                        consistency: ConsistencyLevel::Strong,
                    },
                    pattern_tags: vec!["fts".to_string()],
                },
                as_of: None,
            },
            method: "POST".to_string(),
            path: format!("/v1/namespaces/{NS}/query"),
            status: 200,
            response,
            outcome: "applied".to_string(),
            target_node: 0,
            execution: Default::default(),
            gen_after: Some(1),
            duration_ms: 1,
            violations: Vec::new(),
        }
    }

    fn batch_record(response: serde_json::Value) -> OpRecord {
        OpRecord {
            index: 1,
            wall_ms: 0,
            op: Op::BatchQuery {
                actor: ActorSel::ADMIN,
                ns: NS.to_string(),
                qs: vec![GeneratedQuery {
                    body: json!({
                        "sources": [{
                            "type": "ann",
                            "vector": [0.0, 0.0],
                            "nprobe": 1
                        }],
                        "fusion": { "type": "none" },
                        "top_k": 1,
                        "candidate_k": 1,
                        "consistency": "strong"
                    }),
                    class: QueryOracleClass::Membership {
                        consistency: ConsistencyLevel::Strong,
                    },
                    pattern_tags: vec!["batch".to_string()],
                }],
            },
            method: "POST".to_string(),
            path: format!("/v1/namespaces/{NS}/query/batch"),
            status: 200,
            response,
            outcome: "applied".to_string(),
            target_node: 0,
            execution: Default::default(),
            gen_after: Some(1),
            duration_ms: 1,
            violations: Vec::new(),
        }
    }

    fn grouping_query(top_k: usize) -> serde_json::Value {
        json!({
            "vector": [0.0, 0.0],
            "top_k": top_k,
            "consistency": "strong",
            "grouping": {
                "type": "field",
                "field": "group",
                "max_per_group": 2
            }
        })
    }

    fn grouped_response(group_count: usize, per_group: usize) -> serde_json::Value {
        let mut flat_results = Vec::new();
        let mut groups = Vec::new();
        for group_index in 0..group_count {
            let mut group_results = Vec::new();
            for result_index in 0..per_group {
                let id = format!("g{group_index}_{result_index}");
                let result = json!({
                    "id": id,
                    "score": (group_index * per_group + result_index) as f32,
                    "attributes": null
                });
                flat_results.push(result.clone());
                group_results.push(result);
            }
            groups.push(json!({
                "key": format!("g{group_index}"),
                "results": group_results
            }));
        }
        json!({
            "results": flat_results,
            "groups": groups
        })
    }

    #[test]
    fn i20_rejects_divergent_success_when_namespace_has_tainted_storage() {
        let model = model_with_record("row", None);
        let rec = fetch_record([9.0, 9.0]);
        let tainted_keys = BTreeSet::from(["ns/segments/cluster_0.bin".to_string()]);
        let corruption = CorruptionContext {
            tainted_keys: &tainted_keys,
            fault_window_active: true,
        };

        let violations =
            check_op_with_faults(&model, &rec, RunMode::Chaos, None, Some(&corruption));

        assert!(
            violations
                .iter()
                .any(|violation| violation.id == ViolationId::I20CorruptionSurfaced),
            "{violations:#?}"
        );
    }

    #[test]
    fn i20_accepts_model_consistent_success_with_tainted_storage() {
        let model = model_with_record("row", None);
        let rec = fetch_record([0.0, 0.0]);
        let tainted_keys = BTreeSet::from(["ns/segments/cluster_0.bin".to_string()]);
        let corruption = CorruptionContext {
            tainted_keys: &tainted_keys,
            fault_window_active: true,
        };

        let violations =
            check_op_with_faults(&model, &rec, RunMode::Chaos, None, Some(&corruption));

        assert!(
            violations
                .iter()
                .all(|violation| violation.id != ViolationId::I20CorruptionSurfaced),
            "{violations:#?}"
        );
    }

    #[test]
    fn i20_does_not_attribute_divergence_without_tainted_storage() {
        let model = model_with_record("row", None);
        let rec = fetch_record([9.0, 9.0]);
        let tainted_keys = BTreeSet::new();
        let corruption = CorruptionContext {
            tainted_keys: &tainted_keys,
            fault_window_active: false,
        };

        let violations =
            check_op_with_faults(&model, &rec, RunMode::Chaos, None, Some(&corruption));

        assert!(
            violations
                .iter()
                .all(|violation| violation.id != ViolationId::I20CorruptionSurfaced),
            "{violations:#?}"
        );
    }

    fn paginate_record(class: QueryOracleClass, response: serde_json::Value) -> OpRecord {
        OpRecord {
            index: 1,
            wall_ms: 0,
            op: Op::PaginateAll {
                actor: ActorSel::ADMIN,
                ns: NS.to_string(),
                q: GeneratedQuery {
                    body: json!({
                        "sources": [{
                            "type": "ann",
                            "vector": [0.0, 0.0],
                            "nprobe": 1
                        }],
                        "fusion": { "type": "none" },
                        "top_k": 2,
                        "candidate_k": 2,
                        "consistency": "strong"
                    }),
                    class,
                    pattern_tags: vec!["pagination".to_string()],
                },
                page_size: 1,
            },
            method: "POST".to_string(),
            path: format!("/v1/namespaces/{NS}/query"),
            status: 200,
            response,
            outcome: "applied".to_string(),
            target_node: 0,
            execution: Default::default(),
            gen_after: Some(1),
            duration_ms: 1,
            violations: Vec::new(),
        }
    }

    fn pagination_response(
        paged: &[(&str, f32)],
        big: &[(&str, f32)],
        shape_changed: bool,
    ) -> serde_json::Value {
        let pages = paged
            .iter()
            .enumerate()
            .map(|(idx, (id, score))| {
                let mut body = json!({
                    "results": [{
                        "id": id,
                        "score": score,
                        "attributes": null
                    }],
                    "scanned_fragments": if shape_changed && idx > 0 { 0 } else { 1 },
                    "scanned_segments": 1
                });
                if idx + 1 < paged.len() {
                    body.as_object_mut()
                        .expect("pagination body is object")
                        .insert("next_cursor".to_string(), json!("cursor"));
                }
                json!({
                    "status": 200,
                    "body": body
                })
            })
            .collect::<Vec<_>>();
        let big_results = big
            .iter()
            .map(|(id, score)| {
                json!({
                    "id": id,
                    "score": score,
                    "attributes": null
                })
            })
            .collect::<Vec<_>>();
        json!({
            "pages": pages,
            "big": {
                "status": 200,
                "body": {
                    "results": big_results,
                    "scanned_fragments": 1,
                    "scanned_segments": 1
                }
            }
        })
    }

    fn storage_error() -> serde_json::Value {
        json!({
            "code": "STORAGE_ERROR",
            "error": "a transient storage error occurred; please retry",
            "request_id": "req",
            "retryable": true,
            "status": 500
        })
    }

    #[test]
    fn i5_allows_matching_nested_errors_for_membership_queries() {
        let rec = batch_record(json!({
            "batch": {
                "results": [{
                    "ok": false,
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "error": "an internal error occurred",
                        "retryable": false,
                        "status": 500
                    },
                    "metadata": { "latency_ms": 1 }
                }]
            },
            "individual": [{
                "status": 500,
                "body": {
                    "code": "INTERNAL_ERROR",
                    "error": "an internal error occurred",
                    "request_id": "req",
                    "retryable": false,
                    "status": 500
                }
            }]
        }));

        let violations = check_i5_batch_equivalence(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i7_allows_hybrid_ann_rows_without_bm25_text() {
        let model = model_with_record("row", None);
        let rec = query_record(json!({
            "sources": [{
                "type": "ann",
                "vector": [0.0, 0.0],
                "nprobe": 1
            }, {
                "type": "bm25",
                "rank_by": ["body", "BM25", "needle"]
            }],
            "fusion": { "type": "rrf", "k": 60 },
            "top_k": 1,
            "consistency": "strong",
            "explain": "full"
        }));

        let violations = check_i7_fts_membership(&model, &rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i7_rejects_pure_bm25_rows_without_query_words() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "body".to_string(),
            AttributeValue::String("orange pear".to_string()),
        );
        let model = model_with_record("row", Some(attributes));
        let rec = query_record(json!({
            "rank_by": ["body", "BM25", "needle"],
            "top_k": 1,
            "consistency": "strong"
        }));

        let violations = check_i7_fts_membership(&model, &rec);

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I7FtsMembership);
        assert_eq!(
            violations[0].detail,
            "FTS query returned id whose body had none of the query words"
        );
    }

    #[test]
    fn i12_allows_grouped_flat_results_above_top_k() {
        let rec = query_record_with_response(grouping_query(4), grouped_response(4, 2));

        let violations = check_i12_structural_sanity(&Model::default(), &rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i12_rejects_group_count_above_top_k() {
        let rec = query_record_with_response(grouping_query(4), grouped_response(5, 2));

        let violations = check_i12_structural_sanity(&Model::default(), &rec);

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I12StructuralSanity);
        assert_eq!(
            violations[0].detail,
            "grouped query returned more groups than top_k"
        );
    }

    #[test]
    fn i6_allows_membership_pages_that_differ_from_big_query() {
        let rec = paginate_record(
            QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            },
            pagination_response(&[("a", 1.0), ("b", 2.0)], &[("a", 1.0), ("c", 2.0)], false),
        );

        let violations = check_i6_pagination_equivalence(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i11_accepts_paginateall_big_error_envelope() {
        let mut rec = paginate_record(
            QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            },
            json!({
                "pages": [{
                    "status": 200,
                    "body": {
                        "results": [{
                            "id": "a",
                            "score": 1.0,
                            "attributes": null
                        }]
                    }
                }],
                "big": {
                    "status": 500,
                    "body": storage_error()
                }
            }),
        );
        rec.status = 500;

        let violations = check_i11_error_envelope(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i11_accepts_paginateall_page_error_envelope() {
        let mut rec = paginate_record(
            QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            },
            json!({
                "pages": [
                    {
                        "status": 200,
                        "body": {
                            "results": [{
                                "id": "a",
                                "score": 1.0,
                                "attributes": null
                            }],
                            "next_cursor": "cursor"
                        }
                    },
                    {
                        "status": 500,
                        "body": storage_error()
                    }
                ],
                "big": {
                    "status": 200,
                    "body": {
                        "results": [{
                            "id": "a",
                            "score": 1.0,
                            "attributes": null
                        }]
                    }
                }
            }),
        );
        rec.status = 500;

        let violations = check_i11_error_envelope(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i11_accepts_batch_level_concurrency_limit_envelope() {
        let concurrency_limit = json!({
            "code": "CONCURRENCY_LIMIT",
            "error": "query concurrency limit reached, try again later",
            "request_id": "req",
            "retryable": true,
            "status": 503
        });
        let mut rec = batch_record(json!({
            "batch": concurrency_limit,
            "individual": [{
                "status": 503,
                "body": {
                    "code": "CONCURRENCY_LIMIT",
                    "error": "query concurrency limit reached, try again later",
                    "request_id": "req-individual",
                    "retryable": true,
                    "status": 503
                }
            }]
        }));
        rec.status = 503;

        let violations = check_i11_error_envelope(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i6_rejects_exact_pages_that_differ_from_big_query() {
        let rec = paginate_record(
            QueryOracleClass::ExactAnn {
                top_k: 2,
                consistency: ConsistencyLevel::Strong,
                filter: None,
            },
            pagination_response(&[("a", 1.0), ("b", 2.0)], &[("a", 1.0), ("c", 2.0)], false),
        );

        let violations = check_i6_pagination_equivalence(&rec);

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I6PaginationEquivalent);
        assert_eq!(
            violations[0].detail,
            "paged ids did not match big query ids"
        );
    }

    #[test]
    fn i6_allows_exact_id_mismatch_when_scan_shape_changes() {
        let rec = paginate_record(
            QueryOracleClass::ExactAnn {
                top_k: 2,
                consistency: ConsistencyLevel::Strong,
                filter: None,
            },
            pagination_response(&[("a", 1.0), ("b", 2.0)], &[("a", 1.0), ("c", 2.0)], true),
        );

        let violations = check_i6_pagination_equivalence(&rec);

        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn i23_aggregate_scan_finds_counts_facets_and_groups() {
        let leaks = security_response_aggregate_leaks(&json!({
            "count": 2,
            "facets": {"group": [{"value": "g1", "count": 2}]},
            "groups": [{"key": "g1", "count": 2}],
            "empty_buckets": {"buckets": []}
        }));

        assert!(leaks.contains("/count"));
        assert!(leaks.contains("/facets"));
        assert!(leaks.contains("/groups"));
        assert!(!leaks.contains("/empty_buckets/buckets"));
    }
}
