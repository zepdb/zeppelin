mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use common::server::{
    cleanup_ns, client_with_bearer, expired_test_entitlements, start_test_server_with_config,
    start_test_server_with_entitlements,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use zeppelin::config::{ApiKeyConfig, Config};
use zeppelin::namespace::manager::{
    CompactionHealth, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
};
use zeppelin::security::Entitlements;
use zeppelin::types::{DistanceMetric, IndexType};

const FIXTURE_VERSION: &str = "v0.3.0";
const CONTRACT_FORBIDDEN_BEARER: &str =
    "zpk1_contract_forbidden.AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const CONTRACT_PRIMARY_KEY_ID: &str = "zpk1_contract_primary";
const CONTRACT_PRIMARY_API_KEY: &str =
    "zpk1_contract_primary.AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const CONTRACT_ROTATED_KEY_ID: &str = "zpk1_contract_rotated";
const CONTRACT_ROTATED_API_KEY: &str =
    "zpk1_contract_rotated.AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI";

const ROUTED_OPERATIONS: &[(&str, &str)] = &[
    ("get", "/healthz"),
    ("get", "/readyz"),
    ("get", "/metrics"),
    ("get", "/v1/config/query"),
    ("put", "/v1/config/query"),
    ("patch", "/v1/config/query"),
    ("get", "/v1/security/principals"),
    ("post", "/v1/security/principals"),
    ("get", "/v1/security/keys"),
    ("post", "/v1/security/keys"),
    ("post", "/v1/security/keys/{key_id}/rotate"),
    ("delete", "/v1/security/keys/{key_id}"),
    ("get", "/v1/security/grants"),
    ("post", "/v1/security/grants"),
    ("delete", "/v1/security/grants"),
    ("get", "/v1/security/policy"),
    ("post", "/v1/security/tokens"),
    ("get", "/v1/security/preservation"),
    ("post", "/v1/security/preservation"),
    ("post", "/v1/security/preservation/{lock_id}/release"),
    ("post", "/v1/namespaces"),
    ("get", "/v1/namespaces/{ns}"),
    ("delete", "/v1/namespaces/{ns}"),
    ("get", "/v1/namespaces/{ns}/branches"),
    ("post", "/v1/namespaces/{ns}/branches"),
    ("get", "/v1/namespaces/{ns}/snapshots"),
    ("put", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("get", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("delete", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("post", "/v1/namespaces/{ns}/clone"),
    ("patch", "/v1/namespaces/{ns}/index_config"),
    ("put", "/v1/namespaces/{ns}/embedding_profile"),
    ("post", "/v1/namespaces/{ns}/compact"),
    ("get", "/v1/namespaces/{ns}/compact/status"),
    ("post", "/v1/namespaces/{ns}/hydrate"),
    ("post", "/v1/namespaces/{ns}/vectors"),
    ("delete", "/v1/namespaces/{ns}/vectors"),
    ("post", "/v1/namespaces/{ns}/vectors/get"),
    ("post", "/v1/namespaces/{ns}/query"),
    ("post", "/v1/namespaces/{ns}/query/batch"),
];

// OpenAPI describes this build-time optional route even when the current test
// binary does not enable the `profiling` feature.
const FEATURE_GATED_ROUTED_OPERATIONS: &[(&str, &str)] = &[("get", "/debug/pprof/cpu")];

const FIXTURE_CASES: &[&str] = &[
    "unauthenticated_401",
    "forbidden_403",
    "feature_not_licensed_403",
    "license_expired_403",
    "readyz_gated_401",
    "security_create_principal",
    "security_list_principals",
    "security_create_key",
    "security_rotate_key",
    "security_revoke_key",
    "security_list_keys",
    "security_create_grant",
    "security_list_grants",
    "security_delete_grant",
    "security_mint_token",
    "security_get_policy",
    "security_create_preservation",
    "security_list_preservation",
    "security_release_preservation",
    "error_constraint_violation_403",
    "error_cursor_policy_stale_400",
    "create_namespace",
    "get_namespace",
    "patch_index_config",
    "get_query_config",
    "patch_query_config",
    "upsert_vectors",
    "get_vectors",
    "delete_vectors",
    "query_ann",
    "query_by_id",
    "query_hybrid_rrf",
    "query_weighted_fusion",
    "query_vector_rerank",
    "query_bm25_rerank",
    "query_cursor_page1",
    "query_cursor_page2",
    "query_grouping",
    "query_facets",
    "query_explain_plan",
    "query_explain_full",
    "batch_query",
    "compact_status",
    "compact_namespace_accepted",
    "hydrate_namespace_disabled",
    "delete_namespace_accepted",
    "error_validation_400",
    "error_not_found_404",
    "error_conflict_409",
    "error_deleting_410",
    "error_payload_too_large_413",
    "error_not_implemented_501",
];

#[derive(Debug, Clone)]
struct Fixture {
    name: &'static str,
    method: &'static str,
    path: String,
    status: u16,
    request: Value,
    response: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FixtureManifest {
    version: String,
    generated_by: String,
    cases: Vec<FixtureManifestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FixtureManifestCase {
    name: String,
    method: String,
    path: String,
    status: u16,
    request: String,
    response: String,
}

#[test]
fn openapi_documents_exact_routed_surface() {
    let documented = documented_operations(include_str!("../api/zeppelin-api.yaml"));
    let routed = ROUTED_OPERATIONS
        .iter()
        .chain(FEATURE_GATED_ROUTED_OPERATIONS.iter())
        .map(|(method, path)| ((*method).to_string(), (*path).to_string()))
        .collect::<BTreeSet<_>>();

    let missing = routed.difference(&documented).cloned().collect::<Vec<_>>();
    let undocumented = documented.difference(&routed).cloned().collect::<Vec<_>>();

    assert!(
        missing.is_empty() && undocumented.is_empty(),
        "OpenAPI route drift\nmissing from api.yaml: {missing:#?}\nnot routed: {undocumented:#?}"
    );
}

#[test]
fn branching_release_contract_is_gated_and_has_no_merge_surface() {
    let api = include_str!("../api/zeppelin-api.yaml");
    assert!(api.contains("/v1/namespaces/{ns}/branches:"));
    assert!(!api.contains("/merge"));
    assert!(!api.contains("/rebase"));
    assert!(!api.contains("/diff"));
    assert!(!zeppelin::config::BranchingConfig::default().enabled);
}

#[test]
fn namespace_delete_conflict_documents_filtered_children_without_an_exact_total() {
    let api = include_str!("../api/zeppelin-api.yaml");
    let list = operation_block(api, "get", "/v1/namespaces/{ns}/branches");
    assert!(list.contains("Disclosure is checked before target metadata is read"));
    assert!(list.contains("branch_integrity_error"));

    let delete = operation_block(api, "delete", "/v1/namespaces/{ns}");
    assert!(delete.contains("#/components/responses/NamespaceDeleteConflict"));
    let response = component_schema_block(api, "NamespaceDeleteConflict");
    assert!(response.contains("oneOf:"));
    assert!(response.contains("#/components/schemas/NamespaceHasLiveBranchesErrorResponse"));
    assert!(response.contains("#/components/schemas/NamespaceDeleteGenericConflictErrorResponse"));
    assert!(!response.contains("#/components/schemas/ErrorResponse"));

    let conflict = component_schema_block(api, "NamespaceHasLiveBranchesErrorResponse");
    assert!(conflict.contains("additionalProperties: false"));
    assert!(conflict.contains(
        "required: [code, error, status, retryable, visible_children, has_additional_children]"
    ));
    assert!(conflict.contains("enum: [namespace_has_live_branches, branch_has_live_children]"));
    assert!(conflict.contains("const: 409"));
    assert!(conflict.contains("const: false"));
    assert!(conflict.contains("#/components/schemas/VisibleBranchChild"));
    assert!(conflict.contains("denied or truncated children"));
    assert!(!conflict.contains("child_count"));
    assert!(!conflict.contains("total_children"));
    assert!(!conflict.contains("total:"));

    let generic = component_schema_block(api, "NamespaceDeleteGenericConflictErrorResponse");
    assert!(generic.contains("additionalProperties: false"));
    assert!(generic.contains("required: [code, error, status, retryable]"));
    assert!(generic.contains("enum: [preservation_locked, CONFLICT_RETRY]"));

    let child = component_schema_block(api, "VisibleBranchChild");
    assert!(child.contains("additionalProperties: false"));
    assert!(child.contains("required: [namespace, branch_id]"));

    let errors = component_schema_block(api, "ErrorResponse");
    assert!(errors.contains("namespace_has_live_branches"));
    assert!(errors.contains("branch_has_live_children"));
    assert!(errors.contains("branch_integrity_error"));
}

#[test]
fn openapi_documents_bearer_security_for_every_protected_operation() {
    let api = include_str!("../api/zeppelin-api.yaml");
    assert!(
        api.contains("\nsecurity:\n  - bearerAuth: []\n"),
        "OpenAPI must make bearerAuth the default security requirement"
    );
    assert!(
        api.contains(
            "  securitySchemes:\n    bearerAuth:\n      type: http\n      scheme: bearer\n"
        ),
        "OpenAPI must define bearerAuth as an HTTP bearer scheme"
    );

    for (method, path) in ROUTED_OPERATIONS
        .iter()
        .chain(FEATURE_GATED_ROUTED_OPERATIONS.iter())
    {
        let statuses = documented_statuses(api, method, path);
        if *path == "/healthz" {
            let operation = operation_block(api, method, path);
            assert!(
                operation.contains("      security: []"),
                "GET /healthz must explicitly override global authentication"
            );
            assert!(!statuses.contains(&401));
            assert!(!statuses.contains(&403));
            continue;
        }

        assert!(statuses.contains(&401), "{method} {path} must document 401");
        assert!(statuses.contains(&403), "{method} {path} must document 403");
        let operation = operation_block(api, method, path);
        assert!(
            operation.contains(
                "        \"401\":\n          $ref: \"#/components/responses/UnauthorizedError\""
            ),
            "{method} {path} must use the canonical 401 response"
        );
        assert!(
            operation.contains(
                "        \"403\":\n          $ref: \"#/components/responses/ForbiddenError\""
            ),
            "{method} {path} must use the canonical 403 response"
        );
    }

    let readiness = operation_block(api, "get", "/readyz");
    assert!(
        readiness.contains("readyz_public"),
        "readiness docs must describe the explicit public-readiness override"
    );
    assert!(
        readiness.contains("protected by default"),
        "readiness docs must describe its default gated semantics"
    );

    let profiling = operation_block(api, "get", "/debug/pprof/cpu");
    assert!(
        profiling.contains("x-zeppelin-feature: profiling")
            && profiling.contains("available only when Zeppelin is built"),
        "profiling docs must identify the build-time feature gate"
    );
}

#[test]
fn openapi_security_admin_contract_is_exact_and_redacted() {
    let api = include_str!("../api/zeppelin-api.yaml");
    let expected_statuses: &[(&str, &str, &[u16])] = &[
        ("get", "/v1/security/principals", &[200, 401, 403, 429]),
        (
            "post",
            "/v1/security/principals",
            &[201, 400, 401, 403, 409, 415, 422, 429],
        ),
        ("get", "/v1/security/keys", &[200, 401, 403, 429]),
        (
            "post",
            "/v1/security/keys",
            &[201, 400, 401, 403, 404, 409, 415, 422, 429],
        ),
        (
            "post",
            "/v1/security/keys/{key_id}/rotate",
            &[201, 400, 401, 403, 404, 409, 415, 422, 429],
        ),
        (
            "delete",
            "/v1/security/keys/{key_id}",
            &[200, 400, 401, 403, 404, 409, 429],
        ),
        ("get", "/v1/security/grants", &[200, 401, 403, 429]),
        (
            "post",
            "/v1/security/grants",
            &[201, 400, 401, 403, 404, 409, 415, 422, 429],
        ),
        (
            "delete",
            "/v1/security/grants",
            &[200, 400, 401, 403, 404, 409, 415, 422, 429],
        ),
        ("get", "/v1/security/policy", &[200, 401, 403, 429]),
        (
            "post",
            "/v1/security/tokens",
            &[201, 400, 401, 403, 415, 422, 429],
        ),
        (
            "get",
            "/v1/security/preservation",
            &[200, 401, 403, 429, 503],
        ),
        (
            "post",
            "/v1/security/preservation",
            &[201, 400, 401, 403, 409, 415, 422, 429, 500],
        ),
        (
            "post",
            "/v1/security/preservation/{lock_id}/release",
            &[200, 400, 401, 403, 404, 409, 429, 500],
        ),
    ];
    for (method, path, expected) in expected_statuses {
        let actual = documented_statuses(api, method, path);
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(actual, expected, "unexpected statuses for {method} {path}");
    }

    for (method, path) in [
        ("post", "/v1/security/preservation"),
        ("post", "/v1/security/preservation/{lock_id}/release"),
        ("delete", "/v1/namespaces/{ns}"),
    ] {
        assert!(
            operation_block(api, method, path).contains(
                "        \"500\":\n          $ref: \"#/components/responses/AuditUnavailable\""
            ),
            "{method} {path} must use the canonical audit-unavailable response"
        );
    }

    let key_view = component_schema_block(api, "SecurityKeyView");
    assert!(key_view.contains("additionalProperties: false"));
    assert!(!key_view.contains("sha256_hex"));
    assert!(!key_view.contains("api_key"));

    for schema in [
        "SecurityGlobalGrantScope",
        "SecurityNamespaceGrantScope",
        "SecurityAllGrantActions",
        "SecuritySelectedGrantActions",
    ] {
        let block = component_schema_block(api, schema);
        assert!(
            block.contains("additionalProperties: false"),
            "{schema} must reject unknown fields"
        );
        assert!(
            block.contains("const:"),
            "{schema} must carry an exact serde-compatible tag"
        );
    }

    let issue = component_schema_block(api, "IssueSecurityKeyResponse");
    let rotate = component_schema_block(api, "RotateSecurityKeyResponse");
    assert!(issue.contains("api_key:"));
    assert!(rotate.contains("api_key:"));

    let errors = component_schema_block(api, "ErrorResponse");
    assert!(errors.contains("- constraint_violation"));
    assert!(errors.contains("- cursor_policy_stale"));

    let all_actions = component_schema_block(api, "SecurityAllGrantActions");
    assert!(all_actions.contains("AttributeAdmin"));
    assert!(all_actions.contains("are excluded"));
    assert!(all_actions.contains("CredentialDelegate"));
    assert!(all_actions.contains("NamespaceFork"));
    assert!(all_actions.contains("SecurityAdminWrite"));
    let actions = component_schema_block(api, "SecurityAction");
    assert!(actions.contains("NamespaceFork"));
    let grant = component_schema_block(api, "SecurityGrantMutationRequest");
    for constraint in [
        "mandatory_filter:",
        "field_mask:",
        "write_constraints:",
        "require_approval:",
    ] {
        assert!(
            grant.contains(constraint),
            "grant schema missing {constraint}"
        );
    }
    let removal = component_schema_block(api, "SecurityGrantRemovalRequest");
    assert!(!removal.contains("mandatory_filter:"));
    assert!(!removal.contains("field_mask:"));
    assert!(!removal.contains("write_constraints:"));
    assert!(!removal.contains("require_approval:"));

    let mint = component_schema_block(api, "MintDelegatedTokenRequest");
    for contract in [
        "additionalProperties: false",
        "required: [actions, namespaces, purpose, expires_in_secs]",
        "minItems: 1",
        "maxLength: 512",
        "delegated_token_max_ttl_secs",
    ] {
        assert!(mint.contains(contract), "mint schema missing {contract}");
    }
    let minted = component_schema_block(api, "MintDelegatedTokenResponse");
    assert!(minted.contains("required: [policy_version, token_id, token, expires_at]"));
    assert!(minted.contains("returned by a later read"));
    let delegated_actions = component_schema_block(api, "DelegatedSecurityAction");
    assert!(delegated_actions.contains("NamespaceFork"));
    assert!(delegated_actions.contains("VectorDelete"));
    assert!(!delegated_actions.contains("SecurityAdminWrite"));
    assert!(!delegated_actions.contains("CredentialDelegate"));
    for (method, path) in [
        ("post", "/v1/security/principals"),
        ("post", "/v1/security/keys"),
        ("delete", "/v1/security/keys/{key_id}"),
        ("post", "/v1/security/keys/{key_id}/rotate"),
        ("post", "/v1/security/grants"),
        ("delete", "/v1/security/grants"),
        ("delete", "/v1/namespaces/{ns}"),
        ("delete", "/v1/namespaces/{ns}/snapshots/{name}"),
        ("delete", "/v1/namespaces/{ns}/vectors"),
    ] {
        assert!(
            operation_block(api, method, path).contains("#/components/parameters/ZeppelinApproval"),
            "{method} {path} must document its optional approval credential"
        );
    }
    assert!(api.contains("name: X-Zeppelin-Approval"));
    assert!(api.contains("must be redacted from"));

    let policy_filter = component_schema_block(api, "SecurityPolicyFilter");
    assert!(policy_filter.contains("publication-time fail-closed"));
    let attribute_value = component_schema_block(api, "AttributeValue");
    assert!(attribute_value.contains("anyOf:"));
    assert!(!attribute_value.contains("oneOf:"));
    let policy_range = component_schema_block(api, "SecurityPolicyFilterRange");
    for bound in [
        "required: [gte]",
        "required: [lte]",
        "required: [gt]",
        "required: [lt]",
    ] {
        assert!(
            policy_range.contains(bound),
            "policy range schema missing {bound}"
        );
    }
    for schema in [
        "SecurityPolicyFilterIn",
        "SecurityPolicyFilterNotIn",
        "SecurityPolicyFilterContainsAllTokens",
        "SecurityPolicyFilterContainsTokenSequence",
        "SecurityPolicyFilterAnd",
        "SecurityPolicyFilterOr",
    ] {
        assert!(
            component_schema_block(api, schema).contains("minItems: 1"),
            "{schema} must reject an empty policy-filter list"
        );
    }
}

#[test]
fn openapi_phase_four_constraint_contract_is_strict_and_redacted() {
    let api = include_str!("../api/zeppelin-api.yaml");

    for schema in ["SecurityGrant", "SecurityGrantMutationRequest"] {
        let block = component_schema_block(api, schema);
        assert!(
            block.contains("$ref: \"#/components/schemas/SecurityPolicyFilter\""),
            "{schema}.mandatory_filter must use the recursively strict policy schema"
        );
        for constraint in ["mandatory_filter:", "field_mask:", "write_constraints:"] {
            assert!(block.contains(constraint), "{schema} missing {constraint}");
        }
    }

    let policy_filter = component_schema_block(api, "SecurityPolicyFilter");
    for variant in [
        "SecurityPolicyFilterEq",
        "SecurityPolicyFilterNotEq",
        "SecurityPolicyFilterRange",
        "SecurityPolicyFilterIn",
        "SecurityPolicyFilterNotIn",
        "SecurityPolicyFilterContains",
        "SecurityPolicyFilterContainsAllTokens",
        "SecurityPolicyFilterContainsTokenSequence",
        "SecurityPolicyFilterAnd",
        "SecurityPolicyFilterOr",
        "SecurityPolicyFilterNot",
    ] {
        assert!(
            policy_filter.contains(&format!("#/components/schemas/{variant}")),
            "policy-filter union missing {variant}"
        );
    }

    for schema in [
        "FilterEq",
        "FilterNotEq",
        "FilterRange",
        "FilterIn",
        "FilterNotIn",
        "FilterContains",
        "FilterContainsAllTokens",
        "FilterContainsTokenSequence",
        "FilterAnd",
        "FilterOr",
        "FilterNot",
    ] {
        assert!(
            component_schema_block(api, schema).contains("additionalProperties: false"),
            "{schema} must match Filter's deny_unknown_fields runtime contract"
        );
    }

    for (schema, base) in [
        ("SecurityPolicyFilterEq", "FilterEq"),
        ("SecurityPolicyFilterNotEq", "FilterNotEq"),
        ("SecurityPolicyFilterRange", "FilterRange"),
        ("SecurityPolicyFilterIn", "FilterIn"),
        ("SecurityPolicyFilterNotIn", "FilterNotIn"),
        ("SecurityPolicyFilterContains", "FilterContains"),
        (
            "SecurityPolicyFilterContainsAllTokens",
            "FilterContainsAllTokens",
        ),
        (
            "SecurityPolicyFilterContainsTokenSequence",
            "FilterContainsTokenSequence",
        ),
    ] {
        let block = component_schema_block(api, schema);
        assert!(
            block.contains(&format!("#/components/schemas/{base}")),
            "{schema} must retain the existing public Filter wire shape"
        );
        assert!(
            block.contains(r#"pattern: "\\S""#),
            "{schema} must reject blank policy field names"
        );
    }

    let range = component_schema_block(api, "SecurityPolicyFilterRange");
    for bound in ["gte", "lte", "gt", "lt"] {
        assert!(
            range.contains(&format!("required: [{bound}]")),
            "policy range must accept {bound} as a nonempty bound"
        );
    }
    for schema in ["SecurityPolicyFilterIn", "SecurityPolicyFilterNotIn"] {
        assert!(
            component_schema_block(api, schema).contains("minItems: 1"),
            "{schema} must reject an empty membership list"
        );
    }
    for schema in [
        "SecurityPolicyFilterContainsAllTokens",
        "SecurityPolicyFilterContainsTokenSequence",
    ] {
        let block = component_schema_block(api, schema);
        assert!(block.contains("minItems: 1"));
        assert!(block.contains(r#"pattern: "\\S""#));
        assert!(block.contains("default production tokenizer"));
        assert!(block.contains("at least one analyzed token"));
    }
    for schema in ["SecurityPolicyFilterAnd", "SecurityPolicyFilterOr"] {
        let block = component_schema_block(api, schema);
        assert!(block.contains("minItems: 1"));
        assert!(block.contains("#/components/schemas/SecurityPolicyFilter"));
    }
    assert!(component_schema_block(api, "SecurityPolicyFilterNot")
        .contains("#/components/schemas/SecurityPolicyFilter"));

    let field_mask = component_schema_block(api, "SecurityFieldMask");
    for rule in ["minItems: 1", "uniqueItems: true", r#"pattern: "\\S""#] {
        assert!(
            field_mask.contains(rule),
            "field-mask schema missing {rule}"
        );
    }
    assert!(field_mask.contains("cannot be referenced by caller filters"));
    assert!(field_mask.contains("ranking expressions"));
    assert!(field_mask.contains("value oracles"));
    let writes = component_schema_block(api, "SecurityWriteConstraints");
    for rule in [
        "anyOf:",
        "minProperties: 1",
        "minItems: 1",
        "AttributeAdmin",
    ] {
        assert!(
            writes.contains(rule),
            "write-constraint schema missing {rule}"
        );
    }

    let delete_request = component_schema_block(api, "DeleteVectorsRequest");
    assert!(delete_request.contains("DeleteVectorsByIdsRequest"));
    assert!(delete_request.contains("DeleteVectorsByFilterRequest"));
    for schema in ["DeleteVectorsByIdsRequest", "DeleteVectorsByFilterRequest"] {
        assert!(
            component_schema_block(api, schema).contains("additionalProperties: false"),
            "{schema} must preserve the ids/filter XOR"
        );
    }
    let upsert = operation_block(api, "post", "/v1/namespaces/{ns}/vectors");
    assert!(upsert.contains("stamps"));
    assert!(upsert.contains("forbid_set"));
    assert!(upsert.contains("before a"));
    assert!(upsert.contains("WAL fragment is published"));
    assert!(upsert.contains("out-of-slice ID"));
    assert!(upsert.contains("server-owned"));
    assert!(upsert.contains("update-only"));
    assert!(upsert.contains("UpsertVectorsResponse"));
    let upsert_request = component_schema_block(api, "UpsertVectorsRequest");
    assert!(upsert_request.contains("#/components/schemas/UpsertVectorInput"));
    let upsert_input = component_schema_block(api, "UpsertVectorInput");
    assert!(upsert_input.contains("required: [values]"));
    assert!(upsert_input.contains("server-owned"));
    assert!(upsert_input.contains("update-only"));
    let upsert_response = component_schema_block(api, "UpsertVectorsResponse");
    assert!(upsert_response.contains("generated_ids"));
    assert!(upsert_response.contains("#/components/schemas/GeneratedVectorId"));
    let generated_id = component_schema_block(api, "GeneratedVectorId");
    assert!(generated_id.contains("required: [index, id]"));
    let msgpack_upsert = component_schema_block(api, "MessagePackUpsertRequest");
    assert!(msgpack_upsert.contains("server-owned"));
    assert!(msgpack_upsert.contains("update-only"));
    let delete = operation_block(api, "delete", "/v1/namespaces/{ns}/vectors");
    assert!(delete.contains("outside the permitted row slice"));
    assert!(delete.contains("nonexistent IDs"));

    let explain = component_schema_block(api, "QueryExplainPlan");
    assert!(explain.contains("- policy_filter_applied"));
    assert!(explain.contains("The predicate itself is never exposed"));

    let query = operation_block(api, "post", "/v1/namespaces/{ns}/query");
    assert!(query.contains("debug: true"));
    assert!(query.contains("constraint_violation"));
    assert!(query.contains("Row-scoped responses report zero"));
    assert!(query.contains("physical scan"));
    assert!(query.contains("counters"));
    let query_request = component_schema_block(api, "QueryRequest");
    assert!(query_request.contains("debug: true"));
    assert!(query_request.contains("constraint_violation"));
    assert!(
        component_schema_block(api, "BatchQueryRequest").contains("additionalProperties: false"),
        "BatchQueryRequest must match its deny_unknown_fields runtime contract"
    );
    let query_response = component_schema_block(api, "QueryResponse");
    assert_eq!(
        query_response
            .matches("cross-slice activity oracle")
            .count(),
        2,
        "both physical scan counters must document policy-scoped redaction"
    );

    let fetch = operation_block(api, "post", "/v1/namespaces/{ns}/vectors/get");
    assert!(fetch.contains("no existence distinction"));
    let fetch_response = component_schema_block(api, "GetVectorsResponse");
    assert!(fetch_response.contains("These cases are intentionally"));
    assert!(fetch_response.contains("indistinguishable and preserve request order"));
    assert!(api.contains("current policy constraints always apply"));
    assert!(api.contains("an older manifest never restores older access"));
    assert!(api.contains("every continuation must"));
    assert!(api.contains("repeat the same `as_of` value"));

    let clone = operation_block(api, "post", "/v1/namespaces/{ns}/clone");
    assert!(clone.contains("Cloning never copies or"));
    assert!(clone.contains("creates a security grant for the target namespace"));
    for contract in [
        "NamespaceRead",
        "NamespaceCreate",
        "unconstrained",
        "same policy version",
        "policy-wide",
        "source-to-target dominance",
    ] {
        assert!(clone.contains(contract), "clone docs missing {contract}");
    }

    let cursor = component_schema_block(api, "CursorSpec");
    for contract in [
        "opaque",
        "authenticated with HMAC-SHA256",
        "result-affecting query shape, and issuing",
        "policy version. Consuming a token",
        "cursor_policy_stale",
        "restart from the first page",
    ] {
        assert!(cursor.contains(contract), "cursor docs missing {contract}");
    }
}

#[test]
fn contract_fixture_inventory_is_complete() {
    if std::env::var("UPDATE_CONTRACT_FIXTURES").as_deref() == Ok("1") {
        return;
    }
    let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contract")
        .join("fixtures")
        .join(FIXTURE_VERSION);
    assert!(
        fixture_root.is_dir(),
        "fixture directory missing: {}",
        fixture_root.display()
    );
    assert!(
        fixture_root.join("manifest.json").is_file(),
        "fixture manifest missing"
    );

    for case in FIXTURE_CASES {
        let req = fixture_root.join(format!("{case}.req.json"));
        let resp = fixture_root.join(format!("{case}.resp.json"));
        assert!(req.is_file(), "request fixture missing: {}", req.display());
        assert!(
            resp.is_file(),
            "response fixture missing: {}",
            resp.display()
        );
    }
}

#[tokio::test]
async fn contract_fixtures_match_real_engine_output() {
    let fixtures = build_contract_fixtures().await;
    assert_eq!(
        fixtures.len(),
        FIXTURE_CASES.len(),
        "fixture builder and inventory diverged"
    );

    let names = fixtures
        .iter()
        .map(|fixture| fixture.name)
        .collect::<BTreeSet<_>>();
    let expected = FIXTURE_CASES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(names, expected, "fixture builder missed a curated case");

    let fixture_security_operations = fixtures
        .iter()
        .filter(|fixture| fixture.name.starts_with("security_"))
        .map(|fixture| (fixture.method.to_string(), fixture.openapi_path()))
        .collect::<BTreeSet<_>>();
    let routed_security_operations = ROUTED_OPERATIONS
        .iter()
        .filter(|(_, path)| path.starts_with("/v1/security/"))
        .map(|(method, path)| ((*method).to_string(), (*path).to_string()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        fixture_security_operations, routed_security_operations,
        "security-admin fixtures must cover every routed operation exactly once"
    );

    let api = include_str!("../api/zeppelin-api.yaml");
    for fixture in &fixtures {
        assert_operation_status_documented(api, fixture);
        assert_response_contract_shape(fixture);
    }

    write_or_compare_fixtures(&fixtures);
}

fn documented_operations(api: &str) -> BTreeSet<(String, String)> {
    let mut operations = BTreeSet::new();
    let mut in_paths = false;
    let mut current_path: Option<String> = None;

    for line in api.lines() {
        if line == "paths:" {
            in_paths = true;
            continue;
        }
        if line == "components:" {
            break;
        }
        if !in_paths {
            continue;
        }
        if let Some(path) = line
            .strip_prefix("  /")
            .and_then(|rest| rest.strip_suffix(':'))
        {
            current_path = Some(format!("/{path}"));
            continue;
        }
        let Some(path) = current_path.as_ref() else {
            continue;
        };
        let Some(method) = line
            .strip_prefix("    ")
            .and_then(|rest| rest.strip_suffix(':'))
        else {
            continue;
        };
        if matches!(method, "get" | "post" | "put" | "patch" | "delete") {
            operations.insert((method.to_string(), path.clone()));
        }
    }

    operations
}

async fn build_contract_fixtures() -> Vec<Fixture> {
    let mut config = Config::default();
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 4;
    config.indexing.max_nprobe = 32;
    config.server.default_top_k = 3;
    config.server.max_top_k = 100;
    config.server.max_query_batch_size = 2;
    config.security.set_cursor_hmac_key_hex("55".repeat(32));
    config.security.api_keys.push(ApiKeyConfig {
        key_id: "zpk1_contract_forbidden".to_string(),
        name: "contract-forbidden".to_string(),
        sha256_hex: "56d5fa7333f6d747db42c239407e5da4c32f4c79f35d092b134fd35a402d9c5c".to_string(),
        actions: vec!["SystemRead".to_string()],
        namespaces: vec!["*".to_string()],
        expires_at: None,
    });
    config
        .validate()
        .expect("contract fixture security config must satisfy the boot contract");

    let (base_url, harness, _cache, cache_dir, admin_bearer) =
        start_test_server_with_config(Some(config)).await;
    let client = client_with_bearer(&admin_bearer);
    let unauthenticated_client = reqwest::Client::new();
    let forbidden_client = client_with_bearer(CONTRACT_FORBIDDEN_BEARER);

    let main_ns = format!("{}-contract-main", harness.prefix);
    let compact_ns = format!("{}-contract-compact", harness.prefix);
    let delete_ns = format!("{}-contract-delete", harness.prefix);
    let conflict_ns = format!("{}-contract-conflict", harness.prefix);
    let deleting_ns = format!("{}-contract-deleting", harness.prefix);
    let missing_ns = format!("{}-contract-missing", harness.prefix);

    let replacements = vec![
        (main_ns.clone(), "contract-main".to_string()),
        (compact_ns.clone(), "contract-compact".to_string()),
        (delete_ns.clone(), "contract-delete".to_string()),
        (conflict_ns.clone(), "contract-conflict".to_string()),
        (deleting_ns.clone(), "contract-deleting".to_string()),
        (missing_ns.clone(), "contract-missing".to_string()),
    ];

    let mut fixtures =
        security_error_fixtures(&unauthenticated_client, &forbidden_client, &base_url).await;
    fixtures.extend(entitlement_error_fixtures().await);
    fixtures.extend(security_admin_fixtures(&client, &base_url).await);

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "create_namespace",
            "post",
            "/v1/namespaces",
            "/v1/namespaces",
            201,
            json!({
                "name": main_ns,
                "dimensions": 2,
                "distance_metric": "euclidean",
                "full_text_search": {"title": {}}
            }),
            json!({
                "name": "contract-main",
                "dimensions": 2,
                "distance_metric": "euclidean",
                "full_text_search": {"title": {}}
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_query_config",
            "get",
            "/v1/config/query",
            "/v1/config/query",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "patch_query_config",
            "patch",
            "/v1/config/query",
            "/v1/config/query",
            200,
            json!({
                "default_top_k": 3,
                "cost_latency_profile": "low_latency"
            }),
            json!({
                "default_top_k": 3,
                "cost_latency_profile": "low_latency"
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "patch_index_config",
            "patch",
            &format!("/v1/namespaces/{main_ns}/index_config"),
            "/v1/namespaces/contract-main/index_config",
            202,
            json!({
                "nlist": 4,
                "quantization": "scalar",
                "bitmap_index": true
            }),
            json!({
                "nlist": 4,
                "quantization": "scalar",
                "bitmap_index": true
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "upsert_vectors",
            "post",
            &format!("/v1/namespaces/{main_ns}/vectors"),
            "/v1/namespaces/contract-main/vectors",
            200,
            main_vectors(),
            main_vectors_canonical(),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_namespace",
            "get",
            &format!("/v1/namespaces/{main_ns}"),
            "/v1/namespaces/contract-main",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_vectors",
            "post",
            &format!("/v1/namespaces/{main_ns}/vectors/get"),
            "/v1/namespaces/contract-main/vectors/get",
            200,
            json!({
                "ids": ["seed", "near", "missing"],
                "include_vector": true,
                "include_attributes": true,
                "attribute_fields": ["category", "title"],
                "consistency": "strong"
            }),
            json!({
                "ids": ["seed", "near", "missing"],
                "include_vector": true,
                "include_attributes": true,
                "attribute_fields": ["category", "title"],
                "consistency": "strong"
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.extend(query_fixtures(&client, &base_url, &main_ns, &replacements).await);
    fixtures
        .extend(phase4_security_error_fixtures(&client, &base_url, &main_ns, &replacements).await);

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "delete_vectors",
            "delete",
            &format!("/v1/namespaces/{main_ns}/vectors"),
            "/v1/namespaces/contract-main/vectors",
            204,
            json!({"ids": ["other"]}),
            json!({"ids": ["other"]}),
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &compact_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    send_json(
        &client,
        &base_url,
        "post",
        &format!("/v1/namespaces/{compact_ns}/vectors"),
        "setup_compact_vectors",
        json!({
            "vectors": [{"id": "compact-a", "values": [0.0, 0.0]}]
        }),
    )
    .await;

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "compact_status",
            "get",
            &format!("/v1/namespaces/{compact_ns}/compact/status"),
            "/v1/namespaces/contract-compact/compact/status",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "compact_namespace_accepted",
            "post",
            &format!("/v1/namespaces/{compact_ns}/compact"),
            "/v1/namespaces/contract-compact/compact",
            202,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "hydrate_namespace_disabled",
            "post",
            &format!("/v1/namespaces/{main_ns}/hydrate"),
            "/v1/namespaces/contract-main/hydrate",
            409,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &delete_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "delete_namespace_accepted",
            "delete",
            &format!("/v1/namespaces/{delete_ns}"),
            "/v1/namespaces/contract-delete",
            202,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &conflict_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    seed_deleting_namespace(&harness.store, &deleting_ns).await;

    fixtures.extend(
        error_fixtures(
            &client,
            &base_url,
            &main_ns,
            &conflict_ns,
            &deleting_ns,
            &missing_ns,
            &replacements,
        )
        .await,
    );

    for ns in [
        &main_ns,
        &compact_ns,
        &delete_ns,
        &conflict_ns,
        &deleting_ns,
    ] {
        cleanup_ns(&harness.store, ns).await;
    }
    harness.cleanup().await;
    drop(cache_dir);

    fixtures
}

async fn entitlement_error_fixtures() -> Vec<Fixture> {
    let request = json!({
        "principal_id": "zpk1_test_admin",
        "name": "contract-license-probe"
    });
    let (community_url, community_harness, _cache, community_cache_dir, community_bearer) =
        start_test_server_with_entitlements(Config::default(), Entitlements::community()).await;
    let community = capture_json(
        &client_with_bearer(&community_bearer),
        &community_url,
        "feature_not_licensed_403",
        "post",
        "/v1/security/keys",
        "/v1/security/keys",
        403,
        request.clone(),
        request.clone(),
        &[],
        &[],
    )
    .await;
    community_harness.cleanup().await;
    drop(community_cache_dir);

    let expired = expired_test_entitlements();
    let (expired_url, expired_harness, _cache, expired_cache_dir, expired_bearer) =
        start_test_server_with_entitlements(Config::default(), expired).await;
    let frozen = capture_json(
        &client_with_bearer(&expired_bearer),
        &expired_url,
        "license_expired_403",
        "post",
        "/v1/security/keys",
        "/v1/security/keys",
        403,
        request.clone(),
        request,
        &[],
        &[],
    )
    .await;
    expired_harness.cleanup().await;
    drop(expired_cache_dir);

    vec![community, frozen]
}

async fn security_error_fixtures(
    unauthenticated_client: &reqwest::Client,
    forbidden_client: &reqwest::Client,
    base_url: &str,
) -> Vec<Fixture> {
    vec![
        capture_json(
            unauthenticated_client,
            base_url,
            "unauthenticated_401",
            "post",
            "/v1/namespaces/contract-main/query",
            "/v1/namespaces/contract-main/query",
            401,
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 1
            }),
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 1
            }),
            &[],
            &[],
        )
        .await,
        capture_json(
            forbidden_client,
            base_url,
            "forbidden_403",
            "get",
            "/v1/config/query",
            "/v1/config/query",
            403,
            Value::Null,
            Value::Null,
            &[],
            &[],
        )
        .await,
        capture_json(
            unauthenticated_client,
            base_url,
            "readyz_gated_401",
            "get",
            "/readyz",
            "/readyz",
            401,
            Value::Null,
            Value::Null,
            &[],
            &[],
        )
        .await,
    ]
}

async fn security_admin_fixtures(client: &reqwest::Client, base_url: &str) -> Vec<Fixture> {
    let mut fixtures = Vec::new();

    let principal_request = json!({
        "principal_id": "service:contract",
        "kind": "service",
        "display_name": "contract-service"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_create_principal",
            "post",
            "/v1/security/principals",
            "/v1/security/principals",
            201,
            principal_request.clone(),
            principal_request,
            &[],
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_list_principals",
            "get",
            "/v1/security/principals",
            "/v1/security/principals",
            200,
            Value::Null,
            Value::Null,
            &[],
            &[],
        )
        .await,
    );

    let create_key_request = json!({
        "principal_id": "service:contract",
        "name": "contract-primary"
    });
    let (status, create_key_response) = send_json(
        client,
        base_url,
        "post",
        "/v1/security/keys",
        "security_create_key",
        create_key_request.clone(),
    )
    .await;
    assert_eq!(
        status, 201,
        "fixture security_create_key expected status 201, got {status}: {create_key_response}"
    );
    let primary_key_id = response_string(&create_key_response, "key_id", "security_create_key");
    let primary_api_key = response_string(&create_key_response, "api_key", "security_create_key");
    let mut security_replacements = vec![
        (primary_api_key, CONTRACT_PRIMARY_API_KEY.to_string()),
        (primary_key_id.clone(), CONTRACT_PRIMARY_KEY_ID.to_string()),
    ];
    fixtures.push(fixture_from_response(
        "security_create_key",
        "post",
        "/v1/security/keys",
        201,
        create_key_request,
        create_key_response,
        &security_replacements,
    ));

    let rotate_request = json!({"overlap_secs": 0});
    let rotate_path = format!("/v1/security/keys/{primary_key_id}/rotate");
    let (status, rotate_response) = send_json(
        client,
        base_url,
        "post",
        &rotate_path,
        "security_rotate_key",
        rotate_request.clone(),
    )
    .await;
    assert_eq!(
        status, 201,
        "fixture security_rotate_key expected status 201, got {status}: {rotate_response}"
    );
    let rotated_key_id = response_string(&rotate_response, "key_id", "security_rotate_key");
    let rotated_api_key = response_string(&rotate_response, "api_key", "security_rotate_key");
    security_replacements.splice(
        0..0,
        [
            (rotated_api_key, CONTRACT_ROTATED_API_KEY.to_string()),
            (rotated_key_id.clone(), CONTRACT_ROTATED_KEY_ID.to_string()),
        ],
    );
    fixtures.push(fixture_from_response(
        "security_rotate_key",
        "post",
        &format!("/v1/security/keys/{CONTRACT_PRIMARY_KEY_ID}/rotate"),
        201,
        rotate_request,
        rotate_response,
        &security_replacements,
    ));

    let revoke_path = format!("/v1/security/keys/{rotated_key_id}");
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_revoke_key",
            "delete",
            &revoke_path,
            &format!("/v1/security/keys/{CONTRACT_ROTATED_KEY_ID}"),
            200,
            Value::Null,
            Value::Null,
            &security_replacements,
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_list_keys",
            "get",
            "/v1/security/keys",
            "/v1/security/keys",
            200,
            Value::Null,
            Value::Null,
            &security_replacements,
            &[],
        )
        .await,
    );

    let grant_request = json!({
        "principal_id": "service:contract",
        "scope": {"kind": "namespace", "namespace": "contract-main"},
        "actions": {"kind": "selected", "actions": ["NamespaceRead", "Query", "VectorUpsert"]},
        "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
        "field_mask": {"deny": ["salary", "ssn"]},
        "write_constraints": {
            "stamp": {"tenant_id": "acme"},
            "forbid_set": ["classification", "is_public"]
        }
    });
    let grant_removal_request = json!({
        "principal_id": "service:contract",
        "scope": {"kind": "namespace", "namespace": "contract-main"},
        "actions": {"kind": "selected", "actions": ["NamespaceRead", "Query", "VectorUpsert"]}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_create_grant",
            "post",
            "/v1/security/grants",
            "/v1/security/grants",
            201,
            grant_request.clone(),
            grant_request.clone(),
            &security_replacements,
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_list_grants",
            "get",
            "/v1/security/grants",
            "/v1/security/grants",
            200,
            Value::Null,
            Value::Null,
            &security_replacements,
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_delete_grant",
            "delete",
            "/v1/security/grants",
            "/v1/security/grants",
            200,
            grant_removal_request.clone(),
            grant_removal_request,
            &security_replacements,
            &[],
        )
        .await,
    );

    let (status, response) = send_json(
        client,
        base_url,
        "post",
        "/v1/security/grants",
        "setup_contract_delegation_grant",
        json!({
            "principal_id": "zpk1_test_admin",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
    )
    .await;
    assert_eq!(
        status, 201,
        "contract delegation grant setup failed: {response}"
    );
    let mint_request = json!({
        "actions": ["Query"],
        "namespaces": ["contract-main"],
        "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
        "purpose": "contract fixture delegated retrieval",
        "expires_in_secs": 300
    });
    let (status, mint_response) = send_json(
        client,
        base_url,
        "post",
        "/v1/security/tokens",
        "security_mint_token",
        mint_request.clone(),
    )
    .await;
    assert_eq!(
        status, 201,
        "fixture security_mint_token expected status 201, got {status}: {mint_response}"
    );
    let minted_token = response_string(&mint_response, "token", "security_mint_token");
    let minted_token_id = response_string(&mint_response, "token_id", "security_mint_token");
    security_replacements.splice(
        0..0,
        [
            (minted_token, "zpt1_contract_token.signature".to_string()),
            (
                minted_token_id,
                "zdt1_00000000000000000000000000".to_string(),
            ),
        ],
    );
    let mut mint_response = mint_response;
    normalize_contract_value(&mut mint_response, &security_replacements, &[]);
    fixtures.push(fixture_from_response(
        "security_mint_token",
        "post",
        "/v1/security/tokens",
        201,
        mint_request,
        mint_response,
        &[],
    ));

    let (status, policy_response) = send_json(
        client,
        base_url,
        "get",
        "/v1/security/policy",
        "security_get_policy",
        Value::Null,
    )
    .await;
    assert_eq!(
        status, 200,
        "fixture security_get_policy expected status 200, got {status}: {policy_response}"
    );
    let object_key = response_string(&policy_response, "object_key", "security_get_policy");
    let checksum = response_string(&policy_response, "checksum", "security_get_policy");
    security_replacements.splice(
        0..0,
        [
            (
                object_key,
                "_security/policies/contract-policy.json".to_string(),
            ),
            (checksum, "0".repeat(64)),
        ],
    );
    fixtures.push(fixture_from_response(
        "security_get_policy",
        "get",
        "/v1/security/policy",
        200,
        Value::Null,
        policy_response,
        &security_replacements,
    ));

    let preservation_request = json!({
        "scope": {"kind": "namespace", "namespace": "contract-preserved"},
        "reason_kind": "regulatory",
        "reason_text": "contract fixture preservation evidence"
    });
    let (status, preservation_response) = send_json(
        client,
        base_url,
        "post",
        "/v1/security/preservation",
        "security_create_preservation",
        preservation_request.clone(),
    )
    .await;
    assert_eq!(
        status, 201,
        "fixture security_create_preservation expected status 201, got {status}: {preservation_response}"
    );
    let lock_id = response_string(
        &preservation_response,
        "lock_id",
        "security_create_preservation",
    );
    let preservation_replacements = [(
        lock_id.clone(),
        "plk_00000000000000000000000000".to_string(),
    )];
    fixtures.push(fixture_from_response(
        "security_create_preservation",
        "post",
        "/v1/security/preservation",
        201,
        preservation_request,
        preservation_response,
        &preservation_replacements,
    ));
    fixtures.push(
        capture_json(
            client,
            base_url,
            "security_list_preservation",
            "get",
            "/v1/security/preservation",
            "/v1/security/preservation",
            200,
            Value::Null,
            Value::Null,
            &preservation_replacements,
            &[],
        )
        .await,
    );

    let approver_id = "human:contract-preservation-approver";
    for (name, path, request, expected) in [
        (
            "setup_contract_preservation_approver",
            "/v1/security/principals",
            json!({
                "principal_id": approver_id,
                "kind": "human",
                "display_name": "contract preservation approver"
            }),
            201,
        ),
        (
            "setup_contract_preservation_approver_key",
            "/v1/security/keys",
            json!({"principal_id": approver_id, "name": "contract preservation approval"}),
            201,
        ),
        (
            "setup_contract_preservation_approver_grant",
            "/v1/security/grants",
            json!({
                "principal_id": approver_id,
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["PreservationRelease"]}
            }),
            201,
        ),
    ] {
        let (status, response) = send_json(client, base_url, "post", path, name, request).await;
        assert_eq!(status, expected, "{name} failed: {response}");
        if name == "setup_contract_preservation_approver_key" {
            security_replacements.push((
                response_string(&response, "api_key", name),
                "zpk1_contract_preservation_approver.secret".to_string(),
            ));
        }
    }
    let approval_bearer = security_replacements
        .iter()
        .find(|(_, canonical)| canonical == "zpk1_contract_preservation_approver.secret")
        .map(|(actual, _)| actual.clone())
        .expect("contract preservation approver bearer must be captured");
    let release_path = format!("/v1/security/preservation/{lock_id}/release");
    let release_response = client
        .post(format!("{base_url}{release_path}"))
        .header("x-request-id", "contract-security_release_preservation")
        .header("x-zeppelin-approval", approval_bearer)
        .send()
        .await
        .unwrap();
    let status = release_response.status().as_u16();
    let release_response: Value = release_response.json().await.unwrap();
    assert_eq!(
        status, 200,
        "fixture security_release_preservation expected status 200, got {status}: {release_response}"
    );
    fixtures.push(fixture_from_response(
        "security_release_preservation",
        "post",
        "/v1/security/preservation/plk_00000000000000000000000000/release",
        200,
        Value::Null,
        release_response,
        &preservation_replacements,
    ));

    fixtures
}

async fn phase4_security_error_fixtures(
    admin: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    replacements: &[(String, String)],
) -> Vec<Fixture> {
    let principal_id = "service:contract-phase4-errors";
    let (status, response) = send_json(
        admin,
        base_url,
        "post",
        "/v1/security/principals",
        "setup_phase4_error_principal",
        json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "contract-phase4-errors"
        }),
    )
    .await;
    assert_eq!(
        status, 201,
        "phase4 error principal setup failed: {response}"
    );

    let (status, key_response) = send_json(
        admin,
        base_url,
        "post",
        "/v1/security/keys",
        "setup_phase4_error_key",
        json!({"principal_id": principal_id, "name": "contract-phase4-errors"}),
    )
    .await;
    assert_eq!(status, 201, "phase4 error key setup failed: {key_response}");
    let tenant_bearer = response_string(&key_response, "api_key", "setup_phase4_error_key");
    let tenant = client_with_bearer(&tenant_bearer);

    let query_grant = json!({
        "principal_id": principal_id,
        "scope": {"kind": "namespace", "namespace": namespace},
        "actions": {"kind": "selected", "actions": ["Query"]},
        "mandatory_filter": {"op": "not_eq", "field": "category", "value": "never"},
        "field_mask": {"deny": ["title"]}
    });
    let (status, response) = send_json(
        admin,
        base_url,
        "post",
        "/v1/security/grants",
        "setup_phase4_error_grant",
        query_grant,
    )
    .await;
    assert_eq!(status, 201, "phase4 error grant setup failed: {response}");

    let query_path = format!("/v1/namespaces/{namespace}/query");
    let canonical_query_path = "/v1/namespaces/contract-main/query";
    let debug_request = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "debug": true
    });
    let constraint = capture_json(
        &tenant,
        base_url,
        "error_constraint_violation_403",
        "post",
        &query_path,
        canonical_query_path,
        403,
        debug_request.clone(),
        debug_request,
        replacements,
        &[],
    )
    .await;

    let page_request = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "cursor": {"type": "none"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let (status, page_response) = send_json(
        &tenant,
        base_url,
        "post",
        &query_path,
        "setup_phase4_stale_cursor",
        page_request,
    )
    .await;
    assert_eq!(status, 200, "phase4 cursor setup failed: {page_response}");
    let stale_cursor = response_string(&page_response, "next_cursor", "setup_phase4_stale_cursor");

    let (status, response) = send_json(
        admin,
        base_url,
        "post",
        "/v1/security/grants",
        "setup_phase4_policy_bump",
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["NamespaceRead"]}
        }),
    )
    .await;
    assert_eq!(status, 201, "phase4 policy bump failed: {response}");

    let stale_request = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "cursor": {"type": "after", "token": stale_cursor},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let canonical_stale_request = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "cursor": {"type": "after", "token": "contract-stale-cursor"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let stale = capture_json(
        &tenant,
        base_url,
        "error_cursor_policy_stale_400",
        "post",
        &query_path,
        canonical_query_path,
        400,
        stale_request,
        canonical_stale_request,
        replacements,
        &[],
    )
    .await;

    vec![constraint, stale]
}

fn response_string(response: &Value, field: &str, fixture: &str) -> String {
    response[field]
        .as_str()
        .unwrap_or_else(|| panic!("fixture {fixture} response missing string field {field}"))
        .to_string()
}

#[allow(clippy::too_many_arguments)]
fn fixture_from_response(
    name: &'static str,
    method: &'static str,
    path: &str,
    status: u16,
    request: Value,
    mut response: Value,
    replacements: &[(String, String)],
) -> Fixture {
    normalize_contract_value(&mut response, replacements, &[]);
    normalize_fixture_order(name, &mut response);
    Fixture {
        name,
        method,
        path: path.to_string(),
        status,
        request,
        response,
    }
}

async fn query_fixtures(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    replacements: &[(String, String)],
) -> Vec<Fixture> {
    let actual_path = format!("/v1/namespaces/{ns}/query");
    let canonical_path = "/v1/namespaces/contract-main/query";
    let mut fixtures = Vec::new();

    let ann = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_ann",
            "post",
            &actual_path,
            canonical_path,
            200,
            ann.clone(),
            ann,
            replacements,
            &[],
        )
        .await,
    );

    let by_id = json!({
        "sources": [{"type": "ann", "id": "seed"}],
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_by_id",
            "post",
            &actual_path,
            canonical_path,
            200,
            by_id.clone(),
            by_id,
            replacements,
            &[],
        )
        .await,
    );

    let hybrid = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "rrf", "k": 20},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_hybrid_rrf",
            "post",
            &actual_path,
            canonical_path,
            200,
            hybrid.clone(),
            hybrid,
            replacements,
            &[],
        )
        .await,
    );

    let weighted = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "weighted", "weights": [0.7, 0.3]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_weighted_fusion",
            "post",
            &actual_path,
            canonical_path,
            200,
            weighted.clone(),
            weighted,
            replacements,
            &[],
        )
        .await,
    );

    let vector_rerank = json!({
        "sources": [{"type": "ann", "vector": [0.6, 0.0]}],
        "rerank": {"type": "vector", "vector": [0.0, 0.0]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_vector_rerank",
            "post",
            &actual_path,
            canonical_path,
            200,
            vector_rerank.clone(),
            vector_rerank,
            replacements,
            &[],
        )
        .await,
    );

    let bm25_rerank = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "rerank": {"type": "bm25", "rank_by": ["title", "BM25", "search"]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_bm25_rerank",
            "post",
            &actual_path,
            canonical_path,
            200,
            bm25_rerank.clone(),
            bm25_rerank,
            replacements,
            &[],
        )
        .await,
    );

    let cursor_page1 = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "none"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let (page1_status, mut page1_response) = send_json(
        client,
        base_url,
        "post",
        &actual_path,
        "query_cursor_page1",
        cursor_page1.clone(),
    )
    .await;
    assert_eq!(
        page1_status, 200,
        "fixture query_cursor_page1 expected status 200, got {page1_status}: {page1_response}"
    );
    let cursor = page1_response
        .get("next_cursor")
        .and_then(Value::as_str)
        .expect("cursor page 1 must return a cursor")
        .to_string();
    normalize_contract_value(
        &mut page1_response,
        replacements,
        std::slice::from_ref(&cursor),
    );
    fixtures.push(Fixture {
        name: "query_cursor_page1",
        method: "post",
        path: canonical_path.to_string(),
        status: page1_status,
        request: cursor_page1,
        response: page1_response,
    });

    let cursor_page2_actual = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "after", "token": cursor},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let cursor_page2_canonical = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "after", "token": "contract-cursor-page-1"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_cursor_page2",
            "post",
            &actual_path,
            canonical_path,
            200,
            cursor_page2_actual,
            cursor_page2_canonical,
            replacements,
            &[],
        )
        .await,
    );

    let grouping = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "grouping": {"type": "field", "field": "category", "max_per_group": 2},
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_grouping",
            "post",
            &actual_path,
            canonical_path,
            200,
            grouping.clone(),
            grouping,
            replacements,
            &[],
        )
        .await,
    );

    let facets = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 3,
        "candidate_k": 5,
        "facets": ["category", "tags"],
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_facets",
            "post",
            &actual_path,
            canonical_path,
            200,
            facets.clone(),
            facets,
            replacements,
            &[],
        )
        .await,
    );

    let explain_plan = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "rrf", "k": 20},
        "top_k": 3,
        "candidate_k": 5,
        "explain": "plan",
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_explain_plan",
            "post",
            &actual_path,
            canonical_path,
            200,
            explain_plan.clone(),
            explain_plan,
            replacements,
            &[],
        )
        .await,
    );

    let explain_full = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "weighted", "weights": [0.5, 0.5]},
        "top_k": 3,
        "candidate_k": 5,
        "explain": "full",
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_explain_full",
            "post",
            &actual_path,
            canonical_path,
            200,
            explain_full.clone(),
            explain_full,
            replacements,
            &[],
        )
        .await,
    );

    let batch = json!({
        "queries": [
            {
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 1,
                "consistency": "strong"
            },
            {
                "sources": [{"type": "ann", "vector": [0.0]}],
                "top_k": 1,
                "consistency": "strong"
            }
        ]
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "batch_query",
            "post",
            &format!("/v1/namespaces/{ns}/query/batch"),
            "/v1/namespaces/contract-main/query/batch",
            200,
            batch.clone(),
            batch,
            replacements,
            &[],
        )
        .await,
    );

    fixtures
}

async fn error_fixtures(
    client: &reqwest::Client,
    base_url: &str,
    main_ns: &str,
    conflict_ns: &str,
    deleting_ns: &str,
    missing_ns: &str,
    replacements: &[(String, String)],
) -> Vec<Fixture> {
    let mut fixtures = Vec::new();

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_validation_400",
            "post",
            &format!("/v1/namespaces/{main_ns}/query"),
            "/v1/namespaces/contract-main/query",
            400,
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 0
            }),
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 0
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_not_found_404",
            "get",
            &format!("/v1/namespaces/{missing_ns}"),
            "/v1/namespaces/contract-missing",
            404,
            Value::Null,
            Value::Null,
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_conflict_409",
            "post",
            "/v1/namespaces",
            "/v1/namespaces",
            409,
            json!({
                "name": conflict_ns,
                "dimensions": 3,
                "distance_metric": "euclidean"
            }),
            json!({
                "name": "contract-conflict",
                "dimensions": 3,
                "distance_metric": "euclidean"
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_deleting_410",
            "post",
            &format!("/v1/namespaces/{deleting_ns}/vectors"),
            "/v1/namespaces/contract-deleting/vectors",
            410,
            json!({
                "vectors": [{"id": "blocked", "values": [0.0, 0.0]}]
            }),
            json!({
                "vectors": [{"id": "blocked", "values": [0.0, 0.0]}]
            }),
            replacements,
            &[],
        )
        .await,
    );

    let too_many_queries = (0..3)
        .map(|_| json!({"sources": [{"type": "ann", "vector": [0.0, 0.0]}]}))
        .collect::<Vec<_>>();
    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_payload_too_large_413",
            "post",
            &format!("/v1/namespaces/{main_ns}/query/batch"),
            "/v1/namespaces/contract-main/query/batch",
            413,
            json!({"queries": too_many_queries}),
            json!({
                "queries": (0..3)
                    .map(|_| json!({"sources": [{"type": "ann", "vector": [0.0, 0.0]}]}))
                    .collect::<Vec<_>>()
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_not_implemented_501",
            "post",
            &format!("/v1/namespaces/{main_ns}/query"),
            "/v1/namespaces/contract-main/query",
            501,
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "projection": {"include_vectors": true}
            }),
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "projection": {"include_vectors": true}
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures
}

#[allow(clippy::too_many_arguments)]
async fn capture_json(
    client: &reqwest::Client,
    base_url: &str,
    name: &'static str,
    method: &'static str,
    actual_path: &str,
    canonical_path: &str,
    expected_status: u16,
    actual_request: Value,
    canonical_request: Value,
    replacements: &[(String, String)],
    cursor_tokens: &[String],
) -> Fixture {
    let (status, mut response) =
        send_json(client, base_url, method, actual_path, name, actual_request).await;
    normalize_contract_value(&mut response, replacements, cursor_tokens);
    normalize_fixture_order(name, &mut response);
    assert_eq!(
        status, expected_status,
        "fixture {name} expected status {expected_status}, got {status}: {response}"
    );
    Fixture {
        name,
        method,
        path: canonical_path.to_string(),
        status,
        request: canonical_request,
        response,
    }
}

fn normalize_fixture_order(name: &str, response: &mut Value) {
    if name == "security_list_keys" {
        response["keys"]
            .as_array_mut()
            .unwrap_or_else(|| panic!("security_list_keys response must contain a key array"))
            .sort_by(|left, right| left["key_id"].as_str().cmp(&right["key_id"].as_str()));
    }
}

async fn send_json(
    client: &reqwest::Client,
    base_url: &str,
    method: &str,
    path: &str,
    name: &str,
    request: Value,
) -> (u16, Value) {
    let method = method.to_uppercase();
    let method = Method::from_bytes(method.as_bytes()).unwrap();
    let mut builder = client
        .request(method, format!("{base_url}{path}"))
        .header("x-request-id", format!("contract-{name}"));
    if !request.is_null() {
        builder = builder.json(&request);
    }
    let response = builder.send().await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.bytes().await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|err| panic!("fixture {name} response is not JSON: {err}"))
    };
    (status, body)
}

async fn setup_namespace(client: &reqwest::Client, base_url: &str, ns: &str, mut body: Value) {
    body["name"] = json!(ns);
    let (status, response) = send_json(
        client,
        base_url,
        "post",
        "/v1/namespaces",
        "setup_namespace",
        body,
    )
    .await;
    assert!(
        status == 201 || status == 200,
        "setup namespace {ns} failed with {status}: {response}"
    );
}

async fn seed_deleting_namespace(store: &zeppelin::storage::ZeppelinStore, ns: &str) {
    let timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let meta = NamespaceMetadata {
        name: ns.to_string(),
        dimensions: 2,
        distance_metric: DistanceMetric::Euclidean,
        index_type: IndexType::default(),
        vector_count: 0,
        created_at: timestamp,
        updated_at: timestamp,
        state: NamespaceState::Deleting,
        destruction_record_key: None,
        deletion_intent: None,
        full_text_search: Default::default(),
        index_config: Some(NamespaceIndexConfig::from_indexing_config(
            &Config::default().indexing,
        )),
        compaction_health: CompactionHealth::default(),
        creation_kind: zeppelin::namespace::branching::NamespaceCreationKind::Root,
        branch_identity: None,
        branch_prepare: None,
        branch_activation: None,
        late_interaction: None,
        incarnation_id: None,
    };
    store
        .put(&NamespaceMetadata::s3_key(ns), meta.to_bytes().unwrap())
        .await
        .unwrap();
    common::seed_bound_manifest(store, ns).await;
}

fn normalize_contract_value(
    value: &mut Value,
    replacements: &[(String, String)],
    cursor_tokens: &[String],
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                match key.as_str() {
                    "created_at" | "updated_at" | "revokes_at" | "expires_at" | "released_at" => {
                        if child.is_string() {
                            *child = json!("2026-01-01T00:00:00+00:00");
                        }
                    }
                    "last_compaction_at" => {
                        if child.is_string() {
                            *child = json!("2026-01-01T00:00:00+00:00");
                        }
                    }
                    "latency_ms" | "wal_ms" | "segment_ms" | "merge_ms" => {
                        if child.is_number() {
                            *child = json!(0);
                        }
                    }
                    "next_cursor" => {
                        if child.is_string() {
                            *child = json!("contract-cursor-page-1");
                        }
                    }
                    "active_segment" | "segment_id" => {
                        if child.is_string() {
                            *child = json!("contract-segment");
                        }
                    }
                    _ => normalize_contract_value(child, replacements, cursor_tokens),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_contract_value(item, replacements, cursor_tokens);
            }
        }
        Value::String(text) => {
            for (actual, canonical) in replacements {
                *text = text.replace(actual, canonical);
            }
            for token in cursor_tokens {
                if text == token {
                    *text = "contract-cursor-page-1".to_string();
                }
            }
        }
        _ => {}
    }
}

fn main_vectors() -> Value {
    json!({
        "vectors": [
            {
                "id": "seed",
                "values": [0.0, 0.0],
                "attributes": {
                    "category": "anchor",
                    "tags": ["blue"],
                    "title": "anchor search"
                }
            },
            {
                "id": "near",
                "values": [0.1, 0.0],
                "attributes": {
                    "category": "alpha",
                    "tags": ["fresh", "red"],
                    "title": "alpha search"
                }
            },
            {
                "id": "mid",
                "values": [0.2, 0.0],
                "attributes": {
                    "category": "alpha",
                    "tags": ["fresh", "blue"],
                    "title": "beta search"
                }
            },
            {
                "id": "far",
                "values": [1.0, 0.0],
                "attributes": {
                    "category": "beta",
                    "tags": ["archive"],
                    "title": "gamma archive"
                }
            },
            {
                "id": "other",
                "values": [0.0, 1.0],
                "attributes": {
                    "category": "beta",
                    "tags": ["fresh"],
                    "title": "search document"
                }
            }
        ]
    })
}

fn main_vectors_canonical() -> Value {
    main_vectors()
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contract")
        .join("fixtures")
        .join(FIXTURE_VERSION)
}

fn write_or_compare_fixtures(fixtures: &[Fixture]) {
    let fixture_root = fixture_root();
    let manifest = FixtureManifest {
        version: FIXTURE_VERSION.to_string(),
        generated_by: "cargo test --test contract_tests contract_fixtures_match_real_engine_output"
            .to_string(),
        cases: fixtures
            .iter()
            .map(|fixture| FixtureManifestCase {
                name: fixture.name.to_string(),
                method: fixture.method.to_uppercase(),
                path: fixture.path.clone(),
                status: fixture.status,
                request: format!("{}.req.json", fixture.name),
                response: format!("{}.resp.json", fixture.name),
            })
            .collect(),
    };

    if std::env::var("UPDATE_CONTRACT_FIXTURES").as_deref() == Ok("1") {
        fs::create_dir_all(&fixture_root).unwrap();
        write_json_file(&fixture_root.join("manifest.json"), &manifest);
        for fixture in fixtures {
            write_json_file(
                &fixture_root.join(format!("{}.req.json", fixture.name)),
                &fixture.request,
            );
            write_json_file(
                &fixture_root.join(format!("{}.resp.json", fixture.name)),
                &fixture.response,
            );
        }
        return;
    }

    let actual_manifest: FixtureManifest = read_json_file(&fixture_root.join("manifest.json"))
        .unwrap_or_else(|err| {
            panic!(
                "failed to read fixture manifest {}: {err}",
                fixture_root.join("manifest.json").display()
            )
        });
    assert_eq!(actual_manifest, manifest, "fixture manifest drifted");

    for fixture in fixtures {
        let request_path = fixture_root.join(format!("{}.req.json", fixture.name));
        let response_path = fixture_root.join(format!("{}.resp.json", fixture.name));
        let request: Value = read_json_file(&request_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", request_path.display()));
        let response: Value = read_json_file(&response_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", response_path.display()));
        assert_eq!(
            request, fixture.request,
            "{} request fixture drifted",
            fixture.name
        );
        assert_eq!(
            response, fixture.response,
            "{} response fixture drifted",
            fixture.name
        );
    }
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) {
    let mut body = serde_json::to_string_pretty(value).unwrap();
    body.push('\n');
    fs::write(path, body).unwrap();
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<T> {
    let body = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&body).unwrap())
}

fn assert_operation_status_documented(api: &str, fixture: &Fixture) {
    let statuses = documented_statuses(api, fixture.method, &fixture.openapi_path());
    assert!(
        statuses.contains(&fixture.status),
        "fixture {} uses undocumented status {} for {} {} (documented: {statuses:?})",
        fixture.name,
        fixture.status,
        fixture.method,
        fixture.openapi_path(),
    );
}

fn assert_response_contract_shape(fixture: &Fixture) {
    if fixture.status == 204 {
        assert!(
            fixture.response.is_null(),
            "{} 204 response must have no JSON body",
            fixture.name
        );
        return;
    }
    if fixture.status >= 400 {
        assert_error_envelope(&fixture.response, fixture.status, fixture.name);
        match fixture.name {
            "unauthenticated_401" | "readyz_gated_401" => {
                assert_eq!(fixture.response["code"], "unauthenticated");
                assert_eq!(fixture.response["error"], "authentication required");
                assert_eq!(fixture.response["retryable"], false);
            }
            "forbidden_403" => {
                assert_eq!(fixture.response["code"], "forbidden");
                assert_eq!(fixture.response["error"], "access forbidden");
                assert_eq!(fixture.response["retryable"], false);
            }
            "feature_not_licensed_403" => {
                assert_eq!(fixture.response["code"], "feature_not_licensed");
                assert_eq!(fixture.response["retryable"], false);
            }
            "license_expired_403" => {
                assert_eq!(fixture.response["code"], "license_expired");
                assert_eq!(fixture.response["retryable"], false);
            }
            "error_constraint_violation_403" => {
                assert_eq!(fixture.response["code"], "constraint_violation");
                assert_eq!(fixture.response["retryable"], false);
            }
            "error_cursor_policy_stale_400" => {
                assert_eq!(fixture.response["code"], "cursor_policy_stale");
                assert_eq!(fixture.response["retryable"], false);
            }
            _ => {}
        }
        return;
    }
    if fixture.name.starts_with("query_") {
        assert_query_response(&fixture.response, fixture.name);
    }
    if fixture.name == "batch_query" {
        let results = fixture.response["results"].as_array().unwrap();
        assert!(!results.is_empty(), "batch fixture must include entries");
        assert!(results.iter().any(|entry| entry["ok"] == true));
        assert!(results.iter().any(|entry| entry["ok"] == false));
    }
    if fixture.name.ends_with("query_config") {
        for key in [
            "rerank_coalesce_gap_bytes",
            "default_nprobe",
            "default_top_k",
            "bm25_max_full_scan_clusters",
            "bm25_max_full_scan_vectors",
        ] {
            assert!(
                fixture.response.get(key).is_some(),
                "{} missing runtime query knob {key}",
                fixture.name
            );
        }
    }
    if fixture.name.starts_with("security_") {
        assert_security_admin_contract(fixture);
    }
}

fn assert_security_admin_contract(fixture: &Fixture) {
    if fixture.name.contains("preservation") {
        let serialized = serde_json::to_string(&fixture.response).unwrap();
        assert!(!serialized.contains("api_key"));
        assert!(!serialized.contains("sha256_hex"));
        assert!(
            fixture.response.get("lock_id").is_some() || fixture.response.get("locks").is_some(),
            "{} must expose lock evidence or the lock inventory",
            fixture.name
        );
        return;
    }
    assert!(
        fixture.response["policy_version"].is_u64(),
        "{} must expose its authoritative policy version",
        fixture.name
    );

    let serialized = serde_json::to_string(&fixture.response).unwrap();
    assert!(
        !serialized.contains("sha256_hex"),
        "{} must never expose a stored credential digest",
        fixture.name
    );
    if matches!(fixture.name, "security_create_key" | "security_rotate_key") {
        assert!(
            fixture.response["api_key"].as_str().is_some(),
            "{} must carry its one-time API-key secret",
            fixture.name
        );
    } else {
        assert!(
            !serialized.contains("api_key"),
            "{} must not expose an API-key secret",
            fixture.name
        );
    }

    if fixture.name == "security_list_keys" {
        let keys = fixture.response["keys"]
            .as_array()
            .expect("security_list_keys must return a key array");
        assert!(
            keys.iter().any(|key| key["rotated_from"].is_string()),
            "redacted key inventory must retain rotation lineage"
        );
        assert!(
            keys.iter().any(|key| key["state"] == "revoked"),
            "redacted key inventory must retain lifecycle state"
        );
    }

    if matches!(
        fixture.name,
        "security_create_grant" | "security_delete_grant"
    ) {
        assert!(
            matches!(
                fixture.request["scope"]["kind"].as_str(),
                Some("global" | "namespace")
            ),
            "{} request must use a tagged grant scope",
            fixture.name
        );
        assert!(
            matches!(
                fixture.request["actions"]["kind"].as_str(),
                Some("all" | "selected")
            ),
            "{} request must use a tagged grant action set",
            fixture.name
        );
        if fixture.name == "security_create_grant" {
            for constraint in ["mandatory_filter", "field_mask", "write_constraints"] {
                assert!(
                    fixture.request.get(constraint).is_some(),
                    "create-grant fixture must publish {constraint}"
                );
                assert!(
                    fixture.response["grant"].get(constraint).is_some(),
                    "create-grant response must retain {constraint}"
                );
            }
        } else {
            for constraint in ["mandatory_filter", "field_mask", "write_constraints"] {
                assert!(
                    fixture.request.get(constraint).is_none(),
                    "delete-grant fixture must identify only the stable binding"
                );
                assert!(
                    fixture.response["grant"].get(constraint).is_none(),
                    "delete-grant response must return only the removed stable binding"
                );
            }
        }
    }
}

fn assert_query_response(response: &Value, name: &str) {
    assert!(
        response["results"].is_array(),
        "{name} missing results array"
    );
    assert!(
        response["scanned_fragments"].is_u64(),
        "{name} missing scanned_fragments"
    );
    assert!(
        response["scanned_segments"].is_u64(),
        "{name} missing scanned_segments"
    );
}

fn assert_error_envelope(response: &Value, status: u16, name: &str) {
    assert!(response["code"].is_string(), "{name} missing code");
    assert!(response["error"].is_string(), "{name} missing error");
    assert_eq!(response["status"].as_u64(), Some(u64::from(status)));
    assert!(
        response["retryable"].is_boolean(),
        "{name} missing retryable"
    );
}

fn documented_statuses(api: &str, method: &str, path: &str) -> BTreeSet<u16> {
    let mut statuses = BTreeSet::new();
    let mut in_target_path = false;
    let mut in_target_method = false;
    let mut in_responses = false;
    let path_line = format!("  {path}:");
    let method_line = format!("    {method}:");

    for line in api.lines() {
        if line.starts_with("  /") {
            in_target_path = line == path_line;
            in_target_method = false;
            in_responses = false;
            continue;
        }
        if !in_target_path {
            continue;
        }
        if line.starts_with("    ") && !line.starts_with("      ") {
            in_target_method = line == method_line;
            in_responses = false;
            continue;
        }
        if !in_target_method {
            continue;
        }
        if line == "      responses:" {
            in_responses = true;
            continue;
        }
        if !in_responses {
            continue;
        }
        if let Some(status) = line
            .trim()
            .strip_prefix('"')
            .and_then(|rest| rest.split_once('"'))
            .and_then(|(status, _)| status.parse::<u16>().ok())
        {
            statuses.insert(status);
        }
    }

    statuses
}

fn operation_block<'a>(api: &'a str, method: &str, path: &str) -> &'a str {
    let path_marker = format!("  {path}:\n");
    let path_start = api
        .find(&path_marker)
        .unwrap_or_else(|| panic!("OpenAPI path is missing: {path}"));
    let path_body = &api[path_start + path_marker.len()..];
    let method_marker = format!("    {method}:\n");
    let method_start = path_body
        .find(&method_marker)
        .unwrap_or_else(|| panic!("OpenAPI operation is missing: {method} {path}"));
    let operation = &path_body[method_start + method_marker.len()..];
    let end = operation
        .lines()
        .scan(0_usize, |offset, line| {
            let start = *offset;
            *offset += line.len() + 1;
            Some((start, line))
        })
        .find_map(|(offset, line)| {
            (line.starts_with("  /")
                || (line.starts_with("    ") && !line.starts_with("      ") && line.ends_with(':')))
            .then_some(offset)
        })
        .unwrap_or(operation.len());
    &operation[..end]
}

fn component_schema_block<'a>(api: &'a str, schema: &str) -> &'a str {
    let marker = format!("    {schema}:\n");
    let start = api
        .find(&marker)
        .unwrap_or_else(|| panic!("OpenAPI schema is missing: {schema}"));
    let body = &api[start + marker.len()..];
    let end = body
        .lines()
        .scan(0_usize, |offset, line| {
            let start = *offset;
            *offset += line.len() + 1;
            Some((start, line))
        })
        .find_map(|(offset, line)| {
            (line.starts_with("    ") && !line.starts_with("      ") && line.ends_with(':'))
                .then_some(offset)
        })
        .unwrap_or(body.len());
    &body[..end]
}

impl Fixture {
    fn openapi_path(&self) -> String {
        self.path
            .replace(CONTRACT_PRIMARY_KEY_ID, "{key_id}")
            .replace(CONTRACT_ROTATED_KEY_ID, "{key_id}")
            .replace("plk_00000000000000000000000000", "{lock_id}")
            .replace("contract-main", "{ns}")
            .replace("contract-compact", "{ns}")
            .replace("contract-delete", "{ns}")
            .replace("contract-deleting", "{ns}")
            .replace("contract-missing", "{ns}")
    }
}
