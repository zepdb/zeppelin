use std::str::FromStr;

use zeppelin::config::Config;
use zeppelin::security::{
    Action, ApiKeyId, Decision, DenyReason, NamespaceId, Principal, PrincipalId, RequestContext,
    Resource, SecurityKernel,
};

fn reader_config() -> Config {
    Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["Query"]
namespaces = ["tenant-a"]
"#,
    )
    .unwrap()
}

#[test]
fn configured_grant_produces_full_shaped_allow_decision() {
    let kernel = SecurityKernel::from_config(&reader_config().security).unwrap();
    let principal = Principal::api_key(
        PrincipalId::new("zpk1_reader").unwrap(),
        ApiKeyId::new("zpk1_reader").unwrap(),
        "reader".to_string(),
        None,
    );
    let resource = Resource::Namespace(NamespaceId::new("tenant-a").unwrap());

    let decision = kernel.authorize(
        &principal,
        Action::Query,
        &resource,
        &RequestContext::new("request-1"),
    );

    let Decision::Allow(allow) = decision else {
        panic!("configured query grant must allow tenant-a");
    };
    assert_eq!(allow.policy_version.get(), 0);
    assert!(allow.mandatory_filter.is_none());
    assert!(allow.field_mask.is_none());
    assert!(allow.write_constraints.is_empty());
    assert!(allow.obligations.is_empty());
}

#[test]
fn configured_namespace_scope_denies_cross_namespace() {
    let kernel = SecurityKernel::from_config(&reader_config().security).unwrap();
    let principal = Principal::api_key(
        PrincipalId::new("zpk1_reader").unwrap(),
        ApiKeyId::new("zpk1_reader").unwrap(),
        "reader".to_string(),
        None,
    );
    let resource = Resource::Namespace(NamespaceId::new("tenant-b").unwrap());

    let decision = kernel.authorize(
        &principal,
        Action::Query,
        &resource,
        &RequestContext::new("request-2"),
    );

    let Decision::Deny(deny) = decision else {
        panic!("cross-namespace access must fail closed");
    };
    assert_eq!(deny.reason, DenyReason::NamespaceNotGranted);
}
