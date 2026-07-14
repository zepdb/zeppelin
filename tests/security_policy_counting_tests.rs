mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::prefix::PrefixStore;
use zeppelin::config::Config;
use zeppelin::security::{
    Action, Decision, PrincipalId, PrincipalKind, RequestContext, Resource, SecurityKernel,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::Clock;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;
use common::server::test_admin_bearer;

fn scoped_store(harness: &TestHarness) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(PrefixStore::new(
        harness.store.inner(),
        Path::from(harness.prefix.clone()),
    )))
}

#[tokio::test]
async fn warmed_authentication_and_authorization_use_zero_s3_operations() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&scoped_store(&harness));
    let mut config = Config::default();
    config.security.policy_refresh_secs = 60;
    let admin_bearer = test_admin_bearer(&mut config);
    let clock = Clock::system();
    let (kernel, adapter) = SecurityKernel::from_store(store, &config.security, clock.clone())
        .await
        .expect("security policy must bootstrap over the counting store");
    let authorization = format!("Bearer {admin_bearer}");

    let warm_principal = adapter
        .authenticate_bearer(&authorization, clock.now())
        .expect("bootstrap administrator must warm authentication");
    let Decision::Allow(_) = kernel.authorize(
        &warm_principal,
        Action::SystemRead,
        &Resource::System,
        &RequestContext::new("warm-policy-request"),
    ) else {
        panic!("bootstrap administrator must warm authorization");
    };

    counter.reset();

    let principal = adapter
        .authenticate_bearer(&authorization, clock.now())
        .expect("warmed administrator must authenticate");
    let Decision::Allow(_) = kernel.authorize(
        &principal,
        Action::SystemRead,
        &Resource::System,
        &RequestContext::new("counted-policy-request"),
    ) else {
        panic!("warmed administrator must authorize");
    };

    let observed_gets = counter.total_observed_gets();
    let observed_heads = counter.total_heads();
    let observed_puts = counter.total_observed_puts();
    let domain_gets = counter.total_gets();
    let domain_puts = counter
        .class_breakdown()
        .values()
        .map(|stats| stats.put_ops)
        .sum::<u64>();
    println!(
        "request_path         observed_get observed_head observed_put domain_get domain_put\n\
         warmed_authn_authz  {observed_gets:<12} {observed_heads:<13} {observed_puts:<12} {domain_gets:<10} {domain_puts}"
    );

    assert_eq!(observed_gets, 0, "request GET table");
    assert_eq!(observed_heads, 0, "request HEAD table");
    assert_eq!(observed_puts, 0, "request PUT table");
    assert_eq!(
        domain_gets, 0,
        "warmed security checks must not alter frozen domain GET totals"
    );
    assert_eq!(
        domain_puts, 0,
        "warmed security checks must not alter frozen domain PUT totals"
    );
}

#[tokio::test]
async fn policy_refresh_uses_at_most_one_conditional_get_per_window() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&scoped_store(&harness));
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let _admin_bearer = test_admin_bearer(&mut config);
    let (_kernel, _adapter) = SecurityKernel::from_store(store, &config.security, Clock::system())
        .await
        .expect("security policy must bootstrap over the counting store");

    counter.reset();
    let head_key = "_security/heads/policy.json";
    let deadline = Instant::now() + Duration::from_secs(4);
    while counter.conditional_gets_matching(head_key) < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        counter.conditional_gets_matching(head_key) >= 2,
        "the refresh task must make two observable conditional head checks"
    );
    let gaps = counter.conditional_get_gaps_matching(head_key);
    assert!(!gaps.is_empty());
    assert!(
        gaps.iter().all(|gap| *gap >= Duration::from_secs(1)),
        "conditional head checks must be separated by the complete refresh window: {gaps:?}"
    );
    assert_eq!(
        counter.gets_matching("_security/policies/"),
        0,
        "an unchanged head must not fetch the immutable snapshot"
    );
}

#[tokio::test]
async fn policy_mutation_is_one_immutable_put_plus_one_cas_put() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&scoped_store(&harness));
    let mut config = Config::default();
    config.security.policy_refresh_secs = 60;
    let admin_bearer = test_admin_bearer(&mut config);
    let clock = Clock::system();
    let now = clock.now();
    let (kernel, adapter) = SecurityKernel::from_store(store, &config.security, clock)
        .await
        .expect("security policy must bootstrap over the counting store");
    let actor = adapter
        .authenticate_bearer(&format!("Bearer {admin_bearer}"), now)
        .expect("bootstrap administrator must authenticate");

    counter.reset();
    kernel
        .create_principal(
            &actor,
            PrincipalId::new("service:counted-mutation")
                .expect("counted principal id must be valid"),
            PrincipalKind::Service,
            "Counted mutation".to_string(),
        )
        .await
        .expect("counted policy mutation must publish");

    let policy_objects = "_security/policies/";
    let policy_head = "_security/heads/policy.json";
    assert_eq!(counter.puts_matching(policy_objects), 1);
    assert_eq!(counter.create_puts_matching(policy_objects), 1);
    assert_eq!(counter.update_puts_matching(policy_objects), 0);
    assert_eq!(counter.puts_matching(policy_head), 1);
    assert_eq!(counter.create_puts_matching(policy_head), 0);
    assert_eq!(counter.update_puts_matching(policy_head), 1);
    assert_eq!(counter.gets_matching(policy_head), 1);
    assert_eq!(counter.heads_matching(policy_head), 0);
    assert_eq!(counter.gets_matching(policy_objects), 1);
    assert_eq!(counter.heads_matching(policy_objects), 0);

    assert_eq!(
        counter.gets_for(ArtifactClass::Other),
        0,
        "security control-plane reads must not alter frozen domain totals"
    );
    assert_eq!(
        counter.puts_for(ArtifactClass::Other),
        0,
        "security control-plane writes must not alter frozen domain totals"
    );

    println!(
        "key_class                         before_get before_head before_put after_get after_head after_put create_put cas_put\n\
         _security/heads/policy.json       0          0           0          {}         {}          {}         {}          {}\n\
         _security/policies/<ulid>.json    0          0           0          {}         {}          {}         {}          {}",
        counter.gets_matching(policy_head),
        counter.heads_matching(policy_head),
        counter.puts_matching(policy_head),
        counter.create_puts_matching(policy_head),
        counter.update_puts_matching(policy_head),
        counter.gets_matching(policy_objects),
        counter.heads_matching(policy_objects),
        counter.puts_matching(policy_objects),
        counter.create_puts_matching(policy_objects),
        counter.update_puts_matching(policy_objects),
    );
}
