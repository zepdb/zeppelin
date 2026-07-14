mod common;

#[tokio::test]
async fn test_servers_generate_distinct_admin_bearers() {
    let (_first_url, _first_harness, first_bearer) = common::server::start_test_server().await;
    let (_second_url, _second_harness, second_bearer) = common::server::start_test_server().await;

    assert_ne!(first_bearer, second_bearer);
}
