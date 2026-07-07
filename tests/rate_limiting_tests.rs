mod common;

use std::net::IpAddr;

use common::server::start_test_server_with_config_no_limit_override;
use serde_json::json;
use zeppelin::config::Config;
use zeppelin::server::{parse_trusted_proxies, resolve_rate_limit_client_ip};

#[test]
fn client_identity_uses_trusted_proxy_xff_rightmost_untrusted() {
    let empty_trusted = parse_trusted_proxies(&[]).unwrap();
    let trusted = vec!["127.0.0.1/32".to_string(), "10.0.0.0/8".to_string()];
    let trusted = parse_trusted_proxies(&trusted).unwrap();
    let peer: IpAddr = "127.0.0.1".parse().unwrap();

    assert_eq!(
        resolve_rate_limit_client_ip(peer, Some("203.0.113.8"), &empty_trusted).unwrap(),
        peer
    );
    assert_eq!(
        resolve_rate_limit_client_ip(peer, None, &trusted).unwrap(),
        peer
    );
    assert_eq!(
        resolve_rate_limit_client_ip(peer, Some("203.0.113.8"), &trusted).unwrap(),
        "203.0.113.8".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        resolve_rate_limit_client_ip(peer, Some("198.51.100.1, 10.1.2.3, 127.0.0.1"), &trusted)
            .unwrap(),
        "198.51.100.1".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        resolve_rate_limit_client_ip(peer, Some("10.1.2.3, 127.0.0.1"), &trusted).unwrap(),
        peer
    );
    assert_eq!(
        resolve_rate_limit_client_ip(peer, Some("203.0.113.8, garbage, 10.1.2.3"), &trusted)
            .unwrap(),
        "203.0.113.8".parse::<IpAddr>().unwrap()
    );

    let untrusted_peer: IpAddr = "198.51.100.200".parse().unwrap();
    assert_eq!(
        resolve_rate_limit_client_ip(untrusted_peer, Some("203.0.113.9"), &trusted).unwrap(),
        untrusted_peer
    );
}

#[tokio::test]
async fn trusted_proxy_xff_clients_have_independent_write_buckets() {
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1/32".to_string()];
    config.server.write_rate_limit_rps = 1;
    config.server.write_rate_limit_burst = 1;
    config.server.rate_limit_rps = 1_000;
    config.server.rate_limit_burst = 1_000;

    let (base_url, harness, _cache, _dir) =
        start_test_server_with_config_no_limit_override(Some(config)).await;
    let client = reqwest::Client::new();

    let first = client
        .post(format!("{base_url}/v1/namespaces"))
        .header("x-forwarded-for", "203.0.113.10")
        .json(&json!({ "dimensions": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 201);

    let same_client = client
        .post(format!("{base_url}/v1/namespaces"))
        .header("x-forwarded-for", "203.0.113.10")
        .json(&json!({ "dimensions": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(same_client.status(), 429);

    let second_client = client
        .post(format!("{base_url}/v1/namespaces"))
        .header("x-forwarded-for", "203.0.113.11")
        .json(&json!({ "dimensions": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(second_client.status(), 201);

    harness.cleanup().await;
}

#[tokio::test]
async fn production_default_write_burst_allows_bulk_shape() {
    let (base_url, harness, _cache, _dir) =
        start_test_server_with_config_no_limit_override(Some(Config::default())).await;
    let client = reqwest::Client::new();

    for _ in 0..30 {
        let resp = client
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({ "dimensions": 4 }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn production_default_read_limit_returns_json_429() {
    let (base_url, harness, _cache, _dir) =
        start_test_server_with_config_no_limit_override(Some(Config::default())).await;
    let client = reqwest::Client::new();
    let queries: Vec<_> = (0..201)
        .map(|_| json!({"vector": [0.0, 0.0, 0.0, 0.0], "top_k": 1}))
        .collect();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/missing/query/batch"))
        .json(&json!({ "queries": queries }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429);
    assert!(resp.headers().contains_key("retry-after"));
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "RATE_LIMITED");
    assert_eq!(body["retryable"], true);

    harness.cleanup().await;
}
