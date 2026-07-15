#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use zeppelin::config::{ApiKeyConfig, SecurityConfig, SecurityMode};
use zeppelin::security::{ApiKeyAdapter, RequestContext};

const CANONICAL_HEADER: &str =
    "Bearer zpk1_fuzz.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn adapter() -> &'static ApiKeyAdapter {
    static ADAPTER: OnceLock<ApiKeyAdapter> = OnceLock::new();
    ADAPTER.get_or_init(|| {
        let mut config = SecurityConfig::default();
        config.mode = SecurityMode::Enforced;
        config.set_cursor_hmac_key_hex("11".repeat(32));
        config.api_keys = vec![ApiKeyConfig {
            key_id: "zpk1_fuzz".to_string(),
            name: "fuzz-key".to_string(),
            sha256_hex:
                "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
                    .to_string(),
            actions: vec!["Query".to_string()],
            namespaces: vec!["tenant-a".to_string()],
            expires_at: None,
        }];
        ApiKeyAdapter::from_config(&config).expect("static fuzz credential must be valid")
    })
}

fuzz_target!(|header: &str| {
    let now = RequestContext::new("fuzz-bearer").now;
    if adapter().authenticate_bearer(header, now).is_ok() {
        assert_eq!(header, CANONICAL_HEADER);
    }
});
