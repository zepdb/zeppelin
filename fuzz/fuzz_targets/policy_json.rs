#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use zeppelin::security::{PolicyHead, PolicyKey, PolicyPrincipal, PolicySnapshot};

const UNKNOWN_FIELD: &str = "__zeppelin_fuzz_unknown";

macro_rules! reject_unknown_field_after_valid_parse {
    ($data:expr, $policy_type:ty) => {
        if let Ok(parsed) = serde_json::from_slice::<$policy_type>($data) {
            let mut value = serde_json::to_value(parsed)
                .expect("a deserialized policy value must serialize without failure");
            let object = value
                .as_object_mut()
                .expect("persisted policy records must serialize as JSON objects");
            object.insert(UNKNOWN_FIELD.to_string(), Value::Bool(true));
            assert!(
                serde_json::from_value::<$policy_type>(value).is_err(),
                "{} accepted an unknown field",
                stringify!($policy_type)
            );
        }
    };
}

fuzz_target!(|data: &[u8]| {
    reject_unknown_field_after_valid_parse!(data, PolicyHead);
    reject_unknown_field_after_valid_parse!(data, PolicyPrincipal);
    reject_unknown_field_after_valid_parse!(data, PolicyKey);
    reject_unknown_field_after_valid_parse!(data, PolicySnapshot);

    if let Ok(snapshot) = serde_json::from_slice::<PolicySnapshot>(data) {
        let checksum_valid = snapshot.verify_checksum().is_ok();
        let compiled = snapshot.validate_for_use();
        if checksum_valid {
            assert!(
                compiled.is_ok(),
                "a checksum-valid policy snapshot must compile for use"
            );
        }
    }
});
