#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    zeppelin::fuzz::rq_cluster_codes_from_bytes(data);
    zeppelin::fuzz::rq_cluster_codes_only_from_bytes(data);
    zeppelin::fuzz::resident_sketch_from_bytes(data);
});
