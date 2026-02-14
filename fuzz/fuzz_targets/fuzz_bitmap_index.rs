#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = zeppelin::index::bitmap::ClusterBitmapIndex::from_bytes(data);
});
