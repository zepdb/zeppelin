#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = zeppelin::fts::global_index::GlobalInvertedIndex::from_bytes(data);
});
