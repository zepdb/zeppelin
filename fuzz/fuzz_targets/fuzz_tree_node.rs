#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = zeppelin::index::hierarchical::deserialize_tree_node(data);
});
