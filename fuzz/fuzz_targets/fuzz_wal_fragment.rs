#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Test both checked and unchecked paths
    let _ = zeppelin::wal::fragment::WalFragment::from_bytes(data);
    let _ = zeppelin::wal::fragment::WalFragment::from_bytes_unchecked(data);
});
