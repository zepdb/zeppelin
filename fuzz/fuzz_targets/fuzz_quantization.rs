#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = zeppelin::index::quantization::sq::SqCalibration::from_bytes(data);
    let _ = zeppelin::index::quantization::pq::PqCodebook::from_bytes(data);
});
