//! Decode entry points for the `fuzz/` crate's libFuzzer targets.
//!
//! The two-bit decoders are crate-internal: persisted payloads are normally
//! decoded through manifest-aware loaders, so no public API reaches them. This
//! feature-gated seam (`--features fuzz`) is the only external caller. Every
//! function feeds arbitrary bytes to a decoder and discards the outcome —
//! malformed input must be rejected with a typed error, never a panic. A
//! panic here is a fuzz finding, not expected behavior.

use bytes::Bytes;

use crate::index::ivf_flat::sketch::ResidentSketch;
use crate::index::quantization::rq::{RqClusterCodes, RqClusterCodesOnly};

/// Decodes one RQ two-bit cluster container (`RQ_MAGIC` header, row IDs,
/// packed planes, factors), discarding the outcome.
pub fn rq_cluster_codes_from_bytes(data: &[u8]) {
    let _ = RqClusterCodes::from_bytes(data);
}

/// Decodes one codes-only two-bit coarse block (the `ZBP5` grouped-object
/// payload), discarding the outcome.
pub fn rq_cluster_codes_only_from_bytes(data: &[u8]) {
    let _ = RqClusterCodesOnly::from_bytes(data);
}

/// Decodes one resident two-bit sketch through the production owned-bytes
/// entry point, discarding the outcome.
pub fn resident_sketch_from_bytes(data: &[u8]) {
    let _ = ResidentSketch::from_owned_bytes(Bytes::copy_from_slice(data));
}
