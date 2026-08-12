//! Chooses how Zeppelin compresses vectors inside immutable search segments.
//!
//! Quantization replaces each full-precision `f32` vector with a smaller,
//! approximate representation for the first-pass nearest-neighbor scan. The
//! index builders create these representations during compaction or initial
//! segment construction, and the IVF-Flat and hierarchical search paths use
//! them to rank a wider candidate set before reranking selected candidates from
//! full-precision data. This module owns the compression algorithms and their
//! binary payloads; [`crate::index::ivf_flat`] and
//! [`crate::index::hierarchical`] own segment construction and search, while
//! [`crate::storage::ZeppelinStore`] owns S3/MinIO access.
//!
//! ```text
//! full-precision vectors
//!          |
//!          | segment build: calibrate or train
//!          v
//! SQ per-dimension ranges      PQ subvector codebooks
//!          |                            |
//!          +----------+-----------------+
//!                     | encode
//!                     v
//!             compact cluster codes
//!                     |
//!                     | coarse approximate scan
//!                     v
//!          candidate IDs and approximate scores
//!                     |
//!                     | rerank selected candidates
//!                     v
//!             full-precision results
//! ```
//!
//! The smaller representation reduces object-store bytes and CPU work during
//! coarse search at the cost of approximate scores. Quantization does not make
//! an artifact visible: S3/MinIO remains the source of truth, segment artifacts
//! remain immutable, and the authoritative [`crate::wal::manifest::Manifest`]
//! decides which completed segment readers may use.
//!
//! ## Reading map
//!
//! 1. Start with [`QuantizationType`] to see the persisted configuration choice.
//! 2. Read [`sq`] for four-to-one scalar quantization using one byte per vector
//!    dimension.
//! 3. Read [`pq`] for product quantization using one byte per subvector.
//!
//! ## Invariants
//!
//! - A segment's quantization choice must agree with the artifacts written for
//!   that segment and recorded in its manifest metadata.
//! - Calibration, codebooks, cluster codes, and full-precision rows must use
//!   the same vector dimension and row order.
//! - Quantized artifacts are approximations and never replace full-precision
//!   data needed for final reranking.
//! - The helpers in this module construct payloads and object keys but never
//!   read, write, publish, or mutate object-store state themselves.
//!
//! ## Rust concepts used here
//!
//! [`QuantizationType`] is an enum rather than a string or integer flag. This
//! lets every `match` be exhaustive: adding a new scheme makes the Rust
//! compiler identify call sites that must decide how to build, search, and
//! garbage-collect it. A Java `enum` offers a similar closed set but does not
//! require exhaustive `switch` handling in all contexts; C usually represents
//! this with an `enum` plus conventions, without preventing arbitrary integer
//! values after casts. Because this enum is [`Copy`], passing it copies only its
//! small discriminant and does not allocate or transfer ownership.

pub mod pq;
// `rabitq.rs` is dual-compiled: `src/bin/quant_bakeoff.rs` includes the same
// source through `#[path]` and drives the one-bit encode/estimate surface that
// the library itself never calls. The allow covers that offline-only half, so
// it is load-bearing rather than stale -- removing it makes the lib build warn
// on roughly thirty items the bake-off still depends on.
#[allow(dead_code)]
#[cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]
pub(crate) mod rabitq;
#[cfg_attr(test, allow(clippy::expect_used))]
pub(crate) mod rq;
pub mod sq;

use serde::{Deserialize, Serialize};

/// Selects the vector representation used for a segment's coarse search.
///
/// The value is serialized as snake-case text in configuration and manifests,
/// so the spellings `none`, `scalar`, and `product` are compatibility-facing.
/// Changing an existing spelling would prevent older persisted metadata or
/// configuration from deserializing normally.
///
/// # Examples
///
/// `Scalar` tells a segment builder to preserve full vectors for reranking and
/// additionally create one-byte-per-dimension codes for the coarse scan.
/// `None` skips those approximate side data and searches full-precision rows.
///
/// # Rust Notes for Java/C Engineers
///
/// Deriving [`Copy`] means assigning or passing this value leaves the original
/// usable, like copying a C enum or Java primitive. [`Serialize`] and
/// [`Deserialize`] are generated at compile time by serde derive macros; unlike
/// Java reflection-based mapping, there is no runtime field inspection here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationType {
    /// Searches stored full-precision `f32` vectors without a compressed
    /// coarse representation.
    #[default]
    None,
    /// Maps every vector dimension to one byte using segment-wide calibration,
    /// normally reducing the vector payload to one quarter of its `f32` size.
    Scalar,
    /// Stores two-bit RaBitQ residual codes for the coarse scan while retaining
    /// full-precision vectors for exact reranking.
    TwoBit,
    /// Maps each learned subvector to one byte naming its nearest codebook
    /// centroid, trading more training work for substantially smaller codes.
    Product,
}
