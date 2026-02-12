//! Vector quantization module for Zeppelin.
//!
//! Provides compression schemes to reduce storage and bandwidth costs
//! while maintaining search quality through reranking.
//!
//! Supported schemes:
//! - **SQ8** (Scalar Quantization): Maps each f32 dimension to u8 (4x compression).
//!   Stores per-dimension min/max for reconstruction. Fast and simple.
//! - **PQ** (Product Quantization): Divides vectors into M subvectors,
//!   trains codebooks via k-means, stores M-byte codes (16-32x compression).

pub mod pq;
pub mod sq;

use serde::{Deserialize, Serialize};

/// Quantization method selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationType {
    /// No quantization — store full f32 vectors.
    #[default]
    None,
    /// Scalar quantization to uint8 (4x compression).
    Scalar,
    /// Product quantization (16-32x compression).
    Product,
}
