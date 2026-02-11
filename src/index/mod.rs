//! Index module for Zeppelin vector search.
//!
//! Provides the `VectorIndex` trait, distance functions, post-filter
//! evaluation, quantization schemes, and concrete index implementations.

pub mod bitmap;
pub mod distance;
pub mod f16_storage;
pub mod filter;
pub mod hierarchical;
pub mod ivf_flat;
pub mod quantization;
pub mod traits;

// Re-export the core trait and the IVF-Flat implementation at the module level
// so callers can write `use crate::index::{VectorIndex, IvfFlatIndex}`.
pub use hierarchical::HierarchicalIndex;
pub use ivf_flat::IvfFlatIndex;
pub use traits::VectorIndex;
