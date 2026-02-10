//! Index module for Zeppelin vector search.
//!
//! Provides the `VectorIndex` trait, distance functions, post-filter
//! evaluation, and concrete index implementations (currently IVF-Flat).

pub mod distance;
pub mod filter;
pub mod ivf_flat;
pub mod traits;

// Re-export the core trait and the IVF-Flat implementation at the module level
// so callers can write `use crate::index::{VectorIndex, IvfFlatIndex}`.
pub use ivf_flat::IvfFlatIndex;
pub use traits::VectorIndex;
