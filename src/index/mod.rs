//! Shared contracts and implementations for Zeppelin vector indexing.
//!
//! This module is the boundary between compaction/query orchestration and the
//! algorithms that build or search immutable vector-index artifacts. Callers
//! use [`crate::index::VectorIndex`] when the flat and hierarchical
//! implementations share a
//! contract, and enter the concrete modules when they need layout-specific
//! controls such as grouped object reads or hierarchical beam search.
//!
//! The module does not choose which segment is visible. Compaction writes a new
//! segment through an index implementation and later publishes it through the
//! authoritative manifest. Query execution first selects a manifest-visible
//! segment, loads its concrete handle, and then searches immutable objects
//! through [`crate::storage::ZeppelinStore`]. Memory and local disk remain
//! optimizations rather than sources of truth.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::index::VectorIndex`] for the build/search contract
//!    common to both production implementations.
//! 2. Read [`crate::index::ivf_flat`] for centroid routing, grouped cluster
//!    objects, quantization, and exact reranking.
//! 3. Read [`crate::index::hierarchical`] for multi-level centroid-tree
//!    construction and beam traversal.
//! 4. Use [`crate::index::distance`] and `topk` to understand shared score
//!    ordering and bounded result selection.
//! 5. Read [`crate::index::bitmap`] and [`crate::index::filter`] for metadata
//!    prefiltering and exact predicate evaluation.
//! 6. Read [`crate::index::quantization`] for the memory, I/O, and recall
//!    tradeoffs of SQ8 and product-quantized candidate scoring.
//! 7. Read [`crate::index::late_interaction`] for MUVERA FDE construction and
//!    scalar MaxSim scoring.
//!
//! ## Build and search boundaries
//!
//! ```text
//! compaction rows
//!       |
//!       v
//! VectorIndex::build ----> immutable segment objects in S3/MinIO
//!       |                              |
//!       | handle + refs                | not visible yet
//!       v                              v
//! compaction publishes authoritative manifest
//!
//! authoritative manifest selects segment
//!       |
//!       v
//! concrete index handle ----> VectorIndex::search
//!                                      |
//!                                      | read selected immutable objects
//!                                      v
//!                              ranked segment results
//! ```
//!
//! ## Invariants
//!
//! - Building objects and publishing a manifest are separate operations.
//! - Segment objects are immutable and addressed through
//!   [`crate::storage::ZeppelinStore`].
//! - Every vector in one index has the same positive dimension.
//! - A query handle represents one segment selected by a manifest snapshot; a
//!   search does not silently switch to another segment.
//! - Approximate routing may reduce recall, but returned vector scores use the
//!   requested distance metric and order better matches before worse matches.
//! - Missing, corrupt, or inconsistent artifacts return errors rather than an
//!   empty fallback index.
//!
//! ## Rust concepts used here
//!
//! [`crate::index::VectorIndex`] is a trait, closest to a Java interface or a C
//! table of function pointers. Its `Send + Sync` bounds let handles cross
//! thread and task boundaries safely. Rust checks those properties from the
//! handle's fields instead of relying on a runtime convention.
//!
//! [the `async_trait` macro][macro@async_trait::async_trait] turns async trait
//! methods into object-safe
//! futures. Borrowed slices such as `&[VectorEntry]` and `&[f32]` let an
//! implementation read caller-owned batches without copying them. `build`
//! returns `Self`, so the caller receives an owned concrete handle that can
//! outlive those input borrows.

pub mod bitmap;
pub mod distance;
pub mod filter;
pub mod hierarchical;
pub mod ivf_flat;
pub mod late_interaction;
pub mod quantization;
pub(crate) mod topk;

// Keep the two production handles beside `VectorIndex` in the public API so a
// caller can import the contract and its chosen implementation together.
pub use hierarchical::HierarchicalIndex;
pub use ivf_flat::IvfFlatIndex;

use async_trait::async_trait;

use crate::config::IndexingConfig;
use crate::error::Result;
use crate::storage::ZeppelinStore;
use crate::types::{DistanceMetric, Filter, SearchResult, VectorEntry};

/// Common lifecycle contract for a persisted vector-index implementation.
///
/// [`IvfFlatIndex`] and [`HierarchicalIndex`] implement this interface so
/// callers can use the same build, search, count, and dimension vocabulary.
/// Layout-specific loaders remain inherent methods on the concrete handles
/// because manifest metadata differs between the two formats.
///
/// `Send + Sync` requires an implementation to be safe to move between threads
/// and to share by reference across concurrent query tasks. Implementations
/// must not hide mutable, thread-unsafe state behind this interface.
///
/// # Consistency
///
/// A successful build means immutable objects exist, not that readers can see
/// them. A higher layer must publish their segment reference through the
/// manifest. Search assumes the handle was created for a segment already chosen
/// from one authoritative manifest snapshot.
///
/// # Examples
///
/// Compaction can select [`IvfFlatIndex`] for a normal segment or
/// [`HierarchicalIndex`] for a larger one, call [`VectorIndex::build`], and
/// publish the returned segment metadata afterward. Query execution later calls
/// [`VectorIndex::search`] on the matching concrete handle.
///
/// # Rust Notes for Java/C Engineers
///
/// The trait resembles a Java interface, while `dyn VectorIndex` resembles a
/// Java interface reference with runtime dispatch. In C, the closest model is a
/// struct containing an opaque data pointer and function pointers. Rust also
/// checks ownership, lifetimes, and `Send + Sync` thread-safety bounds.
///
/// [The `async_trait` macro][macro@async_trait::async_trait] boxes the future
/// produced by each async method when used
/// through the trait abstraction. The `Self: Sized` restriction on `build`
/// keeps construction tied to a concrete implementation because a runtime
/// trait object does not have one statically known return size.
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Builds one immutable segment from a complete vector batch.
    ///
    /// Implementations validate the batch, train their routing structures,
    /// encode row/attribute/quantization artifacts, and write those objects
    /// through the storage boundary. The method deliberately stops before
    /// manifest publication.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed rows to place in the segment. The batch must be
    ///   non-empty and all vector dimensions must match.
    /// - `config`: Borrowed training, layout, quantization, and bitmap choices.
    /// - `store`: Object-store boundary receiving immutable segment artifacts.
    /// - `namespace`: Namespace prefix used in each artifact key.
    /// - `segment_id`: New, caller-chosen segment identity. Reusing an identity
    ///   would violate the immutable-artifact contract.
    ///
    /// # Returns
    ///
    /// An owned concrete index handle containing resident routing metadata and
    /// references needed by the implementation's caller.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or dimensionally invalid input, failed
    /// training/encoding, arithmetic or format violations, and object-store PUT
    /// failures. A late error may leave already written but unreferenced objects
    /// in storage; they remain invisible because no manifest was published.
    ///
    /// # Side Effects
    ///
    /// Uses CPU and memory for training/encoding and performs object-store PUTs.
    /// It does not publish or mutate the namespace manifest.
    ///
    /// # Consistency
    ///
    /// Every object for this build uses `segment_id` or an explicitly recorded
    /// owner. Callers must publish the complete segment description only after
    /// this method succeeds and must never update those objects in place.
    ///
    /// # Performance
    ///
    /// Cost grows with vector count, dimension, and the selected training and
    /// quantization algorithms. Implementations may write independent objects
    /// concurrently, but async execution must not block Tokio worker threads.
    ///
    /// # Examples
    ///
    /// A compaction batch of 100,000 rows can build `seg-42`. On success its
    /// immutable objects exist, but queries still see the previous manifest
    /// until compaction publishes a segment reference. If a later PUT fails,
    /// the partial objects stay unreferenced rather than becoming a segment.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `vectors` and `config` are shared borrows. Unlike an ordinary Java
    /// reference or C pointer, Rust proves they remain valid while the future
    /// uses them. Returning `Self` transfers an owned handle to the caller; it
    /// contains no borrow into the input batch.
    async fn build(
        vectors: &[VectorEntry],
        config: &IndexingConfig,
        store: &ZeppelinStore,
        namespace: &str,
        segment_id: &str,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Searches one immutable segment for its nearest matching rows.
    ///
    /// Approximate routing limits the remote clusters read before exact
    /// filtering and final result construction. IVF-Flat interprets `nprobe`
    /// as a centroid-region budget; the hierarchical implementation uses it as
    /// its beam-width input.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed vector whose component count must equal
    ///   [`VectorIndex::dimension`].
    /// - `top_k`: Maximum result count. Implementations return an empty
    ///   collection for zero.
    /// - `nprobe`: Implementation-specific search breadth controlling the
    ///   recall, object-read, and CPU tradeoff.
    /// - `filter`: Optional borrowed metadata predicate. Prefilters may reduce
    ///   I/O, but exact evaluation determines whether a row matches.
    /// - `distance_metric`: Metric used for routing and returned vector scores.
    /// - `store`: Object-store boundary used to read immutable segment data.
    ///
    /// # Returns
    ///
    /// Up to `top_k` [`SearchResult`] values ordered from best to worst score.
    /// Fewer values means the searched regions contained fewer matching rows.
    /// Unless every region is examined, absence from the result is not proof
    /// that no closer vector exists elsewhere in the segment.
    ///
    /// # Errors
    ///
    /// Returns a dimension mismatch for an incompatible query and propagates
    /// storage, decoding, corruption, and routing errors for required objects.
    /// Implementations fail loudly rather than treating missing data as an
    /// empty cluster.
    ///
    /// # Side Effects
    ///
    /// Performs read-only object-store requests and emits tracing or metrics.
    /// The current trait wrappers do not use the optional disk cache; concrete
    /// query entry points accept one when the coordinator has a cache. Search
    /// never modifies segment artifacts or publishes a manifest.
    ///
    /// # Consistency
    ///
    /// Search stays on the immutable segment represented by `self`. The query
    /// coordinator, not this trait method, selects that handle from the current
    /// authoritative manifest and merges its results with other visible data.
    ///
    /// # Performance
    ///
    /// Every implementation scores resident routing data, then reads selected
    /// remote objects. Raising `nprobe` normally reads and scores more data for
    /// better recall. Filters and quantization can change bytes read and CPU.
    ///
    /// # Examples
    ///
    /// With `top_k = 10`, a narrow probe budget may return ten close category
    /// matches after reading a small part of the segment. Increasing `nprobe`
    /// searches more regions and may replace those rows with closer matches.
    /// A corrupt selected object returns an error, not a shorter result list.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&self`, `&[f32]`, and `Option<&Filter>` borrow existing values across
    /// the async operation. The returned `Vec<SearchResult>` is owned, so the
    /// caller may retain it after all those borrows end. `Option` represents
    /// filter absence without a nullable pointer.
    async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        filter: Option<&Filter>,
        distance_metric: DistanceMetric,
        store: &ZeppelinStore,
    ) -> Result<Vec<SearchResult>>;

    /// Reports how many vectors the segment represents.
    ///
    /// # Returns
    ///
    /// A resident count obtained during build or load. Implementations must not
    /// issue object-store reads merely to answer this accessor.
    ///
    /// # Examples
    ///
    /// A handle loaded from a manifest entry with one million rows returns
    /// `1_000_000` without fetching its cluster payloads.
    fn vector_count(&self) -> usize;

    /// Reports the required component count for stored and query vectors.
    ///
    /// # Returns
    ///
    /// The positive dimension decoded or validated when the handle was built
    /// or loaded.
    ///
    /// # Examples
    ///
    /// A 768-dimensional segment returns `768`; search rejects a query with a
    /// different length before treating it as a valid vector for that segment.
    fn dimension(&self) -> usize;
}
