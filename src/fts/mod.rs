//! Lexical retrieval primitives shared by WAL scans, segment builds, and queries.
//!
//! Zeppelin treats a vector's configured string attributes as text documents.
//! This module normalizes that text, records term-to-document postings in
//! immutable segment artifacts, evaluates BM25 relevance, and handles the
//! uncompacted WAL tail that has not reached a segment yet. BM25 is a lexical
//! ranking formula: rare query terms contribute more than common terms, repeated
//! terms saturate, and document length can reduce a term's contribution.
//!
//! Compaction enters through [`crate::fts::inverted_index`] and
//! [`crate::fts::global_index`] to build
//! persisted indexes. Query execution in [`crate::query`] enters through
//! [`crate::fts::rank_by`], [`crate::fts::tokenizer`], the segment search
//! functions, and [`crate::fts::wal_scan`].
//! Storage and manifest code remain outside this module: an index object's
//! existence does not make it visible, and WAL scanning accepts only decoded
//! fragments already selected by an authoritative manifest snapshot.
//!
//! ```text
//! write + manifest publication                 compaction
//!             |                                    |
//!             v                                    v
//! visible uncompacted WAL                  immutable segment text
//!             |                             attributes by cluster
//!             v                                    |
//! wal_scan -- optional wal_cache                   +--> inverted_index
//!             |                                    +--> global_index
//!             |                                             |
//!             +------------------+--------------------------+
//!                                v
//!                    tokenize query + evaluate BM25
//!                                |
//!                                v
//!                    RankBy composition and top-k
//!                                |
//!                                v
//!                  query layer merges WAL + segment hits
//! ```
//!
//! ## Reading map
//!
//! 1. Start with [`crate::fts::tokenizer`] and the re-exported
//!    [`crate::fts::FtsFieldConfig`] to learn how fields and queries become
//!    normalized tokens.
//! 2. Read [`crate::fts::bm25`] for the higher-is-better scoring arithmetic.
//! 3. Read [`crate::fts::rank_by`] for the client-facing expression tree that
//!    combines field scores.
//! 4. Read [`crate::fts::inverted_index`] for per-cluster postings and
//!    [`crate::fts::global_index`] for the per-segment acceleration layer.
//! 5. Finish with [`crate::fts::wal_scan`] and [`crate::fts::wal_cache`] for
//!    strong-query coverage of newly published, uncompacted updates.
//!
//! ## Invariants
//!
//! - Build and query paths must apply compatible [`crate::fts::FtsFieldConfig`]
//!   settings; case folding, stemming, and stop-word choices define term
//!   identity.
//! - BM25 and composed lexical scores are higher-is-better. Vector distance
//!   uses the opposite ordering, so callers must select the correct comparator.
//! - Segment index bytes and WAL fragments are immutable artifacts. Only their
//!   presence in the current manifest establishes visibility.
//! - A strong merge must let the latest WAL upsert or tombstone suppress an
//!   older copy from a segment, even when the WAL record does not match.
//! - In-memory token caches are disposable derived data and never override S3
//!   or manifest state.
//!
//! ## Rust concepts used here
//!
//! Rust modules provide a compiler-enforced boundary similar to Java packages
//! with explicit exports, while being resolved statically like C translation
//! units plus headers. `pub use` below re-exports
//! [`crate::fts::FtsFieldConfig`] and [`crate::fts::FtsLanguage`] as the stable
//! configuration surface without exposing callers to their physical file
//! location. Serde-derived persisted structs own their decoded data, and
//! `Result`-returning decoders fail loudly on invalid artifact headers rather
//! than returning partially initialized indexes.

pub mod bm25;
pub mod global_index;
pub mod inverted_index;
pub mod rank_by;
pub mod tokenizer;
pub mod wal_cache;
pub mod wal_scan;

pub use tokenizer::{FtsFieldConfig, FtsLanguage};
