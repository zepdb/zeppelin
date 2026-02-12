//! Full-text search (BM25) module for Zeppelin.
//!
//! Provides tokenization, BM25 scoring, inverted index construction,
//! and query evaluation for text-based search alongside vector search.

pub mod bm25;
pub mod inverted_index;
pub mod rank_by;
pub mod tokenizer;
pub mod types;
pub mod wal_cache;
pub mod wal_scan;

pub use types::{FtsFieldConfig, FtsLanguage};
