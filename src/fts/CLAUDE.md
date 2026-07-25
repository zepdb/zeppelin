# src/fts — BM25 lexical retrieval

Configured string attributes are treated as documents. Compaction builds
persisted indexes; query evaluates BM25 over segments plus the uncompacted WAL
tail.

## Signatures worth memorizing

`bm25_term_score(term_idf: f32, term_freq: u32, doc_length: u32, avg_doc_length: f32, params)`

The argument order is easy to get wrong and the result is a plausible-looking
but incorrect ranking, so it fails silently. Check the call site.

## Two index shapes

- `inverted_index.rs` — per-segment, per-cluster postings.
- `global_index.rs` — one global per-segment term map with magic `ZGFTS`.
  This exists to make a lexical query **1 S3 GET instead of N cluster scans**.

A segment without a global FTS index falls back to a full scan and logs a
warning. `SegmentRef::has_global_fts` records which shape a segment has. If you
see BM25 latency spike, check that flag before profiling anything else.

## WAL tail

`wal_scan.rs` scans the uncompacted tail so freshly written documents are
searchable before compaction. It accepts only decoded fragments already
selected by an authoritative manifest snapshot — never re-derive the fragment
set here. `wal_cache.rs` caches decoded tail state.

## Tokenizer

Hardcoded **Lucene 36-word** stop list. **Do not add the `stop-words` crate** —
that was tried and rejected.

`rank_by.rs` owns query-side ranking selection.

## See also

- `tasks/tokenizer/` — the unexecuted Analysis-v2 plan (universal_v1 profile,
  golden-corpus stamps, ZGFTS2 weighted impact-ordered postings). Read it
  before redesigning tokenization; the design work is already done.
