//! Computes the pure numeric BM25 relevance contributions used by Zeppelin's
//! WAL, per-cluster, and global full-text search paths.
//!
//! BM25 rewards documents that contain uncommon query terms, increases the
//! reward when a term occurs repeatedly but with diminishing returns, and can
//! discount long documents. This module owns only those arithmetic operations.
//! Tokenization lives in [`crate::fts::tokenizer`], inverted indexes provide
//! corpus statistics, and [`crate::fts::rank_by`] combines scores across fields.
//! There is no object-store access, visibility decision, allocation, or shared
//! mutation here.
//!
//! BM25 scores are **higher is better**. This is the opposite direction from
//! vector distances such as Euclidean distance, where a lower value is closer.
//!
//! ## Reading map
//!
//! 1. Start with [`Bm25Params`] for the two field-level tuning controls.
//! 2. Read [`idf`] for the corpus-wide rarity signal.
//! 3. Read [`bm25_term_score`] for one query-term contribution.
//! 4. Read [`bm25_score`] for the additive document score.
//!
//! ## Scoring flow
//!
//! ```text
//! corpus: total documents N + documents containing term df
//!                         |
//!                         v
//!                    IDF rarity
//!                         |
//! document: term frequency tf + token length dl + average length avgdl
//!                         |
//!                         v
//!             one saturated, length-normalized term score
//!                         |
//!                         v
//!              sum contributions for all query terms
//!                         |
//!                         v
//!              document BM25 score (higher is better)
//! ```
//!
//! ## Units and invariants
//!
//! - `N`, `df`, `tf`, and document length are counts. Document length is the
//!   number of retained analyzer tokens, not characters or bytes.
//! - Average document length is a token count represented as `f32`.
//! - Index statistics should satisfy `df <= N`; these helpers trust their
//!   callers and do not independently validate persisted statistics.
//! - Production parameters originate in a validated
//!   [`crate::fts::FtsFieldConfig`]. Direct callers that construct
//!   [`Bm25Params`] are responsible for equivalent bounds.
//! - A non-positive or unordered average length uses `1.0` to keep empty-corpus
//!   scoring defined. This is an arithmetic guard, not a fallback to another
//!   index or source of truth.
//!
//! ## Rust concepts used here
//!
//! The functions borrow [`Bm25Params`] and accept slices rather than taking
//! ownership of collections. In C terms these are checked pointer-and-length
//! views; in Java terms they resemble read-only access to an existing parameter
//! object and list. Iterator mapping and summation compile to a loop without
//! dynamic dispatch, while Rust prevents mutation of the borrowed inputs.

/// Groups the two dimensionless tuning parameters used by BM25 term scoring.
///
/// Query paths usually copy these values from a validated
/// [`crate::fts::FtsFieldConfig`]. This lightweight runtime type is [`Copy`], so
/// passing or returning it duplicates two `f32` values rather than allocating
/// or sharing heap storage.
///
/// # Examples
///
/// `k1 = 1.2` and `b = 0.75` provide Zeppelin's default balance between term
/// repetition and document-length normalization.
#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    /// Term-frequency saturation control; validated field configurations keep
    /// this finite and in `(0, 10]`.
    ///
    /// A larger value lets repeated occurrences add relevance for longer before
    /// the contribution flattens. The default is `1.2`.
    pub k1: f32,
    /// Document-length normalization strength; validated configurations keep
    /// this finite and in `[0, 1]`.
    ///
    /// Zero ignores document length, while one applies the full ratio between
    /// the document and corpus-average lengths. The default is `0.75`.
    pub b: f32,
}

impl Default for Bm25Params {
    /// Builds Zeppelin's default BM25 tuning values.
    ///
    /// # Returns
    ///
    /// Returns `k1 = 1.2` and `b = 0.75`.
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Computes the inverse document frequency (IDF) weight for one term.
///
/// IDF represents corpus rarity: a term found in few documents receives a
/// larger positive weight than a term found nearly everywhere. The calculation
/// uses the BM25 form:
///
/// ```text
/// IDF(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
/// ```
///
/// # Parameters
///
/// - `total_docs`: `N`, the number of documents in the relevant field corpus.
/// - `doc_freq`: `df(t)`, the number of those documents containing the term at
///   least once. A valid index statistic does not exceed `total_docs`.
///
/// # Returns
///
/// Returns the dimensionless natural-log rarity weight as `f32`. Under valid
/// corpus statistics it is finite and positive, including for a term present in
/// every document. The calculation uses `f64` internally before narrowing the
/// result.
///
/// # Examples
///
/// In a 1,000-document field, a term appearing once receives a higher IDF than
/// one appearing in 100 documents, which in turn outranks one appearing in 500.
///
/// # Rust Notes for Java/C Engineers
///
/// Integer counts are explicitly converted to `f64`; Rust does not perform the
/// implicit numeric promotions familiar from Java and C. The final `as f32`
/// narrowing is visible in the implementation, making the precision boundary
/// explicit.
#[must_use]
pub fn idf(total_docs: u32, doc_freq: u32) -> f32 {
    let n = total_docs as f64;
    let df = doc_freq as f64;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln() as f32
}

/// Computes one query term's BM25 contribution for one document.
///
/// Term frequency raises the score with saturation controlled by `k1`.
/// Document length is compared with the field's average and blended according
/// to `b`. The implemented formula is:
///
/// ```text
/// score(t, D) = IDF(t) * tf(t,D) * (k1 + 1)
///               -----------------------------------------------
///               tf(t,D) + k1 * (1 - b + b * |D| / avgdl)
/// ```
///
/// # Parameters
///
/// - `term_idf`: Precomputed dimensionless rarity weight from [`idf`].
/// - `term_freq`: Number of occurrences of this analyzed term in the document.
/// - `doc_length`: Total retained-token count for the document field.
/// - `avg_doc_length`: Mean retained-token count for documents in that field's
///   corpus. Values that are not greater than zero, including `NaN`, are
///   replaced with `1.0` for this calculation.
/// - `params`: Borrowed saturation and length-normalization controls. Production
///   callers should use values from a validated field configuration.
///
/// # Returns
///
/// Returns this term's relevance contribution. A zero term frequency produces
/// zero with valid parameters. If the computed denominator is non-positive,
/// the function also returns zero. Non-finite `term_idf` or parameters can still
/// produce a non-finite result because this low-level helper does not validate
/// arbitrary direct inputs.
///
/// # Performance
///
/// Performs constant-time floating-point arithmetic with no allocation or I/O.
/// It is called inside posting-list loops, so keeping it small avoids per-term
/// heap work on the query path.
///
/// # Examples
///
/// For equal IDF and term frequency, a 50-token document scores higher than a
/// 200-token document when the corpus average is 100 tokens and `b` is positive.
/// Raising term frequency from 1 to 10 increases relevance, but by less than a
/// factor of ten because `k1` saturates repetition.
#[must_use]
pub fn bm25_term_score(
    term_idf: f32,
    term_freq: u32,
    doc_length: u32,
    avg_doc_length: f32,
    params: &Bm25Params,
) -> f32 {
    let tf = term_freq as f32;
    let dl = doc_length as f32;
    let avgdl = if avg_doc_length > 0.0 {
        avg_doc_length
    } else {
        1.0
    };

    let numerator = tf * (params.k1 + 1.0);
    let denominator = tf + params.k1 * (1.0 - params.b + params.b * dl / avgdl);

    if denominator <= 0.0 {
        return 0.0;
    }

    term_idf * numerator / denominator
}

/// Adds all supplied query-term contributions into one document BM25 score.
///
/// Each pair describes one analyzed query-token occurrence as its IDF and its
/// frequency in this document. Duplicate pairs are intentionally scored more
/// than once; callers decide whether repeated query terms should remain in the
/// slice.
///
/// # Parameters
///
/// - `term_data`: Borrowed `(IDF, term frequency)` pairs in query-token order.
/// - `doc_length`: Total retained-token count for this document field.
/// - `avg_doc_length`: Mean retained-token count for the field corpus.
/// - `params`: Borrowed field-level BM25 controls used for every term.
///
/// # Returns
///
/// Returns the floating-point sum of [`bm25_term_score`] for every pair. An
/// empty slice returns `0.0`. Mathematically the order does not matter, although
/// ordinary floating-point rounding follows slice order.
///
/// # Performance
///
/// Runs in `O(q)` time for `q` supplied query-token occurrences and allocates
/// nothing. Corpus lookup and posting traversal happen before this helper.
///
/// # Examples
///
/// A document matching both a rare `zeppelin` term and a common `search` term
/// receives the sum of both contributions; the rarer term normally contributes
/// more through its larger IDF.
///
/// # Rust Notes for Java/C Engineers
///
/// `&[(f32, u32)]` is a borrowed contiguous slice: unlike a Java collection it
/// cannot contain null entries, and unlike a raw C pointer Rust carries and
/// checks the length. The closure destructures each copied numeric tuple, while
/// the original slice remains owned and reusable by the caller.
#[must_use]
pub fn bm25_score(
    term_data: &[(f32, u32)],
    doc_length: u32,
    avg_doc_length: f32,
    params: &Bm25Params,
) -> f32 {
    term_data
        .iter()
        .map(|&(term_idf, tf)| bm25_term_score(term_idf, tf, doc_length, avg_doc_length, params))
        .sum()
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Protects the scorer's rarity, saturation, length-normalization, and
    //! arithmetic-guard contracts without involving an inverted index.

    use super::*;

    /// Verifies rarity increases IDF and valid weights remain positive.
    #[test]
    fn test_idf_basic() {
        // A term appearing in 1 of 10 docs should have higher IDF than one in 5 of 10
        let idf_rare = idf(10, 1);
        let idf_common = idf(10, 5);
        assert!(idf_rare > idf_common);
        assert!(idf_rare > 0.0);
        assert!(idf_common > 0.0);
    }

    /// Verifies IDF decreases monotonically as document frequency increases.
    #[test]
    fn test_idf_rare_term_higher() {
        let idf_1 = idf(1000, 1);
        let idf_100 = idf(1000, 100);
        let idf_500 = idf(1000, 500);
        assert!(idf_1 > idf_100);
        assert!(idf_100 > idf_500);
    }

    /// Verifies a ubiquitous term retains a non-negative BM25 IDF.
    #[test]
    fn test_idf_all_docs() {
        // A term in ALL documents still has positive IDF with BM25 formula
        let result = idf(10, 10);
        assert!(result >= 0.0);
    }

    /// Verifies valid single-term statistics produce positive relevance.
    #[test]
    fn test_bm25_single_term() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 2, 100, 100.0, &params);
        assert!(score > 0.0);
    }

    /// Verifies contributions from multiple query terms are accumulated.
    #[test]
    fn test_bm25_multi_term() {
        let params = Bm25Params::default();
        let idf1 = idf(100, 5);
        let idf2 = idf(100, 50);
        let score = bm25_score(&[(idf1, 1), (idf2, 2)], 100, 100.0, &params);
        assert!(score > 0.0);
    }

    /// Verifies positive `b` rewards a shorter document at equal frequency.
    #[test]
    fn test_bm25_length_normalization() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        // Short doc with same tf should score higher than long doc
        let score_short = bm25_term_score(term_idf, 2, 50, 100.0, &params);
        let score_long = bm25_term_score(term_idf, 2, 200, 100.0, &params);
        assert!(score_short > score_long);
    }

    /// Verifies repetition raises scores with sublinear term saturation.
    #[test]
    fn test_bm25_term_saturation() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        // Doubling tf should NOT double the score (saturation)
        let score_1 = bm25_term_score(term_idf, 1, 100, 100.0, &params);
        let score_10 = bm25_term_score(term_idf, 10, 100, 100.0, &params);
        let score_100 = bm25_term_score(term_idf, 100, 100, 100.0, &params);
        assert!(score_10 > score_1);
        assert!(score_100 > score_10);
        // But the ratio should be sublinear
        assert!(score_100 / score_1 < 100.0);
    }

    /// Verifies an absent term contributes exactly zero.
    #[test]
    fn test_bm25_zero_tf() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 0, 100, 100.0, &params);
        assert_eq!(score, 0.0);
    }

    /// Verifies non-default valid tuning values remain usable by the scorer.
    #[test]
    fn test_bm25_custom_params() {
        let params = Bm25Params { k1: 2.0, b: 0.5 };
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 3, 100, 100.0, &params);
        assert!(score > 0.0);
    }

    /// Verifies an empty-corpus average uses the finite arithmetic guard.
    #[test]
    fn test_bm25_zero_avg_doc_length() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        // Should not panic with avg_doc_length of 0
        let score = bm25_term_score(term_idf, 1, 0, 0.0, &params);
        assert!(score.is_finite());
    }
}
