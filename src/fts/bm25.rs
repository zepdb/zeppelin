//! BM25 scoring functions.
//!
//! BM25 scores are **higher is better** (relevance), unlike vector distances
//! which are **lower is better**.

/// BM25 parameters for a field.
#[derive(Debug, Clone, Copy)]
pub struct Bm25Params {
    /// Term-frequency saturation. Default 1.2.
    pub k1: f32,
    /// Document-length normalization. Default 0.75.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Compute Inverse Document Frequency for a term.
///
/// Uses the standard BM25 IDF formula:
///   IDF(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
///
/// where N = total documents, df(t) = documents containing term t.
#[must_use]
pub fn idf(total_docs: u32, doc_freq: u32) -> f32 {
    let n = total_docs as f64;
    let df = doc_freq as f64;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln() as f32
}

/// Compute the BM25 score contribution for a single term in a single document.
///
///   score(t, D) = IDF(t) * (tf(t,D) * (k1 + 1)) / (tf(t,D) + k1 * (1 - b + b * |D| / avgdl))
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

/// Compute the full BM25 score for a document given multiple query terms.
///
/// `term_data` is an iterator of (idf, term_frequency_in_doc) pairs.
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
    use super::*;

    #[test]
    fn test_idf_basic() {
        // A term appearing in 1 of 10 docs should have higher IDF than one in 5 of 10
        let idf_rare = idf(10, 1);
        let idf_common = idf(10, 5);
        assert!(idf_rare > idf_common);
        assert!(idf_rare > 0.0);
        assert!(idf_common > 0.0);
    }

    #[test]
    fn test_idf_rare_term_higher() {
        let idf_1 = idf(1000, 1);
        let idf_100 = idf(1000, 100);
        let idf_500 = idf(1000, 500);
        assert!(idf_1 > idf_100);
        assert!(idf_100 > idf_500);
    }

    #[test]
    fn test_idf_all_docs() {
        // A term in ALL documents still has positive IDF with BM25 formula
        let result = idf(10, 10);
        assert!(result >= 0.0);
    }

    #[test]
    fn test_bm25_single_term() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 2, 100, 100.0, &params);
        assert!(score > 0.0);
    }

    #[test]
    fn test_bm25_multi_term() {
        let params = Bm25Params::default();
        let idf1 = idf(100, 5);
        let idf2 = idf(100, 50);
        let score = bm25_score(&[(idf1, 1), (idf2, 2)], 100, 100.0, &params);
        assert!(score > 0.0);
    }

    #[test]
    fn test_bm25_length_normalization() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        // Short doc with same tf should score higher than long doc
        let score_short = bm25_term_score(term_idf, 2, 50, 100.0, &params);
        let score_long = bm25_term_score(term_idf, 2, 200, 100.0, &params);
        assert!(score_short > score_long);
    }

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

    #[test]
    fn test_bm25_zero_tf() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 0, 100, 100.0, &params);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_bm25_custom_params() {
        let params = Bm25Params { k1: 2.0, b: 0.5 };
        let term_idf = idf(100, 10);
        let score = bm25_term_score(term_idf, 3, 100, 100.0, &params);
        assert!(score > 0.0);
    }

    #[test]
    fn test_bm25_zero_avg_doc_length() {
        let params = Bm25Params::default();
        let term_idf = idf(100, 10);
        // Should not panic with avg_doc_length of 0
        let score = bm25_term_score(term_idf, 1, 0, 0.0, &params);
        assert!(score.is_finite());
    }
}
