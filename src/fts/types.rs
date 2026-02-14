//! FTS configuration types.

use serde::{Deserialize, Serialize};

/// Supported languages for full-text search tokenization.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FtsLanguage {
    /// English language tokenization and stemming.
    #[default]
    English,
}

/// Per-field configuration for full-text search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtsFieldConfig {
    /// Language for tokenization and stemming.
    #[serde(default)]
    pub language: FtsLanguage,
    /// Whether to apply stemming (e.g., "running" -> "run").
    #[serde(default = "default_true")]
    pub stemming: bool,
    /// Whether to remove stopwords (e.g., "the", "is", "at").
    #[serde(default = "default_true")]
    pub remove_stopwords: bool,
    /// Whether matching is case-sensitive.
    #[serde(default)]
    pub case_sensitive: bool,
    /// BM25 k1 parameter: term frequency saturation. Default 1.2.
    #[serde(default = "default_k1")]
    pub k1: f32,
    /// BM25 b parameter: length normalization. Default 0.75.
    #[serde(default = "default_b")]
    pub b: f32,
    /// Maximum token length; tokens exceeding this are discarded.
    #[serde(default = "default_max_token_length")]
    pub max_token_length: usize,
}

fn default_true() -> bool {
    true
}

fn default_k1() -> f32 {
    1.2
}

fn default_b() -> f32 {
    0.75
}

fn default_max_token_length() -> usize {
    40
}

impl Default for FtsFieldConfig {
    fn default() -> Self {
        Self {
            language: FtsLanguage::default(),
            stemming: true,
            remove_stopwords: true,
            case_sensitive: false,
            k1: default_k1(),
            b: default_b(),
            max_token_length: default_max_token_length(),
        }
    }
}

/// Corpus-level statistics needed for BM25 scoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusStats {
    /// Total number of documents in the corpus.
    pub doc_count: u32,
    /// Average document length (in tokens) for each field.
    pub avg_doc_length: f32,
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fts_language_default() {
        assert_eq!(FtsLanguage::default(), FtsLanguage::English);
    }

    #[test]
    fn test_fts_field_config_default() {
        let cfg = FtsFieldConfig::default();
        assert_eq!(cfg.language, FtsLanguage::English);
        assert!(cfg.stemming);
        assert!(cfg.remove_stopwords);
        assert!(!cfg.case_sensitive);
        assert!((cfg.k1 - 1.2).abs() < f32::EPSILON);
        assert!((cfg.b - 0.75).abs() < f32::EPSILON);
        assert_eq!(cfg.max_token_length, 40);
    }

    #[test]
    fn test_fts_field_config_serde_roundtrip() {
        let cfg = FtsFieldConfig {
            language: FtsLanguage::English,
            stemming: false,
            remove_stopwords: false,
            case_sensitive: true,
            k1: 1.5,
            b: 0.5,
            max_token_length: 50,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: FtsFieldConfig = serde_json::from_str(&json).unwrap();
        assert!(!back.stemming);
        assert!(!back.remove_stopwords);
        assert!(back.case_sensitive);
        assert!((back.k1 - 1.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_fts_field_config_from_empty_json() {
        let cfg: FtsFieldConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.language, FtsLanguage::English);
        assert!(cfg.stemming);
        assert!(cfg.remove_stopwords);
    }

    #[test]
    fn test_fts_language_serde_roundtrip() {
        let json = serde_json::to_string(&FtsLanguage::English).unwrap();
        assert_eq!(json, "\"english\"");
        let back: FtsLanguage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, FtsLanguage::English);
    }

    #[test]
    fn test_corpus_stats_serde_roundtrip() {
        let stats = CorpusStats {
            doc_count: 100,
            avg_doc_length: 25.5,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: CorpusStats = serde_json::from_str(&json).unwrap();
        assert_eq!(back.doc_count, 100);
        assert!((back.avg_doc_length - 25.5).abs() < f32::EPSILON);
    }
}
