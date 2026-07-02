//! Unicode-aware tokenizer with stemming and stopword removal,
//! plus the FTS field configuration types that drive it.

use std::collections::HashSet;

use rust_stemmers::{Algorithm, Stemmer};
use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

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

/// Tokenize text according to the given field configuration.
///
/// Steps:
/// 1. Unicode word segmentation
/// 2. Lowercase (unless case_sensitive)
/// 3. Discard tokens exceeding max_token_length
/// 4. Remove stopwords (if enabled)
/// 5. Apply stemming (if enabled)
///
/// When `prefix_mode` is true, the last token is NOT stemmed (for prefix matching).
#[must_use]
pub fn tokenize_text(text: &str, config: &FtsFieldConfig, prefix_mode: bool) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }

    let stopwords: &HashSet<&str> = if config.remove_stopwords {
        load_stopwords(config.language)
    } else {
        &EMPTY_STOPWORDS
    };

    let stemmer = if config.stemming {
        Some(create_stemmer(config.language))
    } else {
        None
    };

    let words: Vec<&str> = text.unicode_words().collect();
    let word_count = words.len();

    words
        .into_iter()
        .enumerate()
        .filter_map(|(i, word)| {
            let token = if config.case_sensitive {
                word.to_string()
            } else {
                word.to_lowercase()
            };

            if token.len() > config.max_token_length || token.is_empty() {
                return None;
            }

            if config.remove_stopwords && stopwords.contains(token.as_str()) {
                return None;
            }

            let is_last = i == word_count - 1;
            if let Some(ref stemmer) = stemmer {
                if !(prefix_mode && is_last) {
                    return Some(stemmer.stem(&token).into_owned());
                }
            }

            Some(token)
        })
        .collect()
}

fn create_stemmer(language: FtsLanguage) -> Stemmer {
    match language {
        FtsLanguage::English => Stemmer::create(Algorithm::English),
    }
}

/// Lucene/Elasticsearch-compatible English stopwords (36 words).
/// Only true function words: articles, prepositions, conjunctions,
/// pronouns, auxiliaries. No content words that users would search for.
static ENGLISH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

lazy_static::lazy_static! {
    static ref ENGLISH_STOPWORD_SET: HashSet<&'static str> =
        ENGLISH_STOPWORDS.iter().copied().collect();
    static ref EMPTY_STOPWORDS: HashSet<&'static str> = HashSet::new();
}

fn load_stopwords(language: FtsLanguage) -> &'static HashSet<&'static str> {
    match language {
        FtsLanguage::English => &ENGLISH_STOPWORD_SET,
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> FtsFieldConfig {
        FtsFieldConfig::default()
    }

    #[test]
    fn test_basic_tokenization() {
        let tokens = tokenize_text("Hello World", &default_config(), false);
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn test_lowercase() {
        let tokens = tokenize_text("RUST Programming", &default_config(), false);
        assert!(tokens.contains(&"rust".to_string()));
        assert!(tokens.contains(&"program".to_string())); // stemmed
    }

    #[test]
    fn test_stemming() {
        let config = default_config();
        let tokens = tokenize_text("running quickly", &config, false);
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn test_stopword_removal() {
        let tokens = tokenize_text("the quick brown fox", &default_config(), false);
        assert!(!tokens.iter().any(|t| t == "the"));
        assert!(tokens.iter().any(|t| t == "quick" || t == "brown"));
    }

    #[test]
    fn test_max_token_length() {
        let mut config = default_config();
        config.max_token_length = 5;
        let tokens = tokenize_text("hi superlongword ok", &config, false);
        assert!(tokens.iter().any(|t| t == "hi"));
        assert!(!tokens.iter().any(|t| t.contains("superlong")));
    }

    #[test]
    fn test_empty_string() {
        let tokens = tokenize_text("", &default_config(), false);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_only_stopwords() {
        let tokens = tokenize_text("the is at", &default_config(), false);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_unicode_punctuation() {
        let tokens = tokenize_text("hello, world! how's it?", &default_config(), false);
        assert!(tokens.iter().any(|t| t == "hello"));
    }

    #[test]
    fn test_prefix_mode() {
        let config = default_config();
        let tokens = tokenize_text("running", &config, true);
        assert_eq!(tokens, vec!["running"]);

        let tokens_normal = tokenize_text("running", &config, false);
        assert_eq!(tokens_normal, vec!["run"]);
    }

    #[test]
    fn test_no_stemming() {
        let mut config = default_config();
        config.stemming = false;
        let tokens = tokenize_text("running quickly", &config, false);
        assert!(tokens.contains(&"running".to_string()));
        assert!(tokens.contains(&"quickly".to_string()));
    }

    #[test]
    fn test_case_sensitive() {
        let mut config = default_config();
        config.case_sensitive = true;
        let tokens = tokenize_text("Hello WORLD", &config, false);
        assert!(tokens
            .iter()
            .any(|t| t.starts_with('H') || t.starts_with('W')));
    }

    #[test]
    fn test_no_stopword_removal() {
        let mut config = default_config();
        config.remove_stopwords = false;
        let tokens = tokenize_text("the cat", &config, false);
        assert!(tokens.len() >= 2);
    }

    #[test]
    fn test_stopwords_are_sane() {
        // Content words should NOT be removed as stopwords
        let config = default_config();
        for word in &[
            "hello", "world", "computer", "system", "help", "run", "work", "quick",
        ] {
            let tokens = tokenize_text(word, &config, false);
            assert!(
                !tokens.is_empty(),
                "'{word}' should not be treated as a stopword"
            );
        }
    }

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
}
