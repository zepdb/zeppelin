//! Unicode-aware tokenizer with stemming and stopword removal.

use std::collections::HashSet;

use rust_stemmers::{Algorithm, Stemmer};
use unicode_segmentation::UnicodeSegmentation;

use crate::fts::types::{FtsFieldConfig, FtsLanguage};

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
}
