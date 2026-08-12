//! Defines Zeppelin's full-text analyzer configuration and turns text into the
//! tokens stored and queried by the FTS indexes.
//!
//! This is the shared analysis boundary between indexing and retrieval. Segment
//! compaction and the WAL token cache call [`tokenize_text`] when indexing
//! document attributes; BM25 query paths call the same function for query text.
//! A namespace persists one [`FtsFieldConfig`] per searchable field in its
//! metadata. Using that same configuration on both sides is essential: a query
//! token can match a posting only if case folding, stopword removal, and
//! stemming made the same decisions when the posting was built.
//!
//! This module is CPU-only. It neither reads object storage nor decides which
//! WAL fragments or segments are visible; callers perform those operations and
//! pass borrowed text and configuration into this leaf module.
//!
//! ## Reading map
//!
//! 1. Start with [`FtsFieldConfig`] for the persisted analyzer and BM25 knobs.
//! 2. Read [`FtsFieldConfig::validate`] for the creation-time numeric bounds.
//! 3. Read [`tokenize_text`] for the indexing and query analysis pipeline.
//! 4. `create_stemmer` and `load_stopwords` select the language-specific tools.
//!
//! ## Analysis pipeline
//!
//! ```text
//! UTF-8 text
//!     |
//!     v
//! Unicode word boundaries, in source order
//!     |
//!     v
//! optional lowercase -> byte-length limit -> optional stopword removal
//!     |
//!     +---- ordinary mode ------> optional English stemming
//!     |
//!     `---- prefix mode --------> keep the final normalized word unstemmed
//!                                      |
//!                                      v
//!                              owned Vec<String>
//! ```
//!
//! ## Invariants and compatibility
//!
//! - Indexing and querying a field must use the same configuration.
//! - Token order is preserved, but punctuation and other non-word boundaries
//!   are not emitted as tokens.
//! - [`FtsFieldConfig::max_token_length`] is measured in UTF-8 bytes after case
//!   conversion, not in Unicode scalar values or user-perceived characters.
//! - `prefix_mode` changes only stemming of the final Unicode word. It does not
//!   make earlier tokens prefixes and does not bypass stopword or length rules.
//!   "Final" is decided before filtering; if that word is discarded, an earlier
//!   retained word is not promoted to become the prefix.
//! - Serde field names and defaults are part of namespace-metadata
//!   compatibility. Unknown fields are rejected rather than silently ignored.
//!
//! ## Rust concepts used here
//!
//! [`tokenize_text`] borrows `&str` and `&FtsFieldConfig`, comparable to
//! read-only Java references or `const` pointers in C, but Rust proves both are
//! non-null and valid for the call. Its iterator pipeline consumes a temporary
//! vector of borrowed word slices and produces owned [`String`] values, so the
//! returned tokens remain valid after the input borrow ends. [`FtsLanguage`] is
//! an exhaustive enum: adding a language makes the compiler identify every
//! selector that must be extended. Lazily initialized stopword [`HashSet`]s
//! provide process-wide immutable lookup tables without manual initialization
//! or cleanup code.

use std::collections::HashSet;
use std::sync::LazyLock;

use rust_stemmers::{Algorithm, Stemmer};
use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

use crate::error::{Result, ZeppelinError};

/// Identifies the language-specific analyzer used for one FTS field.
///
/// The value is persisted in namespace metadata as a snake-case string. Only
/// English is currently supported, so deserializing any other language fails
/// loudly instead of selecting an approximate analyzer.
///
/// # Examples
///
/// A missing `language` property selects [`FtsLanguage::English`]; an explicit
/// JSON value uses `"english"`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FtsLanguage {
    /// Uses Unicode word boundaries, the English stopword set, and the English
    /// Snowball stemmer when those optional stages are enabled.
    #[default]
    English,
}

/// Holds the persisted analysis and BM25 policy for one searchable text field.
///
/// Namespace creation validates the numeric fields through [`Self::validate`]
/// and stores this value in namespace metadata. Compaction, WAL scanning, and
/// query execution then reuse it. Serde rejects unknown properties so a typo
/// cannot silently produce a different analyzer than the operator intended.
///
/// # Examples
///
/// The default analyzer lowercases English text, removes stopwords, stems
/// remaining terms, uses BM25 `k1 = 1.2` and `b = 0.75`, and discards tokens
/// longer than 40 UTF-8 bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FtsFieldConfig {
    /// Selects the stopword table and stemming algorithm.
    ///
    /// Omitted JSON values default to English.
    #[serde(default)]
    pub language: FtsLanguage,
    /// Enables language-specific stemming after case and stopword processing.
    ///
    /// For example, the English stemmer maps `running` to `run`. Omitted JSON
    /// values default to `true`.
    #[serde(default = "default_true")]
    pub stemming: bool,
    /// Removes language-specific function words before stemming when `true`.
    ///
    /// For English, words such as `the`, `is`, and `at` are removed. Omitted
    /// JSON values default to `true`.
    #[serde(default = "default_true")]
    pub remove_stopwords: bool,
    /// Preserves input case when `true`; otherwise tokens are lowercased.
    ///
    /// Case-sensitive mode also makes stopword lookup case-sensitive because
    /// the English table contains lowercase words. Omitted values default to
    /// `false`.
    #[serde(default)]
    pub case_sensitive: bool,
    /// Controls BM25 term-frequency saturation and must be finite in `(0, 10]`.
    ///
    /// Larger values allow repeated occurrences to contribute for longer
    /// before saturating. The serialized default is `1.2`.
    #[serde(default = "default_k1")]
    pub k1: f32,
    /// Controls BM25 document-length normalization and must be finite in
    /// `[0, 1]`.
    ///
    /// Zero disables length normalization; one applies its full effect. The
    /// serialized default is `0.75`.
    #[serde(default = "default_b")]
    pub b: f32,
    /// Maximum token length in UTF-8 bytes after optional lowercasing.
    ///
    /// Tokens longer than this value are discarded, and the validated value
    /// must be at least one. The serialized default is 40 bytes.
    #[serde(default = "default_max_token_length")]
    pub max_token_length: usize,
}

/// Supplies the shared `true` default for optional analyzer switches.
///
/// # Returns
///
/// Returns `true`, enabling stemming or stopword removal when the corresponding
/// JSON property is absent.
fn default_true() -> bool {
    true
}

/// Supplies the default BM25 term-frequency saturation parameter.
///
/// # Returns
///
/// Returns `1.2`, the value used when `k1` is absent from persisted metadata.
fn default_k1() -> f32 {
    1.2
}

/// Supplies the default BM25 document-length normalization parameter.
///
/// # Returns
///
/// Returns `0.75`, the value used when `b` is absent from persisted metadata.
fn default_b() -> f32 {
    0.75
}

/// Supplies the default maximum analyzed token size.
///
/// # Returns
///
/// Returns 40 UTF-8 bytes, used when `max_token_length` is absent from
/// persisted metadata.
fn default_max_token_length() -> usize {
    40
}

impl Default for FtsFieldConfig {
    /// Builds the analyzer and scorer policy used when no field overrides exist.
    ///
    /// # Returns
    ///
    /// Returns an English, case-insensitive configuration with stemming and
    /// stopword removal enabled, `k1 = 1.2`, `b = 0.75`, and a 40-byte token
    /// limit.
    ///
    /// # Examples
    ///
    /// Deserializing `{}` and calling this method produce equivalent settings.
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

impl FtsFieldConfig {
    /// Validates all numeric analyzer and BM25 bounds for one configured field.
    ///
    /// Namespace creation calls this before persisting metadata. The method
    /// collects every detected violation so an operator can correct the whole
    /// field configuration in one request rather than encountering one error at
    /// a time.
    ///
    /// # Parameters
    ///
    /// - `path`: Human-readable configuration path placed in each error, such
    ///   as `full_text_search.content`. It is diagnostic text, not an S3 key.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when `k1` is finite and in `(0, 10]`, `b` is finite and
    /// in `[0, 1]`, and `max_token_length` is at least one.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] containing a bullet for every
    /// failed bound. Validation does not persist or partially apply anything.
    ///
    /// # Examples
    ///
    /// A field with `k1 = -0.1`, `b = 1.1`, and a zero token limit returns one
    /// validation error whose message names all three properties.
    pub fn validate(&self, path: &str) -> Result<()> {
        let mut violations = Vec::new();

        if !self.k1.is_finite() || self.k1 <= 0.0 || self.k1 > 10.0 {
            violations.push(format!("{path}.k1 must be finite and in (0, 10]"));
        }
        if !self.b.is_finite() || self.b < 0.0 || self.b > 1.0 {
            violations.push(format!("{path}.b must be finite and in [0, 1]"));
        }
        if self.max_token_length == 0 {
            violations.push(format!("{path}.max_token_length must be at least 1"));
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(ZeppelinError::Validation(format!(
                "invalid FTS field configuration:\n- {}",
                violations.join("\n- ")
            )))
        }
    }
}

/// Analyzes text into the ordered terms used by FTS indexing and querying.
///
/// Unicode word segmentation finds candidate words. Each word is optionally
/// lowercased, rejected when its UTF-8 byte length exceeds the configured
/// limit, checked against the stopword table, and optionally stemmed. In prefix
/// mode only the final Unicode word skips stemming, allowing later index search
/// to expand that raw prefix (for example, `prog` to `program` and
/// `programming`).
///
/// # Parameters
///
/// - `text`: Borrowed UTF-8 document or query text. It is never modified.
/// - `config`: Borrowed field policy that must match the policy used on the
///   other side of indexing or querying.
/// - `prefix_mode`: When `true`, preserves the final segmented word without
///   stemming. Case folding, byte-length rejection, and stopword removal still
///   apply to it. If that final word is rejected, the preceding retained word
///   remains an ordinary stemmed token.
///
/// # Returns
///
/// Returns newly allocated, owned tokens in source order. Empty input, input
/// containing only stopwords, or input whose words all exceed the limit returns
/// an empty vector. Punctuation is not returned.
///
/// # Performance
///
/// Work is linear in the input plus hashing and stemming costs. The function
/// allocates an intermediate vector of borrowed word slices and one owned
/// [`String`] for each retained token; it performs no I/O.
///
/// # Examples
///
/// With the default configuration, `"The runners are running"` removes `the`
/// and `are` and stems the content words. For the single-word query `"running"`,
/// ordinary mode returns `run`, while prefix mode returns `running`.
///
/// # Rust Notes for Java/C Engineers
///
/// The two `&` parameters are temporary shared borrows; neither the text nor
/// configuration is copied or retained. Word slices initially point into
/// `text`, similar to pointer-and-length views in C, while Rust checks their
/// lifetime. Returning `Vec<String>` deliberately crosses that borrow boundary
/// with owned storage, comparable to returning a Java list of newly created
/// strings but with deterministic deallocation when the vector is dropped.
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

/// Selects the stemming implementation for a configured language.
///
/// # Parameters
///
/// - `language`: Copyable language tag chosen by the field configuration.
///
/// # Returns
///
/// Returns a fresh stemmer for that language. English currently selects the
/// English Snowball algorithm.
///
/// # Rust Notes for Java/C Engineers
///
/// The exhaustive `match` has no default branch. If a language variant is
/// added, Rust requires this selector to handle it before the crate compiles;
/// Java or C code often relies on a runtime default that can hide omissions.
fn create_stemmer(language: FtsLanguage) -> Stemmer {
    match language {
        FtsLanguage::English => Stemmer::create(Algorithm::English),
    }
}

/// Lists the 33 lowercase English function words removed by the default analyzer.
///
/// The list follows the compact Lucene/Elasticsearch-style English set: it
/// contains articles, prepositions, conjunctions, pronouns, and auxiliaries,
/// but deliberately avoids content words that users are likely to search for.
/// Its spelling and case are part of analyzer compatibility between index and
/// query tokenization.
static ENGLISH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// Provides constant-time membership lookup over [`ENGLISH_STOPWORDS`].
///
/// Initialization happens once on first use, after which all callers share
/// an immutable process-lifetime set.
static ENGLISH_STOPWORD_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| ENGLISH_STOPWORDS.iter().copied().collect());
/// Provides an allocation-free shared empty table when removal is disabled.
static EMPTY_STOPWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(HashSet::new);

/// Selects the process-wide stopword lookup table for a language.
///
/// # Parameters
///
/// - `language`: Copyable language tag from the field configuration.
///
/// # Returns
///
/// Returns a process-lifetime shared reference. Callers can perform lookups but
/// cannot mutate the table.
///
/// # Rust Notes for Java/C Engineers
///
/// The `&'static` lifetime states that both the set and its string slices live
/// for the process. This resembles an immutable Java static field or a C table
/// in static storage, with the added compiler guarantee that callers cannot
/// free or mutate it through this reference.
fn load_stopwords(language: FtsLanguage) -> &'static HashSet<&'static str> {
    match language {
        FtsLanguage::English => &ENGLISH_STOPWORD_SET,
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Protects analyzer defaults, pipeline stages, validation, and JSON
    //! compatibility independently of storage and index code.

    use super::*;

    /// Creates the production default policy used by the focused analyzer tests.
    ///
    /// # Returns
    ///
    /// Returns a fresh owned configuration so a test can mutate it without
    /// affecting another test.
    fn default_config() -> FtsFieldConfig {
        FtsFieldConfig::default()
    }

    /// Verifies basic Unicode word extraction and lowercase normalization.
    #[test]
    fn test_basic_tokenization() {
        let tokens = tokenize_text("Hello World", &default_config(), false);
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    /// Verifies uppercase input is normalized before English stemming.
    #[test]
    fn test_lowercase() {
        let tokens = tokenize_text("RUST Programming", &default_config(), false);
        assert!(tokens.contains(&"rust".to_string()));
        assert!(tokens.contains(&"program".to_string())); // stemmed
    }

    /// Verifies related English word forms reduce to their index terms.
    #[test]
    fn test_stemming() {
        let config = default_config();
        let tokens = tokenize_text("running quickly", &config, false);
        assert!(tokens.contains(&"run".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    /// Verifies function words are omitted while searchable words survive.
    #[test]
    fn test_stopword_removal() {
        let tokens = tokenize_text("the quick brown fox", &default_config(), false);
        assert!(!tokens.iter().any(|t| t == "the"));
        assert!(tokens.iter().any(|t| t == "quick" || t == "brown"));
    }

    /// Verifies the configured byte-length ceiling drops oversized words.
    #[test]
    fn test_max_token_length() {
        let mut config = default_config();
        config.max_token_length = 5;
        let tokens = tokenize_text("hi superlongword ok", &config, false);
        assert!(tokens.iter().any(|t| t == "hi"));
        assert!(!tokens.iter().any(|t| t.contains("superlong")));
    }

    /// Verifies empty text produces no tokens and does not invoke later stages.
    #[test]
    fn test_empty_string() {
        let tokens = tokenize_text("", &default_config(), false);
        assert!(tokens.is_empty());
    }

    /// Verifies an input made entirely of stopwords produces no index terms.
    #[test]
    fn test_only_stopwords() {
        let tokens = tokenize_text("the is at", &default_config(), false);
        assert!(tokens.is_empty());
    }

    /// Verifies punctuation separates Unicode words rather than becoming terms.
    #[test]
    fn test_unicode_punctuation() {
        let tokens = tokenize_text("hello, world! how's it?", &default_config(), false);
        assert!(tokens.iter().any(|t| t == "hello"));
    }

    /// Verifies prefix mode preserves the last word while ordinary mode stems it.
    #[test]
    fn test_prefix_mode() {
        let config = default_config();
        let tokens = tokenize_text("running", &config, true);
        assert_eq!(tokens, vec!["running"]);

        let tokens_normal = tokenize_text("running", &config, false);
        assert_eq!(tokens_normal, vec!["run"]);
    }

    /// Verifies disabling stemming preserves the normalized surface forms.
    #[test]
    fn test_no_stemming() {
        let mut config = default_config();
        config.stemming = false;
        let tokens = tokenize_text("running quickly", &config, false);
        assert!(tokens.contains(&"running".to_string()));
        assert!(tokens.contains(&"quickly".to_string()));
    }

    /// Verifies case-sensitive analysis does not lowercase retained tokens.
    #[test]
    fn test_case_sensitive() {
        let mut config = default_config();
        config.case_sensitive = true;
        let tokens = tokenize_text("Hello WORLD", &config, false);
        assert!(tokens
            .iter()
            .any(|t| t.starts_with('H') || t.starts_with('W')));
    }

    /// Verifies disabling stopword removal keeps function words searchable.
    #[test]
    fn test_no_stopword_removal() {
        let mut config = default_config();
        config.remove_stopwords = false;
        let tokens = tokenize_text("the cat", &config, false);
        assert!(tokens.len() >= 2);
    }

    /// Guards the curated stopword set against accidentally removing content words.
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

    /// Locks the default language to English for absent persisted configuration.
    #[test]
    fn test_fts_language_default() {
        assert_eq!(FtsLanguage::default(), FtsLanguage::English);
    }

    /// Locks every programmatic field default to its compatibility value.
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

    /// Verifies validation reports all numeric violations with their field path.
    #[test]
    fn test_fts_field_config_validate_reports_all_bound_violations() {
        let cfg = FtsFieldConfig {
            k1: -0.1,
            b: 1.1,
            max_token_length: 0,
            ..Default::default()
        };

        let err = cfg.validate("full_text_search.content").unwrap_err();
        let message = err.to_string();
        for needle in [
            "full_text_search.content.k1",
            "full_text_search.content.b",
            "full_text_search.content.max_token_length",
        ] {
            assert!(
                message.contains(needle),
                "expected FTS validation error to contain {needle:?}, got: {message}"
            );
        }
    }

    /// Verifies non-default field policy survives JSON persistence unchanged.
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

    /// Verifies an empty JSON object receives every Serde-backed default.
    #[test]
    fn test_fts_field_config_from_empty_json() {
        let cfg: FtsFieldConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.language, FtsLanguage::English);
        assert!(cfg.stemming);
        assert!(cfg.remove_stopwords);
    }

    /// Locks the persisted language spelling to the snake-case JSON contract.
    #[test]
    fn test_fts_language_serde_roundtrip() {
        let json = serde_json::to_string(&FtsLanguage::English).unwrap();
        assert_eq!(json, "\"english\"");
        let back: FtsLanguage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, FtsLanguage::English);
    }
}
