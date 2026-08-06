//! Keeps `tasks/config_tuning.md` honest against the real `Config`.
//!
//! Every backticked dotted path in the doc whose first segment is a Config
//! section (e.g. `cache.memory_cache_max_mb`) must be accepted by the real
//! serde deserializer. Existence is proven by parsing a synthetic document
//! that sets exactly that key: `deny_unknown_fields` turns a stale path into
//! an "unknown field" error, while a type mismatch on a real field proves
//! the field exists and passes. The doc must also cover every top-level
//! section of `Config` so new sections cannot ship undocumented.

use std::collections::BTreeSet;

use zeppelin::config::Config;

const DOC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tasks/config_tuning.md");

/// Top-level `Config` sections, pinned; a new section must be added both
/// here and to the doc (the serialization cross-check below catches drift).
const SECTIONS: [&str; 12] = [
    "server",
    "storage",
    "cache",
    "indexing",
    "compaction",
    "mmli",
    "logging",
    "wal",
    "query",
    "gc",
    "branching",
    "security",
];

fn doc_text() -> String {
    std::fs::read_to_string(DOC_PATH)
        .unwrap_or_else(|error| panic!("failed to read {DOC_PATH}: {error}"))
}

/// Extracts every backticked dotted path rooted at a Config section.
fn documented_paths(doc: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for raw in doc.split('`').skip(1).step_by(2) {
        let candidate = raw.trim();
        let Some((head, _)) = candidate.split_once('.') else {
            continue;
        };
        if !SECTIONS.contains(&head) {
            continue;
        }
        if !candidate
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.')
        {
            continue;
        }
        paths.insert(candidate.to_string());
    }
    paths
}

/// Builds a TOML document that sets exactly `path` (plus the mandatory
/// security mode) and returns the deserializer's verdict on it.
fn field_exists(path: &str) -> Result<(), String> {
    let segments = path.split('.').collect::<Vec<_>>();
    let (leaf, tables) = segments
        .split_last()
        .unwrap_or_else(|| panic!("documented path {path:?} has no segments"));
    let mut doc = String::from("[security]\nmode = \"enforced\"\n");
    if tables == ["security"] {
        doc.push_str(&format!("{leaf} = 1\n"));
    } else {
        doc.push_str(&format!("[{}]\n{leaf} = 1\n", tables.join(".")));
    }
    match toml::from_str::<Config>(&doc) {
        Ok(_) => Ok(()),
        Err(error) => {
            let message = error.to_string();
            if message.contains("unknown field") {
                Err(message)
            } else {
                // A type or validation complaint proves the field is known.
                Ok(())
            }
        }
    }
}

#[test]
fn the_existence_probe_actually_discriminates() {
    // Real field with a type mismatch still proves existence.
    assert!(field_exists("cache.memory_cache_max_mb").is_ok());
    assert!(field_exists("cache.dir").is_ok());
    assert!(field_exists("mmli.segment.read_max_concurrency").is_ok());
    assert!(field_exists("mmli.worker.scratch_dir").is_ok());
    assert!(field_exists("security.cursor_hmac_key_hex").is_ok());
    // Stale/typo'd paths must be rejected at every nesting level.
    assert!(field_exists("cache.not_a_real_knob").is_err());
    assert!(field_exists("indexing.nprobe").is_err());
    assert!(field_exists("mmli.segment.gap_budget").is_err());
    assert!(field_exists("compaction.batch_manifest_size").is_err());
}

#[test]
fn every_documented_knob_exists_in_config() {
    let doc = doc_text();
    let paths = documented_paths(&doc);
    assert!(
        paths.len() >= 50,
        "extraction found only {} dotted paths; the doc or the extractor regressed",
        paths.len()
    );
    let mut stale = Vec::new();
    for path in &paths {
        if let Err(error) = field_exists(path) {
            stale.push(format!("{path}: {error}"));
        }
    }
    assert!(
        stale.is_empty(),
        "tasks/config_tuning.md documents knobs the Config rejects:\n{}",
        stale.join("\n")
    );
}

#[test]
fn every_config_section_is_documented() {
    let doc = doc_text();
    for section in SECTIONS {
        let dotted = format!("`{section}.");
        let table = format!("[{section}]");
        assert!(
            doc.contains(&dotted) || doc.contains(&table),
            "tasks/config_tuning.md never mentions the [{section}] section"
        );
    }
}

#[test]
fn pinned_section_list_matches_the_real_config() {
    // Serialize a default Config and compare its top-level tables against
    // the pinned SECTIONS list, so a new section cannot ship undocumented.
    // Option-typed sections absent from a default serialization (e.g. a
    // None mmli.worker) are still pinned above and proven by field_exists.
    let value = toml::Value::try_from(Config::default())
        .unwrap_or_else(|error| panic!("failed to serialize default Config: {error}"));
    let table = value
        .as_table()
        .unwrap_or_else(|| panic!("default Config did not serialize to a table"));
    for key in table.keys() {
        assert!(
            SECTIONS.contains(&key.as_str()),
            "Config gained a new top-level section [{key}]; add it to \
             SECTIONS here and document it in tasks/config_tuning.md"
        );
    }
}
