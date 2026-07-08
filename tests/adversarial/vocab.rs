pub const WORDS: [&str; 32] = [
    "alpha", "beta", "gamma", "delta", "vector", "matrix", "query", "index", "forest", "river",
    "mountain", "planet", "signal", "orbit", "canvas", "copper", "silver", "gold", "crystal",
    "ember", "velvet", "anchor", "bridge", "circle", "dragon", "engine", "fabric", "galaxy",
    "harbor", "island", "jungle", "kernel",
];

#[must_use]
pub fn words() -> &'static [&'static str] {
    &WORDS
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zeppelin::fts::tokenizer::tokenize_text;
    use zeppelin::fts::FtsFieldConfig;

    use super::WORDS;

    #[test]
    fn vocabulary_tokens_are_stable_and_distinct() {
        let config = FtsFieldConfig::default();
        let mut stems = BTreeMap::new();

        for word in WORDS {
            let tokens = tokenize_text(word, &config, false);
            assert_eq!(tokens.len(), 1, "{word} produced {tokens:?}");

            let stem = tokens[0].clone();
            assert!(
                !stem.is_empty(),
                "{word} was removed by the tokenizer/stopword pipeline"
            );

            let retokenized = tokenize_text(&stem, &config, false);
            assert_eq!(
                retokenized,
                vec![stem.clone()],
                "{word} stem {stem} was not stable"
            );

            assert!(
                stems.insert(stem.clone(), word).is_none(),
                "{word} collided on stem {stem}"
            );
        }
    }
}
