use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeppelin::format::fixture::{generate_current_corpus, GeneratedCorpus, FIXTURE_DIMENSIONS};
use zeppelin::format::FORMATS;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, VectorEntry};

use super::vectors::random_vectors;

pub const VERSION_DIRECTORY: &str = "unreleased-936994f";
pub const GIT_COMMIT: &str = "936994fedbc53a3e78bb7f7a5e44373b6e0ebcd1";
pub const FIXED_TIMESTAMP: &str = "2026-08-15T12:00:00Z";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusMetadata {
    pub version_directory: String,
    pub release_status: String,
    pub crate_version: String,
    pub git_commit: String,
    pub generated_at: String,
    pub namespace: String,
    pub dimensions: usize,
    pub ann_query_vector_id: String,
    pub ann_top3: Vec<String>,
    pub bm25_query: String,
    pub bm25_top3: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureFile {
    pub path: String,
    pub family: String,
    pub producer: String,
    pub comparison: String,
    pub size_bytes: u64,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureManifest {
    pub corpus: CorpusMetadata,
    pub files: Vec<FixtureFile>,
}

pub fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/artifacts")
}

pub fn version_root() -> PathBuf {
    corpus_root().join(VERSION_DIRECTORY)
}

pub fn fixed_vectors() -> Vec<VectorEntry> {
    const TEXTS: [&str; 12] = [
        "zeppelin golden artifact vector engine",
        "zeppelin golden artifact corpus",
        "golden artifact corpus compatibility",
        "zeppelin vector search",
        "immutable object storage segment",
        "manifest source of truth",
        "dense approximate nearest neighbors",
        "lexical retrieval with bm25",
        "zeppelin golden artifact wal",
        "golden wal compatibility",
        "object storage wal fragment",
        "unrelated lexical tail",
    ];
    random_vectors(12, FIXTURE_DIMENSIONS)
        .into_iter()
        .enumerate()
        .map(|(index, mut vector)| {
            vector.attributes = Some(HashMap::from([
                (
                    "tenant".to_string(),
                    AttributeValue::String(if index % 2 == 0 { "blue" } else { "red" }.to_string()),
                ),
                (
                    "body".to_string(),
                    AttributeValue::String(TEXTS[index].to_string()),
                ),
            ]));
            vector
        })
        .collect()
}

pub async fn build_corpus() -> GeneratedCorpus {
    let store = ZeppelinStore::new(Arc::new(InMemory::new()));
    let now = DateTime::parse_from_rfc3339(FIXED_TIMESTAMP)
        .expect("fixed artifact timestamp must parse")
        .with_timezone(&Utc);
    generate_current_corpus(&store, &fixed_vectors(), now)
        .await
        .expect("real artifact builders must generate the corpus")
}

pub async fn write_current_corpus() -> FixtureManifest {
    let generated = build_corpus().await;
    let family_by_name = FORMATS
        .iter()
        .map(|format| (format.name, format))
        .collect::<BTreeMap<_, _>>();
    let generated_families = generated
        .artifacts
        .iter()
        .map(|artifact| artifact.family)
        .collect::<BTreeSet<_>>();
    let registry_families = FORMATS
        .iter()
        .map(|format| format.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        generated_families, registry_families,
        "the registry is the fixture-decision inventory"
    );

    let root = version_root();
    if root.exists() {
        fs::remove_dir_all(&root).expect("existing exact corpus version directory must remove");
    }
    fs::create_dir_all(&root).expect("corpus version directory must create");

    let mut files = Vec::with_capacity(generated.artifacts.len());
    for artifact in generated.artifacts {
        let destination = root.join(&artifact.path);
        assert!(
            destination.starts_with(&root),
            "artifact path must remain under the exact version root"
        );
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).expect("artifact parent directory must create");
        }
        fs::write(&destination, &artifact.bytes).expect("artifact fixture bytes must write");
        let format = family_by_name
            .get(artifact.family)
            .expect("generated family must exist in FORMATS");
        let comparison = if format.checksum_input
            && !(artifact.family == "manifest"
                && (artifact.path.ends_with(".json")
                    || artifact.path == "manifest_envelope_v2.bin"))
        {
            "bytes"
        } else {
            "structure"
        };
        files.push(FixtureFile {
            path: artifact.path,
            family: artifact.family.to_string(),
            producer: artifact.producer.to_string(),
            comparison: comparison.to_string(),
            size_bytes: artifact.bytes.len() as u64,
            sha256: hex_sha256(&artifact.bytes),
            object_key: artifact.object_key,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = FixtureManifest {
        corpus: CorpusMetadata {
            version_directory: VERSION_DIRECTORY.to_string(),
            release_status: "unreleased current-format snapshot; not v0.2.0".to_string(),
            crate_version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: GIT_COMMIT.to_string(),
            generated_at: FIXED_TIMESTAMP.to_string(),
            namespace: zeppelin::format::fixture::FIXTURE_NAMESPACE.to_string(),
            dimensions: FIXTURE_DIMENSIONS,
            ann_query_vector_id: "vec_0".to_string(),
            ann_top3: vec![
                "vec_0".to_string(),
                "vec_5".to_string(),
                "vec_6".to_string(),
            ],
            bm25_query: "zeppelin golden artifact".to_string(),
            bm25_top3: vec![
                "vec_0".to_string(),
                "vec_1".to_string(),
                "vec_2".to_string(),
            ],
        },
        files,
    };
    let encoded = toml::to_string_pretty(&manifest).expect("fixture manifest must encode");
    fs::write(root.join("MANIFEST.toml"), encoded).expect("fixture manifest must write");
    manifest
}

pub fn load_manifest(root: &Path) -> FixtureManifest {
    let bytes = fs::read_to_string(root.join("MANIFEST.toml"))
        .expect("fixture MANIFEST.toml must be readable");
    toml::from_str(&bytes).expect("fixture MANIFEST.toml must decode")
}

pub fn version_directories() -> Vec<PathBuf> {
    let mut directories = fs::read_dir(corpus_root())
        .expect("artifact corpus root must exist")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join("MANIFEST.toml").is_file())
        .collect::<Vec<_>>();
    directories.sort();
    directories
}

pub fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
