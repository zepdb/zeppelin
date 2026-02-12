//! Criterion micro-benchmarks for Zeppelin CPU-bound hot paths.
//!
//! Run all:     `cargo bench`
//! Run subset:  `cargo bench -- distance`
//! Save baseline: `cargo bench -- --save-baseline base`

use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;

use zeppelin::fts::bm25::{self, Bm25Params};
use zeppelin::fts::inverted_index::InvertedIndex;
use zeppelin::fts::tokenizer::tokenize_text;
use zeppelin::fts::types::FtsFieldConfig;
use zeppelin::index::bitmap::build::build_cluster_bitmaps;
use zeppelin::index::bitmap::evaluate::evaluate_filter_bitmap;
use zeppelin::index::distance::{
    compute_distance, cosine_distance, dot_product_distance, euclidean_distance,
};
use zeppelin::index::quantization::pq::PqCodebook;
use zeppelin::index::quantization::sq::SqCalibration;
use zeppelin::types::{AttributeValue, DistanceMetric, Filter};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn random_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect())
        .collect()
}

fn random_vector(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

// ---------------------------------------------------------------------------
// 1. Distance benchmarks
// ---------------------------------------------------------------------------

fn bench_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance");

    for &dim in &[32, 128, 256, 768, 1536] {
        let a = random_vector(dim);
        let b = random_vector(dim);

        group.throughput(Throughput::Elements(dim as u64));

        group.bench_with_input(BenchmarkId::new("euclidean", dim), &dim, |bench, _| {
            bench.iter(|| euclidean_distance(black_box(&a), black_box(&b)));
        });

        group.bench_with_input(BenchmarkId::new("cosine", dim), &dim, |bench, _| {
            bench.iter(|| cosine_distance(black_box(&a), black_box(&b)));
        });

        group.bench_with_input(BenchmarkId::new("dot_product", dim), &dim, |bench, _| {
            bench.iter(|| dot_product_distance(black_box(&a), black_box(&b)));
        });

        group.bench_with_input(BenchmarkId::new("dispatch", dim), &dim, |bench, _| {
            bench
                .iter(|| compute_distance(black_box(&a), black_box(&b), DistanceMetric::Euclidean));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. SQ8 Quantization benchmarks
// ---------------------------------------------------------------------------

fn bench_sq_quantization(c: &mut Criterion) {
    let mut group = c.benchmark_group("sq8");
    let dim = 128;
    let n_train = 1000;

    let vectors = random_vectors(n_train, dim);
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();
    let calibration = SqCalibration::calibrate(&vec_refs, dim);

    let query = random_vector(dim);
    let encoded = calibration.encode(&vectors[0]);

    group.bench_function("calibrate_1k_128d", |bench| {
        bench.iter(|| SqCalibration::calibrate(black_box(&vec_refs), black_box(dim)));
    });

    group.bench_function("encode_128d", |bench| {
        bench.iter(|| calibration.encode(black_box(&vectors[0])));
    });

    group.bench_function("decode_128d", |bench| {
        bench.iter(|| calibration.decode(black_box(&encoded)));
    });

    group.bench_function("encode_batch_1k_128d", |bench| {
        bench.iter(|| calibration.encode_batch(black_box(&vec_refs)));
    });

    group.bench_function("asymmetric_l2_128d", |bench| {
        bench.iter(|| calibration.asymmetric_l2_squared(black_box(&query), black_box(&encoded)));
    });

    group.bench_function("asymmetric_distance_euclidean_128d", |bench| {
        bench.iter(|| {
            calibration.asymmetric_distance(
                black_box(&query),
                black_box(&encoded),
                DistanceMetric::Euclidean,
            )
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. PQ Quantization benchmarks
// ---------------------------------------------------------------------------

fn bench_pq_quantization(c: &mut Criterion) {
    let mut group = c.benchmark_group("pq");
    group.sample_size(20); // PQ train is slow — reduce iterations

    let dim = 128;
    let m = 8;
    let n_train = 1000;

    let vectors = random_vectors(n_train, dim);
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();

    group.bench_function("train_1k_128d_m8", |bench| {
        bench.iter(|| {
            PqCodebook::train(black_box(&vec_refs), black_box(dim), black_box(m), 10).unwrap()
        });
    });

    // Train once for encode/decode/ADC benchmarks
    let codebook = PqCodebook::train(&vec_refs, dim, m, 10).unwrap();
    let query = random_vector(dim);
    let encoded = codebook.encode(&vectors[0]);

    group.bench_function("encode_128d_m8", |bench| {
        bench.iter(|| codebook.encode(black_box(&vectors[0])));
    });

    group.bench_function("decode_128d_m8", |bench| {
        bench.iter(|| codebook.decode(black_box(&encoded)));
    });

    group.bench_function("build_adc_table_128d_m8", |bench| {
        bench.iter(|| codebook.build_adc_table(black_box(&query), DistanceMetric::Euclidean));
    });

    let adc_table = codebook.build_adc_table(&query, DistanceMetric::Euclidean);
    group.bench_function("adc_distance_m8", |bench| {
        bench.iter(|| codebook.adc_distance(black_box(&adc_table), black_box(&encoded)));
    });

    // Batch encode
    group.bench_function("encode_batch_1k_128d_m8", |bench| {
        bench.iter(|| codebook.encode_batch(black_box(&vec_refs)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. BM25 benchmarks
// ---------------------------------------------------------------------------

/// Generate realistic-ish text documents for BM25 benchmarks.
fn generate_documents(n: usize) -> Vec<String> {
    let words = [
        "the",
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "vector",
        "search",
        "engine",
        "database",
        "index",
        "query",
        "fast",
        "performance",
        "benchmark",
        "storage",
        "cloud",
        "native",
        "distributed",
        "system",
        "machine",
        "learning",
        "neural",
        "network",
        "embedding",
        "dimension",
        "cluster",
        "centroid",
        "quantization",
        "compression",
        "latency",
        "throughput",
        "scalable",
        "efficient",
        "algorithm",
        "data",
        "structure",
        "optimization",
        "parallel",
        "concurrent",
        "async",
        "runtime",
        "memory",
        "cache",
        "disk",
        "object",
        "storage",
        "retrieval",
        "ranking",
        "relevance",
        "similarity",
        "distance",
        "metric",
        "cosine",
        "euclidean",
        "product",
    ];

    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| {
            let len = rng.gen_range(10..100);
            (0..len)
                .map(|_| words[rng.gen_range(0..words.len())])
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn bench_bm25(c: &mut Criterion) {
    let mut group = c.benchmark_group("bm25");
    let config = FtsFieldConfig::default();
    let params = Bm25Params::default();

    // Tokenization
    let doc = "The quick brown fox jumps over the lazy dog. Vector search engines \
               provide fast similarity search across high-dimensional embeddings.";

    group.bench_function("tokenize_short", |bench| {
        bench.iter(|| tokenize_text(black_box(doc), black_box(&config), false));
    });

    let long_doc = generate_documents(1)[0].clone();
    group.bench_function("tokenize_long", |bench| {
        bench.iter(|| tokenize_text(black_box(&long_doc), black_box(&config), false));
    });

    // BM25 scoring
    group.bench_function("idf", |bench| {
        bench.iter(|| bm25::idf(black_box(10000), black_box(42)));
    });

    group.bench_function("bm25_term_score", |bench| {
        bench.iter(|| {
            bm25::bm25_term_score(
                black_box(2.5),
                black_box(3),
                black_box(100),
                black_box(85.0),
                black_box(&params),
            )
        });
    });

    let term_data: Vec<(f32, u32)> = vec![(2.5, 3), (1.8, 1), (3.2, 2), (0.5, 5)];
    group.bench_function("bm25_score_4terms", |bench| {
        bench.iter(|| {
            bm25::bm25_score(
                black_box(&term_data),
                black_box(100),
                black_box(85.0),
                black_box(&params),
            )
        });
    });

    // Inverted index build + search
    let n_docs = 10_000;
    let documents = generate_documents(n_docs);
    let mut fts_configs = HashMap::new();
    fts_configs.insert("content".to_string(), FtsFieldConfig::default());

    let attr_maps: Vec<Option<HashMap<String, AttributeValue>>> = documents
        .iter()
        .map(|doc| {
            let mut m = HashMap::new();
            m.insert("content".to_string(), AttributeValue::String(doc.clone()));
            Some(m)
        })
        .collect();
    let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
        attr_maps.iter().map(|a| a.as_ref()).collect();

    group.bench_function("inverted_index_build_10k", |bench| {
        bench.iter(|| InvertedIndex::build(black_box(&attr_refs), black_box(&fts_configs)));
    });

    let index = InvertedIndex::build(&attr_refs, &fts_configs);
    let query_tokens = tokenize_text("vector search engine", &config, false);

    group.bench_function("inverted_index_search_10k", |bench| {
        bench.iter(|| {
            index.search(
                black_box("content"),
                black_box(&query_tokens),
                black_box(&params),
            )
        });
    });

    let prefix_tokens = tokenize_text("vector sea", &config, true);
    group.bench_function("inverted_index_prefix_search_10k", |bench| {
        bench.iter(|| {
            index.search_prefix(
                black_box("content"),
                black_box(&prefix_tokens),
                black_box(&params),
            )
        });
    });

    // Serialization round-trip
    let index_bytes = index.to_bytes().unwrap();
    let byte_len = index_bytes.len();
    group.throughput(Throughput::Bytes(byte_len as u64));

    group.bench_function("inverted_index_serialize_10k", |bench| {
        bench.iter(|| index.to_bytes().unwrap());
    });

    group.bench_function("inverted_index_deserialize_10k", |bench| {
        bench.iter(|| InvertedIndex::from_bytes(black_box(&index_bytes)).unwrap());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Bitmap benchmarks
// ---------------------------------------------------------------------------

fn bench_bitmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitmap");

    for &n_vecs in &[100, 1000, 10_000] {
        let mut rng = rand::thread_rng();

        let categories = ["cat_a", "cat_b", "cat_c", "cat_d", "cat_e"];

        let attr_maps: Vec<Option<HashMap<String, AttributeValue>>> = (0..n_vecs)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert(
                    "category".to_string(),
                    AttributeValue::String(categories[rng.gen_range(0..categories.len())].into()),
                );
                m.insert(
                    "score".to_string(),
                    AttributeValue::Integer(rng.gen_range(0..100)),
                );
                m.insert("id".to_string(), AttributeValue::Integer(i as i64));
                Some(m)
            })
            .collect();
        let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
            attr_maps.iter().map(|a| a.as_ref()).collect();

        group.bench_with_input(BenchmarkId::new("build", n_vecs), &n_vecs, |bench, _| {
            bench.iter(|| build_cluster_bitmaps(black_box(&attr_refs)));
        });

        let bitmap_index = build_cluster_bitmaps(&attr_refs);

        // Eq filter
        let eq_filter = Filter::Eq {
            field: "category".to_string(),
            value: AttributeValue::String("cat_a".to_string()),
        };
        group.bench_with_input(BenchmarkId::new("eval_eq", n_vecs), &n_vecs, |bench, _| {
            bench.iter(|| evaluate_filter_bitmap(black_box(&eq_filter), black_box(&bitmap_index)));
        });

        // And filter
        let and_filter = Filter::And {
            filters: vec![
                Filter::Eq {
                    field: "category".to_string(),
                    value: AttributeValue::String("cat_a".to_string()),
                },
                Filter::Range {
                    field: "score".to_string(),
                    gte: Some(25.0),
                    lte: Some(75.0),
                    gt: None,
                    lt: None,
                },
            ],
        };
        group.bench_with_input(BenchmarkId::new("eval_and", n_vecs), &n_vecs, |bench, _| {
            bench.iter(|| evaluate_filter_bitmap(black_box(&and_filter), black_box(&bitmap_index)));
        });

        // Or filter
        let or_filter = Filter::Or {
            filters: vec![
                Filter::Eq {
                    field: "category".to_string(),
                    value: AttributeValue::String("cat_a".to_string()),
                },
                Filter::Eq {
                    field: "category".to_string(),
                    value: AttributeValue::String("cat_b".to_string()),
                },
            ],
        };
        group.bench_with_input(BenchmarkId::new("eval_or", n_vecs), &n_vecs, |bench, _| {
            bench.iter(|| evaluate_filter_bitmap(black_box(&or_filter), black_box(&bitmap_index)));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_distance,
    bench_sq_quantization,
    bench_pq_quantization,
    bench_bm25,
    bench_bitmap,
);

criterion_main!(benches);
