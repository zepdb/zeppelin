//! Ignored flat-PQ candidate measurement for MMLI-2 Phase 10 (W10.1).
//!
//! Measures MUVERA-recipe PQ-256-8 (8-dimensional groups, 256 centroids per
//! group, asymmetric scoring with an uncompressed query FDE) as a flat
//! candidate selector over the pinned Phase 2 text FDEs, against the
//! uncompressed exhaustive baseline and the pinned flat-SQ8 numbers. Purely
//! in-memory: candidate-frontier gold containment equals post-rerank hits
//! because exact rerank of any frontier keeps every contained gold in the
//! top ten.
//!
//! Codebooks are trained per group through the production
//! `ivf_flat::kmeans::train_kmeans` seam (deterministic content-derived
//! seeding). No production default, format, or dense path is touched; this is
//! the measured go/no-go for productizing a flat-PQ arm.

use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::index::ivf_flat::kmeans::train_kmeans;

use super::phase9_flat_sq8_bench::{
    build_production_fdes, checksum_vectors, hex, load_gold, load_tensor, parallel_indexed_map,
    required_path, GoldDocument, CANDIDATE_K, DIMENSION, DOCUMENT_COUNT, DOCUMENT_DIGEST,
    EXPECTED_HITS_K1000, FDE_DIMENSION, FDE_SEED, GOLD_PER_QUERY, PRODUCTION_DOCUMENT_FDE_DIGEST,
    PRODUCTION_QUERY_FDE_DIGEST, QUERY_COUNT, QUERY_DIGEST, TRANSFORM_DIGEST,
};
use super::{FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection};

const GROUP_DIMENSION: usize = 8;
const GROUP_COUNT: usize = FDE_DIMENSION / GROUP_DIMENSION;
const CENTROIDS_PER_GROUP: usize = 256;
const KMEANS_MAX_ITERATIONS: usize = 25;
const KMEANS_EPSILON: f64 = 1e-4;
const REPORT_KS: [usize; 3] = [700, CANDIDATE_K, 1_500];
const GATE_MAX_RECALL_DELTA: f64 = 0.01;

type BenchResult<T> = Result<T, String>;

#[tokio::test]
#[ignore = "requires the pinned Phase 2 text tensors; run --release"]
async fn phase10_flat_pq_benchmark() {
    if let Err(error) = run_benchmark() {
        panic!("phase10 flat PQ benchmark failed: {error}");
    }
}

fn run_benchmark() -> BenchResult<()> {
    let started = Instant::now();
    let tensor_dir = required_path("MMLI_REAL_MATRIX_DIR")?;
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    eprintln!("flat-pq: loading and verifying pinned tensors");
    let documents = load_tensor(
        &tensor_dir.join("text-documents.f16"),
        &tensor_dir.join("text-documents.json"),
        DOCUMENT_DIGEST,
    )?;
    let queries = load_tensor(
        &tensor_dir.join("text-queries.f16"),
        &tensor_dir.join("text-queries.json"),
        QUERY_DIGEST,
    )?;
    if documents.sidecar.ids.len() != DOCUMENT_COUNT || queries.sidecar.ids.len() != QUERY_COUNT {
        return Err("pinned tensor counts changed".to_string());
    }
    let gold = load_gold(
        &manifest_dir.join("tasks/MMLI-2/results/lab-diagnostics.json"),
        &documents,
        &queries,
    )?;

    eprintln!("flat-pq: building production-semantics FDEs");
    let transform = FdeTransform::generate(
        &FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 40,
            simhash_bits: 4,
            input_dimension: DIMENSION as u32,
            inner: InnerProjection::Rademacher { d_proj: 16 },
            final_projection: FinalProjection::None,
        },
        FDE_SEED,
    )
    .map_err(|error| error.to_string())?;
    if hex(&transform.checksum()) != TRANSFORM_DIGEST
        || transform.output_dimension() != FDE_DIMENSION
    {
        return Err("config-E transform mismatch".to_string());
    }
    let (document_fdes, query_fdes, _) = build_production_fdes(&documents, &queries, &transform)?;
    if checksum_vectors(&document_fdes, FDE_DIMENSION) != PRODUCTION_DOCUMENT_FDE_DIGEST {
        return Err("production document FDEs diverge from the pinned diagnosis".to_string());
    }
    if checksum_vectors(&query_fdes, FDE_DIMENSION) != PRODUCTION_QUERY_FDE_DIGEST {
        return Err("production query FDEs diverge from the pinned diagnosis".to_string());
    }

    eprintln!("flat-pq: scoring the uncompressed exhaustive baseline");
    let raw_frontiers = parallel_indexed_map(QUERY_COUNT, |query_index| {
        let query = &query_fdes[query_index];
        let scores: Vec<f32> = document_fdes
            .iter()
            .map(|document| dot(query, document))
            .collect();
        Ok(selection_order(&scores))
    })?;

    eprintln!("flat-pq: training PQ-256-8 codebooks through the kmeans seam");
    let train_started = Instant::now();
    let codebooks = parallel_indexed_map(GROUP_COUNT, |group| {
        let offset = group * GROUP_DIMENSION;
        let sub_vectors: Vec<Vec<f32>> = document_fdes
            .iter()
            .map(|fde| fde[offset..offset + GROUP_DIMENSION].to_vec())
            .collect();
        let sub_refs: Vec<&[f32]> = sub_vectors.iter().map(Vec::as_slice).collect();
        let mut centroids = train_kmeans(
            &sub_refs,
            GROUP_DIMENSION,
            CENTROIDS_PER_GROUP,
            KMEANS_MAX_ITERATIONS,
            KMEANS_EPSILON,
        )
        .map_err(|error| error.to_string())?;
        while centroids.len() < CENTROIDS_PER_GROUP {
            let last = centroids
                .last()
                .ok_or_else(|| "empty PQ group codebook".to_string())?
                .clone();
            centroids.push(last);
        }
        Ok(centroids)
    })?;
    let train_ms = train_started.elapsed().as_millis();
    let codebook_digest = checksum_codebooks(&codebooks);

    eprintln!("flat-pq: encoding document codes");
    let encode_started = Instant::now();
    let codes = parallel_indexed_map(DOCUMENT_COUNT, |document_index| {
        let fde = &document_fdes[document_index];
        let mut row = Vec::with_capacity(GROUP_COUNT);
        for (group, codebook) in codebooks.iter().enumerate() {
            let offset = group * GROUP_DIMENSION;
            let sub = &fde[offset..offset + GROUP_DIMENSION];
            let mut best = 0_usize;
            let mut best_distance = f32::INFINITY;
            for (index, centroid) in codebook.iter().enumerate() {
                let distance = squared_l2(sub, centroid);
                if distance < best_distance {
                    best_distance = distance;
                    best = index;
                }
            }
            row.push(best as u8);
        }
        Ok(row)
    })?;
    let encode_ms = encode_started.elapsed().as_millis();

    eprintln!("flat-pq: scoring PQ frontiers asymmetrically");
    let scan_started = Instant::now();
    let pq_frontiers = parallel_indexed_map(QUERY_COUNT, |query_index| {
        let query = &query_fdes[query_index];
        let mut tables = vec![0.0_f32; GROUP_COUNT * CENTROIDS_PER_GROUP];
        for (group, codebook) in codebooks.iter().enumerate() {
            let offset = group * GROUP_DIMENSION;
            let sub = &query[offset..offset + GROUP_DIMENSION];
            let table = &mut tables
                [group * CENTROIDS_PER_GROUP..group * CENTROIDS_PER_GROUP + CENTROIDS_PER_GROUP];
            for (slot, centroid) in table.iter_mut().zip(codebook) {
                *slot = dot(sub, centroid);
            }
        }
        let scores: Vec<f32> = codes
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(group, &code)| tables[group * CENTROIDS_PER_GROUP + usize::from(code)])
                    .sum()
            })
            .collect();
        Ok(selection_order(&scores))
    })?;
    let scan_ms = scan_started.elapsed().as_millis();

    let gold_count = QUERY_COUNT * GOLD_PER_QUERY;
    let mut report_lines = Vec::new();
    let mut gate_passed = None;
    for k in REPORT_KS {
        let raw_hits = frontier_hits(&raw_frontiers, &gold, k);
        let pq_hits = frontier_hits(&pq_frontiers, &gold, k);
        let raw_recall = raw_hits as f64 / gold_count as f64;
        let pq_recall = pq_hits as f64 / gold_count as f64;
        let delta = raw_recall - pq_recall;
        if k == CANDIDATE_K {
            gate_passed = Some(delta <= GATE_MAX_RECALL_DELTA);
        }
        report_lines.push(format!(
            "k={k} raw_hits={raw_hits}/{gold_count} raw_recall={raw_recall:.6} \
             pq_hits={pq_hits}/{gold_count} pq_recall={pq_recall:.6} delta_points={:.4}",
            delta * 100.0
        ));
    }
    let gate_passed =
        gate_passed.ok_or_else(|| "report Ks did not include the operating point".to_string())?;

    println!(
        "phase10_flat_pq lane=text recipe=pq-256-8 groups={GROUP_COUNT} \
         codes_bytes_per_unit={GROUP_COUNT} sq8_bytes_per_unit={FDE_DIMENSION} \
         codebook_bytes={} codebook_sha256={codebook_digest} \
         sq8_pinned_hits_k1000={EXPECTED_HITS_K1000} kmeans_iters={KMEANS_MAX_ITERATIONS} \
         train_ms={train_ms} encode_ms={encode_ms} scan_ms_total={scan_ms} \
         gate_delta_max={GATE_MAX_RECALL_DELTA} gate_passed_k1000={gate_passed} \
         wall_s={:.1}",
        GROUP_COUNT * CENTROIDS_PER_GROUP * GROUP_DIMENSION * size_of::<f32>(),
        started.elapsed().as_secs_f64(),
    );
    for line in report_lines {
        println!("phase10_flat_pq {line}");
    }
    Ok(())
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum()
}

fn squared_l2(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

/// Deterministic descending selection order: score desc, then index asc.
fn selection_order(scores: &[f32]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..scores.len() as u32).collect();
    order.sort_unstable_by(|left, right| {
        scores[*right as usize]
            .total_cmp(&scores[*left as usize])
            .then_with(|| left.cmp(right))
    });
    order
}

fn frontier_hits(frontiers: &[Vec<u32>], gold: &[Vec<GoldDocument>], k: usize) -> usize {
    frontiers
        .iter()
        .zip(gold)
        .map(|(order, golds)| {
            let frontier: std::collections::HashSet<u32> = order.iter().take(k).copied().collect();
            golds
                .iter()
                .filter(|gold| frontier.contains(&(gold.document_index as u32)))
                .count()
        })
        .sum()
}

fn checksum_codebooks(codebooks: &[Vec<Vec<f32>>]) -> String {
    let mut hasher = Sha256::new();
    for codebook in codebooks {
        for centroid in codebook {
            for value in centroid {
                hasher.update(value.to_le_bytes());
            }
        }
    }
    hex(&hasher.finalize())
}
