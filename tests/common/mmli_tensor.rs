//! File-backed f16 matrix replay for the ignored real-matrix MMLI gate.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::task;
use zeppelin::embedding::{
    ContentHash, EncoderDocumentInput, EncoderQueryInput, MultiVectorEmbedding,
    MultiVectorEmbeddingBatch, MultiVectorEncoder, MultiVectorEpoch, MultiVectorEpochId,
};
use zeppelin::error::ZeppelinError;

const F16_BYTES: usize = 2;
const IO_BUFFER_BYTES: usize = 1024 * 1024;
const GOLD_PER_QUERY: usize = 10;

type ReplayResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Dtype {
    F16,
    F32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sidecar {
    rows: Vec<usize>,
    dim: usize,
    dtype: Dtype,
    ids: Vec<String>,
}

#[derive(Debug)]
struct TensorData {
    raw: PathBuf,
    ids: Vec<String>,
    rows: Vec<usize>,
    offsets: Vec<usize>,
    dim: usize,
    total_rows: usize,
    raw_bytes: u64,
}

/// A validated canonical little-endian f16 lab tensor kept on disk.
#[derive(Clone, Debug)]
pub struct FileBackedF16Tensor(Arc<TensorData>);

impl FileBackedF16Tensor {
    /// Validate the strict JSON sidecar, raw shape and finite f16 values, then
    /// verify the durable composite digest used by `mmli_lab`.
    pub fn load_verified(
        raw: impl AsRef<Path>,
        sidecar: impl AsRef<Path>,
        expected_sha256: &str,
    ) -> ReplayResult<Self> {
        if expected_sha256.len() != 64
            || !expected_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "expected SHA-256 must be 64 lowercase hex characters, got {expected_sha256:?}"
            ));
        }
        let raw = raw.as_ref().to_path_buf();
        let sidecar_path = sidecar.as_ref().to_path_buf();
        let sidecar_bytes = read_regular_file(&sidecar_path)?;
        let sidecar: Sidecar = serde_json::from_slice(&sidecar_bytes)
            .map_err(|error| format!("invalid {}: {error}", sidecar_path.display()))?;
        if sidecar.dtype != Dtype::F16 {
            return Err(format!(
                "{} declares {:?}, expected f16",
                sidecar_path.display(),
                sidecar.dtype
            ));
        }
        if sidecar.dim == 0 || sidecar.rows.is_empty() {
            return Err(format!(
                "{} must describe at least one positive-dimensional matrix",
                sidecar_path.display()
            ));
        }
        if sidecar.rows.len() != sidecar.ids.len() {
            return Err(format!(
                "{} has {} row counts but {} ids",
                sidecar_path.display(),
                sidecar.rows.len(),
                sidecar.ids.len()
            ));
        }

        let mut ids = HashSet::with_capacity(sidecar.ids.len());
        let mut offsets = Vec::with_capacity(sidecar.rows.len());
        let mut total_rows = 0usize;
        let mut scalars = 0usize;
        for (index, (&rows, id)) in sidecar.rows.iter().zip(&sidecar.ids).enumerate() {
            if rows == 0 || id.is_empty() || !ids.insert(id.as_str()) {
                return Err(format!(
                    "{} matrix {index} has zero rows, an empty id, or a repeated id",
                    sidecar_path.display()
                ));
            }
            offsets.push(scalars);
            total_rows = total_rows
                .checked_add(rows)
                .ok_or_else(|| format!("{} row count overflows", sidecar_path.display()))?;
            scalars = scalars
                .checked_add(
                    rows.checked_mul(sidecar.dim)
                        .ok_or_else(|| format!("{} shape overflows", sidecar_path.display()))?,
                )
                .ok_or_else(|| format!("{} shape overflows", sidecar_path.display()))?;
        }
        let raw_bytes = scalars
            .checked_mul(F16_BYTES)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| format!("{} byte count overflows", raw.display()))?;
        let file = open_regular_file(&raw)?;
        let actual_raw_bytes = file
            .metadata()
            .map_err(|error| format!("cannot stat {}: {error}", raw.display()))?
            .len();
        if actual_raw_bytes != raw_bytes {
            return Err(format!(
                "{} is {actual_raw_bytes} bytes, expected {raw_bytes}",
                raw.display()
            ));
        }

        let mut hasher = Sha256::new();
        hash_frame(&mut hasher, b"sidecar", &sidecar_bytes);
        hasher.update((b"raw".len() as u64).to_le_bytes());
        hasher.update(b"raw");
        hasher.update(raw_bytes.to_le_bytes());
        let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
        let mut buffer = vec![0_u8; IO_BUFFER_BYTES];
        let mut scalar_index = 0usize;
        while scalar_index < scalars {
            let count = (scalars - scalar_index).min(buffer.len() / F16_BYTES);
            let bytes = count * F16_BYTES;
            reader
                .read_exact(&mut buffer[..bytes])
                .map_err(|error| format!("cannot read {}: {error}", raw.display()))?;
            hasher.update(&buffer[..bytes]);
            for (offset, chunk) in buffer[..bytes].chunks_exact(F16_BYTES).enumerate() {
                if !f16_to_f32(u16::from_le_bytes([chunk[0], chunk[1]])).is_finite() {
                    return Err(format!(
                        "{} contains non-finite scalar {}",
                        raw.display(),
                        scalar_index + offset
                    ));
                }
            }
            scalar_index += count;
        }
        let actual_sha256 = hex(&hasher.finalize());
        if actual_sha256 != expected_sha256 {
            return Err(format!(
                "tensor digest mismatch: expected {expected_sha256}, got {actual_sha256}"
            ));
        }
        Ok(Self(Arc::new(TensorData {
            raw,
            ids: sidecar.ids,
            rows: sidecar.rows,
            offsets,
            dim: sidecar.dim,
            total_rows,
            raw_bytes,
        })))
    }

    /// Return the number of sidecar matrices.
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.rows.len()
    }

    /// Return the greatest matrix row count.
    #[must_use]
    pub fn max_rows(&self) -> usize {
        self.0.rows.iter().copied().max().unwrap_or(0)
    }

    /// Return one matrix's canonical f16 byte count.
    pub fn matrix_bytes(&self, index: usize) -> ReplayResult<usize> {
        self.rows(index)?
            .checked_mul(self.0.dim)
            .and_then(|count| count.checked_mul(F16_BYTES))
            .ok_or_else(|| format!("matrix {index} byte count overflows"))
    }

    /// Reproduce the lab's evenly spaced global-row mean sample.
    pub fn sampled_mean(&self, maximum_rows: usize) -> ReplayResult<Vec<f32>> {
        if maximum_rows == 0 {
            return Err("mean sample row limit must be positive".to_string());
        }
        let sample_count = self.0.total_rows.min(maximum_rows);
        let row_bytes = self
            .0
            .dim
            .checked_mul(F16_BYTES)
            .ok_or_else(|| "mean row byte count overflows".to_string())?;
        let mut file = self.open_checked()?;
        let mut bytes = vec![0_u8; row_bytes];
        let mut mean = vec![0.0_f64; self.0.dim];
        for sample in 0..sample_count {
            let row = sample
                .checked_mul(self.0.total_rows)
                .ok_or_else(|| "mean sample index overflows".to_string())?
                / sample_count;
            let offset = row
                .checked_mul(row_bytes)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or_else(|| "mean sample offset overflows".to_string())?;
            file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.read_exact(&mut bytes))
                .map_err(|error| format!("cannot sample {}: {error}", self.0.raw.display()))?;
            for (sum, chunk) in mean.iter_mut().zip(bytes.chunks_exact(F16_BYTES)) {
                *sum += f64::from(f16_to_f32(u16::from_le_bytes([chunk[0], chunk[1]])));
            }
        }
        Ok(mean
            .into_iter()
            .map(|sum| (sum / sample_count as f64) as f32)
            .collect())
    }

    fn rows(&self, index: usize) -> ReplayResult<usize> {
        self.0
            .rows
            .get(index)
            .copied()
            .ok_or_else(|| format!("matrix index {index} exceeds count {}", self.count()))
    }

    fn read_matrix(&self, index: usize) -> ReplayResult<Vec<f32>> {
        let bytes = self.matrix_bytes(index)?;
        let offset = self.0.offsets[index]
            .checked_mul(F16_BYTES)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("matrix {index} offset overflows"))?;
        let mut file = self.open_checked()?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| format!("cannot seek {}: {error}", self.0.raw.display()))?;
        let mut encoded = vec![0_u8; bytes];
        file.read_exact(&mut encoded)
            .map_err(|error| format!("cannot read {}: {error}", self.0.raw.display()))?;
        Ok(encoded
            .chunks_exact(F16_BYTES)
            .map(|chunk| f16_to_f32(u16::from_le_bytes([chunk[0], chunk[1]])))
            .collect())
    }

    fn open_checked(&self) -> ReplayResult<File> {
        let file = open_regular_file(&self.0.raw)?;
        let bytes = file
            .metadata()
            .map_err(|error| format!("cannot stat {}: {error}", self.0.raw.display()))?
            .len();
        if bytes != self.0.raw_bytes {
            return Err(format!(
                "{} changed size: expected {}, got {bytes}",
                self.0.raw.display(),
                self.0.raw_bytes
            ));
        }
        Ok(file)
    }
}

/// Convert a sidecar index into a lexicographically order-preserving VectorId.
#[must_use]
pub fn production_document_id(index: usize) -> String {
    format!("mmli-replay-d-{index:020}")
}

#[derive(Debug, Deserialize)]
struct Diagnostics {
    cells: Vec<DiagnosticCell>,
}

#[derive(Debug, Deserialize)]
struct DiagnosticCell {
    config: String,
    gold_ranks: Vec<GoldRank>,
}

#[derive(Debug, Deserialize)]
struct GoldRank {
    query_index: usize,
    query_id: String,
    document_index: usize,
    document_id: String,
    exact_rank: usize,
}

/// Load the unique config-E cell as production IDs ordered by query and rank.
pub fn load_config_e_gold_ranks(
    path: impl AsRef<Path>,
    documents: &FileBackedF16Tensor,
    queries: &FileBackedF16Tensor,
) -> ReplayResult<Vec<Vec<String>>> {
    let path = path.as_ref();
    let diagnostics: Diagnostics =
        serde_json::from_reader(BufReader::new(open_regular_file(path)?))
            .map_err(|error| format!("invalid {}: {error}", path.display()))?;
    let mut cells = diagnostics
        .cells
        .into_iter()
        .filter(|cell| cell.config == "E");
    let cell = cells
        .next()
        .ok_or_else(|| format!("{} has no config-E cell", path.display()))?;
    if cells.next().is_some() {
        return Err(format!("{} has multiple config-E cells", path.display()));
    }
    if cell.gold_ranks.len() != queries.count() * GOLD_PER_QUERY {
        return Err(format!(
            "{} has {} config-E ranks, expected {}",
            path.display(),
            cell.gold_ranks.len(),
            queries.count() * GOLD_PER_QUERY
        ));
    }
    let mut gold = vec![vec![None; GOLD_PER_QUERY]; queries.count()];
    let mut seen = vec![HashSet::with_capacity(GOLD_PER_QUERY); queries.count()];
    for rank in cell.gold_ranks {
        if queries.0.ids.get(rank.query_index) != Some(&rank.query_id) {
            return Err(format!(
                "{} query index/id mismatch at {}",
                path.display(),
                rank.query_index
            ));
        }
        if documents.0.ids.get(rank.document_index) != Some(&rank.document_id) {
            return Err(format!(
                "{} document index/id mismatch at {}",
                path.display(),
                rank.document_index
            ));
        }
        if !(1..=GOLD_PER_QUERY).contains(&rank.exact_rank)
            || !seen[rank.query_index].insert(rank.document_index)
            || gold[rank.query_index][rank.exact_rank - 1].is_some()
        {
            return Err(format!(
                "{} has invalid or duplicate config-E rank for query {}",
                path.display(),
                rank.query_index
            ));
        }
        gold[rank.query_index][rank.exact_rank - 1] =
            Some(production_document_id(rank.document_index));
    }
    gold.into_iter()
        .enumerate()
        .map(|(query, ranks)| {
            ranks
                .into_iter()
                .map(|rank| rank.ok_or_else(|| format!("query {query} has an incomplete top ten")))
                .collect()
        })
        .collect()
}

/// File-backed encoder keyed by document ContentHash and exact query text.
#[derive(Clone, Debug)]
pub struct FileBackedMultiVectorEncoder {
    epoch: MultiVectorEpochId,
    dim: usize,
    max_query_rows: usize,
    max_document_rows: usize,
    documents: FileBackedF16Tensor,
    queries: FileBackedF16Tensor,
    documents_by_hash: Arc<HashMap<ContentHash, usize>>,
    queries_by_text: Arc<HashMap<String, usize>>,
}

impl FileBackedMultiVectorEncoder {
    /// Bind sidecar-order document hashes and query texts to verified tensors.
    pub fn new(
        epoch: &MultiVectorEpoch,
        documents: FileBackedF16Tensor,
        queries: FileBackedF16Tensor,
        document_hashes: Vec<ContentHash>,
        query_texts: Vec<String>,
    ) -> ReplayResult<Self> {
        epoch
            .validate()
            .map_err(|error| format!("invalid replay epoch: {error}"))?;
        let dim = epoch.vector_dimension as usize;
        let max_query_rows = epoch.max_query_vectors as usize;
        let max_document_rows = epoch.max_document_vectors as usize;
        // The encoder always emits f16 rows; the epoch's persisted matrix
        // dtype (f16 or int8_sym_v1) is applied by Rust at enrichment and
        // does not constrain the replay tensors.
        if documents.0.dim != dim
            || queries.0.dim != dim
            || documents.max_rows() > max_document_rows
            || queries.max_rows() > max_query_rows
        {
            return Err("replay tensors do not match epoch dimension/row limits".to_string());
        }
        if document_hashes.len() != documents.count() || query_texts.len() != queries.count() {
            return Err("replay lookup inputs do not match tensor counts".to_string());
        }
        let documents_by_hash = unique_map(document_hashes, "document content hash")?;
        if query_texts.iter().any(|text| text.trim().is_empty()) {
            return Err("replay query text must not be empty".to_string());
        }
        let queries_by_text = unique_map(query_texts, "exact query text")?;
        Ok(Self {
            epoch: epoch.id,
            dim,
            max_query_rows,
            max_document_rows,
            documents,
            queries,
            documents_by_hash: Arc::new(documents_by_hash),
            queries_by_text: Arc::new(queries_by_text),
        })
    }
}

#[async_trait]
impl MultiVectorEncoder for FileBackedMultiVectorEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.epoch
    }

    fn output_dimension(&self) -> usize {
        self.dim
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> zeppelin::error::Result<MultiVectorEmbeddingBatch> {
        let indices = inputs
            .iter()
            .map(|input| {
                self.documents_by_hash
                    .get(&input.content_hash())
                    .copied()
                    .ok_or_else(|| replay_error("unknown document content hash"))
            })
            .collect::<zeppelin::error::Result<Vec<_>>>()?;
        let tensor = self.documents.clone();
        let dim = self.dim;
        let maximum = self.max_document_rows;
        let embeddings = task::spawn_blocking(move || {
            indices
                .into_iter()
                .map(|index| {
                    MultiVectorEmbedding::new(
                        tensor.read_matrix(index).map_err(replay_error)?,
                        tensor.rows(index).map_err(replay_error)?,
                        dim,
                        maximum,
                    )
                })
                .collect::<zeppelin::error::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| replay_error(format!("blocking task failed: {error}")))??;
        MultiVectorEmbeddingBatch::new(self.epoch, inputs.len(), self.dim, embeddings)
    }

    async fn encode_query(
        &self,
        input: EncoderQueryInput<'_>,
    ) -> zeppelin::error::Result<MultiVectorEmbedding> {
        let index = self
            .queries_by_text
            .get(input.text())
            .copied()
            .ok_or_else(|| replay_error("unknown exact query text"))?;
        let tensor = self.queries.clone();
        let dim = self.dim;
        let maximum = self.max_query_rows;
        task::spawn_blocking(move || {
            MultiVectorEmbedding::new(
                tensor.read_matrix(index).map_err(replay_error)?,
                tensor.rows(index).map_err(replay_error)?,
                dim,
                maximum,
            )
        })
        .await
        .map_err(|error| replay_error(format!("blocking task failed: {error}")))?
    }
}

fn unique_map<T: Eq + std::hash::Hash>(
    values: Vec<T>,
    role: &str,
) -> ReplayResult<HashMap<T, usize>> {
    let mut map = HashMap::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        if map.insert(value, index).is_some() {
            return Err(format!("replay repeats {role} at index {index}"));
        }
    }
    Ok(map)
}

fn open_regular_file(path: &Path) -> ReplayResult<File> {
    let file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    if !file
        .metadata()
        .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
        .is_file()
    {
        return Err(format!("{} is not a regular file", path.display()));
    }
    Ok(file)
}

fn read_regular_file(path: &Path) -> ReplayResult<Vec<u8>> {
    let mut bytes = Vec::new();
    BufReader::new(open_regular_file(path)?)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    Ok(bytes)
}

fn hash_frame(hasher: &mut Sha256, label: &[u8], bytes: &[u8]) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn replay_error(error: impl std::fmt::Display) -> ZeppelinError {
    ZeppelinError::Validation(format!("MMLI matrix replay failed: {error}"))
}

fn f16_to_f32(bits: u16) -> f32 {
    let sign = u32::from(bits & 0x8000) << 16;
    let exponent = (bits >> 10) & 0x1f;
    let fraction = bits & 0x03ff;
    match exponent {
        0 if fraction == 0 => f32::from_bits(sign),
        0 => {
            let magnitude = f32::from(fraction) * 2.0_f32.powi(-24);
            if sign == 0 {
                magnitude
            } else {
                -magnitude
            }
        }
        0x1f => f32::from_bits(sign | 0x7f80_0000 | (u32::from(fraction) << 13)),
        _ => f32::from_bits(
            sign | (u32::from(exponent + (127 - 15)) << 23) | (u32::from(fraction) << 13),
        ),
    }
}
