//! Pinned, offline subprocess execution for production multi-vector encoders.
//!
//! The subprocess is deliberately a narrow trust boundary. Rust verifies the
//! locally materialized bundle before spawn, binds the worker handshake to the
//! complete semantic epoch, bounds every request and response, and validates
//! every raw f16 tensor before exposing it to the enrichment coordinator.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::embedding::encoder::{
    EncoderDocumentInput, EncoderQueryInput, MultiVectorEmbedding, MultiVectorEmbeddingBatch,
    MultiVectorEncoder,
};
use crate::embedding::types::validate_bundle_prefix;
use crate::embedding::{
    EncoderInputRef, InputModality, MultiVectorEpoch, MultiVectorEpochId, TextContentRef,
};
use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

const PROTOCOL_VERSION: u32 = 1;
const BUNDLE_MANIFEST: &str = "worker.json";
const MAX_BUNDLE_MANIFEST_BYTES: usize = 1024 * 1024;

/// Process and resource bounds for one pinned encoder session.
#[derive(Debug, Clone)]
pub struct PinnedWorkerConfig {
    /// Absolute virtual-environment directory selected by the operator.
    pub venv_dir: PathBuf,
    /// Absolute Python binary path inside `venv_dir`.
    pub python_binary: PathBuf,
    /// Absolute path to the committed `worker.py`.
    pub worker_script: PathBuf,
    /// Absolute root for disposable per-session sidecar directories.
    pub scratch_dir: PathBuf,
    /// Absolute path to one locally materialized and pinned model bundle.
    pub model_bundle_dir: PathBuf,
    /// Maximum document units admitted in one request.
    pub max_batch_units: usize,
    /// Maximum aggregate source bytes admitted in one request.
    pub max_batch_input_bytes: u64,
    /// Maximum aggregate declared image pixels admitted in one request.
    pub max_batch_pixels: u64,
    /// Maximum aggregate embedding rows admitted in one response.
    pub max_batch_rows: usize,
    /// Maximum bytes admitted from any one tensor sidecar.
    pub max_tensor_bytes: u64,
    /// Maximum bytes in one JSON control frame, excluding its newline.
    pub max_protocol_line_bytes: usize,
    /// Maximum tail of worker stderr retained for a diagnostic.
    pub max_stderr_bytes: usize,
    /// Deadline for process startup and identity handshake.
    pub handshake_timeout: Duration,
    /// Deadline for one complete request, response, and tensor validation.
    pub request_timeout: Duration,
}

impl PinnedWorkerConfig {
    fn validate(&self) -> Result<()> {
        for (label, path) in [
            ("venv directory", &self.venv_dir),
            ("python binary", &self.python_binary),
            ("worker script", &self.worker_script),
            ("scratch directory", &self.scratch_dir),
            ("model bundle directory", &self.model_bundle_dir),
        ] {
            if !path.is_absolute() {
                return Err(worker_error(format!(
                    "pinned worker {label} must be an absolute path"
                )));
            }
        }
        if !self.python_binary.starts_with(&self.venv_dir) {
            return Err(worker_error(
                "pinned worker python binary must be selected through the configured venv"
                    .to_string(),
            ));
        }
        if self.max_batch_units == 0
            || self.max_batch_input_bytes == 0
            || self.max_batch_pixels == 0
            || self.max_batch_rows == 0
            || self.max_tensor_bytes == 0
            || self.max_protocol_line_bytes == 0
            || self.max_stderr_bytes == 0
            || self.handshake_timeout.is_zero()
            || self.request_timeout.is_zero()
        {
            return Err(worker_error(
                "pinned worker resource limits and timeouts must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

/// One warm, identity-pinned encoder subprocess.
///
/// Protocol access is serialized. Once startup, transport, or tensor
/// validation fails, the child is killed and this object remains failed; it
/// never silently starts a replacement process.
pub struct PinnedWorker {
    epoch: MultiVectorEpoch,
    limits: WorkerLimits,
    scratch_dir: PathBuf,
    owned_bundle_dir: Option<PathBuf>,
    session: Mutex<Session>,
}

impl PinnedWorker {
    /// Materialize an authoritative S3 bundle, then spawn the pinned worker.
    ///
    /// `bundle_prefix` names the directory-like object prefix containing
    /// `worker.json` and exactly the files it declares. `bundle_cache_root` is
    /// disposable local state; every invocation re-fetches and verifies S3
    /// before the local bytes can be used.
    pub async fn spawn_from_s3(
        store: &ZeppelinStore,
        mut config: PinnedWorkerConfig,
        bundle_prefix: &str,
        bundle_cache_root: &Path,
        epoch: MultiVectorEpoch,
    ) -> Result<Self> {
        let materialized =
            materialize_bundle_from_s3(store, bundle_prefix, bundle_cache_root, &epoch).await?;
        config.model_bundle_dir.clone_from(&materialized);
        match Self::spawn(config, epoch).await {
            Ok(mut worker) => {
                worker.owned_bundle_dir = Some(materialized);
                Ok(worker)
            }
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&materialized).await;
                Err(error)
            }
        }
    }

    /// Verify the local bundle, spawn its offline worker, and bind its identity.
    pub async fn spawn(config: PinnedWorkerConfig, epoch: MultiVectorEpoch) -> Result<Self> {
        config.validate()?;
        epoch.validate()?;

        let paths = ValidatedPaths::resolve(&config).await?;
        let bundle = load_and_verify_bundle(&paths.model_bundle_dir, &epoch).await?;
        let scratch_dir = create_session_scratch(&paths.scratch_dir).await?;
        let limits = WorkerLimits::from_config(&config);
        let mut command = Command::new(&paths.python_binary);
        command
            .arg(&paths.worker_script)
            .arg("--model-bundle")
            .arg(&paths.model_bundle_dir)
            .arg("--scratch")
            .arg(&scratch_dir)
            .arg("--max-batch-units")
            .arg(config.max_batch_units.to_string())
            .arg("--max-batch-input-bytes")
            .arg(config.max_batch_input_bytes.to_string())
            .arg("--max-batch-pixels")
            .arg(config.max_batch_pixels.to_string())
            .arg("--max-batch-rows")
            .arg(config.max_batch_rows.to_string())
            .arg("--max-tensor-bytes")
            .arg(config.max_tensor_bytes.to_string())
            .arg("--max-line-bytes")
            .arg(config.max_protocol_line_bytes.to_string())
            .current_dir(&scratch_dir)
            .env("HF_HUB_OFFLINE", "1")
            .env("TRANSFORMERS_OFFLINE", "1")
            .env("HF_DATASETS_OFFLINE", "1")
            .env("TOKENIZERS_PARALLELISM", "false")
            .env("HF_HOME", scratch_dir.join("hf-home"))
            .env("XDG_CACHE_HOME", scratch_dir.join("xdg-cache"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(|error| worker_error(format!("failed to spawn pinned worker: {error}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| worker_error("pinned worker stdin was not piped".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| worker_error("pinned worker stdout was not piped".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| worker_error("pinned worker stderr was not piped".to_string()))?;
        let stderr_tail = Arc::new(StdMutex::new(VecDeque::new()));
        let stderr_task = drain_stderr(stderr, Arc::clone(&stderr_tail), config.max_stderr_bytes);

        let mut session = Session {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            stderr_tail,
            stderr_task,
            failed: false,
        };
        let handshake = match timeout(
            config.handshake_timeout,
            read_json_frame::<_, WorkerHandshake>(
                &mut session.stdout,
                config.max_protocol_line_bytes,
            ),
        )
        .await
        {
            Ok(Ok(handshake)) => handshake,
            Ok(Err(error)) => {
                fail_session(&mut session).await;
                return Err(with_stderr(error, &session));
            }
            Err(_) => {
                fail_session(&mut session).await;
                return Err(with_stderr(
                    worker_error("pinned worker handshake timed out".to_string()),
                    &session,
                ));
            }
        };
        if let Err(error) = validate_handshake(&handshake, &bundle, &epoch) {
            fail_session(&mut session).await;
            return Err(with_stderr(error, &session));
        }

        Ok(Self {
            epoch,
            limits,
            scratch_dir,
            owned_bundle_dir: None,
            session: Mutex::new(session),
        })
    }

    /// Stop the owned subprocess and remove disposable session state.
    ///
    /// The process is killed rather than reused after shutdown. A bundle
    /// materialized by [`Self::spawn_from_s3`] is owned by this session and is
    /// removed with the scratch directory.
    pub async fn shutdown(&self) -> Result<()> {
        let mut session = self.session.lock().await;
        fail_session(&mut session).await;
        drop(session);

        remove_disposable_directory(&self.scratch_dir, "worker scratch directory").await?;
        if let Some(bundle_dir) = &self.owned_bundle_dir {
            remove_disposable_directory(bundle_dir, "materialized worker bundle").await?;
        }
        Ok(())
    }

    async fn encode_document_batch(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        let (wire_inputs, input_sidecars) = self.prepare_documents(inputs).await?;
        let request = WorkerRequest::EncodeDocuments {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::new_v4().to_string(),
            inputs: wire_inputs,
        };
        let result = self
            .execute(
                request,
                inputs.len(),
                usize::try_from(self.epoch.max_document_vectors).map_err(|_| {
                    worker_error("epoch document-vector limit exceeds usize".to_string())
                })?,
            )
            .await;
        let cleanup = remove_sidecars(&input_sidecars).await;
        match (result, cleanup) {
            (Ok(embeddings), Ok(())) => MultiVectorEmbeddingBatch::new(
                self.epoch.id,
                inputs.len(),
                self.output_dimension(),
                embeddings,
            ),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => {
                self.poison().await;
                Err(error)
            }
        }
    }

    async fn encode_text_query(
        &self,
        input: EncoderQueryInput<'_>,
    ) -> Result<MultiVectorEmbedding> {
        let text_bytes = u64::try_from(input.text().len())
            .map_err(|_| worker_error("query byte count exceeds u64".to_string()))?;
        if text_bytes > self.limits.max_batch_input_bytes {
            return Err(worker_error(format!(
                "query bytes exceed worker limit {}",
                self.limits.max_batch_input_bytes
            )));
        }
        let request = WorkerRequest::EncodeQuery {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::new_v4().to_string(),
            text: input.text().to_string(),
        };
        let mut embeddings = self
            .execute(
                request,
                1,
                usize::try_from(self.epoch.max_query_vectors).map_err(|_| {
                    worker_error("epoch query-vector limit exceeds usize".to_string())
                })?,
            )
            .await?;
        embeddings
            .pop()
            .ok_or_else(|| worker_error("worker returned no query matrix".to_string()))
    }

    async fn prepare_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<(Vec<WireDocumentInput>, Vec<PathBuf>)> {
        if inputs.is_empty() || inputs.len() > self.limits.max_batch_units {
            return Err(worker_error(format!(
                "document batch count must be in 1..={}",
                self.limits.max_batch_units
            )));
        }

        let mut total_bytes = 0_u64;
        let mut total_pixels = 0_u64;
        for input in inputs {
            total_bytes = total_bytes
                .checked_add(input.input_ref().referenced_content_bytes()?)
                .ok_or_else(|| worker_error("document batch byte count overflow".to_string()))?;
            if let Some((width, height)) = image_dimensions(input.input_ref()) {
                total_pixels = total_pixels
                    .checked_add(u64::from(width) * u64::from(height))
                    .ok_or_else(|| {
                        worker_error("document batch pixel count overflow".to_string())
                    })?;
            }
        }
        if total_bytes > self.limits.max_batch_input_bytes {
            return Err(worker_error(format!(
                "document batch bytes {total_bytes} exceed worker limit {}",
                self.limits.max_batch_input_bytes
            )));
        }
        if total_pixels > self.limits.max_batch_pixels {
            return Err(worker_error(format!(
                "document batch pixels {total_pixels} exceed worker limit {}",
                self.limits.max_batch_pixels
            )));
        }

        let mut wire_inputs = Vec::with_capacity(inputs.len());
        let mut sidecars = Vec::new();
        for input in inputs {
            let wire = match input.input_ref() {
                EncoderInputRef::Text {
                    content: TextContentRef::Inline(text),
                } => WireDocumentInput::Text { text: text.clone() },
                EncoderInputRef::Image { image } => {
                    let path = match self.write_image_sidecar(input).await {
                        Ok(path) => path,
                        Err(error) => {
                            let _ = remove_sidecars(&sidecars).await;
                            return Err(error);
                        }
                    };
                    let relative_path = sidecar_name(&path)?;
                    sidecars.push(path);
                    WireDocumentInput::Image {
                        path: relative_path,
                        media_type: image.media_type.clone(),
                        width: image.width,
                        height: image.height,
                        encoded_size_bytes: image.encoded_size_bytes,
                    }
                }
                EncoderInputRef::ImageText {
                    image,
                    text: TextContentRef::Inline(text),
                } => {
                    let path = match self.write_image_sidecar(input).await {
                        Ok(path) => path,
                        Err(error) => {
                            let _ = remove_sidecars(&sidecars).await;
                            return Err(error);
                        }
                    };
                    let relative_path = sidecar_name(&path)?;
                    sidecars.push(path);
                    WireDocumentInput::ImageText {
                        path: relative_path,
                        media_type: image.media_type.clone(),
                        width: image.width,
                        height: image.height,
                        encoded_size_bytes: image.encoded_size_bytes,
                        text: text.clone(),
                    }
                }
            };
            wire_inputs.push(wire);
        }
        Ok((wire_inputs, sidecars))
    }

    async fn write_image_sidecar(&self, input: &EncoderDocumentInput) -> Result<PathBuf> {
        let bytes = input.image_bytes().ok_or_else(|| {
            worker_error("validated image input unexpectedly has no image bytes".to_string())
        })?;
        let name = format!("input-{}.bin", Uuid::new_v4());
        let path = self.scratch_dir.join(name);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(|error| worker_error(format!("failed to create image sidecar: {error}")))?;
        file.write_all(bytes)
            .await
            .map_err(|error| worker_error(format!("failed to write image sidecar: {error}")))?;
        file.flush()
            .await
            .map_err(|error| worker_error(format!("failed to flush image sidecar: {error}")))?;
        Ok(path)
    }

    async fn execute(
        &self,
        request: WorkerRequest,
        expected_count: usize,
        max_vectors: usize,
    ) -> Result<Vec<MultiVectorEmbedding>> {
        let request_id = request.request_id().to_string();
        let mut session = self.session.lock().await;
        if session.failed {
            return Err(with_stderr(
                worker_error("pinned worker session has already failed".to_string()),
                &session,
            ));
        }
        let transaction = async {
            write_json_frame(
                &mut session.stdin,
                &request,
                self.limits.max_protocol_line_bytes,
            )
            .await?;
            let response: WorkerResponse =
                read_json_frame(&mut session.stdout, self.limits.max_protocol_line_bytes).await?;
            let outputs = response.into_outputs(&request_id, &self.epoch.id)?;
            self.decode_outputs(outputs, expected_count, max_vectors)
                .await
        };
        match timeout(self.limits.request_timeout, transaction).await {
            Ok(Ok(embeddings)) => Ok(embeddings),
            Ok(Err(ZeppelinError::InvalidImageInput)) => Err(ZeppelinError::InvalidImageInput),
            Ok(Err(error)) => {
                fail_session(&mut session).await;
                Err(with_stderr(error, &session))
            }
            Err(_) => {
                fail_session(&mut session).await;
                Err(with_stderr(
                    worker_error("pinned worker request timed out".to_string()),
                    &session,
                ))
            }
        }
    }

    async fn decode_outputs(
        &self,
        outputs: Vec<TensorSidecar>,
        expected_count: usize,
        max_vectors: usize,
    ) -> Result<Vec<MultiVectorEmbedding>> {
        if outputs.len() != expected_count {
            return Err(worker_error(format!(
                "worker output count mismatch: expected {expected_count}, got {}",
                outputs.len()
            )));
        }
        let expected_dimension = self.output_dimension();
        let mut total_rows = 0_usize;
        let mut embeddings = Vec::with_capacity(outputs.len());
        for output in outputs {
            if output.dtype != "f16_le" {
                return Err(worker_error(format!(
                    "worker tensor dtype must be f16_le, got {}",
                    output.dtype
                )));
            }
            let rows = usize::try_from(output.rows)
                .map_err(|_| worker_error("worker tensor rows exceed usize".to_string()))?;
            let columns = usize::try_from(output.columns)
                .map_err(|_| worker_error("worker tensor columns exceed usize".to_string()))?;
            if rows == 0 || rows > max_vectors {
                return Err(worker_error(format!(
                    "worker tensor row count {rows} is outside 1..={max_vectors}"
                )));
            }
            if columns != expected_dimension {
                return Err(worker_error(format!(
                    "worker tensor dimension mismatch: expected {expected_dimension}, got {columns}"
                )));
            }
            total_rows = total_rows
                .checked_add(rows)
                .ok_or_else(|| worker_error("worker batch row count overflow".to_string()))?;
            if total_rows > self.limits.max_batch_rows {
                return Err(worker_error(format!(
                    "worker batch rows exceed limit {}",
                    self.limits.max_batch_rows
                )));
            }
            let expected_bytes = rows
                .checked_mul(columns)
                .and_then(|scalars| scalars.checked_mul(2))
                .ok_or_else(|| worker_error("worker tensor byte length overflow".to_string()))?;
            let expected_bytes_u64 = u64::try_from(expected_bytes)
                .map_err(|_| worker_error("worker tensor bytes exceed u64".to_string()))?;
            if expected_bytes_u64 > self.limits.max_tensor_bytes {
                return Err(worker_error(format!(
                    "worker tensor bytes {expected_bytes_u64} exceed limit {}",
                    self.limits.max_tensor_bytes
                )));
            }

            let path = confined_sidecar_path(&self.scratch_dir, &output.path).await?;
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|error| worker_error(format!("failed to stat tensor sidecar: {error}")))?;
            if !metadata.is_file() || metadata.len() != expected_bytes_u64 {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(worker_error(format!(
                    "worker tensor length mismatch: expected {expected_bytes_u64}, got {}",
                    metadata.len()
                )));
            }
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|error| worker_error(format!("failed to read tensor sidecar: {error}")))?;
            tokio::fs::remove_file(&path).await.map_err(|error| {
                worker_error(format!("failed to remove tensor sidecar: {error}"))
            })?;
            if bytes.len() != expected_bytes {
                return Err(worker_error(format!(
                    "worker tensor changed length while reading: expected {expected_bytes}, got {}",
                    bytes.len()
                )));
            }
            let mut values = Vec::with_capacity(rows * columns);
            for pair in bytes.chunks_exact(2) {
                let bits = u16::from_le_bytes([pair[0], pair[1]]);
                if bits & 0x7c00 == 0x7c00 {
                    return Err(worker_error(
                        "worker tensor contains a non-finite f16 value".to_string(),
                    ));
                }
                values.push(f16_to_f32(bits));
            }
            embeddings.push(MultiVectorEmbedding::new(
                values,
                rows,
                columns,
                max_vectors,
            )?);
        }
        Ok(embeddings)
    }

    async fn poison(&self) {
        let mut session = self.session.lock().await;
        fail_session(&mut session).await;
    }

    #[cfg(test)]
    async fn failed_and_exited(&self) -> bool {
        let mut session = self.session.lock().await;
        session.failed && session.child.try_wait().ok().flatten().is_some()
    }
}

#[async_trait]
impl MultiVectorEncoder for PinnedWorker {
    fn epoch(&self) -> MultiVectorEpochId {
        self.epoch.id
    }

    fn output_dimension(&self) -> usize {
        self.epoch.vector_dimension as usize
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        self.encode_document_batch(inputs).await
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        self.encode_text_query(input).await
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkerLimits {
    max_batch_units: usize,
    max_batch_input_bytes: u64,
    max_batch_pixels: u64,
    max_batch_rows: usize,
    max_tensor_bytes: u64,
    max_protocol_line_bytes: usize,
    request_timeout: Duration,
}

impl WorkerLimits {
    fn from_config(config: &PinnedWorkerConfig) -> Self {
        Self {
            max_batch_units: config.max_batch_units,
            max_batch_input_bytes: config.max_batch_input_bytes,
            max_batch_pixels: config.max_batch_pixels,
            max_batch_rows: config.max_batch_rows,
            max_tensor_bytes: config.max_tensor_bytes,
            max_protocol_line_bytes: config.max_protocol_line_bytes,
            request_timeout: config.request_timeout,
        }
    }
}

struct Session {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr_tail: Arc<StdMutex<VecDeque<u8>>>,
    stderr_task: JoinHandle<()>,
    failed: bool,
}

#[derive(Debug)]
struct ValidatedPaths {
    python_binary: PathBuf,
    worker_script: PathBuf,
    scratch_dir: PathBuf,
    model_bundle_dir: PathBuf,
}

impl ValidatedPaths {
    async fn resolve(config: &PinnedWorkerConfig) -> Result<Self> {
        let venv_dir = canonical_directory(&config.venv_dir, "venv directory").await?;
        let python_binary = canonical_file(&config.python_binary, "python binary").await?;
        let worker_script = canonical_file(&config.worker_script, "worker script").await?;
        let scratch_dir = canonical_directory(&config.scratch_dir, "scratch directory").await?;
        let model_bundle_dir =
            canonical_directory(&config.model_bundle_dir, "model bundle directory").await?;
        if !config.python_binary.starts_with(&config.venv_dir) {
            return Err(worker_error(
                "pinned worker python binary must be selected through the configured venv"
                    .to_string(),
            ));
        }
        let _ = venv_dir;
        Ok(Self {
            python_binary,
            worker_script,
            scratch_dir,
            model_bundle_dir,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleManifest {
    protocol_version: u32,
    epoch_id: String,
    implementation: String,
    version: String,
    preprocessing_digest: String,
    supported_modalities: Vec<String>,
    artifacts: BTreeMap<String, BundleArtifact>,
    model: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleArtifact {
    path: String,
    sha256: String,
}

/// Fetch and verify one complete S3 model bundle into disposable local storage.
///
/// S3 is re-read on every call. A fresh unreturned directory receives one
/// verified artifact at a time; it is deleted on any error and becomes visible
/// to callers only after final local verification succeeds.
pub async fn materialize_bundle_from_s3(
    store: &ZeppelinStore,
    bundle_prefix: &str,
    bundle_cache_root: &Path,
    epoch: &MultiVectorEpoch,
) -> Result<PathBuf> {
    epoch.validate()?;
    if !bundle_cache_root.is_absolute() {
        return Err(worker_error(
            "pinned worker bundle cache root must be an absolute path".to_string(),
        ));
    }
    let cache_root = canonical_directory(bundle_cache_root, "bundle cache root").await?;
    let prefix = normalize_bundle_prefix(bundle_prefix)?;
    let manifest_key = format!("{prefix}/{BUNDLE_MANIFEST}");
    let manifest_bytes = store.get(&manifest_key).await?;
    let manifest = parse_and_validate_bundle_manifest(&manifest_bytes, epoch)?;

    let mut expected_keys = BTreeSet::from([manifest_key]);
    let mut artifact_keys = Vec::new();
    artifact_keys
        .try_reserve_exact(manifest.artifacts.len())
        .map_err(|error| {
            worker_error(format!("bundle materialization allocation failed: {error}"))
        })?;
    for (name, expected) in &epoch.encoder.artifact_digests {
        let artifact = manifest.artifacts.get(name).ok_or_else(|| {
            worker_error(format!("pinned worker bundle is missing artifact {name}"))
        })?;
        let relative = safe_bundle_relative_path(&artifact.path)?.to_path_buf();
        let key = format!("{prefix}/{}", artifact.path);
        if !expected_keys.insert(key.clone()) {
            return Err(worker_error(
                "pinned worker bundle maps multiple artifacts to one S3 object".to_string(),
            ));
        }
        artifact_keys.push((name.as_str(), relative, key, *expected));
    }

    let bundle_dir = cache_root.join(format!("bundle-{}-{}", epoch.id.to_hex(), Uuid::new_v4()));
    tokio::fs::create_dir(&bundle_dir).await.map_err(|error| {
        worker_error(format!("failed to create bundle cache directory: {error}"))
    })?;
    let materialized = async {
        for (name, relative, key, expected) in artifact_keys {
            let bytes = store.get(&key).await?;
            let actual: [u8; 32] = Sha256::digest(&bytes).into();
            if actual != *expected.as_bytes() {
                return Err(worker_error(format!(
                    "pinned worker S3 artifact digest mismatch for {name}"
                )));
            }
            write_materialized_bundle_file(&bundle_dir, &relative, &bytes).await?;
        }
        let observed_keys = store
            .list_prefix(&format!("{prefix}/"))
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if observed_keys != expected_keys {
            return Err(worker_error(
                "pinned worker S3 bundle contains an undeclared or missing object".to_string(),
            ));
        }
        write_materialized_bundle_file(&bundle_dir, Path::new(BUNDLE_MANIFEST), &manifest_bytes)
            .await?;
        load_and_verify_bundle(&bundle_dir, epoch).await?;
        canonical_directory(&bundle_dir, "materialized model bundle").await
    }
    .await;
    if materialized.is_err() {
        let _ = tokio::fs::remove_dir_all(&bundle_dir).await;
    }
    materialized
}

async fn remove_disposable_directory(path: &Path, label: &str) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(worker_error(format!("failed to remove {label}: {error}"))),
    }
}

async fn load_and_verify_bundle(
    bundle_dir: &Path,
    epoch: &MultiVectorEpoch,
) -> Result<BundleManifest> {
    let manifest_path = bundle_dir.join(BUNDLE_MANIFEST);
    let manifest_bytes = tokio::fs::read(&manifest_path).await.map_err(|error| {
        worker_error(format!(
            "failed to read pinned worker bundle manifest: {error}"
        ))
    })?;
    let manifest = parse_and_validate_bundle_manifest(&manifest_bytes, epoch)?;
    let mut declared_paths = BTreeSet::new();
    for (name, expected) in &epoch.encoder.artifact_digests {
        let artifact = manifest.artifacts.get(name).ok_or_else(|| {
            worker_error(format!("pinned worker bundle is missing artifact {name}"))
        })?;
        let relative = safe_bundle_relative_path(&artifact.path)?;
        if !declared_paths.insert(relative.to_path_buf()) {
            return Err(worker_error(
                "pinned worker bundle maps multiple artifacts to one file".to_string(),
            ));
        }
        let path = confined_bundle_path(bundle_dir, &artifact.path).await?;
        let actual = sha256_file(&path).await?;
        if actual != *expected.as_bytes() {
            return Err(worker_error(format!(
                "pinned worker bundle artifact digest mismatch for {name}"
            )));
        }
    }
    verify_bundle_inventory(bundle_dir, &declared_paths).await?;
    Ok(manifest)
}

fn parse_and_validate_bundle_manifest(
    manifest_bytes: &[u8],
    epoch: &MultiVectorEpoch,
) -> Result<BundleManifest> {
    if manifest_bytes.len() > MAX_BUNDLE_MANIFEST_BYTES {
        return Err(worker_error(
            "pinned worker bundle manifest exceeds 1 MiB".to_string(),
        ));
    }
    let manifest: BundleManifest = serde_json::from_slice(manifest_bytes)
        .map_err(|error| worker_error(format!("invalid pinned worker bundle manifest: {error}")))?;
    if manifest.protocol_version != PROTOCOL_VERSION
        || manifest.epoch_id != epoch.id.to_hex()
        || manifest.implementation != epoch.encoder.implementation
        || manifest.version != epoch.encoder.version
        || manifest.preprocessing_digest != epoch.preprocessing_digest.to_hex()
    {
        return Err(worker_error(
            "pinned worker bundle identity does not match the semantic epoch".to_string(),
        ));
    }
    let expected_modalities = canonical_modalities(&epoch.encoder.supported_modalities);
    let mut actual_modalities = manifest.supported_modalities.clone();
    actual_modalities.sort();
    actual_modalities.dedup();
    if actual_modalities != expected_modalities {
        return Err(worker_error(
            "pinned worker bundle modalities do not match the semantic epoch".to_string(),
        ));
    }
    if manifest.artifacts.len() != epoch.encoder.artifact_digests.len() {
        return Err(worker_error(
            "pinned worker bundle artifact set does not match the semantic epoch".to_string(),
        ));
    }
    for (name, expected) in &epoch.encoder.artifact_digests {
        let artifact = manifest.artifacts.get(name).ok_or_else(|| {
            worker_error(format!("pinned worker bundle is missing artifact {name}"))
        })?;
        if artifact.sha256 != expected.to_hex() {
            return Err(worker_error(format!(
                "pinned worker bundle digest declaration mismatch for artifact {name}"
            )));
        }
        safe_bundle_relative_path(&artifact.path)?;
    }
    if !manifest.model.is_object() {
        return Err(worker_error(
            "pinned worker bundle model declaration must be an object".to_string(),
        ));
    }
    Ok(manifest)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerHandshake {
    #[serde(rename = "type")]
    kind: String,
    protocol_version: u32,
    epoch_id: String,
    implementation: String,
    version: String,
    preprocessing_digest: String,
    supported_modalities: Vec<String>,
    artifact_digests: BTreeMap<String, String>,
    output_dimension: u32,
}

fn validate_handshake(
    handshake: &WorkerHandshake,
    bundle: &BundleManifest,
    epoch: &MultiVectorEpoch,
) -> Result<()> {
    if handshake.kind != "hello"
        || handshake.protocol_version != PROTOCOL_VERSION
        || handshake.epoch_id != epoch.id.to_hex()
        || handshake.epoch_id != bundle.epoch_id
        || handshake.implementation != epoch.encoder.implementation
        || handshake.version != epoch.encoder.version
        || handshake.preprocessing_digest != epoch.preprocessing_digest.to_hex()
        || handshake.output_dimension != epoch.vector_dimension
    {
        return Err(worker_error(
            "pinned worker handshake identity mismatch".to_string(),
        ));
    }
    let mut modalities = handshake.supported_modalities.clone();
    modalities.sort();
    modalities.dedup();
    if modalities != canonical_modalities(&epoch.encoder.supported_modalities) {
        return Err(worker_error(
            "pinned worker handshake modality mismatch".to_string(),
        ));
    }
    let expected_digests = epoch
        .encoder
        .artifact_digests
        .iter()
        .map(|(name, digest)| (name.clone(), digest.to_hex()))
        .collect::<BTreeMap<_, _>>();
    if handshake.artifact_digests != expected_digests {
        return Err(worker_error(
            "pinned worker handshake artifact digest mismatch".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerRequest {
    EncodeDocuments {
        protocol_version: u32,
        request_id: String,
        inputs: Vec<WireDocumentInput>,
    },
    EncodeQuery {
        protocol_version: u32,
        request_id: String,
        text: String,
    },
}

impl WorkerRequest {
    fn request_id(&self) -> &str {
        match self {
            Self::EncodeDocuments { request_id, .. } | Self::EncodeQuery { request_id, .. } => {
                request_id
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireDocumentInput {
    Text {
        text: String,
    },
    Image {
        path: String,
        media_type: String,
        width: u32,
        height: u32,
        encoded_size_bytes: u64,
    },
    ImageText {
        path: String,
        media_type: String,
        width: u32,
        height: u32,
        encoded_size_bytes: u64,
        text: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerResponse {
    Encoded {
        protocol_version: u32,
        request_id: String,
        epoch_id: String,
        outputs: Vec<TensorSidecar>,
    },
    Error {
        protocol_version: u32,
        request_id: String,
        epoch_id: String,
        code: String,
        message: String,
    },
}

impl WorkerResponse {
    fn into_outputs(
        self,
        expected_request_id: &str,
        expected_epoch: &MultiVectorEpochId,
    ) -> Result<Vec<TensorSidecar>> {
        match self {
            Self::Encoded {
                protocol_version,
                request_id,
                epoch_id,
                outputs,
            } => {
                validate_response_identity(
                    protocol_version,
                    &request_id,
                    &epoch_id,
                    expected_request_id,
                    expected_epoch,
                )?;
                Ok(outputs)
            }
            Self::Error {
                protocol_version,
                request_id,
                epoch_id,
                code,
                message,
            } => {
                validate_response_identity(
                    protocol_version,
                    &request_id,
                    &epoch_id,
                    expected_request_id,
                    expected_epoch,
                )?;
                if code == "invalid_image" {
                    Err(ZeppelinError::InvalidImageInput)
                } else {
                    Err(worker_error(format!(
                        "pinned worker rejected request ({code}): {message}"
                    )))
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TensorSidecar {
    path: String,
    dtype: String,
    rows: u64,
    columns: u64,
}

fn validate_response_identity(
    protocol_version: u32,
    request_id: &str,
    epoch_id: &str,
    expected_request_id: &str,
    expected_epoch: &MultiVectorEpochId,
) -> Result<()> {
    if protocol_version != PROTOCOL_VERSION
        || request_id != expected_request_id
        || epoch_id != expected_epoch.to_hex()
    {
        return Err(worker_error(
            "pinned worker response identity mismatch".to_string(),
        ));
    }
    Ok(())
}

async fn write_json_frame<W: AsyncWriteExt + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    max_bytes: usize,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| worker_error(format!("failed to serialize worker request: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(worker_error(format!(
            "worker request frame exceeds {max_bytes} bytes"
        )));
    }
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| worker_error(format!("failed to write worker request: {error}")))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|error| worker_error(format!("failed to terminate worker request: {error}")))?;
    writer
        .flush()
        .await
        .map_err(|error| worker_error(format!("failed to flush worker request: {error}")))
}

async fn read_json_frame<R: AsyncBufRead + Unpin, T: for<'de> Deserialize<'de>>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<T> {
    let bytes = read_bounded_line(reader, max_bytes).await?;
    serde_json::from_slice(&bytes)
        .map_err(|error| worker_error(format!("invalid worker JSON frame: {error}")))
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| worker_error(format!("failed to read worker response: {error}")))?;
        if available.is_empty() {
            return Err(worker_error(
                "worker closed stdout before completing a JSON frame".to_string(),
            ));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index);
        if output.len().saturating_add(take) > max_bytes {
            return Err(worker_error(format!(
                "worker response frame exceeds {max_bytes} bytes"
            )));
        }
        output.extend_from_slice(&available[..take]);
        let consumed = take + usize::from(newline.is_some());
        reader.consume(consumed);
        if newline.is_some() {
            if output.last() == Some(&b'\r') {
                output.pop();
            }
            return Ok(output);
        }
    }
}

fn drain_stderr(
    mut stderr: ChildStderr,
    tail: Arc<StdMutex<VecDeque<u8>>>,
    max_bytes: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        loop {
            let count = match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(count) => count,
            };
            if let Ok(mut retained) = tail.lock() {
                for byte in &buffer[..count] {
                    retained.push_back(*byte);
                    if retained.len() > max_bytes {
                        retained.pop_front();
                    }
                }
            }
        }
    })
}

async fn fail_session(session: &mut Session) {
    if !session.failed {
        session.failed = true;
        let _ = session.child.kill().await;
        let _ = session.child.wait().await;
        session.stderr_task.abort();
    }
}

fn with_stderr(error: ZeppelinError, session: &Session) -> ZeppelinError {
    let stderr = session
        .stderr_tail
        .lock()
        .ok()
        .map(|tail| tail.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    if stderr.is_empty() {
        return error;
    }
    worker_error(format!(
        "{error}; worker stderr tail: {}",
        String::from_utf8_lossy(&stderr)
    ))
}

async fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| worker_error(format!("failed to resolve {label}: {error}")))?;
    let metadata = tokio::fs::metadata(&canonical)
        .await
        .map_err(|error| worker_error(format!("failed to stat {label}: {error}")))?;
    if !metadata.is_dir() {
        return Err(worker_error(format!("{label} is not a directory")));
    }
    Ok(canonical)
}

async fn canonical_file(path: &Path, label: &str) -> Result<PathBuf> {
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| worker_error(format!("failed to resolve {label}: {error}")))?;
    let metadata = tokio::fs::metadata(&canonical)
        .await
        .map_err(|error| worker_error(format!("failed to stat {label}: {error}")))?;
    if !metadata.is_file() {
        return Err(worker_error(format!("{label} is not a file")));
    }
    Ok(canonical)
}

async fn create_session_scratch(root: &Path) -> Result<PathBuf> {
    let path = root.join(format!("worker-{}", Uuid::new_v4()));
    tokio::fs::create_dir(&path)
        .await
        .map_err(|error| worker_error(format!("failed to create worker scratch: {error}")))?;
    canonical_directory(&path, "worker session scratch").await
}

fn normalize_bundle_prefix(value: &str) -> Result<String> {
    validate_bundle_prefix(value)?;
    Ok(value.to_string())
}

async fn write_materialized_bundle_file(
    bundle_dir: &Path,
    relative: &Path,
    bytes: &[u8],
) -> Result<()> {
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let parent_dir = bundle_dir.join(parent);
    tokio::fs::create_dir_all(&parent_dir)
        .await
        .map_err(|error| worker_error(format!("failed to create bundle cache path: {error}")))?;
    let path = bundle_dir.join(relative);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(|error| worker_error(format!("failed to create cached bundle file: {error}")))?;
    file.write_all(bytes)
        .await
        .map_err(|error| worker_error(format!("failed to write cached bundle file: {error}")))?;
    file.flush()
        .await
        .map_err(|error| worker_error(format!("failed to flush cached bundle file: {error}")))?;
    Ok(())
}

async fn confined_bundle_path(bundle_dir: &Path, relative: &str) -> Result<PathBuf> {
    let relative = safe_bundle_relative_path(relative)?;
    let path = canonical_file(&bundle_dir.join(relative), "bundle artifact").await?;
    if !path.starts_with(bundle_dir) {
        return Err(worker_error(
            "pinned worker bundle artifact escapes the bundle directory".to_string(),
        ));
    }
    Ok(path)
}

fn safe_bundle_relative_path(value: &str) -> Result<&Path> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || value.contains('\\')
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(worker_error(
            "bundle artifact path must stay relative to the bundle".to_string(),
        ));
    }
    Ok(path)
}

async fn confined_sidecar_path(scratch_dir: &Path, relative: &str) -> Result<PathBuf> {
    let relative = strict_relative_path(relative, "tensor sidecar")?;
    let path = canonical_file(&scratch_dir.join(relative), "tensor sidecar").await?;
    if !path.starts_with(scratch_dir) {
        return Err(worker_error(
            "worker tensor sidecar escapes the session scratch directory".to_string(),
        ));
    }
    Ok(path)
}

fn strict_relative_path<'a>(value: &'a str, label: &str) -> Result<&'a Path> {
    let path = Path::new(value);
    let mut components = path.components();
    let Some(Component::Normal(_)) = components.next() else {
        return Err(worker_error(format!(
            "{label} path must be a relative file name"
        )));
    };
    if components.next().is_some() {
        return Err(worker_error(format!(
            "{label} path must be a relative file name"
        )));
    }
    Ok(path)
}

fn sidecar_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| worker_error("worker sidecar name is not valid UTF-8".to_string()))
}

async fn sha256_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| worker_error(format!("failed to open bundle artifact: {error}")))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| worker_error(format!("failed to hash bundle artifact: {error}")))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize().into())
}

async fn verify_bundle_inventory(
    bundle_dir: &Path,
    declared_paths: &BTreeSet<PathBuf>,
) -> Result<()> {
    let mut pending = vec![bundle_dir.to_path_buf()];
    let mut observed = BTreeSet::new();
    while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(&directory)
            .await
            .map_err(|error| worker_error(format!("failed to inspect model bundle: {error}")))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| worker_error(format!("failed to inspect model bundle: {error}")))?
        {
            let path = entry.path();
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|error| worker_error(format!("failed to stat model bundle: {error}")))?;
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                return Err(worker_error(
                    "pinned worker bundle contains a non-file artifact".to_string(),
                ));
            }
            let relative = path.strip_prefix(bundle_dir).map_err(|_| {
                worker_error("pinned worker bundle entry escapes its root".to_string())
            })?;
            if relative == Path::new(BUNDLE_MANIFEST) {
                continue;
            }
            observed.insert(relative.to_path_buf());
        }
    }
    if &observed != declared_paths {
        return Err(worker_error(
            "pinned worker bundle contains an undeclared or missing artifact file".to_string(),
        ));
    }
    Ok(())
}

async fn remove_sidecars(paths: &[PathBuf]) -> Result<()> {
    for path in paths {
        tokio::fs::remove_file(path)
            .await
            .map_err(|error| worker_error(format!("failed to remove input sidecar: {error}")))?;
    }
    Ok(())
}

fn image_dimensions(input: &EncoderInputRef) -> Option<(u32, u32)> {
    match input {
        EncoderInputRef::Text { .. } => None,
        EncoderInputRef::Image { image } | EncoderInputRef::ImageText { image, .. } => {
            Some((image.width, image.height))
        }
    }
}

fn canonical_modalities(modalities: &[InputModality]) -> Vec<String> {
    let mut values = modalities
        .iter()
        .map(|modality| modality.as_str().to_string())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn f16_to_f32(bits: u16) -> f32 {
    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exponent = (bits >> 10) & 0x1f;
    let mantissa = bits & 0x03ff;
    if exponent == 0 {
        sign * f32::from(mantissa) * 2.0_f32.powi(-24)
    } else {
        sign * (1.0 + f32::from(mantissa) / 1024.0) * 2.0_f32.powi(i32::from(exponent) - 15)
    }
}

fn worker_error(message: String) -> ZeppelinError {
    ZeppelinError::EncoderWorker(message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::{
        load_and_verify_bundle, materialize_bundle_from_s3, MultiVectorEncoder, PinnedWorker,
        PinnedWorkerConfig,
    };
    use crate::embedding::encoder::EncoderQueryInput;
    use crate::embedding::{
        ArtifactChecksum, EncoderExecutionRef, ExactScorerVersion, InputModality, MatrixDtype,
        MultiVectorEpoch, MultiVectorEpochId, NormalizationRecipe, VectorTransformRecipe,
    };
    use crate::error::ZeppelinError;
    use crate::storage::ZeppelinStore;

    const TEST_ARTIFACT_BYTES: &[u8] = b"pinned-test-artifact";

    fn python_binary() -> PathBuf {
        for candidate in [
            "/usr/bin/python3",
            "/usr/local/bin/python3",
            "/opt/homebrew/bin/python3",
        ] {
            let path = PathBuf::from(candidate);
            if path.is_file() {
                return path;
            }
        }
        panic!("worker unit tests require an absolute python3 binary");
    }

    fn epoch(artifact_digest: ArtifactChecksum) -> MultiVectorEpoch {
        let mut artifacts = BTreeMap::new();
        artifacts.insert("model.bin".to_string(), artifact_digest);
        let mut epoch = MultiVectorEpoch {
            id: MultiVectorEpochId::new([0; 32]),
            encoder: EncoderExecutionRef {
                implementation: "test_worker".to_string(),
                version: "v1".to_string(),
                bundle_prefix: Some("models/test-worker-v1".to_string()),
                artifact_digests: artifacts,
                supported_modalities: vec![InputModality::Text],
            },
            preprocessing_digest: ArtifactChecksum::new([3; 32]),
            vector_dimension: 2,
            max_query_vectors: 4,
            max_document_vectors: 4,
            output_normalization: NormalizationRecipe::L2,
            exact_scoring_transform: VectorTransformRecipe::Identity,
            matrix_dtype: MatrixDtype::F16,
            exact_scorer: ExactScorerVersion::MaxSimV1,
        };
        epoch.id = epoch.canonical_id().expect("test epoch canonicalizes");
        epoch
    }

    fn fixture(script_body: &str) -> (TempDir, PinnedWorkerConfig, MultiVectorEpoch) {
        let temp = TempDir::new().expect("tempdir");
        let bundle = temp.path().join("bundle");
        let scratch = temp.path().join("scratch");
        let script = temp.path().join("worker.py");
        fs::create_dir(&bundle).expect("bundle dir");
        fs::create_dir(&scratch).expect("scratch dir");
        fs::write(&script, script_body).expect("script");
        fs::write(bundle.join("model.bin"), TEST_ARTIFACT_BYTES).expect("artifact");
        let artifact_digest = ArtifactChecksum::new(Sha256::digest(TEST_ARTIFACT_BYTES).into());
        let epoch = epoch(artifact_digest);
        fs::write(
            bundle.join("worker.json"),
            bundle_manifest(&epoch, "model.bin"),
        )
        .expect("manifest");
        let python = python_binary();
        let config = PinnedWorkerConfig {
            venv_dir: python.parent().expect("python parent").to_path_buf(),
            python_binary: python,
            worker_script: script,
            scratch_dir: scratch,
            model_bundle_dir: bundle,
            max_batch_units: 4,
            max_batch_input_bytes: 1024,
            max_batch_pixels: 1024,
            max_batch_rows: 8,
            max_tensor_bytes: 1024,
            max_protocol_line_bytes: 16 * 1024,
            max_stderr_bytes: 4096,
            handshake_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_millis(100),
        };
        (temp, config, epoch)
    }

    fn bundle_manifest(epoch: &MultiVectorEpoch, artifact_path: &str) -> Vec<u8> {
        let artifact_digest = epoch
            .encoder
            .artifact_digests
            .get("model.bin")
            .expect("model digest");
        let manifest = json!({
            "protocol_version": 1,
            "epoch_id": epoch.id.to_hex(),
            "implementation": "test_worker",
            "version": "v1",
            "preprocessing_digest": epoch.preprocessing_digest.to_hex(),
            "supported_modalities": ["text"],
            "artifacts": {
                "model.bin": {
                    "path": artifact_path,
                    "sha256": artifact_digest.to_hex()
                }
            },
            "model": {"kind": "test"}
        });
        serde_json::to_vec(&manifest).expect("manifest JSON")
    }

    fn hello_script(epoch: &MultiVectorEpoch, request_body: &str) -> String {
        let digests = epoch
            .encoder
            .artifact_digests
            .iter()
            .map(|(name, digest)| (name.clone(), digest.to_hex()))
            .collect::<BTreeMap<_, _>>();
        format!(
            r#"import json, sys, time
hello = {{
  "type": "hello",
  "protocol_version": 1,
  "epoch_id": {epoch_id:?},
  "implementation": "test_worker",
  "version": "v1",
  "preprocessing_digest": {preprocessing:?},
  "supported_modalities": ["text"],
  "artifact_digests": {digests},
  "output_dimension": 2
}}
print(json.dumps(hello), flush=True)
request = json.loads(sys.stdin.readline())
{request_body}
"#,
            epoch_id = epoch.id.to_hex(),
            preprocessing = epoch.preprocessing_digest.to_hex(),
            digests = serde_json::to_string(&digests).expect("digests JSON"),
        )
    }

    #[tokio::test]
    async fn s3_bundle_materializes_only_after_complete_verification() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let cache = TempDir::new().expect("cache");
        let artifact_digest = ArtifactChecksum::new(Sha256::digest(TEST_ARTIFACT_BYTES).into());
        let epoch = epoch(artifact_digest);
        store
            .put_create(
                "models/qualified/worker.json",
                Bytes::from(bundle_manifest(&epoch, "nested/model.bin")),
            )
            .await
            .expect("manifest");
        store
            .put_create(
                "models/qualified/nested/model.bin",
                Bytes::from_static(TEST_ARTIFACT_BYTES),
            )
            .await
            .expect("artifact");

        let materialized =
            materialize_bundle_from_s3(&store, "models/qualified", cache.path(), &epoch)
                .await
                .expect("materialized bundle");
        assert_eq!(
            tokio::fs::read(materialized.join("nested/model.bin"))
                .await
                .expect("cached artifact"),
            TEST_ARTIFACT_BYTES
        );
        load_and_verify_bundle(&materialized, &epoch)
            .await
            .expect("local verification reuses the production verifier");
    }

    #[tokio::test]
    async fn s3_spawn_owns_and_removes_its_materialized_bundle() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let script = hello_script(&epoch, "pass");
        let (_temp, config, epoch) = fixture(&script);
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let cache = TempDir::new().expect("cache");
        store
            .put_create(
                "models/test-worker-v1/worker.json",
                Bytes::from(bundle_manifest(&epoch, "model.bin")),
            )
            .await
            .expect("manifest");
        store
            .put_create(
                "models/test-worker-v1/model.bin",
                Bytes::from_static(TEST_ARTIFACT_BYTES),
            )
            .await
            .expect("artifact");

        let worker = PinnedWorker::spawn_from_s3(
            &store,
            config,
            "models/test-worker-v1",
            cache.path(),
            epoch,
        )
        .await
        .expect("S3-backed worker starts");
        let materialized = worker
            .owned_bundle_dir
            .clone()
            .expect("S3 spawn owns its bundle");
        assert!(materialized.join("worker.json").is_file());

        worker.shutdown().await.expect("worker shutdown");
        assert!(!materialized.exists());
    }

    #[tokio::test]
    async fn s3_bundle_rejects_missing_and_digest_mismatched_files() {
        let cache = TempDir::new().expect("cache");
        let artifact_digest = ArtifactChecksum::new(Sha256::digest(TEST_ARTIFACT_BYTES).into());
        let epoch = epoch(artifact_digest);

        let missing_store = ZeppelinStore::new(Arc::new(InMemory::new()));
        missing_store
            .put_create(
                "models/missing/worker.json",
                Bytes::from(bundle_manifest(&epoch, "model.bin")),
            )
            .await
            .expect("manifest");
        assert!(
            materialize_bundle_from_s3(&missing_store, "models/missing", cache.path(), &epoch)
                .await
                .is_err()
        );

        let corrupt_store = ZeppelinStore::new(Arc::new(InMemory::new()));
        corrupt_store
            .put_create(
                "models/corrupt/worker.json",
                Bytes::from(bundle_manifest(&epoch, "model.bin")),
            )
            .await
            .expect("manifest");
        corrupt_store
            .put_create("models/corrupt/model.bin", Bytes::from_static(b"wrong"))
            .await
            .expect("corrupt artifact");
        let error =
            materialize_bundle_from_s3(&corrupt_store, "models/corrupt", cache.path(), &epoch)
                .await
                .expect_err("digest mismatch");
        assert!(error.to_string().contains("digest mismatch"));
        let mut cache_entries = tokio::fs::read_dir(cache.path())
            .await
            .expect("read cache root");
        assert!(
            cache_entries
                .next_entry()
                .await
                .expect("read cache entry")
                .is_none(),
            "failed materialization must remove its partial directory"
        );
    }

    #[tokio::test]
    async fn s3_bundle_rejects_unsafe_paths_and_extra_objects() {
        let cache = TempDir::new().expect("cache");
        let artifact_digest = ArtifactChecksum::new(Sha256::digest(TEST_ARTIFACT_BYTES).into());
        let epoch = epoch(artifact_digest);

        let unsafe_store = ZeppelinStore::new(Arc::new(InMemory::new()));
        unsafe_store
            .put_create(
                "models/unsafe/worker.json",
                Bytes::from(bundle_manifest(&epoch, "../model.bin")),
            )
            .await
            .expect("manifest");
        let error =
            materialize_bundle_from_s3(&unsafe_store, "models/unsafe", cache.path(), &epoch)
                .await
                .expect_err("unsafe path");
        assert!(error.to_string().contains("stay relative"));

        let extra_store = ZeppelinStore::new(Arc::new(InMemory::new()));
        extra_store
            .put_create(
                "models/extra/worker.json",
                Bytes::from(bundle_manifest(&epoch, "model.bin")),
            )
            .await
            .expect("manifest");
        extra_store
            .put_create(
                "models/extra/model.bin",
                Bytes::from_static(TEST_ARTIFACT_BYTES),
            )
            .await
            .expect("artifact");
        extra_store
            .put_create("models/extra/undeclared.bin", Bytes::from_static(b"extra"))
            .await
            .expect("extra artifact");
        let error = materialize_bundle_from_s3(&extra_store, "models/extra", cache.path(), &epoch)
            .await
            .expect_err("extra object");
        assert!(error.to_string().contains("undeclared or missing object"));
        let mut cache_entries = tokio::fs::read_dir(cache.path())
            .await
            .expect("read cache root");
        assert!(
            cache_entries
                .next_entry()
                .await
                .expect("read cache entry")
                .is_none(),
            "inventory failure must remove its partial directory"
        );
    }

    #[tokio::test]
    async fn handshake_identity_mismatch_is_fatal() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let script = hello_script(&epoch, "").replace(&epoch.id.to_hex(), &"f".repeat(64));
        let (_temp, config, epoch) = fixture(&script);
        let error = PinnedWorker::spawn(config, epoch)
            .await
            .err()
            .expect("mismatched handshake must fail");
        assert!(error.to_string().contains("handshake"));
    }

    #[tokio::test]
    async fn request_timeout_kills_and_permanently_fails_session() {
        let (_bootstrap_temp, bootstrap_config, epoch) = fixture("");
        let script = hello_script(&epoch, "time.sleep(60)");
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");
        let error = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("request must time out");
        assert!(error.to_string().contains("timed out"));
        assert!(worker.failed_and_exited().await);
        let second = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("failed session must not restart");
        assert!(second.to_string().contains("already failed"));
        drop(bootstrap_config);
    }

    #[tokio::test]
    async fn invalid_image_rejection_is_typed_and_session_remains_usable() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let body = r#"print(json.dumps({
  "type": "error", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "code": "invalid_image", "message": "encoded image cannot be decoded"
}), flush=True)
request = json.loads(sys.stdin.readline())
open("tensor.bin", "wb").write(bytes.fromhex("003c003c"))
print(json.dumps({
  "type": "encoded", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "outputs": [{"path": "tensor.bin", "dtype": "f16_le", "rows": 1, "columns": 2}]
}), flush=True)"#;
        let script = hello_script(&epoch, body);
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");

        let error = worker
            .encode_query(EncoderQueryInput::new("bad image").expect("query"))
            .await
            .expect_err("invalid image response must be rejected");
        assert!(matches!(error, ZeppelinError::InvalidImageInput));

        worker
            .encode_query(EncoderQueryInput::new("later query").expect("query"))
            .await
            .expect("request-specific rejection must preserve the session");
        worker.shutdown().await.expect("worker shutdown");
    }

    #[tokio::test]
    async fn non_image_worker_rejection_permanently_fails_session() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let body = r#"print(json.dumps({
  "type": "error", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "code": "encoder_output", "message": "invalid encoder output"
}), flush=True)"#;
        let script = hello_script(&epoch, body);
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");

        let error = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("encoder-output rejection must fail");
        assert!(matches!(error, ZeppelinError::EncoderWorker(_)));
        assert!(worker.failed_and_exited().await);

        let second = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("failed session must not be retained as usable");
        assert!(second.to_string().contains("already failed"));
    }

    #[tokio::test]
    async fn malformed_tensor_response_is_rejected() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let body = r#"print(json.dumps({
  "type": "encoded", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "outputs": [{"path": "tensor.bin", "dtype": "f32_le", "rows": 1, "columns": 2}]
}), flush=True)"#;
        let script = hello_script(&epoch, body);
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");
        let error = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("malformed tensor must fail");
        assert!(error.to_string().contains("dtype"));
    }

    #[tokio::test]
    async fn out_of_scratch_tensor_sidecar_is_rejected() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let body = r#"print(json.dumps({
  "type": "encoded", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "outputs": [{"path": "../escape.bin", "dtype": "f16_le", "rows": 1, "columns": 2}]
}), flush=True)"#;
        let script = hello_script(&epoch, body);
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");
        let error = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("escaping sidecar must fail");
        assert!(error.to_string().contains("relative file name"));
    }

    #[tokio::test]
    async fn wrong_length_tensor_sidecar_is_rejected() {
        let (_bootstrap_temp, _bootstrap_config, epoch) = fixture("");
        let body = r#"open("tensor.bin", "wb").write(b"\x00\x00")
print(json.dumps({
  "type": "encoded", "protocol_version": 1,
  "request_id": request["request_id"], "epoch_id": hello["epoch_id"],
  "outputs": [{"path": "tensor.bin", "dtype": "f16_le", "rows": 1, "columns": 2}]
}), flush=True)"#;
        let script = hello_script(&epoch, body);
        let (_temp, config, epoch) = fixture(&script);
        let worker = PinnedWorker::spawn(config, epoch)
            .await
            .expect("worker starts");
        let error = worker
            .encode_query(EncoderQueryInput::new("query").expect("query"))
            .await
            .expect_err("wrong-length sidecar must fail");
        assert!(error.to_string().contains("length mismatch"));
    }

    /// Manual production-bundle round trip. Set all paths before running.
    #[tokio::test]
    #[ignore = "requires a pinned local model bundle and production Python venv"]
    async fn real_model_round_trip() {
        let venv = PathBuf::from(
            std::env::var_os("ZEPPELIN_MMLI_TEST_VENV")
                .expect("ZEPPELIN_MMLI_TEST_VENV must be set"),
        );
        let bundle = PathBuf::from(
            std::env::var_os("ZEPPELIN_MMLI_TEST_BUNDLE")
                .expect("ZEPPELIN_MMLI_TEST_BUNDLE must be set"),
        );
        let scratch = PathBuf::from(
            std::env::var_os("ZEPPELIN_MMLI_TEST_SCRATCH")
                .expect("ZEPPELIN_MMLI_TEST_SCRATCH must be set"),
        );
        let epoch_path = PathBuf::from(
            std::env::var_os("ZEPPELIN_MMLI_TEST_EPOCH_JSON")
                .expect("ZEPPELIN_MMLI_TEST_EPOCH_JSON must be set"),
        );
        let epoch: MultiVectorEpoch = serde_json::from_slice(
            &fs::read(epoch_path).expect("production epoch JSON must be readable"),
        )
        .expect("production epoch JSON must decode");
        let worker_script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lab/mmli/worker/worker.py");
        let python_binary = if cfg!(windows) {
            venv.join("Scripts/python.exe")
        } else {
            venv.join("bin/python")
        };
        let worker = PinnedWorker::spawn(
            PinnedWorkerConfig {
                venv_dir: venv,
                python_binary,
                worker_script,
                scratch_dir: scratch,
                model_bundle_dir: bundle,
                max_batch_units: 8,
                max_batch_input_bytes: 32 * 1024 * 1024,
                max_batch_pixels: 64 * 1024 * 1024,
                max_batch_rows: 16 * 1024,
                max_tensor_bytes: 8 * 1024 * 1024,
                max_protocol_line_bytes: 1024 * 1024,
                max_stderr_bytes: 64 * 1024,
                handshake_timeout: Duration::from_secs(120),
                request_timeout: Duration::from_secs(120),
            },
            epoch,
        )
        .await
        .expect("production worker must start");
        let embedding = worker
            .encode_query(EncoderQueryInput::new("scientific evidence").expect("query"))
            .await
            .expect("production query must encode");
        assert!(embedding.vector_count() > 0);
    }
}
