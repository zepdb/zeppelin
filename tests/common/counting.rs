//! GET-counting wrapper around any `ObjectStore`, for asserting how many
//! S3 GETs a code path performs per key pattern.
//!
//! Wraps the harness's real backend (S3/MinIO/memory), so tests still hit
//! real object storage — the wrapper only observes.
//!
//! F1/H12: in addition to per-key counts, every GET and PUT is attributed to
//! an [`ArtifactClass`] bucket
//! (cluster_/attrs_/bootstrap/sq_/bitmap_/coarse_sketch/fts/wal/manifest) with BOTH
//! operation counts and byte counts per bucket. GET bytes are the actual
//! returned payload size (`GetResult::range`), PUT bytes the actual request
//! body size (`PutPayload::content_length`).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use serde::Serialize;
use zeppelin::storage::ZeppelinStore;

/// The kind of S3 artifact a key refers to, derived from the real key
/// builders in `src/` (see `classify`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum ArtifactClass {
    /// `{ns}/segments/{seg}/cluster_{i}.bin` — raw f32 cluster blobs.
    Cluster,
    /// `{ns}/segments/{seg}/attrs_{i}.bin` — per-cluster attribute blobs.
    Attrs,
    /// `{ns}/segments/{seg}/centroids.bin` — IVF centroid blob.
    Centroids,
    /// `{ns}/segments/{seg}/bootstrap.bin` — immutable centroids + sketch blob.
    Bootstrap,
    /// Quantization sidecars: `sq_calibration.bin`, `sq_cluster_{i}.bin`,
    /// `pq_codebook.bin`, `pq_cluster_{i}.bin`.
    Sq,
    /// `{ns}/segments/{seg}/bitmap_{i}.bin` — roaring bitmap pre-filter.
    Bitmap,
    /// `{ns}/segments/{seg}/coarse_sketch.bin` — resident cluster-selection sketch.
    Sketch,
    /// `global_fts.bin`, `fts_index_{i}.bin`, `fts_meta.json`.
    Fts,
    /// `{ns}/wal/{ulid}.wal` — WAL fragments.
    Wal,
    /// `{ns}/manifest.json` — the authoritative manifest.
    Manifest,
    /// Anything else (`meta.json`, `lease.json`, `tree_meta.json`,
    /// `node_{id}.bin`, unknown keys).
    Other,
}

/// All classes, in display/index order. Index MUST match `as usize`.
pub const ALL_CLASSES: [ArtifactClass; 11] = [
    ArtifactClass::Cluster,
    ArtifactClass::Attrs,
    ArtifactClass::Centroids,
    ArtifactClass::Bootstrap,
    ArtifactClass::Sq,
    ArtifactClass::Bitmap,
    ArtifactClass::Sketch,
    ArtifactClass::Fts,
    ArtifactClass::Wal,
    ArtifactClass::Manifest,
    ArtifactClass::Other,
];

const NUM_CLASSES: usize = ALL_CLASSES.len();

impl ArtifactClass {
    fn index(self) -> usize {
        self as usize
    }

    /// Short lowercase name for reports.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            ArtifactClass::Cluster => "cluster",
            ArtifactClass::Attrs => "attrs",
            ArtifactClass::Centroids => "centroids",
            ArtifactClass::Bootstrap => "bootstrap",
            ArtifactClass::Sq => "sq",
            ArtifactClass::Bitmap => "bitmap",
            ArtifactClass::Sketch => "sketch",
            ArtifactClass::Fts => "fts",
            ArtifactClass::Wal => "wal",
            ArtifactClass::Manifest => "manifest",
            ArtifactClass::Other => "other",
        }
    }
}

impl fmt::Display for ArtifactClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Map an S3 key to its artifact class, based on the real key builders:
///
/// - `src/index/ivf_flat/build.rs`: `cluster_{i}.bin`, `attrs_{i}.bin`,
///   `centroids.bin`, `bootstrap.bin`
/// - `src/index/quantization/sq.rs`: `sq_calibration.bin`, `sq_cluster_{i}.bin`
/// - `src/index/quantization/pq.rs`: `pq_codebook.bin`, `pq_cluster_{i}.bin`
/// - `src/index/bitmap/mod.rs`: `bitmap_{i}.bin`
/// - `src/index/ivf_flat/sketch.rs`: `coarse_sketch.bin`
/// - `src/fts/global_index.rs` + `src/fts/inverted_index.rs`:
///   `global_fts.bin`, `fts_index_{i}.bin`, `fts_meta.json`
/// - `src/wal/fragment.rs`: `{ns}/wal/{ulid}.wal`
/// - `src/wal/manifest.rs`: `{ns}/manifest.json`
#[must_use]
pub fn classify(key: &str) -> ArtifactClass {
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename.starts_with("cluster_") {
        ArtifactClass::Cluster
    } else if filename.starts_with("attrs_") {
        ArtifactClass::Attrs
    } else if filename == "centroids.bin" {
        ArtifactClass::Centroids
    } else if filename == "bootstrap.bin" {
        ArtifactClass::Bootstrap
    } else if filename.starts_with("sq_") || filename.starts_with("pq_") {
        ArtifactClass::Sq
    } else if filename.starts_with("bitmap_") {
        ArtifactClass::Bitmap
    } else if filename == "coarse_sketch.bin" {
        ArtifactClass::Sketch
    } else if filename == "global_fts.bin" || filename.starts_with("fts_") {
        ArtifactClass::Fts
    } else if filename.ends_with(".wal") {
        ArtifactClass::Wal
    } else if filename == "manifest.json" {
        ArtifactClass::Manifest
    } else {
        ArtifactClass::Other
    }
}

/// Ops + bytes recorded for a single artifact class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ClassStats {
    pub get_ops: u64,
    pub get_bytes: u64,
    pub put_ops: u64,
    pub put_bytes: u64,
}

/// Per-class atomic counters (indexed by `ArtifactClass as usize`).
#[derive(Debug)]
struct ClassCounters {
    get_ops: [AtomicU64; NUM_CLASSES],
    get_bytes: [AtomicU64; NUM_CLASSES],
    put_ops: [AtomicU64; NUM_CLASSES],
    put_bytes: [AtomicU64; NUM_CLASSES],
}

impl Default for ClassCounters {
    fn default() -> Self {
        Self {
            get_ops: std::array::from_fn(|_| AtomicU64::new(0)),
            get_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            put_ops: std::array::from_fn(|_| AtomicU64::new(0)),
            put_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl ClassCounters {
    fn record_get_attempt(&self, class: ArtifactClass) {
        self.get_ops[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_bytes(&self, class: ArtifactClass, bytes: u64) {
        self.get_bytes[class.index()].fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_put(&self, class: ArtifactClass, bytes: u64) {
        self.put_ops[class.index()].fetch_add(1, Ordering::Relaxed);
        self.put_bytes[class.index()].fetch_add(bytes, Ordering::Relaxed);
    }

    fn reset(&self) {
        for i in 0..NUM_CLASSES {
            self.get_ops[i].store(0, Ordering::Relaxed);
            self.get_bytes[i].store(0, Ordering::Relaxed);
            self.put_ops[i].store(0, Ordering::Relaxed);
            self.put_bytes[i].store(0, Ordering::Relaxed);
        }
    }
}

/// Shared handle for inspecting GET/PUT counts recorded by a [`CountingStore`].
#[derive(Clone, Debug, Default)]
pub struct GetCounter {
    gets: Arc<DashMap<String, u64>>,
    puts: Arc<DashMap<String, u64>>,
    heads: Arc<DashMap<String, u64>>,
    lists: Arc<DashMap<String, u64>>,
    delimiter_lists: Arc<DashMap<String, u64>>,
    classes: Arc<ClassCounters>,
}

impl GetCounter {
    /// Total number of GETs whose key contains `substr`.
    pub fn gets_matching(&self, substr: &str) -> u64 {
        self.gets
            .iter()
            .filter(|r| r.key().contains(substr))
            .map(|r| *r.value())
            .sum()
    }

    /// Total number of PUTs whose key contains `substr`.
    pub fn puts_matching(&self, substr: &str) -> u64 {
        self.puts
            .iter()
            .filter(|r| r.key().contains(substr))
            .map(|r| *r.value())
            .sum()
    }

    /// Total number of HEADs whose key contains `substr`.
    #[must_use]
    pub fn heads_matching(&self, substr: &str) -> u64 {
        self.heads
            .iter()
            .filter(|r| r.key().contains(substr))
            .map(|r| *r.value())
            .sum()
    }

    /// Total HEAD operations across all keys.
    #[must_use]
    pub fn total_heads(&self) -> u64 {
        self.heads.iter().map(|r| *r.value()).sum()
    }

    /// Total recursive list calls whose prefix exactly matches `prefix`.
    #[must_use]
    pub fn list_calls_for_prefix(&self, prefix: &str) -> u64 {
        self.lists.get(prefix).map(|value| *value).unwrap_or(0)
    }

    /// Total delimiter list calls whose prefix exactly matches `prefix`.
    #[must_use]
    pub fn delimiter_list_calls_for_prefix(&self, prefix: &str) -> u64 {
        self.delimiter_lists
            .get(prefix)
            .map(|value| *value)
            .unwrap_or(0)
    }

    /// Number of GETs attributed to `class`.
    #[must_use]
    pub fn gets_for(&self, class: ArtifactClass) -> u64 {
        self.classes.get_ops[class.index()].load(Ordering::Relaxed)
    }

    /// Bytes returned by GETs attributed to `class` (actual payload sizes).
    #[must_use]
    pub fn get_bytes_for(&self, class: ArtifactClass) -> u64 {
        self.classes.get_bytes[class.index()].load(Ordering::Relaxed)
    }

    /// Number of PUTs attributed to `class`.
    #[must_use]
    pub fn puts_for(&self, class: ArtifactClass) -> u64 {
        self.classes.put_ops[class.index()].load(Ordering::Relaxed)
    }

    /// Bytes written by PUTs attributed to `class` (actual body sizes).
    #[must_use]
    pub fn put_bytes_for(&self, class: ArtifactClass) -> u64 {
        self.classes.put_bytes[class.index()].load(Ordering::Relaxed)
    }

    /// Total GET operations across all classes.
    #[must_use]
    pub fn total_gets(&self) -> u64 {
        ALL_CLASSES.iter().map(|&c| self.gets_for(c)).sum()
    }

    /// Total GET bytes across all classes.
    #[must_use]
    pub fn total_get_bytes(&self) -> u64 {
        ALL_CLASSES.iter().map(|&c| self.get_bytes_for(c)).sum()
    }

    /// Snapshot of the per-class breakdown (serializable to JSON).
    #[must_use]
    pub fn class_breakdown(&self) -> BTreeMap<ArtifactClass, ClassStats> {
        ALL_CLASSES
            .iter()
            .map(|&class| {
                (
                    class,
                    ClassStats {
                        get_ops: self.gets_for(class),
                        get_bytes: self.get_bytes_for(class),
                        put_ops: self.puts_for(class),
                        put_bytes: self.put_bytes_for(class),
                    },
                )
            })
            .collect()
    }

    /// Human-readable per-class table of {ops, bytes} for GETs and PUTs.
    #[must_use]
    pub fn report(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "{:<10} {:>8} {:>12} {:>8} {:>12}\n",
            "class", "get_ops", "get_bytes", "put_ops", "put_bytes"
        ));
        let mut totals = ClassStats::default();
        for (class, stats) in self.class_breakdown() {
            out.push_str(&format!(
                "{:<10} {:>8} {:>12} {:>8} {:>12}\n",
                class.name(),
                stats.get_ops,
                stats.get_bytes,
                stats.put_ops,
                stats.put_bytes
            ));
            totals.get_ops += stats.get_ops;
            totals.get_bytes += stats.get_bytes;
            totals.put_ops += stats.put_ops;
            totals.put_bytes += stats.put_bytes;
        }
        out.push_str(&format!(
            "{:<10} {:>8} {:>12} {:>8} {:>12}\n",
            "TOTAL", totals.get_ops, totals.get_bytes, totals.put_ops, totals.put_bytes
        ));
        out
    }

    /// Reset all recorded counts.
    pub fn reset(&self) {
        self.gets.clear();
        self.puts.clear();
        self.heads.clear();
        self.lists.clear();
        self.delimiter_lists.clear();
        self.classes.reset();
    }
}

/// `ObjectStore` decorator that counts GET operations per key.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counter: GetCounter,
}

impl CountingStore {
    /// Wrap an existing store, returning the wrapper and its counter handle.
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> (Self, GetCounter) {
        let counter = GetCounter::default();
        (
            Self {
                inner,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

/// Wrap a harness store in a counting layer, returning a new `ZeppelinStore`
/// backed by the same objects plus the counter handle.
pub fn counting_store(store: &ZeppelinStore) -> (ZeppelinStore, GetCounter) {
    let (counting, counter) = CountingStore::wrap(store.inner());
    (ZeppelinStore::new(Arc::new(counting)), counter)
}

/// Wrap a dedicated performance-harness store while preserving native bulk DELETEs.
///
/// General tests intentionally use [`counting_store`], whose `ZeppelinStore`
/// keeps legacy prefix-cleanup scheduling. Perf scenarios use this constructor
/// so the gateway selects native batching and the forwarded
/// `ObjectStore::delete_stream` reaches the real S3 adapter as one request.
pub fn perf_counting_store(store: &ZeppelinStore) -> (ZeppelinStore, GetCounter) {
    let (counting, counter) = CountingStore::wrap(store.inner());
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(counting)),
        counter,
    )
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.counter
            .puts
            .entry(location.to_string())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.counter
            .classes
            .record_put(classify(location.as_ref()), payload.content_length() as u64);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    // All GET variants funnel through get_opts (the trait's `get`,
    // `get_range`, and `get_ranges` default impls delegate here).
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let class = classify(location.as_ref());
        self.counter
            .gets
            .entry(location.to_string())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        // Count attempts before the backend returns so conditional 304-style
        // freshness checks are visible as GET ops with zero returned bytes.
        self.counter.classes.record_get_attempt(class);
        let result = self.inner.get_opts(location, options).await?;
        // `GetResult::range` is the byte range actually returned (the whole
        // object for plain GETs, the requested slice for range GETs).
        let bytes = (result.range.end - result.range.start) as u64;
        self.counter.classes.record_get_bytes(class, bytes);
        Ok(result)
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.counter
            .heads
            .entry(location.to_string())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        self.counter
            .lists
            .entry(key)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        self.counter
            .delimiter_lists
            .entry(key)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
    };

    use super::{counting_store, perf_counting_store};
    use zeppelin::storage::ZeppelinStore;

    #[derive(Debug)]
    struct DeleteStreamProbe {
        inner: Arc<dyn ObjectStore>,
        native_stream_calls: AtomicUsize,
    }

    impl DeleteStreamProbe {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                native_stream_calls: AtomicUsize::new(0),
            }
        }
    }

    impl std::fmt::Display for DeleteStreamProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("DeleteStreamProbe")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DeleteStreamProbe {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> OsResult<()> {
            self.inner.delete(location).await
        }

        fn delete_stream<'a>(
            &'a self,
            locations: BoxStream<'a, OsResult<Path>>,
        ) -> BoxStream<'a, OsResult<Path>> {
            self.native_stream_calls.fetch_add(1, Ordering::SeqCst);
            locations
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn counting_delete_stream_forwards_once_and_preserves_input_order() {
        let probe = Arc::new(DeleteStreamProbe::new());
        let base = ZeppelinStore::new(probe.clone());
        let (counted, _counter) = counting_store(&base);
        let backend = counted.inner();
        let expected = (0..33)
            .map(|index| Path::from(format!("object-{index:04}")))
            .collect::<Vec<_>>();
        let input = futures::stream::iter(expected.iter().cloned().map(Ok)).boxed();

        let results = backend.delete_stream(input).collect::<Vec<_>>().await;

        assert_eq!(
            results
                .into_iter()
                .collect::<OsResult<Vec<_>>>()
                .expect("counting delete stream failed"),
            expected
        );
        assert_eq!(probe.native_stream_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dedicated_perf_delete_stream_forwards_one_native_batch() {
        let probe = Arc::new(DeleteStreamProbe::new());
        let base = ZeppelinStore::new(probe.clone());
        let (counted, _counter) = perf_counting_store(&base);
        let backend = counted.inner();
        let expected = (0..3)
            .map(|index| Path::from(format!("object-{index:04}")))
            .collect::<Vec<_>>();
        let input = futures::stream::iter(expected.iter().cloned().map(Ok)).boxed();

        let results = backend.delete_stream(input).collect::<Vec<_>>().await;

        assert_eq!(
            results
                .into_iter()
                .collect::<OsResult<Vec<_>>>()
                .expect("native delete stream failed"),
            expected
        );
        assert_eq!(probe.native_stream_calls.load(Ordering::SeqCst), 1);
    }
}
