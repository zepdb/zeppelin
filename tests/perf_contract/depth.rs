//! Object-store roundtrip depth instrumentation for performance contracts.
//!
//! [`DepthStore`] observes the real object store without changing its
//! behavior. The recorded sequence intervals let the runner recover the
//! longest chain of non-overlapping storage operations without asserting
//! wall-clock latency.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use serde::Serialize;
use zeppelin::storage::ZeppelinStore;

use crate::common::counting::{classify, ArtifactClass};

/// The logical object-store operation represented by an [`OpSpan`].
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub enum SpanKind {
    Get,
    Head,
    Put,
    List,
    Copy,
    Delete,
}

/// Semantic request shape presented to the `ObjectStore` adapter.
///
/// This deliberately omits ETag values and other high-cardinality request
/// data while preserving the distinctions that change S3 cost or behavior.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalRequest {
    GetFull,
    GetRange { start: usize, end: Option<usize> },
    GetSuffix { bytes: usize },
    GetConditional,
    GetConditionalRange { start: usize, end: Option<usize> },
    GetConditionalSuffix { bytes: usize },
    PutOverwrite,
    PutCreate,
    PutUpdate,
    Head,
    ListRecursive,
    ListDelimiter,
    CopyOverwrite,
    CopyIfAbsent,
    Delete,
    DeleteBatch,
}

/// Typed completion state for one observed object-store operation.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpanOutcome {
    Success,
    NotModified,
    Precondition,
    NotFound,
    Failure,
    Cancelled,
}

impl SpanOutcome {
    fn from_error(error: &object_store::Error) -> Self {
        match error {
            object_store::Error::NotModified { .. } => Self::NotModified,
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. } => Self::Precondition,
            object_store::Error::NotFound { .. } => Self::NotFound,
            _ => Self::Failure,
        }
    }

    fn from_result<T>(result: &OsResult<T>) -> Self {
        match result {
            Ok(_) => Self::Success,
            Err(error) => Self::from_error(error),
        }
    }
}

impl PhysicalRequest {
    fn from_get_options(options: &GetOptions) -> Self {
        if options.head {
            return Self::Head;
        }
        let conditional = options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some();
        match (&options.range, conditional) {
            (None, false) => Self::GetFull,
            (None, true) => Self::GetConditional,
            (Some(GetRange::Bounded(range)), false) => Self::GetRange {
                start: range.start,
                end: Some(range.end),
            },
            (Some(GetRange::Offset(start)), false) => Self::GetRange {
                start: *start,
                end: None,
            },
            (Some(GetRange::Suffix(bytes)), false) => Self::GetSuffix { bytes: *bytes },
            (Some(GetRange::Bounded(range)), true) => Self::GetConditionalRange {
                start: range.start,
                end: Some(range.end),
            },
            (Some(GetRange::Offset(start)), true) => Self::GetConditionalRange {
                start: *start,
                end: None,
            },
            (Some(GetRange::Suffix(bytes)), true) => Self::GetConditionalSuffix { bytes: *bytes },
        }
    }

    fn from_put_mode(mode: &PutMode) -> Self {
        match mode {
            PutMode::Overwrite => Self::PutOverwrite,
            PutMode::Create => Self::PutCreate,
            PutMode::Update(_) => Self::PutUpdate,
        }
    }
}

/// One input member consumed by a batch object-store invocation.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BatchMember {
    pub class: ArtifactClass,
    pub key: String,
}

/// One observed object-store operation.
#[derive(Debug, Clone, Serialize)]
pub struct OpSpan {
    pub kind: SpanKind,
    pub request: PhysicalRequest,
    pub class: ArtifactClass,
    pub key: String,
    pub start_seq: u64,
    pub end_seq: u64,
    pub bytes: u64,
    pub outcome: SpanOutcome,
    /// Compatibility projection retained for frozen reports and contracts.
    pub ok: bool,
    pub wall_start_us: u64,
    pub wall_end_us: u64,
    /// Input members consumed by a batch adapter invocation.
    ///
    /// Empty metadata is omitted so existing non-batch span JSON remains
    /// byte-for-byte compatible.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub batch_members: Vec<BatchMember>,
}

/// A maximal chain under the interval order used for roundtrip depth.
#[derive(Debug, Clone, Serialize)]
pub struct CriticalPath {
    pub depth: u32,
    pub chain: Vec<OpSpan>,
}

/// Per-class operation and byte totals assigned to one interval-DAG stage.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct StageClassTotals {
    pub ops: u64,
    pub bytes: u64,
}

/// All selected operations whose longest predecessor chain has this depth.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DepthStage {
    pub depth: u32,
    pub ops: u64,
    pub bytes: u64,
    pub classes: BTreeMap<String, StageClassTotals>,
}

#[derive(Debug)]
struct TrackerState {
    counter: AtomicU64,
    active: AtomicU64,
    spans: Mutex<Vec<OpSpan>>,
    epoch: Instant,
}

/// Shared handle for inspecting spans recorded by a [`DepthStore`].
///
/// # Soundness precondition
///
/// Critical-path depth is meaningful only while exactly one client request is
/// in flight and no background compaction, GC, hydration, or other storage
/// loop is running. The perf-contract runner is responsible for enforcing this
/// precondition; otherwise unrelated operations can introduce false chaining.
#[derive(Clone, Debug)]
pub struct DepthTracker {
    state: Arc<TrackerState>,
}

#[derive(Debug)]
struct PendingSpan {
    kind: SpanKind,
    request: PhysicalRequest,
    class: ArtifactClass,
    key: String,
    start_seq: u64,
    bytes: u64,
    wall_start_us: u64,
}

struct GetSpanStream {
    inner: BoxStream<'static, OsResult<Bytes>>,
    tracker: DepthTracker,
    pending: Option<PendingSpan>,
    bytes: u64,
}

impl GetSpanStream {
    fn new(
        inner: BoxStream<'static, OsResult<Bytes>>,
        tracker: DepthTracker,
        pending: PendingSpan,
    ) -> Self {
        Self {
            inner,
            tracker,
            pending: Some(pending),
            bytes: 0,
        }
    }

    fn close(&mut self, outcome: SpanOutcome) {
        if let Some(pending) = self.pending.take() {
            self.tracker.finish(pending, outcome, Some(self.bytes));
        }
    }
}

impl Stream for GetSpanStream {
    type Item = OsResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.bytes = self
                    .bytes
                    .checked_add(bytes.len() as u64)
                    .expect("depth tracker GET byte count overflowed");
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.close(SpanOutcome::from_error(&error));
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.close(SpanOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Drop for GetSpanStream {
    fn drop(&mut self) {
        self.close(SpanOutcome::Cancelled);
    }
}

struct ListSpanStream<'a> {
    inner: BoxStream<'a, OsResult<ObjectMeta>>,
    tracker: DepthTracker,
    pending: Option<PendingSpan>,
}

impl<'a> ListSpanStream<'a> {
    fn new(
        inner: BoxStream<'a, OsResult<ObjectMeta>>,
        tracker: DepthTracker,
        pending: PendingSpan,
    ) -> Self {
        Self {
            inner,
            tracker,
            pending: Some(pending),
        }
    }

    fn close(&mut self, outcome: SpanOutcome) {
        if let Some(pending) = self.pending.take() {
            self.tracker.finish(pending, outcome, None);
        }
    }
}

impl Stream for ListSpanStream<'_> {
    type Item = OsResult<ObjectMeta>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(meta))) => Poll::Ready(Some(Ok(meta))),
            Poll::Ready(Some(Err(error))) => {
                self.close(SpanOutcome::from_error(&error));
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.close(SpanOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Drop for ListSpanStream<'_> {
    fn drop(&mut self) {
        self.close(SpanOutcome::Cancelled);
    }
}

struct DeleteBatchSpanStream<'a> {
    inner: BoxStream<'a, OsResult<Path>>,
    tracker: DepthTracker,
    pending: Option<PendingSpan>,
    failure: Option<SpanOutcome>,
    members: Arc<Mutex<Vec<BatchMember>>>,
}

impl<'a> DeleteBatchSpanStream<'a> {
    fn new(
        inner: BoxStream<'a, OsResult<Path>>,
        tracker: DepthTracker,
        pending: PendingSpan,
        members: Arc<Mutex<Vec<BatchMember>>>,
    ) -> Self {
        Self {
            inner,
            tracker,
            pending: Some(pending),
            failure: None,
            members,
        }
    }

    fn close(&mut self, default: SpanOutcome) {
        if let Some(pending) = self.pending.take() {
            let members = self
                .members
                .lock()
                .expect("depth tracker batch-members mutex poisoned")
                .clone();
            self.tracker
                .finish_batch(pending, self.failure.unwrap_or(default), members);
        }
    }
}

impl Stream for DeleteBatchSpanStream<'_> {
    type Item = OsResult<Path>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(path))) => Poll::Ready(Some(Ok(path))),
            Poll::Ready(Some(Err(error))) => {
                if self.failure.is_none() {
                    self.failure = Some(SpanOutcome::from_error(&error));
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.close(SpanOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Drop for DeleteBatchSpanStream<'_> {
    fn drop(&mut self) {
        self.close(SpanOutcome::Cancelled);
    }
}

impl DepthTracker {
    fn new() -> Self {
        Self {
            state: Arc::new(TrackerState {
                counter: AtomicU64::new(0),
                active: AtomicU64::new(0),
                spans: Mutex::new(Vec::new()),
                epoch: Instant::now(),
            }),
        }
    }

    /// Return the current timestamp in the same epoch as span wall fields.
    #[must_use]
    pub fn elapsed_us(&self) -> u64 {
        u64::try_from(self.state.epoch.elapsed().as_micros())
            .expect("depth tracker epoch exceeded u64 microseconds")
    }

    /// Return the number of object-store operations that have started but not
    /// yet recorded their completion span.
    #[must_use]
    pub fn active_operations(&self) -> u64 {
        self.state.active.load(Ordering::SeqCst)
    }

    fn begin(
        &self,
        kind: SpanKind,
        request: PhysicalRequest,
        class: ArtifactClass,
        key: String,
        bytes: u64,
    ) -> PendingSpan {
        let start_seq = self.state.counter.load(Ordering::SeqCst);
        self.state
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                active.checked_add(1)
            })
            .expect("depth tracker active operation count overflowed");
        PendingSpan {
            kind,
            request,
            class,
            key,
            start_seq,
            bytes,
            wall_start_us: self.elapsed_us(),
        }
    }

    fn finish(&self, pending: PendingSpan, outcome: SpanOutcome, bytes: Option<u64>) {
        self.finish_inner(pending, outcome, bytes, Vec::new());
    }

    fn finish_batch(
        &self,
        mut pending: PendingSpan,
        outcome: SpanOutcome,
        members: Vec<BatchMember>,
    ) {
        pending.class = members
            .first()
            .map(|first| first.class)
            .filter(|class| members.iter().all(|member| member.class == *class))
            .unwrap_or(ArtifactClass::Other);
        pending.key = format!("<delete-batch:{}>", members.len());
        self.finish_inner(pending, outcome, None, members);
    }

    fn finish_inner(
        &self,
        pending: PendingSpan,
        outcome: SpanOutcome,
        bytes: Option<u64>,
        batch_members: Vec<BatchMember>,
    ) {
        let end_seq = self
            .state
            .counter
            .fetch_add(1, Ordering::SeqCst)
            .checked_add(1)
            .expect("depth tracker event counter overflowed");
        let span = OpSpan {
            kind: pending.kind,
            request: pending.request,
            class: pending.class,
            key: pending.key,
            start_seq: pending.start_seq,
            end_seq,
            bytes: bytes.unwrap_or(pending.bytes),
            outcome,
            ok: outcome == SpanOutcome::Success,
            wall_start_us: pending.wall_start_us,
            wall_end_us: self.elapsed_us(),
            batch_members,
        };
        self.state
            .spans
            .lock()
            .expect("depth tracker spans mutex poisoned")
            .push(span);
        self.state
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                active.checked_sub(1)
            })
            .expect("depth tracker active operation count underflowed");
    }

    /// Clear all recorded spans. The global event counter intentionally keeps
    /// ticking so a reset cannot reuse sequence numbers.
    pub fn reset(&self) {
        self.state
            .spans
            .lock()
            .expect("depth tracker spans mutex poisoned")
            .clear();
    }

    /// Drain and return all currently recorded spans.
    #[must_use]
    pub fn take_spans(&self) -> Vec<OpSpan> {
        std::mem::take(
            &mut *self
                .state
                .spans
                .lock()
                .expect("depth tracker spans mutex poisoned"),
        )
    }

    /// Compute the longest interval-order chain among the selected kinds.
    ///
    /// An operation `A` may precede `B` iff `A.end_seq <= B.start_seq`.
    /// Spans starting after `cutoff_us` are excluded when a cutoff is present.
    /// The caller must uphold the [`DepthTracker`] soundness precondition.
    ///
    /// Spans are sorted by completion sequence. A prefix maximum stores the
    /// deepest eligible predecessor for each completion prefix, yielding
    /// `O(n log n)` time and `O(n)` space, including chain reconstruction.
    #[must_use]
    pub fn critical_path(
        spans: &[OpSpan],
        kinds: &[SpanKind],
        cutoff_us: Option<u64>,
    ) -> CriticalPath {
        let mut ordered: Vec<&OpSpan> = spans
            .iter()
            .filter(|span| kinds.contains(&span.kind))
            .filter(|span| cutoff_us.is_none_or(|cutoff| span.wall_start_us <= cutoff))
            .collect();
        ordered.sort_by_key(|span| span.end_seq);

        let mut ends = Vec::with_capacity(ordered.len());
        let mut depths: Vec<u32> = Vec::with_capacity(ordered.len());
        let mut predecessors = Vec::with_capacity(ordered.len());
        let mut prefix_best = Vec::with_capacity(ordered.len());

        for (index, span) in ordered.iter().enumerate() {
            let eligible = ends.partition_point(|end| *end <= span.start_seq);
            let predecessor = eligible
                .checked_sub(1)
                .map(|prefix_index| prefix_best[prefix_index]);
            let predecessor_depth: u32 = predecessor.map_or(0, |candidate| depths[candidate]);
            let depth = predecessor_depth
                .checked_add(1)
                .expect("critical-path depth overflowed u32");

            ends.push(span.end_seq);
            depths.push(depth);
            predecessors.push(predecessor);

            let best = prefix_best.last().copied().map_or(index, |previous| {
                if depth > depths[previous] {
                    index
                } else {
                    previous
                }
            });
            prefix_best.push(best);
        }

        let Some(&maximal) = prefix_best.last() else {
            return CriticalPath {
                depth: 0,
                chain: Vec::new(),
            };
        };

        let mut chain = Vec::with_capacity(depths[maximal] as usize);
        let mut cursor = Some(maximal);
        while let Some(index) = cursor {
            chain.push(ordered[index].clone());
            cursor = predecessors[index];
        }
        chain.reverse();

        CriticalPath {
            depth: depths[maximal],
            chain,
        }
    }

    /// Group selected spans by their interval-DAG depth.
    ///
    /// A span's stage is one plus the deepest operation that completed before
    /// it started. Parallel operations therefore share a stage. Tier 2 uses
    /// the resulting per-stage op and byte totals as the direct input to its
    /// transfer-latency equation.
    #[must_use]
    pub fn stages(spans: &[OpSpan], kinds: &[SpanKind], cutoff_us: Option<u64>) -> Vec<DepthStage> {
        let mut ordered = spans
            .iter()
            .filter(|span| kinds.contains(&span.kind))
            .filter(|span| cutoff_us.is_none_or(|cutoff| span.wall_start_us <= cutoff))
            .collect::<Vec<_>>();
        ordered.sort_by_key(|span| span.end_seq);

        let mut ends = Vec::with_capacity(ordered.len());
        let mut prefix_max = Vec::<u32>::with_capacity(ordered.len());
        let mut stages = BTreeMap::<u32, DepthStage>::new();
        for span in ordered {
            let eligible = ends.partition_point(|end| *end <= span.start_seq);
            let predecessor_depth = eligible.checked_sub(1).map_or(0, |index| prefix_max[index]);
            let depth = predecessor_depth
                .checked_add(1)
                .expect("depth stage overflowed u32");
            ends.push(span.end_seq);
            prefix_max.push(prefix_max.last().copied().unwrap_or(0).max(depth));

            let stage = stages.entry(depth).or_insert_with(|| DepthStage {
                depth,
                ops: 0,
                bytes: 0,
                classes: BTreeMap::new(),
            });
            stage.ops = stage.ops.checked_add(1).expect("stage op count overflowed");
            stage.bytes = stage
                .bytes
                .checked_add(span.bytes)
                .expect("stage byte count overflowed");
            let class = stage
                .classes
                .entry(span.class.name().to_string())
                .or_default();
            class.ops = class
                .ops
                .checked_add(1)
                .expect("stage class ops overflowed");
            class.bytes = class
                .bytes
                .checked_add(span.bytes)
                .expect("stage class bytes overflowed");
        }
        stages.into_values().collect()
    }
}

/// Object-store decorator that records operation intervals without replacing
/// the real storage backend.
#[derive(Debug)]
pub struct DepthStore {
    inner: Arc<dyn ObjectStore>,
    tracker: DepthTracker,
}

/// Wrap a Zeppelin store in depth instrumentation.
#[must_use]
pub fn depth_store(store: &ZeppelinStore) -> (ZeppelinStore, DepthTracker) {
    let tracker = DepthTracker::new();
    let depth = DepthStore {
        inner: store.inner(),
        tracker: tracker.clone(),
    };
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(depth)),
        tracker,
    )
}

impl fmt::Display for DepthStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DepthStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for DepthStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let pending = self.tracker.begin(
            SpanKind::Put,
            PhysicalRequest::from_put_mode(&opts.mode),
            classify(location.as_ref()),
            location.to_string(),
            payload.content_length() as u64,
        );
        let result = self.inner.put_opts(location, payload, opts).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        // Phase 1 has no sound span model for the multipart handle's later
        // part uploads and completion, so match CountingStore and delegate it.
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let pending = self.tracker.begin(
            SpanKind::Get,
            PhysicalRequest::from_get_options(&options),
            classify(location.as_ref()),
            location.to_string(),
            0,
        );
        let result = match self.inner.get_opts(location, options).await {
            Ok(result) => result,
            Err(error) => {
                self.tracker
                    .finish(pending, SpanOutcome::from_error(&error), Some(0));
                return Err(error);
            }
        };
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let inner = result.into_stream();
        let payload = GetResultPayload::Stream(Box::pin(GetSpanStream::new(
            inner,
            self.tracker.clone(),
            pending,
        )));

        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
        })
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let pending = self.tracker.begin(
            SpanKind::Head,
            PhysicalRequest::Head,
            classify(location.as_ref()),
            location.to_string(),
            0,
        );
        let result = self.inner.head(location).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let pending = self.tracker.begin(
            SpanKind::Delete,
            PhysicalRequest::Delete,
            classify(location.as_ref()),
            location.to_string(),
            0,
        );
        let result = self.inner.delete(location).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        let members = Arc::new(Mutex::new(Vec::new()));
        let observed_members = Arc::clone(&members);
        let locations = locations
            .map(move |result| {
                if let Ok(path) = &result {
                    observed_members
                        .lock()
                        .expect("depth tracker batch-members mutex poisoned")
                        .push(BatchMember {
                            class: classify(path.as_ref()),
                            key: path.to_string(),
                        });
                }
                result
            })
            .boxed();
        let pending = self.tracker.begin(
            SpanKind::Delete,
            PhysicalRequest::DeleteBatch,
            ArtifactClass::Other,
            "<delete-batch>".to_string(),
            0,
        );
        Box::pin(DeleteBatchSpanStream::new(
            self.inner.delete_stream(locations),
            self.tracker.clone(),
            pending,
            members,
        ))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        let pending = self.tracker.begin(
            SpanKind::List,
            PhysicalRequest::ListRecursive,
            classify(&key),
            key,
            0,
        );
        Box::pin(ListSpanStream::new(
            self.inner.list(prefix),
            self.tracker.clone(),
            pending,
        ))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        let pending = self.tracker.begin(
            SpanKind::List,
            PhysicalRequest::ListDelimiter,
            classify(&key),
            key,
            0,
        );
        let result = self.inner.list_with_delimiter(prefix).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let pending = self.tracker.begin(
            SpanKind::Copy,
            PhysicalRequest::CopyOverwrite,
            classify(to.as_ref()),
            format!("{from}->{to}"),
            0,
        );
        let result = self.inner.copy(from, to).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let pending = self.tracker.begin(
            SpanKind::Copy,
            PhysicalRequest::CopyIfAbsent,
            classify(to.as_ref()),
            format!("{from}->{to}"),
            0,
        );
        let result = self.inner.copy_if_not_exists(from, to).await;
        self.tracker
            .finish(pending, SpanOutcome::from_result(&result), None);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use object_store::{GetRange, PutMode, UpdateVersion};

    async fn instrumented_get(payload: &'static [u8]) -> (GetResult, DepthTracker) {
        let location = Path::from("segments/test/cluster_0.bin");
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner
            .put(&location, PutPayload::from_static(payload))
            .await
            .expect("seed in-memory GET payload");
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let result = store
            .inner()
            .get(&location)
            .await
            .expect("get in-memory payload headers");
        (result, tracker)
    }

    fn span(key: &str, start_seq: u64, end_seq: u64) -> OpSpan {
        OpSpan {
            kind: SpanKind::Get,
            request: PhysicalRequest::GetFull,
            class: classify(key),
            key: key.to_string(),
            start_seq,
            end_seq,
            bytes: 0,
            outcome: SpanOutcome::Success,
            ok: true,
            wall_start_us: start_seq,
            wall_end_us: end_seq,
            batch_members: Vec::new(),
        }
    }

    #[test]
    fn pure_parallel_batch_has_depth_one() {
        let spans = vec![
            span("cluster_0.bin", 0, 1),
            span("cluster_1.bin", 0, 2),
            span("cluster_2.bin", 0, 3),
        ];

        let path = DepthTracker::critical_path(&spans, &[SpanKind::Get], None);

        assert_eq!(path.depth, 1);
        assert_eq!(path.chain.len(), 1);
    }

    #[test]
    fn non_batch_span_json_omits_empty_batch_metadata() {
        let json = serde_json::to_value(span("manifest.json", 0, 1))
            .expect("serialize non-batch depth span");

        assert!(json.get("batch_members").is_none());
    }

    #[test]
    fn two_parallel_stages_have_depth_two() {
        let spans = vec![
            span("coarse_0.bin", 0, 1),
            span("coarse_1.bin", 0, 2),
            span("cluster_0.bin", 2, 3),
            span("cluster_1.bin", 2, 4),
        ];

        let path = DepthTracker::critical_path(&spans, &[SpanKind::Get], None);

        assert_eq!(path.depth, 2);
        assert_eq!(path.chain.len(), 2);
        assert!(path.chain[0].end_seq <= path.chain[1].start_seq);
    }

    #[test]
    fn overlapping_intervals_do_not_chain() {
        let spans = vec![span("cluster_0.bin", 0, 2), span("cluster_1.bin", 1, 3)];

        let path = DepthTracker::critical_path(&spans, &[SpanKind::Get], None);

        assert_eq!(path.depth, 1);
        assert_eq!(path.chain.len(), 1);
    }

    #[tokio::test]
    async fn successful_get_finishes_only_after_payload_eof() {
        let (result, tracker) = instrumented_get(b"payload").await;
        assert!(tracker.take_spans().is_empty());

        let bytes = result.bytes().await.expect("consume GET payload");
        assert_eq!(bytes, Bytes::from_static(b"payload"));

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].kind, SpanKind::Get);
        assert_eq!(spans[0].bytes, 7);
        assert_eq!(spans[0].outcome, SpanOutcome::Success);
        assert!(spans[0].ok);
    }

    #[tokio::test]
    async fn dropping_get_payload_finishes_once_as_cancelled() {
        let (result, tracker) = instrumented_get(b"payload").await;
        assert!(tracker.take_spans().is_empty());

        drop(result);

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].bytes, 0);
        assert_eq!(spans[0].outcome, SpanOutcome::Cancelled);
        assert!(!spans[0].ok);
    }

    #[tokio::test]
    async fn get_payload_error_finishes_once_as_failed() {
        let tracker = DepthTracker::new();
        let pending = tracker.begin(
            SpanKind::Get,
            PhysicalRequest::GetFull,
            ArtifactClass::Cluster,
            "segments/test/cluster_0.bin".to_string(),
            0,
        );
        let error = object_store::Error::Generic {
            store: "depth_test",
            source: Box::new(std::io::Error::other("injected stream error")),
        };
        let inner = futures::stream::once(async move { Err(error) }).boxed();
        let mut stream = GetSpanStream::new(inner, tracker.clone(), pending);

        assert!(stream.next().await.expect("error stream item").is_err());
        drop(stream);

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].bytes, 0);
        assert_eq!(spans[0].outcome, SpanOutcome::Failure);
        assert!(!spans[0].ok);
    }

    #[tokio::test]
    async fn active_get_operations_include_payload_lifetime() {
        let (result, tracker) = instrumented_get(b"payload").await;
        assert_eq!(tracker.active_operations(), 1);

        result.bytes().await.expect("consume GET payload");
        assert_eq!(tracker.active_operations(), 0);

        let (result, tracker) = instrumented_get(b"payload").await;
        assert_eq!(tracker.active_operations(), 1);

        drop(result);
        assert_eq!(tracker.active_operations(), 0);
    }

    #[tokio::test]
    async fn conditional_not_modified_is_a_typed_expected_outcome() {
        let location = Path::from("manifest.json");
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let put = inner
            .put(&location, PutPayload::from_static(b"manifest"))
            .await
            .expect("seed conditional GET object");
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let options = GetOptions {
            if_none_match: put.e_tag,
            ..GetOptions::default()
        };

        let result = store.inner().get_opts(&location, options).await;

        assert!(matches!(
            result,
            Err(object_store::Error::NotModified { .. })
        ));
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].outcome, SpanOutcome::NotModified);
        assert!(!spans[0].ok);
    }

    #[tokio::test]
    async fn recursive_list_stays_active_until_stream_eof() {
        let location = Path::from("ns/meta.json");
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner
            .put(&location, PutPayload::from_static(b"metadata"))
            .await
            .expect("seed recursive LIST object");
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let prefix = Path::from("ns");
        let backend = store.inner();
        let mut stream = backend.list(Some(&prefix));

        assert_eq!(tracker.active_operations(), 1);
        assert!(tracker.take_spans().is_empty());
        assert!(stream.next().await.expect("recursive LIST item").is_ok());
        assert!(stream.next().await.is_none());

        assert_eq!(tracker.active_operations(), 0);
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].outcome, SpanOutcome::Success);
        assert!(spans[0].ok);
    }

    #[tokio::test]
    async fn dropping_recursive_list_records_cancelled_once() {
        let location = Path::from("ns/meta.json");
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        inner
            .put(&location, PutPayload::from_static(b"metadata"))
            .await
            .expect("seed cancelled recursive LIST object");
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let prefix = Path::from("ns");
        let backend = store.inner();
        let stream = backend.list(Some(&prefix));

        assert_eq!(tracker.active_operations(), 1);
        drop(stream);

        assert_eq!(tracker.active_operations(), 0);
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].outcome, SpanOutcome::Cancelled);
        assert!(!spans[0].ok);
    }

    #[tokio::test]
    async fn recursive_list_stream_error_records_failure_once() {
        let tracker = DepthTracker::new();
        let pending = tracker.begin(
            SpanKind::List,
            PhysicalRequest::ListRecursive,
            ArtifactClass::Other,
            "ns".to_string(),
            0,
        );
        let error = object_store::Error::Generic {
            store: "depth_test",
            source: Box::new(std::io::Error::other("injected LIST stream error")),
        };
        let inner = futures::stream::once(async move { Err(error) }).boxed();
        let mut stream = ListSpanStream::new(inner, tracker.clone(), pending);

        assert!(stream
            .next()
            .await
            .expect("LIST error stream item")
            .is_err());
        drop(stream);

        assert_eq!(tracker.active_operations(), 0);
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].outcome, SpanOutcome::Failure);
        assert!(!spans[0].ok);
    }

    #[tokio::test]
    async fn delete_stream_records_one_batch_span_with_mixed_members() {
        let locations = [
            Path::from("ns/segments/seg/attrs_7.bin"),
            Path::from("ns/wal/01ARZ3NDEKTSV4RRFFQ69G5FAV.wal"),
            Path::from("ns/manifests/123.msgpack"),
            Path::from("ns/delete-fixture/0001.bin"),
            Path::from("ns/delete-fixture/0002.bin"),
        ];
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for location in &locations {
            inner
                .put(location, PutPayload::from_static(b"delete me"))
                .await
                .expect("seed batch-delete object");
        }
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let backend = store.inner();
        let input = futures::stream::iter(locations.iter().cloned().map(Ok)).boxed();

        let deleted = backend
            .delete_stream(input)
            .try_collect::<Vec<_>>()
            .await
            .expect("batch delete should succeed");

        assert_eq!(deleted, locations);
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].kind, SpanKind::Delete);
        assert_eq!(spans[0].request, PhysicalRequest::DeleteBatch);
        assert_eq!(spans[0].class, ArtifactClass::Other);
        assert_eq!(spans[0].key, "<delete-batch:5>");
        assert_eq!(spans[0].outcome, SpanOutcome::Success);
        assert_eq!(spans[0].batch_members.len(), locations.len());
        assert_eq!(
            spans[0]
                .batch_members
                .iter()
                .map(|member| (member.class, member.key.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (ArtifactClass::Attrs, locations[0].as_ref()),
                (ArtifactClass::Wal, locations[1].as_ref()),
                (ArtifactClass::Other, locations[2].as_ref()),
                (ArtifactClass::Other, locations[3].as_ref()),
                (ArtifactClass::Other, locations[4].as_ref()),
            ]
        );
    }

    #[tokio::test]
    async fn delete_stream_error_records_failure_once() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let backend = store.inner();
        let error = object_store::Error::Generic {
            store: "depth_test",
            source: Box::new(std::io::Error::other("injected batch error")),
        };
        let attempted = Path::from("ns/wal/attempted.wal");
        let input = futures::stream::iter(vec![Ok(attempted.clone()), Err(error)]).boxed();
        let deleted = backend.delete_stream(input).collect::<Vec<_>>().await;

        assert_eq!(deleted.len(), 2);
        assert!(deleted[0].is_ok());
        assert!(deleted[1].is_err());

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].request, PhysicalRequest::DeleteBatch);
        assert_eq!(spans[0].outcome, SpanOutcome::Failure);
        assert_eq!(spans[0].class, ArtifactClass::Wal);
        assert_eq!(spans[0].key, "<delete-batch:1>");
        assert_eq!(spans[0].batch_members.len(), 1);
        assert_eq!(spans[0].batch_members[0].class, ArtifactClass::Wal);
        assert_eq!(spans[0].batch_members[0].key, attempted.as_ref());
    }

    #[tokio::test]
    async fn empty_delete_stream_records_other_zero_member_batch() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let deleted = store
            .inner()
            .delete_stream(futures::stream::empty().boxed())
            .collect::<Vec<_>>()
            .await;

        assert!(deleted.is_empty());
        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].request, PhysicalRequest::DeleteBatch);
        assert_eq!(spans[0].class, ArtifactClass::Other);
        assert_eq!(spans[0].key, "<delete-batch:0>");
        assert_eq!(spans[0].outcome, SpanOutcome::Success);
        assert!(spans[0].batch_members.is_empty());
    }

    #[tokio::test]
    async fn dropping_delete_stream_records_cancelled_once() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let backend = store.inner();
        let attempted = Path::from("ns/manifests/456.msgpack");
        let input = futures::stream::once({
            let attempted = attempted.clone();
            async move { Ok(attempted) }
        })
        .chain(futures::stream::pending::<OsResult<Path>>())
        .boxed();
        let mut deleted = backend.delete_stream(input);

        assert_eq!(tracker.active_operations(), 1);
        assert_eq!(
            deleted
                .next()
                .await
                .expect("first batch-delete result")
                .expect("first batch-delete member succeeds"),
            attempted
        );
        drop(deleted);

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].request, PhysicalRequest::DeleteBatch);
        assert_eq!(spans[0].outcome, SpanOutcome::Cancelled);
        assert_eq!(spans[0].class, ArtifactClass::Other);
        assert_eq!(spans[0].key, "<delete-batch:1>");
        assert_eq!(spans[0].batch_members.len(), 1);
        assert_eq!(spans[0].batch_members[0].class, ArtifactClass::Other);
        assert_eq!(spans[0].batch_members[0].key, attempted.as_ref());
    }

    #[tokio::test]
    async fn missing_get_and_create_conflict_have_typed_outcomes() {
        let location = Path::from("meta.json");
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ZeppelinStore::new(inner);
        let (store, tracker) = depth_store(&store);
        let backend = store.inner();

        let missing = backend.get(&location).await;
        assert!(matches!(missing, Err(object_store::Error::NotFound { .. })));

        backend
            .put(&location, PutPayload::from_static(b"first"))
            .await
            .expect("seed create-conflict object");
        let conflict = backend
            .put_opts(
                &location,
                PutPayload::from_static(b"second"),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        assert!(matches!(
            conflict,
            Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. })
        ));

        let spans = tracker.take_spans();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].outcome, SpanOutcome::NotFound);
        assert_eq!(spans[1].outcome, SpanOutcome::Success);
        assert_eq!(spans[2].outcome, SpanOutcome::Precondition);
    }

    #[test]
    fn get_request_shape_distinguishes_full_range_and_conditional_reads() {
        let full = GetOptions::default();
        assert_eq!(
            PhysicalRequest::from_get_options(&full),
            PhysicalRequest::GetFull
        );

        let ranged = GetOptions {
            range: Some(GetRange::Bounded(7..19)),
            ..GetOptions::default()
        };
        assert_eq!(
            PhysicalRequest::from_get_options(&ranged),
            PhysicalRequest::GetRange {
                start: 7,
                end: Some(19),
            }
        );

        let conditional = GetOptions {
            if_none_match: Some("etag".to_string()),
            ..GetOptions::default()
        };
        assert_eq!(
            PhysicalRequest::from_get_options(&conditional),
            PhysicalRequest::GetConditional
        );
    }

    #[test]
    fn put_request_shape_distinguishes_overwrite_create_and_update() {
        assert_eq!(
            PhysicalRequest::from_put_mode(&PutMode::Overwrite),
            PhysicalRequest::PutOverwrite
        );
        assert_eq!(
            PhysicalRequest::from_put_mode(&PutMode::Create),
            PhysicalRequest::PutCreate
        );
        assert_eq!(
            PhysicalRequest::from_put_mode(&PutMode::Update(UpdateVersion {
                e_tag: Some("etag".to_string()),
                version: None,
            })),
            PhysicalRequest::PutUpdate
        );
    }
}
