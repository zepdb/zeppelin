//! Resolves HTTP point-in-time targets to retained immutable manifests.
//!
//! Query and namespace-clone handlers accept an `as_of` string naming an exact
//! manifest generation, an RFC3339 timestamp, or a named snapshot. This module
//! translates that user-facing selector into one retained
//! [`crate::wal::manifest::Manifest`] read directly from object storage. It does
//! not use the live manifest cache: PITR correctness depends on the
//! authoritative live-generation boundary and immutable history objects in
//! S3/MinIO.
//!
//! A named snapshot is a durable pin to one generation, not a second manifest.
//! Numeric and snapshot targets require that exact history object to remain.
//! Timestamp targets scan retained generations in order and choose the newest
//! generation whose `updated_at` is at or before the requested time. The scan
//! does not assume timestamps increase with generation, so writer clock skew
//! cannot make it stop too early.
//!
//! ## Reading map
//!
//! 1. Start with
//!    [`resolve_manifest`][crate::server::handlers::as_of::resolve_manifest]
//!    for selector parsing and dispatch.
//! 2. Read `read_retained_history_generation` for exact generation/snapshot
//!    enforcement.
//! 3. Read `resolve_history_at_or_before_timestamp` for the full clock-skew-safe
//!    history scan.
//! 4. Finish with `live_manifest_version` and `point_in_time_not_retained` for
//!    the authority boundary and caller-facing failure.
//!
//! ## Resolution flow
//!
//! ```text
//! as_of string
//!    |
//!    +-- snapshot:name --> read immutable pin --> exact history generation
//!    |
//!    +-- unsigned integer ---------------------> exact history generation
//!    |
//!    +-- RFC3339 time --> list sorted history --> newest updated_at <= time
//!                                      |
//!                                      | ignore generations ahead of live
//!                                      v
//!                         retained historical Manifest
//! ```
//!
//! ## Invariants
//!
//! - The current live manifest version caps every historical target. A stray
//!   history object ahead of live state is never exposed.
//! - Generation zero is unpublished and cannot be queried.
//! - Missing/pruned exact history returns `PointInTimeNotRetained`; it never
//!   falls back to a nearby generation.
//! - A key returned by history LIST but missing at GET is a storage invariant
//!   error, not normal retention.
//! - Resolving history is read-only and cannot make that generation live.
//!
//! ## Rust concepts used here
//!
//! `strip_prefix`, numeric parsing, and RFC3339 parsing form explicit branches
//! without nullable sentinel values. `Option::ok_or_else` converts “object not
//! found” into the domain-specific error only when needed, while `?` preserves
//! storage/decoding failures. Java would commonly chain exceptions and nullable
//! results; C would pair status codes with output pointers. Rust's `Result`
//! makes every early failure path part of the function's type.

use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};

use crate::error::ZeppelinError;
use crate::storage::ZeppelinStore;
use crate::wal::manifest::{Manifest, NamedSnapshot};

const TIMESTAMP_HISTORY_GET_CONCURRENCY: usize = 16;

/// Resolves a generation, timestamp, or named snapshot to retained history.
///
/// Parsing precedence is explicit: a `snapshot:` prefix is always a snapshot,
/// an unsigned decimal string is a generation, and every other value must be
/// RFC3339. The returned manifest is historical input for query/clone logic; it
/// does not replace the live namespace manifest.
///
/// # Parameters
///
/// - `store`: Object-store boundary used for snapshot, live-manifest, history
///   LIST, and history GET operations.
/// - `namespace`: Namespace owning the live and retained manifest objects.
/// - `as_of`: Caller selector: positive generation, RFC3339 timestamp, or
///   `snapshot:<name>`.
///
/// # Returns
///
/// The exact retained [`Manifest`] selected by the target rules.
///
/// # Errors
///
/// Returns validation for an empty/malformed selector, `SnapshotNotFound` for a
/// missing named pin, `PointInTimeNotRetained` when no eligible retained
/// generation exists, and propagates object-store/manifest decoding errors.
///
/// # Side Effects
///
/// Performs read-only object-store requests. It does not consult or populate
/// process caches and does not change manifest visibility.
///
/// # Consistency
///
/// Every branch reads the current live manifest version before accepting
/// history. A named pin still fails if its referenced history object is no
/// longer retained.
///
/// # Performance
///
/// Exact generation targets need a live-manifest GET plus one history GET;
/// snapshots add a pin GET. Timestamp targets list history and GET every
/// retained generation up to the live version.
///
/// # Examples
///
/// `"12"` reads generation 12 exactly. `"snapshot:before-import"` reads the
/// pin and then its exact generation. `"2026-01-01T00:00:00Z"` returns the
/// highest retained generation with an eligible timestamp even if a later
/// generation's clock is earlier than its predecessor.
pub async fn resolve_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    as_of: &str,
) -> Result<Manifest, ZeppelinError> {
    if as_of.is_empty() {
        return Err(ZeppelinError::Validation(
            "as_of must be a generation, RFC3339 timestamp, or snapshot:name".into(),
        ));
    }

    if let Some(snapshot_name) = as_of.strip_prefix("snapshot:") {
        if snapshot_name.is_empty() {
            return Err(ZeppelinError::Validation(
                "as_of snapshot target must be snapshot:<name>".into(),
            ));
        }
        let snapshot = NamedSnapshot::read(store, namespace, snapshot_name)
            .await?
            .ok_or_else(|| ZeppelinError::SnapshotNotFound {
                namespace: namespace.to_string(),
                name: snapshot_name.to_string(),
            })?;
        let target = format!("snapshot:{snapshot_name}");
        return read_retained_history_generation(store, namespace, snapshot.generation, &target)
            .await;
    }

    if let Ok(generation) = as_of.parse::<u64>() {
        return read_retained_history_generation(store, namespace, generation, as_of).await;
    }

    let timestamp = DateTime::parse_from_rfc3339(as_of)
        .map_err(|e| {
            ZeppelinError::Validation(format!(
                "as_of must be a generation, RFC3339 timestamp, or snapshot:name: {e}"
            ))
        })?
        .with_timezone(&Utc);
    resolve_history_at_or_before_timestamp(store, namespace, timestamp, as_of).await
}

/// Reads one exact retained generation after checking the live boundary.
///
/// # Parameters
///
/// - `store`: Object-store boundary for live and history GETs.
/// - `namespace`: Namespace owning both objects.
/// - `generation`: Exact positive generation requested by number or snapshot.
/// - `target`: Original caller-facing selector preserved in errors.
///
/// # Returns
///
/// The manifest stored at exactly `generation`.
///
/// # Errors
///
/// Returns `PointInTimeNotRetained` for generation zero, a generation ahead of
/// the live manifest, or a missing/pruned history object. Storage and decoding
/// errors propagate.
///
/// # Performance
///
/// Performs one live-manifest GET and, for an eligible generation, one history
/// GET.
///
/// # Examples
///
/// With live generation 20, target 12 succeeds only if history 12 exists.
/// Targets 0, 21, and a pruned generation 5 all produce the same retention
/// error category while retaining the original target text.
async fn read_retained_history_generation(
    store: &ZeppelinStore,
    namespace: &str,
    generation: u64,
    target: &str,
) -> Result<Manifest, ZeppelinError> {
    let live_version = live_manifest_version(store, namespace).await?;
    if generation == 0 || generation > live_version {
        return Err(point_in_time_not_retained(namespace, target));
    }

    Manifest::read_history(store, namespace, generation)
        .await?
        .ok_or_else(|| point_in_time_not_retained(namespace, target))
}

/// Selects the newest retained generation not later than a timestamp.
///
/// The history list is generation-sorted. This function scans all eligible
/// entries rather than assuming `updated_at` is monotonic, so clock skew can
/// change which timestamp qualifies without changing generation precedence.
///
/// # Parameters
///
/// - `store`: Object-store boundary for live GET, history LIST, and history GETs.
/// - `namespace`: Namespace whose retained timeline is searched.
/// - `timestamp`: UTC cutoff parsed from the caller's RFC3339 value.
/// - `target`: Original selector used in the retention error.
///
/// # Returns
///
/// The highest-generation retained manifest with `updated_at <= timestamp`.
///
/// # Errors
///
/// Returns `PointInTimeNotRetained` when no retained manifest qualifies.
/// Propagates live/list/decode errors. If LIST names a history key that vanishes
/// before GET, returns `NotFound` with that exact key.
///
/// # Side Effects
///
/// Performs read-only object-store requests.
///
/// # Consistency
///
/// Entries with a generation greater than the current live version are ignored
/// even if their timestamps qualify.
///
/// # Performance
///
/// Performs one live GET, one prefix LIST, and one history GET per listed
/// generation up to live. History GETs use bounded concurrency; total work is
/// linear in retained history count.
///
/// # Examples
///
/// If generations 8, 9, and 10 have times 10:00, 09:55, and 10:05, a 10:00
/// target chooses generation 9, the newest generation whose own time qualifies.
async fn resolve_history_at_or_before_timestamp(
    store: &ZeppelinStore,
    namespace: &str,
    timestamp: DateTime<Utc>,
    target: &str,
) -> Result<Manifest, ZeppelinError> {
    let live_version = live_manifest_version(store, namespace).await?;
    let history = Manifest::list_history(store, namespace).await?;
    let reads = stream::iter(
        history
            .into_iter()
            .take_while(|entry| entry.version <= live_version)
            .map(|entry| async move {
                Manifest::read_history(store, namespace, entry.version)
                    .await?
                    .ok_or_else(|| ZeppelinError::NotFound { key: entry.key })
            }),
    )
    .buffered(TIMESTAMP_HISTORY_GET_CONCURRENCY);
    tokio::pin!(reads);

    let mut selected = None;
    while let Some(manifest) = reads.next().await {
        let manifest = manifest?;
        if manifest.updated_at <= timestamp {
            selected = Some(manifest);
        }
    }

    selected.ok_or_else(|| point_in_time_not_retained(namespace, target))
}

/// Reads the authoritative live generation for a namespace.
///
/// # Parameters
///
/// - `store`: Object-store boundary for the live manifest GET.
/// - `namespace`: Namespace whose visibility boundary is required.
///
/// # Returns
///
/// The live manifest's generation. Published legacy manifests can report zero.
///
/// # Errors
///
/// Propagates object-store and manifest decoding failures.
///
/// # Consistency
///
/// Reads S3/MinIO directly rather than a TTL cache because this value fences
/// historical generations from stray objects ahead of live publication.
///
/// # Examples
///
/// A published legacy manifest returns zero, making every positive PITR target
/// unretained. A missing live object fails as an integrity error. A live
/// generation 12 returns 12.
async fn live_manifest_version(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<u64, ZeppelinError> {
    Ok(Manifest::read_required(store, namespace).await?.version())
}

/// Builds the stable domain error for an unavailable point-in-time target.
///
/// # Parameters
///
/// - `namespace`: Namespace searched for history.
/// - `target`: Original generation, timestamp, or snapshot selector.
///
/// # Returns
///
/// An owned [`ZeppelinError::PointInTimeNotRetained`] preserving both strings
/// for HTTP status/code mapping.
///
/// # Examples
///
/// A pruned `snapshot:before-import` target produces an error mentioning that
/// selector rather than only its resolved numeric generation.
fn point_in_time_not_retained(namespace: &str, target: &str) -> ZeppelinError {
    ZeppelinError::PointInTimeNotRetained {
        namespace: namespace.to_string(),
        target: target.to_string(),
    }
}
