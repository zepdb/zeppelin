//! HTTP handlers for process-local runtime query defaults.
//!
//! These endpoints expose [`crate::runtime_config::QueryKnobs`] at
//! `/v1/config/query`. `GET` returns an immutable snapshot; `PATCH` and its
//! `PUT` alias apply the same partial
//! [`crate::runtime_config::QueryKnobsPatch`] semantics. Omitted fields retain
//! their current values.
//! The runtime holder validates boot-time bounds and publishes every accepted
//! field together so concurrent queries see either the old or the new snapshot,
//! never a partially applied update.
//!
//! This configuration is process-local and ephemeral. It does not write S3,
//! mutate a namespace manifest, or coordinate other Zeppelin nodes. Restarting
//! the process seeds a new snapshot from boot-time configuration.
//!
//! ## Request flow
//!
//! ```text
//! GET /v1/config/query
//!         |
//!         v
//! clone current Arc snapshot --> owned JSON response
//!
//! PATCH or PUT + partial JSON
//!         |
//!         v
//! Axum decode/deny unknown fields
//!         |
//!         v
//! validate startup bounds --> atomically replace snapshot --> JSON response
//!              |
//!              +-- validation failure --> ApiError; old snapshot remains
//! ```
//!
//! ## Rust concepts used here
//!
//! Axum extractors move an owned [`crate::server::AppState`] handle and decoded
//! patch into the async handler. The state internally shares services through
//! [`std::sync::Arc`].
//! `snapshot()` clones the `Arc`, then this thin HTTP layer clones the small
//! [`crate::runtime_config::QueryKnobs`] value into an owned JSON body. Java
//! would pass shared object references; C would need explicit reference
//! counting and response ownership.

use axum::extract::{Extension, State};
use axum::Json;

use crate::runtime_config::{QueryKnobs, QueryKnobsPatch};
use crate::security::AllowDecision;
use crate::server::AppState;

use super::ApiError;

/// Returns the current immutable process-local query defaults.
///
/// # Parameters
///
/// - `state`: Axum application state containing the shared runtime holder.
///
/// # Returns
///
/// A JSON body owning a clone of the snapshot current during this call.
///
/// # Panics
///
/// Panics if the runtime configuration lock is poisoned. Zeppelin fails loudly
/// rather than serving configuration from an untrustworthy lock state.
///
/// # Side Effects
///
/// None beyond normal HTTP response construction. No object-store, manifest,
/// or namespace state is read or written.
///
/// # Performance
///
/// Acquires the runtime holder's read lock briefly, increments an `Arc` count,
/// then clones the small `QueryKnobs` struct for serialization.
///
/// # Examples
///
/// If the current defaults are `top_k = 10` and the nprobe floor is 32, GET
/// returns both values even while an older in-flight query retains a previous
/// snapshot. Flat segments may resolve an omitted nprobe above that floor.
pub async fn get_query_config(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
) -> Json<QueryKnobs> {
    Json(state.runtime_query_config.snapshot().as_ref().clone())
}

/// Validates and atomically publishes a partial runtime query update.
///
/// `PUT` is registered as an alias for this handler with the same partial
/// update semantics as `PATCH`: omitted fields are left unchanged.
///
/// # Parameters
///
/// - `state`: Shared runtime holder and immutable boot-time validation bounds.
/// - `patch`: Owned JSON-decoded changes. Unknown fields are rejected by serde
///   before this function runs.
///
/// # Returns
///
/// The complete newly published [`QueryKnobs`] snapshot as JSON.
///
/// # Errors
///
/// Returns [`ApiError`] for mutually exclusive rerank settings, zero/out-of-
/// bounds defaults, or a poisoned configuration lock. Validation errors leave
/// the current snapshot unchanged and map through the central API error layer.
///
/// # Side Effects
///
/// Replaces the process-local snapshot atomically, emits structured change
/// logs, and updates the rerank-gap metric. It performs no object-store or
/// manifest writes.
///
/// # Consistency
///
/// Existing queries keep their old immutable `Arc` snapshot; later queries see
/// all accepted fields together. `PUT` is intentionally not full replacement.
///
/// # Performance
///
/// Acquires one short process-local write lock and clones a small configuration
/// value. This administration path performs no network I/O.
///
/// # Examples
///
/// Patching only `default_nprobe` to 32 preserves `default_top_k`. If the
/// boot-time maximum is 16, the handler returns a validation error and publishes
/// nothing.
///
/// # Rust Notes for Java/C Engineers
///
/// `Json(patch)` destructures an owned extractor; no nullable field signals are
/// needed because [`QueryKnobsPatch`] uses [`Option`] for omission. The `?`
/// operator is written as `map_err(...)?` here so the domain error crosses the
/// HTTP boundary as [`ApiError`] while successful control flow stays linear.
pub async fn update_query_config(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Json(patch): Json<QueryKnobsPatch>,
) -> Result<Json<QueryKnobs>, ApiError> {
    let updated = state
        .runtime_query_config
        .update(patch, &state.query_knob_bounds)
        .map_err(ApiError::from)?;

    Ok(Json(updated.as_ref().clone()))
}
