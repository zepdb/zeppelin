//! Shared handler utilities, operational endpoints, and API error rendering.
//!
//! Endpoint-specific modules translate JSON and path inputs into calls on the
//! services in [`crate::server::AppState`]. This file owns the behavior shared
//! across those endpoints: canonical client-safe error envelopes, finite-float
//! validation, liveness/readiness, Prometheus exposition, and the optional CPU
//! profiler. It does not duplicate namespace, WAL, compaction, or query rules;
//! those remain in their domain layers and are surfaced through typed errors.
//!
//! Domain failures and a small set of middleware-only statuses meet at one
//! public response shape:
//!
//! ```text
//! handler returns ZeppelinError                 Tower/Axum returns bare status
//!            |                                              |
//!            v                                              v
//! ApiError -> status/code/retry/redacted text      envelope_for_status
//!            |                                              |
//!            +------------------+---------------------------+
//!                               v
//!       {code, error, status, request_id?, retryable}
//!                               |
//!               full internal detail stays in logs
//! ```
//!
//! Operational endpoints have narrower contracts. `/healthz` proves only that
//! the process can answer; `/readyz` performs an object-store list operation;
//! `/metrics` exports process metrics; and the feature-gated profiling route
//! performs blocking CPU sampling off the Tokio worker pool. Their success and
//! failure bodies are not all canonical domain-error envelopes, so callers
//! should follow each endpoint's documented response format.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::server::handlers::ApiError`] and
//!    [`crate::server::handlers::error_response`] for domain-error mapping.
//! 2. Read [`crate::server::handlers::envelope_for_status`] and
//!    [`crate::server::handlers::render_status_envelope`] for pre-handler errors.
//! 3. Read [`crate::server::handlers::health_check`],
//!    [`crate::server::handlers::readiness_check`], and
//!    [`crate::server::handlers::metrics_handler`] for operator routes.
//! 4. Continue into [`crate::server::handlers::namespace`],
//!    [`crate::server::handlers::vectors`],
//!    [`crate::server::handlers::query`], and
//!    [`crate::server::handlers::config`] for resource semantics; `as_of` is
//!    their internal shared point-in-time resolver.
//!
//! ## Invariants
//!
//! - Client bodies use [`ZeppelinError::client_message`]; full `Display` text,
//!   which may contain S3 keys or lease details, is logged but not returned.
//! - Stable machine-readable codes come from [`ZeppelinError::error_code`], not
//!   from parsing prose.
//! - A missing object referenced by authoritative state is a redacted server
//!   failure, not a client-facing resource 404.
//! - Readiness reports storage reachability without returning endpoint, bucket,
//!   or object-store diagnostics to unauthenticated callers.
//!
//! ## Rust concepts used here
//!
//! [`crate::server::handlers::ApiError`] is a newtype that implements Axum's
//! [`IntoResponse`](axum::response::IntoResponse) trait. It
//! is similar to a Java exception mapper or a C error-to-response adapter, but
//! the compiler chooses the conversion from the concrete return type. Handler
//! signatures use [`Result`](std::result::Result) so `?` can move a failure into
//! this adapter. Profiling uses [`tokio::task::spawn_blocking`] to move a
//! blocking closure to the runtime's blocking pool; awaiting its join handle
//! does not block an async worker thread.

/// Internal point-in-time manifest resolution shared by query and clone handlers.
pub(crate) mod as_of;
/// Runtime query-configuration read and update endpoints.
pub mod config;
/// Namespace, snapshot, clone, compaction, and hydration endpoints.
pub mod namespace;
/// Vector similarity, BM25, hybrid, and batch query endpoints.
pub mod query;
/// Signed receipt verification and manifest-root inspection endpoints.
pub mod receipt;
/// Security principal, credential, grant, and policy administration endpoints.
pub mod security;
/// Vector upsert, lookup, and delete endpoints.
pub mod vectors;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use prometheus::{Encoder, TextEncoder};
use serde_json::{json, Value};

use crate::error::ZeppelinError;
use crate::server::{current_request_id, AppState};

/// Adapts an owned Zeppelin domain failure to Axum's response protocol.
///
/// Handlers return `Result<T, ApiError>` and use `?` through the [`From`]
/// implementation below. Conversion to a response centralizes status, stable
/// code, retry policy, request correlation, and redaction instead of allowing
/// each endpoint to invent an error body.
///
/// # Examples
///
/// A missing namespace becomes a JSON 404 with code `NAMESPACE_NOT_FOUND`. A
/// missing S3 object referenced by a manifest becomes a redacted JSON 500 with
/// code `INTERNAL_DATA_MISSING`; its key appears only in the structured log.
///
/// # Rust Notes for Java/C Engineers
///
/// This one-field newtype has no intended semantic overhead over the wrapped
/// enum. Unlike a Java subclass, it uses trait implementations to opt into
/// framework behavior. Unlike a C typedef, it is a distinct type, so only this
/// explicit wrapper can be converted automatically into an HTTP response.
pub struct ApiError(
    /// Owned error whose classification and sanitized message define the response.
    pub ZeppelinError,
);

/// Stable machine code retained internally for response-side audit middleware.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuditErrorCode(pub &'static str);

/// Finds the first non-finite component in a decoded vector.
///
/// JSON cannot spell NaN or infinity directly, but a finite JSON number such as
/// `1e39` can overflow while narrowing to `f32`. Rejecting that value at the API
/// boundary prevents unordered distance comparisons and non-finite values from
/// entering immutable WAL data or later K-means centroids.
///
/// # Parameters
///
/// - `values`: Borrowed vector components after JSON deserialization and `f32`
///   conversion.
///
/// # Returns
///
/// `Some((dimension_index, kind))` for the first offending component, where
/// `kind` is `"NaN"`, `"inf"`, or `"-inf"`; `None` when every component is
/// finite.
///
/// # Performance
///
/// Performs one allocation-free, linear `is_finite` scan. Across a batch the
/// caller pays O(total dimensions), typically much less than JSON parsing.
///
/// # Examples
///
/// `[0.25, f32::INFINITY, 0.75]` returns `Some((1, "inf"))`. A normal embedding
/// returns `None` and can proceed to dimension validation and WAL publication.
///
/// # Rust Notes for Java/C Engineers
///
/// The input is a borrowed slice, comparable to a Java array view or
/// `const float *` plus length in C, but Rust guarantees non-nullness and bounds
/// checks. The iterator returns only an index; the short static kind string does
/// not allocate or borrow from the slice.
pub(crate) fn find_non_finite(values: &[f32]) -> Option<(usize, &'static str)> {
    values.iter().position(|v| !v.is_finite()).map(|i| {
        let v = values[i];
        let kind = if v.is_nan() {
            "NaN"
        } else if v > 0.0 {
            "inf"
        } else {
            "-inf"
        };
        (i, kind)
    })
}

/// Moves a domain error into the HTTP adapter used by handler return types.
///
/// # Parameters
///
/// - `e`: Owned Zeppelin failure to classify when Axum requests a response.
///
/// # Returns
///
/// An [`ApiError`] containing the same enum value without altering its detail.
///
/// # Examples
///
/// `some_domain_call().await.map_err(ApiError::from)?` preserves the failure
/// until the framework renders the canonical response.
///
/// # Rust Notes for Java/C Engineers
///
/// [`From`] is a compiler-known conversion used by the `?` operator. Ownership
/// moves into the wrapper; there is no exception allocation or error copy.
impl From<ZeppelinError> for ApiError {
    /// Wraps an owned domain error without changing its classification or text.
    ///
    /// # Parameters
    ///
    /// - `e`: Failure moved out of the domain result.
    ///
    /// # Returns
    ///
    /// The HTTP adapter that now owns `e`.
    fn from(e: ZeppelinError) -> Self {
        ApiError(e)
    }
}

/// Renders one domain failure as the canonical client-safe JSON response.
///
/// The envelope is `{code, error, status, request_id?, retryable}`. Numeric
/// status, stable code, retry advice, and sanitized prose all come from
/// [`ZeppelinError`]'s classification methods. The full `Display` value may
/// contain object keys, endpoints, fencing tokens, or lease-holder IDs, so it
/// goes only to structured logs; the body uses
/// [`ZeppelinError::client_message`].
///
/// # Parameters
///
/// - `err`: Borrowed failure. Rendering does not consume it, although the
///   resulting response owns its serialized body.
///
/// # Returns
///
/// An Axum response with the mapped status and JSON envelope. A task-local
/// request ID is included when available, and errors with a retry delay receive
/// a `Retry-After` header. An invalid numeric mapping defensively becomes 500.
///
/// # Side Effects
///
/// Logs 4xx failures at WARN and 5xx failures at ERROR, including full internal
/// detail and the correlation ID. It allocates the JSON body but performs no
/// domain or storage operation.
///
/// # Examples
///
/// A dimension mismatch produces status 400 and code `DIMENSION_MISMATCH` with
/// non-retryable advice. A storage failure logs its endpoint detail, returns a
/// generic `STORAGE_ERROR` message, and marks the response retryable.
pub fn error_response(err: &ZeppelinError) -> Response {
    let status = err.status_code();
    let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let request_id = current_request_id();

    // Log the FULL internal error (with its detail) keyed by request_id; the
    // client body only ever sees the sanitized message.
    if status_code.is_server_error() {
        tracing::error!(error = %err, code = err.error_code(), status, request_id = ?request_id, "server error");
    } else if status_code.is_client_error() {
        tracing::warn!(error = %err, code = err.error_code(), status, request_id = ?request_id, "client error");
    }

    let mut body = json!({
        "code": err.error_code(),
        "error": err.client_message(),
        "status": status,
        "retryable": err.retryable(),
    });
    if let Some(rid) = request_id {
        body["request_id"] = json!(rid);
    }
    if let ZeppelinError::Branch(branch_error) = err {
        let disclosure = match branch_error.as_ref() {
            crate::namespace::branching::BranchError::NamespaceHasLiveBranches {
                visible_children,
                has_additional_children,
                ..
            }
            | crate::namespace::branching::BranchError::BranchHasLiveChildren {
                visible_children,
                has_additional_children,
                ..
            } => Some((visible_children, *has_additional_children)),
            _ => None,
        };
        if let Some((visible_children, has_additional_children)) = disclosure {
            body["visible_children"] = json!(visible_children
                .iter()
                .map(|child| json!({
                    "namespace": child.namespace.to_string(),
                    "branch_id": child.branch_id.to_string(),
                }))
                .collect::<Vec<_>>());
            body["has_additional_children"] = json!(has_additional_children);
        }
    }

    let mut response = (status_code, axum::Json(body)).into_response();
    response
        .extensions_mut()
        .insert(AuditErrorCode(err.error_code()));
    if let Some(secs) = err.retry_after_secs() {
        if let Ok(val) = secs.to_string().parse() {
            response.headers_mut().insert("retry-after", val);
        }
    }
    response
}

/// Lets Axum render [`ApiError`] through the shared canonical mapper.
///
/// # Returns
///
/// The response produced by [`error_response`] after borrowing the wrapped
/// error. Consuming `self` then drops the owned error normally.
///
/// # Examples
///
/// When a handler returns `Err(ApiError(...))`, Axum invokes this method and
/// sends the mapped status, headers, and JSON body.
impl IntoResponse for ApiError {
    /// Consumes the adapter and renders its domain error canonically.
    ///
    /// # Returns
    ///
    /// An owned Axum response from [`error_response`].
    fn into_response(self) -> Response {
        error_response(&self.0)
    }
}

/// Classifies a bare framework status for canonical envelope rendering.
///
/// Middleware failures do not carry a [`ZeppelinError`], so the server owns a
/// small explicit mapping for request timeout, body limit, unmatched route, and
/// method mismatch. All other statuses remain the responsibility of the
/// handler or layer that produced them.
///
/// # Parameters
///
/// - `status`: HTTP status observed by the outer normalization middleware.
///
/// # Returns
///
/// `Some((stable_code, client_message, retryable))` for 408, 413, 404, or 405;
/// `None` for successes, redirects, domain-specific errors, and unowned
/// failures such as 500.
///
/// # Examples
///
/// Status 408 maps to `("REQUEST_TIMEOUT", ..., true)`. Status 503 returns
/// `None` because a concurrency 503 is already rendered from
/// [`ZeppelinError::QueryConcurrencyExhausted`].
pub fn envelope_for_status(status: StatusCode) -> Option<(&'static str, &'static str, bool)> {
    // (code, client message, retryable)
    match status {
        StatusCode::REQUEST_TIMEOUT => Some((
            "REQUEST_TIMEOUT",
            "the request timed out; please retry",
            true,
        )),
        StatusCode::PAYLOAD_TOO_LARGE => Some((
            "PAYLOAD_TOO_LARGE",
            "request body exceeds the configured size limit",
            false,
        )),
        StatusCode::NOT_FOUND => Some(("NOT_FOUND", "no such route or resource", false)),
        StatusCode::METHOD_NOT_ALLOWED => Some((
            "METHOD_NOT_ALLOWED",
            "the HTTP method is not allowed for this route",
            false,
        )),
        _ => None,
    }
}

/// Returns the canonical response for a path that matched no registered route.
///
/// # Returns
///
/// A JSON 404 carrying code `NOT_FOUND`. [`render_status_envelope`] adds the
/// current request ID when the fallback runs inside a request scope.
///
/// # Examples
///
/// `GET /v1/this/route/does/not/exist` reaches this fallback instead of
/// returning Axum's default empty 404 body.
pub async fn not_found_fallback() -> Response {
    render_status_envelope(StatusCode::NOT_FOUND, None)
}

/// Renders a framework-generated status with the canonical JSON shape.
///
/// The outer response-normalization layer runs after the inner request-ID scope
/// has ended, so it can pass an explicit ID captured from headers. Without an
/// override this function consults [`current_request_id`]. A resolved ID appears
/// in both the body and response header.
///
/// # Parameters
///
/// - `status`: Bare framework status to preserve on the response.
/// - `request_id_override`: Owned correlation ID captured outside task-local
///   scope, or `None` to use the current scope.
///
/// # Returns
///
/// A JSON response containing the mapped code, message, numeric status,
/// optional request ID, and retry flag. Unrecognized statuses receive generic
/// code `ERROR` and non-retryable advice. Status 408 also receives
/// `Retry-After: 1`.
///
/// # Examples
///
/// Rendering 413 with ID `upload-7` produces code `PAYLOAD_TOO_LARGE` and
/// echoes `upload-7` in the header and body. Rendering an unowned 418 directly
/// preserves 418 but uses the generic `ERROR` classification.
///
/// # Rust Notes for Java/C Engineers
///
/// `request_id_override.or_else(current_request_id)` consumes the optional
/// owned string only when present and lazily calls the fallback otherwise. The
/// later `ref` pattern borrows that string for JSON construction before it is
/// moved into header parsing; Rust prevents use-after-move at compile time.
pub fn render_status_envelope(status: StatusCode, request_id_override: Option<String>) -> Response {
    let (code, message, retryable) =
        envelope_for_status(status).unwrap_or(("ERROR", "request failed", false));
    let rid = request_id_override.or_else(current_request_id);
    let mut body = json!({
        "code": code,
        "error": message,
        "status": status.as_u16(),
        "retryable": retryable,
    });
    if let Some(ref id) = rid {
        body["request_id"] = json!(id);
    }
    let mut response = (status, Json(body)).into_response();
    if let Some(id) = rid {
        if let Ok(val) = id.parse() {
            response.headers_mut().insert("x-request-id", val);
        }
    }
    if status == StatusCode::REQUEST_TIMEOUT {
        // Nudge clients to back off before retrying a timed-out request.
        if let Ok(val) = "1".parse() {
            response.headers_mut().insert("retry-after", val);
        }
    }
    response
}

/// Reports process liveness without consulting storage or domain state.
///
/// # Returns
///
/// JSON `{"status":"ok"}` with status 200 whenever the Axum task can execute
/// this handler.
///
/// # Side Effects
///
/// None beyond normal request middleware. In particular, this endpoint performs
/// no S3/MinIO request and does not prove the service can answer queries.
///
/// # Examples
///
/// An orchestrator uses `/healthz` to decide whether the process should be
/// restarted. It uses `/readyz`, not this response, before sending data traffic.
pub async fn health_check() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

/// Checks whether the configured object store is reachable for list operations.
///
/// Readiness lists an intentionally unlikely prefix through
/// [`crate::storage::ZeppelinStore`]. A successful empty listing is sufficient;
/// this probe does not read a namespace manifest or prove that any particular
/// immutable artifact exists. Failure text from `object_store` may include an
/// endpoint, port, or bucket, so only a generic reason reaches the caller.
///
/// # Parameters
///
/// - `state`: Shared application state containing the configured store handle.
///
/// # Returns
///
/// `Ok` with JSON `{"status":"ready","s3_connected":true}` and status 200
/// when listing succeeds. Returns a direct `(503, JSON)` rejection with
/// `s3_connected:false` when it fails; this operational body is intentionally
/// distinct from the canonical domain-error envelope.
///
/// # Errors
///
/// The error return represents an unavailable durable audit actor or an
/// unreachable storage backend. No raw backend diagnostic is returned, and no
/// partial mutation can occur because the storage probe only lists.
///
/// # Side Effects
///
/// Checks the process-local durable-audit health latch, then performs one
/// object-store list request. It logs either full failure and `/readyz` bypasses
/// rate-limit charging.
///
/// # Consistency
///
/// Success means the audit actor has not failed and S3 is reachable at that
/// instant; it is not authoritative namespace state or a promise that a later
/// request cannot fail.
///
/// # Examples
///
/// A healthy audit actor and MinIO bucket return 200 even when
/// `__healthcheck__` contains no objects. Audit lease loss, credentials, or
/// networking failures return 503 while detailed diagnostics remain in logs.
pub async fn readiness_check(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !state.audit.is_healthy() {
        tracing::error!("readiness check failed: durable audit writer is unavailable");
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "s3_connected": true,
                "audit_writer_healthy": false,
                "error": "durable audit writer is unavailable",
            })),
        ));
    }
    match state.store.list_prefix("__healthcheck__").await {
        Ok(_) => Ok(Json(json!({"status": "ready", "s3_connected": true}))),
        Err(e) => {
            tracing::error!(error = %e, "readiness check failed: storage backend unreachable");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "s3_connected": false,
                    "error": "storage backend is unreachable",
                })),
            ))
        }
    }
}

/// Encodes the current Prometheus registry in text exposition format.
///
/// # Returns
///
/// Status 200, Prometheus content type, and encoded bytes on success. Encoder
/// failure returns status 500, a plain-text content type, and diagnostic bytes;
/// this operational response is not a canonical JSON domain-error envelope.
///
/// # Errors
///
/// Failures are represented directly in the returned response tuple rather
/// than the Rust return type. They indicate metrics encoding failure, not a
/// storage or manifest failure.
///
/// # Side Effects
///
/// Gathers registered metric families, allocates an output buffer, and logs an
/// encoder failure. `/metrics` bypasses rate-limit charging.
///
/// # Performance
///
/// Work and response size scale with the number of registered metric families
/// and label combinations. No object-store request occurs.
///
/// # Examples
///
/// Prometheus scraping `/metrics` receives lines such as request counters and
/// active-query gauges in version 0.0.4 text format.
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    match encoder.encode(&families, &mut buf) {
        Ok(()) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.04; charset=utf-8",
            )],
            buf,
        ),
        Err(e) => {
            tracing::error!(error = %e, "failed to encode prometheus metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                format!("metrics encoding failed: {e}").into_bytes(),
            )
        }
    }
}

/// Decoded query parameters for the feature-gated CPU profiling endpoint.
///
/// Serde supplies `default_profile_seconds` when `seconds` is absent. The
/// handler subsequently clamps explicit values, so this type may temporarily
/// contain zero or a value greater than 300 after deserialization.
#[cfg(feature = "profiling")]
#[derive(serde::Deserialize)]
pub struct ProfileParams {
    /// Requested sampling duration in seconds; [`cpu_profile`] clamps it to 1–300.
    #[serde(default = "default_profile_seconds")]
    pub seconds: u64,
}

#[cfg(feature = "profiling")]
/// Supplies the 30-second profiling duration used when the query omits it.
///
/// # Returns
///
/// `30`, expressed as seconds.
///
/// # Examples
///
/// `/debug/pprof/cpu` samples for 30 seconds before returning its SVG.
fn default_profile_seconds() -> u64 {
    30
}

/// Samples process CPU activity and returns an SVG flamegraph.
///
/// The requested duration is clamped to 1–300 seconds. Sampling sleeps and
/// renders synchronously, so the work is moved to Tokio's blocking pool rather
/// than occupying an async worker thread.
///
/// ```text
/// HTTP query -> clamp seconds -> spawn blocking profiler -> await join
///                                      |                    |
///                               sample + render       SVG or plain 500
/// ```
///
/// # Parameters
///
/// - `params`: Deserialized query string; absent `seconds` defaults to 30.
///
/// # Returns
///
/// Status 200 with `image/svg+xml` bytes when profiling succeeds. Profiler
/// setup/rendering failure or blocking-task panic returns a plain-text 500 with
/// a diagnostic; these feature-only errors do not use [`ApiError`].
///
/// # Side Effects
///
/// Spawns one blocking task, samples the entire process at 99 Hz for the chosen
/// duration, allocates report and SVG buffers, and emits success or failure
/// logs. Concurrent calls may run concurrent profilers.
///
/// # Performance
///
/// The response cannot complete before the sampling duration. Profiling adds
/// process-wide sampling overhead and blocking-pool occupancy, but does not
/// block a Tokio async worker.
///
/// # Examples
///
/// `/debug/pprof/cpu?seconds=10` returns ten seconds of samples. A value of zero
/// is clamped to one second; a value of 600 is clamped to 300 seconds.
///
/// # Rust Notes for Java/C Engineers
///
/// `spawn_blocking` moves an owned closure to a dedicated pool and returns a
/// typed join handle. This resembles submitting a Java `Callable` to an
/// executor. In C it would require a thread-pool job plus explicit result and
/// error channels. The nested `Result` distinguishes task panic/cancellation
/// from an ordinary profiler error.
#[cfg(feature = "profiling")]
pub async fn cpu_profile(
    axum::extract::Query(params): axum::extract::Query<ProfileParams>,
) -> Response {
    let seconds = params.seconds.clamp(1, 300);

    // Run the profiler on a blocking thread to avoid starving the async runtime.
    let result = tokio::task::spawn_blocking(move || collect_profile(seconds)).await;

    match result {
        Ok(Ok(svg_bytes)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "image/svg+xml")],
            svg_bytes,
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "CPU profiling failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("profiling failed: {e}"),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "profiling task panicked");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("profiling task panicked: {e}"),
            )
                .into_response()
        }
    }
}

#[cfg(feature = "profiling")]
/// Performs synchronous CPU sampling and flamegraph rendering.
///
/// # Parameters
///
/// - `seconds`: Already-clamped sampling duration. The function sleeps for this
///   many seconds while the profiler guard records samples.
///
/// # Returns
///
/// Owned SVG bytes on success.
///
/// # Errors
///
/// Returns descriptive text when profiler initialization, report construction,
/// flamegraph rendering, or protobuf generation fails. Sampling may already
/// have consumed the requested time before a later rendering error occurs.
///
/// # Side Effects
///
/// Installs a 99 Hz profiler guard, blocks the calling thread for the duration,
/// allocates report buffers, and logs SVG and protobuf sizes. The caller must
/// keep this work off async runtime workers.
///
/// # Performance
///
/// Wall time is at least `seconds`; memory depends on the number and diversity
/// of captured stacks plus the rendered SVG size.
///
/// # Examples
///
/// A call with `10` samples for ten seconds and returns a complete SVG buffer;
/// no partial SVG is returned if final rendering fails.
fn collect_profile(seconds: u64) -> Result<Vec<u8>, String> {
    use pprof::protos::Message;

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| format!("failed to start profiler: {e}"))?;

    std::thread::sleep(std::time::Duration::from_secs(seconds));

    let report = guard
        .report()
        .build()
        .map_err(|e| format!("failed to build report: {e}"))?;

    let mut svg_buf = Vec::new();
    report
        .flamegraph(&mut svg_buf)
        .map_err(|e| format!("failed to render flamegraph: {e}"))?;

    // Also log the protobuf size for debugging
    let proto = report
        .pprof()
        .map_err(|e| format!("failed to build pprof proto: {e}"))?;
    let proto_size = proto.encoded_len();
    tracing::info!(
        seconds,
        svg_bytes = svg_buf.len(),
        proto_bytes = proto_size,
        "CPU profile collected"
    );

    Ok(svg_buf)
}
