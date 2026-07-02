/// Namespace CRUD handlers.
pub mod namespace;
/// Vector similarity and BM25 query handler.
pub mod query;
/// Vector upsert and delete handlers.
pub mod vectors;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use prometheus::{Encoder, TextEncoder};
use serde_json::{json, Value};

use crate::error::ZeppelinError;
use crate::server::AppState;

/// Wrapper that converts `ZeppelinError` into an HTTP response.
pub struct ApiError(pub ZeppelinError);

/// Converts a `ZeppelinError` into an `ApiError`.
impl From<ZeppelinError> for ApiError {
    fn from(e: ZeppelinError) -> Self {
        ApiError(e)
    }
}

/// Maps `ApiError` to an HTTP response with a JSON body and appropriate status code.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.0.status_code();
        let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        if status_code.is_server_error() {
            tracing::error!(error = %self.0, status, "server error");
        } else if status_code.is_client_error() {
            tracing::warn!(error = %self.0, status, "client error");
        }
        let body = json!({
            "error": self.0.to_string(),
            "status": status,
        });
        // Include Retry-After header for rate-limited responses.
        if let ZeppelinError::RateLimitExceeded { retry_after_secs } = &self.0 {
            let mut response = (status_code, axum::Json(body)).into_response();
            if let Ok(val) = retry_after_secs.to_string().parse() {
                response.headers_mut().insert("retry-after", val);
            }
            return response;
        }
        (status_code, axum::Json(body)).into_response()
    }
}

/// Liveness probe: returns 200 OK if the server process is running.
pub async fn health_check() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

/// Readiness probe: returns 200 OK when S3 connectivity is confirmed.
pub async fn readiness_check(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    match state.store.list_prefix("__healthcheck__").await {
        Ok(_) => Ok(Json(json!({"status": "ready", "s3_connected": true}))),
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready", "s3_connected": false, "error": e.to_string()})),
        )),
    }
}

/// Serves Prometheus metrics in the text exposition format.
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

/// Query parameters for the CPU profiling endpoint.
#[cfg(feature = "profiling")]
#[derive(serde::Deserialize)]
pub struct ProfileParams {
    /// Duration of CPU profiling in seconds (1-300).
    #[serde(default = "default_profile_seconds")]
    pub seconds: u64,
}

#[cfg(feature = "profiling")]
fn default_profile_seconds() -> u64 {
    30
}

/// GET /debug/pprof/cpu?seconds=N
///
/// Samples CPU at 99 Hz for N seconds (clamped 1-300), returns an SVG flamegraph.
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
