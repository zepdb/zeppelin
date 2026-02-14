use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus::{Encoder, TextEncoder};

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
