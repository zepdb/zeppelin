/// Health and readiness probe handlers.
pub mod health;
/// Prometheus metrics exposition handler.
pub mod metrics;
/// Namespace CRUD handlers.
pub mod namespace;
#[cfg(feature = "profiling")]
/// CPU profiling handler (feature-gated).
pub mod profiling;
/// Vector similarity and BM25 query handler.
pub mod query;
/// Vector upsert and delete handlers.
pub mod vectors;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::error::ZeppelinError;

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
        (status_code, axum::Json(body)).into_response()
    }
}
