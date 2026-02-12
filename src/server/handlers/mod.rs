pub mod health;
pub mod metrics;
pub mod namespace;
#[cfg(feature = "profiling")]
pub mod profiling;
pub mod query;
pub mod vectors;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::error::ZeppelinError;

/// Wrapper that converts `ZeppelinError` into an HTTP response.
pub struct ApiError(pub ZeppelinError);

impl From<ZeppelinError> for ApiError {
    fn from(e: ZeppelinError) -> Self {
        ApiError(e)
    }
}

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
