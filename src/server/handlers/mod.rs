pub mod health;
pub mod namespace;
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
        let body = json!({
            "error": self.0.to_string(),
            "status": status,
        });
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            axum::Json(body),
        )
            .into_response()
    }
}
