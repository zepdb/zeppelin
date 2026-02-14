use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};

use crate::server::AppState;

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
