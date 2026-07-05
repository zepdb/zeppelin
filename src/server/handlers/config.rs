use axum::extract::State;
use axum::Json;

use crate::runtime_config::{QueryKnobs, QueryKnobsPatch};
use crate::server::AppState;

use super::ApiError;

/// Returns the current runtime query configuration.
pub async fn get_query_config(State(state): State<AppState>) -> Json<QueryKnobs> {
    Json(state.runtime_query_config.snapshot().as_ref().clone())
}

/// Partially updates the runtime query configuration.
///
/// `PUT` is registered as an alias for this handler with the same partial
/// update semantics as `PATCH`: omitted fields are left unchanged.
pub async fn update_query_config(
    State(state): State<AppState>,
    Json(patch): Json<QueryKnobsPatch>,
) -> Result<Json<QueryKnobs>, ApiError> {
    let updated = state
        .runtime_query_config
        .update(patch, &state.query_knob_bounds)
        .map_err(ApiError::from)?;

    Ok(Json(updated.as_ref().clone()))
}
