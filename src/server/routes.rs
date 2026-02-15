use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

#[cfg(feature = "profiling")]
use super::handlers::profiling;
use super::handlers::{health, metrics, namespace, query, vectors};
use super::middleware;
use super::AppState;

/// Builds the axum router with all routes, middleware, and shared state.
pub fn build_router(state: AppState) -> Router {
    let timeout = Duration::from_secs(state.config.server.request_timeout_secs);
    let body_limit = state.config.server.max_request_body_mb * 1024 * 1024;

    // Query route: lightweight middleware (no request_id span, no trace layer).
    // Removes ~2μs per-request overhead from tracing span creation and traversal.
    // The query handler has its own #[instrument] for structured logging.
    let query_routes = Router::new()
        .route(
            "/v1/namespaces/:ns/query",
            post(query::query_namespace).layer(axum::middleware::from_fn_with_state(
                state.clone(),
                middleware::concurrency_limit,
            )),
        )
        .layer(axum::middleware::from_fn(middleware::http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit));

    // All other routes: full middleware stack with request tracing.
    #[allow(unused_mut)]
    let mut other_routes = Router::new()
        .route("/healthz", get(health::health_check))
        .route("/readyz", get(health::readiness_check))
        .route("/metrics", get(metrics::metrics_handler));

    #[cfg(feature = "profiling")]
    {
        other_routes = other_routes.route("/debug/pprof/cpu", get(profiling::cpu_profile));
    }

    other_routes = other_routes
        .route("/v1/namespaces", post(namespace::create_namespace))
        .route(
            "/v1/namespaces/:ns",
            get(namespace::get_namespace).delete(namespace::delete_namespace),
        )
        .route(
            "/v1/namespaces/:ns/vectors",
            post(vectors::upsert_vectors).delete(vectors::delete_vectors),
        )
        .layer(axum::middleware::from_fn(middleware::http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(axum::middleware::from_fn(middleware::request_id));

    query_routes.merge(other_routes).with_state(state)
}
