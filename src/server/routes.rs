use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use super::handlers::{health, metrics, namespace, query, vectors};
use super::middleware;
use super::AppState;

pub fn build_router(state: AppState) -> Router {
    let timeout = Duration::from_secs(state.config.server.request_timeout_secs);

    Router::new()
        .route("/healthz", get(health::health_check))
        .route("/readyz", get(health::readiness_check))
        .route("/metrics", get(metrics::metrics_handler))
        .route(
            "/v1/namespaces",
            post(namespace::create_namespace).get(namespace::list_namespaces),
        )
        .route(
            "/v1/namespaces/:ns",
            get(namespace::get_namespace).delete(namespace::delete_namespace),
        )
        .route(
            "/v1/namespaces/:ns/vectors",
            post(vectors::upsert_vectors).delete(vectors::delete_vectors),
        )
        .route(
            "/v1/namespaces/:ns/query",
            post(query::query_namespace).layer(axum::middleware::from_fn_with_state(
                state.clone(),
                middleware::concurrency_limit,
            )),
        )
        .layer(axum::middleware::from_fn(middleware::http_metrics))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(
            state.config.server.max_request_body_mb * 1024 * 1024,
        ))
        .layer(RequestBodyLimitLayer::new(
            state.config.server.max_request_body_mb * 1024 * 1024,
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(axum::middleware::from_fn(middleware::request_id))
        .with_state(state)
}
