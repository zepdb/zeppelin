use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use super::handlers::{health, namespace, query, vectors};
use super::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health::health_check))
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
            post(query::query_namespace),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
