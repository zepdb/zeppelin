/// HTTP request handlers for all API endpoints.
pub mod handlers;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, DefaultBodyLimit, MatchedPath, State};
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Instrument, Level};

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::config::Config;
use crate::error::ZeppelinError;
use crate::fts::wal_cache::WalFtsCache;
use crate::metrics::{HTTP_REQUESTS_TOTAL, RATE_LIMITED_TOTAL, REQUESTS_BY_IP_TOTAL};
use crate::namespace::NamespaceManager;
use crate::storage::ZeppelinStore;
use crate::wal::BatchWalWriter;
use crate::wal::{WalReader, WalWriter};

use self::handlers::{namespace, query, vectors, ApiError};

/// Shared application state injected into all handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// S3-backed object store for all persistence operations.
    pub store: ZeppelinStore,
    /// Manages namespace CRUD and metadata.
    pub namespace_manager: Arc<NamespaceManager>,
    /// Writes WAL fragments to S3.
    pub wal_writer: Arc<WalWriter>,
    /// Reads WAL fragments from S3.
    pub wal_reader: Arc<WalReader>,
    /// Global server and indexing configuration.
    pub config: Arc<Config>,
    /// LRU disk cache for segment data.
    pub cache: Arc<DiskCache>,
    /// In-memory manifest cache with TTL.
    pub manifest_cache: Arc<ManifestCache>,
    /// In-memory cache for WAL-level full-text search indexes.
    pub fts_cache: Arc<WalFtsCache>,
    /// Semaphore that caps concurrent in-flight queries.
    pub query_semaphore: Arc<Semaphore>,
    /// Optional batched WAL writer (enabled when batch_manifest_size > 1).
    pub batch_wal_writer: Option<Arc<BatchWalWriter>>,
    /// Per-IP token bucket state for rate limiting.
    /// Maps IP → (available tokens, last refill time).
    pub rate_limiters: Arc<DashMap<IpAddr, (u64, Instant)>>,
}

/// Middleware that increments `HTTP_REQUESTS_TOTAL` for every response
/// and logs request details (IP, path, status, latency) via structured tracing.
///
/// Uses `MatchedPath` to normalize route patterns (avoids unbounded cardinality
/// from namespace names in URLs).
#[allow(clippy::unwrap_used)]
pub async fn http_metrics(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    matched_path: Option<MatchedPath>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let path = matched_path
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let uri = request.uri().path().to_string();
    let start = Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16();
    let latency_ms = start.elapsed().as_millis();
    let status_str = status.to_string();
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status_str])
        .inc();
    REQUESTS_BY_IP_TOTAL
        .with_label_values(&[&addr.ip().to_string(), &method, &path, &status_str])
        .inc();
    tracing::info!(
        ip = %addr.ip(),
        method = %method,
        path = %uri,
        status = status,
        latency_ms = latency_ms,
        "request"
    );
    response
}

/// Middleware that attaches a request ID to every request.
///
/// - Respects an incoming `x-request-id` header if present.
/// - Otherwise generates a UUID v4.
/// - Creates a tracing span so all downstream logs include the request ID.
/// - Returns the request ID in the response `x-request-id` header.
#[allow(clippy::unwrap_used)]
pub async fn request_id(request: Request<axum::body::Body>, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let rid = id.clone();
    async move {
        let mut response = next.run(request).await;
        response
            .headers_mut()
            .insert("x-request-id", rid.parse().unwrap());
        response
    }
    .instrument(tracing::info_span!("request", request_id = %id))
    .await
}

/// Middleware that limits concurrent query execution.
///
/// Acquires a permit from the query semaphore before forwarding the request.
/// Returns 503 Service Unavailable when all permits are exhausted.
pub async fn concurrency_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    match state.query_semaphore.try_acquire() {
        Ok(_permit) => next.run(request).await,
        Err(_) => ApiError(ZeppelinError::QueryConcurrencyExhausted).into_response(),
    }
}

/// Per-IP token-bucket rate limiter.
///
/// Allows `rate_limit_burst` tokens initially, refilling at `rate_limit_rps` tokens/sec.
/// Skips rate limiting for health/readiness/metrics endpoints.
/// Returns 429 with `Retry-After` header when tokens are exhausted.
pub async fn rate_limit(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    // Skip rate limiting for infrastructure endpoints.
    if path == "/healthz" || path == "/readyz" || path == "/metrics" {
        return next.run(request).await;
    }

    let ip = addr.ip();
    let rps = state.config.server.rate_limit_rps as u64;
    let burst = state.config.server.rate_limit_burst as u64;
    let now = Instant::now();

    let allowed = {
        let mut entry = state
            .rate_limiters
            .entry(ip)
            .or_insert_with(|| (burst, now));
        let (ref mut tokens, ref mut last_refill) = *entry;

        // Refill tokens based on elapsed time.
        let elapsed = now.duration_since(*last_refill);
        let refill = elapsed.as_secs_f64() * rps as f64;
        if refill >= 1.0 {
            *tokens = (*tokens + refill as u64).min(burst);
            *last_refill = now;
        }

        if *tokens > 0 {
            *tokens -= 1;
            true
        } else {
            false
        }
    };

    if allowed {
        next.run(request).await
    } else {
        let retry_after_secs = if rps > 0 { 1 } else { 60 };
        RATE_LIMITED_TOTAL
            .with_label_values(&[&ip.to_string()])
            .inc();
        tracing::warn!(ip = %ip, "rate limit exceeded");
        ApiError(ZeppelinError::RateLimitExceeded { retry_after_secs }).into_response()
    }
}

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
                concurrency_limit,
            )),
        )
        .layer(axum::middleware::from_fn(http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit));

    // All other routes: full middleware stack with request tracing.
    #[allow(unused_mut)]
    let mut other_routes = Router::new()
        .route("/healthz", get(handlers::health_check))
        .route("/readyz", get(handlers::readiness_check))
        .route("/metrics", get(handlers::metrics_handler));

    #[cfg(feature = "profiling")]
    {
        other_routes = other_routes.route("/debug/pprof/cpu", get(handlers::cpu_profile));
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
        .layer(axum::middleware::from_fn(http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(axum::middleware::from_fn(request_id));

    query_routes.merge(other_routes).with_state(state)
}
