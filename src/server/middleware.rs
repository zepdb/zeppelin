use std::net::SocketAddr;
use std::time::Instant;

use axum::extract::{ConnectInfo, MatchedPath, State};
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::Instrument;

use crate::error::ZeppelinError;
use crate::metrics::{HTTP_REQUESTS_TOTAL, RATE_LIMITED_TOTAL, REQUESTS_BY_IP_TOTAL};
use crate::server::handlers::ApiError;
use crate::server::AppState;

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
