/// HTTP request handlers for all API endpoints.
pub mod handlers;

use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, DefaultBodyLimit, MatchedPath, State};
use axum::http::{Method, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::Router;
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Instrument, Level};

use crate::cache::hydration::SegmentHydrator;
use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::compaction::Compactor;
use crate::config::Config;
use crate::error::ZeppelinError;
use crate::fts::wal_cache::WalFtsCache;
use crate::metrics::{HTTP_REQUESTS_TOTAL, RATE_LIMITED_TOTAL};
use crate::namespace::NamespaceManager;
use crate::runtime_config::{QueryKnobBounds, RuntimeQueryConfig};
use crate::storage::ZeppelinStore;
use crate::wal::{LeaseManager, WalReader, WalWriter};

use self::handlers::{config as config_handler, namespace, query, vectors, ApiError};

tokio::task_local! {
    /// The current request's ID, set by the `request_id` middleware and read by
    /// the error envelope (`handlers::error_response`) so error bodies can carry
    /// `request_id` without threading it through every handler signature.
    static REQUEST_ID: String;
}

/// The current request's ID if inside a `REQUEST_ID` scope, else `None`.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// Rate-limit bucket class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitClass {
    /// Read/query route.
    Read,
    /// Write/admin route.
    Write,
}

impl RateLimitClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Client identity resolved by the rate-limit middleware.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitIdentity {
    /// Client IP after trusted-proxy extraction.
    pub ip: IpAddr,
}

/// Key for one client/class token bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitKey {
    ip: IpAddr,
    class: RateLimitClass,
}

impl Hash for RateLimitKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.class.hash(state);
    }
}

/// Token bucket state.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitBucket {
    tokens: u64,
    last_refill: Instant,
    last_seen: Instant,
}

/// Shared application state injected into all handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// S3-backed object store for all persistence operations.
    pub store: ZeppelinStore,
    /// Manages namespace CRUD and metadata.
    pub namespace_manager: Arc<NamespaceManager>,
    /// Optional prefix for server-generated namespace names.
    ///
    /// Production leaves this unset. Test servers use it to keep API-created
    /// namespaces under the same random harness prefix as direct storage keys.
    pub namespace_name_prefix: Option<String>,
    /// Writes WAL fragments to S3.
    pub wal_writer: Arc<WalWriter>,
    /// Reads WAL fragments from S3.
    pub wal_reader: Arc<WalReader>,
    /// Lease-protected compactor shared by background and manual admin paths.
    pub compactor: Arc<Compactor>,
    /// Per-namespace compaction lease manager.
    pub lease_manager: Arc<LeaseManager>,
    /// Global server and indexing configuration.
    pub config: Arc<Config>,
    /// Trusted proxy CIDRs parsed once at startup for rate-limit client-IP resolution.
    pub trusted_proxies: Arc<[IpCidr]>,
    /// Runtime-mutable query configuration snapshots.
    pub runtime_query_config: Arc<RuntimeQueryConfig>,
    /// Boot-time validation bounds for runtime query knob updates.
    pub query_knob_bounds: QueryKnobBounds,
    /// LRU disk cache for segment data.
    pub cache: Arc<DiskCache>,
    /// In-memory manifest cache with TTL.
    pub manifest_cache: Arc<ManifestCache>,
    /// Optional background warm-set hydrator.
    pub hydrator: Option<Arc<SegmentHydrator>>,
    /// In-memory cache for WAL-level full-text search indexes.
    pub fts_cache: Arc<WalFtsCache>,
    /// Semaphore that caps concurrent in-flight queries.
    pub query_semaphore: Arc<Semaphore>,
    /// Per-client, per-class token bucket state for rate limiting.
    pub rate_limiters: Arc<DashMap<RateLimitKey, RateLimitBucket>>,
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
    let request_id = current_request_id();
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status_str])
        .inc();
    tracing::info!(
        request_id = request_id.as_deref().unwrap_or(""),
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
    // Run downstream inside the REQUEST_ID task-local scope so the error
    // envelope can stamp `request_id` on any error produced below.
    REQUEST_ID
        .scope(id.clone(), async move {
            let mut response = next.run(request).await;
            response
                .headers_mut()
                .insert("x-request-id", rid.parse().unwrap());
            response
        })
        .instrument(tracing::info_span!("request", request_id = %id))
        .await
}

/// Minimal request-id middleware for the query hot path.
///
/// This preserves the lightweight router's no-TraceLayer/no-span shape while
/// still returning `x-request-id` and making the id available to error
/// envelopes and explicit query-route logs.
#[allow(clippy::unwrap_used)]
pub async fn query_request_id(request: Request<axum::body::Body>, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let rid = id.clone();
    REQUEST_ID
        .scope(id, async move {
            let mut response = next.run(request).await;
            response
                .headers_mut()
                .insert("x-request-id", rid.parse().unwrap());
            response
        })
        .await
}

/// Middleware that normalizes middleware/layer-produced error responses into
/// the canonical JSON envelope (Task 11 I4).
///
/// Tower layers like `TimeoutLayer` (408) and `RequestBodyLimitLayer` (413)
/// emit bare/plain-text bodies that never pass through a handler. This runs
/// OUTERMOST and rewrites any response whose status we own but whose body is
/// not already our JSON envelope (detected via `content-type`). Handler and
/// `ApiError` responses are already `application/json`, so they pass through
/// untouched — this only catches the layer-produced stragglers.
pub async fn normalize_error_responses(request: Request<axum::body::Body>, next: Next) -> Response {
    // Capture the request's id BEFORE running downstream: this layer sits above
    // the request_id middleware (and its REQUEST_ID scope), so when it rewrites
    // a layer-produced error it must thread the id through explicitly or the
    // 408/413 envelope would lose it. Prefer the response's x-request-id (set
    // by the inner middleware, incl. a server-generated UUID) and fall back to
    // the incoming request header.
    let req_id_hdr = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let response = next.run(request).await;
    let status = response.status();
    if handlers::envelope_for_status(status).is_none() {
        return response; // not a status we own (incl. all 2xx/3xx)
    }
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"));
    if is_json {
        return response; // already enveloped by a handler / ApiError
    }
    let rid = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or(req_id_hdr);
    handlers::render_status_envelope(status, rid)
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

/// Parsed IP CIDR range used for trusted-proxy matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

/// Resolve the rate-limit client IP from a peer IP and optional XFF header.
///
/// X-Forwarded-For is trusted only when `peer_ip` belongs to one of
/// `trusted_proxies`. When trusted, the selected client is the rightmost XFF
/// address that is not itself trusted.
pub fn resolve_rate_limit_client_ip(
    peer_ip: IpAddr,
    x_forwarded_for: Option<&str>,
    trusted_proxies: &[IpCidr],
) -> Result<IpAddr, ZeppelinError> {
    if trusted_proxies.is_empty() {
        return Ok(peer_ip);
    }
    if !trusted_proxies.iter().any(|cidr| cidr.contains(peer_ip)) {
        return Ok(peer_ip);
    }

    let Some(x_forwarded_for) = x_forwarded_for else {
        return Ok(peer_ip);
    };
    for entry in x_forwarded_for.split(',').rev() {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(ip) = trimmed.parse::<IpAddr>() else {
            continue;
        };
        if !trusted_proxies.iter().any(|cidr| cidr.contains(ip)) {
            return Ok(ip);
        }
    }
    Ok(peer_ip)
}

/// Parse configured trusted-proxy CIDR strings into reusable matcher ranges.
pub fn parse_trusted_proxies(values: &[String]) -> Result<Vec<IpCidr>, ZeppelinError> {
    values
        .iter()
        .map(|value| parse_ip_cidr(value))
        .collect::<Result<Vec<_>, _>>()
}

fn parse_ip_cidr(value: &str) -> Result<IpCidr, ZeppelinError> {
    let (ip, prefix) = value.split_once('/').ok_or_else(|| {
        ZeppelinError::Config(format!("trusted proxy {value:?} must be an IP CIDR range"))
    })?;
    let network = ip.parse::<IpAddr>().map_err(|e| {
        ZeppelinError::Config(format!("trusted proxy {value:?} has invalid IP: {e}"))
    })?;
    let prefix = prefix.parse::<u8>().map_err(|e| {
        ZeppelinError::Config(format!("trusted proxy {value:?} has invalid prefix: {e}"))
    })?;
    let max_prefix = match network {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max_prefix {
        return Err(ZeppelinError::Config(format!(
            "trusted proxy {value:?} prefix must be <= {max_prefix}"
        )));
    }
    Ok(IpCidr { network, prefix })
}

impl IpCidr {
    fn contains(self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => ipv4_in_prefix(ip, network, self.prefix),
            (IpAddr::V6(network), IpAddr::V6(ip)) => ipv6_in_prefix(ip, network, self.prefix),
            _ => false,
        }
    }
}

fn ipv4_in_prefix(ip: Ipv4Addr, network: Ipv4Addr, prefix: u8) -> bool {
    let ip = u32::from(ip);
    let network = u32::from(network);
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    (ip & mask) == (network & mask)
}

fn ipv6_in_prefix(ip: Ipv6Addr, network: Ipv6Addr, prefix: u8) -> bool {
    let ip = u128::from(ip);
    let network = u128::from(network);
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    (ip & mask) == (network & mask)
}

/// Per-client token-bucket rate limiter.
///
/// Resolves client identity from X-Forwarded-For only when the socket peer is
/// a configured trusted proxy, then applies separate read/write buckets.
/// Skips rate limiting for health/readiness/metrics endpoints.
/// Returns 429 with `Retry-After` header when tokens are exhausted.
pub async fn rate_limit(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let Some(class) = rate_limit_class(request.method(), path) else {
        return next.run(request).await;
    };

    let x_forwarded_for = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok());
    let ip = match resolve_rate_limit_client_ip(
        addr.ip(),
        x_forwarded_for,
        state.trusted_proxies.as_ref(),
    ) {
        Ok(ip) => ip,
        Err(err) => return ApiError(err).into_response(),
    };
    request.extensions_mut().insert(RateLimitIdentity { ip });

    match consume_rate_limit(&state, ip, class, 1) {
        Ok(()) => next.run(request).await,
        Err(err) => ApiError(err).into_response(),
    }
}

/// Consume `tokens` from the per-client rate limiter.
///
/// Batch query calls this after deserialization to charge each entry rather
/// than each HTTP request. The route-level middleware has already consumed one
/// token, so handlers should pass only the additional token count.
pub(crate) fn consume_rate_limit(
    state: &AppState,
    ip: IpAddr,
    class: RateLimitClass,
    tokens_to_consume: u64,
) -> Result<(), ZeppelinError> {
    if tokens_to_consume == 0 {
        return Ok(());
    }

    let (rps, burst) = rate_limit_settings(&state.config, class);
    if rps == 0 {
        return Ok(());
    }
    let now = Instant::now();
    evict_idle_rate_limiters(
        &state.rate_limiters,
        now,
        Duration::from_secs(state.config.server.rate_limit_idle_ttl_secs),
    );
    let key = RateLimitKey { ip, class };

    let allowed = {
        let mut entry = state
            .rate_limiters
            .entry(key)
            .or_insert_with(|| RateLimitBucket {
                tokens: burst,
                last_refill: now,
                last_seen: now,
            });
        let bucket = entry.value_mut();

        // Refill tokens based on elapsed time.
        let elapsed = now.duration_since(bucket.last_refill);
        let refill = elapsed.as_secs_f64() * rps as f64;
        if refill >= 1.0 {
            bucket.tokens = (bucket.tokens + refill as u64).min(burst);
            bucket.last_refill = now;
        }
        bucket.last_seen = now;

        if bucket.tokens >= tokens_to_consume {
            bucket.tokens -= tokens_to_consume;
            true
        } else {
            false
        }
    };

    if allowed {
        Ok(())
    } else {
        let retry_after_secs = retry_after_secs(
            state
                .rate_limiters
                .get(&key)
                .map(|bucket| bucket.tokens)
                .unwrap_or(0),
            tokens_to_consume,
            rps,
        );
        RATE_LIMITED_TOTAL
            .with_label_values(&[class.as_str()])
            .inc();
        tracing::warn!(
            ip = %ip,
            class = class.as_str(),
            requested_tokens = tokens_to_consume,
            "rate limit exceeded"
        );
        Err(ZeppelinError::RateLimitExceeded { retry_after_secs })
    }
}

fn rate_limit_settings(config: &Config, class: RateLimitClass) -> (u64, u64) {
    match class {
        RateLimitClass::Read => (
            config.server.rate_limit_rps as u64,
            config.server.rate_limit_burst as u64,
        ),
        RateLimitClass::Write => (
            config.server.write_rate_limit_rps as u64,
            config.server.write_rate_limit_burst as u64,
        ),
    }
}

fn retry_after_secs(available: u64, requested: u64, rps: u64) -> u64 {
    let deficit = requested.saturating_sub(available).max(1);
    deficit.div_ceil(rps).max(1)
}

fn rate_limit_class(method: &Method, path: &str) -> Option<RateLimitClass> {
    if path == "/healthz" || path == "/readyz" || path == "/metrics" {
        return None;
    }
    if method == Method::GET
        || path.ends_with("/query")
        || path.ends_with("/query/batch")
        || path.ends_with("/vectors/get")
    {
        Some(RateLimitClass::Read)
    } else {
        Some(RateLimitClass::Write)
    }
}

fn evict_idle_rate_limiters(
    rate_limiters: &DashMap<RateLimitKey, RateLimitBucket>,
    now: Instant,
    idle_ttl: Duration,
) {
    rate_limiters.retain(|_, bucket| now.duration_since(bucket.last_seen) < idle_ttl);
}

/// Builds the axum router with all routes, middleware, and shared state.
pub fn build_router(state: AppState) -> Router {
    let timeout = Duration::from_secs(state.config.server.request_timeout_secs);
    let body_limit = state.config.server.max_request_body_mb * 1024 * 1024;

    // Query route: lightweight middleware (request id, no trace layer/span
    // traversal). The query handler has its own #[instrument] for structured
    // logging.
    let query_routes = Router::new()
        .route(
            "/v1/namespaces/:ns/query",
            post(query::query_namespace).layer(axum::middleware::from_fn_with_state(
                state.clone(),
                concurrency_limit,
            )),
        )
        .route(
            "/v1/namespaces/:ns/query/batch",
            post(query::batch_query_namespace).layer(axum::middleware::from_fn_with_state(
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
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(axum::middleware::from_fn(query_request_id));

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
        .route(
            "/v1/config/query",
            get(config_handler::get_query_config)
                .patch(config_handler::update_query_config)
                .put(config_handler::update_query_config),
        )
        .route("/v1/namespaces", post(namespace::create_namespace))
        .route(
            "/v1/namespaces/:ns",
            get(namespace::get_namespace).delete(namespace::delete_namespace),
        )
        .route(
            "/v1/namespaces/:ns/index_config",
            patch(namespace::patch_index_config),
        )
        .route(
            "/v1/namespaces/:ns/compact",
            post(namespace::compact_namespace),
        )
        .route(
            "/v1/namespaces/:ns/compact/status",
            get(namespace::get_compaction_status),
        )
        .route(
            "/v1/namespaces/:ns/hydrate",
            post(namespace::trigger_hydration),
        )
        .route("/v1/namespaces/:ns/vectors/get", post(vectors::get_vectors))
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

    query_routes
        .merge(other_routes)
        // Unmatched routes → canonical 404 envelope (I4).
        .fallback(handlers::not_found_fallback)
        // Outermost: normalize any layer-produced bare error body (408/413/…)
        // into the canonical envelope. Runs after all inner layers so it sees
        // their responses; JSON envelopes from handlers pass through.
        .layer(axum::middleware::from_fn(normalize_error_responses))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_eviction_removes_idle_entries() {
        let buckets = DashMap::new();
        let now = Instant::now();
        let old_key = RateLimitKey {
            ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            class: RateLimitClass::Read,
        };
        let fresh_key = RateLimitKey {
            ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 2)),
            class: RateLimitClass::Write,
        };
        buckets.insert(
            old_key,
            RateLimitBucket {
                tokens: 1,
                last_refill: now - Duration::from_secs(60),
                last_seen: now - Duration::from_secs(60),
            },
        );
        buckets.insert(
            fresh_key,
            RateLimitBucket {
                tokens: 1,
                last_refill: now,
                last_seen: now,
            },
        );

        evict_idle_rate_limiters(&buckets, now, Duration::from_secs(10));

        assert!(!buckets.contains_key(&old_key));
        assert!(buckets.contains_key(&fresh_key));
    }
}
