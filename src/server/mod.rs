//! HTTP routing, middleware, and shared service composition.
//!
//! This module is the boundary between Axum/Tower and Zeppelin's domain
//! services. Startup constructs one [`crate::server::AppState`],
//! [`crate::server::build_router`] attaches it to every route, and the modules
//! under [`crate::server::handlers`] translate HTTP requests
//! into namespace, WAL, compaction, cache, and query operations. This layer
//! does not make object storage or local caches authoritative: handlers must
//! continue to honor the manifest and storage contracts enforced below it.
//!
//! The router deliberately gives query endpoints a lighter tracing stack than
//! administrative and write endpoints, while preserving request IDs, metrics,
//! timeouts, body limits, rate limits, and the query concurrency cap. Tower
//! layers execute from the last layer added toward the handler:
//!
//! ```text
//! request
//!   |
//!   v
//! normalize selected bare error responses             (all routes)
//!   |
//!   v
//! request ID -> body limits -> timeout -> rate limit
//!   |
//!   v
//! full trace (non-query only) -> HTTP metrics
//!   |
//!   v
//! query semaphore (query only) -> handler -> domain/storage services
//!   |
//!   v
//! response unwinds through the same layers in reverse order
//! ```
//!
//! The request ID uses task-local state, not a process-global variable, so
//! concurrent requests cannot overwrite one another's correlation value. Rate
//! limiting and query admission are separate controls: a token bucket limits a
//! client over time, while a [`Semaphore`](tokio::sync::Semaphore) bounds the
//! number of query futures executing at once.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::server::AppState`] to see the domain services available
//!    to handlers.
//! 2. Read [`crate::server::build_router`] for routes and middleware order.
//! 3. Read [`crate::server::request_id`],
//!    [`crate::server::normalize_error_responses`], and
//!    [`crate::server::http_metrics`] for correlation and observability.
//! 4. Read [`crate::server::rate_limit`] and
//!    [`crate::server::concurrency_limit`] for admission control.
//! 5. Continue into [`crate::server::handlers`] for endpoint response mapping.
//!
//! ## Lifecycle and invariants
//!
//! - [`crate::startup::build_app`] owns service construction and background-task
//!   startup. This module only composes those handles into a router.
//! - [`crate::startup::shutdown_background_tasks`] stops compaction; dropping a
//!   router alone is not Zeppelin's graceful-shutdown protocol.
//! - A client-supplied `X-Forwarded-For` value affects rate limiting only when
//!   the socket peer belongs to a configured trusted-proxy CIDR.
//! - Middleware-generated 404, 405, 408, and 413 responses use the same public
//!   envelope shape as handler errors; internal error details stay in logs.
//! - Health, readiness, and metrics bypass token-bucket charging so operators
//!   can observe an overloaded service.
//!
//! ## Rust concepts used here
//!
//! [`Arc`](std::sync::Arc) gives cloned [`crate::server::AppState`] values shared
//! ownership of expensive services. Cloning an `Arc` increments a reference count; it does
//! not duplicate a cache, compactor, or WAL client. This resembles sharing a
//! Java reference, while also making cross-thread ownership explicit. In C it
//! replaces an informal pointer/refcount convention with compiler-checked
//! cleanup. [`DashMap`](dashmap::DashMap) permits concurrent token-bucket access without one
//! application-wide mutex, and RAII releases each semaphore permit even when a
//! request future is cancelled.

/// HTTP handlers and shared response helpers for every API endpoint.
pub mod handlers;

use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, DefaultBodyLimit, MatchedPath, State};
use axum::http::{Method, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, MethodRouter};
use axum::Router;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::Semaphore;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{Instrument, Level};

use crate::cache::decoded_cache::DecodedArtifactCache;
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
use crate::security::{
    classify_route, Action, AllowDecision, CredentialAdapter, Decision, NamespaceId, Principal,
    RequestContext, Resource, RouteClass, SecurityError, SecurityKernel, SnapshotName,
};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::wal::{LeaseManager, WalFragmentCache, WalReader, WalWriter};

use self::handlers::{config as config_handler, namespace, query, vectors, ApiError};

tokio::task_local! {
    /// Correlation ID scoped to the currently executing request future.
    ///
    /// [`request_id`] and [`query_request_id`] establish the scope;
    /// [`handlers::error_response`] reads it without adding an ID parameter to
    /// every handler and domain function. Tokio keeps values isolated between
    /// concurrent tasks.
    static REQUEST_ID: String;
}

/// Returns the correlation ID associated with the current request task.
///
/// # Returns
///
/// An owned copy of the ID inside [`request_id`] or [`query_request_id`], or
/// `None` when called outside either middleware scope. Cloning lets an error
/// response retain the ID after the task-local borrow ends.
///
/// # Examples
///
/// A handler reached through [`build_router`] receives `Some(id)`. Startup code
/// calling this function before serving a request receives `None`.
///
/// # Rust Notes for Java/C Engineers
///
/// Tokio task-local storage is analogous to Java `ThreadLocal` in purpose, but
/// follows an async task when that task moves between runtime threads. In C an
/// explicit request-context pointer would normally be threaded through calls.
pub fn current_request_id() -> Option<String> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// Selects the independent token budget charged by an HTTP operation.
///
/// Reads and writes intentionally do not consume one another's burst capacity.
/// The classification is part of server admission control, not a statement
/// about whether the eventual domain operation performs object-store reads or
/// writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitClass {
    /// Query, lookup, or other route classified as read traffic.
    Read,
    /// Mutation or administrative route classified as write traffic.
    Write,
}

impl RateLimitClass {
    /// Returns the stable metrics label for this budget class.
    ///
    /// # Returns
    ///
    /// The static label `"read"` or `"write"`; no allocation occurs.
    ///
    /// # Examples
    ///
    /// A rejected query increments the counter carrying the `"read"` label.
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Carries the trusted client identity from middleware into a handler.
///
/// [`rate_limit`] inserts this value into request extensions after applying
/// trusted-proxy rules. Batch query handling reuses it to charge additional
/// entries to the same bucket instead of reparsing headers.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitIdentity {
    /// Socket peer or rightmost untrusted forwarded address selected by policy.
    pub ip: IpAddr,
}

/// Identifies one client's read or write token bucket.
///
/// The same IP owns two independent entries, one for each [`RateLimitClass`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitKey {
    /// Trusted client address used to partition rate-limit state.
    ip: IpAddr,
    /// Independent read or write budget associated with the address.
    class: RateLimitClass,
}

impl Hash for RateLimitKey {
    /// Feeds both identity components into a hash-map hasher.
    ///
    /// # Parameters
    ///
    /// - `state`: Hasher selected by the surrounding [`DashMap`].
    ///
    /// # Side Effects
    ///
    /// Mutates only the hasher state; the key remains unchanged.
    ///
    /// # Examples
    ///
    /// `203.0.113.7/read` and `203.0.113.7/write` hash as distinct keys even
    /// though they share the same client address.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.class.hash(state);
    }
}

/// Stores the mutable state for one client's token bucket.
///
/// Buckets begin with the configured burst capacity. Refills use elapsed
/// monotonic time, and `consume_rate_limit` updates `last_seen` so idle state
/// can be reclaimed without depending on wall-clock changes.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitBucket {
    /// Whole tokens currently available for an atomic charge.
    tokens: u64,
    /// Monotonic instant from which the next refill is calculated.
    last_refill: Instant,
    /// Monotonic instant of the most recent attempted charge.
    last_seen: Instant,
}

/// Owns the shared service handles injected into every HTTP handler.
///
/// Axum clones this value when composing or extracting state. The clones share
/// service instances through [`Arc`]; they do not clone manifests, disk caches,
/// or background tasks. The plain [`ZeppelinStore`] handle is likewise a cheap
/// clone of the storage abstraction. S3/MinIO and its published manifest remain
/// authoritative; the cache fields here only accelerate access.
///
/// ```text
///                         AppState clone per request
///                                   |
///          +------------------------+-------------------------+
///          |                        |                         |
///          v                        v                         v
/// shared domain/store Arcs   shared cache/config Arcs   admission-control Arcs
///          |                        |                         |
///          +------------------------+-------------------------+
///                                   |
///                                   v
///                      handler borrows the extracted state
/// ```
///
/// [`crate::startup::build_app`] creates the production value. Tests may set
/// [`AppState::namespace_name_prefix`] and use isolated store prefixes, but the
/// router and handlers are otherwise the same production code.
#[derive(Clone)]
pub struct AppState {
    /// Storage abstraction through which handlers reach authoritative S3/MinIO.
    pub store: ZeppelinStore,
    /// Shared wall clock for all correctness-sensitive request paths.
    pub clock: Clock,
    /// Central pure-CPU authorization kernel compiled at startup.
    pub security: Arc<SecurityKernel>,
    /// Transport credential boundary; phase 1 installs the named API-key adapter.
    pub credential_adapter: Arc<dyn CredentialAdapter>,
    /// Domain service for namespace CRUD and authoritative metadata changes.
    pub namespace_manager: Arc<NamespaceManager>,
    /// Optional prefix for server-generated namespace names.
    ///
    /// Production leaves this unset. Test servers use it to keep API-created
    /// namespaces under the same random harness prefix as direct storage keys.
    pub namespace_name_prefix: Option<String>,
    /// Service that writes immutable WAL fragments and publishes visibility.
    pub wal_writer: Arc<WalWriter>,
    /// Service that discovers visible WAL fragments through the manifest.
    pub wal_reader: Arc<WalReader>,
    /// Lease-protected compactor shared by background and manual admin paths.
    pub compactor: Arc<Compactor>,
    /// Per-namespace compaction lease manager.
    pub lease_manager: Arc<LeaseManager>,
    /// Immutable boot-time server, storage, indexing, and compaction settings.
    pub config: Arc<Config>,
    /// Trusted proxy CIDRs parsed once at startup for rate-limit client-IP resolution.
    pub trusted_proxies: Arc<[IpCidr]>,
    /// Atomically replaceable query settings read as consistent snapshots.
    pub runtime_query_config: Arc<RuntimeQueryConfig>,
    /// Boot-time validation bounds for runtime query knob updates.
    pub query_knob_bounds: QueryKnobBounds,
    /// Disposable LRU disk cache for immutable segment data.
    pub cache: Arc<DiskCache>,
    /// Disposable in-memory manifest cache with a bounded freshness policy.
    pub manifest_cache: Arc<ManifestCache>,
    /// Optional background warm-set hydrator.
    pub hydrator: Option<Arc<SegmentHydrator>>,
    /// Disposable in-memory cache for WAL-level full-text search indexes.
    pub fts_cache: Arc<WalFtsCache>,
    /// Bounded disposable memo of decoded immutable WAL fragments.
    pub fragment_cache: Arc<WalFragmentCache>,
    /// Bounded disposable memo of decoded immutable segment FTS artifacts.
    pub decoded_artifact_cache: Arc<DecodedArtifactCache>,
    /// Non-blocking admission semaphore for in-flight query handlers.
    pub query_semaphore: Arc<Semaphore>,
    /// Concurrent, process-local token buckets keyed by client and traffic class.
    pub rate_limiters: Arc<DashMap<RateLimitKey, RateLimitBucket>>,
}

/// Records HTTP response counts, latency, and structured request context.
///
/// The metrics label uses Axum's normalized [`MatchedPath`] rather than the raw
/// URI, preventing namespace and vector identifiers from creating unbounded
/// Prometheus label cardinality. The structured log still includes the raw path
/// for diagnosis. Only requests that reach this inner middleware are counted;
/// an outer body-limit or rate-limit rejection may return before this function
/// runs.
///
/// # Parameters
///
/// - `addr`: Socket peer supplied by Axum's connect-info service.
/// - `matched_path`: Normalized route template, or `None` for an unmatched path.
/// - `request`: Owned request forwarded to the next service exactly once.
/// - `next`: Remaining middleware and handler stack.
///
/// # Returns
///
/// The downstream response, unchanged.
///
/// # Side Effects
///
/// Increments `HTTP_REQUESTS_TOTAL` after downstream completion and emits one
/// structured `request` log with peer IP, method, raw path, status, latency,
/// and the task-local request ID when available.
///
/// # Performance
///
/// Adds one monotonic timer, small string allocations for metric labels and
/// logging, and a Prometheus counter update. It performs no storage I/O.
///
/// # Examples
///
/// Requests for `/v1/namespaces/books` and `/v1/namespaces/movies` are logged
/// with their concrete paths but share the same route-pattern metric label.
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

/// Correlates a non-query request, its logs, and its response with one ID.
///
/// A text-compatible incoming `x-request-id` is preserved; otherwise the
/// server generates a UUID v4. Downstream execution occurs inside both a Tokio
/// task-local scope and a tracing span, and the selected value is echoed in the
/// response header.
///
/// # Parameters
///
/// - `request`: Owned request whose optional header supplies the correlation ID.
/// - `next`: Remaining middleware and handler stack.
///
/// # Returns
///
/// The downstream response with `x-request-id` inserted or replaced.
///
/// # Panics
///
/// Header insertion assumes the chosen ID can be represented as an HTTP header
/// value. That is guaranteed for a UUID and for a value already accepted by
/// `HeaderValue::to_str`; a panic would indicate that invariant changed.
///
/// # Side Effects
///
/// Generates randomness when no usable ID was supplied and creates an INFO
/// tracing span around downstream work. It performs no domain or storage I/O.
///
/// # Examples
///
/// A request carrying `x-request-id: import-42` receives the same response
/// header, and handler errors include `"request_id":"import-42"`. A request
/// without that header receives a generated UUID instead.
///
/// # Rust Notes for Java/C Engineers
///
/// `REQUEST_ID.scope` binds data to the async future, and `.instrument` binds a
/// tracing span to that future. Unlike a Java thread-local or C thread-local,
/// both remain correct when Tokio polls the future on different worker threads.
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

/// Correlates a query request without creating the general-purpose trace span.
///
/// The selection and response-header contract matches [`request_id`], but the
/// query handler performs its own targeted instrumentation. Avoiding an extra
/// span and `TraceLayer` traversal keeps the hot path lightweight while still
/// making [`current_request_id`] available to errors and explicit query logs.
///
/// # Parameters
///
/// - `request`: Owned query request and optional client correlation header.
/// - `next`: Remaining query middleware and handler stack.
///
/// # Returns
///
/// The downstream response with the chosen `x-request-id` header.
///
/// # Panics
///
/// As in [`request_id`], insertion relies on the generated or previously parsed
/// ID being a valid HTTP header value.
///
/// # Examples
///
/// A nearest-neighbor query with no ID gets a generated ID in both a canonical
/// error envelope and the response header, without a router-level trace span.
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

/// Rewrites selected bare middleware errors into the canonical JSON envelope.
///
/// Tower body-limit and timeout layers can produce responses before a handler
/// creates an [`ApiError`]. Because [`build_router`] installs this middleware
/// outermost, it sees those responses on the way out. Statuses recognized by
/// [`handlers::envelope_for_status`] are rewritten unless their content type
/// already begins with `application/json`; JSON is treated as evidence that a
/// handler already rendered its intended body.
///
/// ```text
/// downstream response
///        |
///        +-- status not owned ----------------------> unchanged
///        |
///        +-- owned status + application/json ------> unchanged
///        |
///        `-- owned status + bare body
///                       |
///                       v
///             canonical JSON + request ID
/// ```
///
/// # Parameters
///
/// - `request`: Owned request; its incoming ID is retained as a fallback.
/// - `next`: Complete inner router stack.
///
/// # Returns
///
/// Either the original response or a canonical 404, 405, 408, or 413 envelope.
///
/// # Side Effects
///
/// May discard a bare downstream response body and allocate a JSON replacement.
/// It performs no storage work and emits no additional error log.
///
/// # Examples
///
/// If `RequestBodyLimitLayer` rejects a four-megabyte body with status 413,
/// this function replaces Tower's plain response with code
/// `PAYLOAD_TOO_LARGE`. A handler-produced JSON 404 remains unchanged so its
/// more specific `NAMESPACE_NOT_FOUND` code is preserved.
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

/// Resolve a credential or explicit anonymous identity before authorization.
pub async fn authenticate(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(matched_path) = request.extensions().get::<MatchedPath>() else {
        return ApiError(SecurityError::UnmappedRoute.into()).into_response();
    };
    let Some(class) = classify_route(
        request.method(),
        matched_path.as_str(),
        state.config.security.readyz_public,
    ) else {
        return ApiError(SecurityError::UnmappedRoute.into()).into_response();
    };

    if class == RouteClass::Public {
        request.extensions_mut().insert(Principal::anonymous());
        return next.run(request).await;
    }

    let request_id = match required_security_request_id() {
        Ok(request_id) => request_id,
        Err(error) => return ApiError(error.into()).into_response(),
    };
    let context = RequestContext::at(request_id, state.clock.now());

    if state.config.security.mode == crate::config::SecurityMode::OpenUnsafe {
        request.extensions_mut().insert(Principal::anonymous());
        request.extensions_mut().insert(context);
        return next.run(request).await;
    }

    match state
        .credential_adapter
        .authenticate(request.headers(), context.now)
    {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            request.extensions_mut().insert(context);
            next.run(request).await
        }
        Err(failure) => ApiError(SecurityError::Authentication(failure).into()).into_response(),
    }
}

fn required_security_request_id() -> Result<String, SecurityError> {
    current_request_id().ok_or(SecurityError::MissingRequestContext)
}

/// Invoke the central route map and kernel before any protected handler.
pub async fn authorize(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(matched_path) = request.extensions().get::<MatchedPath>() else {
        return ApiError(SecurityError::UnmappedRoute.into()).into_response();
    };
    let matched_path = matched_path.as_str().to_string();
    let Some(class) = classify_route(
        request.method(),
        &matched_path,
        state.config.security.readyz_public,
    ) else {
        return ApiError(SecurityError::UnmappedRoute.into()).into_response();
    };
    let RouteClass::Protected(action) = class else {
        return next.run(request).await;
    };
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
        return ApiError(SecurityError::MissingPrincipal.into()).into_response();
    };
    let resource = match route_resource(&matched_path, request.uri().path()) {
        Ok(resource) => resource,
        Err(error) => return ApiError(error.into()).into_response(),
    };
    let Some(context) = request.extensions().get::<RequestContext>().cloned() else {
        return ApiError(SecurityError::MissingRequestContext.into()).into_response();
    };

    let allow = match state
        .security
        .authorize(&principal, action, &resource, &context)
    {
        Decision::Allow(allow) => allow,
        Decision::Deny(deny) => {
            return ApiError(SecurityError::Authorization(deny.reason).into()).into_response();
        }
    };

    if action == Action::NamespaceClone {
        for (required_action, required_resource) in [
            (Action::NamespaceRead, resource.clone()),
            (Action::NamespaceCreate, Resource::System),
        ] {
            if let Decision::Deny(deny) =
                state
                    .security
                    .authorize(&principal, required_action, &required_resource, &context)
            {
                return ApiError(SecurityError::Authorization(deny.reason).into()).into_response();
            }
        }
    }

    request.extensions_mut().insert::<AllowDecision>(allow);
    next.run(request).await
}

fn route_resource(matched_path: &str, request_path: &str) -> Result<Resource, SecurityError> {
    if matched_path == "/v1/config/query" {
        return Ok(Resource::RuntimeConfig);
    }
    if matched_path == "/v1/namespaces" {
        return Ok(Resource::System);
    }
    if !matched_path.starts_with("/v1/namespaces/:ns") {
        return Ok(Resource::System);
    }

    let segments: Vec<_> = request_path.trim_matches('/').split('/').collect();
    let namespace = segments
        .get(2)
        .ok_or(SecurityError::InvalidNamespaceId)
        .and_then(|namespace| NamespaceId::new((*namespace).to_string()))?;
    if matched_path == "/v1/namespaces/:ns/snapshots/:name" {
        let snapshot = segments
            .get(4)
            .ok_or(SecurityError::InvalidSnapshotName)
            .and_then(|name| SnapshotName::new((*name).to_string()))?;
        Ok(Resource::Snapshot(namespace, snapshot))
    } else {
        Ok(Resource::Namespace(namespace))
    }
}

/// Authorize a body-derived namespace target before any domain or storage I/O.
///
/// Create and clone targets live in JSON rather than the matched URL, so route
/// middleware can prove the action grant but cannot apply the namespace scope.
/// Their handlers call this same central kernel immediately after extraction
/// and before touching the namespace manager or object store.
pub(crate) fn authorize_namespace_action(
    state: &AppState,
    principal: &Principal,
    context: &RequestContext,
    action: Action,
    namespace: &str,
) -> Result<AllowDecision, SecurityError> {
    let resource = Resource::Namespace(NamespaceId::new(namespace.to_string())?);
    match state
        .security
        .authorize(principal, action, &resource, context)
    {
        Decision::Allow(allow) => Ok(allow),
        Decision::Deny(deny) => Err(SecurityError::Authorization(deny.reason)),
    }
}

/// Rejects a declared oversized request while continuing to drain its body.
///
/// [`RequestBodyLimitLayer`] returns immediately when `Content-Length` exceeds
/// its limit. On HTTP/1, dropping that unread request body can reset the socket
/// while the client is still uploading, preventing it from observing the 413
/// response. Retaining and polling the rejected body in a detached task lets
/// the connection finish the upload or notice that the client stopped after
/// receiving the response. Bodies without a trustworthy declared length still
/// flow through the streaming limit layer below this middleware.
async fn reject_oversized_content_length(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let body_limit = state.config.server.max_request_body_mb * 1024 * 1024;
    let is_oversized = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > body_limit);
    if !is_oversized {
        return next.run(request).await;
    }

    let body = request.into_body();
    tokio::spawn(async move {
        let mut stream = body.into_data_stream();
        while let Some(frame) = stream.next().await {
            if let Err(error) = frame {
                tracing::debug!(
                    error = %error,
                    "oversized request body drain stopped before end of stream"
                );
                break;
            }
        }
    });

    handlers::render_status_envelope(axum::http::StatusCode::PAYLOAD_TOO_LARGE, None)
}

/// Admits a query only when an in-flight semaphore permit is immediately free.
///
/// This is load shedding, not a waiting queue. The permit remains alive across
/// the downstream `.await` and is released by RAII on completion, cancellation,
/// or timeout. Exhaustion returns the canonical 503 `CONCURRENCY_LIMIT` error
/// with a one-second retry hint.
///
/// # Parameters
///
/// - `state`: Shared application state containing the query semaphore.
/// - `request`: Owned query request to run after admission.
/// - `next`: Query handler stack protected by the permit.
///
/// # Returns
///
/// The downstream response when admitted, or an immediate 503 response when no
/// permit is available.
///
/// # Side Effects
///
/// Temporarily decrements the semaphore's available-permit count. It does not
/// spawn work or mutate authoritative data itself.
///
/// # Examples
///
/// With a limit of 32, the first 32 simultaneous queries run. A 33rd request is
/// rejected rather than queued; once any running future ends, a later request
/// can acquire the returned permit.
///
/// # Rust Notes for Java/C Engineers
///
/// `_permit` is an RAII guard. It resembles a Java `Semaphore` permit released
/// in `finally`, or a C cleanup path that must always call `sem_post`, but Rust
/// runs `Drop` automatically on every normal or cancellation path.
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

/// Represents one validated IPv4 or IPv6 trusted-proxy CIDR.
///
/// Host bits need not be zero in configuration because membership comparison
/// masks both this stored address and the candidate. Address-family mismatches
/// never match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpCidr {
    /// Configured address whose first `prefix` bits define the network.
    network: IpAddr,
    /// Significant leading bits, bounded to 32 for IPv4 or 128 for IPv6.
    prefix: u8,
}

/// Resolves the client address used to partition rate-limit buckets.
///
/// `X-Forwarded-For` is ignored unless `peer_ip` belongs to a configured
/// trusted proxy. For a trusted peer, entries are scanned from right to left;
/// blank, malformed, and still-trusted hops are skipped, and the first
/// untrusted address becomes the client. If no such address exists, the socket
/// peer remains the identity.
///
/// # Parameters
///
/// - `peer_ip`: Address of the TCP peer observed by the server.
/// - `x_forwarded_for`: Raw comma-separated forwarding header, when present and
///   text-compatible.
/// - `trusted_proxies`: CIDRs validated once during startup.
///
/// # Returns
///
/// The peer address or rightmost untrusted forwarded address. The current
/// implementation is infallible and always returns `Ok`; malformed header
/// entries are deliberately ignored rather than treated as configuration
/// failures.
///
/// # Errors
///
/// No error is currently produced. The `Result` keeps the middleware boundary
/// able to propagate future identity-policy failures as [`ZeppelinError`].
///
/// # Examples
///
/// With trusted ranges `127.0.0.1/32` and `10.0.0.0/8`, a loopback peer and
/// header `198.51.100.7, 10.1.2.3, 127.0.0.1` resolve to `198.51.100.7`. The
/// same header from an untrusted peer is ignored, preventing address spoofing.
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

/// Parses all configured trusted-proxy CIDRs before the server accepts traffic.
///
/// # Parameters
///
/// - `values`: Borrowed configuration entries in `address/prefix` form.
///
/// # Returns
///
/// An owned vector in configuration order. An empty input produces an empty
/// vector, which disables all trust in forwarded addresses.
///
/// # Errors
///
/// Returns [`ZeppelinError::Config`] for the first missing slash, invalid IP,
/// non-numeric prefix, or prefix wider than its address family. No partially
/// parsed vector is returned.
///
/// # Examples
///
/// `127.0.0.1/32` and `10.0.0.0/8` become two reusable matchers. `10.0.0.0/33`
/// fails startup validation rather than silently broadening or narrowing trust.
///
/// # Rust Notes for Java/C Engineers
///
/// The iterator pipeline borrows every input string and uses `collect` over
/// `Result`. Rust stops at the first error and returns it; successful parsed
/// values accumulated before that point are dropped automatically. Java often
/// expresses this with a loop and exception, while C needs explicit cleanup of
/// the partial array.
pub fn parse_trusted_proxies(values: &[String]) -> Result<Vec<IpCidr>, ZeppelinError> {
    values
        .iter()
        .map(|value| parse_ip_cidr(value))
        .collect::<Result<Vec<_>, _>>()
}

/// Parses and validates one trusted-proxy CIDR entry.
///
/// # Parameters
///
/// - `value`: Borrowed `IPv4/prefix` or `IPv6/prefix` text from configuration.
///
/// # Returns
///
/// A compact copyable matcher containing the parsed address and prefix width.
///
/// # Errors
///
/// Returns a configuration error for malformed structure, address, prefix, or
/// address-family bounds. The error includes the offending configuration text.
///
/// # Examples
///
/// `2001:db8::/32` succeeds; `2001:db8::/129` is rejected because IPv6 has only
/// 128 address bits.
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
    /// Reports whether an address belongs to this CIDR's network prefix.
    ///
    /// # Parameters
    ///
    /// - `ip`: Candidate peer or forwarded address.
    ///
    /// # Returns
    ///
    /// `true` when the address family matches and the leading `prefix` bits are
    /// equal; `false` for a mismatch or a different address family.
    ///
    /// # Examples
    ///
    /// `10.4.5.6` belongs to `10.0.0.0/8`; an IPv6 address does not.
    fn contains(self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => ipv4_in_prefix(ip, network, self.prefix),
            (IpAddr::V6(network), IpAddr::V6(ip)) => ipv6_in_prefix(ip, network, self.prefix),
            _ => false,
        }
    }
}

/// Compares the significant leading bits of two IPv4 addresses.
///
/// # Parameters
///
/// - `ip`: Candidate address.
/// - `network`: Configured address that defines the network bits.
/// - `prefix`: Leading-bit count already validated to be at most 32.
///
/// # Returns
///
/// `true` when both masked values match. Prefix zero matches every IPv4 address.
///
/// # Panics
///
/// A prefix greater than 32 can make the mask shift invalid. Callers preserve
/// the bound established by `parse_ip_cidr`.
///
/// # Examples
///
/// `192.0.2.99` matches `192.0.2.0/24` but not `192.0.3.0/24`.
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

/// Compares the significant leading bits of two IPv6 addresses.
///
/// # Parameters
///
/// - `ip`: Candidate address.
/// - `network`: Configured address that defines the network bits.
/// - `prefix`: Leading-bit count already validated to be at most 128.
///
/// # Returns
///
/// `true` when both masked values match. Prefix zero matches every IPv6 address.
///
/// # Panics
///
/// A prefix greater than 128 can make the mask shift invalid. Callers preserve
/// the bound established by `parse_ip_cidr`.
///
/// # Examples
///
/// `2001:db8::1` matches `2001:db8::/32`; `2001:db9::1` does not.
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

/// Applies the route-level, per-client token-bucket admission policy.
///
/// The function classifies the request, resolves a spoof-resistant identity,
/// stores that identity in request extensions, and charges one token. Read and
/// write traffic use independent buckets. Health, readiness, and metrics return
/// `None` from route classification and pass through without a bucket.
///
/// ```text
/// method + path ----> exempt ------------------------------> handler
///       |
///       v
/// peer + trusted XFF -> client IP -> read/write bucket -> token available?
///                                                    | yes       | no
///                                                    v           v
///                                                 handler    JSON 429
/// ```
///
/// # Parameters
///
/// - `state`: Shared configuration, trusted CIDRs, metrics, and bucket map.
/// - `addr`: Socket peer supplied by the connection service.
/// - `request`: Owned request whose method, path, headers, and extensions may be
///   inspected or updated.
/// - `next`: Remaining middleware and route handler.
///
/// # Returns
///
/// The downstream response when exempt or admitted; otherwise a canonical 429
/// response carrying `Retry-After`.
///
/// # Side Effects
///
/// Creates, refills, charges, or evicts process-local buckets; inserts
/// [`RateLimitIdentity`] into admitted non-exempt requests; and records/logs a
/// rejection. It never writes object storage.
///
/// # Consistency
///
/// Bucket state is local to one Zeppelin process and is not a distributed
/// quota. Restarting a stateless node resets its buckets. This admission state
/// has no authority over manifests or namespace data.
///
/// # Examples
///
/// A write client with burst capacity 100 can issue 100 immediate namespace
/// mutations. The next write receives 429 until tokens refill. A readiness
/// probe from the same address remains uncharged.
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

/// Atomically charges one client's selected rate-limit bucket.
///
/// Buckets start full, refill in whole tokens according to monotonic elapsed
/// time, and never exceed the configured burst. The charge is all-or-nothing.
/// Batch query invokes this after deserialization so a request with `N` entries
/// costs `N` tokens: [`rate_limit`] already charged one, and the handler charges
/// `N - 1` here.
///
/// # Parameters
///
/// - `state`: Shared configuration and concurrent bucket map.
/// - `ip`: Trusted client identity established by middleware.
/// - `class`: Independent read or write budget to charge.
/// - `tokens_to_consume`: Additional whole tokens required by the operation.
///
/// # Returns
///
/// `Ok(())` when charging is disabled, zero tokens were requested, or the full
/// charge succeeds.
///
/// # Errors
///
/// Returns [`ZeppelinError::RateLimitExceeded`] when the bucket lacks the full
/// requested amount. No tokens are deducted on that failure. The error carries
/// the computed whole-second retry hint.
///
/// # Side Effects
///
/// Scans out idle buckets, then creates or mutates one [`DashMap`] entry. A
/// rejection increments `RATE_LIMITED_TOTAL` and emits a structured warning.
///
/// # Performance
///
/// Charging one bucket is expected constant time, but idle eviction calls
/// `DashMap::retain` and therefore scans all current buckets on every nonzero,
/// enabled charge. No network or object-store request occurs.
///
/// # Examples
///
/// If a read bucket has 10 tokens and a batch needs 4 additional tokens, the
/// function succeeds and leaves 6. If it needs 11, the function returns 429
/// metadata and leaves all 10 available.
///
/// # Rust Notes for Java/C Engineers
///
/// `DashMap::entry` provides a scoped mutable guard for one key. The guard is
/// dropped before this function performs the later lookup and logging. Rust's
/// guard lifetime prevents retaining an unlocked raw pointer to the bucket, a
/// hazard that would need manual discipline with a C hash table and lock.
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

/// Selects the sustained rate and burst capacity for a traffic class.
///
/// # Parameters
///
/// - `config`: Borrowed immutable boot configuration.
/// - `class`: Read or write budget selector.
///
/// # Returns
///
/// `(tokens_per_second, maximum_tokens)` converted to `u64` for arithmetic.
///
/// # Examples
///
/// With read settings `100/200`, [`RateLimitClass::Read`] returns `(100, 200)`.
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

/// Estimates the whole-second delay needed to refill a token deficit.
///
/// # Parameters
///
/// - `available`: Tokens currently present in the bucket.
/// - `requested`: Tokens required by the rejected atomic charge.
/// - `rps`: Positive refill rate in tokens per second.
///
/// # Returns
///
/// The deficit divided by the refill rate, rounded up and clamped to at least
/// one second.
///
/// # Panics
///
/// Division requires `rps > 0`. [`consume_rate_limit`] returns before calling
/// this helper when the configured rate is zero.
///
/// # Examples
///
/// With 2 available, 12 requested, and 4 tokens per second, the hint is
/// `ceil(10 / 4) = 3` seconds.
fn retry_after_secs(available: u64, requested: u64, rps: u64) -> u64 {
    let deficit = requested.saturating_sub(available).max(1);
    deficit.div_ceil(rps).max(1)
}

/// Classifies an HTTP method and path for route-level token charging.
///
/// # Parameters
///
/// - `method`: Request method after HTTP parsing.
/// - `path`: Raw URI path without the query string.
///
/// # Returns
///
/// `None` for `/healthz`, `/readyz`, and `/metrics`; read class for every GET,
/// query endpoint, or vector-get endpoint; write class for everything else.
///
/// # Examples
///
/// `POST /v1/namespaces/books/query` is read traffic even though it uses POST;
/// `DELETE /v1/namespaces/books` is write traffic.
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

/// Removes token buckets whose clients have been inactive for the configured TTL.
///
/// # Parameters
///
/// - `rate_limiters`: Shared process-local bucket map.
/// - `now`: One monotonic instant used for every entry in this eviction pass.
/// - `idle_ttl`: Maximum permitted duration since `last_seen`.
///
/// # Side Effects
///
/// Deletes entries with age greater than or equal to the TTL. Fresh entries and
/// their token counts remain unchanged.
///
/// # Performance
///
/// Scans the map and briefly locks shards as required by [`DashMap::retain`].
///
/// # Examples
///
/// With a ten-second TTL, a bucket last seen 60 seconds ago is removed while a
/// bucket touched now remains available.
fn evict_idle_rate_limiters(
    rate_limiters: &DashMap<RateLimitKey, RateLimitBucket>,
    now: Instant,
    idle_ttl: Duration,
) {
    rate_limiters.retain(|_, bucket| now.duration_since(bucket.last_seen) < idle_ttl);
}

/// Apply authentication and central authorization only to registered methods.
///
/// `MethodRouter::route_layer` deliberately leaves Axum's method-not-allowed
/// fallback unwrapped. That preserves canonical 405 responses while ensuring
/// every actual handler, including Axum's implicit HEAD dispatch for GET, runs
/// through the same route map before domain code.
fn secure_route(methods: MethodRouter<AppState>, state: &AppState) -> MethodRouter<AppState> {
    methods
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authorize,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authenticate,
        ))
}

/// Builds the complete Axum service from initialized Zeppelin dependencies.
///
/// Query routes and all other routes are composed separately, then merged.
/// Queries receive dedicated concurrency admission and a lighter request-ID
/// path; other routes receive Tower HTTP tracing. Both groups enforce the same
/// body size, timeout, rate-limit, metrics, and canonical-error policies.
///
/// Layer order is security- and observability-sensitive. Axum runs the last
/// attached layer first on requests. The effective request paths are:
///
/// ```text
/// query:
/// normalize -> query ID -> body limits -> timeout -> rate limit
///           -> HTTP metrics -> authn -> authz -> concurrency permit
///           -> query handler
///
/// other:
/// normalize -> request ID -> TraceLayer -> body limits -> timeout
///           -> rate limit -> HTTP metrics -> authn -> authz
///           -> endpoint handler
/// ```
///
/// # Parameters
///
/// - `state`: Fully initialized application state. Configuration has already
///   passed startup validation, and background-service ownership remains with
///   [`crate::startup::build_app`].
///
/// # Returns
///
/// An owned [`Router`] ready for `into_make_service_with_connect_info` or the
/// test harness. The router owns a cloneable shared state handle; it does not
/// return separate service instances per route.
///
/// # Side Effects
///
/// Router construction allocates route and middleware service structures but
/// performs no network bind, object-store request, or background-task spawn.
/// The optional profiling route is included only with the `profiling` feature.
///
/// # Consistency
///
/// This function establishes HTTP policy only. Handler services remain
/// responsible for manifest authority, immutable artifact publication, lease
/// fencing, and cache-as-optimization rules.
///
/// # Examples
///
/// Production startup passes one [`AppState`] and later serves the returned
/// router with socket connect information. An oversized query can be rejected
/// by the outer body limit before it consumes a rate token or query permit; an
/// admitted query then holds a permit only during handler execution.
///
/// # Rust Notes for Java/C Engineers
///
/// Tower's `Layer` composition is a typed decorator stack. It resembles nested
/// Java servlet filters or C function-pointer wrappers, but generics assemble
/// the chain at compile time. Moving `state` into `with_state` transfers the
/// final owned handle to the router; earlier `state.clone()` calls clone the
/// internal shared handles needed to configure middleware.
pub fn build_router(state: AppState) -> Router {
    let timeout = Duration::from_secs(state.config.server.request_timeout_secs);
    let body_limit = state.config.server.max_request_body_mb * 1024 * 1024;

    // Query route: lightweight middleware (request id, no trace layer/span
    // traversal). The query handler has its own #[instrument] for structured
    // logging.
    let query_routes = Router::new()
        .route(
            "/v1/namespaces/:ns/query",
            secure_route(
                post(query::query_namespace).layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    concurrency_limit,
                )),
                &state,
            ),
        )
        .route(
            "/v1/namespaces/:ns/query/batch",
            secure_route(
                post(query::batch_query_namespace).layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    concurrency_limit,
                )),
                &state,
            ),
        )
        .layer(axum::middleware::from_fn(http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            reject_oversized_content_length,
        ))
        .layer(axum::middleware::from_fn(query_request_id));

    // All other routes: full middleware stack with request tracing.
    #[allow(unused_mut)]
    let mut other_routes = Router::new()
        .route(
            "/healthz",
            secure_route(get(handlers::health_check), &state),
        )
        .route(
            "/readyz",
            secure_route(get(handlers::readiness_check), &state),
        )
        .route(
            "/metrics",
            secure_route(get(handlers::metrics_handler), &state),
        );

    #[cfg(feature = "profiling")]
    {
        other_routes = other_routes.route(
            "/debug/pprof/cpu",
            secure_route(get(handlers::cpu_profile), &state),
        );
    }

    other_routes = other_routes
        .route(
            "/v1/config/query",
            secure_route(
                get(config_handler::get_query_config)
                    .patch(config_handler::update_query_config)
                    .put(config_handler::update_query_config),
                &state,
            ),
        )
        .route(
            "/v1/namespaces",
            secure_route(post(namespace::create_namespace), &state),
        )
        .route(
            "/v1/namespaces/:ns",
            secure_route(
                get(namespace::get_namespace).delete(namespace::delete_namespace),
                &state,
            ),
        )
        .route(
            "/v1/namespaces/:ns/snapshots",
            secure_route(get(namespace::list_snapshots), &state),
        )
        .route(
            "/v1/namespaces/:ns/snapshots/:name",
            secure_route(
                get(namespace::get_snapshot)
                    .put(namespace::put_snapshot)
                    .delete(namespace::delete_snapshot),
                &state,
            ),
        )
        .route(
            "/v1/namespaces/:ns/clone",
            secure_route(post(namespace::clone_namespace), &state),
        )
        .route(
            "/v1/namespaces/:ns/index_config",
            secure_route(patch(namespace::patch_index_config), &state),
        )
        .route(
            "/v1/namespaces/:ns/compact",
            secure_route(post(namespace::compact_namespace), &state),
        )
        .route(
            "/v1/namespaces/:ns/compact/status",
            secure_route(get(namespace::get_compaction_status), &state),
        )
        .route(
            "/v1/namespaces/:ns/hydrate",
            secure_route(post(namespace::trigger_hydration), &state),
        )
        .route(
            "/v1/namespaces/:ns/vectors/get",
            secure_route(post(vectors::get_vectors), &state),
        )
        .route(
            "/v1/namespaces/:ns/vectors",
            secure_route(
                post(vectors::upsert_vectors).delete(vectors::delete_vectors),
                &state,
            ),
        )
        .layer(axum::middleware::from_fn(http_metrics))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ))
        .layer(TimeoutLayer::new(timeout))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            reject_oversized_content_length,
        ))
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
    //! Unit tests for server-local admission helpers that need no storage backend.

    use super::*;

    /// Confirms idle cleanup removes stale clients without disturbing active buckets.
    ///
    /// This catches both inverted TTL comparisons and accidental whole-map
    /// clearing. The test supplies one old read bucket and one fresh write
    /// bucket, then verifies their independent survival outcomes.
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

    #[test]
    fn security_request_context_never_defaults_a_missing_request_id() {
        assert!(matches!(
            required_security_request_id(),
            Err(SecurityError::MissingRequestContext)
        ));
    }
}
