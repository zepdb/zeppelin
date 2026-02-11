use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;

use crate::metrics::HTTP_REQUESTS_TOTAL;

/// Middleware that increments `HTTP_REQUESTS_TOTAL` for every response.
///
/// Uses `MatchedPath` to normalize route patterns (avoids unbounded cardinality
/// from namespace names in URLs).
pub async fn http_metrics(
    matched_path: Option<MatchedPath>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let path = matched_path
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let response = next.run(request).await;
    let status = response.status().as_u16().to_string();
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status])
        .inc();
    response
}

/// Middleware that attaches a request ID to every request.
///
/// - Respects an incoming `x-request-id` header if present.
/// - Otherwise generates a UUID v4.
/// - Creates a tracing span so all downstream logs include the request ID.
/// - Returns the request ID in the response `x-request-id` header.
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
