use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus::{Encoder, TextEncoder};

pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.04; charset=utf-8",
        )],
        buf,
    )
}
