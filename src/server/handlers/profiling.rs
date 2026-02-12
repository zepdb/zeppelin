use axum::extract::Query;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ProfileParams {
    /// Duration of CPU profiling in seconds (1-300).
    #[serde(default = "default_seconds")]
    pub seconds: u64,
}

fn default_seconds() -> u64 {
    30
}

/// GET /debug/pprof/cpu?seconds=N
///
/// Samples CPU at 99 Hz for N seconds (clamped 1-300), returns an SVG flamegraph.
pub async fn cpu_profile(Query(params): Query<ProfileParams>) -> Response {
    let seconds = params.seconds.clamp(1, 300);

    // Run the profiler on a blocking thread to avoid starving the async runtime.
    let result = tokio::task::spawn_blocking(move || collect_profile(seconds)).await;

    match result {
        Ok(Ok(svg_bytes)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "image/svg+xml")],
            svg_bytes,
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "CPU profiling failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("profiling failed: {e}"),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "profiling task panicked");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("profiling task panicked: {e}"),
            )
                .into_response()
        }
    }
}

fn collect_profile(seconds: u64) -> Result<Vec<u8>, String> {
    use pprof::protos::Message;

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| format!("failed to start profiler: {e}"))?;

    std::thread::sleep(std::time::Duration::from_secs(seconds));

    let report = guard
        .report()
        .build()
        .map_err(|e| format!("failed to build report: {e}"))?;

    let mut svg_buf = Vec::new();
    report
        .flamegraph(&mut svg_buf)
        .map_err(|e| format!("failed to render flamegraph: {e}"))?;

    // Also log the protobuf size for debugging
    let proto = report
        .pprof()
        .map_err(|e| format!("failed to build pprof proto: {e}"))?;
    let proto_size = proto.encoded_len();
    tracing::info!(
        seconds,
        svg_bytes = svg_buf.len(),
        proto_bytes = proto_size,
        "CPU profile collected"
    );

    Ok(svg_buf)
}
