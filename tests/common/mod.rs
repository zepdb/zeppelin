#[allow(dead_code)]
pub mod assertions;
#[allow(dead_code)]
pub mod counting;
#[allow(dead_code)]
pub mod fault_injection;
#[allow(dead_code)]
pub mod harness;
#[allow(dead_code)]
pub mod server;
#[allow(dead_code)]
pub mod vectors;

#[allow(dead_code)]
pub fn default_gc_upload_window() -> std::time::Duration {
    std::time::Duration::from_secs(
        zeppelin::config::GcConfig::default().compaction_upload_window_secs,
    )
}
