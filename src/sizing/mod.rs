//! Analytic sizing, cost, and throughput modeling for deployment planning.
//!
//! This module is the library home of the Tier 2 perf-contract prediction
//! engine, promoted out of the test tree so operator tooling (the
//! `zeppelin_advisor` binary) can rank hardware candidates and price
//! deployments without linking test-only instrumentation.
//!
//! - [`model`](crate::sizing::model) — [`model::predict`](crate::sizing::model::predict) turns per-query counters into QPS,
//!   latency percentiles, and dollars; [`model::CalibratedShapeModel`](crate::sizing::model::CalibratedShapeModel)
//!   scales fitted constants to arbitrary dataset shapes.
//! - [`catalog`](crate::sizing::catalog) — the embedded, snapshot-dated cloud hardware and
//!   object-store pricing dataset the advisor ranks candidates from.
//! - [`advisor`](crate::sizing::advisor) — deterministic hardware enumeration, constraint filtering,
//!   prediction, monthly pricing, and ranking for one deployment shape.
//! - [`tuner`](crate::sizing::tuner) — pure hardware-to-configuration tuning rules used after an
//!   operator selects one advisor candidate.
//! - [`emit`](crate::sizing::emit) — commented TOML rendering, real-loader round-trip validation,
//!   intent cross-checks, and refusal to overwrite without explicit force.
//! - [`profiles`](crate::sizing::profiles) — strict TOML profiles describing object-store behavior,
//!   pricing, node fleets, and client populations.
//! - [`lognormal`](crate::sizing::lognormal) — the deterministic lognormal TTFB distribution shared
//!   with the perf-contract latency injector.
//! - [`rows`](crate::sizing::rows) — closed-form stored-bytes-per-row for each quantization,
//!   pinned against the production encoders.
//!
//! The perf-contract suite re-exports these types so its Tier 2 assertions
//! and this module can never drift apart: the calibration (fit) still runs
//! there, against real MinIO measurements, and its tolerances gate any
//! change to the math here.
//!
//! # Calibration provenance
//!
//! The request/byte shape inputs come from a MinIO perf-contract snapshot and
//! are contract-gated against a held-out shape. GT-A and GT-B both select the
//! local-MinIO profile. Cloud object-store TTFB and per-connection throughput
//! values are assumed modeling anchors, not measurements made by this project;
//! the advisor banner calls out the S3 TTFB assumption explicitly.

pub mod advisor;
pub mod catalog;
pub mod emit;
pub mod lognormal;
pub mod model;
pub mod profiles;
pub mod rows;
pub mod tuner;
