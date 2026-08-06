//! Analytic sizing, cost, and throughput modeling for deployment planning.
//!
//! This module is the library home of the Tier 2 perf-contract prediction
//! engine, promoted out of the test tree so operator tooling (the
//! `zeppelin_advisor` binary) can rank hardware candidates and price
//! deployments without linking test-only instrumentation.
//!
//! - [`model`] — [`model::predict`] turns per-query counters into QPS,
//!   latency percentiles, and dollars; [`model::CalibratedShapeModel`]
//!   scales fitted constants to arbitrary dataset shapes.
//! - [`catalog`] — the embedded, snapshot-dated cloud hardware and
//!   object-store pricing dataset the advisor ranks candidates from.
//! - [`advisor`] — deterministic hardware enumeration, constraint filtering,
//!   prediction, monthly pricing, and ranking for one deployment shape.
//! - [`tuner`] — pure hardware-to-configuration tuning rules used after an
//!   operator selects one advisor candidate.
//! - [`emit`] — commented TOML rendering, real-loader round-trip validation,
//!   intent cross-checks, and refusal to overwrite without explicit force.
//! - [`profiles`] — strict TOML profiles describing object-store behavior,
//!   pricing, node fleets, and client populations.
//! - [`lognormal`] — the deterministic lognormal TTFB distribution shared
//!   with the perf-contract latency injector.
//! - [`rows`] — closed-form stored-bytes-per-row for each quantization,
//!   pinned against the production encoders.
//!
//! The perf-contract suite re-exports these types so its Tier 2 assertions
//! and this module can never drift apart: the calibration (fit) still runs
//! there, against real MinIO measurements, and its tolerances gate any
//! change to the math here.

pub mod advisor;
pub mod catalog;
pub mod emit;
pub mod lognormal;
pub mod model;
pub mod profiles;
pub mod rows;
pub mod tuner;
