//! Server-derived request context supplied to authorization.
//!
//! This module owns the small bundle of facts that authorization needs but
//! must never accept from a caller: the request identifier used to correlate
//! an audit record, and the single trusted instant that every expiry check in
//! one request evaluates against. It owns no policy, no identity, and no
//! decision logic; it is the neutral clock and correlation carrier that
//! [`super::kernel`] reads while evaluating an [`super::Action`].
//!
//! The security middleware in [`server`](crate::server) constructs one
//! [`RequestContext`] per protected request from the server's injected clock,
//! then places it in the request extensions. [`super::kernel`] re-derives a
//! context at the moment of authorization when it needs to stamp a decision
//! with the instant that decision became effective.
//!
//! ## Why the instant is captured once
//!
//! Credential expiry, delegated-token windows, and preservation holds are all
//! compared against [`RequestContext::now`] rather than against a fresh
//! `Utc::now()` at each comparison site. Capturing the instant once means a
//! single request cannot observe a credential as valid in one check and
//! expired in the next simply because wall-clock time advanced between them.
//! A request is evaluated entirely at one point in time, or not at all.
//!
//! [`RequestContext::at`] exists so tests can pin that instant and assert
//! behavior exactly on either side of an expiry boundary without sleeping.
//!
//! ## Rust concepts used here
//!
//! `impl Into<String>` lets callers pass a `&str` or an owned `String` without
//! the module choosing an allocation strategy for them; the conversion happens
//! once, at the boundary. In Java this resembles an overload taking
//! `CharSequence`, except the conversion cost is explicit and happens exactly
//! where `.into()` is written.

use chrono::{DateTime, Utc};

/// Immutable context for one authorization evaluation.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Canonical request identifier used for tracing and audit.
    pub request_id: String,
    /// Current trusted wall-clock time used for credential expiry.
    pub now: DateTime<Utc>,
}

impl RequestContext {
    /// Construct context using the current UTC wall clock.
    #[must_use]
    pub fn new(request_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            now: Utc::now(),
        }
    }

    /// Construct context at an explicit instant for deterministic tests.
    #[must_use]
    pub fn at(request_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            request_id: request_id.into(),
            now,
        }
    }
}
