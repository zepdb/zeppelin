//! Server-derived request context supplied to authorization.

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
