//! Explicit wall-clock construction seam.
//!
//! Production components use [`Clock::system`]. Tests can provide a custom
//! [`TimeSource`] at construction time without relying on global mutable state.

use std::sync::Arc;

use chrono::{DateTime, Utc};

/// Supplies the current wall-clock timestamp.
///
/// Implementations must be safe to share across asynchronous tasks. The
/// production implementation reads the system wall clock; tests may inject an
/// offset- or freeze-capable implementation.
pub trait TimeSource: Send + Sync + std::fmt::Debug {
    /// Returns the current wall-clock timestamp.
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug)]
struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Cloneable wall-clock handle threaded through component constructors.
#[derive(Clone, Debug)]
pub struct Clock(Arc<dyn TimeSource>);

impl Clock {
    /// Creates a clock backed by the system wall clock.
    #[must_use]
    pub fn system() -> Self {
        Self(Arc::new(SystemTimeSource))
    }

    /// Creates a clock backed by the supplied source.
    #[must_use]
    pub fn from_source(source: Arc<dyn TimeSource>) -> Self {
        Self(source)
    }

    /// Returns the current wall-clock timestamp from this clock's source.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        self.0.now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FixedTimeSource(DateTime<Utc>);

    impl TimeSource for FixedTimeSource {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    #[test]
    fn system_clock_returns_current_time() {
        let before = Utc::now();
        let observed = Clock::system().now();
        let after = Utc::now();

        assert!(observed >= before);
        assert!(observed <= after);
    }

    #[test]
    fn from_source_uses_injected_time() {
        let Some(expected) = DateTime::from_timestamp(1_700_000_000, 123_000_000) else {
            panic!("fixed test timestamp must be representable");
        };
        let clock = Clock::from_source(Arc::new(FixedTimeSource(expected)));

        assert_eq!(clock.now(), expected);
    }
}
