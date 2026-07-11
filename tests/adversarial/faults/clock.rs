use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use zeppelin::time::TimeSource;

/// Shared wall clock whose offset and frozen state are controlled by a test.
#[derive(Debug)]
pub struct TestClock {
    offset_ms: AtomicI64,
    frozen_at: Mutex<Option<DateTime<Utc>>>,
}

impl Default for TestClock {
    fn default() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
            frozen_at: Mutex::new(None),
        }
    }
}

impl TimeSource for TestClock {
    fn now(&self) -> DateTime<Utc> {
        let frozen = self
            .frozen_at
            .lock()
            .expect("test clock frozen-state mutex poisoned");
        frozen.as_ref().copied().unwrap_or_else(|| {
            Utc::now() + Duration::milliseconds(self.offset_ms.load(Ordering::SeqCst))
        })
    }
}

impl TestClock {
    /// Applies a cumulative signed wall-clock offset.
    pub fn jump(&self, delta_ms: i64) {
        let mut frozen = self
            .frozen_at
            .lock()
            .expect("test clock frozen-state mutex poisoned");
        self.offset_ms.fetch_add(delta_ms, Ordering::SeqCst);
        if let Some(frozen_at) = frozen.as_mut() {
            *frozen_at += Duration::milliseconds(delta_ms);
        }
    }

    /// Freezes time at the current offset-adjusted timestamp.
    pub fn freeze(&self) {
        let mut frozen = self
            .frozen_at
            .lock()
            .expect("test clock frozen-state mutex poisoned");
        if frozen.is_none() {
            *frozen =
                Some(Utc::now() + Duration::milliseconds(self.offset_ms.load(Ordering::SeqCst)));
        }
    }

    /// Resumes offset-adjusted system wall-clock flow.
    pub fn thaw(&self) {
        *self
            .frozen_at
            .lock()
            .expect("test clock frozen-state mutex poisoned") = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jump_freeze_and_thaw_share_one_offset() {
        let clock = TestClock::default();
        clock.jump(30_000);
        let shifted = clock.now();
        assert!(shifted >= Utc::now() + Duration::seconds(29));

        clock.freeze();
        let frozen = clock.now();
        clock.jump(-5_000);
        assert_eq!(clock.now(), frozen - Duration::seconds(5));

        clock.thaw();
        let thawed = clock.now();
        assert!(thawed >= Utc::now() + Duration::seconds(24));
    }
}
