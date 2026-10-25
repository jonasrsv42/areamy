//! [TimerGuard] — an armed timer owned by a future.

use super::{Timer, TimerKey};

use std::time::Instant;

/// An armed timer owned by a future. Dropping releases the heap slot
/// (a fired key is a no-op), so early resolve, lost `Select` races,
/// and teardown all share one cancellation path.
pub struct TimerGuard {
    timer: Timer,
    key: TimerKey,
}

impl TimerGuard {
    /// Arm `timer` at `deadline`. O(log h), h heap entries: [Timer::schedule_at].
    #[must_use]
    pub fn arm(timer: Timer, deadline: Instant) -> Self {
        let key = timer.schedule_at(deadline);
        Self { timer, key }
    }

    /// The timer that will re-poll the node. O(1).
    pub fn timer(&self) -> &Timer {
        &self.timer
    }

    #[cfg(test)]
    pub(crate) fn key(&self) -> TimerKey {
        self.key
    }
}

impl Drop for TimerGuard {
    /// O(1): [Timer::cancel].
    fn drop(&mut self) {
        self.timer.cancel(self.key);
    }
}
