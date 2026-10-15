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
    /// Arm `timer` at `deadline`.
    #[must_use]
    pub fn arm(timer: Timer, deadline: Instant) -> Self {
        let key = timer.schedule_at(deadline);
        Self { timer, key }
    }

    /// The timer that will re-poll the node.
    pub fn timer(&self) -> &Timer {
        &self.timer
    }

    #[cfg(test)]
    pub(crate) fn key(&self) -> TimerKey {
        self.key
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        self.timer.cancel(self.key);
    }
}
