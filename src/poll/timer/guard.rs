//! [TimerGuard] — an armed timer owned by a future.

use super::{Timer, TimerKey};

/// An armed timer, from [Timer::arm]: dropping it cancels the deadline, so early resolve, a lost
/// [race](fn@crate::poll::race) and teardown share one cancellation path.
pub struct TimerGuard {
    timer: Timer,
    key: TimerKey,
}

impl TimerGuard {
    pub(super) fn new(timer: Timer, key: TimerKey) -> Self {
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
    /// O(1): frees the timer's cell; its heap entry goes once it reaches the top.
    fn drop(&mut self) {
        self.timer.cancel(self.key);
    }
}
