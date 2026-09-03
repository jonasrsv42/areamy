//! [TimerGuard] — an armed timer owned by a future.

use crate::connect::poll::queue::TimerKey;
use crate::connect::waker::ThreadLocalWaker;

use std::time::Instant;

/// An armed timer owned by a future. Dropping releases the heap slot
/// (a fired key is a no-op), so early resolve, lost `Select` races,
/// and teardown all share one cancellation path.
pub struct TimerGuard {
    local: ThreadLocalWaker,
    key: TimerKey,
}

impl TimerGuard {
    /// Arm a timer at `deadline` via `local`.
    #[must_use]
    pub fn arm(local: &ThreadLocalWaker, deadline: Instant) -> Self {
        Self {
            local: local.clone(),
            key: local.schedule_at(deadline),
        }
    }

    /// The waker this timer will re-poll through.
    pub fn waker(&self) -> &ThreadLocalWaker {
        &self.local
    }

    #[cfg(test)]
    pub(crate) fn key(&self) -> TimerKey {
        self.key
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        self.local.cancel(self.key);
    }
}
