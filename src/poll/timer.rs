//! Deadline re-polls: a [Timer] re-polls its node once a deadline passes.

mod guard;
mod key;

pub use guard::TimerGuard;
pub use key::TimerKey;
pub(crate) use key::{Generation, SlotId};

use crate::poll::marker::NodeId;
use crate::poll::queue::ThreadLocalProducer;

use std::time::Instant;

/// Re-polls one node at a deadline. `Clone`, `!Send`, `!Sync`.
#[derive(Clone)]
pub struct Timer {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl Timer {
    pub fn new(id: NodeId, producer: &ThreadLocalProducer) -> Self {
        Self {
            id,
            producer: producer.clone(),
        }
    }

    /// Re-poll the node at `deadline`. The key cancels the timer; dropping it means the timer
    /// just fires.
    #[must_use]
    pub fn schedule_at(&self, deadline: Instant) -> TimerKey {
        self.producer.schedule(self.id, deadline)
    }

    /// Release a timer before it fires. Dead keys are a no-op.
    pub fn cancel(&self, key: TimerKey) {
        self.producer.cancel(key);
    }

    /// True iff `other` re-polls the same node on the same queue.
    pub fn same_target(&self, other: &Timer) -> bool {
        self.id == other.id && self.producer.same_queue(&other.producer)
    }
}

/// Timer mocks for tests — this crate's, and downstream crates' via the `testing` feature
/// (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::Timer;
    use crate::poll::queue::PollQueue;

    /// A timer on a private queue nobody drains: keys are real and deadlines register, nothing
    /// fires.
    pub fn noop_timer() -> Timer {
        let (_consumer, producer) = PollQueue::new().local();
        Timer::new(0, &producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::queue::PollQueue;
    use std::time::Duration;

    #[test]
    fn schedule_at_drives_consumer() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let timer = Timer::new(42, &local);
        let past = Instant::now() - Duration::from_millis(50);
        let _ = timer.schedule_at(past);
        // An expired deadline comes out as the node's id.
        assert_eq!(consumer.next().unwrap(), 42);
    }

    #[test]
    fn same_target_needs_same_node_and_queue() {
        let (_, local) = PollQueue::new().local();
        let (_, other) = PollQueue::new().local();
        let timer = Timer::new(1, &local);
        assert!(timer.same_target(&Timer::new(1, &local)));
        assert!(!timer.same_target(&Timer::new(2, &local)));
        assert!(!timer.same_target(&Timer::new(1, &other)));
    }
}
