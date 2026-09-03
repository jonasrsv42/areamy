//! Thread-local waker backed by [ThreadLocalProducer].
//!
//! Enqueues a node ID without signaling — the consumer is on the
//! same thread and already awake. `!Send`, `!Sync`.

use crate::connect::poll::marker::NodeId;
use crate::connect::poll::queue::ThreadLocalProducer;
use crate::connect::poll::queue::TimerKey;
use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker};

use std::time::Instant;

/// Thread-local wake impl backed by [ThreadLocalProducer].
struct Wake {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl ThreadLocalWake for Wake {
    fn wake(&self) {
        self.producer.push(self.id);
    }
    fn schedule_at(&self, deadline: Instant) -> Option<TimerKey> {
        Some(self.producer.schedule(self.id, deadline))
    }
    fn cancel(&self, key: TimerKey) {
        self.producer.cancel(key);
    }
}

impl ThreadLocalWaker {
    /// Create a thread-local waker for a node ID using the given producer.
    pub fn from_producer(id: NodeId, producer: &ThreadLocalProducer) -> Self {
        Self::new(Wake {
            id,
            producer: producer.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::poll::queue::PollQueue;
    use std::time::Duration;

    #[test]
    fn wake_via_thread_local_waker_drives_consumer() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let waker = ThreadLocalWaker::from_producer(7, &local);
        waker.wake();
        assert_eq!(consumer.next().unwrap(), 7);
    }

    #[test]
    fn schedule_at_via_thread_local_waker_drives_consumer() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let waker = ThreadLocalWaker::from_producer(42, &local);
        let past = Instant::now() - Duration::from_millis(50);
        let _ = waker.schedule_at(past);
        // schedule_at routes through Wake → ThreadLocalProducer →
        // Scheduler::schedule, pushing the sentinel + heap entry.
        // Consumer's next() picks up the expired entry.
        assert_eq!(consumer.next().unwrap(), 42);
    }
}
