//! Local waker backed by [ThreadLocalProducer].
//!
//! Enqueues a node ID without signaling — the consumer is on the
//! same thread and already awake. `!Send`, `!Sync`.

use crate::poll::marker::NodeId;
use crate::poll::queue::ThreadLocalProducer;
use crate::wake::{LocalWake, LocalWaker};

/// Local wake impl backed by [ThreadLocalProducer].
struct Wake {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl LocalWake for Wake {
    fn wake(&self) {
        self.producer.push(self.id);
    }
}

impl LocalWaker {
    /// Create a local waker for a node ID using the given producer.
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
    use crate::poll::queue::PollQueue;

    #[test]
    fn wake_via_local_waker_drives_consumer() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let waker = LocalWaker::from_producer(7, &local);
        waker.wake();
        assert_eq!(consumer.next().unwrap(), 7);
    }
}
