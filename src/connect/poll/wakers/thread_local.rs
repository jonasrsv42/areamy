//! Thread-local waker backed by [ThreadLocalProducer].
//!
//! Enqueues a node ID without signaling — the consumer is on the
//! same thread and already awake. `!Send`, `!Sync`.

use crate::connect::poll::marker::NodeId;
use crate::connect::poll::queue::ThreadLocalProducer;
use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker};

/// Thread-local wake impl backed by [ThreadLocalProducer].
struct Wake {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl ThreadLocalWake for Wake {
    fn wake(&self) {
        self.producer.push(self.id);
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
