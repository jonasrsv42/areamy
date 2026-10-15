//! Local waker backed by [ThreadLocalProducer].
//!
//! Enqueues a node ID without waking the parker — the consumer is on
//! the same thread and not parked. `!Send`, `!Sync`.

use crate::poll::marker::NodeId;
use crate::poll::queue::ThreadLocalProducer;
use crate::wake::{LocalWake, LocalWaker};

/// Local wake impl backed by [ThreadLocalProducer].
pub(crate) struct Waker {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl Waker {
    /// A local waker for a node ID on the given producer.
    pub(crate) fn local(id: NodeId, producer: &ThreadLocalProducer) -> LocalWaker {
        LocalWaker::new(Self {
            id,
            producer: producer.clone(),
        })
    }
}

impl LocalWake for Waker {
    fn wake(&self) {
        self.producer.push(self.id);
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
        let waker = Waker::local(7, &local);
        waker.wake();
        assert_eq!(consumer.next(), 7);
    }
}
