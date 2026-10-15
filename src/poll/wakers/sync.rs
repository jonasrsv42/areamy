//! Sync (cross-thread) waker for async poll runtime.
//!
//! `Waker` implements [alloc::task::Wake] — enqueues a node ID via
//! [crate::poll::queue::Producer], which wakes the consumer.

use crate::poll::marker::NodeId;
use crate::poll::queue::Producer;

use alloc::sync::Arc;
use alloc::task::Wake;

/// Sync waker backed by [Producer]. `Send + Sync`.
///
/// When woken, enqueues the node ID and wakes the consumer.
pub(crate) struct Waker {
    id: NodeId,
    producer: Producer,
}

impl Waker {
    pub(crate) fn task(id: NodeId, producer: &Producer) -> core::task::Waker {
        core::task::Waker::from(Arc::new(Self {
            id,
            producer: producer.clone(),
        }))
    }
}

impl Wake for Waker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // The push also wakes the consumer.
        self.producer.push(self.id);
    }
}
