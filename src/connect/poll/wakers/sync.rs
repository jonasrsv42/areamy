//! Sync (cross-thread) waker for async poll runtime.
//!
//! [Waker] implements [alloc::task::Wake] — enqueues a node ID via
//! [Producer](crate::connect::poll::queue::Producer) with signal.

use crate::connect::poll::marker::NodeId;
use crate::connect::poll::queue::Producer;

use alloc::sync::Arc;
use alloc::task::Wake;

/// Sync waker backed by [Producer]. `Send + Sync`.
///
/// When woken, enqueues the node ID and signals the consumer.
pub(crate) struct Waker {
    id: NodeId,
    producer: Producer,
}

impl Waker {
    pub(crate) fn new(id: NodeId, producer: &Producer) -> core::task::Waker {
        core::task::Waker::from(Arc::new(Self {
            id,
            producer: producer.clone(),
        }))
    }
}

impl Wake for Waker {
    fn wake(self: Arc<Self>) {
        if let Err(e) = self.producer.push(self.id) {
            #[cfg(not(feature = "silent"))]
            eprintln!("sync::Waker: failed to enqueue node {}: {}", self.id, e);
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Err(e) = self.producer.push(self.id) {
            #[cfg(not(feature = "silent"))]
            eprintln!("sync::Waker: failed to enqueue node {}: {}", self.id, e);
        }
    }
}
