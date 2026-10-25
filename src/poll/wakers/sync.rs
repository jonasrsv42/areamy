//! Sync (cross-thread) waker for async poll runtime.
//!
//! `Waker` implements [alloc::task::Wake] — wakes a node ID through the
//! poll thread's `Producer`, from any thread.

use crate::poll::marker::NodeId;
use crate::poll::scheduler::Producer;

use alloc::sync::Arc;
use alloc::task::Wake;

/// Sync waker backed by [Producer]. `Send + Sync`.
pub(crate) struct Waker {
    id: NodeId,
    producer: Producer,
}

impl Waker {
    /// O(1): an `Arc` for the waker, and the producer's two `Arc` clones.
    pub(crate) fn task(id: NodeId, producer: &Producer) -> core::task::Waker {
        core::task::Waker::from(Arc::new(Self {
            id,
            producer: producer.clone(),
        }))
    }
}

impl Wake for Waker {
    /// O(1): [Self::wake_by_ref].
    #[inline]
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// O(1): a [Producer::push].
    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.producer.push(self.id);
    }
}
