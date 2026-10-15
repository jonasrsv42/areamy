//! [Producer] — cross-thread producer. `Send + Sync + Clone`.

use super::core::VyukovQueue;
use crate::poll::marker::NodeId;

use alloc::sync::Arc;
use core::task::Waker;

/// Cross-thread producer. `Send + Sync + Clone`.
/// Enqueues an item and wakes the consumer.
#[derive(Clone)]
pub struct Producer {
    pub(super) inner: Arc<VyukovQueue>,
    /// Wakes the consumer's parker.
    pub(super) waker: Waker,
}

impl Producer {
    /// Enqueue an item and wake the consumer.
    pub fn push(&self, id: NodeId) {
        // Link first: the wake's lock orders the link before the woken consumer's re-pop.
        self.inner.push(id);
        // Always wake: a consumer that saw the list half-linked and parked waits on this.
        self.waker.wake_by_ref();
    }
}
