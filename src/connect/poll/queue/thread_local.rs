//! [ThreadLocalProducer] — same-thread producer. `Clone`, `!Send`, `!Sync`.

use super::core::VyukovQueue;
use crate::connect::poll::marker::NodeId;

use alloc::sync::Arc;
use core::marker::PhantomData;

/// Thread-local producer. `Clone`, `!Send`, `!Sync`.
/// Enqueues without signaling — the consumer is already awake.
///
/// Correctness of the lock-free queue depends on ThreadLocalProducer
/// not crossing threads. See `core.rs` module docs.
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{poll_queue, ThreadLocalProducer};
/// fn require_send<T: Send>() {}
/// let (_, _, local) = poll_queue();
/// require_send::<ThreadLocalProducer>(); // must not compile
/// ```
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{poll_queue, ThreadLocalProducer};
/// fn require_sync<T: Sync>() {}
/// let (_, _, local) = poll_queue();
/// require_sync::<ThreadLocalProducer>(); // must not compile
/// ```
pub struct ThreadLocalProducer {
    pub(super) inner: Arc<VyukovQueue>,
    pub(super) _local: PhantomData<*const ()>,
}

impl Clone for ThreadLocalProducer {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _local: PhantomData,
        }
    }
}

impl ThreadLocalProducer {
    /// Enqueue an item. No signal — consumer is on the same thread.
    pub fn push(&self, id: NodeId) {
        self.inner.push(id);
    }
}
