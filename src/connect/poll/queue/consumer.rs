//! [Consumer] — dequeues items from the poll queue. `!Send`, `!Sync`.

use super::core::VyukovQueue;
use crate::connect::poll::marker::NodeId;
use crate::error::Error;

use alloc::sync::Arc;
use core::marker::PhantomData;

/// Dequeues items from the poll queue. `!Send`, `!Sync` —
/// must stay on the async thread.
///
/// Correctness of the lock-free queue depends on Consumer not crossing
/// threads. See `core.rs` module docs.
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{poll_queue, Consumer};
/// fn require_send<T: Send>() {}
/// let (consumer, _, _) = poll_queue();
/// require_send::<Consumer>(); // must not compile
/// ```
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{poll_queue, Consumer};
/// fn require_sync<T: Sync>() {}
/// let (consumer, _, _) = poll_queue();
/// require_sync::<Consumer>(); // must not compile
/// ```
pub struct Consumer {
    pub(super) inner: Arc<VyukovQueue>,
    pub(super) _local: PhantomData<*const ()>,
}

impl Consumer {
    /// Dequeue the next item. Blocks if empty until a [Producer](super::Producer) signals.
    pub fn next(&self) -> Result<NodeId, Error> {
        self.inner.next()
    }
}
