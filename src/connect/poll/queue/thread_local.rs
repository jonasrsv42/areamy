//! [ThreadLocalProducer] — same-thread producer. `Clone`, `!Send`, `!Sync`.
//!
//! Thin handle over [super::scheduler::Scheduler]. Clone'd by every
//! waker that wants same-thread wake/schedule.

use super::scheduler::Scheduler;
use super::timers::TimerKey;
use crate::connect::poll::marker::NodeId;

use alloc::rc::Rc;
use core::cell::RefCell;
use std::time::Instant;

/// ```compile_fail
/// use areamy::connect::poll::queue::{PollQueue, ThreadLocalProducer};
/// fn require_send<T: Send>() {}
/// let q = PollQueue::new();
/// let (_, local) = q.local();
/// require_send::<ThreadLocalProducer>(); // must not compile
/// ```
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{PollQueue, ThreadLocalProducer};
/// fn require_sync<T: Sync>() {}
/// let q = PollQueue::new();
/// let (_, local) = q.local();
/// require_sync::<ThreadLocalProducer>(); // must not compile
/// ```
#[derive(Clone)]
pub struct ThreadLocalProducer {
    pub(super) inner: Rc<RefCell<Scheduler>>,
}

impl ThreadLocalProducer {
    /// Enqueue a wake. No signal — consumer is on the same thread.
    pub fn push(&self, id: NodeId) {
        self.inner.borrow_mut().push(id);
    }

    /// Register a wake at `deadline` for `node_id`.
    #[must_use]
    pub fn schedule(&self, node_id: NodeId, deadline: Instant) -> TimerKey {
        self.inner.borrow_mut().schedule(node_id, deadline)
    }

    /// Release a timer before it fires. Dead keys are a no-op.
    pub fn cancel(&self, key: TimerKey) {
        self.inner.borrow_mut().cancel(key);
    }
}
