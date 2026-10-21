//! [Consumer] — dequeues nodes from the poll queue. `!Send`, `!Sync`.
//!
//! Thin handle over [super::scheduler::Scheduler]. Single-consumer
//! invariant: not `Clone`, [Consumer::next] takes `&mut self`.

use super::core::Scheduler;
use crate::poll::marker::NodeId;

use alloc::rc::Rc;
use core::cell::RefCell;

/// ```compile_fail
/// use areamy::poll::scheduler::{PollQueue, Consumer};
/// fn require_send<T: Send>() {}
/// let q = PollQueue::new();
/// let (consumer, _) = q.local();
/// require_send::<Consumer>(); // must not compile
/// ```
///
/// ```compile_fail
/// use areamy::poll::scheduler::{PollQueue, Consumer};
/// fn require_sync<T: Sync>() {}
/// let q = PollQueue::new();
/// let (consumer, _) = q.local();
/// require_sync::<Consumer>(); // must not compile
/// ```
pub struct Consumer {
    pub(super) inner: Rc<RefCell<Scheduler>>,
}

impl Consumer {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> NodeId {
        self.inner.borrow_mut().next()
    }
}
