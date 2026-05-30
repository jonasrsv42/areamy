//! [Consumer] — dequeues nodes from the poll queue. `!Send`, `!Sync`.
//!
//! Thin handle over [super::scheduler::Scheduler]. Single-consumer
//! invariant: not `Clone`, [Consumer::next] takes `&mut self`.

use super::scheduler::Scheduler;
use crate::connect::poll::marker::NodeId;
use crate::error::Error;

use alloc::rc::Rc;
use core::cell::RefCell;

/// ```compile_fail
/// use areamy::connect::poll::queue::{PollQueue, Consumer};
/// fn require_send<T: Send>() {}
/// let q = PollQueue::new();
/// let (consumer, _) = q.local();
/// require_send::<Consumer>(); // must not compile
/// ```
///
/// ```compile_fail
/// use areamy::connect::poll::queue::{PollQueue, Consumer};
/// fn require_sync<T: Sync>() {}
/// let q = PollQueue::new();
/// let (consumer, _) = q.local();
/// require_sync::<Consumer>(); // must not compile
/// ```
pub struct Consumer {
    pub(super) inner: Rc<RefCell<Scheduler>>,
}

impl Consumer {
    pub fn next(&mut self) -> Result<NodeId, Error> {
        self.inner.borrow_mut().next()
    }
}
