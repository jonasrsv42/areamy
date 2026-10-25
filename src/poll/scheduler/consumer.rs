//! [Consumer] — hands out woken nodes. `!Send`, `!Sync`.
//!
//! Thin handle over [super::core::Scheduler]. Single-consumer
//! invariant: not `Clone`, [Consumer::next] takes `&mut self`.

use super::core::Scheduler;
use crate::poll::marker::NodeId;

use alloc::rc::Rc;
use core::cell::RefCell;

/// The consumer before its arena exists. `!Send`, not `Clone`; [Self::init] makes the
/// [Consumer], once.
pub struct Uninit {
    pub(super) inner: Rc<RefCell<Scheduler>>,
}

impl Uninit {
    /// The graph is ids `0..n`: makes the arena and pushes every id for its first poll. O(n).
    pub fn init(self, n: usize) -> Consumer {
        self.inner.borrow_mut().init(n);
        Consumer { inner: self.inner }
    }
}

pub struct Consumer {
    pub(super) inner: Rc<RefCell<Scheduler>>,
}

impl Consumer {
    /// The next woken id; parks while there is none.
    /// O(1): a `RefCell` borrow (a load, a branch, two stores) and [Scheduler::next].
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> NodeId {
        self.inner.borrow_mut().next()
    }
}

#[cfg(test)]
impl Consumer {
    /// Every id [Self::next] would hand out before it parks, in its order.
    pub(crate) fn woken(&mut self) -> Vec<NodeId> {
        self.inner.borrow_mut().woken()
    }
}
