//! [ThreadLocalProducer] — same-thread producer. `Clone`, `!Send`, `!Sync`.
//!
//! Thin handle over [super::core::Scheduler]: pushes local wakes and registers timers.

use super::core::Scheduler;
use super::treiber::{Pushed, TreiberStack};
use crate::poll::marker::NodeId;
use crate::poll::timer::TimerKey;

use alloc::rc::Rc;
use alloc::sync::Arc;
use core::cell::RefCell;
use std::time::Instant;

#[derive(Clone)]
pub struct ThreadLocalProducer {
    pub(super) inner: Rc<RefCell<Scheduler>>,
    /// The stack local wakes push onto.
    pub(super) stack: Arc<TreiberStack>,
}

impl ThreadLocalProducer {
    /// Pushes `id`. O(1): a [TreiberStack::push].
    #[inline]
    pub(crate) fn push(&self, id: NodeId) -> Pushed {
        self.stack.push(id)
    }

    /// Register a wake at `deadline` for `node_id`. O(log h): [Scheduler::schedule].
    #[must_use]
    pub fn schedule(&self, node_id: NodeId, deadline: Instant) -> TimerKey {
        self.inner.borrow_mut().schedule(node_id, deadline)
    }

    /// Release a timer before it fires. Dead keys are a no-op. O(1): [Scheduler::cancel].
    pub fn cancel(&self, key: TimerKey) {
        self.inner.borrow_mut().cancel(key);
    }

    /// True iff both handles feed the same queue. Node ids alone don't identify a node on a
    /// thread: a nested runtime numbers its nodes from 0 too. O(1): a pointer compare.
    pub(crate) fn same_queue(&self, other: &ThreadLocalProducer) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}
