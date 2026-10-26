//! Deadline re-polls: a [Timer] re-polls its node once a deadline passes.

pub(crate) mod deadline;
mod guard;
mod key;

pub use guard::TimerGuard;
pub(crate) use key::TimerKey;

use crate::poll::graph::NodeId;
use deadline::DeadlineHeap;

use alloc::rc::Rc;
use core::cell::RefCell;
use std::time::Instant;

/// A poll thread's timers: one heap, shared by its [Timer]s and its consumer. `Clone`, `!Send`.
///
/// Each borrow lasts one call and runs no user code, so borrows never overlap; `expire`'s `wake`
/// must not touch the timers.
#[derive(Clone)]
pub(crate) struct Timers {
    heap: Rc<RefCell<DeadlineHeap>>,
}

impl Timers {
    /// O(1): an `Rc`; the heap allocates on its first timer.
    pub(crate) fn new() -> Self {
        Self {
            heap: Rc::new(RefCell::new(DeadlineHeap::new())),
        }
    }

    /// The earliest live deadline. O(1), plus O(log h) per dead entry dropped off the top.
    pub(crate) fn earliest(&self) -> Option<Instant> {
        self.heap.borrow_mut().peek()
    }

    /// Hands `wake` each expired timer's node, earliest first. O(k log h), k expired; nothing
    /// due: a peek, and a clock read while a timer is live.
    pub(crate) fn expire(&self, mut wake: impl FnMut(NodeId)) {
        let mut heap = self.heap.borrow_mut();
        // `peek` drops dead entries: only a live one is worth a clock read.
        if heap.peek().is_some() {
            let now = Instant::now();
            while let Some(id) = heap.pop(now) {
                wake(id);
            }
        }
    }
}

/// Re-polls one node at a deadline. `Clone`, `!Send`, `!Sync`.
#[derive(Clone)]
pub struct Timer {
    id: NodeId,
    timers: Timers,
}

impl Timer {
    /// O(1): an `Rc` clone.
    pub(crate) fn new(id: NodeId, timers: &Timers) -> Self {
        Self {
            id,
            timers: timers.clone(),
        }
    }

    /// Re-poll the node at `deadline`. O(log h), h timers on the thread's heap: a cell and a heap
    /// push; no RMW.
    pub fn schedule_at(&self, deadline: Instant) {
        self.register(deadline);
    }

    /// Re-poll the node at `deadline`, unless the guard drops first. O(log h): as
    /// [Self::schedule_at].
    #[must_use = "dropping the guard cancels the timer"]
    pub fn arm(&self, deadline: Instant) -> TimerGuard {
        TimerGuard::new(self.clone(), self.register(deadline))
    }

    fn register(&self, deadline: Instant) -> TimerKey {
        self.timers.heap.borrow_mut().register(self.id, deadline)
    }

    /// Release a timer before it fires. Dead keys are a no-op.
    /// O(1): its cell is freed, its heap entry discarded once it reaches the top.
    pub(crate) fn cancel(&self, key: TimerKey) {
        self.timers.heap.borrow_mut().cancel(key);
    }

    /// True iff `other` re-polls the same node from the same heap. Ids alone don't identify a node
    /// on a thread: a nested runtime numbers its nodes from 0 too. O(1): two compares.
    #[cfg(any(test, debug_assertions))]
    pub(crate) fn same_target(&self, other: &Timer) -> bool {
        self.id == other.id && Rc::ptr_eq(&self.timers.heap, &other.timers.heap)
    }
}

/// Timer mocks for tests — this crate's, and downstream crates' via the `testing` feature
/// (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::{Timer, Timers};

    /// A timer on a heap nobody drains: deadlines register, nothing fires.
    pub fn noop_timer() -> Timer {
        Timer::new(0, &Timers::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::scheduler::Scheduler;
    use crate::tests::support::{DEADLINE, deadline};
    use core::time::Duration;

    #[test]
    fn schedule_at_drives_consumer() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, local, _, _) = Scheduler::started(43);
        local
            .context(42)
            .timer
            .schedule_at(Instant::now() - Duration::from_millis(50));
        // An expired deadline comes out as the node's id.
        assert_eq!(consumer.next(), 42);
    }

    #[test]
    fn a_dropped_guard_cancels_its_timer() {
        let (mut consumer, local, _, _) = Scheduler::started(4);
        drop(
            local
                .context(3)
                .timer
                .arm(Instant::now() - Duration::from_millis(50)),
        );
        assert!(consumer.woken().is_empty());
    }

    #[test]
    fn same_target_needs_same_node_and_heap() {
        let timers = Timers::new();
        let timer = Timer::new(1, &timers);
        assert!(timer.same_target(&Timer::new(1, &timers)));
        assert!(!timer.same_target(&Timer::new(2, &timers)));
        assert!(!timer.same_target(&Timer::new(1, &Timers::new())));
    }
}
