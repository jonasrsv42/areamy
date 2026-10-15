//! Waker types for areamy's poll system.
//!
//! [LocalWaker] is a `!Send` waker for same-thread wake.
//!
//! [Waker] bundles a sync waker (for I/O, standard futures) with a
//! local waker (for cheap same-thread wake) and the node's [Timer]. Passed to
//! [Pollable::poll](crate::poll::Pollable).

use crate::poll::timer::Timer;

use alloc::rc::Rc;

/// A same-thread wake. `!Send`, `!Sync`.
///
/// Wakes by reference: wakers are stored and woken many times, never given up. Std's consuming
/// `Waker::wake(self)` saves a refcount when the run queue holds the task itself (the woken
/// `Arc<Task>` moves in); ours holds a `Copy` node id, so consuming would save nothing.
pub trait LocalWake {
    fn wake(&self);
}

/// A same-thread waker handle. `!Send`, `!Sync`, `Clone`.
///
/// Uses `Rc<dyn LocalWake>` — cheap clone, naturally `!Send`.
#[derive(Clone)]
pub struct LocalWaker {
    inner: Rc<dyn LocalWake>,
}

impl LocalWaker {
    pub fn new(wake: impl LocalWake + 'static) -> Self {
        Self {
            inner: Rc::new(wake),
        }
    }

    pub fn wake(&self) {
        self.inner.wake();
    }

    /// True iff `other` wakes the same target (same underlying wake
    /// impl). Mirrors `std::task::Waker::will_wake`.
    pub fn will_wake(&self, other: &LocalWaker) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Waker pair plus timer for [Pollable::poll](crate::poll::Pollable).
///
/// Always carries both sync and local wakers. Created on the
/// async thread during graph construction — never crosses threads.
///
/// - `sync`: for I/O registration and polling standard futures
/// - `local`: for cheap same-thread wake (no mutex, no syscall)
/// - `timer`: re-polls the node at a deadline
pub struct Waker {
    pub sync: core::task::Waker,
    pub local: LocalWaker,
    pub timer: Timer,
}

/// Shared [LocalWake] mocks for tests — this crate's, and downstream crates' via the
/// `testing` feature (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::{LocalWake, LocalWaker};
    use crate::poll::timer::mock::noop_timer;

    use std::cell::Cell;
    use std::rc::Rc;

    /// Test wake running a closure.
    struct MockWake<F>(F);

    impl<F: Fn()> LocalWake for MockWake<F> {
        fn wake(&self) {
            (self.0)();
        }
    }

    fn waker(on_wake: impl Fn() + 'static) -> LocalWaker {
        LocalWaker::new(MockWake(on_wake))
    }

    /// Waker whose `wake()` does nothing.
    pub fn noop_local_waker() -> LocalWaker {
        waker(|| {})
    }

    /// Waker plus the flag its `wake()` sets.
    pub fn tracking_local_waker() -> (LocalWaker, Rc<Cell<bool>>) {
        let woken = Rc::new(Cell::new(false));
        let flag = woken.clone();
        (waker(move || flag.set(true)), woken)
    }

    /// Waker plus the counter its `wake()` increments.
    pub fn counting_local_waker() -> (LocalWaker, Rc<Cell<usize>>) {
        let count = Rc::new(Cell::new(0));
        let counter = count.clone();
        (waker(move || counter.set(counter.get() + 1)), count)
    }

    /// Full [super::Waker] whose wakes do nothing and whose timer never fires.
    pub fn noop_waker() -> super::Waker {
        super::Waker {
            sync: std::task::Waker::noop().clone(),
            local: noop_local_waker(),
            timer: noop_timer(),
        }
    }
}
