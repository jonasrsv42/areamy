//! Wakers: the currency every scheduling paradigm wakes with, as [Message](crate::message::Message)
//! is the currency of dataflow.
//!
//! [LocalWaker] is a `!Send` waker for same-thread wake.
//!
//! [Waker] bundles a sync waker (for I/O, standard futures) with a
//! local waker (for cheap same-thread wake).

use alloc::rc::Rc;

/// A same-thread wake. `!Send`, `!Sync`.
///
/// Wakes by reference: wakers are stored and woken many times, this makes us diverge
/// a bit from Std's consuming wake but it is an optimization that saves a refcount on
/// each wake for areamy's reusable wakers.
///
/// We can do this because areamy scheduling is a `Copy` of ID. Std's `Wake` makes the
/// consuming `wake` the primitive because its run queues may take the task itself (the woken
/// `Arc<Task>` moves in, saving a refcount); its default `wake_by_ref` clones to call it.
/// Ours has nothing to hand over, so by reference is the primitive and no wake clones.
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

/// A sync and a local waker for the same target.
///
/// - `sync`: for I/O registration and polling standard futures
/// - `local`: for cheap same-thread wake (no mutex, no syscall)
pub struct Waker {
    pub sync: core::task::Waker,
    pub local: LocalWaker,
}

/// Shared [LocalWake] mocks for tests — this crate's, and downstream crates' via the
/// `testing` feature (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::{LocalWake, LocalWaker, Waker};

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

    /// [Waker] whose halves both do nothing.
    pub fn noop_waker() -> Waker {
        Waker {
            sync: std::task::Waker::noop().clone(),
            local: noop_local_waker(),
        }
    }
}
