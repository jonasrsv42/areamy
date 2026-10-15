//! Waker types for areamy's poll system.
//!
//! [ThreadLocalWaker] is a `!Send` waker for same-thread wake — our
//! equivalent of nightly `core::task::LocalWaker`. When that stabilizes,
//! this can wrap it.
//!
//! [Waker] bundles a sync waker (for I/O, standard futures) with a
//! thread-local waker (for cheap same-thread wake) and the node's [Timer]. Passed to
//! [Pollable::poll](crate::poll::Pollable).

use crate::poll::timer::Timer;

use alloc::rc::Rc;

/// Trait for thread-local wake implementations. `!Send`, `!Sync`.
///
/// Mirrors `std::task::Wake` but without `Send + Sync` requirements.
pub trait ThreadLocalWake {
    fn wake(&self);
}

/// A thread-local waker handle. `!Send`, `!Sync`, `Clone`.
///
/// Our equivalent of nightly `core::task::LocalWaker`.
/// Uses `Rc<dyn ThreadLocalWake>` — cheap clone, naturally `!Send`.
#[derive(Clone)]
pub struct ThreadLocalWaker {
    inner: Rc<dyn ThreadLocalWake>,
}

impl ThreadLocalWaker {
    pub fn new(wake: impl ThreadLocalWake + 'static) -> Self {
        Self {
            inner: Rc::new(wake),
        }
    }

    pub fn wake(&self) {
        self.inner.wake();
    }

    /// True iff `other` wakes the same target (same underlying wake
    /// impl). Mirrors `std::task::Waker::will_wake`.
    pub fn will_wake(&self, other: &ThreadLocalWaker) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Waker pair plus timer for [Pollable::poll](crate::poll::Pollable).
///
/// Always carries both sync and thread-local wakers. Created on the
/// async thread during graph construction — never crosses threads.
///
/// - `sync`: for I/O registration and polling standard futures
/// - `local`: for cheap same-thread wake (no mutex, no syscall)
/// - `timer`: re-polls the node at a deadline
pub struct Waker {
    pub sync: core::task::Waker,
    pub local: ThreadLocalWaker,
    pub timer: Timer,
}

/// Shared [ThreadLocalWake] mocks for tests — this crate's, and downstream crates' via the
/// `testing` feature (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::{ThreadLocalWake, ThreadLocalWaker};
    use crate::poll::timer::mock::noop_timer;

    use std::cell::Cell;
    use std::rc::Rc;

    /// Test wake running a closure.
    struct MockWake<F>(F);

    impl<F: Fn()> ThreadLocalWake for MockWake<F> {
        fn wake(&self) {
            (self.0)();
        }
    }

    /// Waker whose `wake()` does nothing.
    pub fn noop_local_waker() -> ThreadLocalWaker {
        ThreadLocalWaker::new(MockWake(|| {}))
    }

    /// Waker plus the flag its `wake()` sets.
    pub fn tracking_local_waker() -> (ThreadLocalWaker, Rc<Cell<bool>>) {
        let woken = Rc::new(Cell::new(false));
        let flag = woken.clone();
        (
            ThreadLocalWaker::new(MockWake(move || flag.set(true))),
            woken,
        )
    }

    /// Waker plus the counter its `wake()` increments.
    pub fn counting_local_waker() -> (ThreadLocalWaker, Rc<Cell<usize>>) {
        let count = Rc::new(Cell::new(0));
        let counter = count.clone();
        (
            ThreadLocalWaker::new(MockWake(move || counter.set(counter.get() + 1))),
            count,
        )
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
