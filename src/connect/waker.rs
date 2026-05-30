//! Waker types for areamy's poll system.
//!
//! [ThreadLocalWaker] is a `!Send` waker for same-thread wake — our
//! equivalent of nightly `core::task::LocalWaker`. When that stabilizes,
//! this can wrap it.
//!
//! [Waker] bundles a sync waker (for I/O, standard futures) with a
//! thread-local waker (for cheap same-thread wake). Passed to
//! [Pollable::poll](crate::Pollable).

use alloc::rc::Rc;
use std::time::Instant;

/// Trait for thread-local wake implementations. `!Send`, `!Sync`.
///
/// Mirrors `std::task::Wake` but without `Send + Sync` requirements,
/// and adds a deadline-bound variant for timer-driven polls.
pub trait ThreadLocalWake {
    fn wake(&self);
    /// Schedule the owning node to be polled at `deadline`.
    fn schedule_at(&self, deadline: Instant);
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

    pub fn schedule_at(&self, deadline: Instant) {
        self.inner.schedule_at(deadline);
    }
}

/// Waker pair for [Pollable::poll](crate::Pollable).
///
/// Always carries both sync and thread-local wakers. Created on the
/// async thread during graph construction — never crosses threads.
///
/// - `sync`: for I/O registration and polling standard futures
/// - `local`: for cheap same-thread wake (no mutex, no syscall)
pub struct Waker {
    pub sync: core::task::Waker,
    pub local: ThreadLocalWaker,
}

impl Waker {
    /// Schedule the owning node to be polled at `deadline`. Delegates
    /// to [`ThreadLocalWaker::schedule_at`].
    pub fn schedule_at(&self, deadline: Instant) {
        self.local.schedule_at(deadline);
    }
}
