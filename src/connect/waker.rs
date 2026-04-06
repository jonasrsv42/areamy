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
///
/// ThreadLocalWaker is useful for driving a state machine through cheaper
/// [`ThreadLocalWake::wake`] that does not need to cross any thread boundary.
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
