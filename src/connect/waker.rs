//! Waker types for areamy's poll system.
//!
//! [ThreadLocalWaker] is a `!Send` waker for same-thread wake — our
//! equivalent of nightly `core::task::LocalWaker`. When that stabilizes,
//! this can wrap it.
//!
//! [Waker] bundles a sync waker (for I/O, standard futures) with a
//! thread-local waker (for cheap same-thread wake). Passed to
//! [Pollable::poll](crate::Pollable).

use crate::connect::poll::queue::TimerKey;

use alloc::rc::Rc;
use std::time::Instant;

/// Trait for thread-local wake implementations. `!Send`, `!Sync`.
///
/// Mirrors `std::task::Wake` but without `Send + Sync` requirements,
/// and adds a deadline-bound variant for timer-driven polls.
pub trait ThreadLocalWake {
    fn wake(&self);
    /// Schedule the owning node to be polled at `deadline`. The key
    /// cancels the timer; dropping it means the timer just fires.
    /// Infallible: every waker must be backed by a timer source
    /// (tests use [mock], which carries a private scheduler).
    #[must_use]
    fn schedule_at(&self, deadline: Instant) -> TimerKey;
    /// Release a timer before it fires. Dead keys are a no-op.
    fn cancel(&self, key: TimerKey);
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

    #[must_use]
    pub fn schedule_at(&self, deadline: Instant) -> TimerKey {
        self.inner.schedule_at(deadline)
    }

    pub fn cancel(&self, key: TimerKey) {
        self.inner.cancel(key);
    }

    /// True iff `other` wakes the same target (same underlying wake
    /// impl). Mirrors `std::task::Waker::will_wake`.
    pub fn will_wake(&self, other: &ThreadLocalWaker) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
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
    #[must_use]
    pub fn schedule_at(&self, deadline: Instant) -> TimerKey {
        self.local.schedule_at(deadline)
    }

    /// Release a timer before it fires. Delegates to
    /// [`ThreadLocalWaker::cancel`].
    pub fn cancel(&self, key: TimerKey) {
        self.local.cancel(key);
    }
}

/// Shared timer-less [ThreadLocalWake] mocks for the crate's tests.
#[cfg(test)]
pub mod mock {
    use super::{ThreadLocalWake, ThreadLocalWaker};
    use crate::connect::poll::queue::{PollQueue, ThreadLocalProducer, TimerKey};

    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Instant;

    /// Test wake with real timer support: schedule/cancel go to a
    /// private scheduler nobody drains (keys are real, deadlines
    /// register, nothing fires); `wake` sets the optional flag.
    struct MockWake {
        woken: Option<Rc<Cell<bool>>>,
        producer: ThreadLocalProducer,
    }

    impl MockWake {
        fn new(woken: Option<Rc<Cell<bool>>>) -> Self {
            let (_consumer, producer) = PollQueue::new().local();
            Self { woken, producer }
        }
    }

    impl ThreadLocalWake for MockWake {
        fn wake(&self) {
            if let Some(woken) = &self.woken {
                woken.set(true);
            }
        }
        fn schedule_at(&self, deadline: Instant) -> TimerKey {
            self.producer.schedule(0, deadline)
        }
        fn cancel(&self, key: TimerKey) {
            self.producer.cancel(key);
        }
    }

    /// Waker whose `wake()` does nothing.
    pub fn noop_local_waker() -> ThreadLocalWaker {
        ThreadLocalWaker::new(MockWake::new(None))
    }

    /// Waker plus the flag its `wake()` sets.
    pub fn tracking_local_waker() -> (ThreadLocalWaker, Rc<Cell<bool>>) {
        let woken = Rc::new(Cell::new(false));
        (
            ThreadLocalWaker::new(MockWake::new(Some(woken.clone()))),
            woken,
        )
    }
}
