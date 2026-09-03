//! Current-node waker TLS.
//!
//! The poll loop installs the waker of the node it is about to poll;
//! leaf futures created without a handle ([sleep](crate::poll::sleep))
//! capture it on their first poll. One slot suffices: polling on a
//! thread is serial, so the slot always names the owner of the
//! innermost running poll frame. That holds for donated threads too
//! (`Thread::run`), including a runtime nested inside another's poll
//! frame: each dispatch guard saves the outer waker and restores it,
//! so inner nodes shadow the outer only for the length of their own
//! frame.
//!
//! # Stopgap
//!
//! This is the stable-Rust stand-in for `Context::local_waker()`
//! (unstable `local_waker` / `context_ext` features). Once those ride
//! stable, the attach sites are the `Context`-building
//! `FutureRoutine::poll` impls (poll/future/line/routine.rs,
//! poll/future/biunion/routine.rs — both have `waker.local` in
//! scope); `SleepFut` then reads `cx.local_waker()` (vtable-checked
//! downcast via an extension trait — sound for the `!Send`
//! `LocalWaker`) and this module gets deleted. Caveat for that
//! migration: a cx-carried local waker is stripped by any layer that
//! rebuilds a `Context` via plain `from_waker` — a pattern TLS
//! survives today.

use crate::connect::waker::ThreadLocalWaker;

use std::cell::Cell;

thread_local! {
    static CURRENT: Cell<Option<ThreadLocalWaker>> = const { Cell::new(None) };
}

/// Installs a node's waker as the thread's current for one poll
/// dispatch; restores the previous value on drop (panic-safe, and
/// correct under nested polls — see module docs).
pub(crate) struct ThreadLocalGuard {
    prev: Option<ThreadLocalWaker>,
}

impl ThreadLocalGuard {
    pub(crate) fn set(waker: ThreadLocalWaker) -> Self {
        Self {
            prev: CURRENT.replace(Some(waker)),
        }
    }
}

impl Drop for ThreadLocalGuard {
    fn drop(&mut self) {
        CURRENT.set(self.prev.take());
    }
}

/// Waker of the node currently being polled on this thread, if any.
pub(crate) fn current() -> Option<ThreadLocalWaker> {
    // Cell take/set-back: no borrow flag, no panic path.
    CURRENT.with(|c| {
        let waker = c.take();
        let out = waker.clone();
        c.set(waker);
        out
    })
}

/// Whether the current frame's waker wakes the same target as
/// `waker`. `None` when no frame is installed (manual poll). Compares
/// in place — no clone, no refcount traffic.
#[cfg(debug_assertions)]
pub(crate) fn current_will_wake(waker: &ThreadLocalWaker) -> Option<bool> {
    CURRENT.with(|c| {
        let current = c.take();
        let same = current.as_ref().map(|cur| cur.will_wake(waker));
        c.set(current);
        same
    })
}
