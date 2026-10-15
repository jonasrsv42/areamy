//! Current-node timer TLS.
//!
//! The poll loop installs the timer of the node it is about to poll;
//! leaf futures created without a handle ([sleep](crate::poll::sleep))
//! capture it on their first poll. One slot suffices: polling on a
//! thread is serial, so the slot always names the owner of the
//! innermost running poll frame. That holds for donated threads too
//! (`Thread::run`), including a runtime nested inside another's poll
//! frame: each dispatch guard saves the outer timer and restores it,
//! so inner nodes shadow the outer only for the length of their own
//! frame.
//!
//! # Stopgap
//!
//! This is the stable-Rust stand-in for passing the timer through the
//! `Context` (unstable `context_ext`). Once that rides stable, the
//! attach sites are the `Context`-building `FutureRoutine::poll` impls
//! (node/line/poll/future.rs, node/biunion/poll/future.rs — both have
//! `cx.timer` in scope); `SleepFut` then reads the timer from
//! `cx.ext()` and this module gets deleted. Caveat for that migration:
//! a cx-carried extension is stripped by any layer that rebuilds a
//! `Context` via plain `from_waker` — a pattern TLS survives today.

use crate::poll::timer::Timer;

use std::cell::Cell;

thread_local! {
    static CURRENT: Cell<Option<Timer>> = const { Cell::new(None) };
}

/// Installs a node's timer as the thread's current for one poll
/// dispatch; restores the previous value on drop (panic-safe, and
/// correct under nested polls — see module docs).
pub(crate) struct ThreadLocalGuard {
    prev: Option<Timer>,
}

impl ThreadLocalGuard {
    pub(crate) fn set(timer: Timer) -> Self {
        Self {
            prev: CURRENT.replace(Some(timer)),
        }
    }
}

impl Drop for ThreadLocalGuard {
    fn drop(&mut self) {
        CURRENT.set(self.prev.take());
    }
}

/// Timer of the node currently being polled on this thread, if any.
pub(crate) fn current() -> Option<Timer> {
    // Cell take/set-back: no borrow flag, no panic path.
    CURRENT.with(|c| {
        let timer = c.take();
        let out = timer.clone();
        c.set(timer);
        out
    })
}

/// Whether the current frame's timer re-polls the same node as
/// `timer`. `None` when no frame is installed (manual poll). Compares
/// in place — no clone, no refcount traffic.
#[cfg(debug_assertions)]
pub(crate) fn current_same_target(timer: &Timer) -> Option<bool> {
    CURRENT.with(|c| {
        let current = c.take();
        let same = current.as_ref().map(|cur| cur.same_target(timer));
        c.set(current);
        same
    })
}
