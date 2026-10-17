//! `on_done` callback storage and panic-safe dispatch.
//!
//! Both [`ThreadStream`] (sync) and [`Thread`] (poll) hold a list of
//! callbacks to fire on thread exit. The list is wrapped in an
//! [`InitGuard`] when the thread becomes a task, before any spawn: the
//! normal exit path takes the callbacks out via [`InitGuard::drain`]
//! and invokes them with the actual [`Done`] outcome, while the guard's
//! [`Drop`] covers the rest: a panic, or a spawn the OS refused.
//!
//! User callbacks are dispatched through [`invoke`], which wraps each
//! call in [`catch_unwind`] so that a misbehaving callback can never
//! abort the process. This matters most on the panic path: without
//! the guard, a callback that panics while the thread is already
//! unwinding would double-panic and abort. With the guard, the
//! callback's panic is swallowed (and logged unless the `silent`
//! feature is set), and the next callback still gets a turn.
//!
//! [`ThreadStream`]: crate::work::ThreadStream
//! [`Thread`]: crate::poll::Thread

use super::NeverStarted;
use super::done::Done;
use core::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

pub type OnDone = Box<dyn FnOnce(&Done) + Send + 'static>;

/// Holds `on_done` callbacks from before the thread runs until it exits, so every exit fires them
/// once. Before the task starts: dropped, it fires a [NeverStarted] [Done::Error], because the
/// task was dropped unrun (its spawn was refused). The OS's reason for that goes to `join`: the
/// guard is dropped inside the refused spawn and never sees it. [start](Self::start) hands the
/// callbacks to a [RunGuard].
pub struct InitGuard {
    callbacks: Vec<OnDone>,
    /// [ThreadId::name](crate::ThreadId::name), for a [NeverStarted].
    thread: &'static str,
}

impl InitGuard {
    pub fn new(thread: &'static str, callbacks: Vec<OnDone>) -> Self {
        Self { callbacks, thread }
    }

    /// The thread's task has begun. Consumed empty, this guard's drop fires nothing.
    pub fn start(mut self) -> RunGuard {
        RunGuard {
            callbacks: core::mem::take(&mut self.callbacks),
        }
    }
}

impl Drop for InitGuard {
    fn drop(&mut self) {
        if self.callbacks.is_empty() {
            return;
        }
        let never_started = NeverStarted {
            thread: self.thread,
            reason: None,
        }
        .into_error();
        fire(
            core::mem::take(&mut self.callbacks),
            &Done::Error(&never_started),
        );
    }
}

/// The callbacks while the task runs. The normal exit [drain](Self::drain)s them to fire with the
/// real [Done]; dropped undrained, the task is unwinding, so it fires [Done::Panic].
pub struct RunGuard {
    callbacks: Vec<OnDone>,
}

impl RunGuard {
    /// Take the callbacks for the normal exit. Consumed empty, this guard's drop fires nothing.
    pub fn drain(mut self) -> Vec<OnDone> {
        core::mem::take(&mut self.callbacks)
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if self.callbacks.is_empty() {
            return;
        }
        // Undrained means unwinding. A future early return between `start` and `drain` would
        // report a clean exit as a panic; this trips on it in debug builds instead.
        debug_assert!(
            std::thread::panicking(),
            "RunGuard dropped undrained outside a panic: drain it on every exit"
        );
        fire(core::mem::take(&mut self.callbacks), &Done::Panic);
    }
}

/// Invoke every callback with the same `&Done` value, in order.
/// Each callback is isolated via [`catch_unwind`].
pub fn fire(callbacks: Vec<OnDone>, done: &Done) {
    for cb in callbacks {
        invoke(cb, done);
    }
}

/// Call a single callback, catching any panic so it cannot propagate
/// into the spawn closure (which would double-panic when invoked from
/// `InitGuard::drop`) and so it cannot stop later callbacks in
/// [`fire`] / `InitGuard::drop` from running.
fn invoke(cb: OnDone, done: &Done) {
    if catch_unwind(AssertUnwindSafe(move || cb(done))).is_err() {
        #[cfg(not(feature = "silent"))]
        eprintln!("on_done callback panicked, ignoring");
    }
}
