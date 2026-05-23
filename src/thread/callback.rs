//! `on_done` callback storage and panic-safe dispatch.
//!
//! Both [`ThreadStream`] (sync) and [`Thread`] (poll) hold a list of
//! callbacks to fire on thread exit. The list is wrapped in
//! [`PanicGuard`] inside the spawn closure: the normal exit path
//! takes the callbacks out via [`PanicGuard::drain`] and invokes them
//! with the actual [`Done`] outcome, while the guard's [`Drop`] only
//! runs if the closure is unwinding, dispatching `Done::Panic`.
//!
//! User callbacks are dispatched through [`invoke`], which wraps each
//! call in [`catch_unwind`] so that a misbehaving callback can never
//! abort the process. This matters most on the panic path: without
//! the guard, a callback that panics while the thread is already
//! unwinding would double-panic and abort. With the guard, the
//! callback's panic is swallowed (and logged unless the `silent`
//! feature is set), and the next callback still gets a turn.
//!
//! [`ThreadStream`]: crate::thread::ThreadStream
//! [`Thread`]: crate::thread::poll::stream::Thread

use super::done::Done;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub type OnDone = Box<dyn FnOnce(&Done) + Send + 'static>;

/// Holds `on_done` callbacks across the spawn closure and dispatches
/// `Done::Panic` to them if the closure unwinds.
pub struct PanicGuard {
    pub callbacks: Vec<OnDone>,
}

impl PanicGuard {
    pub fn new(callbacks: Vec<OnDone>) -> Self {
        Self { callbacks }
    }

    /// Take the callbacks out so the normal path can invoke them with
    /// the actual `Done` value. After this, the guard's `Drop` is a
    /// no-op (the vec is empty).
    pub fn drain(&mut self) -> Vec<OnDone> {
        std::mem::take(&mut self.callbacks)
    }
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        for cb in self.callbacks.drain(..) {
            invoke(cb, &Done::Panic);
        }
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
/// `PanicGuard::drop`) and so it cannot stop later callbacks in
/// [`fire`] / `PanicGuard::drop` from running.
fn invoke(cb: OnDone, done: &Done) {
    if catch_unwind(AssertUnwindSafe(move || cb(done))).is_err() {
        #[cfg(not(feature = "silent"))]
        eprintln!("on_done callback panicked, ignoring");
    }
}
