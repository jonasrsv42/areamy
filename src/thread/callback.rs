//! `on_done` callback storage and panic-safe dispatch.
//!
//! Both [`ThreadStream`] (sync) and [`Thread`] (poll) hold a list of
//! callbacks to fire on thread exit. The list is wrapped in
//! [`PanicGuard`] inside the spawn closure: the normal exit path
//! takes the callbacks out via [`PanicGuard::drain`] and invokes them
//! with the actual [`Done`] outcome, while the guard's [`Drop`] only
//! runs if the closure is unwinding, dispatching `Done::Panic`.
//!
//! [`ThreadStream`]: crate::thread::ThreadStream
//! [`Thread`]: crate::thread::poll::stream::Thread

use super::done::Done;

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
            cb(&Done::Panic);
        }
    }
}

/// Invoke every callback with the same `&Done` value, in order.
pub fn fire(callbacks: Vec<OnDone>, done: &Done) {
    for cb in callbacks {
        cb(done);
    }
}
