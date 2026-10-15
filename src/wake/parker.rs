//! [Parker]: blocks one thread until woken.

use alloc::sync::Arc;
use alloc::task::Wake;
use core::cell::Cell;
use core::marker::PhantomData;
use core::task::Waker;
use std::mem;
use std::sync::{Condvar, Mutex};

/// One thread's park token. Wakes coalesce and are never lost: a wake before [Parker::park]
/// makes the next one return at once.
///
/// Binds no thread, unlike `thread::park`: its waker exists before the thread that parks (a node
/// is built on one thread and runs on another).
pub(crate) struct Parker {
    /// Shared with every waker. Private: a second parker on it would break the skip-notify.
    inner: Arc<Inner>,
    /// `Send`, not `Sync`: exactly one thread parks, which the skip-notify relies on.
    _single: PhantomData<Cell<()>>,
}

struct Inner {
    /// The token and whether the parker sleeps on it.
    state: Mutex<State>,
    /// Where the parker sleeps until a wake sets the token.
    unpark: Condvar,
}

#[derive(Default)]
struct State {
    /// The token: set by a wake, cleared by the park it releases.
    woken: bool,
    /// Asleep on `unpark`; a wake only notifies when set.
    parked: bool,
}

impl Parker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                unpark: Condvar::new(),
            }),
            _single: PhantomData,
        }
    }

    /// The waking half: clone freely, store anywhere.
    pub(crate) fn waker(&self) -> Waker {
        // Wakes go through `impl Wake for Inner`; the waker shares the state.
        Waker::from(self.inner.clone())
    }

    /// Block until woken since the last park.
    pub(crate) fn park(&self) {
        // Poison is recovered: the state is two bools, valid in every combination.
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        // Woken before we parked: consume the token without sleeping.
        if state.woken {
            state.woken = false;
            return;
        }
        // Tell wakes to notify. Only a wake clears it, and that wake also sets the token, which
        // ends the loop.
        state.parked = true;
        // Loop: the condvar may return spuriously, without a wake.
        while !state.woken {
            // Unlocks while asleep, so a wake can take the lock; relocks before returning.
            state = self
                .inner
                .unpark
                .wait(state)
                .unwrap_or_else(|p| p.into_inner());
        }
        // Consume the token: the next park sleeps until a later wake.
        state.woken = false;
    }
}

#[cfg(test)]
impl Parker {
    /// Read and clear a pending wake, so later checks see only wakes after this point.
    pub(crate) fn take_woken(&self) -> bool {
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        mem::take(&mut state.woken)
    }

    /// Whether the parker is asleep, readable from another thread.
    pub(crate) fn parked_probe(&self) -> impl Fn() -> bool + Send + 'static {
        let inner = self.inner.clone();
        move || inner.state.lock().unwrap_or_else(|p| p.into_inner()).parked
    }
}

/// By reference too, so a stored waker never clones its `Arc`.
impl Wake for Inner {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let parked = {
            // Poison is recovered, as in `park`.
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            // Set the token: kept for the next park if none is asleep. Wakes coalesce into it.
            state.woken = true;
            // Claim the notify: a later wake sees false and skips it, this one releases the park.
            mem::take(&mut state.parked)
        };
        // Notify unlocked, so the woken parker doesn't block straight on our lock. Skipped when
        // nobody sleeps, which saves the syscall.
        if parked {
            self.unpark.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::sync::tests::wait_until;
    use std::thread;

    #[test]
    fn wake_by_ref_wakes() {
        let parker = Parker::new();
        parker.waker().wake_by_ref();
        assert!(parker.take_woken());
    }

    #[test]
    fn wake_wakes() {
        let parker = Parker::new();
        parker.waker().wake();
        assert!(parker.take_woken());
    }

    #[test]
    fn park_returns_after_an_earlier_wake_and_clears_it() {
        let parker = Parker::new();
        parker.waker().wake_by_ref();
        parker.park();
        assert!(!parker.take_woken());
    }

    #[test]
    fn park_returns_when_woken_while_asleep() {
        let parker = Parker::new();
        let waker = parker.waker();
        let parked = parker.parked_probe();
        thread::scope(|s| {
            s.spawn(move || {
                // Wake only once the parker sleeps, so the notify path is what releases it.
                wait_until(parked);
                waker.wake();
            });
            parker.park();
        });
        assert!(!parker.parked_probe()());
    }
}
