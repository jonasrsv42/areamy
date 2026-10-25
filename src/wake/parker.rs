//! [Parker]: blocks one thread until woken.

use alloc::sync::Arc;
use alloc::task::Wake;
use core::cell::Cell;
use core::marker::PhantomData;
use core::mem;
use core::task::Waker;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// One thread's park token. Wakes coalesce and are never lost: a wake before a park
/// ([Parker::park] or [Parker::park_until]) makes it return at once.
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
        self.sleep(None);
    }

    /// Block until woken since the last park, or until `deadline`. A wake present at the
    /// deadline wins and is consumed, so no stale token outlives the call.
    pub(crate) fn park_until(&self, deadline: Instant) {
        self.sleep(Some(deadline));
    }

    /// [Self::park] without a deadline, [Self::park_until] with one: one loop, so both share the
    /// lost-wake reasoning.
    fn sleep(&self, deadline: Option<Instant>) {
        // Poison is recovered: the state is two bools, valid in every combination.
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        // Woken before we parked: consume the token without sleeping; `parked` stays clear, so
        // wakes skip the notify.
        if state.woken {
            state.woken = false;
            return;
        }
        // Tell wakes to notify. Only a wake (which also sets the token, ending the loop) or our
        // timeout clears it.
        state.parked = true;
        // Loop: the condvar may return spuriously, without a wake.
        while !state.woken {
            // Each wait unlocks while asleep, so a wake can take the lock; relocks before
            // returning.
            state = match deadline {
                None => self
                    .inner
                    .unpark
                    .wait(state)
                    .unwrap_or_else(|p| p.into_inner()),
                Some(deadline) => {
                    // Recomputed each turn: a spurious return must not restart the full wait.
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    // Timed out unwoken (checked after the token, so a wake wins a tie): stop
                    // asking wakes to notify a parker that left.
                    if remaining.is_zero() {
                        state.parked = false;
                        return;
                    }
                    self.inner
                        .unpark
                        .wait_timeout(state, remaining)
                        // The timed-out flag is dropped: the token and the clock decide, above.
                        .map(|(state, _)| state)
                        .unwrap_or_else(|p| p.into_inner().0)
                }
            };
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
    pub(crate) fn parked_probe(&self) -> impl Fn() -> bool + Send + 'static + use<> {
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
            // Poison is recovered, as in `sleep`.
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
    use crate::tests::support::{DEADLINE, deadline};
    use core::time::Duration;
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
        // Never slept, so later wakes must skip the notify.
        assert!(!parker.parked_probe()());
    }

    #[test]
    fn park_returns_when_woken_while_asleep() {
        let _deadline = deadline(DEADLINE);
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
        // The wake was consumed: the next park sleeps until a later one.
        assert!(!parker.take_woken());
    }

    #[test]
    fn park_until_times_out_without_a_wake() {
        let parker = Parker::new();
        let start = Instant::now();
        parker.park_until(start + Duration::from_millis(20));
        assert!(start.elapsed() >= Duration::from_millis(20));
        // The parker left: no token, and wakes must not notify it.
        assert!(!parker.take_woken());
        assert!(!parker.parked_probe()());
    }

    #[test]
    fn park_until_a_past_deadline_returns_at_once() {
        let parker = Parker::new();
        let start = Instant::now();
        parker.park_until(start - Duration::from_millis(50));
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!parker.parked_probe()());
    }

    #[test]
    fn park_until_consumes_an_earlier_wake() {
        let parker = Parker::new();
        parker.waker().wake_by_ref();
        let start = Instant::now();
        parker.park_until(start + Duration::from_secs(60));
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!parker.take_woken());
        assert!(!parker.parked_probe()());
    }

    #[test]
    fn park_until_a_past_deadline_still_consumes_a_wake() {
        let parker = Parker::new();
        parker.waker().wake_by_ref();
        parker.park_until(Instant::now() - Duration::from_millis(50));
        // The wake wins the tie: consumed, not left for the next park.
        assert!(!parker.take_woken());
    }

    #[test]
    fn park_until_returns_when_woken_while_asleep() {
        let _deadline = deadline(DEADLINE);
        let parker = Parker::new();
        let waker = parker.waker();
        let parked = parker.parked_probe();
        let start = Instant::now();
        thread::scope(|s| {
            s.spawn(move || {
                wait_until(parked);
                waker.wake();
            });
            parker.park_until(start + Duration::from_secs(60));
        });
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!parker.parked_probe()());
        assert!(!parker.take_woken());
    }
}
