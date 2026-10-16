//! Futures parked on a [FutureRoutine](super::line::FutureRoutine) queue.
//!
//! A future parks while its queue can't serve it (`recv` on empty, `send` on full) and is woken
//! by the next change (a send, a pop). Two kinds of parked future:
//!
//! - **Home:** polled with the node's own waker (plain `.await`, or a combinator that passes `cx`
//!   through, as [try_join](fn@crate::poll::try_join) does). Recognised by a pointer compare and
//!   woken through the node's [LocalWaker](crate::wake::LocalWaker): no atomics, no clone.
//! - **Other:** polled with a combinator's per-branch waker (e.g. `FuturesUnordered`). That waker
//!   marks the branch ready, then wakes the node; waking the node alone would re-poll the
//!   combinator but not the branch. Kept by clone, deduped by `will_wake`.
//!
//! The queue's wakes of the node's phases (Input after a refusal, Output after an empty pop) go
//! to fixed wakers and live beside this, in the queue.

use crate::wake;
use core::task::Waker;
use std::mem;

/// Who waits on a queue. Lives under the queue's borrow: [Waiters::take] there, then
/// [Awaking::wake] after releasing it.
#[derive(Default)]
pub(crate) struct Waiters {
    /// The home future waits. A flag, not a waker: the home waker is fixed (the queue's).
    home: bool,
    /// Per-branch wakers, each once.
    others: Vec<Waker>,
}

impl Waiters {
    /// Records `waker` once; clones only a new other. `home` is the node's waker, the one the
    /// routine is polled with.
    pub(crate) fn park(&mut self, waker: &Waker, home: &wake::Waker) {
        if waker.will_wake(&home.sync) {
            // A pointer compare: true for every clone of the node's waker.
            self.home = true;
        } else if !self.others.iter().any(|other| other.will_wake(waker)) {
            // Deduped: a re-poll of a parked branch must not record it twice.
            self.others.push(waker.clone());
        }
    }

    /// Takes every parked future, under the queue's borrow; nobody parked takes nothing. Taking
    /// is what makes a wake one-shot: a future re-parks only when polled again.
    pub(crate) fn take(&mut self) -> Awaking {
        Awaking {
            home: mem::take(&mut self.home),
            others: mem::take(&mut self.others),
        }
    }
}

/// Wakes taken from a queue, owed once its borrow is released: a wake may re-enter the queue.
#[must_use = "taken waiters are lost unless woken"]
pub(crate) struct Awaking {
    home: bool,
    others: Vec<Waker>,
}

impl Awaking {
    /// `home` is the queue's, as passed to [Waiters::park].
    pub(crate) fn wake(self, home: &wake::Waker) {
        if self.home {
            home.local.wake();
        }
        for other in self.others {
            other.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wake::mock::{counting_local_waker, counting_waker};
    use std::sync::atomic::Ordering;

    #[test]
    fn home_wakes_through_its_local_half_only() {
        let (sync, sync_count) = counting_waker();
        let (local, local_count) = counting_local_waker();
        let home = wake::Waker {
            sync: sync.sync,
            local,
        };
        let mut waiters = Waiters::default();
        waiters.park(&home.sync.clone(), &home);
        waiters.take().wake(&home);
        assert_eq!(local_count.get(), 1);
        assert_eq!(sync_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn others_are_deduped_and_all_woken() {
        let (home, _) = counting_waker();
        let (first, first_count) = counting_waker();
        let (second, second_count) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.park(&first.sync, &home);
        waiters.park(&first.sync, &home);
        waiters.park(&second.sync, &home);
        waiters.take().wake(&home);
        assert_eq!(first_count.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn take_leaves_nobody_behind() {
        let (sync, _) = counting_waker();
        let (local, home_count) = counting_local_waker();
        let home = wake::Waker {
            sync: sync.sync,
            local,
        };
        let (other, other_count) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.park(&home.sync.clone(), &home);
        waiters.park(&other.sync, &home);
        waiters.take().wake(&home);
        waiters.take().wake(&home);
        assert_eq!(home_count.get(), 1);
        assert_eq!(other_count.load(Ordering::SeqCst), 1);
    }
}
