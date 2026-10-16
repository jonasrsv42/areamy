//! Futures parked on a [FutureRoutine](super::line::FutureRoutine) queue.
//!
//! A future parks while its queue can't serve it (`recv` on empty, `send` on full) and is woken
//! by the next change (a send, a pop). Two kinds of parked future:
//!
//! - **Home:** polled with the node's own waker (plain `.await`, or a combinator that passes `cx`
//!   through, as [try_join](fn@crate::poll::try_join) does). Recognised by a pointer compare and
//!   woken through the node's [LocalWaker]: no atomics, no clone.
//! - **Other:** polled with a combinator's per-branch waker (e.g. `FuturesUnordered`). That waker
//!   marks the branch ready, then wakes the node; waking the node alone would re-poll the
//!   combinator but not the branch. Kept by clone, deduped by `will_wake`.
//!
//! The queue's wakes of the node's phases (Input after a refusal, Output after an empty pop) go
//! to fixed wakers and live beside this, in the queue.

use crate::wake::{self, LocalWaker};
use core::task::Waker;
use std::mem;

/// Who waits on a queue. Lives under the queue's borrow: [Waiters::take] there, then
/// [Awaking::wake] after releasing it.
#[derive(Default)]
pub(crate) struct Waiters {
    /// The node's waker, from the routine's first poll.
    home: Option<wake::Waker>,
    home_parked: bool,
    others: Vec<Waker>,
}

impl Waiters {
    /// An empty list keeping the home.
    pub(crate) fn renewed(&self) -> Self {
        Self {
            home: self.home.clone(),
            ..Self::default()
        }
    }

    /// The node's waker; the first call wins.
    pub(crate) fn home(&mut self, wake: &wake::Waker) {
        if self.home.is_none() {
            self.home = Some(wake.clone());
        }
    }

    /// Records `waker` once; clones only a new other.
    pub(crate) fn park(&mut self, waker: &Waker) {
        if self
            .home
            .as_ref()
            .is_some_and(|home| waker.will_wake(&home.sync))
        {
            self.home_parked = true;
        } else if !self.others.iter().any(|other| other.will_wake(waker)) {
            self.others.push(waker.clone());
        }
    }

    /// Takes every parked future; nobody parked takes nothing.
    pub(crate) fn take(&mut self) -> Awaking {
        let home = match mem::take(&mut self.home_parked) {
            true => self.home.as_ref().map(|home| home.local.clone()),
            false => None,
        };
        Awaking {
            home,
            others: mem::take(&mut self.others),
        }
    }
}

/// Wakes taken from a queue, for after its borrow is released.
#[must_use = "taken waiters are lost unless woken"]
pub(crate) struct Awaking {
    home: Option<LocalWaker>,
    others: Vec<Waker>,
}

impl Awaking {
    pub(crate) fn wake(self) {
        if let Some(home) = self.home {
            home.wake();
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
        let cx_waker = sync.sync.clone();
        let mut waiters = Waiters::default();
        waiters.home(&wake::Waker {
            sync: sync.sync,
            local,
        });
        waiters.park(&cx_waker);
        waiters.take().wake();
        assert_eq!(local_count.get(), 1);
        assert_eq!(sync_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn the_first_home_wins() {
        let (first, first_count) = counting_waker();
        let (second, _) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.home(&first);
        waiters.home(&second);
        // `first` is home, woken through its (noop) local half: its sync count stays 0.
        waiters.park(&first.sync);
        waiters.take().wake();
        assert_eq!(first_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn others_are_deduped_and_all_woken() {
        let (first, first_count) = counting_waker();
        let (second, second_count) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.park(&first.sync);
        waiters.park(&first.sync);
        waiters.park(&second.sync);
        waiters.take().wake();
        assert_eq!(first_count.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn take_leaves_nobody_behind() {
        let (home, home_count) = counting_local_waker();
        let (sync, _) = counting_waker();
        let cx_waker = sync.sync.clone();
        let (other, other_count) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.home(&wake::Waker {
            sync: sync.sync,
            local: home,
        });
        waiters.park(&cx_waker);
        waiters.park(&other.sync);
        waiters.take().wake();
        waiters.take().wake();
        assert_eq!(home_count.get(), 1);
        assert_eq!(other_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn renewed_keeps_the_home_and_drops_the_parked() {
        let (home, home_count) = counting_local_waker();
        let (sync, _) = counting_waker();
        let cx_waker = sync.sync.clone();
        let (other, other_count) = counting_waker();
        let mut waiters = Waiters::default();
        waiters.home(&wake::Waker {
            sync: sync.sync,
            local: home,
        });
        waiters.park(&cx_waker);
        waiters.park(&other.sync);
        let mut renewed = waiters.renewed();
        renewed.take().wake();
        assert_eq!(home_count.get(), 0);
        assert_eq!(other_count.load(Ordering::SeqCst), 0);
        renewed.park(&cx_waker);
        renewed.take().wake();
        assert_eq!(home_count.get(), 1);
    }
}
