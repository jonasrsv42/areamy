//! [Consumer] — hands out woken ids. `!Send`, `!Sync`.
//!
//! [Consumer::next] walks a batch taken from the [TreiberStack]; at each boundary it pushes expired
//! timers, then takes again. A batch is every id pushed since the last take, newest first, each at
//! most once: an id that wakes itself waits a batch.
//!
//! Costs: n graph ids, h timer heap entries (dead ones included).

use super::treiber::{Link, TreiberStack};
use crate::poll::graph::NodeId;
use crate::poll::timer::Timers;
use crate::wake::Parker;

use alloc::sync::Arc;

/// The consumer before its arena exists. Not `Clone`; [Self::init] makes the [Consumer], once.
pub(crate) struct Uninit {
    consumer: Consumer,
}

impl Uninit {
    pub(super) fn new(stack: Arc<TreiberStack>, timers: Timers, parker: Parker) -> Self {
        Self {
            consumer: Consumer {
                stack,
                batch: Link::NONE,
                timers,
                parker,
            },
        }
    }

    /// The graph is ids `0..n`: makes the arena and pushes every id for its first poll, in id
    /// order. O(n): one allocation.
    pub(crate) fn init(self, n: usize) -> Consumer {
        let consumer = self.consumer;
        consumer.stack.init(n);
        // Last to first, so the first batch runs in id order.
        for id in (0..n).rev() {
            consumer.stack.push(id);
        }
        consumer
    }
}

pub(crate) struct Consumer {
    /// Woken ids, pushed from any thread.
    stack: Arc<TreiberStack>,
    /// The batch being walked.
    batch: Link,
    /// Expired ones are pushed at each batch boundary.
    timers: Timers,
    /// Parked on while a take finds the stack empty; every remote push wakes it.
    ///
    /// # Same-thread assumption
    ///
    /// Local pushes and timers never wake it. They run on this thread, inside polls, which only
    /// run after `park` returns. `!Send` on [Local](super::Local), its wakers and timers, and the
    /// consumer keeps them on one thread; a local push during a park would never be served.
    parker: Parker,
}

impl Consumer {
    /// The next id to poll; parks while none is pushed. O(1) per id.
    #[allow(clippy::should_implement_trait)]
    pub(crate) fn next(&mut self) -> NodeId {
        loop {
            if let Some(id) = self.try_next() {
                return id;
            }
            // The earliest timer bounds the park.
            match self.timers.earliest() {
                Some(deadline) => self.parker.park_until(deadline),
                None => self.parker.park(),
            }
        }
    }

    /// The next id to poll, or `None` where [Self::next] parks. O(1); a batch boundary adds the
    /// expired timers' pushes and a take.
    fn try_next(&mut self) -> Option<NodeId> {
        if let Some(id) = self.stack.next(&mut self.batch) {
            return Some(id);
        }
        // Pushed last before the take, so they lead the next batch.
        self.timers.expire(|id| self.stack.push(id));
        self.batch = self.stack.take();
        self.stack.next(&mut self.batch)
    }
}

#[cfg(test)]
impl Consumer {
    /// What [Self::next] hands out before it parks; fails if it never would.
    pub(crate) fn woken(&mut self) -> Vec<NodeId> {
        const ENDLESS: usize = 1 << 16;
        let ids: Vec<_> = core::iter::from_fn(|| self.try_next())
            .take(ENDLESS)
            .collect();
        assert!(ids.len() < ENDLESS, "never parks");
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::sync::tests::wait_until;
    use crate::poll::scheduler::{Local, Producer, Scheduler};
    use crate::tests::support::{DEADLINE, deadline};
    use alloc::task::Wake;
    use core::task::Waker;
    use core::time::Duration;
    use std::sync::{Mutex, mpsc};
    use std::thread;
    use std::time::Instant;

    const BITS: usize = usize::BITS as usize;

    /// The consumer before init; the graph's local half; a producer.
    fn unstarted() -> (Uninit, Local, Producer) {
        let scheduler = Scheduler::new();
        let producer = scheduler.producer().clone();
        let (uninit, local) = scheduler.local();
        (uninit, local, producer)
    }

    /// As a local waker pushes.
    fn wake(s: &Consumer, id: NodeId) {
        s.stack.push(id);
    }

    fn past() -> Instant {
        Instant::now() - Duration::from_millis(50)
    }

    // ---- local and remote wakes ----

    #[test]
    fn local_and_remote_wakes_come_out_newest_first() {
        let (mut s, _, producer, _) = Scheduler::started(4);
        producer.push(0);
        wake(&s, 2);
        wake(&s, 1);
        assert_eq!(s.woken(), [1, 2, 0]);
    }

    #[test]
    fn a_one_id_graph_takes_remote_wakes() {
        let (mut s, _, producer, _) = Scheduler::started(1);
        producer.push(0);
        assert_eq!(s.woken(), [0]);
    }

    #[test]
    fn a_remote_wake_of_an_id_still_queued_polls_it_once() {
        let (mut s, _, producer, _) = Scheduler::started(4);
        wake(&s, 3);
        assert_eq!(s.try_next(), Some(3));
        // Queued: the remote wake coalesces.
        wake(&s, 1);
        producer.push(1);
        assert_eq!(s.woken(), [1]);
    }

    /// Covered by init's first poll of every id, not polled twice.
    #[test]
    fn a_remote_wake_before_init_is_covered_by_the_first_batch() {
        let (uninit, _, producer) = unstarted();
        producer.push(1);
        let mut s = uninit.init(3);
        assert_eq!(s.woken(), [0, 1, 2]);
        producer.push(1);
        assert_eq!(s.woken(), [1]);
    }

    /// The wake finds the nodes: pushed first, so out last, and signalled.
    #[test]
    fn a_remote_wake_between_the_nodes_and_the_first_pushes_is_first() {
        let (uninit, _, producer) = unstarted();
        let mut s = uninit.consumer;
        s.stack.init(3);
        producer.push(1);
        for id in (0..3).rev() {
            s.stack.push(id);
        }
        assert_eq!(s.woken(), [0, 2, 1]);
        assert!(s.parker.take_woken());
    }

    #[test]
    fn next_parks_until_a_remote_wake() {
        let _deadline = deadline(DEADLINE);
        let (mut s, _, producer, parked) = Scheduler::started(8);
        let handle = thread::spawn(move || {
            // Once it sleeps, so this wake is what releases it.
            wait_until(parked);
            producer.push(7);
        });
        assert_eq!(s.next(), 7);
        handle.join().unwrap();
    }

    #[test]
    fn next_parks_for_real_after_a_stale_token() {
        let _deadline = deadline(DEADLINE);
        let (mut s, _, producer, parked) = Scheduler::started(8);
        // Taken while awake: the token is left stale.
        producer.push(1);
        assert_eq!(s.next(), 1);
        let handle = thread::spawn(move || {
            // The stale token costs one empty take; the next park sleeps.
            wait_until(parked);
            producer.push(2);
        });
        assert_eq!(s.next(), 2);
        handle.join().unwrap();
    }

    /// The first producer is preempted between its push and its wake, as a low-priority task
    /// would be: a later push's own wake serves both ids.
    #[test]
    fn a_wake_never_waits_on_a_stalled_first_producer() {
        struct Stalled {
            pushed: Mutex<mpsc::Sender<()>>,
            resume: Mutex<mpsc::Receiver<()>>,
        }
        impl Wake for Stalled {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                let _ = self.pushed.lock().map(|tx| tx.send(()));
                let _ = self.resume.lock().map(|rx| rx.recv());
            }
        }
        let _deadline = deadline(DEADLINE);
        let (mut s, _, fast, parked) = Scheduler::started(8);
        let (pushed, landed) = mpsc::channel();
        let (resume, stalled) = mpsc::channel();
        let slow = Producer {
            stack: fast.stack.clone(),
            unpark: Waker::from(Arc::new(Stalled {
                pushed: Mutex::new(pushed),
                resume: Mutex::new(stalled),
            })),
        };
        let pusher = thread::spawn(move || {
            wait_until(parked);
            let low = thread::spawn(move || slow.push(3));
            landed.recv().unwrap();
            fast.push(5);
            low
        });
        assert_eq!((s.next(), s.next()), (5, 3));
        resume.send(()).unwrap();
        pusher.join().unwrap().join().unwrap();
    }

    // ---- timers ----

    /// The first boundary pushes it, coalescing with init's push of 2.
    #[test]
    fn a_timer_armed_before_init_fires_with_the_first_batch() {
        let (uninit, local, _) = unstarted();
        local.context(2).timer.schedule_at(past());
        let mut s = uninit.init(4);
        assert_eq!(s.woken(), [0, 1, 2, 3]);
    }

    /// Armed before init, it can only be checked when it fires.
    #[test]
    fn a_timer_past_the_graph_is_dropped_when_it_fires() {
        let (mut s, local, _, _) = Scheduler::started(BITS + 2);
        local.context(BITS + 2).timer.schedule_at(past());
        assert!(s.woken().is_empty());
    }

    #[test]
    fn an_expired_timer_comes_out_without_parking() {
        let _deadline = deadline(DEADLINE);
        let (mut s, local, _, _) = Scheduler::started(14);
        local.context(13).timer.schedule_at(past());
        assert_eq!(s.next(), 13);
    }

    #[test]
    fn a_timer_due_now_fires_at_the_next_boundary() {
        let (mut s, local, _, _) = Scheduler::started(4);
        // The boundary reads the clock later; it never goes back.
        local.context(2).timer.schedule_at(Instant::now());
        assert_eq!(s.woken(), [2]);
    }

    /// Pushed in deadline order (each `past()` is later), so out in reverse.
    #[test]
    fn every_expired_timer_comes_out_once() {
        let (mut s, local, _, _) = Scheduler::started(4);
        for id in [3, 1, 2, 1] {
            local.context(id).timer.schedule_at(past());
        }
        assert_eq!(s.woken(), [2, 1, 3]);
    }

    #[test]
    fn an_expired_timer_on_an_id_still_queued_polls_it_once() {
        let (mut s, local, _, _) = Scheduler::started(4);
        wake(&s, 3);
        assert_eq!(s.try_next(), Some(3));
        // Queued: the timer's push coalesces.
        wake(&s, 2);
        local.context(2).timer.schedule_at(past());
        assert_eq!(s.woken(), [2]);
    }

    #[test]
    fn an_expired_timer_is_not_starved_by_busy_local_work() {
        let (mut s, local, _, _) = Scheduler::started(4);
        local.context(3).timer.schedule_at(past());
        // 0 wakes itself every poll: the stack is never empty.
        wake(&s, 0);
        let polled: Vec<_> = (0..3)
            .filter_map(|_| {
                let id = s.try_next();
                if id == Some(0) {
                    wake(&s, 0);
                }
                id
            })
            .collect();
        // Pushed at the boundary after 0's wake: out first.
        assert_eq!(polled, [3, 0, 0]);
    }

    #[test]
    fn the_earlier_of_two_timers_on_one_id_fires() {
        let _deadline = deadline(DEADLINE);
        let (mut s, local, _, _) = Scheduler::started(6);
        let start = Instant::now();
        local
            .context(5)
            .timer
            .schedule_at(start + Duration::from_millis(20));
        local
            .context(5)
            .timer
            .schedule_at(start + Duration::from_secs(60));
        assert_eq!(s.next(), 5);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(20), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    }

    #[test]
    fn a_cancelled_timer_never_fires() {
        let (mut s, local, _, _) = Scheduler::started(14);
        drop(local.context(13).timer.arm(past()));
        assert!(s.woken().is_empty());
        assert_eq!(s.timers.earliest(), None);
    }

    #[test]
    fn cancelling_one_of_two_timers_leaves_the_other_firing() {
        let (mut s, local, _, _) = Scheduler::started(3);
        let guard = local
            .context(1)
            .timer
            .arm(Instant::now() + Duration::from_secs(60));
        local.context(2).timer.schedule_at(past());
        drop(guard);
        assert_eq!(s.woken(), [2]);
        assert_eq!(s.timers.earliest(), None);
    }

    #[test]
    fn a_timed_park_yields_to_a_remote_wake() {
        let _deadline = deadline(DEADLINE);
        let (mut s, local, producer, parked) = Scheduler::started(BITS + 3);
        let due = Instant::now() + Duration::from_secs(60);
        local.context(1).timer.schedule_at(due);
        let handle = thread::spawn(move || {
            // Once it sleeps, so this wake is what releases it.
            wait_until(parked);
            producer.push(BITS + 1);
        });
        assert_eq!(s.next(), BITS + 1);
        handle.join().unwrap();
        // Out early, the timer still armed.
        assert_eq!(s.timers.earliest(), Some(due));
    }

    #[test]
    fn a_timer_and_a_remote_wake_due_together_poll_once() {
        let (mut s, local, producer, _) = Scheduler::started(BITS + 3);
        local.context(1).timer.schedule_at(Instant::now());
        producer.push(1);
        assert_eq!(s.woken(), [1]);
    }
}
