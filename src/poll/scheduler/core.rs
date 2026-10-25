//! [Scheduler] — the orchestrator behind the poll thread's [Consumer](super::Consumer) and every
//! [ThreadLocalProducer](super::ThreadLocalProducer).
//!
//! One per poll thread, in an `Rc<RefCell<Scheduler>>`. [Scheduler::next] walks a batch taken from
//! the [TreiberStack]; at each boundary it pushes expired timers, then takes again.
//!
//! A batch is every id pushed since the last take, newest first, each at most once: an id that
//! wakes itself waits a batch.
//!
//! Costs: n graph ids, h timer heap entries (dead ones included).

use super::treiber::{Link, TreiberStack};
use crate::poll::marker::NodeId;
use crate::poll::timer::TimerKey;
use crate::poll::timer::deadline::DeadlineHeap;
use crate::wake::Parker;

use alloc::sync::Arc;
use std::time::Instant;

pub(super) struct Scheduler {
    /// Woken ids, pushed from any thread.
    stack: Arc<TreiberStack>,
    /// The batch being walked.
    batch: Link,
    /// Timers; expired ones are pushed at each batch boundary.
    deadline: DeadlineHeap,
    /// Parked on while a take finds the stack empty; every remote push wakes it.
    ///
    /// # Same-thread assumption
    ///
    /// Local pushes and timers never wake it. They run on this thread, inside polls, which only
    /// run after `park` returns. `!Send` on [ThreadLocalProducer](super::ThreadLocalProducer) and
    /// [Consumer](super::Consumer) keeps them on one thread; a local push during a park would
    /// never be served.
    parker: Parker,
}

impl Scheduler {
    /// O(1): no allocation.
    pub(super) fn new(stack: Arc<TreiberStack>, parker: Parker) -> Self {
        Self {
            stack,
            batch: Link::NONE,
            deadline: DeadlineHeap::new(),
            parker,
        }
    }

    /// The graph is ids `0..len`; each polls once first, in id order. O(n).
    pub(super) fn init(&mut self, len: usize) {
        self.stack.init(len);
        // Last to first, so the first batch runs in id order.
        for id in (0..len).rev() {
            self.stack.push(id);
        }
    }

    /// Registers a wake at `deadline` for `node_id`. O(log h); no RMW.
    pub(super) fn schedule(&mut self, node_id: NodeId, deadline: Instant) -> TimerKey {
        self.deadline.register(node_id, deadline)
    }

    /// Releases a timer before it fires; a dead key is a no-op. O(1).
    pub(super) fn cancel(&mut self, key: TimerKey) {
        self.deadline.cancel(key);
    }

    /// The next id to poll; parks while none is pushed. O(1) per id.
    pub(super) fn next(&mut self) -> NodeId {
        loop {
            if let Some(id) = self.try_next() {
                return id;
            }
            // The earliest timer bounds the park.
            match self.deadline.peek() {
                Some(deadline) => self.parker.park_until(deadline),
                None => self.parker.park(),
            }
        }
    }

    /// The next id to poll, or `None` where [Self::next] parks. O(1); a batch boundary adds a
    /// [Self::push_expired] and a take.
    fn try_next(&mut self) -> Option<NodeId> {
        if let Some(id) = self.stack.next(&mut self.batch) {
            return Some(id);
        }
        self.push_expired();
        self.batch = self.stack.take();
        self.stack.next(&mut self.batch)
    }

    /// Pushes every expired timer's id, last before the take, so they lead the next batch.
    /// O(k log h), k expired; nothing due: a heap peek, and a clock read while a timer is live.
    fn push_expired(&mut self) {
        // `peek` drops dead entries: only a live one is worth a clock read.
        if self.deadline.peek().is_some() {
            let now = Instant::now();
            while let Some(id) = self.deadline.pop(now) {
                self.stack.push(id);
            }
        }
    }
}

#[cfg(test)]
impl Scheduler {
    /// What [Self::next] hands out before it parks; fails if it never would.
    pub(super) fn woken(&mut self) -> Vec<NodeId> {
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
    use crate::poll::scheduler::Producer;
    use crate::tests::support::{DEADLINE, allocations, deadline, live_bytes};
    use alloc::task::Wake;
    use core::task::Waker;
    use core::time::Duration;
    use std::sync::{Mutex, mpsc};
    use std::thread;

    const BITS: usize = usize::BITS as usize;

    /// Ids `0..len`, past their first polls; a producer; whether it's parked.
    fn started(len: usize) -> (Scheduler, Producer, impl Fn() -> bool + Send + 'static) {
        let stack = Arc::new(TreiberStack::new());
        let parker = Parker::new();
        let producer = Producer {
            stack: stack.clone(),
            waker: parker.waker(),
        };
        let parked = parker.parked_probe();
        let mut s = Scheduler::new(stack, parker);
        s.init(len);
        assert_eq!(s.woken(), (0..len).collect::<Vec<_>>());
        (s, producer, parked)
    }

    fn unstarted() -> (Scheduler, Producer) {
        let stack = Arc::new(TreiberStack::new());
        let parker = Parker::new();
        let producer = Producer {
            stack: stack.clone(),
            waker: parker.waker(),
        };
        (Scheduler::new(stack, parker), producer)
    }

    /// As a local waker pushes.
    fn wake(s: &Scheduler, id: NodeId) {
        s.stack.push(id);
    }

    fn past() -> Instant {
        Instant::now() - Duration::from_millis(50)
    }

    // ---- local and remote wakes ----

    #[test]
    fn local_and_remote_wakes_come_out_newest_first() {
        let (mut s, producer, _) = started(4);
        producer.push(0);
        wake(&s, 2);
        wake(&s, 1);
        assert_eq!(s.woken(), [1, 2, 0]);
    }

    #[test]
    fn a_one_id_graph_takes_remote_wakes() {
        let (mut s, producer, _) = started(1);
        producer.push(0);
        assert_eq!(s.woken(), [0]);
    }

    #[test]
    fn a_remote_wake_of_an_id_still_queued_polls_it_once() {
        let (mut s, producer, _) = started(4);
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
        let (mut s, producer) = unstarted();
        producer.push(1);
        s.init(3);
        assert_eq!(s.woken(), [0, 1, 2]);
        producer.push(1);
        assert_eq!(s.woken(), [1]);
    }

    /// The wake finds the nodes: pushed first, so out last, and signalled.
    #[test]
    fn a_remote_wake_between_the_nodes_and_the_first_pushes_is_first() {
        let (mut s, producer) = unstarted();
        s.stack.init(3);
        producer.push(1);
        for id in (0..3).rev() {
            s.stack.push(id);
        }
        assert_eq!(s.woken(), [0, 2, 1]);
        assert!(s.parker.take_woken());
    }

    #[test]
    fn init_makes_the_nodes_in_one_allocation() {
        let before = live_bytes();
        let (mut s, _) = unstarted();
        let made = allocations();
        s.init(BITS * BITS + 2);
        assert_eq!(allocations() - made, 1);
        drop(s);
        assert_eq!(live_bytes(), before);
    }

    #[test]
    fn next_parks_until_a_remote_wake() {
        let _deadline = deadline(DEADLINE);
        let (mut s, producer, parked) = started(8);
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
        let (mut s, producer, parked) = started(8);
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
        let (mut s, fast, parked) = started(8);
        let (pushed, landed) = mpsc::channel();
        let (resume, stalled) = mpsc::channel();
        let slow = Producer {
            stack: fast.stack.clone(),
            waker: Waker::from(Arc::new(Stalled {
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
        let (mut s, _) = unstarted();
        let _ = s.schedule(2, past());
        s.init(4);
        assert_eq!(s.woken(), [0, 1, 2, 3]);
    }

    /// Armed before init, it can only be checked when it fires.
    #[test]
    fn a_timer_past_the_graph_is_dropped_when_it_fires() {
        let (mut s, _, _) = started(BITS + 2);
        let _ = s.schedule(BITS + 2, past());
        assert!(s.woken().is_empty());
    }

    #[test]
    fn an_expired_timer_comes_out_without_parking() {
        let _deadline = deadline(DEADLINE);
        let (mut s, _, _) = started(14);
        let _ = s.schedule(13, past());
        assert_eq!(s.next(), 13);
    }

    #[test]
    fn a_timer_due_now_fires_at_the_next_boundary() {
        let (mut s, _, _) = started(4);
        // The boundary reads the clock later; it never goes back.
        let _ = s.schedule(2, Instant::now());
        assert_eq!(s.woken(), [2]);
    }

    /// Pushed in deadline order (each `past()` is later), so out in reverse.
    #[test]
    fn every_expired_timer_comes_out_once() {
        let (mut s, _, _) = started(4);
        for id in [3, 1, 2, 1] {
            let _ = s.schedule(id, past());
        }
        assert_eq!(s.woken(), [2, 1, 3]);
    }

    #[test]
    fn an_expired_timer_on_an_id_still_queued_polls_it_once() {
        let (mut s, _, _) = started(4);
        wake(&s, 3);
        assert_eq!(s.try_next(), Some(3));
        // Queued: the timer's push coalesces.
        wake(&s, 2);
        let _ = s.schedule(2, past());
        assert_eq!(s.woken(), [2]);
    }

    #[test]
    fn an_expired_timer_is_not_starved_by_busy_local_work() {
        let (mut s, _, _) = started(4);
        let _ = s.schedule(3, past());
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
        let (mut s, _, _) = started(6);
        let start = Instant::now();
        let _ = s.schedule(5, start + Duration::from_millis(20));
        let _ = s.schedule(5, start + Duration::from_secs(60));
        assert_eq!(s.next(), 5);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(20), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    }

    #[test]
    fn a_cancelled_timer_never_fires() {
        let (mut s, _, _) = started(14);
        let key = s.schedule(13, past());
        s.cancel(key);
        assert!(s.woken().is_empty());
        assert_eq!(s.deadline.peek(), None);
    }

    #[test]
    fn cancelling_one_of_two_timers_leaves_the_other_firing() {
        let (mut s, _, _) = started(3);
        let key = s.schedule(1, Instant::now() + Duration::from_secs(60));
        let _ = s.schedule(2, past());
        s.cancel(key);
        assert_eq!(s.woken(), [2]);
        assert_eq!(s.deadline.peek(), None);
    }

    #[test]
    fn a_timed_park_yields_to_a_remote_wake() {
        let _deadline = deadline(DEADLINE);
        let (mut s, producer, parked) = started(BITS + 3);
        let due = Instant::now() + Duration::from_secs(60);
        let _ = s.schedule(1, due);
        let handle = thread::spawn(move || {
            // Once it sleeps, so this wake is what releases it.
            wait_until(parked);
            producer.push(BITS + 1);
        });
        assert_eq!(s.next(), BITS + 1);
        handle.join().unwrap();
        // Out early, the timer still armed.
        assert_eq!(s.deadline.peek(), Some(due));
    }

    #[test]
    fn a_timer_and_a_remote_wake_due_together_poll_once() {
        let (mut s, producer, _) = started(BITS + 3);
        let _ = s.schedule(1, Instant::now());
        producer.push(1);
        assert_eq!(s.woken(), [1]);
    }
}
