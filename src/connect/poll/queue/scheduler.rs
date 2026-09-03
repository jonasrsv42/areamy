//! [Scheduler] — shared orchestrator behind the consumer thread.
//!
//! One per `ThreadStream`, wrapped in `Rc<RefCell<Scheduler>>` and
//! shared by [Consumer](super::Consumer) and every
//! [ThreadLocalProducer](super::ThreadLocalProducer) clone.
//!
//! [Self::next] is timer-unaware on the fast path: pop, dispatch on
//! [Item]. A `DeadlinePending` pop hands off to [Self::timer_dispatch]
//! which owns all deadline state.
//!
//! `pending: bool` dedupes the cycling `DeadlinePending` sentinel —
//! exactly one is ever live in the queue.

use super::core::{Item, VyukovQueue, Wait};
use super::deadline::DeadlineHeap;
use super::timers::TimerKey;
use crate::connect::poll::marker::NodeId;
use crate::error::Error;

use alloc::sync::Arc;
use core::mem;
use std::time::Instant;

pub(super) struct Scheduler {
    // Inner thread-safe queue for scheduling work.
    queue: Arc<VyukovQueue>,
    // Current future-scheduled work.
    deadline: DeadlineHeap,
    /// True iff a `DeadlinePending` sentinel is live in the queue or
    /// being processed in [Self::timer_dispatch].
    pending: bool,
}

impl Scheduler {
    pub(super) fn new(queue: Arc<VyukovQueue>) -> Self {
        Self {
            queue,
            deadline: DeadlineHeap::new(),
            pending: false,
        }
    }

    pub(super) fn push(&mut self, id: NodeId) {
        self.queue.push(id);
    }

    /// Push the sentinel iff none is in flight — the single
    /// enforcement point of the "at most one sentinel live" invariant.
    fn arm(&mut self) {
        if !mem::replace(&mut self.pending, true) {
            self.queue.push_value(Item::DeadlinePending);
        }
    }

    /// Register a deadline; arm the sentinel.
    pub(super) fn schedule(&mut self, node_id: NodeId, deadline: Instant) -> TimerKey {
        let key = self.deadline.register(node_id, deadline);
        self.arm();
        key
    }

    /// Release a timer before it fires. Dead keys are a no-op. Any
    /// in-flight sentinel resolves itself: `on_deadline` peeks, finds
    /// nothing live, drops the coupon.
    pub(super) fn cancel(&mut self, key: TimerKey) {
        self.deadline.cancel(key);
    }

    /// Fast path: pop, dispatch, park. Delegates to [Self::timer_dispatch]
    /// only when a `DeadlinePending` sentinel surfaces.
    pub(super) fn next(&mut self) -> Result<NodeId, Error> {
        loop {
            match self.queue.try_pop_value() {
                Some(Item::Id(id)) => return Ok(id),
                Some(Item::DeadlinePending) => {
                    if let Some(id) = self.on_deadline()? {
                        return Ok(id);
                    }
                    // Heap was empty — coupon dropped; resume fast path.
                }
                None => self.queue.wait()?,
            }
        }
    }

    /// Caller has popped a `DeadlinePending` sentinel. Run the timer
    /// state machine until either a node is ready (`Ok(Some)`) or the
    /// heap is empty and the coupon should be dropped (`Ok(None)`).
    fn on_deadline(&mut self) -> Result<Option<NodeId>, Error> {
        // Sentinel just consumed — clear flag so re-arm sites below
        // can push a fresh one.
        self.pending = false;

        loop {
            // Expired entries first, every iteration: a busy queue
            // would otherwise starve them.
            if let Some(first) = self.schedule_expired() {
                return Ok(Some(first));
            }

            // Re-peek each iteration. `peek` drains stale; None here
            // means no fresh work — drop coupon, fast path resumes.
            let deadline = match self.deadline.peek() {
                None => return Ok(None),
                Some(d) => d,
            };

            match self.queue.try_pop_value() {
                Some(Item::Id(id)) => {
                    // Real wake in timer mode: re-arm so next next()
                    // re-enters and re-bounds its park.
                    self.arm();
                    return Ok(Some(id));
                }
                Some(Item::DeadlinePending) => {
                    // Unreachable under the dedup invariant; reset
                    // defensively and loop. Log under non-silent
                    // builds so the invariant violation is visible.
                    #[cfg(not(feature = "silent"))]
                    eprintln!(
                        "Scheduler::on_deadline: extra DeadlinePending sentinel — dedup invariant violated"
                    );
                    self.pending = false;
                }
                None => match self.queue.wait_until(deadline)? {
                    // Both arms loop back; top handles uniformly via
                    // drain_expired (on Timeout) or try_pop (on Signal).
                    Wait::Signal | Wait::Timeout => continue,
                },
            }
        }
    }

    /// Pop all expired entries: return the first, re-queue the rest
    /// as `Id`s. Re-arm the sentinel iff heap still has entries.
    fn schedule_expired(&mut self) -> Option<NodeId> {
        let now = Instant::now();
        // None = nothing expired; caller handles by peeking/parking.
        let first = self.deadline.pop(now)?;
        // Hand the rest back as plain wakes so the poll loop sees
        // them on subsequent next() iterations (FIFO behind anything
        // already queued).
        while let Some(id) = self.deadline.pop(now) {
            self.queue.push_value(Item::Id(id));
        }
        // Live entries remain: re-arm so the next next() re-enters
        // timer mode and re-bounds its park. peek is the liveness
        // truth — the heap has no is_empty (its raw len counts dead
        // entries).
        if self.deadline.peek().is_some() {
            self.arm();
        }
        Some(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn sched() -> Scheduler {
        Scheduler::new(Arc::new(VyukovQueue::new()))
    }

    // ---- fast path ----

    #[test]
    fn next_returns_pushed_id() {
        let mut s = sched();
        s.queue.push(42);
        assert_eq!(s.next().unwrap(), 42);
    }

    #[test]
    fn next_blocks_until_cross_thread_push() {
        let mut s = sched();
        let shared = s.queue.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            shared.push(7);
            shared.signal().unwrap();
        });
        assert_eq!(s.next().unwrap(), 7);
        handle.join().unwrap();
    }

    // ---- sentinel dedup ----

    #[test]
    fn schedule_pushes_sentinel_on_first_arm() {
        let mut s = sched();
        s.schedule(0, Instant::now() + Duration::from_secs(60));
        assert!(matches!(
            s.queue.try_pop_value(),
            Some(Item::DeadlinePending)
        ));
        assert!(s.queue.try_pop_value().is_none());
    }

    #[test]
    fn schedule_repeated_pushes_only_one_sentinel() {
        let mut s = sched();
        for n in 0..50 {
            s.schedule(n, Instant::now() + Duration::from_secs(60));
        }
        let mut count = 0;
        while let Some(v) = s.queue.try_pop_value() {
            if matches!(v, Item::DeadlinePending) {
                count += 1;
            }
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn schedule_after_sentinel_consumed_pushes_a_new_one() {
        let mut s = sched();
        s.schedule(0, Instant::now() + Duration::from_secs(60));
        assert!(matches!(
            s.queue.try_pop_value(),
            Some(Item::DeadlinePending)
        ));
        s.pending = false; // simulate next() consuming it
        s.schedule(1, Instant::now() + Duration::from_secs(60));
        assert!(matches!(
            s.queue.try_pop_value(),
            Some(Item::DeadlinePending)
        ));
    }

    // ---- timer dispatch ----

    #[test]
    fn empty_heap_sentinel_drops_coupon() {
        let mut s = sched();
        s.queue.push_value(Item::DeadlinePending);
        s.pending = true;
        s.queue.push(99);
        assert_eq!(s.next().unwrap(), 99);
    }

    #[test]
    fn deadline_in_past_drains_and_returns_node() {
        let mut s = sched();
        let past = Instant::now() - Duration::from_millis(50);
        s.schedule(13, past);
        let start = Instant::now();
        assert_eq!(s.next().unwrap(), 13);
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn multi_expiry_returns_first_queues_rest() {
        let mut s = sched();
        let past = Instant::now() - Duration::from_millis(50);
        s.schedule(1, past);
        s.schedule(2, past);
        s.schedule(3, past);
        let mut got = [s.next().unwrap(), s.next().unwrap(), s.next().unwrap()];
        got.sort();
        assert_eq!(got, [1, 2, 3]);
    }

    #[test]
    fn expired_deadline_not_starved_by_busy_queue() {
        // Regression: a routine that pushes a wake every poll keeps
        // the queue non-empty. The expired heap entry (node 999, an
        // id never pushed via queue.push) must still be served —
        // otherwise it's starved forever.
        let mut s = sched();
        let past = Instant::now() - Duration::from_millis(50);
        s.schedule(999, past);
        for i in 0..100 {
            s.queue.push(i);
            if s.next().unwrap() == 999 {
                return;
            }
        }
        panic!("deadline node 999 was never served despite past deadline");
    }

    #[test]
    fn same_node_earliest_of_two_deadlines_fires() {
        // Regression: per-node supersede semantics let a later
        // registration silently cancel an earlier one — the earlier
        // deadline fired late (or never). Timers are now independent.
        let mut s = sched();
        s.schedule(5, Instant::now() + Duration::from_millis(20));
        s.schedule(5, Instant::now() + Duration::from_secs(60));
        let start = Instant::now();
        assert_eq!(s.next().unwrap(), 5);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn cancelled_deadline_never_wakes() {
        let mut s = sched();
        let key = s.schedule(13, Instant::now() - Duration::from_millis(50));
        s.cancel(key);
        // Sentinel is still queued; on_deadline finds an empty heap,
        // drops the coupon, and the real wake surfaces.
        s.queue.push(7);
        assert_eq!(s.next().unwrap(), 7);
        assert_eq!(s.deadline.peek(), None);
    }

    #[test]
    fn cancel_one_of_two_leaves_other_firing() {
        let mut s = sched();
        let key = s.schedule(1, Instant::now() + Duration::from_secs(60));
        s.schedule(2, Instant::now() - Duration::from_millis(50));
        s.cancel(key);
        assert_eq!(s.next().unwrap(), 2);
        assert_eq!(s.deadline.peek(), None);
    }

    #[test]
    fn future_deadline_park_yields_to_real_wake() {
        let mut s = sched();
        s.schedule(99, Instant::now() + Duration::from_secs(60));
        let shared = s.queue.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            shared.push(7);
            shared.signal().unwrap();
        });
        assert_eq!(s.next().unwrap(), 7);
        assert!(s.pending, "sentinel must be re-armed");
        handle.join().unwrap();
    }
}
