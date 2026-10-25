//! The poll thread's scheduler, with two-stage construction.
//!
//! Stage 1 (main thread): [PollQueue] creates [Producer]s for
//! cross-thread wakers. `PollQueue` is `Send` — crosses threads — but
//! not `Sync`: it owns the consumer's parker, which one thread parks on.
//!
//! Stage 2 (async thread): [PollQueue::local] consumes the config
//! and returns [Uninit] + [ThreadLocalProducer] for the async thread.
//! Once the graph is built, [Uninit::init] makes the [Consumer].
//!
//! - [Uninit]: the consumer before its arena exists, `!Send`, `!Sync`
//! - [Consumer]: the next woken id, parking while there is none, `!Send`, `!Sync`
//! - [Producer]: push from any thread, `Send + Sync + Clone`
//! - [ThreadLocalProducer]: local wakers and timers, `Clone`, `!Send`, `!Sync`

mod consumer;
mod core;
mod producer;
mod thread_local;
mod treiber;

pub use consumer::{Consumer, Uninit};
pub use producer::Producer;
pub use thread_local::ThreadLocalProducer;
pub(crate) use treiber::TreiberStack;

use crate::wake::Parker;
use ::core::cell::RefCell;
use alloc::rc::Rc;
use alloc::sync::Arc;

pub struct PollQueue {
    /// Shared by every producer, local waker and the consumer.
    stack: Arc<TreiberStack>,
    /// Built here, before the poll thread exists: producers take its waker now, the consumer
    /// takes the parker itself in [Self::local].
    parker: Parker,
}

impl Default for PollQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PollQueue {
    /// O(1): an `Arc` each for the stack (no nodes until init) and the parker.
    pub fn new() -> Self {
        Self {
            stack: Arc::new(TreiberStack::new()),
            parker: Parker::new(),
        }
    }

    /// O(1): two `Arc` clones, an RMW each.
    pub fn producer(&self) -> Producer {
        Producer {
            stack: self.stack.clone(),
            // Every producer wakes the one consumer.
            waker: self.parker.waker(),
        }
    }

    /// Consume config and create the async-thread handles.
    /// Must be called on the async thread — returns `!Send` types.
    /// O(1): an `Rc` for the scheduler, and an `Arc` clone.
    pub fn local(self) -> (Uninit, ThreadLocalProducer) {
        let stack = self.stack.clone();
        // The parker moves to the thread that parks on it; producers keep only its waker.
        let scheduler = Rc::new(RefCell::new(core::Scheduler::new(self.stack, self.parker)));
        (
            Uninit {
                inner: scheduler.clone(),
            },
            ThreadLocalProducer {
                inner: scheduler,
                stack,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::sync::tests::wait_until;
    use crate::poll::marker::NodeId;
    use crate::poll::wakers::thread_local::Waker;
    use crate::tests::support::{DEADLINE, allocations, deadline, live_bytes};
    use crate::wake::LocalWaker;
    use ::core::sync::atomic::{AtomicBool, Ordering};
    use ::core::time::Duration;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Instant;

    /// Ids per machine word: graph sizes here reach past it and its square.
    const BITS: usize = usize::BITS as usize;

    fn require_send<T: Send>() {}
    fn require_sync<T: Sync>() {}

    /// `<T as NotSend<_>>::check` resolves only for a `!Send` `T`: a `Send` one matches both
    /// impls, so `_` can't be inferred (E0283).
    trait NotSend<Which> {
        fn check() {}
    }
    impl<T: ?Sized> NotSend<()> for T {}
    impl<T: ?Sized + Send> NotSend<u8> for T {}

    /// The same for `Sync`.
    trait NotSync<Which> {
        fn check() {}
    }
    impl<T: ?Sized> NotSync<()> for T {}
    impl<T: ?Sized + Sync> NotSync<u8> for T {}

    /// The thread model's bounds, checked at compile time.
    const _: () = {
        // The config crosses to the poll thread, where one thread parks on its parker.
        let _: fn() = require_send::<PollQueue>;
        let _: fn() = <PollQueue as NotSync<_>>::check;
        let _: fn() = require_send::<Producer>;
        let _: fn() = require_sync::<Producer>;
        // Local wakes skip the parker: sound only while they and the consumer share its thread.
        let _: fn() = <Uninit as NotSend<_>>::check;
        let _: fn() = <Uninit as NotSync<_>>::check;
        let _: fn() = <Consumer as NotSend<_>>::check;
        let _: fn() = <Consumer as NotSync<_>>::check;
        let _: fn() = <ThreadLocalProducer as NotSend<_>>::check;
        let _: fn() = <ThreadLocalProducer as NotSync<_>>::check;
        let _: fn() = <LocalWaker as NotSend<_>>::check;
        let _: fn() = <LocalWaker as NotSync<_>>::check;
    };

    /// A queue whose graph is ids `0..len`, past their first polls; a producer waking it; and
    /// whether its consumer is parked, readable from another thread.
    fn started(
        len: usize,
    ) -> (
        Consumer,
        ThreadLocalProducer,
        Producer,
        impl Fn() -> bool + Send + 'static,
    ) {
        let queue = PollQueue::new();
        let producer = queue.producer();
        let parked = queue.parker.parked_probe();
        let (uninit, local) = queue.local();
        let mut consumer = uninit.init(len);
        assert_eq!(consumer.woken(), (0..len).collect::<Vec<NodeId>>());
        (consumer, local, producer, parked)
    }

    #[test]
    fn a_local_wake_comes_out_of_next() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, local, _, _) = started(43);
        Waker::local(42, &local).wake();
        assert_eq!(consumer.next(), 42);
    }

    #[test]
    fn local_and_cross_thread_wakes_both_come_out() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, local, producer, _) = started(4);
        Waker::local(1, &local).wake();
        Waker::local(2, &local).wake();
        thread::spawn(move || producer.push(3)).join().unwrap();
        // Newest first.
        assert_eq!(
            [consumer.next(), consumer.next(), consumer.next()],
            [3, 2, 1]
        );
    }

    #[test]
    fn cross_thread_blocks_until_woken() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, _, producer, parked) = started(BITS);
        let handle = thread::spawn(move || {
            // Wake only once the consumer sleeps, so the wake's notify is what releases it.
            wait_until(parked);
            producer.push(BITS - 1);
        });
        assert_eq!(consumer.next(), BITS - 1);
        handle.join().unwrap();
    }

    /// Many park/wake round trips: a push that skipped its wake while the consumer parked would
    /// sooner or later leave it asleep on a pushed id.
    #[test]
    fn cross_thread_ping_pong_never_loses_a_wake() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, _, producer, _) = started(BITS);
        let (ack, acked) = mpsc::channel::<()>();
        let handle = thread::spawn(move || {
            for round in 0..20_000 {
                producer.push(round % BITS);
                // One in flight at a time, so the consumer parks between wakes.
                acked.recv().unwrap();
            }
        });
        for round in 0..20_000 {
            assert_eq!(consumer.next(), round % BITS);
            ack.send(()).unwrap();
        }
        handle.join().unwrap();
    }

    /// Past `BITS` ids, remote and local wakes alike poll only their id.
    #[test]
    fn remote_and_local_wakes_poll_only_their_id() {
        let (mut consumer, local, producer, _) = started(2 * BITS + 2);
        producer.push(BITS + 1);
        assert_eq!(consumer.woken(), [BITS + 1]);
        Waker::local(BITS + 1, &local).wake();
        assert_eq!(consumer.woken(), [BITS + 1]);
    }

    /// As a build mints them.
    #[test]
    fn local_wakers_minted_before_init_push_only_their_id() {
        let queue = PollQueue::new();
        let (uninit, local) = queue.local();
        let wakers: Vec<_> = (0..BITS + 3).map(|id| Waker::local(id, &local)).collect();
        let mut consumer = uninit.init(BITS + 3);
        assert_eq!(consumer.woken().len(), BITS + 3);
        for id in [BITS + 2, 2, BITS] {
            wakers[id].wake();
        }
        assert_eq!(consumer.woken(), [BITS, 2, BITS + 2]);
    }

    /// Ids across a big graph, woken from another thread in reverse: one batch, newest first, so
    /// in id order.
    #[test]
    fn remote_wakes_come_out_newest_first() {
        let _deadline = deadline(DEADLINE);
        let len = BITS * BITS + 2 * BITS;
        let ids = [0, BITS - 1, BITS, BITS * BITS - 1, BITS * BITS, len - 1];
        let (mut consumer, _, producer, _) = started(len);
        thread::spawn(move || {
            for id in ids.into_iter().rev() {
                producer.push(id);
            }
        })
        .join()
        .unwrap();
        assert_eq!(consumer.woken(), ids);
    }

    /// A queue's whole life, local and remote wakes across a big graph, timers and parker
    /// included, frees all it made.
    #[test]
    fn a_queue_frees_all_it_made() {
        let life = || {
            let len = BITS * BITS + 2 * BITS;
            let ids = [0, BITS + 1, BITS * BITS + 1, len - 1];
            let (mut consumer, local, producer, _) = started(len);
            let wakers: Vec<_> = ids.iter().map(|&id| Waker::local(id, &local)).collect();
            for (&id, waker) in ids.iter().zip(&wakers) {
                producer.push(id);
                waker.wake();
            }
            let _ = local.schedule(1, Instant::now());
            let _ = local.schedule(2, Instant::now() + Duration::from_secs(60));
            // Newest first: the due timer, pushed at the boundary, then the wakes, each local one
            // coalesced into its remote one.
            assert_eq!(consumer.woken(), [1, len - 1, BITS * BITS + 1, BITS + 1, 0]);
        };
        // Anything made once per thread, such as by a first clock read, is made now.
        life();
        let before = live_bytes();
        life();
        assert_eq!(live_bytes(), before);
    }

    /// The nodes of a big graph come at init: one allocation, and none at the wakes after it.
    #[test]
    fn init_makes_the_nodes_in_one_block() {
        let len = BITS * BITS + 2 * BITS;
        let queue = PollQueue::new();
        let producer = queue.producer();
        let (uninit, local) = queue.local();
        let waker = Waker::local(len - 1, &local);
        let made = allocations();
        let mut consumer = uninit.init(len);
        assert_eq!(allocations() - made, 1);
        assert_eq!(consumer.woken().len(), len);
        let made = allocations();
        waker.wake();
        producer.push(BITS + 1);
        assert_eq!(allocations(), made, "a wake allocates nothing");
        // Newest first.
        assert_eq!(consumer.woken(), [BITS + 1, len - 1]);
    }

    // ---- stress ----

    /// Seeded xorshift: the same ids on every run.
    struct Rng(u64);

    impl Rng {
        /// The next number below `bound`.
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % bound as u64) as usize
        }
    }

    /// A flag per id, set before each wake and cleared by each poll of the id: one left set is a
    /// wake no poll followed.
    fn owed(len: usize) -> Arc<Vec<AtomicBool>> {
        Arc::new((0..len).map(|_| AtomicBool::new(false)).collect())
    }

    #[test]
    fn wakes_from_three_threads_are_all_served() {
        let len = 3 * BITS + 5;
        let (consumer, local, producer, _) = started(len);
        three_threads_serve_every_wake(len, 4_000, (local, producer), || consumer);
    }

    #[test]
    fn wakes_from_three_threads_into_a_big_graph_are_all_served() {
        let len = BITS * BITS + 3 * BITS + 5;
        let (consumer, local, producer, _) = started(len);
        three_threads_serve_every_wake(len, 4_000, (local, producer), || consumer);
    }

    /// Init lands while the threads already wake: wakes before it, during it and after it are
    /// all served, the first by init's first poll of every id.
    #[test]
    fn wakes_racing_init_are_all_served() {
        let len = 3 * BITS + 5;
        for _ in 0..5 {
            let queue = PollQueue::new();
            let producer = queue.producer();
            let (uninit, local) = queue.local();
            three_threads_serve_every_wake(len, 1_000, (local, producer), || uninit.init(len));
        }
    }

    /// Three threads wake random ids of a `len`-id graph, each thread its own ids, while the
    /// consumer's polls wake themselves and arm due timers: every wake is followed by a poll of
    /// its id. One wake in flight per id: a later wake of the id would hide a lost earlier one.
    /// `consumer` runs once the threads have woken some ids.
    fn three_threads_serve_every_wake(
        len: usize,
        rounds: usize,
        (local, producer): (ThreadLocalProducer, Producer),
        consumer: impl FnOnce() -> Consumer,
    ) {
        let _deadline = deadline(DEADLINE);
        // Never drawn: woken once, after every other wake, to end the test.
        let last = len - 1;
        let owed = owed(len);
        let done = Arc::new(AtomicBool::new(false));
        let wakes = thread::spawn({
            let (owed, done) = (owed.clone(), done.clone());
            move || {
                let threads: Vec<_> = (0..3)
                    .map(|first| {
                        let (producer, owed) = (producer.clone(), owed.clone());
                        thread::spawn(move || {
                            let mut rng = Rng(first as u64 + 1);
                            // This thread's ids: `first`, `first + 3`, … below `last`.
                            let count = (last - first).div_ceil(3);
                            for _ in 0..rounds {
                                let id = first + 3 * rng.below(count);
                                while owed[id].load(Ordering::SeqCst) {
                                    thread::yield_now();
                                }
                                owed[id].store(true, Ordering::SeqCst);
                                producer.push(id);
                            }
                        })
                    })
                    .collect();
                for handle in threads {
                    handle.join().unwrap();
                }
                done.store(true, Ordering::SeqCst);
                producer.push(last);
            }
        });
        wait_until(|| owed.iter().any(|owed| owed.load(Ordering::SeqCst)));
        let mut consumer = consumer();
        let mut rng = Rng(7);
        loop {
            let id = consumer.next();
            owed[id].store(false, Ordering::SeqCst);
            // Its first poll aside, `last` polls only after `done`.
            if id == last && done.load(Ordering::SeqCst) {
                break;
            }
            match rng.below(16) {
                0 => Waker::local(id, &local).wake(),
                1 => {
                    let _ = local.schedule(id, Instant::now());
                }
                _ => {}
            }
        }
        wakes.join().unwrap();
        // Every wake is made by now: poll what the last batches left.
        for id in consumer.woken() {
            owed[id].store(false, Ordering::SeqCst);
        }
        for (id, owed) in owed.iter().enumerate() {
            assert!(
                !owed.load(Ordering::SeqCst),
                "{id} woken, never polled after"
            );
        }
        assert!(consumer.woken().is_empty());
    }

    #[test]
    fn ping_pong_serves_every_wake() {
        ping_pong_serves_every_wake_of(2 * BITS + 3);
    }

    #[test]
    fn ping_pong_serves_every_wake_in_a_big_graph() {
        ping_pong_serves_every_wake_of(BITS * BITS + 2 * BITS + 3);
    }

    /// One remote wake in flight at a time, on ids of a `len`-id graph, the consumer parking
    /// between most: each is followed by a poll of its id, or the test hangs.
    fn ping_pong_serves_every_wake_of(len: usize) {
        let _deadline = deadline(DEADLINE);
        // Never drawn: woken once, after the rounds, to end the test.
        let last = len - 1;
        let (mut consumer, _, producer, _) = started(len);
        let owed = owed(len);
        let done = Arc::new(AtomicBool::new(false));
        let wakes = thread::spawn({
            let (owed, done) = (owed.clone(), done.clone());
            move || {
                let mut rng = Rng(11);
                for _ in 0..5_000 {
                    let id = rng.below(last);
                    owed[id].store(true, Ordering::SeqCst);
                    producer.push(id);
                    while owed[id].load(Ordering::SeqCst) {
                        thread::yield_now();
                    }
                }
                done.store(true, Ordering::SeqCst);
                producer.push(last);
            }
        });
        loop {
            let id = consumer.next();
            owed[id].store(false, Ordering::SeqCst);
            if id == last && done.load(Ordering::SeqCst) {
                break;
            }
        }
        wakes.join().unwrap();
        // Exact: nothing else was woken.
        assert!(consumer.woken().is_empty());
    }

    /// Timers alone drive the consumer, through one timed park after another.
    #[test]
    fn timer_ping_pong_wakes_through_timed_parks() {
        let _deadline = deadline(DEADLINE);
        let len = BITS + 3;
        let (mut consumer, local, _, _) = started(len);
        let mut rng = Rng(3);
        for _ in 0..100 {
            let id = rng.below(len);
            let start = Instant::now();
            let delay = Duration::from_micros(rng.below(1_000) as u64);
            let _ = local.schedule(id, start + delay);
            assert_eq!(consumer.next(), id);
            assert!(start.elapsed() >= delay);
            assert!(consumer.woken().is_empty());
        }
    }
}
