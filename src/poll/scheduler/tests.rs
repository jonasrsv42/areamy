use super::*;
use crate::edge::sync::tests::wait_until;
use crate::tests::support::{DEADLINE, allocations, deadline, live_bytes};
use crate::wake::LocalWaker;
use ::core::sync::atomic::{AtomicBool, Ordering};
use ::core::time::Duration;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

/// A size unit for test graphs; sizes here reach past it and its square.
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
    // Made with the thread, it crosses to the poll thread, where one thread parks on its parker.
    let _: fn() = require_send::<Scheduler>;
    let _: fn() = <Scheduler as NotSync<_>>::check;
    let _: fn() = require_send::<Producer>;
    let _: fn() = require_sync::<Producer>;
    // Local wakes skip the parker: sound only while they and the consumer share its thread.
    let _: fn() = <Uninit as NotSend<_>>::check;
    let _: fn() = <Uninit as NotSync<_>>::check;
    let _: fn() = <Consumer as NotSend<_>>::check;
    let _: fn() = <Consumer as NotSync<_>>::check;
    let _: fn() = <Local as NotSend<_>>::check;
    let _: fn() = <Local as NotSync<_>>::check;
    let _: fn() = <LocalWaker as NotSend<_>>::check;
    let _: fn() = <LocalWaker as NotSync<_>>::check;
};

#[test]
fn a_local_wake_comes_out_of_next() {
    let _deadline = deadline(DEADLINE);
    let (mut consumer, local, _, _) = Scheduler::started(43);
    local.context(42).wake.local.wake();
    assert_eq!(consumer.next(), 42);
}

#[test]
fn local_and_cross_thread_wakes_both_come_out() {
    let _deadline = deadline(DEADLINE);
    let (mut consumer, local, producer, _) = Scheduler::started(4);
    local.context(1).wake.local.wake();
    local.context(2).wake.local.wake();
    thread::spawn(move || producer.push(3)).join().unwrap();
    // Newest first.
    assert_eq!(
        [consumer.next(), consumer.next(), consumer.next()],
        [3, 2, 1]
    );
}

#[test]
fn a_sync_waker_wakes_its_id_from_any_thread() {
    let _deadline = deadline(DEADLINE);
    let (mut consumer, local, _, parked) = Scheduler::started(8);
    let waker = local.context(6).wake.sync;
    let handle = thread::spawn(move || {
        wait_until(parked);
        waker.wake();
    });
    assert_eq!(consumer.next(), 6);
    handle.join().unwrap();
}

#[test]
fn cross_thread_blocks_until_woken() {
    let _deadline = deadline(DEADLINE);
    let (mut consumer, _, producer, parked) = Scheduler::started(BITS);
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
    let (mut consumer, _, producer, _) = Scheduler::started(BITS);
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
    let (mut consumer, local, producer, _) = Scheduler::started(2 * BITS + 2);
    producer.push(BITS + 1);
    assert_eq!(consumer.woken(), [BITS + 1]);
    local.context(BITS + 1).wake.local.wake();
    assert_eq!(consumer.woken(), [BITS + 1]);
}

/// As a build mints them.
#[test]
fn local_wakers_minted_before_init_push_only_their_id() {
    let (uninit, local) = Scheduler::new().local();
    let wakers: Vec<_> = (0..BITS + 3)
        .map(|id| local.context(id).wake.local)
        .collect();
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
    let (mut consumer, _, producer, _) = Scheduler::started(len);
    thread::spawn(move || {
        for id in ids.into_iter().rev() {
            producer.push(id);
        }
    })
    .join()
    .unwrap();
    assert_eq!(consumer.woken(), ids);
}

/// A scheduler's whole life, local and remote wakes across a big graph, timers and parker
/// included, frees all it made.
#[test]
fn a_scheduler_frees_all_it_made() {
    let life = || {
        let len = BITS * BITS + 2 * BITS;
        let ids = [0, BITS + 1, BITS * BITS + 1, len - 1];
        let (mut consumer, local, producer, _) = Scheduler::started(len);
        let wakers: Vec<_> = ids.iter().map(|&id| local.context(id).wake.local).collect();
        for (&id, waker) in ids.iter().zip(&wakers) {
            producer.push(id);
            waker.wake();
        }
        local.context(1).timer.schedule_at(Instant::now());
        local
            .context(2)
            .timer
            .schedule_at(Instant::now() + Duration::from_secs(60));
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
    let scheduler = Scheduler::new();
    let producer = scheduler.producer().clone();
    let (uninit, local) = scheduler.local();
    let waker = local.context(len - 1).wake.local;
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
    let (consumer, local, producer, _) = Scheduler::started(len);
    three_threads_serve_every_wake(len, 4_000, (local, producer), || consumer);
}

#[test]
fn wakes_from_three_threads_into_a_big_graph_are_all_served() {
    let len = BITS * BITS + 3 * BITS + 5;
    let (consumer, local, producer, _) = Scheduler::started(len);
    three_threads_serve_every_wake(len, 4_000, (local, producer), || consumer);
}

/// Init lands while the threads already wake: wakes before it, during it and after it are
/// all served, the first by init's first poll of every id.
#[test]
fn wakes_racing_init_are_all_served() {
    let len = 3 * BITS + 5;
    for _ in 0..5 {
        let scheduler = Scheduler::new();
        let producer = scheduler.producer().clone();
        let (uninit, local) = scheduler.local();
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
    (local, producer): (Local, Producer),
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
            0 => local.context(id).wake.local.wake(),
            1 => local.context(id).timer.schedule_at(Instant::now()),
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
    let (mut consumer, _, producer, _) = Scheduler::started(len);
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
    let (mut consumer, local, _, _) = Scheduler::started(len);
    let mut rng = Rng(3);
    for _ in 0..100 {
        let id = rng.below(len);
        let start = Instant::now();
        let delay = Duration::from_micros(rng.below(1_000) as u64);
        local.context(id).timer.schedule_at(start + delay);
        assert_eq!(consumer.next(), id);
        assert!(start.elapsed() >= delay);
        assert!(consumer.woken().is_empty());
    }
}
