use super::shared::Shared;
use super::*;
use crate::Trackable;
use crate::error::{Error, ErrorKind};
use crate::graph::{Closeable, Get, Pushable, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::poll::Sink as PollSink;
use crate::wake::mock::{counting_waker, noop_local_waker};
use crate::wake::{self, Parker};
use crate::work::Sink;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Wake, Waker as SyncWaker};
use std::thread;
use std::time::{Duration, Instant};

type TestSignal = Trackable<&'static str>;

const TIMEOUT: Duration = Duration::from_secs(5);

fn bounded(bound: usize) -> Receiver<usize, TestSignal> {
    Receiver::bounded(NonZeroUsize::new(bound).unwrap())
}

/// Spin until `condition` holds; fails after `TIMEOUT` so a broken edge doesn't hang.
pub(crate) fn wait_until(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting");
        thread::sleep(Duration::from_millis(1));
    }
}

/// Join a thread, failing instead of hanging if it never finishes.
pub(crate) fn join_within<T>(handle: thread::JoinHandle<T>) -> T {
    wait_until(|| handle.is_finished());
    handle.join().unwrap()
}

fn is_closed(result: Result<(), Error>) -> bool {
    matches!(
        result,
        Err(Error {
            kind: ErrorKind::Closed,
            ..
        })
    )
}

#[test]
fn try_push_full_at_bound() {
    let rx = bounded(2);
    let mut tx = rx.sender();
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert_eq!(tx.try_push_back(Message::Data(2)).unwrap(), TryPush::Pushed);
    assert_eq!(
        tx.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Full(Message::Data(3))
    );
    assert_eq!(rx.len().unwrap(), 2);
}

#[test]
fn pop_makes_room() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    assert_eq!(tx.try_push_back(Message::Data(2)).unwrap(), TryPush::Pushed);
}

#[test]
fn signals_count_toward_bound() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    assert_eq!(
        tx.try_push_back(Message::Flush("f".into())).unwrap(),
        TryPush::Pushed
    );
    assert_eq!(
        tx.try_push_back(Message::Data(1)).unwrap(),
        TryPush::Full(Message::Data(1))
    );
}

#[test]
fn refused_again_without_pop_is_stuck() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(Message::Data(2))
    );
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Stuck(Message::Data(2))
    );
}

/// Refused, then `pop` frees a slot that another producer refills: the retry must be `Full`
/// (something drained), and a further retry `Stuck` (nothing drained since).
fn pop_between_refusals_gives_full(pop: impl Fn(&Receiver<usize, TestSignal>)) {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let other = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(Message::Data(2))
    );
    pop(&rx);
    other.push_back(Message::Data(3)).unwrap();
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(Message::Data(2))
    );
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Stuck(Message::Data(2))
    );
}

#[test]
fn read_front_counts_as_progress() {
    pop_between_refusals_gives_full(|rx| {
        rx.read_front().unwrap();
    });
}

#[test]
fn poll_counts_as_progress() {
    pop_between_refusals_gives_full(|rx| {
        rx.poll().unwrap();
    });
}

#[test]
fn read_all_counts_as_progress() {
    pop_between_refusals_gives_full(|rx| {
        rx.read_all().unwrap();
    });
}

#[test]
fn each_sender_tracks_its_own_refusals() {
    let rx = bounded(1);
    let (mut a, mut b) = (rx.sender(), rx.sender());
    a.push_back(Message::Data(1)).unwrap();
    assert_eq!(
        a.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(Message::Data(2))
    );
    // B's first refusal, though A was just refused at the same count.
    assert_eq!(
        b.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Full(Message::Data(3))
    );
    // A clone is a new producer, not A.
    let mut c = a.clone();
    assert_eq!(
        c.try_push_back(Message::Data(4)).unwrap(),
        TryPush::Full(Message::Data(4))
    );
    assert_eq!(
        a.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Stuck(Message::Data(2))
    );
}

#[test]
fn closed_beats_full() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    rx.close().unwrap();
    assert!(matches!(
        tx.try_push_back(Message::Data(2)),
        Err(Error {
            kind: ErrorKind::Closed,
            ..
        })
    ));
}

#[test]
fn blocking_push_waits_for_read_front() {
    let rx = bounded(1);
    // Kept alive: dropping the last sender would close the edge.
    let first = rx.sender();
    first.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

    wait_until(|| rx.producers_waiting() == 1);
    // Still blocked: the second message isn't queued yet.
    assert_eq!(rx.len().unwrap(), 1);

    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    join_within(producer).unwrap();
    // Counted out: a stale count would make every later pop notify.
    assert_eq!(rx.producers_waiting(), 0);
    assert_eq!(rx.read_front().unwrap(), Message::Data(2));
}

#[test]
fn poll_wakes_blocked_producer() {
    let rx = bounded(1);
    // Kept alive: dropping the last sender would close the edge.
    let first = rx.sender();
    first.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

    wait_until(|| rx.producers_waiting() == 1);
    assert_eq!(rx.poll().unwrap(), Some(Message::Data(1)));
    join_within(producer).unwrap();
    assert_eq!(rx.poll().unwrap(), Some(Message::Data(2)));
}

#[test]
fn read_all_wakes_every_blocked_producer() {
    let rx = bounded(2);
    let tx = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    tx.push_back(Message::Data(2)).unwrap();
    let (tx3, tx4) = (rx.sender(), rx.sender());
    let first = thread::spawn(move || tx3.push_back(Message::Data(3)));
    let second = thread::spawn(move || tx4.push_back(Message::Data(4)));

    wait_until(|| rx.producers_waiting() == 2);
    assert_eq!(
        rx.read_all().unwrap(),
        vec![Message::Data(1), Message::Data(2)]
    );
    // Both freed slots are taken without any further pop.
    join_within(first).unwrap();
    join_within(second).unwrap();

    let mut rest = Message::data_from_iter(rx.read_all().unwrap().into_iter());
    rest.sort();
    assert_eq!(rest, vec![3, 4]);
}

#[test]
fn receiver_close_releases_blocked_producer() {
    let rx = bounded(1);
    // Kept alive: dropping the last sender would close the edge.
    let first = rx.sender();
    first.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

    wait_until(|| rx.producers_waiting() == 1);
    rx.close().unwrap();
    assert!(is_closed(join_within(producer)));
}

#[test]
fn receiver_drop_releases_blocked_producer() {
    let rx = bounded(1);
    // Kept alive: dropping the last sender would close the edge.
    let first = rx.sender();
    first.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

    wait_until(|| rx.producers_waiting() == 1);
    drop(rx);
    assert!(is_closed(join_within(producer)));
}

/// One producer finishing doesn't end the others: a blocked producer stays blocked until room,
/// and the edge closes only after the last producer is done.
#[test]
fn sender_close_leaves_other_producers_open() {
    let rx = bounded(1);
    let mut closer = rx.sender();
    closer.push_back(Message::Data(1)).unwrap();
    let mut tx = rx.sender();
    let producer = thread::spawn(move || {
        tx.push_back(Message::Data(2))?;
        tx.close()
    });

    wait_until(|| rx.producers_waiting() == 1);
    closer.close().unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    join_within(producer).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(2));
    assert!(is_closed(rx.read_front().map(drop)));
}

/// Pushes through a closed handle fail, and its clones are done too: they can't reopen it.
#[test]
fn a_closed_sender_and_its_clones_are_done() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut tx = rx.sender();
    let other = rx.sender();
    tx.close().unwrap();
    let clone = tx.clone();
    assert!(is_closed(tx.push_back(Message::Data(1))));
    assert!(is_closed(clone.push_back(Message::Data(1))));
    // The other producer keeps the edge open.
    other.push_back(Message::Data(2)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(2));
    drop((clone, other));
    assert!(is_closed(rx.read_front().map(drop)));
}

#[test]
fn bound_holds_under_concurrent_producers() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: usize = 1000;
    let rx = bounded(3);
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|id| {
            let tx = rx.sender();
            thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    tx.push_back(Message::Data(id * PER_PRODUCER + seq))
                        .unwrap();
                }
            })
        })
        .collect();

    // On its own thread so a stuck edge fails the test via `join_within` instead of hanging.
    // Ends with Closed once every producer has finished and dropped its sender.
    let consumer = thread::spawn(move || {
        let mut next = [0; PRODUCERS];
        let mut received = 0;
        loop {
            assert!(rx.len().unwrap() <= 3);
            let Ok(message) = rx.read_front() else { break };
            let value = message.data().unwrap();
            let (id, seq) = (value / PER_PRODUCER, value % PER_PRODUCER);
            // Each producer's messages arrive once, in order.
            assert_eq!(seq, next[id]);
            next[id] += 1;
            received += 1;
        }
        received
    });
    assert_eq!(join_within(consumer), PRODUCERS * PER_PRODUCER);
    for producer in producers {
        join_within(producer);
    }
}

#[test]
fn unbounded_never_full() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut tx = rx.sender();
    for value in 0..10_000 {
        assert_eq!(
            tx.try_push_back(Message::Data(value)).unwrap(),
            TryPush::Pushed
        );
    }
    assert_eq!(rx.len().unwrap(), 10_000);
}

#[test]
fn unbounded_sender_always_pushes() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut tx = rx.sender();
    assert_eq!(tx.try_push(Message::Data(5)).unwrap(), TryPush::Pushed);
    assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
}

fn _assert_sender_send_sync() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<Sender<usize, TestSignal>>();
}

fn _assert_receiver_send() {
    fn require_send<T: Send>() {}
    require_send::<Receiver<usize, TestSignal>>();
}

#[test]
fn roundtrip() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    tx.push_back(Message::Data(42)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(42));
}

#[test]
fn dropping_last_sender_closes_receiver() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    drop(tx);
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn dropping_last_sender_unblocks_waiting_receiver() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();

    let received = Arc::new(AtomicBool::new(false));
    let received_clone = received.clone();

    let reader = thread::spawn(move || {
        let result = rx.read_front();
        received_clone.store(true, Ordering::SeqCst);
        result
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!received.load(Ordering::SeqCst));

    drop(tx);

    let result = reader.join().unwrap();
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn dropping_receiver_closes_sender() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    drop(rx);
    assert!(matches!(
        tx.push_back(Message::Data(1)).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn multi_sender_only_closes_on_last_drop() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let tx2 = tx.clone();
    let tx3 = tx.clone();

    drop(tx);
    drop(tx2);

    tx3.push_back(Message::Data(7)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(7));

    drop(tx3);

    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn clone_does_not_close_prematurely() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    for _ in 0..10 {
        let cloned = tx.clone();
        drop(cloned);
    }
    tx.push_back(Message::Data(1)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
}

#[test]
fn explicit_close_on_sender() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut tx = rx.sender();
    tx.close().unwrap();
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
    tx.close().unwrap();
}

#[test]
fn explicit_close_on_receiver() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    rx.close().unwrap();
    assert!(matches!(
        tx.push_back(Message::Data(1)).unwrap_err().kind,
        ErrorKind::Closed
    ));
    rx.close().unwrap();
}

#[test]
fn buffered_data_readable_after_sender_drop() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    tx.push_back(Message::Data(2)).unwrap();
    drop(tx);

    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    assert_eq!(rx.read_front().unwrap(), Message::Data(2));
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn poll_returns_none_when_open_and_empty() {
    let rx = Receiver::<usize, TestSignal>::new();
    let _tx = rx.sender();
    assert_eq!(rx.poll().unwrap(), None);
}

#[test]
fn poll_returns_closed_after_sender_drop_and_drain() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    tx.push_back(Message::Data(1)).unwrap();
    drop(tx);

    assert_eq!(rx.poll().unwrap(), Some(Message::Data(1)));
    assert!(matches!(rx.poll().unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn pushable_trait_round_trips() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> = Box::new(tx);
    pushable.push(Message::Data(11)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(11));
}

#[test]
fn closeable_trait_closes_edge() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let mut closeable: Box<dyn Sink<DataType = usize, SignalType = TestSignal>> = Box::new(tx);
    closeable.push(Message::Data(1)).unwrap();
    Closeable::close(closeable.as_mut()).unwrap();

    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn get_dyn_pushable_returns_working_handle() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let mut handle: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Get::get(&tx).unwrap();
    handle.push(Message::Data(3)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(3));
}

#[test]
fn dyn_sink_try_push_hands_back_on_full_bounded_edge() {
    let rx = bounded(1);
    let mut sink: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Get::get(&rx).unwrap();
    // On its own thread so a `try_push` that blocks fails via `join_within` instead of hanging.
    let pusher = thread::spawn(move || {
        let pushed = sink.try_push(Message::Data(1)).unwrap();
        let full = sink.try_push(Message::Data(2)).unwrap();
        (pushed, full)
    });
    assert_eq!(
        join_within(pusher),
        (TryPush::Pushed, TryPush::Full(Message::Data(2)))
    );
}

#[test]
fn get_dyn_closeable_returns_working_handle() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let handle: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Get::get(&tx).unwrap();
    // Original tx still alive — handle is an additional clone.
    drop(handle);
    // tx still alive, edge still open.
    tx.push_back(Message::Data(1)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
}

#[test]
fn dropping_boxed_pushable_decrements_count() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let handle: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Get::get(&tx).unwrap();
    drop(tx);
    assert_eq!(rx.poll().unwrap(), None);
    drop(handle);
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn wait_front_unblocked_by_last_sender_drop() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();

    let reached = Arc::new(AtomicBool::new(false));
    let reached_clone = reached.clone();

    let waiter = thread::spawn(move || {
        let result = rx.wait_front();
        reached_clone.store(true, Ordering::SeqCst);
        result
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!reached.load(Ordering::SeqCst));

    drop(tx);

    let result = waiter.join().unwrap();
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn read_all_unblocked_by_last_sender_drop() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();

    let reached = Arc::new(AtomicBool::new(false));
    let reached_clone = reached.clone();

    let reader = thread::spawn(move || {
        let result = rx.read_all();
        reached_clone.store(true, Ordering::SeqCst);
        result
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!reached.load(Ordering::SeqCst));

    drop(tx);

    let result = reader.join().unwrap();
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn push_back_after_explicit_sender_close_returns_closed() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut tx = rx.sender();
    tx.close().unwrap();
    assert!(matches!(
        tx.push_back(Message::Data(1)).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn receiver_moved_cross_thread_reads() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();

    let reader = thread::spawn(move || {
        let mut got = Vec::new();
        loop {
            match rx.read_front() {
                Ok(Message::Data(v)) => got.push(v),
                Err(e) if e.is_closed() => break,
                other => panic!("unexpected {:?}", other),
            }
        }
        got
    });

    for i in 0..3 {
        tx.push_back(Message::Data(i)).unwrap();
    }
    drop(tx);

    let got = reader.join().unwrap();
    assert_eq!(got, vec![0, 1, 2]);
}

#[test]
fn receiver_first_then_last_sender_drop_is_safe() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    drop(rx);
    // Edge already marked closed by receiver drop. The sender's
    // last-drop close_infallible runs against an already-closed
    // state and must not panic or deadlock.
    drop(tx);
}

#[test]
fn drop_both_ends_immediately() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    drop(rx);
    drop(tx);
    // Constructing and tearing down a channel with no traffic
    // must not panic, leak, or deadlock.
}

#[test]
fn get_on_receiver_mints_sender() {
    let rx = Receiver::<usize, TestSignal>::new();
    let mut pushable: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Get::get(&rx).unwrap();
    pushable.push(Message::Data(9)).unwrap();
    assert_eq!(rx.read_front().unwrap(), Message::Data(9));
    drop(pushable);
    assert!(matches!(
        rx.read_front().unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn cross_thread_fan_in() {
    let rx = Receiver::<usize, TestSignal>::new();
    let tx = rx.sender();
    let mut handles = Vec::new();
    for i in 0..5 {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            tx.push_back(Message::Data(i)).unwrap();
        }));
    }
    drop(tx);

    for h in handles {
        h.join().unwrap();
    }

    let mut got = Vec::new();
    loop {
        match rx.read_front() {
            Ok(Message::Data(v)) => got.push(v),
            Err(e) if e.is_closed() => break,
            other => panic!("unexpected {:?}", other),
        }
    }
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}

// ---- Schedule: a refused sender's bound waker is woken by the next pop or close ----

fn wakes(count: &AtomicUsize) -> usize {
    count.load(Ordering::SeqCst)
}

/// `waker` as a full bundle for [Schedule] (the local half is never used by a `Send` edge).
fn bundle(sync: SyncWaker) -> wake::Waker {
    wake::Waker {
        sync,
        local: noop_local_waker(),
    }
}

/// A bound sender on a full bound-1 edge, refused once.
fn refused() -> (
    Receiver<usize, TestSignal>,
    Sender<usize, TestSignal>,
    Arc<AtomicUsize>,
) {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let (waker, counter) = counting_waker();
    Schedule::schedule(&mut tx, waker);
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert_eq!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(Message::Data(2))
    );
    (rx, tx, counter)
}

#[test]
fn retries_in_one_epoch_record_the_waker_once() {
    let (rx, mut tx, counter) = refused();
    for _ in 0..5 {
        assert_eq!(
            tx.try_push_back(Message::Data(3)).unwrap(),
            TryPush::Stuck(Message::Data(3))
        );
    }
    assert_eq!(wakes(&counter), 0, "a refusal alone wakes nothing");
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 1, "one entry, however many retries");
    // The pop took the list: the next pop has nothing to wake.
    assert_eq!(tx.try_push_back(Message::Data(4)).unwrap(), TryPush::Pushed);
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 1);
}

/// A pop that took the refused list is progress too: the next refusal is `Full`, not `Stuck`.
#[test]
fn a_pop_that_woke_refused_senders_counts_as_progress() {
    let (rx, mut tx, counter) = refused();
    rx.poll().unwrap();
    assert_eq!(tx.try_push_back(Message::Data(3)).unwrap(), TryPush::Pushed);
    assert!(matches!(
        tx.try_push_back(Message::Data(4)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 2);
}

/// The scratch list is reused: a waker woken in one epoch must not resurface in a later one.
#[test]
fn a_woken_waker_does_not_resurface_in_later_epochs() {
    let (rx, mut tx, counter) = refused();
    for epoch in 1..=4 {
        rx.poll().unwrap();
        assert_eq!(wakes(&counter), epoch);
        assert_eq!(tx.try_push_back(Message::Data(0)).unwrap(), TryPush::Pushed);
        assert!(matches!(
            tx.try_push_back(Message::Data(0)).unwrap(),
            TryPush::Full(_)
        ));
    }
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 5);
}

/// With a refused sender present, a drain still wakes every blocked thread, not just one.
#[test]
fn read_all_with_refused_senders_wakes_every_blocked_producer() {
    let rx = bounded(2);
    let mut tx = rx.sender();
    let (waker, counter) = counting_waker();
    Schedule::schedule(&mut tx, waker);
    tx.push_back(Message::Data(1)).unwrap();
    tx.push_back(Message::Data(2)).unwrap();
    assert!(matches!(
        tx.try_push_back(Message::Data(0)).unwrap(),
        TryPush::Full(_)
    ));
    let (tx3, tx4) = (rx.sender(), rx.sender());
    let first = thread::spawn(move || tx3.push_back(Message::Data(3)));
    let second = thread::spawn(move || tx4.push_back(Message::Data(4)));
    wait_until(|| rx.producers_waiting() == 2);
    assert_eq!(rx.read_all().unwrap().len(), 2);
    assert_eq!(wakes(&counter), 1);
    // Both freed slots are taken without any further pop.
    join_within(first).unwrap();
    join_within(second).unwrap();
}

#[test]
fn every_pop_path_wakes_the_refused_sender() {
    let pops: [fn(&Receiver<usize, TestSignal>); 3] = [
        |rx| drop(rx.read_front().unwrap()),
        |rx| drop(rx.poll().unwrap()),
        |rx| drop(rx.read_all().unwrap()),
    ];
    for pop in pops {
        let (rx, _tx, counter) = refused();
        pop(&rx);
        assert_eq!(wakes(&counter), 1);
    }
}

#[test]
fn every_refused_sender_is_woken() {
    let rx = bounded(1);
    let mut first = rx.sender();
    let mut second = rx.sender();
    let (first_waker, first_count) = counting_waker();
    let (second_waker, second_count) = counting_waker();
    Schedule::schedule(&mut first, first_waker);
    Schedule::schedule(&mut second, second_waker);
    assert_eq!(
        first.try_push_back(Message::Data(1)).unwrap(),
        TryPush::Pushed
    );
    assert!(matches!(
        first.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    assert!(matches!(
        second.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    assert_eq!((wakes(&first_count), wakes(&second_count)), (1, 1));
}

#[test]
fn an_unbound_refusal_records_nothing_and_a_clone_is_unbound() {
    let (rx, tx, counter) = refused();
    let mut clone = tx.clone();
    // Refused too, but unbound: the clone doesn't carry the waker.
    assert!(matches!(
        clone.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    // Only the original's refusal is woken.
    assert_eq!(wakes(&counter), 1);
}

#[test]
fn scheduling_after_an_unbound_refusal_still_gets_woken() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert!(matches!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    let (waker, counter) = counting_waker();
    Schedule::schedule(&mut tx, waker);
    // The unbound refusal recorded nothing, so this retry must record, not be `Stuck`.
    assert!(matches!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 1);
}

#[test]
fn rescheduling_replaces_the_waker() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let (old, old_count) = counting_waker();
    let (new, new_count) = counting_waker();
    Schedule::schedule(&mut tx, old);
    Schedule::schedule(&mut tx, new);
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert!(matches!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    assert_eq!((wakes(&old_count), wakes(&new_count)), (0, 1));
}

/// The old waker's recorded refusal still fires once; the new waker records on its own `Full`.
#[test]
fn rescheduling_after_a_refusal_wakes_both_once() {
    let (rx, mut tx, old_count) = refused();
    let (new, new_count) = counting_waker();
    Schedule::schedule(&mut tx, new);
    // A new target resets the refusal, so this records, not `Stuck`.
    assert!(matches!(
        tx.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Full(_)
    ));
    rx.poll().unwrap();
    assert_eq!((wakes(&old_count), wakes(&new_count)), (1, 1));
}

#[test]
fn rescheduling_the_same_target_keeps_one_entry() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let (waker, counter) = counting_waker();
    let same = waker.sync.clone();
    Schedule::schedule(&mut tx, waker);
    assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert!(matches!(
        tx.try_push_back(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    Schedule::schedule(&mut tx, bundle(same));
    // Still recorded this epoch: no second entry.
    assert!(matches!(
        tx.try_push_back(Message::Data(3)).unwrap(),
        TryPush::Stuck(_)
    ));
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 1);
}

/// Each close path checked while the other end is still alive: its own later close would wake
/// too and hide a path that doesn't.
#[test]
fn every_close_path_wakes_the_refused_sender() {
    // After each close, dropping both ends closes again: still once, so the close took the list.
    let (rx, mut tx, counter) = refused();
    tx.close().unwrap();
    assert_eq!(wakes(&counter), 1, "Sender::close");
    drop((rx, tx));
    assert_eq!(wakes(&counter), 1);

    let (rx, tx, counter) = refused();
    rx.close().unwrap();
    assert_eq!(wakes(&counter), 1, "Receiver::close");
    drop((rx, tx));
    assert_eq!(wakes(&counter), 1);

    // The last sender's drop closes infallibly.
    let (rx, tx, counter) = refused();
    drop(tx);
    assert_eq!(wakes(&counter), 1, "last Sender drop");
    drop(rx);

    let (rx, tx, counter) = refused();
    drop(rx);
    assert_eq!(wakes(&counter), 1, "Receiver drop");
    // Exactly once: the first close took the list, this one finds it empty.
    drop(tx);
    assert_eq!(wakes(&counter), 1);
}

#[test]
fn a_poisoned_edge_still_wakes_on_drop() {
    let (rx, tx, counter) = refused();
    let shared = tx.shared.clone();
    // Poison the edge's lock.
    let _ = thread::spawn(move || {
        let _guard = shared.inner.lock().unwrap();
        panic!("poison");
    })
    .join();
    drop(rx);
    assert_eq!(wakes(&counter), 1);
    drop(tx);
}

#[test]
fn a_dropped_refused_sender_is_still_woken() {
    let (rx, tx, counter) = refused();
    // Kept alive: dropping the last sender would close the edge, which wakes too.
    let _other = rx.sender();
    // No removal on drop: the entry stays and fires once more, harmlessly.
    drop(tx);
    assert_eq!(wakes(&counter), 0);
    rx.poll().unwrap();
    assert_eq!(wakes(&counter), 1);
}

/// With both a refused sender and a blocked thread, a pop takes the refused path, which must
/// still notify the thread: each pop path, so a single pop's `notify_one` is pinned too.
#[test]
fn every_pop_path_wakes_blocked_threads_and_refused_senders() {
    let pops: [fn(&Receiver<usize, TestSignal>); 3] = [
        |rx| drop(rx.read_front().unwrap()),
        |rx| drop(rx.poll().unwrap()),
        |rx| drop(rx.read_all().unwrap()),
    ];
    for pop in pops {
        let (rx, _tx, counter) = refused();
        let blocked = rx.sender();
        let thread = thread::spawn(move || blocked.push_back(Message::Data(9)));
        wait_until(|| rx.producers_waiting() == 1);
        pop(&rx);
        assert_eq!(wakes(&counter), 1);
        join_within(thread).unwrap();
    }
}

/// Records whether the edge's lock was free when it was woken.
struct Unlocked {
    shared: Arc<Shared<usize, TestSignal>>,
    free: AtomicUsize,
}

impl Wake for Unlocked {
    fn wake(self: Arc<Self>) {
        if self.shared.inner.try_lock().is_ok() {
            self.free.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A woken producer retries at once, so every wake (from a pop or a close) must run unlocked.
#[test]
fn wakes_run_after_unlocking() {
    type Path = fn(Receiver<usize, TestSignal>, Sender<usize, TestSignal>);
    let paths: [Path; 7] = [
        |rx, _tx| drop(rx.read_front().unwrap()),
        |rx, _tx| drop(rx.poll().unwrap()),
        |rx, _tx| drop(rx.read_all().unwrap()),
        |_rx, mut tx| tx.close().unwrap(),
        |rx, _tx| rx.close().unwrap(),
        |rx, tx| {
            drop(tx);
            drop(rx);
        },
        |rx, tx| {
            drop(rx);
            drop(tx);
        },
    ];
    for path in paths {
        let rx = bounded(1);
        let mut tx = rx.sender();
        let unlocked = Arc::new(Unlocked {
            shared: tx.shared.clone(),
            free: AtomicUsize::new(0),
        });
        Schedule::schedule(&mut tx, bundle(SyncWaker::from(unlocked.clone())));
        assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert!(matches!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Full(_)
        ));
        path(rx, tx);
        assert_eq!(unlocked.free.load(Ordering::SeqCst), 1);
    }
}

/// Park until woken; fail instead of hanging if the wake never comes.
fn park_or_fail(parker: &Parker) {
    let start = Instant::now();
    parker.park_until(start + TIMEOUT);
    assert!(
        start.elapsed() < TIMEOUT,
        "lost wake: parked a whole timeout"
    );
}

/// Push `message`, parking on `parker` while refused. Returns how many refusals were `Full`.
fn push_parked(
    tx: &mut Sender<usize, TestSignal>,
    parker: &Parker,
    mut message: Message<usize, TestSignal>,
) -> usize {
    let mut fulls = 0;
    loop {
        match tx.try_push_back(message).unwrap() {
            TryPush::Pushed => return fulls,
            TryPush::Full(back) => {
                fulls += 1;
                message = back;
                park_or_fail(parker);
            }
            // Already recorded this epoch: the wake is still owed.
            TryPush::Stuck(back) => {
                message = back;
                park_or_fail(parker);
            }
        }
    }
}

/// Deterministic: the producer is refused, then the pop wakes it.
#[test]
fn a_parked_producer_is_woken_by_the_pop() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let refused = Arc::new(AtomicBool::new(false));
    let seen = refused.clone();
    let producer = thread::spawn(move || {
        let parker = Parker::new();
        Schedule::schedule(&mut tx, bundle(parker.waker()));
        // The empty edge takes it, filling it.
        assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
        let mut message = Message::Data(2);
        loop {
            match tx.try_push_back(message).unwrap() {
                TryPush::Pushed => return,
                TryPush::Full(back) | TryPush::Stuck(back) => {
                    message = back;
                    seen.store(true, Ordering::SeqCst);
                    // A pop racing ahead of the park still left the token: no hang either way.
                    park_or_fail(&parker);
                }
            }
        }
    });
    wait_until(|| refused.load(Ordering::SeqCst));
    assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    join_within(producer);
    assert_eq!(rx.read_front().unwrap(), Message::Data(2));
}

/// Many park/wake rounds through a bound-1 edge: every message arrives and no refused producer
/// waits a whole timeout.
#[test]
fn a_parked_producer_never_loses_a_wake() {
    let rx = bounded(1);
    let mut tx = rx.sender();
    let producer = thread::spawn(move || {
        let parker = Parker::new();
        Schedule::schedule(&mut tx, bundle(parker.waker()));
        (0..2_000)
            .map(|id| push_parked(&mut tx, &parker, Message::Data(id)))
            .sum::<usize>()
    });
    let consumer = thread::spawn(move || {
        for id in 0..2_000 {
            assert_eq!(rx.read_front().unwrap(), Message::Data(id));
        }
    });
    join_within(consumer);
    assert!(join_within(producer) > 0, "the producer was never refused");
}

/// Poll and blocking producers on one bound-1 edge, the consumer alternating single pops and
/// whole drains: no producer loses a wake. Several poll producers are what can catch a drain
/// that bumps progress and takes the list in separate critical sections.
#[test]
fn mixed_producers_never_lose_a_wake() {
    const EACH: usize = 1_000;
    const PARKED: usize = 3;
    const BLOCKING: usize = 2;
    let rx = bounded(1);
    let mut parked = Vec::new();
    for id in 0..PARKED {
        let mut tx = rx.sender();
        parked.push(thread::spawn(move || {
            let parker = Parker::new();
            Schedule::schedule(&mut tx, bundle(parker.waker()));
            (0..EACH)
                .map(|n| push_parked(&mut tx, &parker, Message::Data(id * EACH + n)))
                .sum::<usize>()
        }));
    }
    let mut blocking = Vec::new();
    for id in PARKED..PARKED + BLOCKING {
        let tx = rx.sender();
        blocking.push(thread::spawn(move || {
            for n in 0..EACH {
                tx.push_back(Message::Data(id * EACH + n)).unwrap();
            }
        }));
    }
    let total = (PARKED + BLOCKING) * EACH;
    let consumer = thread::spawn(move || {
        let mut got = Vec::with_capacity(total);
        while got.len() < total {
            if got.len() % 2 == 0 {
                got.push(rx.read_front().unwrap());
            } else {
                got.extend(rx.read_all().unwrap());
            }
        }
        got
    });
    let fulls: usize = parked.into_iter().map(join_within).sum();
    assert!(fulls > 0, "no parked producer was ever refused");
    for producer in blocking {
        join_within(producer);
    }
    let mut got: Vec<usize> = join_within(consumer)
        .into_iter()
        .map(|message| match message {
            Message::Data(n) => n,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(got, (0..total).collect::<Vec<_>>());
}

/// Only parked producers, and a consumer spinning on non-blocking pops so drains race refusals
/// as often as possible: the sharpest probe of a drain that bumps progress and takes the list in
/// separate critical sections (a producer refused between the two would never be woken).
#[test]
fn racing_pops_never_lose_a_parked_wake() {
    const EACH: usize = 2_000;
    const PARKED: usize = 4;
    let rx = bounded(1);
    let mut producers = Vec::new();
    for id in 0..PARKED {
        let mut tx = rx.sender();
        producers.push(thread::spawn(move || {
            let parker = Parker::new();
            Schedule::schedule(&mut tx, bundle(parker.waker()));
            (0..EACH)
                .map(|n| push_parked(&mut tx, &parker, Message::Data(id * EACH + n)))
                .sum::<usize>()
        }));
    }
    let total = PARKED * EACH;
    let consumer = thread::spawn(move || {
        let deadline = Instant::now() + TIMEOUT;
        let mut got = 0;
        while got < total {
            assert!(
                Instant::now() < deadline,
                "consumer starved: a wake was lost"
            );
            if rx.poll().unwrap().is_some() {
                got += 1;
            }
        }
    });
    let fulls: usize = producers.into_iter().map(join_within).sum();
    assert!(fulls > 0, "no producer was ever refused");
    join_within(consumer);
}

/// A `sync::Sender` is a poll sink: a poll node can push into it without blocking.
fn _assert_sender_is_a_poll_sink(rx: &Receiver<usize, TestSignal>) {
    let _: Box<dyn PollSink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
        Box::new(rx.sender());
}
