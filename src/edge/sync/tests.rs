use super::*;
use crate::Trackable;
use crate::error::{Error, ErrorKind};
use crate::graph::{Closeable, Get, Pushable, TryPush, TryPushable};
use crate::message::Message;
use crate::work::Sink;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

#[test]
fn sender_close_releases_other_blocked_producer() {
    let rx = bounded(1);
    let closer = rx.sender();
    closer.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

    wait_until(|| rx.producers_waiting() == 1);
    closer.close().unwrap();
    assert!(is_closed(join_within(producer)));
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
    let tx = rx.sender();
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
    let mut handle: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
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
    let handle: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
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
    let tx = rx.sender();
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
                Err(e) if matches!(e.kind, ErrorKind::Closed) => break,
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
    let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
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
            Err(e) if matches!(e.kind, ErrorKind::Closed) => break,
            other => panic!("unexpected {:?}", other),
        }
    }
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3, 4]);
}
