use super::*;
use crate::Trackable;
use crate::edge::sync::tests::{join_within, wait_until};
use crate::error::ErrorKind;
use crate::poll::Sink as PollSink;
use crate::tests::support::{DEADLINE, deadline};
use crate::wake::Parker;
use crate::wake::mock::counting_waker;
use crate::work::Sink as WorkSink;
use alloc::sync::Arc;
use alloc::task::Wake;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

type Signal = Trackable<&'static str>;

/// A pop on an empty edge takes nothing and reports whether it is closed.
fn closed_and_empty(rx: &Receiver<usize, Signal>) -> bool {
    matches!(rx.inner.poll(), Err(e) if e.is_closed())
}

fn _assert_sender_send_sync() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<Sender<usize, Signal>>();
}

fn _assert_receiver_send() {
    fn require_send<T: Send>() {}
    require_send::<Receiver<usize, Signal>>();
}

/// Both sink kinds: work parents and poll parents can push into it.
fn _assert_sender_is_both_sinks(rx: &Receiver<usize, Signal>) {
    let _: Box<dyn WorkSink<DataType = usize, SignalType = Signal> + Send + Sync> =
        Box::new(rx.sender());
    let _: Box<dyn PollSink<DataType = usize, SignalType = Signal> + Send + Sync> =
        Box::new(rx.sender());
}

fn counted(bound: Option<NonZeroUsize>) -> (Receiver<usize, Signal>, Arc<AtomicUsize>) {
    let (waker, wakes) = counting_waker();
    (Receiver::new(waker.sync, bound), wakes)
}

fn wakes(counter: &Arc<AtomicUsize>) -> usize {
    counter.load(Ordering::SeqCst)
}

/// A consumer waker that looks at its own edge when woken: `(len, closed)` per wake. It locks
/// the edge, so a wake under the edge lock deadlocks instead of passing.
#[derive(Default)]
struct Probe {
    rx: Mutex<Option<Receiver<usize, Signal>>>,
    seen: Mutex<Vec<(usize, bool)>>,
}

impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(rx) = self.rx.lock().unwrap().as_ref() {
            let len = rx.inner.len().unwrap();
            let seen = (len, len == 0 && closed_and_empty(rx));
            self.seen.lock().unwrap().push(seen);
        }
    }
}

/// An edge whose consumer waker is a [Probe] holding it, plus one sender.
fn probed() -> (Arc<Probe>, Sender<usize, Signal>) {
    let probe = Arc::new(Probe::default());
    let rx = Receiver::new(Waker::from(probe.clone()), None);
    let tx = rx.sender();
    *probe.rx.lock().unwrap() = Some(rx);
    (probe, tx)
}

/// Runs `act` on another thread (a wake under the lock hangs instead of failing) and returns
/// what the probe saw. What `act` hands back (a sender, whose drop would wake again) is dropped
/// only after, with the probe ↔ receiver cycle broken.
fn seen_by<T: Send + 'static>(
    probe: Arc<Probe>,
    act: impl FnOnce() -> T + Send + 'static,
) -> Vec<(usize, bool)> {
    let kept = join_within(thread::spawn(act));
    let seen = probe.seen.lock().unwrap().clone();
    let rx = probe.rx.lock().unwrap().take();
    drop(rx);
    drop(kept);
    seen
}

#[test]
fn push_back_wakes_after_the_message_lands_unlocked() {
    let (probe, tx) = probed();
    let seen = seen_by(probe, move || {
        tx.push_back(Message::Data(1)).unwrap();
        tx
    });
    assert_eq!(seen, vec![(1, false)]);
}

#[test]
fn try_push_wakes_after_the_message_lands_unlocked() {
    let (probe, mut tx) = probed();
    let seen = seen_by(probe, move || {
        assert_eq!(tx.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        tx
    });
    assert_eq!(seen, vec![(1, false)]);
}

#[test]
fn close_wakes_after_the_edge_is_closed() {
    let (probe, mut tx) = probed();
    let seen = seen_by(probe, move || {
        tx.close().unwrap();
        tx
    });
    assert_eq!(seen, vec![(0, true)]);
}

/// The last sender's drop closes the edge before it wakes: woken first, the consumer would find
/// it open and empty, and sleep for good.
#[test]
fn the_last_drop_closes_before_it_wakes() {
    let (probe, tx) = probed();
    let seen = seen_by(probe, move || drop(tx));
    assert_eq!(seen, vec![(0, true)]);
}

#[test]
fn try_push_wakes_only_when_pushed() {
    let (rx, counter) = counted(NonZeroUsize::new(1));
    let mut tx = rx.sender();
    assert_eq!(tx.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert_eq!(wakes(&counter), 1);
    assert!(matches!(
        tx.try_push(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    assert!(matches!(
        tx.try_push(Message::Data(2)).unwrap(),
        TryPush::Stuck(_)
    ));
    // Nothing landed, so the consumer must not wake for it.
    assert_eq!(wakes(&counter), 1);
}

#[test]
fn every_drop_wakes_and_only_the_last_closes() {
    let (rx, counter) = counted(None);
    let first = rx.sender();
    let second = rx.sender();
    drop(first);
    assert_eq!(wakes(&counter), 1);
    assert!(!closed_and_empty(&rx));
    drop(second);
    assert_eq!(wakes(&counter), 2);
    assert!(closed_and_empty(&rx));
}

/// A scheduled sender on `rx` (bound 1), refused once: nothing woken yet.
fn refused(rx: &Receiver<usize, Signal>) -> (Sender<usize, Signal>, Arc<AtomicUsize>) {
    let mut tx = rx.sender();
    let (waker, counter) = counting_waker();
    Schedule::schedule(&mut tx, waker);
    assert_eq!(tx.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
    assert!(matches!(
        tx.try_push(Message::Data(2)).unwrap(),
        TryPush::Full(_)
    ));
    assert_eq!(wakes(&counter), 0);
    (tx, counter)
}

#[test]
fn a_scheduled_sender_is_woken_by_the_pop_after_full() {
    let (mut rx, _consumer) = counted(NonZeroUsize::new(1));
    let (_tx, producer) = refused(&rx);
    rx.try_recv().unwrap();
    assert_eq!(wakes(&producer), 1);
}

/// Closing the receiver wakes refused producers, but not its own consumer: that is the caller.
#[test]
fn receiver_close_wakes_producers_not_its_consumer() {
    let (rx, consumer) = counted(NonZeroUsize::new(1));
    let (tx, producer) = refused(&rx);
    let before = wakes(&consumer);
    // Asserted while `rx` lives: its drop would close too.
    rx.close().unwrap();
    assert_eq!(wakes(&producer), 1);
    assert_eq!(wakes(&consumer), before);
    assert!(matches!(
        tx.push_back(Message::Data(3)).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn a_scheduled_sender_is_woken_by_receiver_drop() {
    let (rx, _consumer) = counted(NonZeroUsize::new(1));
    let (_tx, producer) = refused(&rx);
    drop(rx);
    assert_eq!(wakes(&producer), 1);
}

#[test]
fn blocked_push_wakes_after_it_lands() {
    let parker = Parker::new();
    let rx = Receiver::<usize, Signal>::new(parker.waker(), NonZeroUsize::new(1));
    // Kept alive: dropping the last sender would close the edge.
    let first = rx.sender();
    first.push_back(Message::Data(1)).unwrap();
    let tx = rx.sender();
    // Hands the sender back: its drop wakes too, which would hide a missing wake.
    let producer = thread::spawn(move || tx.push_back(Message::Data(2)).map(|()| tx));

    wait_until(|| rx.inner.producers_waiting() == 1);
    // Forget wakes from before the block; only the landing may wake now.
    parker.take_woken();
    assert_eq!(rx.poll().unwrap(), Some(Message::Data(1)));
    let _tx = join_within(producer).unwrap();
    assert!(parker.take_woken());
    assert_eq!(rx.poll().unwrap(), Some(Message::Data(2)));
}

#[test]
fn try_recv_drains_then_reports_closed() {
    let (mut rx, _counter) = counted(None);
    // Unconnected: nothing can ever arrive, so done from the start.
    assert!(rx.try_recv().unwrap_err().is_closed());
    let tx = rx.sender();
    // A live producer keeps it open.
    assert_eq!(rx.try_recv().unwrap(), None);
    tx.push_back(Message::Data(1)).unwrap();
    tx.push_back(Message::Flush(Trackable::new("f"))).unwrap();
    drop(tx);
    // Closed, but what is buffered still drains first.
    assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(1)));
    assert_eq!(
        rx.try_recv().unwrap(),
        Some(Message::Flush(Trackable::new("f")))
    );
    assert!(matches!(rx.try_recv().unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn get_mints_a_work_sink_and_a_poll_sink() {
    let (mut rx, _counter) = counted(None);
    let mut work: Box<dyn WorkSink<DataType = usize, SignalType = Signal> + Send + Sync> =
        Get::get(&rx).unwrap();
    let mut poll: Box<dyn PollSink<DataType = usize, SignalType = Signal> + Send + Sync> =
        Get::get(&rx).unwrap();
    work.push(Message::Data(1)).unwrap();
    assert_eq!(poll.try_push(Message::Data(2)).unwrap(), TryPush::Pushed);
    assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(1)));
    assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(2)));
    // Each got sink is a producer: the edge closes after the last.
    drop(work);
    assert!(!closed_and_empty(&rx));
    drop(poll);
    assert!(closed_and_empty(&rx));
}

/// A consumer that sleeps on every empty pop is always woken: a push racing the empty pop wakes
/// after it lands. The sender outlives the run, so no close or drop wake can rescue it.
#[test]
fn a_racing_push_never_strands_the_consumer() {
    let _deadline = deadline(DEADLINE);
    const MESSAGES: usize = 10_000;
    let parker = Parker::new();
    let rx = Receiver::<usize, Signal>::new(parker.waker(), None);
    let tx = rx.sender();
    let producer = thread::spawn(move || {
        for i in 0..MESSAGES {
            tx.push_back(Message::Data(i)).unwrap();
        }
        tx
    });
    let consumer = thread::spawn(move || {
        let mut got = 0;
        while got < MESSAGES {
            match rx.poll().unwrap() {
                Some(_) => got += 1,
                None => parker.park(),
            }
        }
        rx
    });
    let _tx = join_within(producer);
    let _rx = join_within(consumer);
}

#[test]
fn dropping_the_receiver_closes_senders() {
    let (rx, _counter) = counted(None);
    let tx = rx.sender();
    drop(rx);
    assert!(matches!(
        tx.push_back(Message::Data(1)).unwrap_err().kind,
        ErrorKind::Closed
    ));
}
