use super::*;
use crate::Trackable;
use crate::error::{Error, ErrorKind};
use crate::graph::{Closeable, Receivable, Schedule, TryPush, TryPushable};
use crate::wake::mock::{counting_local_waker, counting_waker, noop_local_waker};
use crate::wake::{LocalWake, Waker};
use core::cell::Cell;
use core::sync::atomic::Ordering;
use core::task;

type Signal = Trackable<&'static str>;
type Edge = (Sender<usize, Signal>, Receiver<usize, Signal>);

fn bounded(bound: usize) -> Edge {
    channel(noop_local_waker(), NonZeroUsize::new(bound))
}

fn push(tx: &mut Sender<usize, Signal>, value: usize) {
    assert_eq!(tx.try_push(Message::Data(value)).unwrap(), TryPush::Pushed);
}

fn is_full(tx: &mut Sender<usize, Signal>, value: usize) -> bool {
    matches!(tx.try_push(Message::Data(value)), Ok(TryPush::Full(_)))
}

fn is_stuck(tx: &mut Sender<usize, Signal>, value: usize) -> bool {
    matches!(tx.try_push(Message::Data(value)), Ok(TryPush::Stuck(_)))
}

fn pop(rx: &mut Receiver<usize, Signal>) -> Option<usize> {
    match rx.try_recv().unwrap() {
        Some(Message::Data(value)) => Some(value),
        Some(_) => panic!("expected data"),
        None => None,
    }
}

/// Schedules a counting local waker on `tx`; returns its count.
fn scheduled(tx: &mut Sender<usize, Signal>) -> Rc<Cell<usize>> {
    let (local, count) = counting_local_waker();
    Schedule::schedule(
        tx,
        Waker {
            sync: task::Waker::noop().clone(),
            local,
        },
    );
    count
}

fn is_closed<T>(result: Result<T, Error>) -> bool {
    matches!(
        result,
        Err(Error {
            kind: ErrorKind::Closed,
            ..
        })
    )
}

/// Test wake running a closure.
struct OnWake<F>(F);

impl<F: Fn()> LocalWake for OnWake<F> {
    fn wake(&self) {
        (self.0)();
    }
}

#[test]
fn unbounded_is_never_full() {
    let (mut tx, _rx) = channel::<usize, Signal>(noop_local_waker(), None);
    for i in 0..100 {
        push(&mut tx, i);
    }
}

#[test]
fn a_full_edge_hands_the_message_back() {
    let (mut tx, _rx) = bounded(2);
    push(&mut tx, 1);
    push(&mut tx, 2);
    assert_eq!(
        tx.try_push(Message::Data(3)).unwrap(),
        TryPush::Full(Message::Data(3))
    );
    // No pop since: a retry would spin.
    assert_eq!(
        tx.try_push(Message::Data(3)).unwrap(),
        TryPush::Stuck(Message::Data(3))
    );
}

#[test]
fn signals_take_slots() {
    let (mut tx, _rx) = bounded(1);
    assert_eq!(
        tx.try_push(Message::Flush(Trackable::new("f"))).unwrap(),
        TryPush::Pushed
    );
    assert!(is_full(&mut tx, 1));
}

#[test]
fn a_pop_after_a_refusal_wakes_the_producer_once() {
    let (mut tx, mut rx) = bounded(1);
    let wakes = scheduled(&mut tx);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    assert!(is_stuck(&mut tx, 2));
    assert_eq!(wakes.get(), 0);
    assert_eq!(pop(&mut rx), Some(1));
    assert_eq!(wakes.get(), 1);

    // No refusal since: the next pop owes nothing.
    push(&mut tx, 2);
    assert_eq!(pop(&mut rx), Some(2));
    assert_eq!(wakes.get(), 1);
}

/// A wake leaves the producer scheduled: every later refusal is woken too.
#[test]
fn the_producer_stays_scheduled_across_refusals() {
    let (mut tx, mut rx) = bounded(1);
    let wakes = scheduled(&mut tx);
    for round in 1..=3 {
        push(&mut tx, round);
        assert!(is_full(&mut tx, round));
        assert_eq!(pop(&mut rx), Some(round));
        assert_eq!(wakes.get(), round);
    }
}

#[test]
fn a_pop_turns_stuck_back_into_full() {
    let (mut tx, mut rx) = bounded(1);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    assert!(is_stuck(&mut tx, 2));
    assert_eq!(pop(&mut rx), Some(1));
    push(&mut tx, 2);
    assert!(is_full(&mut tx, 3));
}

/// The waker is read when the pop fires, so a refusal before scheduling is still owed it.
#[test]
fn a_refusal_before_schedule_is_still_woken() {
    let (mut tx, mut rx) = bounded(1);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    let wakes = scheduled(&mut tx);
    assert_eq!(pop(&mut rx), Some(1));
    assert_eq!(wakes.get(), 1);
}

#[test]
fn rescheduling_replaces_the_waker() {
    let (mut tx, mut rx) = bounded(1);
    let first = scheduled(&mut tx);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    let second = scheduled(&mut tx);
    assert_eq!(pop(&mut rx), Some(1));
    assert_eq!((first.get(), second.get()), (0, 1));
}

/// Only `.local`: the edge never leaves its thread.
#[test]
fn schedule_keeps_only_the_local_waker() {
    let (mut tx, mut rx) = bounded(1);
    let (waker, sync_wakes) = counting_waker();
    Schedule::schedule(&mut tx, waker);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    assert_eq!(pop(&mut rx), Some(1));
    assert_eq!(sync_wakes.load(Ordering::SeqCst), 0);
}

#[test]
fn pushes_wake_the_consumer_and_refusals_do_not() {
    let (consumer, wakes) = counting_local_waker();
    let (mut tx, _rx) = channel::<usize, Signal>(consumer, NonZeroUsize::new(1));
    push(&mut tx, 1);
    assert_eq!(wakes.get(), 1);
    assert!(is_full(&mut tx, 2));
    assert!(is_stuck(&mut tx, 2));
    assert_eq!(wakes.get(), 1);
}

#[test]
fn closed_beats_full() {
    let (mut tx, rx) = bounded(1);
    push(&mut tx, 1);
    rx.close();
    assert!(is_closed(tx.try_push(Message::Data(2))));
}

#[test]
fn receiver_close_wakes_a_refused_producer() {
    let (mut tx, rx) = bounded(1);
    let wakes = scheduled(&mut tx);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    rx.close();
    assert_eq!(wakes.get(), 1);
    assert!(is_closed(tx.try_push(Message::Data(2))));
}

#[test]
fn receiver_drop_wakes_a_refused_producer() {
    let (mut tx, rx) = bounded(1);
    let wakes = scheduled(&mut tx);
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    drop(rx);
    assert_eq!(wakes.get(), 1);
    assert!(is_closed(tx.try_push(Message::Data(2))));
}

#[test]
fn receiver_drop_without_a_refusal_wakes_nothing() {
    let (mut tx, rx) = bounded(1);
    let wakes = scheduled(&mut tx);
    drop(rx);
    assert_eq!(wakes.get(), 0);
    assert!(is_closed(tx.try_push(Message::Data(1))));
}

#[test]
fn sender_drop_closes_and_wakes_the_consumer() {
    let (consumer, wakes) = counting_local_waker();
    let (mut tx, mut rx) = channel::<usize, Signal>(consumer, None);
    push(&mut tx, 1);
    drop(tx);
    assert_eq!(wakes.get(), 2);
    // Buffered data still drains before the close.
    assert_eq!(pop(&mut rx), Some(1));
    assert!(is_closed(rx.try_recv()));
}

#[test]
fn sender_close_wakes_the_consumer_once() {
    let (consumer, wakes) = counting_local_waker();
    let (mut tx, mut rx) = channel::<usize, Signal>(consumer, None);
    Closeable::close(&mut tx).unwrap();
    Closeable::close(&mut tx).unwrap();
    drop(tx);
    assert_eq!(wakes.get(), 1);
    assert!(is_closed(rx.try_recv()));
}

/// Close bypasses the bound; what was buffered still drains.
#[test]
fn a_full_edge_closes_and_drains() {
    let (mut tx, mut rx) = bounded(1);
    push(&mut tx, 1);
    Closeable::close(&mut tx).unwrap();
    assert_eq!(pop(&mut rx), Some(1));
    assert!(is_closed(rx.try_recv()));
}

/// Whether each waker's touch of the edge went through: a borrowed buffer fails it.
type Seen = Rc<RefCell<Vec<bool>>>;

/// The pop's wake runs outside the buffer's borrow: a producer pushing from inside it succeeds.
#[test]
fn a_pop_wakes_outside_the_borrow() {
    let holder: Rc<RefCell<Option<Sender<usize, Signal>>>> = Rc::default();
    let seen: Seen = Rc::default();
    let (on_holder, on_seen) = (holder.clone(), seen.clone());
    let producer = LocalWaker::new(OnWake(move || {
        if let Some(tx) = on_holder.borrow_mut().as_mut() {
            let pushed = matches!(tx.try_push(Message::Data(9)), Ok(TryPush::Pushed));
            on_seen.borrow_mut().push(pushed);
        }
    }));
    let (mut tx, mut rx) = bounded(1);
    Schedule::schedule(
        &mut tx,
        Waker {
            sync: task::Waker::noop().clone(),
            local: producer,
        },
    );
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    *holder.borrow_mut() = Some(tx);

    assert_eq!(pop(&mut rx), Some(1));
    assert_eq!(*seen.borrow(), vec![true]);
    assert_eq!(pop(&mut rx), Some(9));
    // Breaks the waker ↔ holder cycle.
    let _tx = holder.borrow_mut().take();
}

/// The push's wake runs outside the buffer's borrow: a consumer popping from inside it succeeds.
#[test]
fn a_push_wakes_outside_the_borrow() {
    let holder: Rc<RefCell<Option<Receiver<usize, Signal>>>> = Rc::default();
    let seen: Seen = Rc::default();
    let (on_holder, on_seen) = (holder.clone(), seen.clone());
    let consumer = LocalWaker::new(OnWake(move || {
        if let Some(rx) = on_holder.borrow_mut().as_mut() {
            let popped = matches!(rx.try_recv(), Ok(Some(_)));
            on_seen.borrow_mut().push(popped);
        }
    }));
    let (mut tx, rx) = channel::<usize, Signal>(consumer, None);
    *holder.borrow_mut() = Some(rx);

    push(&mut tx, 1);
    assert_eq!(*seen.borrow(), vec![true]);
    let _rx = holder.borrow_mut().take();
}

/// A producer that re-schedules from inside its wake keeps the new waker.
#[test]
fn a_reschedule_from_inside_the_wake_sticks() {
    let holder: Rc<RefCell<Option<Sender<usize, Signal>>>> = Rc::default();
    let (second, second_wakes) = counting_local_waker();
    let first_wakes = Rc::new(Cell::new(0));
    let (on_holder, on_count) = (holder.clone(), first_wakes.clone());
    let first = LocalWaker::new(OnWake(move || {
        on_count.set(on_count.get() + 1);
        if let Some(tx) = on_holder.borrow_mut().as_mut() {
            Schedule::schedule(
                tx,
                Waker {
                    sync: task::Waker::noop().clone(),
                    local: second.clone(),
                },
            );
        }
    }));
    let (mut tx, mut rx) = bounded(1);
    Schedule::schedule(
        &mut tx,
        Waker {
            sync: task::Waker::noop().clone(),
            local: first,
        },
    );
    push(&mut tx, 1);
    assert!(is_full(&mut tx, 2));
    *holder.borrow_mut() = Some(tx);
    assert_eq!(pop(&mut rx), Some(1));

    // The next refusal wakes the waker scheduled from inside the first wake.
    if let Some(tx) = holder.borrow_mut().as_mut() {
        push(tx, 2);
        assert!(is_full(tx, 3));
    }
    assert_eq!(pop(&mut rx), Some(2));
    assert_eq!((first_wakes.get(), second_wakes.get()), (1, 1));
    let _tx = holder.borrow_mut().take();
}
