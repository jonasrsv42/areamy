use super::{Receiver, Sender, channel};
use crate::poll::queue::PollQueue;
use crate::poll::timer::Timer;
use crate::wake::mock::{counting_local_waker, counting_waker, local_waker, noop_local_waker};
use crate::wake::{self, LocalWaker};
use core::task::{Context, Poll};
use std::cell::Cell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::Waker;
use std::time::Duration;

fn timer() -> Timer {
    // A real Timer bound to a real Scheduler; the consumer never runs here.
    let pq = PollQueue::new();
    let (_consumer, local_producer) = pq.local();
    Timer::new(0, &local_producer)
}

fn unbounded(output: LocalWaker) -> (Sender<usize>, Receiver<usize>) {
    channel(output, timer(), None)
}

fn bounded(output: LocalWaker, bound: usize) -> (Sender<usize>, Receiver<usize>) {
    channel(output, timer(), NonZeroUsize::new(bound))
}

fn poll_with<F: Future + Unpin>(fut: &mut F, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    Pin::new(fut).poll(&mut cx)
}

fn sent(sender: &Sender<usize>, item: usize) {
    assert!(matches!(
        poll_with(&mut sender.send(item), Waker::noop()),
        Poll::Ready(Ok(()))
    ));
}

/// A waker popping the receiver in `slot`, as a same-thread wake may re-enter the queue; counts
/// the pops that didn't fail.
fn popping(slot: &Rc<Cell<Option<Receiver<usize>>>>) -> (LocalWaker, Rc<Cell<usize>>) {
    let popped = Rc::new(Cell::new(0));
    let (slot, count) = (slot.clone(), popped.clone());
    let waker = local_waker(move || {
        if let Some(receiver) = slot.take() {
            if receiver.pop().is_ok() {
                count.set(count.get() + 1);
            }
            slot.set(Some(receiver));
        }
    });
    (waker, popped)
}

// ---- bound ----

#[test]
fn send_waits_exactly_at_the_bound() {
    let (sender, receiver) = bounded(noop_local_waker(), 2);
    sent(&sender, 1);
    sent(&sender, 2);
    let mut fut = sender.send(3);
    assert!(poll_with(&mut fut, Waker::noop()).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert!(poll_with(&mut fut, Waker::noop()).is_ready());
    assert_eq!(receiver.pop().unwrap(), Some(2));
    assert_eq!(receiver.pop().unwrap(), Some(3));
    assert_eq!(receiver.pop().unwrap(), None);
}

#[test]
fn unbounded_never_waits() {
    let (sender, receiver) = unbounded(noop_local_waker());
    for item in 0..1000 {
        sent(&sender, item);
    }
    assert_eq!(receiver.pop().unwrap(), Some(0));
}

#[test]
fn a_dropped_waiting_send_queues_nothing() {
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    sent(&sender, 1);
    {
        let mut fut = sender.send(2);
        assert!(poll_with(&mut fut, Waker::noop()).is_pending());
    }
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(receiver.pop().unwrap(), None);
}

#[test]
fn a_resolved_send_errors_rather_than_parks() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    let mut fut = sender.send(1);
    assert!(poll_with(&mut fut, &foreign.sync).is_ready());
    assert!(matches!(
        poll_with(&mut fut, &foreign.sync),
        Poll::Ready(Err(_))
    ));
    // Nothing parked: the pop wakes nobody.
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

// ---- waking the future ----

#[test]
fn a_pop_wakes_a_waiting_home_send_locally() {
    let (sync, sync_count) = counting_waker();
    let (local, local_count) = counting_local_waker();
    let cx_waker = sync.sync.clone();
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    receiver
        .home(&wake::Waker {
            sync: sync.sync,
            local,
        })
        .unwrap();
    sent(&sender, 1);
    let mut fut = sender.send(2);
    assert!(poll_with(&mut fut, &cx_waker).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(local_count.get(), 1);
    assert_eq!(sync_count.load(Ordering::SeqCst), 0);
}

#[test]
fn one_pop_wakes_every_waiting_send() {
    let (first, first_count) = counting_waker();
    let (second, second_count) = counting_waker();
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    let other = sender.clone();
    sent(&sender, 1);
    let mut first_fut = sender.send(2);
    let mut second_fut = other.send(3);
    assert!(poll_with(&mut first_fut, &first.sync).is_pending());
    assert!(poll_with(&mut second_fut, &second.sync).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(first_count.load(Ordering::SeqCst), 1);
    assert_eq!(second_count.load(Ordering::SeqCst), 1);
    // One slot: one send lands, the other waits again and the next pop wakes it.
    assert!(poll_with(&mut first_fut, &first.sync).is_ready());
    assert!(poll_with(&mut second_fut, &second.sync).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(2));
    assert_eq!(second_count.load(Ordering::SeqCst), 2);
    assert!(poll_with(&mut second_fut, &second.sync).is_ready());
    assert_eq!(receiver.pop().unwrap(), Some(3));
}

#[test]
fn a_pop_wakes_only_while_a_send_waits() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = bounded(noop_local_waker(), 2);
    sent(&sender, 1);
    sent(&sender, 2);
    let mut fut = sender.send(3);
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(receiver.pop().unwrap(), Some(2));
    assert_eq!(receiver.pop().unwrap(), None);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn the_send_wake_runs_after_the_borrow() {
    let slot = Rc::new(Cell::new(None));
    let (local, popped) = popping(&slot);
    let (sync, _) = counting_waker();
    let cx_waker = sync.sync.clone();
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    receiver
        .home(&wake::Waker {
            sync: sync.sync,
            local,
        })
        .unwrap();
    sent(&sender, 1);
    let mut fut = sender.send(2);
    assert!(poll_with(&mut fut, &cx_waker).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    // The send's wake popped with no borrow held: the woken `home` sees the queue empty.
    slot.set(Some(receiver));
    assert!(poll_with(&mut fut, &cx_waker).is_ready());
    assert_eq!(popped.get(), 0);
}

// ---- waking Output ----

#[test]
fn a_send_wakes_output_only_after_an_empty_pop() {
    let (output, count) = counting_local_waker();
    let (sender, receiver) = unbounded(output);
    sent(&sender, 1);
    sent(&sender, 2);
    assert_eq!(count.get(), 1);
    assert_eq!(receiver.pop().unwrap(), Some(1));
    sent(&sender, 3);
    assert_eq!(count.get(), 1);
    assert_eq!(receiver.pop().unwrap(), Some(2));
    assert_eq!(receiver.pop().unwrap(), Some(3));
    assert_eq!(receiver.pop().unwrap(), None);
    sent(&sender, 4);
    assert_eq!(count.get(), 2);
}

#[test]
fn a_waiting_send_does_not_wake_output() {
    let (output, count) = counting_local_waker();
    let (sender, _receiver) = bounded(output, 1);
    sent(&sender, 1);
    assert_eq!(count.get(), 1);
    let mut fut = sender.send(2);
    assert!(poll_with(&mut fut, Waker::noop()).is_pending());
    assert_eq!(count.get(), 1);
}

#[test]
fn the_output_wake_runs_after_the_borrow() {
    let slot = Rc::new(Cell::new(None));
    let (output, popped) = popping(&slot);
    let (sender, receiver) = unbounded(output);
    slot.set(Some(receiver));
    sent(&sender, 1);
    assert_eq!(popped.get(), 1);
}

// ---- send_with_timeout ----

#[test]
fn a_timed_send_with_room_queues_at_once() {
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    let mut fut = sender.send_with_timeout(1, Duration::from_secs(60));
    assert!(matches!(
        poll_with(&mut fut, Waker::noop()),
        Poll::Ready(Ok(None))
    ));
    assert_eq!(receiver.pop().unwrap(), Some(1));
}

#[test]
fn a_timed_send_queues_once_a_pop_frees_room() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    sent(&sender, 1);
    let mut fut = sender.send_with_timeout(2, Duration::from_secs(60));
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_with(&mut fut, &foreign.sync),
        Poll::Ready(Ok(None))
    ));
    assert_eq!(receiver.pop().unwrap(), Some(2));
}

#[test]
fn an_elapsed_timed_send_hands_the_item_back() {
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    sent(&sender, 1);
    // Zero: the deadline has passed by the first poll (`Instant` is monotonic).
    let mut fut = sender.send_with_timeout(2, Duration::ZERO);
    assert!(matches!(
        poll_with(&mut fut, Waker::noop()),
        Poll::Ready(Ok(Some(2)))
    ));
    assert_eq!(receiver.pop().unwrap(), Some(1));
    assert_eq!(receiver.pop().unwrap(), None);
}

#[test]
fn room_beats_an_elapsed_timeout() {
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    let mut fut = sender.send_with_timeout(1, Duration::ZERO);
    assert!(matches!(
        poll_with(&mut fut, Waker::noop()),
        Poll::Ready(Ok(None))
    ));
    assert_eq!(receiver.pop().unwrap(), Some(1));
}

#[test]
fn a_timed_send_duration_max_saturates() {
    let (sender, _receiver) = bounded(noop_local_waker(), 1);
    sent(&sender, 1);
    let mut fut = sender.send_with_timeout(2, Duration::MAX);
    assert!(poll_with(&mut fut, Waker::noop()).is_pending());
}
