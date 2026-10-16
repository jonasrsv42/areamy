use super::{Input, Receiver, Sender, channel};
use crate::node::routine::Accept;
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
    // A real Timer bound to a real Scheduler. Node id is arbitrary — we never run the
    // consumer in these tests.
    let pq = PollQueue::new();
    let (_consumer, local_producer) = pq.local();
    Timer::new(0, &local_producer)
}

fn unbounded() -> (Sender<usize>, Receiver<usize>) {
    channel(noop_local_waker(), timer(), None)
}

fn bounded(input: LocalWaker, bound: usize) -> (Sender<usize>, Receiver<usize>) {
    channel(input, timer(), NonZeroUsize::new(bound))
}

fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
    poll_with(fut, Waker::noop())
}

fn poll_with<F: Future + Unpin>(fut: &mut F, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    Pin::new(fut).poll(&mut cx)
}

fn accepted(sender: &Sender<usize>, item: usize) {
    assert_eq!(sender.try_send(item).unwrap(), Accept::Accepted);
}

// ---- bound and refusal ----

#[test]
fn try_send_refuses_exactly_at_the_bound() {
    let (sender, _receiver) = bounded(noop_local_waker(), 2);
    accepted(&sender, 1);
    accepted(&sender, 2);
    assert_eq!(sender.try_send(3).unwrap(), Accept::Full(3));
}

#[test]
fn unbounded_never_refuses() {
    let (sender, _receiver) = unbounded();
    for item in 0..1000 {
        accepted(&sender, item);
    }
}

#[test]
fn the_first_pop_after_a_refusal_wakes_input_once() {
    let (input, count) = counting_local_waker();
    let (sender, receiver) = bounded(input, 2);
    accepted(&sender, 1);
    accepted(&sender, 2);
    assert_eq!(sender.try_send(3).unwrap(), Accept::Full(3));
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(count.get(), 1);
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(count.get(), 1);
}

#[test]
fn a_pop_without_a_refusal_wakes_nobody() {
    let (input, count) = counting_local_waker();
    let (sender, receiver) = bounded(input, 2);
    accepted(&sender, 1);
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(count.get(), 0);
}

#[test]
fn every_refusal_owes_a_wake() {
    let (input, count) = counting_local_waker();
    let (sender, receiver) = bounded(input, 1);
    for expected in 1..=2 {
        accepted(&sender, expected);
        assert_eq!(sender.try_send(0).unwrap(), Accept::Full(0));
        assert!(matches!(
            poll_once(&mut receiver.recv()),
            Poll::Ready(Ok(_))
        ));
        assert_eq!(count.get(), expected);
    }
}

#[test]
fn popping_a_flush_after_a_refusal_wakes_input() {
    let (input, count) = counting_local_waker();
    let (sender, receiver) = bounded(input, 1);
    sender.flush().unwrap();
    assert_eq!(sender.try_send(1).unwrap(), Accept::Full(1));
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(Input::Flush))
    ));
    assert_eq!(count.get(), 1);
}

#[test]
fn flush_lands_past_the_bound() {
    let (sender, receiver) = bounded(noop_local_waker(), 1);
    accepted(&sender, 1);
    sender.flush().unwrap();
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(Input::Data(1)))
    ));
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(Input::Flush))
    ));
}

#[test]
fn the_input_wake_runs_after_the_borrow() {
    // Input's wake re-enters the queue, as a same-thread wake may.
    let slot: Rc<Cell<Option<Sender<usize>>>> = Rc::new(Cell::new(None));
    let reentered = Rc::new(Cell::new(false));
    let input = {
        let (slot, reentered) = (slot.clone(), reentered.clone());
        local_waker(move || {
            if let Some(sender) = slot.take() {
                reentered.set(sender.try_send(9).is_ok());
                slot.set(Some(sender));
            }
        })
    };
    let (sender, receiver) = bounded(input, 1);
    accepted(&sender, 1);
    assert_eq!(sender.try_send(2).unwrap(), Accept::Full(2));
    slot.set(Some(sender));
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(_))
    ));
    assert!(reentered.get());
}

// ---- waking the future ----

#[test]
fn a_send_wakes_a_parked_home_recv_locally() {
    let (sync, sync_count) = counting_waker();
    let (local, local_count) = counting_local_waker();
    let cx_waker = sync.sync.clone();
    let (sender, receiver) = unbounded();
    sender
        .home(&wake::Waker {
            sync: sync.sync,
            local,
        })
        .unwrap();
    let mut fut = receiver.recv();
    assert!(poll_with(&mut fut, &cx_waker).is_pending());
    accepted(&sender, 1);
    assert_eq!(local_count.get(), 1);
    assert_eq!(sync_count.load(Ordering::SeqCst), 0);
}

#[test]
fn a_send_wakes_a_parked_foreign_waker() {
    let (home, home_count) = counting_waker();
    let (foreign, foreign_count) = counting_waker();
    let (sender, receiver) = unbounded();
    sender.home(&home).unwrap();
    let mut fut = receiver.recv();
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    accepted(&sender, 1);
    assert_eq!(foreign_count.load(Ordering::SeqCst), 1);
    assert_eq!(home_count.load(Ordering::SeqCst), 0);
}

#[test]
fn a_send_wakes_only_while_a_recv_is_parked() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = unbounded();
    accepted(&sender, 1);
    assert!(poll_with(&mut receiver.recv(), &foreign.sync).is_ready());
    accepted(&sender, 2);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(poll_with(&mut receiver.recv(), &foreign.sync).is_ready());
    let mut fut = receiver.recv();
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    accepted(&sender, 3);
    accepted(&sender, 4);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_flush_wakes_a_parked_recv() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = unbounded();
    let mut fut = receiver.recv();
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    sender.flush().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_parked_timed_recv_is_woken_by_a_send() {
    let (foreign, count) = counting_waker();
    let (sender, receiver) = unbounded();
    let mut fut = receiver.recv_with_timeout(Duration::from_secs(60));
    assert!(poll_with(&mut fut, &foreign.sync).is_pending());
    accepted(&sender, 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ---- renew ----

#[test]
fn renew_opens_a_fresh_queue_with_the_same_wakes_and_bound() {
    let (input, input_count) = counting_local_waker();
    let (home, home_count) = counting_local_waker();
    let (sync, _) = counting_waker();
    let cx_waker = sync.sync.clone();
    let (mut sender, receiver) = bounded(input, 1);
    sender
        .home(&wake::Waker {
            sync: sync.sync,
            local: home,
        })
        .unwrap();
    sender.flush().unwrap();
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Ok(Input::Flush))
    ));
    assert!(matches!(
        poll_once(&mut receiver.recv()),
        Poll::Ready(Err(_))
    ));

    let receiver = sender.renew().unwrap();
    // Home carried over: a parked recv is woken locally.
    let mut fut = receiver.recv();
    assert!(poll_with(&mut fut, &cx_waker).is_pending());
    accepted(&sender, 1);
    assert_eq!(home_count.get(), 1);
    // Bound and Input waker carried over.
    assert_eq!(sender.try_send(2).unwrap(), Accept::Full(2));
    assert!(matches!(
        poll_with(&mut fut, &cx_waker),
        Poll::Ready(Ok(Input::Data(1)))
    ));
    assert_eq!(input_count.get(), 1);
}

#[test]
fn renew_rejects_an_open_queue() {
    let (mut sender, _receiver) = unbounded();
    assert!(sender.renew().is_err());
}

#[test]
fn renew_rejects_an_undrained_queue() {
    let (mut sender, receiver) = unbounded();
    sender.flush().unwrap();
    assert!(poll_once(&mut receiver.recv()).is_ready());
    accepted(&sender, 1);
    assert!(sender.renew().is_err());
}

// ---- recv_with_timeout: data / flush / closed paths ----

#[test]
fn timeout_duration_max_saturates() {
    // Duration::MAX as "never" must not panic on Instant overflow.
    let (_sender, receiver) = unbounded();
    let mut fut = receiver.recv_with_timeout(Duration::MAX);
    assert!(matches!(poll_once(&mut fut), Poll::Pending));
}

#[test]
fn timeout_returns_buffered_item() {
    let (sender, receiver) = unbounded();
    accepted(&sender, 7);
    let mut fut = receiver.recv_with_timeout(Duration::from_secs(60));
    let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
        panic!("expected Ready(Some(Data))");
    };
    assert_eq!(n, 7);
}

#[test]
fn timeout_returns_flush_and_marks_closed() {
    let (sender, receiver) = unbounded();
    sender.flush().unwrap();
    let mut fut = receiver.recv_with_timeout(Duration::from_secs(60));
    assert!(matches!(
        poll_once(&mut fut),
        Poll::Ready(Ok(Some(Input::Flush)))
    ));
    // After Flush, subsequent recvs see the queue closed.
    let mut next = receiver.recv_with_timeout(Duration::from_secs(60));
    assert!(matches!(poll_once(&mut next), Poll::Ready(Err(_))));
}

#[test]
fn timeout_pending_when_empty_and_deadline_future() {
    let (_sender, receiver) = unbounded();
    let mut fut = receiver.recv_with_timeout(Duration::from_secs(60));
    assert!(matches!(poll_once(&mut fut), Poll::Pending));
}

#[test]
fn an_elapsed_timeout_returns_none() {
    // Zero: the deadline has passed by the first poll (`Instant` is monotonic).
    let (_sender, receiver) = unbounded();
    let mut fut = receiver.recv_with_timeout(Duration::ZERO);
    assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(None))));
}

#[test]
fn a_buffered_item_beats_an_elapsed_timeout() {
    let (sender, receiver) = unbounded();
    accepted(&sender, 99);
    let mut fut = receiver.recv_with_timeout(Duration::ZERO);
    let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
        panic!("expected Ready(Some(Data)) — item beats timeout");
    };
    assert_eq!(n, 99);
}

#[test]
fn timeout_push_after_pending_resolves_with_data() {
    let (sender, receiver) = unbounded();
    let mut fut = receiver.recv_with_timeout(Duration::from_secs(60));
    assert!(matches!(poll_once(&mut fut), Poll::Pending));
    accepted(&sender, 11);
    let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
        panic!("expected Ready(Some(Data))");
    };
    assert_eq!(n, 11);
}
