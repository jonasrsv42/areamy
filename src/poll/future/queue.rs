//! Producer/consumer queues for [FutureRoutine](super::FutureRoutine).
//!
//! Two queue types, each split into producer + consumer:
//!
//! - [InputQueue]: node pushes data in, future awaits via [InputConsumer::recv].
//!   Uses `std::task::Waker` (standard futures contract).
//!
//! - [OutputQueue]: future pushes data out, node drains via [OutputConsumer::pop].
//!   Uses [ThreadLocalWaker] to wake the Output phase (same thread, no atomics).
//!
//! All types are `!Send` — they live on the async thread.

use crate::connect::waker::ThreadLocalWaker;
use crate::error::Error;
use core::task::Poll;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Waker;
use std::time::{Duration, Instant};

// ============================================================
// Input queue
// ============================================================

/// Input item received by the future via [InputConsumer::recv].
pub enum Input<T> {
    Data(T),
    Flush,
}

struct InputInner<T> {
    buffer: VecDeque<Input<T>>,
    closed: bool,
    /// Std waker set by the future's `cx` on first poll; fired by
    /// [InputProducer::push] when data arrives.
    waker: Waker,
    /// Areamy local waker for the owning node. [RecvDeadlineFut] uses
    /// this to call [ThreadLocalWaker::schedule_at] so the node is
    /// re-polled when its deadline elapses.
    local: ThreadLocalWaker,
}

/// Pushes input data into the queue. Held by the node's Input phase.
pub struct InputProducer<T>(Rc<RefCell<InputInner<T>>>);

/// Consumes input data from the queue. Held by the future.
pub struct InputConsumer<T>(Rc<RefCell<InputInner<T>>>);

impl<T> Clone for InputProducer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for InputConsumer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Input producer/consumer pair.
pub struct InputQueue<T> {
    pub producer: InputProducer<T>,
    pub consumer: InputConsumer<T>,
}

impl<T> InputQueue<T> {
    pub fn new(local: ThreadLocalWaker) -> Self {
        let inner = Rc::new(RefCell::new(InputInner {
            buffer: VecDeque::new(),
            closed: false,
            waker: Waker::noop().clone(),
            local,
        }));
        Self {
            producer: InputProducer(inner.clone()),
            consumer: InputConsumer(inner),
        }
    }
}

impl<T> InputProducer<T> {
    /// Enqueue + wake the std waker the future last registered.
    pub fn push(&self, item: Input<T>) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.push_back(item);
        inner.waker.wake_by_ref();
    }

    /// Re-open after a Flush cycle. Errors if the future didn't fully
    /// drain or didn't observe the Flush — both are routine bugs.
    pub fn reset(&self) -> Result<(), Error> {
        let mut inner = self.0.borrow_mut();
        if !inner.buffer.is_empty() {
            return Err(crate::fatal!(
                "input queue still has items on reset — future returned without draining input"
            ));
        }
        if !inner.closed {
            return Err(crate::fatal!(
                "input queue not closed on reset — future returned without receiving Flush"
            ));
        }
        inner.closed = false;
        Ok(())
    }
}

impl<T> InputConsumer<T> {
    /// Await the next input item.
    ///
    /// Returns `Ok(Input::Data(T))` for data, `Ok(Input::Flush)` when
    /// flushed. After Flush, subsequent calls return `Err(Closed)`.
    pub fn recv(&self) -> RecvFut<T> {
        RecvFut(self.clone())
    }

    /// Await the next input item, bounded by `timeout`.
    ///
    /// Resolves to `Ok(Some(Input::*))` if an item arrives first,
    /// `Ok(None)` if `timeout` elapses first. After a `Flush`,
    /// subsequent calls return `Err(Closed)` (same as [recv]).
    ///
    /// The deadline is fixed at call time (`Instant::now() + timeout`),
    /// not at first poll.
    pub fn recv_with_timeout(&self, timeout: Duration) -> RecvTimeoutFut<T> {
        RecvTimeoutFut {
            consumer: self.clone(),
            deadline: Instant::now() + timeout,
            scheduled: false,
        }
    }
}

/// Future that resolves to the next [Input] item.
///
/// Stores the waker from Work's `cx` — [InputProducer::push] wakes it.
pub struct RecvFut<T>(InputConsumer<T>);

impl<T: Unpin> Future for RecvFut<T> {
    type Output = Result<Input<T>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.0.0.borrow_mut();

        // After Flush, the queue is single-use — subsequent recv()s
        // surface Closed until reset() is called by the Input phase.
        if inner.closed {
            return Poll::Ready(Err(crate::closed!()));
        }

        // Refresh the registered std waker every poll: the executor
        // may have handed us a different one this round.
        inner.waker = cx.waker().clone();

        match inner.buffer.pop_front() {
            Some(item) => {
                if matches!(item, Input::Flush) {
                    inner.closed = true;
                }
                Poll::Ready(Ok(item))
            }
            None => Poll::Pending,
        }
    }
}

/// Future that resolves to the next [Input] item or to `None` on
/// timeout. Returned by [InputConsumer::recv_with_timeout].
pub struct RecvTimeoutFut<T> {
    consumer: InputConsumer<T>,
    deadline: Instant,
    /// `schedule_at` registers a heap entry; calling it on every poll
    /// would create stale entries. Track whether we've already armed
    /// the timer.
    scheduled: bool,
}

impl<T: Unpin> Future for RecvTimeoutFut<T> {
    type Output = Result<Option<Input<T>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        // Snapshot self fields up front. Below we'll hold a borrow on
        // `self.consumer.0` (via `inner`); that borrow chain keeps
        // self immutably borrowed, blocking writes to `self.scheduled`
        // until after `drop(inner)`. So: read scheduled now, write it
        // after the inner borrow is released.
        let deadline = self.deadline;
        let needs_schedule = !self.scheduled;
        let mut inner = self.consumer.0.borrow_mut();

        if inner.closed {
            return Poll::Ready(Err(crate::closed!()));
        }
        if let Some(item) = inner.buffer.pop_front() {
            if matches!(item, Input::Flush) {
                inner.closed = true;
            }
            return Poll::Ready(Ok(Some(item)));
        }
        if Instant::now() >= deadline {
            return Poll::Ready(Ok(None));
        }

        // Empty + deadline still future: arm both wake sources, park.
        inner.waker = cx.waker().clone();
        if needs_schedule {
            inner.local.schedule_at(deadline);
        }
        drop(inner);
        self.scheduled = true;
        Poll::Pending
    }
}

// ============================================================
// Output queue
// ============================================================

struct OutputInner<T> {
    buffer: VecDeque<T>,
    waker: ThreadLocalWaker,
}

/// Pushes output data into the queue. Held by the future.
/// Wakes the Output phase on push via [ThreadLocalWaker].
pub struct OutputProducer<T>(Rc<RefCell<OutputInner<T>>>);

/// Consumes output data from the queue. Held by the node's Output phase.
pub struct OutputConsumer<T>(Rc<RefCell<OutputInner<T>>>);

impl<T> Clone for OutputProducer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for OutputConsumer<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Output producer/consumer pair.
pub struct OutputQueue<T> {
    pub producer: OutputProducer<T>,
    pub consumer: OutputConsumer<T>,
}

impl<T> OutputQueue<T> {
    pub fn new(waker: ThreadLocalWaker) -> Self {
        let inner = Rc::new(RefCell::new(OutputInner {
            buffer: VecDeque::new(),
            waker,
        }));
        Self {
            producer: OutputProducer(inner.clone()),
            consumer: OutputConsumer(inner),
        }
    }
}

impl<T> OutputProducer<T> {
    /// Enqueue one item and wake the Output phase via the local waker.
    pub fn push(&self, item: T) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.push_back(item);
        inner.waker.wake();
    }

    /// Enqueue many items and wake once — saves N-1 wake calls when
    /// producing a batch.
    pub fn extend(&self, items: impl IntoIterator<Item = T>) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.extend(items);
        inner.waker.wake();
    }
}

impl<T> OutputConsumer<T> {
    pub fn pop(&self) -> Option<T> {
        self.0.borrow_mut().buffer.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::poll::queue::PollQueue;
    use std::task::Context;

    fn local_waker() -> ThreadLocalWaker {
        // A real ThreadLocalWaker bound to a real Scheduler. Node id
        // is arbitrary — we never run the consumer in these tests.
        let pq = PollQueue::new();
        let (_consumer, local_producer) = pq.local();
        ThreadLocalWaker::from_producer(0, &local_producer)
    }

    fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        Pin::new(fut).poll(&mut cx)
    }

    // ---- recv_with_timeout: data / flush / closed paths ----

    #[test]
    fn timeout_returns_buffered_item() {
        let q = InputQueue::<usize>::new(local_waker());
        q.producer.push(Input::Data(7));
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
            panic!("expected Ready(Some(Data))");
        };
        assert_eq!(n, 7);
    }

    #[test]
    fn timeout_returns_flush_and_marks_closed() {
        let q = InputQueue::<usize>::new(local_waker());
        q.producer.push(Input::Flush);
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(
            poll_once(&mut fut),
            Poll::Ready(Ok(Some(Input::Flush)))
        ));
        // After Flush, subsequent recvs see the queue closed.
        let mut next = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut next), Poll::Ready(Err(_))));
    }

    #[test]
    fn timeout_pending_when_empty_and_deadline_future() {
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        // The future armed the timer on first poll.
        assert!(fut.scheduled, "schedule_at should have armed the timer");
    }

    #[test]
    fn timeout_idempotent_schedule_on_repoll() {
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(fut.scheduled);
        // Spurious re-poll: still Pending, scheduled stays true (no
        // second schedule_at call).
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(fut.scheduled);
    }

    #[test]
    fn timeout_in_past_returns_none() {
        // Bypass the public API to construct a past deadline without
        // sleeping. RecvTimeoutFut fields are private but visible
        // from this module.
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = RecvTimeoutFut {
            consumer: q.consumer.clone(),
            deadline: Instant::now() - Duration::from_millis(50),
            scheduled: false,
        };
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(None))));
    }

    #[test]
    fn timeout_in_past_with_buffered_item_returns_item() {
        // Item available — takes precedence over expired deadline.
        let q = InputQueue::<usize>::new(local_waker());
        q.producer.push(Input::Data(99));
        let mut fut = RecvTimeoutFut {
            consumer: q.consumer.clone(),
            deadline: Instant::now() - Duration::from_millis(50),
            scheduled: false,
        };
        let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
            panic!("expected Ready(Some(Data)) — item beats timeout");
        };
        assert_eq!(n, 99);
    }

    #[test]
    fn timeout_push_after_pending_resolves_with_data() {
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        q.producer.push(Input::Data(11));
        let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
            panic!("expected Ready(Some(Data))");
        };
        assert_eq!(n, 11);
    }
}
