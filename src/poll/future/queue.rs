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

use crate::connect::poll::queue::TimerKey;
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
    /// Areamy local waker for the owning node. [RecvTimeoutFut] uses
    /// this to call [ThreadLocalWaker::schedule_at] so the node is
    /// re-polled when its deadline elapses.
    local: ThreadLocalWaker,
}

impl<T> InputInner<T> {
    /// Resolve-or-not, shared by both recv futures: closed → `Err`,
    /// buffered item → `Ok` (a Flush closes the queue), empty → `None`
    /// (caller parks).
    fn try_take(&mut self) -> Option<Result<Input<T>, Error>> {
        if self.closed {
            return Some(Err(crate::closed!()));
        }
        let item = self.buffer.pop_front()?;
        if matches!(item, Input::Flush) {
            self.closed = true;
        }
        Some(Ok(item))
    }
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
    /// not at first poll. Huge timeouts (e.g. `Duration::MAX` as
    /// "never") saturate to a far-future deadline instead of
    /// panicking on `Instant` overflow.
    pub fn recv_with_timeout(&self, timeout: Duration) -> RecvTimeoutFut<T> {
        // ~30 years: far beyond any process lifetime, safely addable.
        const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);
        let now = Instant::now();
        RecvTimeoutFut {
            consumer: self.clone(),
            deadline: now.checked_add(timeout).unwrap_or(now + FAR_FUTURE),
            timer: None,
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
        // Register on every poll, Ready included: the queue outlives
        // this future across flush cycles (reset() keeps the waker),
        // so a Ready-only first batch must still leave a live waker
        // behind for the next push.
        inner.waker = cx.waker().clone();
        match inner.try_take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

/// Future that resolves to the next [Input] item or to `None` on
/// timeout. Returned by [InputConsumer::recv_with_timeout].
///
/// Holds its [TimerKey] while armed and cancels it on every resolve
/// path and on drop — the heap never keeps a dead deadline for a
/// finished recv.
pub struct RecvTimeoutFut<T> {
    consumer: InputConsumer<T>,
    deadline: Instant,
    /// Armed timer, registered on first pending poll.
    timer: Option<TimerKey>,
}

impl<T: Unpin> Future for RecvTimeoutFut<T> {
    type Output = Result<Option<Input<T>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        // Unpin: get_mut gives disjoint field borrows, so `inner`
        // (via consumer) and `timer` coexist without snapshots.
        let this = self.get_mut();
        let mut inner = this.consumer.0.borrow_mut();
        // Register on every poll, Ready included — see RecvFut.
        inner.waker = cx.waker().clone();

        // Buffered item / closed beats an expired deadline.
        let poll = match inner.try_take() {
            Some(result) => Poll::Ready(result.map(Some)),
            None if Instant::now() >= this.deadline => Poll::Ready(Ok(None)),
            None => Poll::Pending,
        };

        // Timer transition
        this.timer = match poll {
            // Cancel outstanding timer if we have a data.
            Poll::Ready(_) => {
                if let Some(key) = this.timer {
                    inner.local.cancel(key);
                }
                None
            }
            // Arm a timer if we have no data and no timer yet.
            Poll::Pending => this
                .timer
                .or_else(|| inner.local.schedule_at(this.deadline)),
        };

        poll
    }
}

impl<T> Drop for RecvTimeoutFut<T> {
    /// A dropped-while-armed future (lost `Select` race, cancelled
    /// routine) releases its heap slot instead of leaving a dead
    /// deadline to fire a spurious poll.
    fn drop(&mut self) {
        if let Some(key) = self.timer.take() {
            self.consumer.0.borrow_mut().local.cancel(key);
        }
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

    // ---- waker registration ----

    #[test]
    fn ready_poll_still_registers_waker() {
        // Regression: a first batch resolving every recv() Ready must
        // still install the task waker — reset() keeps the waker
        // across flush cycles, so leaving the initial noop in place
        // stalls the node on the next push.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Flag(AtomicBool);
        impl std::task::Wake for Flag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let q = InputQueue::<usize>::new(local_waker());
        q.producer.push(Input::Data(1));
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = std::task::Waker::from(flag.clone());
        let mut cx = Context::from_waker(&waker);
        let mut fut = q.consumer.recv();
        assert!(matches!(
            Pin::new(&mut fut).poll(&mut cx),
            Poll::Ready(Ok(Input::Data(1)))
        ));
        // The Ready poll installed our waker: the next push fires it.
        q.producer.push(Input::Data(2));
        assert!(flag.0.load(Ordering::SeqCst));
    }

    #[test]
    fn timeout_duration_max_saturates() {
        // Duration::MAX as "never" must not panic on Instant overflow.
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::MAX);
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
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
        assert!(
            fut.timer.is_some(),
            "schedule_at should have armed the timer"
        );
    }

    #[test]
    fn timeout_idempotent_schedule_on_repoll() {
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        let key = fut.timer;
        assert!(key.is_some());
        // Spurious re-poll: still Pending, same key (no second
        // schedule_at call).
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert_eq!(fut.timer, key);
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
            timer: None,
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
            timer: None,
        };
        let Poll::Ready(Ok(Some(Input::Data(n)))) = poll_once(&mut fut) else {
            panic!("expected Ready(Some(Data)) — item beats timeout");
        };
        assert_eq!(n, 99);
    }

    #[test]
    fn timeout_resolving_with_data_releases_timer() {
        let q = InputQueue::<usize>::new(local_waker());
        let mut fut = q.consumer.recv_with_timeout(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(fut.timer.is_some());
        q.producer.push(Input::Data(5));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(Some(_)))));
        // Armed timer cancelled on resolve — no dead deadline left.
        assert!(fut.timer.is_none());
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
