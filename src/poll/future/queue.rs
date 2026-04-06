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
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Waker;

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
    waker: Waker,
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
    pub fn new() -> Self {
        let inner = Rc::new(RefCell::new(InputInner {
            buffer: VecDeque::new(),
            closed: false,
            waker: Waker::noop().clone(),
        }));
        Self {
            producer: InputProducer(inner.clone()),
            consumer: InputConsumer(inner),
        }
    }
}

impl<T> InputProducer<T> {
    pub fn push(&self, item: Input<T>) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.push_back(item);
        inner.waker.wake_by_ref();
    }

    /// Reset closed state for a new segment.
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
}

/// Future that resolves to the next [Input] item.
///
/// Stores the waker from Work's `cx` — [InputProducer::push] wakes it.
pub struct RecvFut<T>(InputConsumer<T>);

impl<T: Unpin> Future for RecvFut<T> {
    type Output = Result<Input<T>, Error>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let mut inner = self.0.0.borrow_mut();

        if inner.closed {
            return core::task::Poll::Ready(Err(crate::closed!()));
        }

        inner.waker = cx.waker().clone();

        match inner.buffer.pop_front() {
            Some(item) => {
                if matches!(item, Input::Flush) {
                    inner.closed = true;
                }
                core::task::Poll::Ready(Ok(item))
            }
            None => core::task::Poll::Pending,
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
    pub fn push(&self, item: T) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.push_back(item);
        inner.waker.wake();
    }

    /// Push multiple items, wake once. Use when producing a batch
    /// to avoid redundant Output phase wakes.
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
