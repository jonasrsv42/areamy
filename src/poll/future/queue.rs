//! Async queue for communication between a [FutureRoutine](super::FutureRoutine)
//! and its future. Zero-cost via `Rc<RefCell>` — single-threaded, no locks.
//!
//! Created on the async thread via [RoutineFactory](crate::RoutineFactory).
//! Never crosses threads.
//!
//! The input queue is closeable: after [Input::Flush] is delivered,
//! subsequent [Queue::recv] calls return [Err(Closed)](crate::error::ErrorKind::Closed),
//! enforcing the contract that the future must return after flush.
//!
//! Waker-aware: [RecvFut] stores the waker from Work's `cx`,
//! [Queue::push] wakes it. This triggers only the Work phase.

use crate::error::Error;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Waker;

/// Input item received by the future via [Queue::recv].
///
/// The user awaits [Queue::recv] which returns `Result<Input<T>, Error>`.
/// After [Input::Flush] is delivered the queue is closed — subsequent
/// recv calls return [Err(Closed)](crate::error::ErrorKind::Closed).
pub enum Input<T> {
    Data(T),
    Flush,
}

struct Inner<T> {
    buffer: VecDeque<T>,
    closed: bool,
    waker: Waker,
}

/// Shared single-threaded queue. Clone shares the same backing store.
///
/// Closeable: once [Input::Flush] is popped via [Queue::recv],
/// the queue transitions to closed and subsequent recv calls
/// return [Err(Closed)](crate::error::ErrorKind::Closed).
///
/// Waker-aware: [Queue::push] wakes the stored waker.
pub struct Queue<T>(Rc<RefCell<Inner<T>>>);

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(Inner {
            buffer: VecDeque::new(),
            closed: false,
            waker: Waker::noop().clone(),
        })))
    }

    pub fn push(&self, item: T) {
        let mut inner = self.0.borrow_mut();
        inner.buffer.push_back(item);
        inner.waker.wake_by_ref();
    }

    pub fn pop(&self) -> Option<T> {
        self.0.borrow_mut().buffer.pop_front()
    }

    /// Reset closed state for a new segment.
    ///
    /// Returns an error if the queue is not empty or not closed —
    /// the future should have consumed all data and received Flush
    /// before returning Ready.
    pub fn reset(&self) -> Result<(), crate::error::Error> {
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

impl<T> Queue<Input<T>> {
    /// Await the next input item.
    ///
    /// Returns `Ok(Input::Data(T))` for data, `Ok(Input::Flush)` when
    /// the stream is flushed. After [Input::Flush] has been delivered,
    /// the queue is closed and all subsequent calls return `Err(Closed)`.
    pub fn recv(&self) -> RecvFut<T> {
        RecvFut(self.clone())
    }
}

/// Future that resolves to the next [Input] item, or errors with
/// [Closed](crate::error::ErrorKind::Closed) if the queue is closed.
///
/// Stores the waker from Work's `cx` — [Queue::push] wakes it,
/// triggering only the Work phase pollable.
pub struct RecvFut<T>(Queue<Input<T>>);

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
