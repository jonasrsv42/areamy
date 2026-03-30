//! Async queue for communication between a [FutureRoutine](super::FutureRoutine)
//! and its future. Zero-cost via `Rc<RefCell>` — single-threaded, no locks.
//!
//! Created on the async thread via [RoutineFactory](crate::RoutineFactory).
//! Never crosses threads.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

/// Shared single-threaded queue. Clone shares the same backing store.
pub struct Queue<T>(Rc<RefCell<VecDeque<T>>>);

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(VecDeque::new())))
    }

    pub fn push(&self, item: T) {
        self.0.borrow_mut().push_back(item);
    }

    pub fn pop(&self) -> Option<T> {
        self.0.borrow_mut().pop_front()
    }

    /// Await the next item. Returns [core::task::Poll::Pending] if empty —
    /// the node's waker handles re-polling when new data arrives.
    pub fn recv(&self) -> RecvFut<T> {
        RecvFut(self.clone())
    }
}

/// Future that resolves when the queue has an item.
pub struct RecvFut<T>(Queue<T>);

impl<T: Unpin> Future for RecvFut<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut core::task::Context<'_>) -> core::task::Poll<T> {
        match self.0.pop() {
            Some(item) => core::task::Poll::Ready(item),
            None => core::task::Poll::Pending,
        }
    }
}
