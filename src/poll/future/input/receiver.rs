use super::Input;
use super::shared::Shared;
use crate::closed;
use crate::error::Error;
use crate::poll::future::deadline::Deadline;
use core::task::{Context, Poll};
use std::future::{Future, poll_fn};
use std::mem;
use std::rc::Rc;
use std::time::Duration;

/// Receives from the queue. Held by the future.
pub struct Receiver<T>(pub(super) Rc<Shared<T>>);

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Receiver<T> {
    /// Await the next input item.
    ///
    /// Returns `Ok(Input::Data(T))` for data, `Ok(Input::Flush)` when
    /// flushed. After Flush, subsequent calls return `Err(Closed)`.
    pub fn recv(&self) -> impl Future<Output = Result<Input<T>, Error>> + '_ {
        poll_fn(|cx| self.poll_take(cx))
    }

    /// Await the next input item, bounded by `timeout`.
    ///
    /// Resolves to `Ok(Some(Input::*))` if an item arrives first,
    /// `Ok(None)` if `timeout` elapses first. After a `Flush`,
    /// subsequent calls return `Err(Closed)` (same as [Self::recv]).
    ///
    /// The deadline is fixed at call time (`Instant::now() + timeout`),
    /// not at first poll. Huge timeouts (e.g. `Duration::MAX` as
    /// "never") saturate to a far-future deadline instead of
    /// panicking on `Instant` overflow.
    pub fn recv_with_timeout(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<Input<T>>, Error>> + '_ {
        let mut deadline = Deadline::after(timeout);
        poll_fn(move |cx| {
            deadline
                .poll(self.poll_take(cx), &self.0.timer)
                .map(Option::transpose)
        })
    }

    /// Take the next item, or park `cx`'s waker while empty. A pop after a refusal wakes Input,
    /// after the borrow.
    fn poll_take(&self, cx: &Context<'_>) -> Poll<Result<Input<T>, Error>> {
        let (item, refused) = {
            let mut inner = match self.0.borrow() {
                Ok(inner) => inner,
                Err(e) => return Poll::Ready(Err(e)),
            };
            if inner.closed {
                return Poll::Ready(Err(closed!()));
            }
            let Some(item) = inner.buffer.pop_front() else {
                inner.waiters.park(cx.waker(), &self.0.home);
                return Poll::Pending;
            };
            if matches!(item, Input::Flush) {
                inner.closed = true;
            }
            (item, mem::take(&mut inner.refused))
        };
        if refused {
            self.0.input.wake();
        }
        Poll::Ready(Ok(item))
    }
}
