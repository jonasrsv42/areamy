use super::shared::Shared;
use crate::error::Error;
use crate::fatal;
use crate::poll::future::deadline::Deadline;
use core::task::{Context, Poll};
use std::future::{Future, poll_fn};
use std::mem;
use std::rc::Rc;
use std::time::Duration;

/// Sends into the queue. Held by the future. No `push`: an unbounded push beside the bound would
/// defeat it.
pub struct Sender<T>(pub(super) Rc<Shared<T>>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Sender<T> {
    /// Resolves once `item` is queued; waits while the queue is full. Dropped while waiting, it
    /// drops `item`.
    pub fn send(&self, item: T) -> impl Future<Output = Result<(), Error>> + '_ {
        let mut item = Some(item);
        poll_fn(move |cx| self.poll_send(&mut item, cx))
    }

    /// Like [Self::send], bounded by `timeout`: resolves to `Ok(None)` once `item` is queued, or
    /// to `Ok(Some(item))`, handing it back, if `timeout` elapses first.
    ///
    /// The deadline is fixed at call time and saturates, as
    /// [recv_with_timeout](crate::poll::future::input::Receiver::recv_with_timeout)'s.
    pub fn send_with_timeout(
        &self,
        item: T,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<T>, Error>> + '_ {
        let mut item = Some(item);
        let mut deadline = Deadline::after(timeout);
        poll_fn(move |cx| {
            // A pending send leaves the item in place, to hand back on expiry.
            match deadline.poll(self.poll_send(&mut item, cx), &self.0.timer) {
                Poll::Ready(Some(sent)) => Poll::Ready(sent.map(|()| None)),
                Poll::Ready(None) => Poll::Ready(Ok(item.take())),
                Poll::Pending => Poll::Pending,
            }
        })
    }

    /// Queue `item` and take it, or park `cx`'s waker while full, leaving it. Wakes Output after
    /// the borrow if its last pop found the queue empty.
    fn poll_send(&self, item: &mut Option<T>, cx: &Context<'_>) -> Poll<Result<(), Error>> {
        // Before the bound check: a resolved send must not park.
        let Some(value) = item.take() else {
            return Poll::Ready(Err(fatal!("send polled after it resolved")));
        };
        let armed = {
            let mut inner = match self.0.borrow() {
                Ok(inner) => inner,
                Err(e) => return Poll::Ready(Err(e)),
            };
            if self
                .0
                .bound
                .is_some_and(|bound| inner.buffer.len() >= bound.get())
            {
                inner.waiters.park(cx.waker(), &self.0.home);
                *item = Some(value);
                return Poll::Pending;
            }
            inner.buffer.push_back(value);
            mem::take(&mut inner.armed)
        };
        if armed {
            self.0.output.wake();
        }
        Poll::Ready(Ok(()))
    }
}
