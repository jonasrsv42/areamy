//! Join combinator for concurrent futures within a single node.
//!
//! Each node runs one future. For concurrent sub-tasks (e.g. bidi
//! writer + reader), use [Join] inside the future. Completed
//! sub-futures are set to None and never re-polled.

use crate::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

/// Join multiple futures concurrently. Completes when all finish.
/// Propagates the first error encountered.
///
/// Safe because [Pin<Box<dyn Future>>] is [Unpin] — no projection needed.
///
/// ```ignore
/// let writer = Box::pin(async { /* ... */ Ok(()) });
/// let reader = Box::pin(async { /* ... */ Ok(()) });
/// Join::new(vec![writer, reader]).await?;
/// ```
pub struct Join {
    futures: Vec<Option<BoxFut>>,
}

impl Join {
    pub fn new(futures: Vec<BoxFut>) -> Self {
        Self {
            futures: futures.into_iter().map(Some).collect(),
        }
    }

    /// Join exactly two futures.
    pub fn pair(a: BoxFut, b: BoxFut) -> Self {
        Self::new(vec![a, b])
    }
}

impl Future for Join {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let mut all_done = true;

        for slot in self.futures.iter_mut() {
            if let Some(fut) = slot {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        *slot = None;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => all_done = false,
                }
            }
        }

        if all_done {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
