//! Join combinator for concurrent futures within a single node.
//!
//! Each node runs one future. For concurrent sub-tasks (e.g. bidi
//! writer + reader), use [Join] inside the future. Completed
//! sub-futures are set to None and never re-polled.

use crate::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

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
    pub fn join(futures: impl IntoIterator<Item = BoxFut>) -> Self {
        Self {
            futures: futures.into_iter().map(Some).collect(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    struct ReadyFut(Option<Result<(), Error>>);

    impl Future for ReadyFut {
        type Output = Result<(), Error>;
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Poll::Ready(self.0.take().unwrap())
        }
    }

    struct PendingForever;
    impl Future for PendingForever {
        type Output = Result<(), Error>;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Poll::Pending
        }
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(std::task::Waker::noop())
    }

    #[test]
    fn join_completes_when_all_ready() {
        let a: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let b: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let mut join = Join::join(vec![a, b]);

        let mut cx = noop_cx();
        assert!(matches!(
            Pin::new(&mut join).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn join_pending_when_one_pending() {
        let a: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let b: BoxFut = Box::pin(PendingForever);
        let mut join = Join::join(vec![a, b]);

        let mut cx = noop_cx();
        assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn join_propagates_first_error() {
        let a: BoxFut = Box::pin(ReadyFut(Some(Err(crate::fatal!("boom")))));
        let b: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let mut join = Join::join(vec![a, b]);

        let mut cx = noop_cx();
        match Pin::new(&mut join).poll(&mut cx) {
            Poll::Ready(Err(e)) => assert!(e.to_string().contains("boom")),
            other => panic!("expected error, got {:?}", other),
        }
    }

    #[test]
    fn join_does_not_repoll_completed() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        struct CountingFut {
            count: Arc<AtomicUsize>,
            ready: bool,
        }
        impl Future for CountingFut {
            type Output = Result<(), Error>;
            fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
                self.count.fetch_add(1, Ordering::SeqCst);
                if self.ready {
                    Poll::Ready(Ok(()))
                } else {
                    self.ready = true;
                    Poll::Pending
                }
            }
        }

        let a: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let b: BoxFut = Box::pin(CountingFut {
            count: count_clone,
            ready: false,
        });
        let mut join = Join::join(vec![a, b]);

        let mut cx = noop_cx();
        // First poll: a completes (set to None), b pending
        assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Pending));
        // Second poll: only b polled (a is None), b completes
        assert!(matches!(
            Pin::new(&mut join).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
        // b was polled exactly 2 times
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
