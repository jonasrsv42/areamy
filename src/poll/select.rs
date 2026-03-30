//! Select combinator — completes when the first future finishes.
//!
//! Returns the index of the completed future. Remaining futures are
//! dropped when Select is dropped.

use crate::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// Select from multiple futures. Completes when the first one finishes.
/// Returns the index of the completed future. Propagates errors.
///
/// Remaining futures are dropped with the Select.
///
/// ```ignore
/// let idx = Select::pair(
///     Box::pin(async { /* task a */ Ok(()) }),
///     Box::pin(async { /* task b */ Ok(()) }),
/// ).await?;
/// // idx == 0 or 1
/// ```
pub struct Select {
    futures: Vec<BoxFut>,
}

impl Select {
    pub fn new(futures: Vec<BoxFut>) -> Self {
        Self { futures }
    }

    pub fn pair(a: BoxFut, b: BoxFut) -> Self {
        Self::new(vec![a, b])
    }
}

impl Future for Select {
    type Output = Result<usize, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<usize, Error>> {
        for (i, fut) in self.futures.iter_mut().enumerate() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(i)),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    fn select_returns_first_ready() {
        let a: BoxFut = Box::pin(PendingForever);
        let b: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let mut select = Select::pair(a, b);

        let mut cx = noop_cx();
        match Pin::new(&mut select).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 1),
            other => panic!("expected Ready(Ok(1)), got {:?}", other),
        }
    }

    #[test]
    fn select_returns_first_in_order() {
        let a: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let b: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let mut select = Select::pair(a, b);

        let mut cx = noop_cx();
        match Pin::new(&mut select).poll(&mut cx) {
            Poll::Ready(Ok(idx)) => assert_eq!(idx, 0),
            other => panic!("expected Ready(Ok(0)), got {:?}", other),
        }
    }

    #[test]
    fn select_propagates_error() {
        let a: BoxFut = Box::pin(ReadyFut(Some(Err(crate::fatal!("boom")))));
        let b: BoxFut = Box::pin(PendingForever);
        let mut select = Select::pair(a, b);

        let mut cx = noop_cx();
        match Pin::new(&mut select).poll(&mut cx) {
            Poll::Ready(Err(_)) => {}
            other => panic!("expected error, got {:?}", other),
        }
    }

    #[test]
    fn select_pending_when_all_pending() {
        let a: BoxFut = Box::pin(PendingForever);
        let b: BoxFut = Box::pin(PendingForever);
        let mut select = Select::pair(a, b);

        let mut cx = noop_cx();
        assert!(matches!(Pin::new(&mut select).poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn select_drops_remaining() {
        let dropped = Arc::new(AtomicUsize::new(0));

        struct DropCounter(Arc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl Future for DropCounter {
            type Output = Result<(), Error>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
                Poll::Pending
            }
        }

        let a: BoxFut = Box::pin(ReadyFut(Some(Ok(()))));
        let b: BoxFut = Box::pin(DropCounter(dropped.clone()));
        let c: BoxFut = Box::pin(DropCounter(dropped.clone()));

        let mut select = Select::new(vec![a, b, c]);
        let mut cx = noop_cx();
        let _ = Pin::new(&mut select).poll(&mut cx);

        drop(select);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}
