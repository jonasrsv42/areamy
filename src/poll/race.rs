//! First-to-complete race between two futures.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

/// One of two heterogeneous values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Race two futures; resolve with whichever finishes first and drop
/// the loser (the pinned locals die with the completing frame).
/// Error-agnostic — fallible arms surface as
/// `Either<Result<..>, Result<..>>` for the caller to judge.
/// Left-biased when both are ready in the same poll.
pub async fn race<LeftFut: Future, RightFut: Future>(
    left: LeftFut,
    right: RightFut,
) -> Either<LeftFut::Output, RightFut::Output> {
    let mut left = pin!(left);
    let mut right = pin!(right);
    poll_fn(|cx| {
        if let Poll::Ready(value) = left.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(value));
        }
        if let Poll::Ready(value) = right.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(value));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;

    fn poll_once<F: Future>(fut: &mut Pin<Box<F>>) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        fut.as_mut().poll(&mut cx)
    }

    #[test]
    fn race_resolves_with_first_ready() {
        let mut fut = Box::pin(race(std::future::pending::<usize>(), async { "b" }));
        assert!(matches!(
            poll_once(&mut fut),
            Poll::Ready(Either::Right("b"))
        ));
    }

    #[test]
    fn race_is_left_biased() {
        let mut fut = Box::pin(race(async { 1usize }, async { 2usize }));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Either::Left(1))));
    }

    #[test]
    fn race_pending_until_either_arm() {
        let mut fut = Box::pin(race(
            std::future::pending::<usize>(),
            std::future::pending::<&str>(),
        ));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
    }

    #[test]
    fn race_drops_the_loser_eagerly() {
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        impl Future for DropFlag {
            type Output = usize;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
                Poll::Pending
            }
        }

        let dropped = Rc::new(Cell::new(false));
        let mut fut = Box::pin(race(async { 1usize }, DropFlag(dropped.clone())));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Either::Left(1))));
        // Eager: the completing frame dropped its pinned locals — the
        // loser died at resolution, not at drop of the combinator.
        assert!(dropped.get());
    }
}
