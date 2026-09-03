//! Value-returning concurrent join with heterogeneous outputs.
//!
//! Deliberately 2-arity ([futures-lite] stance): N arms nest —
//! `try_join(a, try_join(b, c))` — and downstream crates roll flat
//! tuples or fairness if they need them. First `Err` resolves
//! immediately and drops the other arm (the pinned locals die with
//! the completing frame), so cancellation composes with drop-cancels
//! semantics. Zero-alloc: the arms live inline in the returned
//! future, at the price of it being unnameable (`impl Future`).
//!
//! [futures-lite]: https://docs.rs/futures-lite

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

/// Run two fallible futures concurrently; resolve with both values,
/// or with the first error (dropping the other future eagerly).
pub async fn try_join<LeftValue, RightValue, ErrorType>(
    left: impl Future<Output = Result<LeftValue, ErrorType>>,
    right: impl Future<Output = Result<RightValue, ErrorType>>,
) -> Result<(LeftValue, RightValue), ErrorType> {
    let mut left = pin!(left);
    let mut right = pin!(right);
    let mut left_value = None;
    let mut right_value = None;
    poll_fn(|cx| {
        if left_value.is_none() {
            match left.as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => left_value = Some(value),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if right_value.is_none() {
            match right.as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => right_value = Some(value),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if left_value.is_some() && right_value.is_some() {
            if let (Some(left), Some(right)) = (left_value.take(), right_value.take()) {
                return Poll::Ready(Ok((left, right)));
            }
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::pin::Pin;
    use std::task::Context;

    fn poll_once<F: Future>(fut: &mut Pin<Box<F>>) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        fut.as_mut().poll(&mut cx)
    }

    /// Pending once, then ready with the value.
    struct Staged<T>(Option<T>, bool);
    impl<T: Unpin> Future for Staged<T> {
        type Output = Result<T, Error>;
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.1 {
                match self.0.take() {
                    Some(value) => Poll::Ready(Ok(value)),
                    None => Poll::Pending,
                }
            } else {
                self.1 = true;
                Poll::Pending
            }
        }
    }

    #[test]
    fn returns_heterogeneous_values() {
        let mut fut = Box::pin(try_join(async { Ok::<_, Error>(7usize) }, async {
            Ok::<_, Error>("s")
        }));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok((7, "s")))));
    }

    #[test]
    fn waits_for_slowest_arm() {
        let mut fut = Box::pin(try_join(Staged(Some(1usize), false), async {
            Ok::<_, Error>(2usize)
        }));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok((1, 2)))));
    }

    #[test]
    fn first_error_wins() {
        let mut fut = Box::pin(try_join(
            async { Err::<usize, _>(crate::fatal!("boom")) },
            std::future::pending::<Result<usize, Error>>(),
        ));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Err(_))));
    }

    #[test]
    fn error_drops_the_other_arm_eagerly() {
        use std::cell::Cell;
        use std::rc::Rc;

        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        impl Future for DropFlag {
            type Output = Result<usize, Error>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }

        let dropped = Rc::new(Cell::new(false));
        let mut fut = Box::pin(try_join(
            async { Err::<usize, _>(crate::fatal!("boom")) },
            DropFlag(dropped.clone()),
        ));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Err(_))));
        // Eager: the completing frame dropped its pinned locals — the
        // loser died on error resolution, not on drop of the
        // combinator.
        assert!(dropped.get());
    }

    #[test]
    fn nesting_gives_n_arity() {
        let mut fut = Box::pin(try_join(
            async { Ok::<_, Error>(1usize) },
            try_join(Staged(Some("mid"), false), async { Ok::<_, Error>(3.0f32) }),
        ));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(matches!(
            poll_once(&mut fut),
            Poll::Ready(Ok((1, ("mid", x)))) if x == 3.0
        ));
    }
}
