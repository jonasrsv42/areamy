//! Once [FutureRoutine] — a [super::FutureRoutine] whose future runs exactly one segment, so the
//! closure is [FnOnce] and may move its captures into the future (a connection, a lease).
//!
//! The segment ends like any segment: the future reads [Input::Flush](crate::poll::future::Input::Flush)
//! and returns. The routine is then spent: the Flush is forwarded and it idles until close.
//! Any later data or Flush is a wiring bug, raised as [Exhausted] so callers can match it with
//! [Error::is].

use crate::any_err;
use crate::error::{AnyErr, Error};
use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
use crate::node::line::poll::routine::LineRoutine;
use crate::node::routine;
use crate::poll::Context;
use crate::poll::future::{input, output};
use alloc::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::num::NonZeroUsize;
use core::pin::Pin;

/// A spent once routine was handed data or a Flush.
#[derive(Debug)]
pub struct Exhausted;

impl fmt::Display for Exhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "once future routine received input after its segment ended"
        )
    }
}

impl core::error::Error for Exhausted {}

fn exhausted() -> Error {
    any_err!(Box::new(Exhausted) as Box<dyn AnyErr>)
}

/// Async routine running one future, built from an [FnOnce].
pub struct FutureRoutine<InType, OutType, Fut> {
    input: input::Sender<InType>,
    output: output::Receiver<OutType>,
    /// `None` once the segment ended: the routine is spent.
    future: Option<Pin<Box<Fut>>>,
}

impl<InType, OutType, Fut> FutureRoutine<InType, OutType, Fut>
where
    Fut: Future<Output = Result<(), Error>>,
{
    /// `bound` caps the input and the output queue each; `None` is unbounded. Calls `factory`
    /// at once: that only builds the future, which runs from the first poll.
    pub fn new<F>(wakers: LineWakers, bound: Option<NonZeroUsize>, factory: F) -> Self
    where
        F: FnOnce(input::Receiver<InType>, output::Sender<OutType>) -> Fut,
    {
        let (input, receiver) = input::channel(
            wakers.input,
            wakers.work.clone(),
            wakers.timer.clone(),
            bound,
        );
        let (sender, output) = output::channel(wakers.output, wakers.work, wakers.timer, bound);
        Self {
            input,
            output,
            future: Some(Box::pin(factory(receiver, sender))),
        }
    }

    /// Create a [LineRoutineFactory] from an `async fn` or a closure returning a future,
    /// unbounded until [Factory::bounded].
    ///
    /// ```ignore
    /// let routine = once::FutureRoutine::factory(move |input, output| async move {
    ///     let connection = connection; // moved in: the closure runs once
    ///     /* ... */
    ///     Ok(())
    /// });
    /// ```
    pub fn factory<F>(factory: F) -> Factory<InType, OutType, F>
    where
        F: FnOnce(input::Receiver<InType>, output::Sender<OutType>) -> Fut,
    {
        Factory {
            factory,
            bound: None,
            types: PhantomData,
        }
    }
}

/// [LineRoutineFactory] of a once [FutureRoutine]; see [FutureRoutine::factory].
pub struct Factory<InType, OutType, F> {
    factory: F,
    bound: Option<NonZeroUsize>,
    /// `fn` keeps the factory `Send` whatever the queue types.
    types: PhantomData<fn(input::Receiver<InType>, output::Sender<OutType>)>,
}

impl<InType, OutType, F> Factory<InType, OutType, F> {
    /// Caps the input and the output queue at `bound` each; `None` is unbounded. Close drops what
    /// the queues hold; Flush before closing to deliver it.
    pub fn bounded(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        self.bound = bound.into();
        self
    }
}

impl<'params, InType, OutType, F, Fut> LineRoutineFactory<'params> for Factory<InType, OutType, F>
where
    InType: 'params,
    OutType: 'params,
    F: FnOnce(input::Receiver<InType>, output::Sender<OutType>) -> Fut + Send + 'params,
    Fut: Future<Output = Result<(), Error>> + 'params,
{
    type Routine = FutureRoutine<InType, OutType, Fut>;

    fn create(self, wakers: LineWakers) -> Self::Routine {
        FutureRoutine::new(wakers, self.bound, self.factory)
    }
}

impl<InType, OutType, Fut> routine::TrySend<InType> for FutureRoutine<InType, OutType, Fut> {
    fn try_send(&mut self, message: InType) -> Result<routine::Accept<InType>, Error> {
        // Checked here, not at the next poll: the queue would take it and nobody reads it.
        if self.future.is_none() {
            return Err(exhausted());
        }
        self.input.try_send(message)
    }
}

impl<InType, OutType, Fut> routine::Next<OutType> for FutureRoutine<InType, OutType, Fut> {
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        self.output.pop()
    }
}

impl<InType, OutType, Fut> routine::Flush for FutureRoutine<InType, OutType, Fut> {
    fn flush(&mut self) -> Result<(), Error> {
        // Must error: the node would enter Flushing with no future to finish it, and hang.
        if self.future.is_none() {
            return Err(exhausted());
        }
        self.input.flush()
    }
}

impl<InType, OutType, Fut> routine::Poll for FutureRoutine<InType, OutType, Fut>
where
    Fut: Future<Output = Result<(), Error>>,
{
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        // Spent: nothing to drive. Never Flushing here, as `flush` refuses first.
        let Some(future) = self.future.as_mut() else {
            return Ok(core::task::Poll::Pending);
        };
        let mut std_cx = core::task::Context::from_waker(&cx.wake.sync);
        match future.as_mut().poll(&mut std_cx) {
            core::task::Poll::Ready(result) => {
                self.future = None;
                result?;
                self.input.ended()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<InType, OutType, Fut> LineRoutine<InType, OutType> for FutureRoutine<InType, OutType, Fut> where
    Fut: Future<Output = Result<(), Error>>
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::fatal;
    use crate::node::line::poll::routine::tests::noop_line_wakers;
    use crate::node::routine::{Accept, Flush, Next, Poll, TrySend};
    use crate::poll::context::mock::noop_context;
    use crate::poll::future::Input;
    use alloc::rc::Rc;
    use core::cell::Cell;

    /// Echoes every value; returns at Flush.
    fn echo(bound: Option<NonZeroUsize>) -> impl LineRoutine<usize, usize> {
        FutureRoutine::new(noop_line_wakers(), bound, |input, output| async move {
            while let Input::Data(value) = input.recv().await? {
                output.send(value).await?;
            }
            Ok(())
        })
    }

    /// Returns at once, reading nothing. Boxed by hand: still just a future.
    fn returns_at_once() -> impl LineRoutine<usize, usize> {
        FutureRoutine::new(noop_line_wakers(), None, |_input, _output| {
            Box::pin(async { Ok(()) })
        })
    }

    fn poll(routine: &mut impl Poll) -> Result<core::task::Poll<()>, Error> {
        Poll::poll(routine, &mut noop_context())
    }

    fn is_exhausted(result: Result<impl fmt::Debug, Error>) -> bool {
        matches!(result, Err(e) if e.is::<Exhausted>())
    }

    fn is_fatal(result: Result<core::task::Poll<()>, Error>) -> bool {
        matches!(
            result,
            Err(Error {
                kind: ErrorKind::Fatal(_),
                ..
            })
        )
    }

    #[test]
    fn input_before_the_first_poll_is_taken() {
        let mut routine = echo(None);
        assert_eq!(routine.try_send(1).unwrap(), Accept::Accepted);
        assert!(poll(&mut routine).unwrap().is_pending());
        assert_eq!(routine.next().unwrap(), Some(1));
    }

    #[test]
    fn a_bounded_queue_refuses_while_the_segment_runs() {
        let mut routine = echo(NonZeroUsize::new(1));
        assert_eq!(routine.try_send(1).unwrap(), Accept::Accepted);
        assert_eq!(routine.try_send(2).unwrap(), Accept::Full(2));
    }

    #[test]
    fn the_segment_ends_at_flush_and_its_output_stays_readable() {
        let mut routine = echo(None);
        assert_eq!(routine.try_send(1).unwrap(), Accept::Accepted);
        routine.flush().unwrap();
        assert!(poll(&mut routine).unwrap().is_ready());
        assert_eq!(routine.next().unwrap(), Some(1));
        assert_eq!(routine.next().unwrap(), None);
    }

    #[test]
    fn spent_it_idles() {
        let mut routine = echo(None);
        routine.flush().unwrap();
        assert!(poll(&mut routine).unwrap().is_ready());
        // Ready again would be fatal in Running: no flush is pending.
        assert!(poll(&mut routine).unwrap().is_pending());
        assert!(poll(&mut routine).unwrap().is_pending());
    }

    #[test]
    fn spent_it_refuses_data_as_exhausted() {
        let mut routine = echo(None);
        routine.flush().unwrap();
        assert!(poll(&mut routine).unwrap().is_ready());
        assert!(is_exhausted(routine.try_send(2)));
    }

    #[test]
    fn spent_it_refuses_a_flush_as_exhausted() {
        let mut routine = echo(None);
        routine.flush().unwrap();
        assert!(poll(&mut routine).unwrap().is_ready());
        assert!(is_exhausted(routine.flush()));
    }

    #[test]
    fn returning_before_the_flush_is_fatal() {
        let mut routine = returns_at_once();
        assert!(is_fatal(poll(&mut routine)));
    }

    #[test]
    fn returning_with_input_unread_is_fatal() {
        let mut routine = returns_at_once();
        assert_eq!(routine.try_send(1).unwrap(), Accept::Accepted);
        routine.flush().unwrap();
        assert!(is_fatal(poll(&mut routine)));
    }

    #[test]
    fn the_futures_error_comes_back_unchanged() {
        let mut routine = FutureRoutine::<usize, usize, _>::new(
            noop_line_wakers(),
            None,
            |_input, _output| async { Err(fatal!("boom")) },
        );
        match poll(&mut routine) {
            Err(Error {
                kind: ErrorKind::Fatal(message),
                ..
            }) => assert_eq!(message, "boom"),
            other => panic!("expected the future's error, got {other:?}"),
        }
    }

    /// Not `Clone`: only an [FnOnce] can move it into the future.
    struct Lease(Rc<Cell<bool>>);

    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[test]
    fn the_closure_moves_its_captures_into_the_future_which_drops_them_unpolled() {
        let dropped = Rc::new(Cell::new(false));
        let lease = Lease(dropped.clone());
        let routine = FutureRoutine::<usize, usize, _>::new(
            noop_line_wakers(),
            None,
            move |input, _output| async move {
                let _lease = lease;
                while let Input::Data(_) = input.recv().await? {}
                Ok(())
            },
        );
        assert!(!dropped.get());
        drop(routine);
        assert!(dropped.get());
    }
}
