//! [FutureRoutine] — wraps a user-provided async fn into an
//! [LineRoutine](crate::poll::LineRoutine).
//!
//! The user's async fn receives an [InputConsumer] and [OutputProducer].
//! Data arrives as [Input::Data], flush as [Input::Flush].
//! The future drives I/O, reads from input, writes to output.
//!
//! Created via [LineRoutineFactory](crate::node::line::poll::factory::LineRoutineFactory)
//! — the routine is born on the async thread and can hold non-Send types.
//!
//! # Lifecycle
//!
//! When the future completes (returns `Ok(())`), the routine returns
//! [core::task::Poll::Ready]. The node forwards the pending Flush signal
//! (or closes on Closing). On the next poll, the factory recreates the
//! future for the next segment.
//!
//! # Example
//!
//! ```ignore
//! use areamy::poll::future::{FutureRoutine, Input, InputConsumer, OutputProducer};
//! use areamy::poll::try_join;
//!
//! let routine = |output_waker| FutureRoutine::new(output_waker, |input, output| {
//!     Box::pin(async move {
//!         let socket = connect("server").await;
//!
//!         let writer = async {
//!             loop {
//!                 match input.recv().await? {
//!                     Input::Data(val) => socket.write(val).await,
//!                     Input::Flush => { socket.half_close().await; break; }
//!                 }
//!             }
//!             Ok(())
//!         };
//!
//!         let reader = async {
//!             while let Some(val) = socket.read().await {
//!                 output.push(val);
//!             }
//!             Ok(())
//!         };
//!
//!         try_join(writer, reader).await?;
//!         Ok(())
//!     })
//! });
//!
//! let node = thread.line(routine).input::<Sync>().output::<Sync>();
//! ```

use crate::connect::waker;
use crate::error::Error;
use crate::node::Name;
use crate::node::line::poll::factory::LineWakers;
use crate::node::line::poll::routine::LineRoutine;
use crate::poll::future::queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue};
use std::future::Future;
use std::pin::Pin;

/// Boxed future returned by a [`FutureRoutine`] factory closure. The
/// `'params` lifetime bounds non-`'static` captures (e.g. `&Model`) the
/// async body holds — see [`FutureRoutine`] for the cascade rationale.
pub type BoxFut<'params> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'params>>;

/// Async routine wrapping a user-provided future factory.
///
/// Generic over `'params` (the borrow lifetime of anything the future
/// captures from the surrounding scope, e.g. `&Model`), `InType`,
/// `OutType`, and the factory `F`.
pub struct FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    input: InputQueue<InType>,
    output: OutputQueue<OutType>,
    factory: F,
    future: Option<BoxFut<'params>>,
}

impl<'params, InType, OutType, F> FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    pub fn new(wakers: LineWakers, mut factory: F) -> Self {
        // InputQueue gets the *work* waker so `recv_with_timeout`'s
        // `schedule_at` re-polls the future via the work phase (the
        // one that actually polls the routine). OutputQueue gets the
        // output waker so push() nudges Output to drain.
        let input = InputQueue::new(wakers.work);
        let output = OutputQueue::new(wakers.output);
        let future = (factory)(input.consumer.clone(), output.producer.clone());
        Self {
            input,
            output,
            factory,
            future: Some(future),
        }
    }

    /// Create a [LineRoutineFactory](crate::node::line::poll::factory::LineRoutineFactory)
    /// from an async closure. The closure receives input/output queues —
    /// wakers are wired automatically.
    ///
    /// ```ignore
    /// let node = thread.line(FutureRoutine::factory(|input, output| {
    ///     Box::pin(async move { /* ... */ })
    /// })).input::<Sync>().output::<Sync>();
    /// ```
    pub fn factory(f: F) -> impl FnOnce(LineWakers) -> Self + Send + 'params
    where
        F: Send,
    {
        move |wakers| Self::new(wakers, f)
    }
}

impl<'params, InType, OutType, F> crate::Send<InType> for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    fn send(&mut self, message: InType) -> Result<(), Error> {
        self.input.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<'params, InType, OutType, F> crate::Next<OutType>
    for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<'params, InType, OutType, F> crate::Flush for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.producer.push(Input::Flush);
        Ok(())
    }
}

impl<'params, InType, OutType, F> crate::Poll for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
    fn poll(&mut self, waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        let future = self.future.get_or_insert_with(|| {
            (self.factory)(self.input.consumer.clone(), self.output.producer.clone())
        });

        let mut cx = core::task::Context::from_waker(&waker.sync);
        match future.as_mut().poll(&mut cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                self.input.producer.reset()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<'params, InType, OutType, F> Name for FutureRoutine<'params, InType, OutType, F> where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params
{
}

impl<'params, InType, OutType, F> LineRoutine<InType, OutType>
    for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut<'params> + 'params,
{
}

#[cfg(test)]
mod tests {
    //! End-to-end lifetime check: a `FutureRoutine` whose async body
    //! borrows from the surrounding scope. Without `'params` on
    //! [`BoxFut`] the closure would be locked to `'static` and the
    //! `&multiplier` capture wouldn't compile.

    use super::*;
    use crate::connect::sync::Receiver;
    use crate::marker::Unary;
    use crate::poll;
    use crate::thread::{ThreadBundle, ThreadStream};
    use crate::work::{Writer, make_line};
    use crate::{Closeable, Message, Pushable, ThreadId, make_push, make_work};

    #[derive(Debug, Clone)]
    struct LineFutureThread;
    impl ThreadId for LineFutureThread {}

    #[test]
    fn future_routine_can_borrow_from_scope() {
        let multiplier: usize = 7;
        let mult_ref: &usize = &multiplier;

        let mut thread = poll::Thread::<'_, LineFutureThread>::new();
        let mut node = thread
            .line(FutureRoutine::factory(
                move |input: InputConsumer<usize>, output: OutputProducer<usize>| {
                    Box::pin(async move {
                        loop {
                            match input.recv().await? {
                                Input::Data(n) => output.push(n * *mult_ref),
                                Input::Flush => break,
                            }
                        }
                        Ok(())
                    })
                },
            ))
            .input::<poll::Sync>()
            .output::<poll::Sync>();

        // Driver work-line on a sibling thread (FutureRoutine wants its
        // own poll thread; we feed it from a sync line so the test
        // doesn't need to manage two source/sink wakers).
        let mut driver = make_line(crate::poll::future::line::routine::tests::PassThrough::new());
        let mut input = Writer::new(driver.as_ref()).unwrap();
        make_push(driver.as_mut(), &node).unwrap();

        let output = Receiver::new();
        make_push(&mut node, &output).unwrap();

        thread.add(node);
        let mut work_thread = ThreadStream::<'_, LineFutureThread>::new();
        make_work::<Unary, _>(driver, &mut work_thread).unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(work_thread);
        bundle.add(thread);

        std::thread::scope(|s| {
            let handle = bundle.start(s);

            input.push(Message::Data(4)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(28)); // 4 * 7

            input.push(Message::Data(6)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(42)); // 6 * 7

            input.close().unwrap();
            assert!(handle.join().errors().is_empty());
        });

        let _ = multiplier;
    }

    /// Identity passthrough — drives values into the [`FutureRoutine`]
    /// node from a sibling sync thread.
    pub(super) struct PassThrough {
        out: std::collections::VecDeque<usize>,
    }

    impl PassThrough {
        pub(super) fn new() -> Self {
            Self {
                out: std::collections::VecDeque::new(),
            }
        }
    }

    impl crate::Send<usize> for PassThrough {
        fn send(&mut self, m: usize) -> Result<(), Error> {
            self.out.push_back(m);
            Ok(())
        }
    }

    impl crate::Next<usize> for PassThrough {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl crate::Flush for PassThrough {
        fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    impl crate::node::Name for PassThrough {}
    impl crate::node::line::LineRoutine<usize, usize> for PassThrough {}
}
