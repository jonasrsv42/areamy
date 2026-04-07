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
//! use areamy::poll::Join;
//!
//! let routine = |output_waker| FutureRoutine::new(output_waker, |input, output| {
//!     Box::pin(async move {
//!         let socket = connect("server").await;
//!
//!         let writer = Box::pin(async {
//!             loop {
//!                 match input.recv().await? {
//!                     Input::Data(val) => socket.write(val).await,
//!                     Input::Flush => { socket.half_close().await; break; }
//!                 }
//!             }
//!             Ok(())
//!         });
//!
//!         let reader = Box::pin(async {
//!             while let Some(val) = socket.read().await {
//!                 output.push(val);
//!             }
//!             Ok(())
//!         });
//!
//!         Join::pair(writer, reader).await
//!     })
//! });
//!
//! let node = thread.line(routine).input::<Sync>().output::<Sync>();
//! ```

use crate::connect::waker::{self, ThreadLocalWaker};
use crate::error::Error;
use crate::node::Name;
use crate::node::line::poll::routine::LineRoutine;
use crate::poll::future::queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue};
use std::future::Future;
use std::pin::Pin;

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// Async routine wrapping a user-provided future factory.
///
/// Generic over `InType`, `OutType`, and the factory `F`.
pub struct FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    input: InputQueue<InType>,
    output: OutputQueue<OutType>,
    factory: F,
    future: Option<BoxFut>,
}

impl<InType, OutType, F> FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    pub fn new(output_waker: ThreadLocalWaker, factory: F) -> Self {
        let input = InputQueue::new();
        let output = OutputQueue::new(output_waker);
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
    /// the output waker is wired automatically.
    ///
    /// ```ignore
    /// let node = thread.line(FutureRoutine::factory(|input, output| {
    ///     Box::pin(async move { /* ... */ })
    /// })).input::<Sync>().output::<Sync>();
    /// ```
    pub fn factory(f: F) -> impl FnOnce(ThreadLocalWaker) -> Self + Send
    where
        F: Send,
    {
        move |output_waker| Self::new(output_waker, f)
    }
}

impl<InType, OutType, F> crate::Send<InType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn send(&mut self, message: InType) -> Result<(), Error> {
        self.input.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<InType, OutType, F> crate::Next<OutType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<InType, OutType, F> crate::Flush for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.producer.push(Input::Flush);
        Ok(())
    }
}

impl<InType, OutType, F> crate::Poll for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
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

impl<InType, OutType, F> Name for FutureRoutine<InType, OutType, F> where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut
{
}

impl<InType, OutType, F> LineRoutine<InType, OutType> for FutureRoutine<InType, OutType, F> where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut
{
}
