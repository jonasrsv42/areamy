//! [FutureRoutine] — wraps a user-provided async fn into an
//! [AsyncLineRoutine](crate::AsyncLineRoutine).
//!
//! The user's async fn receives an [InputConsumer] and [OutputProducer].
//! Data arrives as [Input::Data], flush as [Input::Flush].
//! The future drives I/O, reads from input, writes to output.
//!
//! Created via [PollLineRoutineFactory](crate::node::line::poll::factory::PollLineRoutineFactory)
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
//! let node = thread.line(routine).typed::<Sync>();
//! ```

use super::queue::{
    Input, InputConsumer, InputProducer, OutputConsumer, OutputProducer, input_queue, output_queue,
};
use crate::connect::waker::ThreadLocalWaker;
use crate::error::Error;
use crate::node::Name;
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
    input_producer: InputProducer<InType>,
    input_consumer: InputConsumer<InType>,
    output_producer: OutputProducer<OutType>,
    output_consumer: OutputConsumer<OutType>,
    factory: F,
    future: Option<BoxFut>,
}

impl<InType, OutType, F> FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    pub fn new(output_waker: ThreadLocalWaker, factory: F) -> Self {
        let (input_producer, input_consumer) = input_queue();
        let (output_producer, output_consumer) = output_queue(output_waker.clone());
        let future = (factory)(input_consumer.clone(), output_producer.clone());
        Self {
            input_producer,
            input_consumer,
            output_producer,
            output_consumer,
            factory,
            future: Some(future),
        }
    }
}

impl<InType, OutType, F> crate::Send<InType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn send(&mut self, message: InType) -> Result<(), Error> {
        self.input_producer.push(Input::Data(message));
        Ok(())
    }
}

impl<InType, OutType, F> crate::Next<OutType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        Ok(self.output_consumer.pop())
    }
}

impl<InType, OutType, F> crate::Flush for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input_producer.push(Input::Flush);
        Ok(())
    }
}

impl<InType, OutType, F> crate::Poll for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
    fn poll(&mut self, cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        let future = self.future.get_or_insert_with(|| {
            (self.factory)(self.input_consumer.clone(), self.output_producer.clone())
        });

        match future.as_mut().poll(cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                self.input_producer.reset()?;
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

impl<InType, OutType, F> crate::AsyncLineRoutine<InType, OutType>
    for FutureRoutine<InType, OutType, F>
where
    F: Fn(InputConsumer<InType>, OutputProducer<OutType>) -> BoxFut,
{
}
