//! [FutureRoutine] — wraps a user-provided async fn into an
//! [AsyncLineRoutine](crate::AsyncLineRoutine).
//!
//! The user's async fn receives an input [Queue] and output [Queue].
//! Data arrives as [Input::Data], flush as [Input::Flush].
//! The future drives I/O, reads from input, writes to output.
//!
//! Created via [RoutineFactory](crate::RoutineFactory) — the routine
//! is born on the async thread and can hold non-Send types like
//! `Rc<RefCell<_>>`.
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
//! use areamy::poll::future::{FutureRoutine, Input, Queue};
//! use areamy::poll::Join;
//!
//! let routine = || FutureRoutine::new(|input, output| {
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
//! let node = thread.node(routine).typed::<Sync>();
//! ```

use super::queue::{Input, Queue};
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
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    input: Queue<Input<InType>>,
    output: Queue<OutType>,
    factory: F,
    future: Option<BoxFut>,
}

impl<InType, OutType, F> FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    pub fn new(factory: F) -> Self {
        let input = Queue::new();
        let output = Queue::new();
        let future = (factory)(input.clone(), output.clone());
        Self {
            input,
            output,
            factory,
            future: Some(future),
        }
    }
}

impl<InType, OutType, F> crate::Send<InType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    fn send(&mut self, message: InType) -> Result<(), Error> {
        self.input.push(Input::Data(message));
        Ok(())
    }
}

impl<InType, OutType, F> crate::Next<OutType> for FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        Ok(self.output.pop())
    }
}

impl<InType, OutType, F> crate::Flush for FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.push(Input::Flush);
        Ok(())
    }
}

impl<InType, OutType, F> crate::Poll for FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
    fn poll(&mut self, cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        let future = self
            .future
            .get_or_insert_with(|| (self.factory)(self.input.clone(), self.output.clone()));

        match future.as_mut().poll(cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                self.input.reset()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<InType, OutType, F> Name for FutureRoutine<InType, OutType, F> where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut
{
}

impl<InType, OutType, F> crate::AsyncLineRoutine<InType, OutType>
    for FutureRoutine<InType, OutType, F>
where
    F: Fn(Queue<Input<InType>>, Queue<OutType>) -> BoxFut,
{
}
