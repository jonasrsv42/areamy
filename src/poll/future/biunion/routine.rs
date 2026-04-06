//! [FutureRoutine] for biunion — wraps a user-provided async fn into a
//! [BiunionRoutine](crate::node::biunion::poll::routine::BiunionRoutine).
//!
//! The async fn receives two [InputConsumer]s (left + right) and one
//! [OutputProducer]. Use [Select](crate::poll::Select) to await either input.

use crate::biunion;
use crate::connect::waker::{self, ThreadLocalWaker};
use crate::error::Error;
use crate::node::Name;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::future::queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue};
use std::future::Future;
use std::pin::Pin;

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// Biunion async routine wrapping a user-provided future factory.
///
/// The factory receives left input consumer, right input consumer,
/// and output producer.
struct Inputs<Left, Right> {
    left: InputQueue<Left>,
    right: InputQueue<Right>,
}

pub struct FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    input: Inputs<Left, Right>,
    output: OutputQueue<Out>,
    factory: F,
    future: Option<BoxFut>,
}

impl<Left, Right, Out, F> FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    pub fn new(output_waker: ThreadLocalWaker, factory: F) -> Self {
        let input = Inputs {
            left: InputQueue::new(),
            right: InputQueue::new(),
        };
        let output = OutputQueue::new(output_waker);
        let future = (factory)(
            input.left.consumer.clone(),
            input.right.consumer.clone(),
            output.producer.clone(),
        );
        Self {
            input,
            output,
            factory,
            future: Some(future),
        }
    }

    /// Create a factory closure for use with `thread.biunion()`.
    /// Users don't need to handle the output waker.
    pub fn factory(f: F) -> impl FnOnce(ThreadLocalWaker) -> Self + Send
    where
        F: Send,
    {
        move |output_waker| Self::new(output_waker, f)
    }
}

impl<Left, Right, Out, F> crate::Send<Left, biunion::Left> for FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    fn send(&mut self, message: Left) -> Result<(), Error> {
        self.input.left.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<Left, Right, Out, F> crate::Send<Right, biunion::Right> for FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    fn send(&mut self, message: Right) -> Result<(), Error> {
        self.input.right.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<Left, Right, Out, F> crate::Next<Out> for FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    fn next(&mut self) -> Result<Option<Out>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<Left, Right, Out, F> crate::Flush for FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.left.producer.push(Input::Flush);
        self.input.right.producer.push(Input::Flush);
        Ok(())
    }
}

impl<Left, Right, Out, F> crate::Poll for FutureRoutine<Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut,
{
    fn poll(&mut self, waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        let future = self.future.get_or_insert_with(|| {
            (self.factory)(
                self.input.left.consumer.clone(),
                self.input.right.consumer.clone(),
                self.output.producer.clone(),
            )
        });

        let mut cx = core::task::Context::from_waker(&waker.sync);
        match future.as_mut().poll(&mut cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                self.input.left.producer.reset()?;
                self.input.right.producer.reset()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<Left, Right, Out, F> Name for FutureRoutine<Left, Right, Out, F> where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut
{
}

impl<Left, Right, Out, F> BiunionRoutine<Left, Right, Out> for FutureRoutine<Left, Right, Out, F> where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut
{
}
