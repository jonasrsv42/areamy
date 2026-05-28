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

/// Boxed future returned by a biunion [`FutureRoutine`] factory
/// closure. The `'params` lifetime bounds non-`'static` captures the
/// async body holds.
pub type BoxFut<'params> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'params>>;

/// Biunion async routine wrapping a user-provided future factory.
///
/// The factory receives left input consumer, right input consumer,
/// and output producer.
struct Inputs<Left, Right> {
    left: InputQueue<Left>,
    right: InputQueue<Right>,
}

pub struct FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
    input: Inputs<Left, Right>,
    output: OutputQueue<Out>,
    factory: F,
    future: Option<BoxFut<'params>>,
}

impl<'params, Left, Right, Out, F> FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
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
    pub fn factory(f: F) -> impl FnOnce(ThreadLocalWaker) -> Self + Send + 'params
    where
        F: Send,
    {
        move |output_waker| Self::new(output_waker, f)
    }
}

impl<'params, Left, Right, Out, F> crate::Send<Left, biunion::Left>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
    fn send(&mut self, message: Left) -> Result<(), Error> {
        self.input.left.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<'params, Left, Right, Out, F> crate::Send<Right, biunion::Right>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
    fn send(&mut self, message: Right) -> Result<(), Error> {
        self.input.right.producer.push(Input::Data(message));
        Ok(())
    }
}

impl<'params, Left, Right, Out, F> crate::Next<Out> for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
    fn next(&mut self) -> Result<Option<Out>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<'params, Left, Right, Out, F> crate::Flush for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.left.producer.push(Input::Flush);
        self.input.right.producer.push(Input::Flush);
        Ok(())
    }
}

impl<'params, Left, Right, Out, F> crate::Poll for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
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

impl<'params, Left, Right, Out, F> Name for FutureRoutine<'params, Left, Right, Out, F> where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params
{
}

impl<'params, Left, Right, Out, F> BiunionRoutine<Left, Right, Out>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: Fn(InputConsumer<Left>, InputConsumer<Right>, OutputProducer<Out>) -> BoxFut<'params>
        + 'params,
{
}

#[cfg(test)]
mod tests {
    //! End-to-end lifetime check: a biunion `FutureRoutine` whose async
    //! body borrows from the surrounding scope. Two sub-futures (one
    //! per input side) run concurrently via [`Join`]; the bias is
    //! captured by reference and used by both halves.
    //!
    //! `Join` (not `Select`) so each side receives its own `Flush` —
    //! the [`FutureRoutine`]'s reset path requires every input to be
    //! drained and closed.

    use super::*;
    use crate::connect::sync::Receiver;
    use crate::poll;
    use crate::poll::Join;
    use crate::thread::ThreadBundle;
    use crate::work::Source;
    use crate::{Closeable, Message, Pushable, ThreadId, biunion as biu, make_push};

    #[derive(Debug, Clone)]
    struct BiunionFutureThread;
    impl ThreadId for BiunionFutureThread {}

    #[test]
    fn future_routine_can_borrow_from_scope() {
        let bias: usize = 5;
        let bias_ref: &usize = &bias;

        let mut thread = poll::Thread::<'_, BiunionFutureThread>::new();
        let mut node = thread
            .biunion(FutureRoutine::factory(
                move |left: InputConsumer<usize>,
                      right: InputConsumer<usize>,
                      output: OutputProducer<usize>| {
                    let left_out = output.clone();
                    let right_out = output;
                    let left_fut: poll::join::BoxFut<'_> = Box::pin(async move {
                        loop {
                            match left.recv().await? {
                                Input::Data(n) => left_out.push(n + *bias_ref),
                                Input::Flush => break,
                            }
                        }
                        Ok(())
                    });
                    let right_fut: poll::join::BoxFut<'_> = Box::pin(async move {
                        loop {
                            match right.recv().await? {
                                Input::Data(n) => right_out.push(n * *bias_ref),
                                Input::Flush => break,
                            }
                        }
                        Ok(())
                    });
                    Box::pin(async move { Join::join([left_fut, right_fut]).await })
                },
            ))
            .input::<biu::Left, poll::Sync>()
            .input::<biu::Right, poll::Sync>()
            .output::<poll::Sync>();

        let mut left_input = Source::new::<biu::Left>(&node).unwrap();
        let mut right_input = Source::new::<biu::Right>(&node).unwrap();
        let output = Receiver::new();
        make_push(&mut node, &output).unwrap();

        thread.add(node);

        let mut bundle = ThreadBundle::new();
        bundle.add(thread);

        std::thread::scope(|s| {
            let handle = bundle.start(s);

            left_input.push(Message::Data(3)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(8)); // 3 + 5

            right_input.push(Message::Data(4)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(20)); // 4 * 5

            // Close one side: the biunion poll node's drain takes the
            // fast-close path — skip flush, skip routine.poll, drain
            // buffered output, close downstream, wake the other input
            // so it returns Closed, exit. The routine future is dropped
            // mid-await (RecvFuts are cancellation-safe).
            //
            // Flushing both sides would deadlock here: the routine's
            // `flush()` impl pushes `Input::Flush` to BOTH queues from
            // a SINGLE Flush message, so pushing Flush a second time
            // queues a duplicate that reset() would later reject.
            left_input.close().unwrap();
            right_input.close().unwrap();
            assert!(handle.join().errors().is_empty());
        });

        let _ = bias;
    }
}
