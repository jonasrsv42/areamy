//! [FutureRoutine] for biunion — wraps a user-provided async fn into a
//! [BiunionRoutine].
//!
//! The async fn receives two [input::Receiver]s (left + right) and one
//! [output::Sender]. Use [race](fn@crate::poll::race) to await either
//! input, or [try_join](fn@crate::poll::try_join) to serve both.
//!
//! A Flush on either side flushes both inputs: each gets an
//! [Input::Flush](crate::poll::future::Input::Flush), and the segment may end only once the
//! routine has read both (anything left unread is a fatal error). With `race`, after one side's
//! Flush, drain the other side up to its Flush before returning; don't `recv` again on a side
//! that read its Flush (it is closed).

use crate::error::Error;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::{BiunionRoutineFactory, BiunionWakers};
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::{biunion, routine};
use crate::poll::Context;
use crate::poll::future::{input, output};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;

/// Boxed future returned by a biunion [`FutureRoutine`] factory
/// closure. The `'params` lifetime bounds non-`'static` captures the
/// async body holds.
pub type BoxFut<'params> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'params>>;

/// Biunion async routine wrapping a user-provided future factory.
///
/// The factory receives the left input, the right input, and the output.
pub struct FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    input: Inputs<input::Sender<Left>, input::Sender<Right>>,
    /// The segment's inputs, handed to its future.
    receiver: Inputs<input::Receiver<Left>, input::Receiver<Right>>,
    output: output::Receiver<Out>,
    sender: output::Sender<Out>,
    factory: F,
    future: Option<BoxFut<'params>>,
}

impl<'params, Left, Right, Out, F> FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    /// `bound` caps each input queue and the output queue; `None` is unbounded.
    pub fn new(wakers: BiunionWakers, bound: Option<NonZeroUsize>, factory: F) -> Self {
        // `work` is the waker the routine is polled with: the queues' home. Every queue gets the
        // timer so a `*_with_timeout` deadline re-polls the routine.
        let (left, left_receiver) = input::channel(
            wakers.input.left,
            wakers.work.clone(),
            wakers.timer.clone(),
            bound,
        );
        let (right, right_receiver) = input::channel(
            wakers.input.right,
            wakers.work.clone(),
            wakers.timer.clone(),
            bound,
        );
        let (sender, output) = output::channel(wakers.output, wakers.work, wakers.timer, bound);
        Self {
            input: Inputs { left, right },
            receiver: Inputs {
                left: left_receiver,
                right: right_receiver,
            },
            output,
            sender,
            factory,
            future: None,
        }
    }

    /// Create a [BiunionRoutineFactory] for `thread.biunion()`, unbounded until
    /// [Factory::bounded]. Users don't need to handle wakers.
    pub fn factory(factory: F) -> Factory<Left, Right, Out, F> {
        Factory {
            factory,
            bound: None,
            types: PhantomData,
        }
    }
}

/// [BiunionRoutineFactory] of a [FutureRoutine]; see [FutureRoutine::factory].
pub struct Factory<Left, Right, Out, F> {
    factory: F,
    bound: Option<NonZeroUsize>,
    /// `fn` keeps the factory `Send` whatever the queue types.
    types: PhantomData<fn(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>)>,
}

impl<Left, Right, Out, F> Factory<Left, Right, Out, F> {
    /// Caps each input queue and the output queue at `bound`; `None` is unbounded. The routine's
    /// queues, not the node's input edges (those are the node builder's `.bounded`). Close drops
    /// what the queues hold; Flush before closing to deliver it.
    pub fn bounded(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        self.bound = bound.into();
        self
    }
}

impl<'params, Left, Right, Out, F> BiunionRoutineFactory<'params> for Factory<Left, Right, Out, F>
where
    Left: 'params,
    Right: 'params,
    Out: 'params,
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + Send
        + 'params,
{
    type Routine = FutureRoutine<'params, Left, Right, Out, F>;

    fn create(self, wakers: BiunionWakers) -> Self::Routine {
        FutureRoutine::new(wakers, self.bound, self.factory)
    }
}

impl<'params, Left, Right, Out, F> routine::TrySend<Left, biunion::Left>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    fn try_send(&mut self, message: Left) -> Result<routine::Accept<Left>, Error> {
        self.input.left.try_send(message)
    }
}

impl<'params, Left, Right, Out, F> routine::TrySend<Right, biunion::Right>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    fn try_send(&mut self, message: Right) -> Result<routine::Accept<Right>, Error> {
        self.input.right.try_send(message)
    }
}

impl<'params, Left, Right, Out, F> routine::Next<Out>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    fn next(&mut self) -> Result<Option<Out>, Error> {
        self.output.pop()
    }
}

impl<'params, Left, Right, Out, F> routine::Flush for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.left.flush()?;
        self.input.right.flush()
    }
}

impl<'params, Left, Right, Out, F> routine::Poll for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let future = self.future.get_or_insert_with(|| {
            (self.factory)(
                self.receiver.left.clone(),
                self.receiver.right.clone(),
                self.sender.clone(),
            )
        });

        let mut std_cx = core::task::Context::from_waker(&cx.wake.sync);
        match future.as_mut().poll(&mut std_cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                // Before Input next sends: it resumes as soon as the node is back to Running.
                self.receiver.left = self.input.left.renew()?;
                self.receiver.right = self.input.right.renew()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<'params, Left, Right, Out, F> BiunionRoutine<Left, Right, Out>
    for FutureRoutine<'params, Left, Right, Out, F>
where
    F: FnMut(input::Receiver<Left>, input::Receiver<Right>, output::Sender<Out>) -> BoxFut<'params>
        + 'params,
{
}

#[cfg(test)]
mod tests {
    //! End-to-end checks of the biunion `FutureRoutine`: a body borrowing from the surrounding
    //! scope, and the `race` pattern the module doc names.

    use super::*;
    use crate::edge::sync::Receiver;
    use crate::poll;
    use crate::poll::future::Input;
    use crate::poll::{Either, race, try_join};
    use crate::thread::ThreadBundle;
    use crate::{At, Closeable, Message, Pushable, Pushes, Trackable, Writes, biunion as biu};

    crate::thread_id!(BiunionFutureThread);

    #[test]
    fn future_routine_can_borrow_from_scope() {
        let bias: usize = 5;
        let bias_ref: &usize = &bias;

        let mut thread = poll::Thread::<'_, BiunionFutureThread>::new();
        let mut node = thread
            .biunion(FutureRoutine::factory(
                move |left: input::Receiver<usize>,
                      right: input::Receiver<usize>,
                      output: output::Sender<usize>| {
                    let left_out = output.clone();
                    let right_out = output;
                    let left_fut = async move {
                        while let Input::Data(n) = left.recv().await? {
                            left_out.send(n + *bias_ref).await?;
                        }
                        Ok::<_, Error>(())
                    };
                    let right_fut = async move {
                        while let Input::Data(n) = right.recv().await? {
                            right_out.send(n * *bias_ref).await?;
                        }
                        Ok::<_, Error>(())
                    };
                    Box::pin(async move {
                        try_join(left_fut, right_fut).await?;
                        Ok(())
                    })
                },
            ))
            .input::<biu::Left, poll::Sync>()
            .input::<biu::Right, poll::Sync>()
            .output::<poll::Sync>();

        let mut left_input = node.at_ref::<biu::Left>().writer().unwrap();
        let mut right_input = node.at_ref::<biu::Right>().writer().unwrap();
        let output = Receiver::new();
        node.pushes(&output).unwrap();

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
            // mid-await (`recv` is cancellation-safe).
            //
            // Not a Flush per side: one Flush message flushes BOTH queues,
            // so a second would only end a second, empty segment.
            left_input.close().unwrap();
            right_input.close().unwrap();
            assert!(handle.join().errors().is_empty());
        });

        let _ = bias;
    }

    /// A routine racing both inputs: after one side's Flush it drains the other to its Flush,
    /// as the contract asks, so each segment ends clean and the next starts fresh.
    #[test]
    fn a_racing_routine_drains_the_other_side_to_its_flush() {
        let mut thread = poll::Thread::<'_, BiunionFutureThread>::new();
        let mut node = thread
            .biunion(FutureRoutine::factory(
                |left: input::Receiver<usize>,
                 right: input::Receiver<usize>,
                 output: output::Sender<usize>| {
                    Box::pin(async move {
                        let other = loop {
                            match race(left.recv(), right.recv()).await {
                                Either::Left(item) => match item? {
                                    Input::Data(value) => output.send(value).await?,
                                    Input::Flush => break &right,
                                },
                                Either::Right(item) => match item? {
                                    Input::Data(value) => output.send(value).await?,
                                    Input::Flush => break &left,
                                },
                            }
                        };
                        while let Input::Data(value) = other.recv().await? {
                            output.send(value).await?;
                        }
                        Ok(())
                    })
                },
            ))
            .input::<biu::Left, poll::Sync>()
            .input::<biu::Right, poll::Sync>()
            .output::<poll::Sync>();

        let mut left_input = node.at_ref::<biu::Left>().writer().unwrap();
        let mut right_input = node.at_ref::<biu::Right>().writer().unwrap();
        let output = Receiver::new();
        node.pushes(&output).unwrap();
        thread.add(node);
        let mut bundle = ThreadBundle::new();
        bundle.add(thread);

        std::thread::scope(|s| {
            let handle = bundle.start(s);
            for segment in 0..2 {
                left_input.push(Message::Data(segment)).unwrap();
                left_input
                    .push(Message::Flush(Trackable::new("f")))
                    .unwrap();
                assert_eq!(output.read_front().unwrap(), Message::Data(segment));
                assert_eq!(
                    output.read_front().unwrap(),
                    Message::Flush(Trackable::new("f"))
                );
            }
            left_input.close().unwrap();
            right_input.close().unwrap();
            assert!(handle.join().errors().is_empty());
        });
    }
}
