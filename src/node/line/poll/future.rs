//! [FutureRoutine] — wraps a user-provided async fn into an
//! [LineRoutine].
//!
//! The user's async fn receives an [input::Receiver] and an [output::Sender].
//! Data arrives as [Input::Data](crate::poll::future::Input::Data), flush as
//! [Input::Flush](crate::poll::future::Input::Flush).
//! The future drives I/O, reads from input, writes to output.
//!
//! Created via [LineRoutineFactory]
//! — the routine is born on the async thread and can hold non-Send types.
//!
//! # Lifecycle
//!
//! When the future completes (returns `Ok(())`), the routine returns
//! [core::task::Poll::Ready]. The node forwards the pending Flush signal
//! (or closes on Closing). On the next poll, the factory recreates the
//! future for the next segment, with a fresh input queue.
//!
//! # Example
//!
//! ```ignore
//! use areamy::poll::future::{line::FutureRoutine, Input};
//! use areamy::poll::try_join;
//!
//! let routine = FutureRoutine::factory(|input, output| {
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
//!                 output.send(val).await?;
//!             }
//!             Ok(())
//!         };
//!
//!         try_join(writer, reader).await?;
//!         Ok(())
//!     })
//! })
//! .bounded(NonZeroUsize::new(8));
//!
//! let node = thread.line(routine).input::<Sync>().output::<Sync>();
//! ```

use crate::error::Error;
use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
use crate::node::line::poll::routine::LineRoutine;
use crate::node::routine;
use crate::poll::Context;
use crate::poll::future::{input, output};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;

pub mod once;

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
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    input: input::Sender<InType>,
    /// The segment's input, handed to its future.
    receiver: input::Receiver<InType>,
    output: output::Receiver<OutType>,
    sender: output::Sender<OutType>,
    factory: F,
    future: Option<BoxFut<'params>>,
}

impl<'params, InType, OutType, F> FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    /// `bound` caps the input and the output queue each; `None` is unbounded.
    pub fn new(wakers: LineWakers, bound: Option<NonZeroUsize>, factory: F) -> Self {
        // `work` is the waker the routine is polled with: the queues' home. The timer re-polls
        // the routine at a `*_with_timeout` deadline.
        let (input, receiver) = input::channel(
            wakers.input,
            wakers.work.clone(),
            wakers.timer.clone(),
            bound,
        );
        let (sender, output) = output::channel(wakers.output, wakers.work, wakers.timer, bound);
        Self {
            input,
            receiver,
            output,
            sender,
            factory,
            future: None,
        }
    }

    /// Create a [LineRoutineFactory] from an async closure, unbounded until
    /// [Factory::bounded]. The closure receives the input/output queues —
    /// wakers are wired automatically.
    ///
    /// ```ignore
    /// let node = thread.line(FutureRoutine::factory(|input, output| {
    ///     Box::pin(async move { /* ... */ })
    /// })).input::<Sync>().output::<Sync>();
    /// ```
    pub fn factory(factory: F) -> Factory<InType, OutType, F> {
        Factory {
            factory,
            bound: None,
            types: PhantomData,
        }
    }
}

/// [LineRoutineFactory] of a [FutureRoutine]; see [FutureRoutine::factory].
pub struct Factory<InType, OutType, F> {
    factory: F,
    bound: Option<NonZeroUsize>,
    /// `fn` keeps the factory `Send` whatever the queue types.
    types: PhantomData<fn(input::Receiver<InType>, output::Sender<OutType>)>,
}

impl<InType, OutType, F> Factory<InType, OutType, F> {
    /// Caps the input and the output queue at `bound` each; `None` is unbounded. The routine's
    /// queues, not the node's input edges (those are the node builder's `.bounded`). Close drops
    /// what the queues hold; Flush before closing to deliver it.
    pub fn bounded(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        self.bound = bound.into();
        self
    }
}

impl<'params, InType, OutType, F> LineRoutineFactory<'params> for Factory<InType, OutType, F>
where
    InType: 'params,
    OutType: 'params,
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + Send + 'params,
{
    type Routine = FutureRoutine<'params, InType, OutType, F>;

    fn create(self, wakers: LineWakers) -> Self::Routine {
        FutureRoutine::new(wakers, self.bound, self.factory)
    }
}

impl<'params, InType, OutType, F> routine::TrySend<InType>
    for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    fn try_send(&mut self, message: InType) -> Result<routine::Accept<InType>, Error> {
        self.input.try_send(message)
    }
}

impl<'params, InType, OutType, F> routine::Next<OutType>
    for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    fn next(&mut self) -> Result<Option<OutType>, Error> {
        self.output.pop()
    }
}

impl<'params, InType, OutType, F> routine::Flush for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.flush()
    }
}

impl<'params, InType, OutType, F> routine::Poll for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let future = self
            .future
            .get_or_insert_with(|| (self.factory)(self.receiver.clone(), self.sender.clone()));

        let mut std_cx = core::task::Context::from_waker(&cx.wake.sync);
        match future.as_mut().poll(&mut std_cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                // Before Input next sends: it resumes as soon as the node is back to Running.
                self.receiver = self.input.renew()?;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<'params, InType, OutType, F> LineRoutine<InType, OutType>
    for FutureRoutine<'params, InType, OutType, F>
where
    F: FnMut(input::Receiver<InType>, output::Sender<OutType>) -> BoxFut<'params> + 'params,
{
}

#[cfg(test)]
mod tests {
    //! End-to-end lifetime check: a `FutureRoutine` whose async body
    //! borrows from the surrounding scope. Without `'params` on
    //! [`BoxFut`] the closure would be locked to `'static` and the
    //! `&multiplier` capture wouldn't compile.

    use super::*;
    use crate::edge::sync::Receiver;
    use crate::poll;
    use crate::poll::future::Input;
    use crate::thread::ThreadBundle;
    use crate::work;
    use crate::{Closeable, Message, Pushable, Pushes, Streams, Writes};

    crate::thread_id!(LineFutureThread);

    #[test]
    fn future_routine_can_borrow_from_scope() {
        let multiplier: usize = 7;
        let mult_ref: &usize = &multiplier;

        let mut thread = poll::Thread::<'_, LineFutureThread>::new();
        let mut node = thread
            .line(FutureRoutine::factory(
                move |input: input::Receiver<usize>, output: output::Sender<usize>| {
                    Box::pin(async move {
                        while let Input::Data(n) = input.recv().await? {
                            output.send(n * *mult_ref).await?;
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
        let mut driver = work::Line::of(PassThrough::new());
        let mut input = driver.writer().unwrap();
        driver.pushes(&node).unwrap();

        let output = Receiver::new();
        node.pushes(&output).unwrap();

        thread.add(node);
        let work_thread = driver.stream::<LineFutureThread>();

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

    impl crate::node::line::LineRoutine<usize, usize> for PassThrough {}
}
