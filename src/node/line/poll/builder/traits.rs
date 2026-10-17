//! Bounds for line builder `.input()` and `.output()`.

use crate::edge::fanout::Fanout;
use crate::poll::DynSink;
use crate::poll::edge::{Edge, Sync};
use crate::poll::input::sync::{Input, Receiver};
use crate::poll::wakers::Slot;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::num::NonZeroUsize;
use std::task::Waker;

/// Resolve input edge. Implemented for [`Sync`] — wakes the reserved `slot`. `bound` caps the edge.
pub trait ResolveInput<'params, InType, SignalType: Origin, ThreadIdType: ThreadId>: Edge {
    fn resolve(
        slot: Slot<Waker>,
        bound: Option<NonZeroUsize>,
    ) -> (
        Self::Input<'params, InType, SignalType, ThreadIdType>,
        Self::Alloc,
    );
}

impl<'params, InType, SignalType, ThreadIdType>
    ResolveInput<'params, InType, SignalType, ThreadIdType> for Sync
where
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn resolve(slot: Slot<Waker>, bound: Option<NonZeroUsize>) -> (Input<InType, SignalType>, ()) {
        let waker = slot.value.clone();
        (
            Input {
                edge: Receiver::new(waker, bound),
                slot,
            },
            (),
        )
    }
}

/// Resolve output edge. Implemented for [`Sync`], which pushes into every edge `pushes` adds, so
/// its data must be `Clone`. Data that isn't leaves a node by one edge: `.sink(..)`, or
/// `node.via(consumer)`.
///
/// ```compile_fail,E0277
/// use areamy::poll::future::{Input, input, line::FutureRoutine, output};
/// use areamy::{DefaultThread, Pushes, Trackable, poll, sync::Receiver};
///
/// struct NotClone;
///
/// let mut thread = poll::Thread::<DefaultThread>::new();
/// let mut node = thread
///     .line(FutureRoutine::factory(
///         |input: input::Receiver<usize>, output: output::Sender<NotClone>| {
///             Box::pin(async move {
///                 while let Input::Data(_) = input.recv().await? {
///                     output.send(NotClone).await?;
///                 }
///                 Ok(())
///             })
///         },
///     ))
///     .input()
///     .output();
/// let downstream = Receiver::<NotClone, Trackable<&'static str>>::new();
/// node.pushes(&downstream).unwrap();
/// ```
pub trait ResolveOutput<'params, OutType, SignalType: Origin>: Edge {
    fn resolve() -> Self::Output<'params, OutType, SignalType>;
}

impl<'params, OutType, SignalType> ResolveOutput<'params, OutType, SignalType> for Sync
where
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
{
    fn resolve() -> Fanout<DynSink<'params, OutType, SignalType>> {
        Fanout::new()
    }
}
