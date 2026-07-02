//! Dispatch traits for line builder `.input::<E>()` and `.output::<E>()`.

use crate::ThreadId;
use crate::connect::poll::edge::{Edge, Sync};
use crate::connect::poll::input::sync::{Input, Receiver};
use crate::connect::poll::wakers::WakerAllocator;
use crate::signal::Origin;

/// Resolve input edge. Implemented for [`Sync`] — allocates a sync waker.
pub trait ResolveInput<'params, InType, SignalType: Origin, ThreadIdType: ThreadId>: Edge {
    fn resolve(
        alloc: &mut WakerAllocator,
    ) -> (
        Self::Input<'params, InType, SignalType, ThreadIdType>,
        Self::Alloc<'static>,
    );
}

impl<'params, InType, SignalType, ThreadIdType>
    ResolveInput<'params, InType, SignalType, ThreadIdType> for Sync
where
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn resolve(alloc: &mut WakerAllocator) -> (Input<InType, SignalType>, ()) {
        let slot = alloc.next();
        let waker = slot.value.clone();
        (
            Input {
                edge: Receiver::new(waker),
                slot,
            },
            (),
        )
    }
}

/// Resolve output edge. Implemented for [`Sync`].
pub trait ResolveOutput<'params, OutType, SignalType: Origin>: Edge {
    fn resolve() -> Self::Output<'params, OutType, SignalType>;
}

impl<'params, OutType, SignalType> ResolveOutput<'params, OutType, SignalType> for Sync
where
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
{
    fn resolve() -> Vec<
        Box<
            dyn crate::Sink<DataType = OutType, SignalType = SignalType>
                + Send
                + std::marker::Sync
                + 'params,
        >,
    > {
        Vec::new()
    }
}
