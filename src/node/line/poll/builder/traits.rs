//! Dispatch traits for line builder `.input::<E>()` and `.output::<E>()`.

use crate::ThreadId;
use crate::connect::poll::edge::{Edge, Sync, SyncBridge, SyncInput};
use crate::connect::poll::wakers::WakerAllocator;
use crate::signal::Origin;
use std::sync::Arc;

/// Resolve input edge. Implemented for [`Sync`] — allocates a sync waker.
pub trait ResolveInput<InType, SignalType: Origin, ThreadIdType: ThreadId>: Edge {
    fn resolve(
        alloc: &mut WakerAllocator,
    ) -> (
        Self::Input<InType, SignalType, ThreadIdType>,
        Self::Alloc<'static>,
    );
}

impl<InType, SignalType, ThreadIdType> ResolveInput<InType, SignalType, ThreadIdType> for Sync
where
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn resolve(alloc: &mut WakerAllocator) -> (SyncInput<InType, SignalType>, ()) {
        let slot = alloc.next();
        let waker = slot.value.clone();
        (
            SyncInput {
                edge: Arc::new(SyncBridge::new(waker)),
                slot,
            },
            (),
        )
    }
}

/// Resolve output edge. Implemented for [`Sync`].
pub trait ResolveOutput<OutType, SignalType: Origin>: Edge {
    fn resolve() -> Self::Output<OutType, SignalType>;
}

impl<OutType, SignalType> ResolveOutput<OutType, SignalType> for Sync
where
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
{
    fn resolve() -> Vec<
        Box<
            dyn crate::Closeable<DataType = OutType, SignalType = SignalType>
                + Send
                + std::marker::Sync,
        >,
    > {
        Vec::new()
    }
}
