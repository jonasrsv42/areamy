//! Edge kind markers for async node builders.
//!
//! [`Sync`] and [`Async`] describe the edge type (cross-thread vs local).
//! [`Deferred`] resolves to either based on how the node is wired.
//!
//! Used as type parameters on [`Node`](crate::node::line::poll::builder::node::Node)
//! to select storage and control which wiring methods are available.

use super::async_in::AsyncIn;
use super::null::Null;
use crate::connect::poll::wakers::WakerAllocator;
use crate::marker::Connection;
use crate::signal::Origin;
use crate::{Closeable, ThreadId};

/// Trait for edge markers. Uses GATs to resolve storage types.
pub trait Edge: Connection {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId>;
    type Output<OutType, SignalType: Origin>;
    /// Allocator state for this edge kind.
    /// `Deferred` holds `&'a mut WakerAllocator`, resolved edges hold
    /// their allocated result or `()`.
    type Alloc<'a>;
}

/// Sync edge — cross-thread, uses [`SyncBridge`] (Mutex + Waker).
pub struct Sync;

/// Async edge — same-thread, uses [`PollEdge`](super::poll::PollEdge) (no Mutex).
pub struct Async;

/// Zero-sized marker indicating that output is provided at build time
/// by a downstream node, not stored in the builder.
#[derive(Default)]
pub struct Linktime;

/// Deferred edge — resolves to [`Sync`] or [`Async`] based on wiring.
pub struct Deferred;

impl Connection for Sync {}
impl Connection for Async {}
impl Connection for Deferred {}

impl Edge for Sync {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> =
        super::sync::SyncInput<InType, SignalType>;
    type Output<OutType, SignalType: Origin> = Vec<
        Box<dyn Closeable<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync>,
    >;
    type Alloc<'a> = ();
}

impl Edge for Async {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> =
        AsyncIn<InType, SignalType, ThreadIdType>;
    type Output<OutType, SignalType: Origin> = Linktime;
    type Alloc<'a> = ();
}

impl Edge for Deferred {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> = Null<InType, SignalType>;
    type Output<OutType, SignalType: Origin> = Null<OutType, SignalType>;
    type Alloc<'a> = &'a mut WakerAllocator;
}
