//! Edge kind markers for async node builders.
//!
//! [`Sync`] and [`Async`] describe the edge type (cross-thread vs local).
//! [`Deferred`] resolves to either based on how the node is wired.
//!
//! Used as type parameters on [`Node`](crate::node::line::poll::builder::Node)
//! to select storage and control which wiring methods are available.

use crate::connect::poll::edge::AsyncEdge;
use crate::connect::poll::sync_bridge::SyncBridge;
use crate::marker::Connection;
use crate::signal::Origin;
use crate::thread::poll::spawn::NodeId;
use crate::{Closeable, Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;

/// Trait for edge kind markers. Uses GATs to resolve storage types.
pub trait EdgeKind: Connection {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId>;
    type Output<OutType, SignalType: Origin>;
}

/// Sync edge kind — cross-thread, uses [`SyncBridge`] (Mutex + Waker).
pub struct Sync;

/// Async edge kind — same-thread, uses [`AsyncEdge`] (no Mutex).
pub struct Async;

/// Zero-sized marker indicating that output is provided at link time
/// by a downstream node, not stored in the builder.
#[derive(Default)]
pub struct Linktime;

/// No-op sink. Discards all data pushed to it. Used as output for
/// sink nodes (no downstream consumers).
pub struct Null<DataType, SignalType: Origin>(
    std::marker::PhantomData<fn() -> (DataType, SignalType)>,
);

impl<DataType, SignalType: Origin> Null<DataType, SignalType> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<DataType, SignalType: Origin> Default for Null<DataType, SignalType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<DataType, SignalType: Origin> Connection for Null<DataType, SignalType> {}

impl<DataType, SignalType: Origin> crate::Pushable for Null<DataType, SignalType> {
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(
        &mut self,
        _msg: crate::message::Message<DataType, SignalType>,
    ) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

impl<DataType, SignalType: Origin> crate::Closeable for Null<DataType, SignalType> {
    fn close(&mut self) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

/// Deferred edge kind — resolves to [`Sync`] or [`Async`] based on wiring.
/// Storage matches [`Sync`] (SyncBridge + Vec outputs), ready for either path.
pub struct Deferred;

impl Connection for Sync {}
impl Connection for Async {}
impl Connection for Deferred {}

impl EdgeKind for Sync {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> =
        Arc<SyncBridge<InType, SignalType>>;
    type Output<OutType, SignalType: Origin> = Vec<
        Box<dyn Closeable<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync>,
    >;
}

impl EdgeKind for Async {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> =
        AsyncIn<InType, SignalType, ThreadIdType>;
    type Output<OutType, SignalType: Origin> = Linktime;
}

impl EdgeKind for Deferred {
    type Input<InType, SignalType: Origin, ThreadIdType: ThreadId> = Null<InType, SignalType>;
    type Output<OutType, SignalType: Origin> = Null<OutType, SignalType>;
}

/// Bundles parent builders + waker for async input nodes.
pub struct AsyncIn<InType, SignalType: Origin, ThreadIdType: ThreadId> {
    pub parents: Vec<
        Box<
            dyn Linkable<
                    crate::marker::Parent,
                    Edge = Rc<RefCell<AsyncEdge<InType, SignalType>>>,
                    Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
                >,
        >,
    >,
    pub waker: Waker,
}
