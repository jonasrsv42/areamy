//! Async poll graph traits.
//!
//! [AsyncParent] is a parent node config that can receive an output edge
//! and be constructed with an allocator. Used by child nodes to link and
//! construct their parents during [GraphBuilder::build](crate::connect::poll::graph::GraphBuilder::build).

use crate::ThreadId;
use crate::connect::poll::edge::PollEdge;
use crate::connect::poll::graph::Graph;
use crate::connect::poll::wakers::ThreadLocalWakerAllocator;
use crate::error::Error;
use crate::signal::Origin;

use alloc::rc::Rc;
use core::cell::RefCell;

/// A parent node in an async graph.
///
/// Each implementor produces one fixed `(OutType, SignalType,
/// ThreadIdType)` triple — a node's output, signal, and home thread
/// are intrinsic to its construction, never varying per call. Encoded
/// as associated types so callers can write bounds that pin only the
/// pieces they care about (e.g. `impl AsyncParent<OutType = Vec<i16>>`)
/// and so projection (`<P as AsyncParent>::OutType`) reads naturally.
///
/// Mirrors [`Pollable`](crate::Pollable)'s `type ThreadId` style.
///
/// Receives an output edge and the allocator, produces a [Graph],
/// and returns the allocator inside it. The child creates the edge and calls
/// [AsyncParent::build] during its own [GraphBuilder::build](crate::connect::poll::graph::GraphBuilder::build).
pub trait AsyncParent: Send {
    type OutType;
    type SignalType: Origin;
    type ThreadIdType: ThreadId;

    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<Self::OutType, Self::SignalType>>>,
        allocator: ThreadLocalWakerAllocator<Self::ThreadIdType>,
    ) -> Result<Graph<Self::ThreadIdType>, Error>;
}
