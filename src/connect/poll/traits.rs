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
/// Receives an output edge and the allocator, produces a [Graph],
/// and returns the allocator inside it. The child creates the edge and calls
/// [AsyncParent::build] during its own [GraphBuilder::build](crate::connect::poll::graph::GraphBuilder::build).
pub trait AsyncParent<OutType, SignalType: Origin, ThreadIdType: ThreadId>: Send {
    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error>;
}
