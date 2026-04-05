//! [PollGraphBuilder] — one-shot builder for async poll graph nodes.
//!
//! A [PollGraphBuilder] is a blueprint that crosses from the main thread to
//! the async thread. [PollGraphBuilder::build] is called once on the async
//! thread with a [ThreadLocalWakerAllocator], producing a [Graph].

use crate::connect::poll::marker::NodeId;
use crate::connect::poll::wakers::ThreadLocalWakerAllocator;
use crate::error::Error;
use crate::{Pollable, ThreadId};

use alloc::vec::Vec;

/// A node ID paired with its pollable, ready for runtime registration.
pub struct PollGraphNode<ThreadIdType: ThreadId> {
    pub id: NodeId,
    pub pollable: Box<dyn Pollable<ThreadId = ThreadIdType>>,
}

/// The result of [PollGraphBuilder::build] — the allocator and produced nodes.
pub struct Graph<ThreadIdType: ThreadId> {
    pub allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    pub nodes: Vec<PollGraphNode<ThreadIdType>>,
}

/// One-shot builder for async poll graph nodes.
///
/// Constructed on any thread ([Send]), then [PollGraphBuilder::build] is called
/// once on the async thread with the allocator. Produces a [Graph].
pub trait PollGraphBuilder<ThreadIdType: ThreadId>: Send {
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error>;
}
