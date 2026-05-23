//! Input bundle for an async poll node whose parents are other
//! async nodes on the same thread.
//!
//! [`Input`] holds the [`AsyncParent`] configs for the node. Parents
//! are built into [`PollEdge`](crate::connect::poll::edge::PollEdge)s
//! during [`GraphBuilder::build`](crate::connect::poll::graph::GraphBuilder::build);
//! data flows at runtime through those edges, not through this struct.

use crate::ThreadId;
use crate::connect::poll::traits::AsyncParent;
use crate::signal::Origin;

/// Bundles parent [`AsyncParent`] configs for an async-input poll node.
pub struct Input<InType, SignalType: Origin, ThreadIdType: ThreadId> {
    pub parents: Vec<
        Box<
            dyn AsyncParent<OutType = InType, SignalType = SignalType, ThreadIdType = ThreadIdType>,
        >,
    >,
}
