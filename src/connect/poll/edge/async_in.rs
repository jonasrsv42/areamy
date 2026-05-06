//! Bundles parent [AsyncParent](crate::connect::poll::traits::AsyncParent) configs
//! for async input nodes.

use crate::ThreadId;
use crate::connect::poll::traits::AsyncParent;
use crate::signal::Origin;

/// Bundles parent [AsyncParent] configs for async input nodes.
/// Parents are built during [GraphBuilder::build](crate::connect::poll::graph::GraphBuilder::build).
pub struct AsyncIn<InType, SignalType: Origin, ThreadIdType: ThreadId> {
    pub parents: Vec<
        Box<
            dyn AsyncParent<OutType = InType, SignalType = SignalType, ThreadIdType = ThreadIdType>,
        >,
    >,
}
