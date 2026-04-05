//! Bundles parent [AsyncParent](crate::connect::poll::traits::AsyncParent) configs
//! for async input nodes.

use crate::ThreadId;
use crate::connect::poll::traits::AsyncParent;
use crate::signal::Origin;

/// Bundles parent [AsyncParent] configs for async input nodes.
/// Parents are built during [PollGraphBuilder::build](crate::connect::poll::graph::PollGraphBuilder::build).
pub struct AsyncIn<InType, SignalType: Origin, ThreadIdType: ThreadId> {
    pub parents: Vec<Box<dyn AsyncParent<InType, SignalType, ThreadIdType>>>,
}
