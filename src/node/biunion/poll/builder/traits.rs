//! Dispatch traits for biunion builder methods.
//!
//! [`Resolve`] dispatches `.parent::<Side>(node)` to resolve
//! left or right input to Async based on the side marker.

use crate::ThreadId;
use crate::connect::poll::traits::AsyncParent;
use crate::signal::Origin;

/// Resolve one input to Async via a parent node.
///
/// `S::Data` determines the parent's data type based on the side.
/// Implemented for `biunion::Left` and `biunion::Right` on each
/// builder state.
pub trait Resolve<Node, SignalType: Origin, ThreadIdType: ThreadId> {
    type Data;
    type Resolved;
    fn resolve(
        node: Node,
        parent: Box<dyn AsyncParent<Self::Data, SignalType, ThreadIdType>>,
    ) -> Self::Resolved;
}
