//! Dispatch traits for biunion builder methods.
//!
//! [`AddParent`] dispatches `.parent::<Side>(node)` to resolve
//! left or right input to Async based on the side marker.

use crate::biunion::Side;

/// Resolve one input to Async via a parent node.
///
/// Implemented per-side (`biunion::Left`, `biunion::Right`) on
/// specific builder states. Enables `node.parent::<Left>(parent)`.
pub trait AddParent<S: Side, ParentType> {
    type Resolved;
    fn parent(self, parent: ParentType) -> Self::Resolved;
}
