use crate::error::Error;
use crate::graph::marker::Unary;
use crate::graph::{Get, Outlet, Outputs};

/// [`Pushes`] is the data-only edge: `up.pushes(&down)?`. `up` keeps running where it is and
/// pushes into `down`'s input.
///
/// Both ends are single-sided; name a side with [crate::At::at_mut] (upstream) or
/// [crate::At::at_ref] (downstream). The data type is an optional annotation:
/// `up.pushes::<Frame>(&down)?`.
///
/// Signals follow data ([crate::SignalPolicy::FollowData]): a push can be a back-edge, where
/// forwarding every signal would loop.
///
/// ```ignore
/// up.pushes(&down)?;
/// router.at_mut::<Right>().pushes(&noise)?;
/// up.pushes::<Frame>(&merge.at_ref::<Left>())?;
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no single output to push from",
    note = "a node with several outputs needs a borrowed side: `node.at_mut::<Left>().pushes(..)`",
    note = "`at` moves the node and only feeds `via`; `pushes` takes `at_mut`",
    note = "a poll node has an output after `.output()`"
)]
pub trait Pushes: Outputs<Unary> {
    /// Pushes this node's output into `down`'s input.
    fn pushes<DataType>(&mut self, down: &impl Get<Self::Sink, Unary>) -> Result<(), Error>
    where
        Self::Sink: Outlet<DataType = DataType>;
}
