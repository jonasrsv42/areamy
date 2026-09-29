use crate::graph::Add;
use crate::graph::marker::{Connection, Multiplicity, Unary};

/// [`Outputs`] names the sink a node's push output takes, so a connection asks the child for
/// exactly that. [Outputs::Sink] is a dyn [crate::graph::Outlet] composite, such as
/// [crate::work::Sink] or [crate::poll::Sink]; [Add] stores it.
///
/// One impl per output [Multiplicity]: a node with two outputs names a sink per side.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no single push output here",
    label = "no `Outputs<{MultiplicityType}>`",
    note = "a node with several outputs needs a side: `node.at::<Left>()`"
)]
pub trait Outputs<MultiplicityType: Multiplicity = Unary>:
    Add<Self::Sink, MultiplicityType>
{
    type Sink: Connection + ?Sized;
}
