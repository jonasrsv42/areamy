use crate::graph::Add;
use crate::graph::marker::{Connection, Multiplicity, Unary};

/// [`Outputs`] names the sink a node's push output takes, so a connection asks the downstream for
/// exactly that. [Outputs::Sink] is a dyn [crate::graph::Outlet] composite, such as
/// [crate::work::Sink] or [crate::poll::Sink]; [Add] stores it.
///
/// One impl per output [Multiplicity]: a node with two outputs names a sink per side.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no single push output here",
    label = "no `Outputs<{MultiplicityType}>`",
    note = "a node with several outputs needs a borrowed side: `node.at_mut::<Left>().pushes(..)`",
    note = "`at` moves the node and only feeds `via`; `pushes` takes `at_mut`",
    note = "a poll node has an output after `.output()`"
)]
pub trait Outputs<MultiplicityType: Multiplicity = Unary>:
    Add<Self::Sink, MultiplicityType>
{
    type Sink: Connection + ?Sized;
}

impl<MultiplicityType: Multiplicity, OutputsType> Outputs<MultiplicityType> for Box<OutputsType>
where
    OutputsType: ?Sized + Outputs<MultiplicityType>,
{
    type Sink = OutputsType::Sink;
}
