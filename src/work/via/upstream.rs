use crate::error::Error;
use crate::graph::marker::{Multiplicity, Unary};
use crate::graph::{Add, Select};
use crate::work::{Sink, Workable};
use std::marker::PhantomData;

type UpstreamSink<'params, DataType, SignalType> =
    dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params;

/// [`Upstream`] is a work node something can own: a single-sided node ([Whole]) or a node moved
/// at a side ([Sided], from [crate::At::at]). Owning edges ([crate::Via]) and readers
/// ([crate::Reads]) take it through here.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be owned here",
    note = "owning moves the node: move a side with `at`; `at_mut` / `at_ref` only borrow",
    note = "to read without owning, use `tee`"
)]
pub trait Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType> {
    /// Adds `sink` as the node's output (at its side) and hands the node over to be owned.
    fn attach(
        self,
        sink: Box<UpstreamSink<'params, DataType, SignalType>>,
    ) -> Result<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>, Error>;
}

/// [Upstream] shape of a single-sided node.
pub struct Whole;

/// [Upstream] shape of a node moved at a side.
pub struct Sided<MultiplicityType>(PhantomData<MultiplicityType>);

impl<'params, NodeType, DataType, SignalType, ThreadIdType>
    Upstream<'params, DataType, SignalType, ThreadIdType, Whole> for NodeType
where
    NodeType: Workable<ThreadId = ThreadIdType>
        + Add<UpstreamSink<'params, DataType, SignalType>, Unary>
        + 'params,
{
    fn attach(
        mut self,
        sink: Box<UpstreamSink<'params, DataType, SignalType>>,
    ) -> Result<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>, Error> {
        Add::add(&mut self, sink)?;
        Ok(Box::new(self))
    }
}

impl<'params, NodeType, MultiplicityType, DataType, SignalType, ThreadIdType>
    Upstream<'params, DataType, SignalType, ThreadIdType, Sided<MultiplicityType>>
    for Select<NodeType, MultiplicityType>
where
    NodeType: Workable<ThreadId = ThreadIdType>
        + Add<UpstreamSink<'params, DataType, SignalType>, MultiplicityType>
        + 'params,
    MultiplicityType: Multiplicity,
{
    fn attach(
        self,
        sink: Box<UpstreamSink<'params, DataType, SignalType>>,
    ) -> Result<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>, Error> {
        let mut node = self.into_node();
        Add::<_, MultiplicityType>::add(&mut node, sink)?;
        Ok(Box::new(node))
    }
}
