use crate::error::Error;
use crate::graph::Get;
use crate::graph::marker::{Connection, Multiplicity, Unary};
use core::marker::PhantomData;

/// [`SelectRef`] is a node borrowed at one [Multiplicity] to [Get] from, exposed as [Unary].
/// See [crate::At::at_ref].
pub struct SelectRef<'node, NodeType, MultiplicityType> {
    node: &'node NodeType,
    multiplicity: PhantomData<MultiplicityType>,
}

impl<'node, NodeType, MultiplicityType> SelectRef<'node, NodeType, MultiplicityType> {
    pub(super) fn new(node: &'node NodeType) -> Self {
        Self {
            node,
            multiplicity: PhantomData,
        }
    }
}

impl<NodeType: Connection, MultiplicityType> Connection
    for SelectRef<'_, NodeType, MultiplicityType>
{
}

impl<ConnectionType, NodeType, MultiplicityType> Get<ConnectionType, Unary>
    for SelectRef<'_, NodeType, MultiplicityType>
where
    ConnectionType: Connection + ?Sized,
    NodeType: Get<ConnectionType, MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        self.node.get()
    }
}
