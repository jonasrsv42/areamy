use crate::error::Error;
use crate::graph::marker::{Connection, Multiplicity, Unary};
use crate::graph::{Add, Get, Outputs};
use core::marker::PhantomData;

/// [`SelectMut`] is a node borrowed at one [Multiplicity], exposed as [Unary]. See
/// [crate::At::at_mut].
pub struct SelectMut<'node, NodeType, MultiplicityType> {
    node: &'node mut NodeType,
    multiplicity: PhantomData<MultiplicityType>,
}

impl<'node, NodeType, MultiplicityType> SelectMut<'node, NodeType, MultiplicityType> {
    pub(super) fn new(node: &'node mut NodeType) -> Self {
        Self {
            node,
            multiplicity: PhantomData,
        }
    }
}

impl<NodeType: Connection, MultiplicityType> Connection
    for SelectMut<'_, NodeType, MultiplicityType>
{
}

impl<ConnectionType, NodeType, MultiplicityType> Get<ConnectionType, Unary>
    for SelectMut<'_, NodeType, MultiplicityType>
where
    ConnectionType: Connection + ?Sized,
    NodeType: Get<ConnectionType, MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        self.node.get()
    }
}

impl<ConnectionType, NodeType, MultiplicityType> Add<ConnectionType, Unary>
    for SelectMut<'_, NodeType, MultiplicityType>
where
    ConnectionType: Connection + ?Sized,
    NodeType: Add<ConnectionType, MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error> {
        self.node.add(connection)
    }
}

impl<NodeType, MultiplicityType> Outputs<Unary> for SelectMut<'_, NodeType, MultiplicityType>
where
    NodeType: Outputs<MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    type Sink = NodeType::Sink;
}
