use crate::error::Error;
use crate::graph::marker::{Connection, Multiplicity, Unary};
use crate::graph::{Add, Get};
use std::marker::PhantomData;

/// [`At`] names one [Multiplicity] of a node, so connections reach a specific side.
///
/// Direction comes from the connection: a parent side is an output ([Add] of a sink),
/// a child side is an input ([Get] of a sink, [Add] of a parent). Single-sided nodes
/// never need it.
///
/// ```ignore
/// Push::connect(&mut left, &biunion.at::<Left>())?;
/// work::Bidi::connect(right, &mut biunion.at::<Right>())?;
/// Push::connect(&mut bifurcation.at::<Left>(), &child)?;
/// ```
pub trait At: Sized {
    fn at<MultiplicityType: Multiplicity>(&mut self) -> Select<'_, Self, MultiplicityType> {
        Select {
            node: self,
            multiplicity: PhantomData,
        }
    }
}

impl<NodeType> At for NodeType {}

/// [`Select`] is a node borrowed at one [Multiplicity], exposed as [Unary]. See [At].
pub struct Select<'node, NodeType, MultiplicityType> {
    node: &'node mut NodeType,
    multiplicity: PhantomData<MultiplicityType>,
}

impl<ConnectionType, NodeType, MultiplicityType> Get<ConnectionType, Unary>
    for Select<'_, NodeType, MultiplicityType>
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
    for Select<'_, NodeType, MultiplicityType>
where
    ConnectionType: Connection + ?Sized,
    NodeType: Add<ConnectionType, MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error> {
        self.node.add(connection)
    }
}
