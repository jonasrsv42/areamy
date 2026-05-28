//! Type markers.
use crate::graph::Add;
use crate::{Closeable, Pushable};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

/// [PhantomNode] is useful for force typing an output of a node.
pub struct PhantomNode<Type> {
    phaton_data: PhantomData<Type>,
}

impl<Type> PhantomNode<Type> {
    pub fn new() -> Self {
        PhantomNode {
            phaton_data: PhantomData,
        }
    }
}

// We assume MessageType is Message<DataType, SignalType>
// This is just a placeholder implementation for PhantomNode
impl<'params, DataType, SignalType>
    Add<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>
    for PhantomNode<crate::message::Message<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: crate::signal::Origin + Send + Sync,
{
    fn add(
        &mut self,
        _connection: Box<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>,
    ) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

impl<'params, DataType, SignalType>
    Add<dyn Closeable<DataType = DataType, SignalType = SignalType> + 'params>
    for PhantomNode<crate::message::Message<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: crate::signal::Origin + Send + Sync,
{
    fn add(
        &mut self,
        _connection: Box<dyn Closeable<DataType = DataType, SignalType = SignalType> + 'params>,
    ) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

impl<'params, DataType, SignalType>
    Add<dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>
    for PhantomNode<crate::message::Message<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: crate::signal::Origin + Send + Sync,
{
    fn add(
        &mut self,
        _connection: Box<
            dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
        >,
    ) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

/// [`Connection`] is the base marker indicating that something can form an edge in our graph.
pub trait Connection {}

impl<ConnectionType: Connection + ?Sized> Connection for Arc<Mutex<ConnectionType>> {}
impl<ConnectionType: Connection + ?Sized> Connection for Arc<ConnectionType> {}
impl<ConnectionType: Connection + ?Sized> Connection for Box<ConnectionType> {}
impl<ConnectionType: Connection> Connection for std::rc::Rc<std::cell::RefCell<ConnectionType>> {}
impl<ConnectionType: Connection> Connection for Vec<ConnectionType> {}

/// [`Multiplicity`] is an identifier of connection of a node.
/// For a node with multiple outbound or inbound connections each connection can
/// be identified with a multiplicity. A [crate::node::line] always has a [Unary]
/// multiplicity since there's only one output and input. But [crate::node::biunion]
/// will have special multiplicity for input edges to differentiate them and
/// [crate::node::bifurcation] for output edges.
pub trait Multiplicity {}

/// [`Unary`] is the default multiplicity of all [Connection]. Implying that
/// it is unique and only one.
pub struct Unary {}

/// [Unary] is a [Multiplicity]
impl Multiplicity for Unary {}

/// [`Linkage`] identifies the role of a node in a two-stage link.
/// Used by [crate::Linkable] to distinguish how a node is being
/// connected during deferred graph construction.
pub trait Linkage {}

/// [`Parent`] linkage — the node is being linked as a parent.
/// It receives an output edge (where to push data to its child).
pub struct Parent;
impl Linkage for Parent {}

/// [`Child`] linkage — the node is being linked as a child.
/// It receives an input edge (where to receive data from its parent).
pub struct Child;
impl Linkage for Child {}

/// [`Terminal`] linkage — standalone node.
/// Uses `Edge = ()` in [crate::Linkable].
pub struct Terminal;
impl Linkage for Terminal {}
