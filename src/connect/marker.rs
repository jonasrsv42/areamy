//! Type markers.
use crate::graph::Add;
use crate::Pushable;
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

impl<MessageType> Add<dyn Pushable<Message = MessageType>> for PhantomNode<MessageType> {
    fn add(
        &mut self,
        _connection: Box<dyn Pushable<Message = MessageType>>,
    ) -> Result<(), crate::error::Error> {
        todo!()
    }
}

/// [`Connection`] is the base marker indicating that something can form an edge in our graph.
pub trait Connection {}

impl<ConnectionType: Connection + ?Sized> Connection for Arc<Mutex<ConnectionType>> {}
impl<ConnectionType: Connection + ?Sized> Connection for Arc<ConnectionType> {}
impl<ConnectionType: Connection + ?Sized> Connection for Box<ConnectionType> {}

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
