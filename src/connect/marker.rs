//! Type markers.
use std::sync::{Arc, Mutex};

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
