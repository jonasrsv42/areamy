// Module with helpers for declaring types in-case of ambiguity.
// e.g. when creating a biunion with only 1 active input.

use crate::graph::Add;
use crate::Pushable;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub struct PhantomNode<Type> {
    push_type: PhantomData<Type>,
}

impl<Type> PhantomNode<Type> {
    pub fn new() -> Self {
        PhantomNode {
            push_type: PhantomData,
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

/// [`Connection`]
pub trait Connection {}

impl<ConnectionType: Connection + ?Sized> Connection for Arc<Mutex<ConnectionType>> {}
impl<ConnectionType: Connection + ?Sized> Connection for Arc<ConnectionType> {}
impl<ConnectionType: Connection + ?Sized> Connection for Box<ConnectionType> {}

// A type Indicating the multiplicity of a connection of a nodes output and input
// For example the biunion node has two inputs hence it
// needs two different types of `AddWorkable` one for
// `AddWorkable<Left>` and for `AddWorkable<Right>`.
//
// Likewise the bifurcation has
// AddPushable<Left> and AddPushable<Right> we are
// just using templating instead of making longer
// function names like `AddPushableLeft` and
// `AddPushableRight` etc. This also allows us
// to have some functions be generic to the
// `Multiplicity`. Like `make_bidi`
pub trait Multiplicity {}

// Default connection. Implies we have only a single
// connection such as input and output of a line.
pub struct Unary {}
impl Multiplicity for Unary {}
