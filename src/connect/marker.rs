// Module with helpers for declaring types in-case of ambiguity.
// e.g. when creating a biunion with only 1 active input.

use crate::error::Error;
use crate::AddPushable;
use std::marker::PhantomData;

pub struct Marker<Type> {
    push_type: PhantomData<Type>,
}

impl<Type> Marker<Type> {
    pub fn new() -> Self {
        Marker {
            push_type: PhantomData,
        }
    }
}

impl<MessageType> AddPushable for Marker<MessageType> {
    type Message = MessageType;
    fn add<PushableType>(&mut self, _pushable: PushableType) -> Result<(), Error> {
        Ok(())
    }
}
