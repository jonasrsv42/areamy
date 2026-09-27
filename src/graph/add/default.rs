use crate::error::Error;
use crate::{
    fatal,
    graph::Add,
    marker::{Connection, Multiplicity},
};
use std::sync::{Arc, Mutex};

impl<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity, AddType>
    Add<ConnectionType, MultiplicityType> for Arc<Mutex<AddType>>
where
    AddType: Add<ConnectionType, MultiplicityType>,
{
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error> {
        let mut owned = self.lock().map_err(|e| fatal!(e))?;
        Add::add(&mut (*owned), connection)
    }
}

impl<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity, AddType>
    Add<ConnectionType, MultiplicityType> for Box<AddType>
where
    AddType: Add<ConnectionType, MultiplicityType>,
{
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error> {
        Add::add(self.as_mut(), connection)
    }
}
