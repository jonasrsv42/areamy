use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::{Connection, Multiplicity};

impl<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity, AddType>
    Add<ConnectionType, MultiplicityType> for Box<AddType>
where
    AddType: ?Sized + Add<ConnectionType, MultiplicityType>,
{
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error> {
        Add::add(self.as_mut(), connection)
    }
}
