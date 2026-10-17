use crate::error::Error;
use crate::graph::Get;
use crate::graph::marker::{Connection, Multiplicity};

impl<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity, GetType>
    Get<ConnectionType, MultiplicityType> for Box<GetType>
where
    GetType: ?Sized + Get<ConnectionType, MultiplicityType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(self.as_ref())
    }
}
