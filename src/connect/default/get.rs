use crate::error::Error;
use crate::fatal;
use crate::{graph::Get, marker::Connection, marker::Multiplicity};
use std::sync::{Arc, Mutex};

impl<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity, GetType>
    Get<ConnectionType, MultiplicityType> for Arc<Mutex<GetType>>
where
    GetType: Get<ConnectionType, MultiplicityType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        let owned = self.lock().map_err(|e| fatal!(e))?;
        Get::get(&*owned)
    }
}

