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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::graph::tests::{SyncBuilder, SyncNode};
    use crate::SyncQueue;
    use crate::{Message, Pushable};
    use std::sync::Arc;

    #[test]
    fn add_pushable_sh_mut() {
        let mut add_pushable = Arc::new(Mutex::new(SyncBuilder(Arc::new(Mutex::new(
            SyncNode::new(),
        )))));

        let pushable: Box<dyn Pushable<Message = Message<usize, usize>>> =
            Box::new(Arc::new(SyncQueue::new()));
        let _ = Add::add(&mut add_pushable, pushable);
    }
}
