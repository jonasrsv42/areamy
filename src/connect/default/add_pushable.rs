use crate::error::Error;
use crate::{fatal, AddPushable, Connection, Pushable};
use std::sync::{Arc, Mutex};

impl<AddPushableType, SinkConnection: Connection> AddPushable<SinkConnection>
    for Arc<Mutex<AddPushableType>>
where
    AddPushableType: AddPushable<SinkConnection>,
{
    type Message = AddPushableType::Message;
    fn add<PushableType: Pushable<Message = AddPushableType::Message> + 'static>(
        &mut self,
        pushable: PushableType,
    ) -> Result<(), Error> {
        let mut owned = self.lock().map_err(|e| fatal!(e))?;
        AddPushableType::add(&mut owned, pushable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::graph::tests::{SyncBuilder, SyncNode};
    use crate::SyncQueue;
    use std::sync::Arc;

    #[test]
    fn add_pushable_sh_mut() {
        let mut add_pushable = Arc::new(Mutex::new(SyncBuilder(Arc::new(Mutex::new(
            SyncNode::new(),
        )))));
        let _ = AddPushable::add(&mut add_pushable, Box::new(Arc::new(SyncQueue::new())));
    }
}
