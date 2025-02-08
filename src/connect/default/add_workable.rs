use crate::error::Error;
use crate::{fatal, AddWorkable, Connection, Workable};
use std::sync::{Arc, Mutex};

impl<'a, AddWorkableType, SourceConnection: Connection> AddWorkable<SourceConnection>
    for Arc<Mutex<AddWorkableType>>
where
    AddWorkableType: AddWorkable<SourceConnection>,
{
    type ThreadId = AddWorkableType::ThreadId;
    fn add<WorkableType: Workable<ThreadId = AddWorkableType::ThreadId> + 'static>(
        &mut self,
        workable: WorkableType,
    ) -> Result<(), Error> {
        let mut owned = self.lock().map_err(|e| fatal!(e))?;
        AddWorkableType::add(&mut owned, workable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::graph::tests::{SyncBuilder, SyncNode};
    use std::sync::Arc;

    #[test]
    fn add_workable_mut_ref() {
        let mut add_workable: SyncBuilder = SyncBuilder(Arc::new(Mutex::new(SyncNode::new())));

        let workable: Arc<Mutex<SyncNode>> = Arc::new(Mutex::new(SyncNode::new()));

        let _ = AddWorkable::add(&mut add_workable, workable);
    }
}
