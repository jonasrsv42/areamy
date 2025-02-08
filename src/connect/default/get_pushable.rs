use crate::error::Error;
use crate::fatal;
use crate::{Connection, GetPushable, Pushable};
use std::sync::{Arc, Mutex};

impl<GetPushableType, PushableType, SourceConnection: Connection> GetPushable<SourceConnection>
    for &GetPushableType
where
    GetPushableType: GetPushable<SourceConnection, Pushable = PushableType>,
    PushableType: Pushable + 'static,
{
    type Pushable = PushableType;

    fn get(&self) -> Result<Self::Pushable, Error> {
        (*self).get()
    }
}

impl<GetPushableType, PushableType, SourceConnection: Connection> GetPushable<SourceConnection>
    for &mut GetPushableType
where
    GetPushableType: GetPushable<SourceConnection, Pushable = PushableType>,
    PushableType: Pushable + 'static,
{
    type Pushable = PushableType;

    fn get(&self) -> Result<PushableType, Error> {
        GetPushableType::get(self)
    }
}

impl<GetPushableType, PushableType, SourceConnection: Connection> GetPushable<SourceConnection>
    for Arc<Mutex<GetPushableType>>
where
    GetPushableType: GetPushable<SourceConnection, Pushable = PushableType>,
    PushableType: Pushable + 'static,
{
    type Pushable = PushableType;

    fn get(&self) -> Result<PushableType, Error> {
        let owned = self.lock().map_err(|e| fatal!(e))?;
        GetPushableType::get(&*owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SyncQueue;
    use std::sync::Arc;

    #[test]
    fn get_pushable_arc_works() {
        let pushable: Arc<SyncQueue<usize>> = Arc::new(SyncQueue::new());
        let _ = GetPushable::get(&pushable);
    }
}
