use crate::error::Error;
use crate::{GetWorkable, ThreadId, Workable};
use std::sync::{Arc, Mutex};

impl<GetWorkableType, WorkableType> GetWorkable for &GetWorkableType
where
    GetWorkableType: GetWorkable<Workable = WorkableType>,
    WorkableType: Workable + 'static,
{
    type Workable = WorkableType;

    fn get(&self) -> Result<WorkableType, Error> {
        GetWorkableType::get(self)
    }
}

impl<ThreadIdType, WorkableType> GetWorkable for Arc<Mutex<WorkableType>>
where
    ThreadIdType: ThreadId,
    WorkableType: Workable<ThreadId = ThreadIdType> + 'static + ?Sized,
{
    type Workable = Arc<Mutex<WorkableType>>;

    fn get(&self) -> Result<Arc<Mutex<WorkableType>>, Error> {
        Ok(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::graph::tests::SyncNode;
    use crate::DefaultThread;
    use std::sync::Arc;

    #[test]
    fn get_workable_arc_mut_works() {
        let node: Arc<Mutex<SyncNode>> = Arc::new(Mutex::new(SyncNode::new()));
        let _ = GetWorkable::get(&node);
    }

    #[test]
    fn get_workable_arc_mut_dyn_works() {
        let node: Arc<Mutex<dyn Workable<ThreadId = DefaultThread>>> =
            Arc::new(Mutex::new(SyncNode::new()));
        let _ = GetWorkable::get(&node);
    }

    #[test]
    fn get_workable_shmut_arc_mut_dyn_works() {
        let node: Arc<Mutex<dyn Workable<ThreadId = DefaultThread>>> =
            Arc::new(Mutex::new(SyncNode::new()));
        let _ = GetWorkable::get(&node);
    }
}
