use crate::error::Error;
use crate::{fatal, ThreadId, Workable};
use std::sync::{Arc, Mutex};

impl<WorkableType, ThreadIdType> Workable for Arc<Mutex<WorkableType>>
where
    ThreadIdType: ThreadId,
    WorkableType: Workable<ThreadId = ThreadIdType> + ?Sized,
{
    fn work(&mut self) -> Result<(), Error> {
        let mut owned = self.lock().map_err(|e| fatal!(e))?;
        owned.work()
    }

    type ThreadId = ThreadIdType;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::graph::tests::SyncNode;
    use crate::{Message, SyncQueue};
    use std::sync::Arc;

    #[test]
    fn workable_can_work() {
        let mut node = SyncNode::new();

        let input = node.input.clone();
        let output = Arc::new(SyncQueue::new());

        node.outputs.push(Box::new(output.clone()));

        input.push_back(Message::Data(0)).unwrap();
        input.push_back(Message::Data(2)).unwrap();
        input.push_back(Message::Data(3)).unwrap();

        node.work().unwrap();
        node.work().unwrap();

        assert_eq!(
            output.read_all().unwrap(),
            vec![Message::Data(1), Message::Data(6)]
        );

        node.work().unwrap();

        assert_eq!(output.read_all().unwrap(), vec![Message::Data(9)]);
    }

    #[test]
    fn workable_arc_can_work() {
        let mut node = Arc::new(Mutex::new(SyncNode::new()));

        let input = node.lock().unwrap().input.clone();
        let output = Arc::new(SyncQueue::new());

        node.lock().unwrap().outputs.push(Box::new(output.clone()));

        input.push_back(Message::Data(0)).unwrap();
        input.push_back(Message::Data(2)).unwrap();
        input.push_back(Message::Data(3)).unwrap();

        node.work().unwrap();
        node.work().unwrap();

        assert_eq!(
            output.read_all().unwrap(),
            vec![Message::Data(1), Message::Data(6)]
        );

        node.work().unwrap();

        assert_eq!(output.read_all().unwrap(), vec![Message::Data(9)]);
    }
}
