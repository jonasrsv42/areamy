use crate::error::Error;
use crate::{ThreadId, Workable, fatal};
use std::sync::{Arc, Mutex};

/// A [Workable] that is [std::marker::Send] + [std::marker::Sync] is also a [Workable]
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
    use crate::Message;
    use crate::connect::graph::tests::Node;
    use crate::connect::sync::Receiver;

    #[test]
    fn workable_can_work() {
        let mut node = Node::new();

        let input = node.input.sender();
        let output = Receiver::<usize, usize>::new();

        node.outputs.push(Box::new(output.sender()));

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
        let mut node = Arc::new(Mutex::new(Node::new()));

        let input = node.lock().unwrap().input.sender();
        let output = Receiver::<usize, usize>::new();

        node.lock().unwrap().outputs.push(Box::new(output.sender()));

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
