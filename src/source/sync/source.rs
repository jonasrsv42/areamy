use crate::error::Error;
use crate::Pushable;
use crate::Trackable;
use crate::{Connection, GetPushable, Message, Origin};

// A `Source` is a convenience type for an input. It forwards data into some `Pushable`.
pub struct Source<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
    pushable: Box<dyn Pushable<Message = Message<DataType, SignalType>>>,
}

impl<DataType> Source<DataType, Trackable<&'static str>>
where
    DataType: Clone + Send + Sync + 'static,
{
    pub fn new<Node, PushableType, ConnectionType>(node: Node) -> Result<Self, Error>
    where
        Node: GetPushable<ConnectionType, Pushable = PushableType>,
        PushableType: Pushable<Message = Message<DataType, Trackable<&'static str>>> + 'static,
        ConnectionType: Connection,
    {
        let pushable = node.get()?;
        Ok(Self {
            pushable: Box::new(pushable),
        })
    }
}

impl<DataType, SignalType> Source<DataType, SignalType>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
    pub fn of<Node, PushableType, ConnectionType>(node: Node) -> Result<Self, Error>
    where
        Node: GetPushable<ConnectionType, Pushable = PushableType>,
        PushableType: Pushable<Message = Message<DataType, SignalType>> + 'static,
        ConnectionType: Connection,
    {
        let pushable = node.get()?;
        Ok(Self {
            pushable: Box::new(pushable),
        })
    }
}

impl<DataType, SignalType> Pushable for Source<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    type Message = Message<DataType, SignalType>;
    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.pushable.push(object)
    }
}

impl<DataType, SignalType> crate::Source for Source<DataType, SignalType>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GetPushable, SyncQueue};
    use std::sync::Arc;

    struct MockNode {
        input: Arc<SyncQueue<Message<usize, Trackable<&'static str>>>>,
    }

    impl GetPushable for MockNode {
        type Pushable = Arc<SyncQueue<Message<usize, Trackable<&'static str>>>>;
        fn get(&self) -> Result<Self::Pushable, Error> {
            Ok(self.input.clone())
        }
    }

    #[test]
    fn source_basic() {
        let mock_node = MockNode {
            input: Arc::new(SyncQueue::new()),
        };

        let mut source = Source::new(&mock_node).unwrap();

        source.push(Message::Data(5)).unwrap();
        source.push(Message::Data(5)).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Data(5), Message::Data(5)]
        );
    }

    #[test]
    fn source_can_flush() {
        let mock_node = MockNode {
            input: Arc::new(SyncQueue::new()),
        };

        let mut source = Source::new(&mock_node).unwrap();

        source.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Flush("hi".into()),]
        );
    }

    #[test]
    fn source_can_mark() {
        let mock_node = MockNode {
            input: Arc::new(SyncQueue::new()),
        };

        let mut source = Source::new(&mock_node).unwrap();

        source.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Marker("hi".into()),]
        );
    }
}
