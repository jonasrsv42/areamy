use crate::error::Error;
use crate::Pushable;
use crate::Trackable;
use crate::{
    graph::Get,
    marker::{Connection, Multiplicity},
    Message, Origin,
};

// A `Source` is a convenience type for an input. It forwards data into some `Pushable`.
pub struct Source<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
    pushable: Box<dyn Pushable<Message = Message<DataType, SignalType>>>,
}

impl<DataType, SignalType> Connection for Source<DataType, SignalType>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
}

impl<DataType> Source<DataType, Trackable<&'static str>>
where
    DataType: Clone + Send + Sync + 'static,
{
    pub fn new<MultiplicityType: Multiplicity>(
        input: &impl Get<
            dyn Pushable<Message = Message<DataType, Trackable<&'static str>>> + 'static,
            MultiplicityType,
        >,
    ) -> Result<Self, Error> {
        let pushable = input.get()?;
        Ok(Self { pushable })
    }
}

impl<DataType, SignalType> Source<DataType, SignalType>
where
    DataType: Send + Sync + Clone,
    SignalType: Send + Sync + Clone + Origin,
{
    pub fn of<Node, MultiplicityType>(node: &Node) -> Result<Self, Error>
    where
        Node: Get<dyn Pushable<Message = Message<DataType, SignalType>>, MultiplicityType>,
        MultiplicityType: Multiplicity,
    {
        let pushable = node.get()?;
        Ok(Self { pushable })
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
    use crate::SyncQueue;
    use std::sync::Arc;

    struct MockNode {
        input: Arc<SyncQueue<Message<usize, Trackable<&'static str>>>>,
    }

    impl Get<dyn Pushable<Message = Message<usize, Trackable<&'static str>>>> for MockNode {
        fn get(
            &self,
        ) -> Result<Box<dyn Pushable<Message = Message<usize, Trackable<&'static str>>>>, Error>
        {
            Get::get(&self.input)
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
