use crate::Pushable;
use crate::Trackable;
use crate::error::Error;
use crate::{
    Message, Origin,
    graph::Get,
    marker::{Connection, Multiplicity},
};

/// A `Source` is a convenience type for an input. It forwards data into some inner source.
pub struct Source<'params, DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
    inner: Box<
        dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>
            + Send
            + Sync
            + 'params,
    >,
}

impl<'params, DataType, SignalType> Connection for Source<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
}

impl<'params, DataType> Source<'params, DataType, Trackable<&'static str>>
where
    DataType: Send + Sync + 'static,
{
    pub fn new<MultiplicityType: Multiplicity>(
        input: &impl Get<
            dyn crate::GraphPushSource<DataType = DataType, SignalType = Trackable<&'static str>>
                + Send
                + Sync
                + 'params,
            MultiplicityType,
        >,
    ) -> Result<Self, Error> {
        let inner = input.get()?;
        Ok(Self { inner })
    }
}

impl<'params, DataType, SignalType> Source<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
    pub fn of<Node, MultiplicityType>(node: &Node) -> Result<Self, Error>
    where
        Node: Get<
                dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>
                    + Send
                    + Sync
                    + 'params,
                MultiplicityType,
            >,
        MultiplicityType: Multiplicity,
    {
        let inner = node.get()?;
        Ok(Self { inner })
    }
}

impl<'params, DataType, SignalType> Pushable for Source<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.inner.push(object)
    }
}

impl<'params, DataType, SignalType> crate::GraphPushSource for Source<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::sync::Receiver;

    struct MockNode {
        input: Receiver<usize, Trackable<&'static str>>,
    }

    impl Get<dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>> for MockNode {
        fn get(
            &self,
        ) -> Result<Box<dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>>, Error>
        {
            Get::get(&self.input)
        }
    }

    impl
        Get<
            dyn crate::GraphPushSource<DataType = usize, SignalType = Trackable<&'static str>>
                + Send
                + Sync,
        > for MockNode
    {
        fn get(
            &self,
        ) -> Result<
            Box<
                dyn crate::GraphPushSource<DataType = usize, SignalType = Trackable<&'static str>>
                    + Send
                    + Sync,
            >,
            Error,
        > {
            Get::get(&self.input)
        }
    }

    #[test]
    fn source_basic() {
        let mock_node = MockNode {
            input: Receiver::new(),
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
            input: Receiver::new(),
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
            input: Receiver::new(),
        };

        let mut source = Source::new(&mock_node).unwrap();

        source.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Marker("hi".into()),]
        );
    }
}
