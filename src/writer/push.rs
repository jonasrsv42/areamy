use crate::error::Error;
use crate::graph::marker::{Connection, Multiplicity};
use crate::graph::{Closeable, Get, Pushable, Sink, TryPush};
use crate::message::Message;
use crate::signal::{Origin, Trackable};

/// A `Writer` is a convenience type for an input. It forwards data into some inner source.
pub struct Writer<'params, DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
    inner: Box<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>,
}

impl<'params, DataType, SignalType> Connection for Writer<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
}

impl<'params, DataType> Writer<'params, DataType, Trackable<&'static str>>
where
    DataType: Send + Sync + 'static,
{
    /// Write into `input`. The side is inferred from the data type, or named with
    /// [crate::graph::At::at]: `Writer::new(&node.at::<Left>())`.
    pub fn new<MultiplicityType: Multiplicity>(
        input: &impl Get<
            dyn Sink<DataType = DataType, SignalType = Trackable<&'static str>>
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

impl<'params, DataType, SignalType> Writer<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Send + Sync + Origin,
{
    /// [Writer::new] with a generic signal type.
    pub fn of<MultiplicityType: Multiplicity>(
        node: &impl Get<
            dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
            MultiplicityType,
        >,
    ) -> Result<Self, Error> {
        let inner = node.get()?;
        Ok(Self { inner })
    }
}

impl<'params, DataType, SignalType> Pushable for Writer<'params, DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.inner.push(object)
    }

    fn try_push(
        &mut self,
        object: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        self.inner.try_push(object)
    }
}

impl<'params, DataType, SignalType> Closeable for Writer<'params, DataType, SignalType>
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
    use crate::edge::sync::Receiver;

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

    impl Get<dyn crate::Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>
        for MockNode
    {
        fn get(
            &self,
        ) -> Result<
            Box<
                dyn crate::Sink<DataType = usize, SignalType = Trackable<&'static str>>
                    + Send
                    + Sync,
            >,
            Error,
        > {
            Get::get(&self.input)
        }
    }

    #[test]
    fn writer_basic() {
        let mock_node = MockNode {
            input: Receiver::new(),
        };

        let mut writer = Writer::new(&mock_node).unwrap();

        writer.push(Message::Data(5)).unwrap();
        writer.push(Message::Data(5)).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Data(5), Message::Data(5)]
        );
    }

    #[test]
    fn writer_can_flush() {
        let mock_node = MockNode {
            input: Receiver::new(),
        };

        let mut writer = Writer::new(&mock_node).unwrap();

        writer.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Flush("hi".into()),]
        );
    }

    #[test]
    fn writer_can_mark() {
        let mock_node = MockNode {
            input: Receiver::new(),
        };

        let mut writer = Writer::new(&mock_node).unwrap();

        writer.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(
            mock_node.input.read_all().unwrap(),
            vec![Message::Marker("hi".into()),]
        );
    }
}
