use crate::connect::sync::Receiver;
use crate::error::Error;
use crate::{Closeable, DefaultThread, Trackable, graph::Add, marker::Multiplicity};
use crate::{Message, Origin};

pub struct Sink<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    buffer: Receiver<DataType, SignalType>,
}

impl<DataType, SignalType> Sink<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    pub fn new<'params, MultiplicityType>(
        workable: &mut (
                 impl Add<
            dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
            MultiplicityType,
        > + 'params
             ),
    ) -> Result<Self, Error>
    where
        MultiplicityType: Multiplicity,
    {
        let buffer = Receiver::new();
        Add::add(workable, Box::new(buffer.sender()))?;
        Ok(Self { buffer })
    }

    pub fn read(&mut self) -> Result<Message<DataType, SignalType>, Error> {
        let output = self.buffer.read_front()?;
        return Ok(output);
    }
}

impl<DataType, SignalType> crate::sink::GraphSink for Sink<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    type DataType = DataType;
    type SignalType = SignalType;
    type ThreadId = DefaultThread;

    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        Sink::read(self)
    }

    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        self.buffer.poll()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Closeable, DefaultThread, Workable, graph::Get, marker::Connection};
    use std::sync::{Arc, Mutex};

    struct MockNode {
        output: Message<usize, &'static str>,
        pushable:
            Vec<Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync>>,
    }

    impl Connection for MockNode {}

    impl Workable for MockNode {
        type ThreadId = DefaultThread;
        fn work(&mut self) -> Result<(), Error> {
            for pushable in self.pushable.iter_mut() {
                pushable.push(self.output.clone())?;
            }
            Ok(())
        }
    }

    impl Get<dyn Workable<ThreadId = DefaultThread>> for Arc<Mutex<MockNode>> {
        fn get(&self) -> Result<Box<dyn Workable<ThreadId = DefaultThread>>, Error> {
            Ok(Box::new(self.clone()))
        }
    }

    impl Add<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> for MockNode {
        fn add(
            &mut self,
            pushable: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync>,
        ) -> Result<(), Error> {
            Ok(self.pushable.push(pushable))
        }
    }

    #[test]
    fn sink_basic() {
        let mut mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Data(5),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(&mut Box::new(mock_node.clone())).unwrap();

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(5));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(5));

        mock_node.lock().unwrap().output = Message::Data(8);

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(8));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(8));

        mock_node.lock().unwrap().output = Message::Data(10);

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(10));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(10));
    }

    #[test]
    fn sink_flush() {
        let mut mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Flush("hi"),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(&mut Box::new(mock_node.clone())).unwrap();

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Flush("hi"));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Flush("hi"));

        mock_node.lock().unwrap().output = Message::Flush("bye");

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Flush("bye"));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Flush("bye"));
    }

    #[test]
    fn sink_mark() {
        let mut mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Marker("hi"),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(&mut Box::new(mock_node.clone())).unwrap();

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));

        mock_node.lock().unwrap().output = Message::Marker("bye");

        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
        mock_node.work().unwrap();
        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
    }
}
