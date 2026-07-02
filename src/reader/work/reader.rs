use crate::connect::sync::Receiver;
use crate::error::Error;
use crate::{DefaultThread, Sink, ThreadId, Trackable, Workable, graph::Add, marker::Multiplicity};
use crate::{Message, Origin};

pub struct Reader<
    'params,
    DataType,
    SignalType = Trackable<&'static str>,
    ThreadIdType = DefaultThread,
> where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    buffer: Receiver<DataType, SignalType>,
}

impl<'params, DataType, SignalType> Reader<'params, DataType, SignalType, DefaultThread>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    pub fn new<MultiplicityType: Multiplicity>(
        mut workable: Box<
            impl Workable<ThreadId = DefaultThread>
            + Add<
                dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
                MultiplicityType,
            > + 'params,
        >,
    ) -> Result<Self, Error> {
        let buffer = Receiver::new();
        Add::add(workable.as_mut(), Box::new(buffer.sender()))?;
        Ok(Self { workable, buffer })
    }
}

impl<'params, DataType, SignalType, ThreadIdType>
    Reader<'params, DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    pub fn of<MultiplicityType>(
        mut workable: Box<
            impl Workable<ThreadId = ThreadIdType>
            + Add<
                dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
                MultiplicityType,
            > + 'params,
        >,
    ) -> Result<Self, Error>
    where
        MultiplicityType: Multiplicity,
    {
        let buffer = Receiver::new();
        Add::add(workable.as_mut(), Box::new(buffer.sender()))?;
        Ok(Self { workable, buffer })
    }

    pub fn read(&mut self) -> Result<Message<DataType, SignalType>, Error> {
        let mut output_is_empty = self.buffer.is_empty()?;
        while output_is_empty {
            self.workable.work()?;

            output_is_empty = self.buffer.is_empty()?;
        }

        let output = self.buffer.read_front()?;
        return Ok(output);
    }
}

impl<'params, DataType, SignalType, ThreadIdType> crate::reader::Reader
    for Reader<'params, DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        Reader::read(self)
    }

    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        self.buffer.poll()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{graph::Get, marker::Connection};
    use std::sync::{Arc, Mutex};

    struct MockNode {
        output: Message<usize, &'static str>,
        pushable: Vec<Box<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync>>,
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

    impl Add<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync> for MockNode {
        fn add(
            &mut self,
            closeable: Box<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync>,
        ) -> Result<(), Error> {
            Ok(self.pushable.push(closeable))
        }
    }

    #[test]
    fn reader_basic() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Data(5),
            pushable: Vec::new(),
        }));
        let mut reader = Reader::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(5));
        assert_eq!(reader.read().unwrap(), Message::Data(5));

        mock_node.lock().unwrap().output = Message::Data(8);

        assert_eq!(reader.read().unwrap(), Message::Data(8));
        assert_eq!(reader.read().unwrap(), Message::Data(8));

        mock_node.lock().unwrap().output = Message::Data(10);

        assert_eq!(reader.read().unwrap(), Message::Data(10));
        assert_eq!(reader.read().unwrap(), Message::Data(10));
    }

    #[test]
    fn reader_flush() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Flush("hi"),
            pushable: Vec::new(),
        }));
        let mut reader = Reader::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("hi"));
        assert_eq!(reader.read().unwrap(), Message::Flush("hi"));

        mock_node.lock().unwrap().output = Message::Flush("bye");

        assert_eq!(reader.read().unwrap(), Message::Flush("bye"));
        assert_eq!(reader.read().unwrap(), Message::Flush("bye"));
    }

    #[test]
    fn reader_mark() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Marker("hi"),
            pushable: Vec::new(),
        }));
        let mut reader = Reader::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Marker("hi"));
        assert_eq!(reader.read().unwrap(), Message::Marker("hi"));

        mock_node.lock().unwrap().output = Message::Marker("bye");

        assert_eq!(reader.read().unwrap(), Message::Marker("bye"));
        assert_eq!(reader.read().unwrap(), Message::Marker("bye"));
    }
}
