use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::Multiplicity;
use crate::message::Message;
use crate::signal::{Origin, Trackable};
use crate::thread::{DefaultThread, ThreadId};
use crate::work::Sink;
use crate::work::Workable;

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
    /// Unbounded: read on the producer's own thread, and one `work()` may push past any bound.
    buffer: Receiver<DataType, SignalType>,
}

impl<'params, DataType, SignalType> Reader<'params, DataType, SignalType, DefaultThread>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    pub fn new<MultiplicityType: Multiplicity>(
        mut workable: impl Workable<ThreadId = DefaultThread>
        + Add<
            dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
            MultiplicityType,
        > + 'params,
    ) -> Result<Self, Error> {
        let buffer = Receiver::new();
        Add::add(&mut workable, Box::new(buffer.sender()))?;
        Ok(Self {
            workable: Box::new(workable),
            buffer,
        })
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
        mut workable: impl Workable<ThreadId = ThreadIdType>
        + Add<
            dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
            MultiplicityType,
        > + 'params,
    ) -> Result<Self, Error>
    where
        MultiplicityType: Multiplicity,
    {
        let buffer = Receiver::new();
        Add::add(&mut workable, Box::new(buffer.sender()))?;
        Ok(Self {
            workable: Box::new(workable),
            buffer,
        })
    }

    pub fn read(&mut self) -> Result<Message<DataType, SignalType>, Error> {
        let mut output_is_empty = self.buffer.is_empty()?;
        while output_is_empty {
            self.workable.work()?;

            output_is_empty = self.buffer.is_empty()?;
        }

        let output = self.buffer.read_front()?;
        Ok(output)
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
    use crate::marker::Connection;
    use std::sync::{Arc, Mutex};

    type Output = Arc<Mutex<Message<usize, Trackable<&'static str>>>>;

    /// Pushes the current `output` on every `work`; the test keeps a handle to change it.
    struct MockNode {
        output: Output,
        pushable: Vec<
            Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>,
        >,
    }

    impl MockNode {
        fn new(output: Message<usize, Trackable<&'static str>>) -> (Self, Output) {
            let output = Arc::new(Mutex::new(output));
            let node = Self {
                output: output.clone(),
                pushable: Vec::new(),
            };
            (node, output)
        }
    }

    impl Connection for MockNode {}

    impl Workable for MockNode {
        type ThreadId = DefaultThread;
        fn work(&mut self) -> Result<(), Error> {
            let output = self.output.lock().unwrap().clone();
            for pushable in self.pushable.iter_mut() {
                pushable.push(output.clone())?;
            }
            Ok(())
        }
    }

    impl Add<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>
        for MockNode
    {
        fn add(
            &mut self,
            closeable: Box<
                dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            >,
        ) -> Result<(), Error> {
            self.pushable.push(closeable);
            Ok(())
        }
    }

    #[test]
    fn reader_basic() {
        let (mock_node, output) = MockNode::new(Message::Data(5));
        let mut reader = Reader::new(mock_node).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(5));
        assert_eq!(reader.read().unwrap(), Message::Data(5));

        *output.lock().unwrap() = Message::Data(8);

        assert_eq!(reader.read().unwrap(), Message::Data(8));
        assert_eq!(reader.read().unwrap(), Message::Data(8));

        *output.lock().unwrap() = Message::Data(10);

        assert_eq!(reader.read().unwrap(), Message::Data(10));
        assert_eq!(reader.read().unwrap(), Message::Data(10));
    }

    #[test]
    fn reader_flush() {
        let (mock_node, output) = MockNode::new(Message::Flush("hi".into()));
        let mut reader = Reader::new(mock_node).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        *output.lock().unwrap() = Message::Flush("bye".into());

        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
    }

    #[test]
    fn reader_mark() {
        let (mock_node, output) = MockNode::new(Message::Marker("hi".into()));
        let mut reader = Reader::new(mock_node).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));
        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));

        *output.lock().unwrap() = Message::Marker("bye".into());

        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
    }
}
