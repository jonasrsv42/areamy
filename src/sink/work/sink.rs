use crate::error::Error;
use crate::SyncEdge;
use crate::{
    graph::Add, marker::Multiplicity, DefaultThread, Pushable, ThreadId, Trackable, Workable,
};
use crate::{Message, Origin};
use std::sync::Arc;

pub struct Sink<DataType, SignalType = Trackable<&'static str>, ThreadIdType = DefaultThread>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
{
    workable: Box<dyn Workable<ThreadId = ThreadIdType>>,
    buffer: Arc<SyncEdge<DataType, SignalType>>,
}

impl<DataType, SignalType> Sink<DataType, SignalType, DefaultThread>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
{
    pub fn new<MultiplicityType>(
        mut workable: Box<
            impl Workable<ThreadId = DefaultThread>
                + Add<dyn Pushable<DataType = DataType, SignalType = SignalType>, MultiplicityType>
                + 'static,
        >,
    ) -> Result<Self, Error>
    where
        MultiplicityType: Multiplicity,
    {
        let shared_buffer = Arc::new(SyncEdge::new());

        Add::add(workable.as_mut(), Box::new(shared_buffer.clone()))?;
        let sink = Self {
            workable,
            buffer: shared_buffer.clone(),
        };

        return Ok(sink);
    }
}

impl<DataType, SignalType, ThreadIdType> Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
{
    pub fn of<MultiplicityType>(
        mut workable: Box<
            impl Workable<ThreadId = ThreadIdType>
                + Add<dyn Pushable<DataType = DataType, SignalType = SignalType>, MultiplicityType>
                + 'static,
        >,
    ) -> Result<Self, Error>
    where
        MultiplicityType: Multiplicity,
    {
        let shared_buffer = Arc::new(SyncEdge::new());

        Add::add(workable.as_mut(), Box::new(shared_buffer.clone()))?;
        let sink = Self {
            workable,
            buffer: shared_buffer.clone(),
        };

        return Ok(sink);
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

impl<DataType, SignalType, ThreadIdType> crate::sink::Sink
    for Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

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
    use crate::{graph::Get, marker::Connection, Pushable};
    use std::sync::Mutex;

    struct MockNode {
        output: Message<usize, &'static str>,
        pushable: Vec<Box<dyn Pushable<DataType = usize, SignalType = &'static str>>>,
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

    impl Add<dyn Pushable<DataType = usize, SignalType = &'static str>> for MockNode {
        fn add(
            &mut self,
            pushable: Box<dyn Pushable<DataType = usize, SignalType = &'static str>>,
        ) -> Result<(), Error> {
            Ok(self.pushable.push(pushable))
        }
    }

    #[test]
    fn sink_basic() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Data(5),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(5));
        assert_eq!(sink.read().unwrap(), Message::Data(5));

        mock_node.lock().unwrap().output = Message::Data(8);

        assert_eq!(sink.read().unwrap(), Message::Data(8));
        assert_eq!(sink.read().unwrap(), Message::Data(8));

        mock_node.lock().unwrap().output = Message::Data(10);

        assert_eq!(sink.read().unwrap(), Message::Data(10));
        assert_eq!(sink.read().unwrap(), Message::Data(10));
    }

    #[test]
    fn sink_flush() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Flush("hi"),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Flush("hi"));
        assert_eq!(sink.read().unwrap(), Message::Flush("hi"));

        mock_node.lock().unwrap().output = Message::Flush("bye");

        assert_eq!(sink.read().unwrap(), Message::Flush("bye"));
        assert_eq!(sink.read().unwrap(), Message::Flush("bye"));
    }

    #[test]
    fn sink_mark() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Marker("hi"),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(Box::new(mock_node.clone())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));
        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));

        mock_node.lock().unwrap().output = Message::Marker("bye");

        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
    }
}
