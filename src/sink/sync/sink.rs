use crate::error::Error;
use crate::SyncQueue;
use crate::{AddPushable, Connection, DefaultThread, GetWorkable, ThreadId, Trackable, Workable};
use crate::{Message, Origin};
use std::sync::Arc;

pub struct Sink<DataType, SignalType = Trackable<&'static str>, ThreadIdType = DefaultThread>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
{
    workable: Box<dyn Workable<ThreadId = ThreadIdType>>,
    buffer: Arc<SyncQueue<Message<DataType, SignalType>>>,
}

impl<DataType, SignalType> Sink<DataType, SignalType, DefaultThread>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
{
    pub fn new<WorkSource, DataSource, WorkableType, ConnectionType>(
        work_source: WorkSource,
        data_source: &mut DataSource,
    ) -> Result<Self, Error>
    where
        WorkSource: GetWorkable<Workable = WorkableType>,
        DataSource: AddPushable<ConnectionType, Message = Message<DataType, SignalType>>,
        WorkableType: Workable<ThreadId = DefaultThread> + 'static,
        ConnectionType: Connection,
    {
        let workable = work_source.get()?;

        let shared_buffer = Arc::new(SyncQueue::new());
        let sink = Self {
            workable: Box::new(workable),
            buffer: shared_buffer.clone(),
        };

        AddPushable::add(data_source, Box::new(shared_buffer))?;

        return Ok(sink);
    }
}

impl<DataType, SignalType, ThreadIdType> Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
{
    pub fn of<WorkSource, DataSource, ConnectionType>(
        work_source: WorkSource,
        data_source: &mut DataSource,
    ) -> Result<Self, Error>
    where
        WorkSource: GetWorkable<Workable = Box<dyn Workable<ThreadId = ThreadIdType>>>,
        DataSource: AddPushable<ConnectionType, Message = Message<DataType, SignalType>>,
        ConnectionType: Connection,
    {
        let workable = work_source.get()?;

        let shared_buffer = Arc::new(SyncQueue::new());
        let sink = Self {
            workable,
            buffer: shared_buffer.clone(),
        };

        AddPushable::add(data_source, Box::new(shared_buffer))?;

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
    type Message = Message<DataType, SignalType>;
    type ThreadId = ThreadIdType;

    fn read(&mut self) -> Result<Self::Message, Error> {
        Sink::read(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pushable;
    use std::sync::Mutex;

    struct MockNode {
        output: Message<usize, &'static str>,
        pushable: Vec<Box<dyn Pushable<Message = Message<usize, &'static str>>>>,
    }

    impl Workable for MockNode {
        type ThreadId = DefaultThread;
        fn work(&mut self) -> Result<(), Error> {
            for pushable in self.pushable.iter_mut() {
                pushable.push(self.output.clone())?;
            }
            Ok(())
        }
    }

    impl AddPushable for MockNode {
        type Message = Message<usize, &'static str>;
        fn add<PushableType>(&mut self, pushable: PushableType) -> Result<(), Error>
        where
            PushableType: Pushable<Message = Message<usize, &'static str>> + 'static,
        {
            Ok(self.pushable.push(Box::new(pushable)))
        }
    }

    #[test]
    fn sink_basic() {
        let mock_node = Arc::new(Mutex::new(MockNode {
            output: Message::Data(5),
            pushable: Vec::new(),
        }));
        let mut sink = Sink::new(mock_node.clone(), &mut mock_node.clone()).unwrap();

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
        let mut sink = Sink::new(mock_node.clone(), &mut mock_node.clone()).unwrap();

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
        let mut sink = Sink::new(mock_node.clone(), &mut mock_node.clone()).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));
        assert_eq!(sink.read().unwrap(), Message::Marker("hi"));

        mock_node.lock().unwrap().output = Message::Marker("bye");

        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
        assert_eq!(sink.read().unwrap(), Message::Marker("bye"));
    }
}
