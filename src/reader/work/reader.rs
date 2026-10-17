use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::signal::{Origin, Trackable};
use crate::thread::{DefaultThread, ThreadId};
use crate::work::{Upstream, Workable};

#[must_use = "a reader owns the node it drives; dropping it drops the subgraph"]
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

/// [`Reads`] owns a node and reads its output on this thread: `let mut out = node.reader()?`.
/// [Reader::read] works the node until it has output.
///
/// Reading is owning, so a side is moved with [crate::At::at]:
/// `bifurcation.at::<Right>().reader()?`. The thread is [DefaultThread]; for another, use
/// [Reader::of]. The data type is an optional annotation: `node.reader::<Frame>()?`. To read
/// a node something else owns, use [crate::Tees::tee].
pub trait Reads<'params, SignalType, ShapeType>: Sized {
    fn reader<DataType>(self) -> Result<Reader<'params, DataType, SignalType>, Error>
    where
        DataType: Send + Sync + 'static,
        SignalType: Origin + Send + Sync + 'static,
        Self: Upstream<'params, DataType, SignalType, DefaultThread, ShapeType>;
}

impl<'params, NodeType: Connection, SignalType, ShapeType> Reads<'params, SignalType, ShapeType>
    for NodeType
{
    fn reader<DataType>(self) -> Result<Reader<'params, DataType, SignalType>, Error>
    where
        DataType: Send + Sync + 'static,
        SignalType: Origin + Send + Sync + 'static,
        Self: Upstream<'params, DataType, SignalType, DefaultThread, ShapeType>,
    {
        Reader::of(self)
    }
}

impl<'params, DataType, SignalType, ThreadIdType>
    Reader<'params, DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    /// Own and read `node` (or a side moved with [crate::At::at]) on a thread other than
    /// [DefaultThread]; otherwise use [Reads::reader].
    pub fn of<ShapeType>(
        node: impl Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType>,
    ) -> Result<Self, Error> {
        let buffer = Receiver::new();
        let workable = node.attach(Box::new(buffer.sender()))?;
        Ok(Self { workable, buffer })
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
    use crate::graph::Add;
    use crate::work::Sink;
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
        let mut reader = mock_node.reader().unwrap();

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
        let mut reader = mock_node.reader().unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        *output.lock().unwrap() = Message::Flush("bye".into());

        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
    }

    #[test]
    fn reader_mark() {
        let (mock_node, output) = MockNode::new(Message::Marker("hi".into()));
        let mut reader = mock_node.reader().unwrap();

        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));
        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));

        *output.lock().unwrap() = Message::Marker("bye".into());

        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
    }
}
