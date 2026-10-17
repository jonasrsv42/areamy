use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::{Connection, Unary};
use crate::message::Message;
use crate::signal::{Origin, Trackable};
use crate::thread::DefaultThread;
use crate::work::Sink;

/// A tee [`Reader`] buffers what a node it doesn't own pushes. See [Tees].
pub struct Reader<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Unbounded: read on the producer's own thread, and one `work()` may push past any bound.
    buffer: Receiver<DataType, SignalType>,
}

/// [`Tees`] reads a node's output without owning it: `let mut out = node.tee()?`. The node
/// keeps running where it is; its pushes land in the reader's buffer.
///
/// Name a side with [crate::At::at_mut]: `node.at_mut::<Left>().tee()?`. The data type is an
/// optional annotation: `node.tee::<Frame>()?`. To own and drive the node, use
/// [crate::Reads::reader].
///
/// The tee gets every message the node pushes, signals included.
pub trait Tees<'params, SignalType> {
    /// Adds a buffered output to this node and returns its reader.
    fn tee<DataType>(&mut self) -> Result<Reader<DataType, SignalType>, Error>
    where
        DataType: Send + Sync + 'static,
        SignalType: Origin + Send + Sync + 'static,
        Self: Add<
                dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
                Unary,
            >;
}

impl<'params, NodeType: Connection, SignalType> Tees<'params, SignalType> for NodeType {
    fn tee<DataType>(&mut self) -> Result<Reader<DataType, SignalType>, Error>
    where
        DataType: Send + Sync + 'static,
        SignalType: Origin + Send + Sync + 'static,
        Self: Add<
                dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
                Unary,
            >,
    {
        let buffer = Receiver::new();
        Add::add(self, Box::new(buffer.sender()))?;
        Ok(Reader { buffer })
    }
}

impl<DataType, SignalType> Reader<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    /// Blocks until the node pushes, then returns the oldest message.
    pub fn read(&mut self) -> Result<Message<DataType, SignalType>, Error> {
        let output = self.buffer.read_front()?;
        Ok(output)
    }
}

impl<DataType, SignalType> crate::reader::Reader for Reader<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    type DataType = DataType;
    type SignalType = SignalType;
    type ThreadId = DefaultThread;

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
    use crate::Workable;

    struct MockNode {
        output: Message<usize, Trackable<&'static str>>,
        pushable: Vec<
            Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>,
        >,
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

    impl Add<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>
        for MockNode
    {
        fn add(
            &mut self,
            pushable: Box<
                dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            >,
        ) -> Result<(), Error> {
            self.pushable.push(pushable);
            Ok(())
        }
    }

    #[test]
    fn reader_basic() {
        let mut mock_node = MockNode {
            output: Message::Data(5),
            pushable: Vec::new(),
        };
        let mut reader = mock_node.tee().unwrap();

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(5));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(5));

        mock_node.output = Message::Data(8);

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(8));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(8));

        mock_node.output = Message::Data(10);

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(10));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(10));
    }

    #[test]
    fn reader_flush() {
        let mut mock_node = MockNode {
            output: Message::Flush("hi".into()),
            pushable: Vec::new(),
        };
        let mut reader = mock_node.tee().unwrap();

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        mock_node.output = Message::Flush("bye".into());

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Flush("bye".into()));
    }

    #[test]
    fn reader_mark() {
        let mut mock_node = MockNode {
            output: Message::Marker("hi".into()),
            pushable: Vec::new(),
        };
        let mut reader = mock_node.tee().unwrap();

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));

        mock_node.output = Message::Marker("bye".into());

        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
        mock_node.work().unwrap();
        assert_eq!(reader.read().unwrap(), Message::Marker("bye".into()));
    }
}
