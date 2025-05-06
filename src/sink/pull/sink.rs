use crate::error::Error;
use crate::Message;
use crate::{marker::Connection, sink, DefaultThread, Origin, Pullable, ThreadId, Trackable};

pub struct Sink<DataType, SignalType = Trackable<&'static str>, ThreadIdType = DefaultThread>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
    // We store it in the heap to mask the recursive `Pullable` type.
    // If one does not want a heap allocation then reading directly from the pullable is OK.
    pullable:
        Box<dyn Pullable<ThreadId = ThreadIdType, DataType = DataType, SignalType = SignalType>>,
}

impl<DataType, SignalType, ThreadIdType> Connection for Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
}

impl<DataType, SignalType> Sink<DataType, SignalType, DefaultThread>
where
    DataType: Sync + Send,
    SignalType: Origin,
{
    pub fn new(
        pullable: impl Pullable<ThreadId = DefaultThread, DataType = DataType, SignalType = SignalType>
            + 'static,
    ) -> Self {
        let sink = Self {
            pullable: Box::new(pullable),
        };

        return sink;
    }
}

impl<DataType, SignalType, ThreadIdType> Pullable for Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        self.pullable.pull()
    }
}

impl<DataType, SignalType, ThreadIdType> sink::Sink for Sink<DataType, SignalType, ThreadIdType>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        Sink::pull(self)
    }

    /// [Pullable] [sink::Sink] is not [sink::Sink::poll]able since it
    /// maintains no buffer.
    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        Ok(None)
    }
}
