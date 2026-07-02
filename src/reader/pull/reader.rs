use crate::Message;
use crate::error::Error;
use crate::{DefaultThread, Origin, Pullable, ThreadId, Trackable, marker::Connection, reader};

pub struct Reader<
    'params,
    DataType,
    SignalType = Trackable<&'static str>,
    ThreadIdType = DefaultThread,
> where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
    // We store it in the heap to mask the recursive `Pullable` type.
    // If one does not want a heap allocation then reading directly from the pullable is OK.
    pullable: Box<
        dyn Pullable<ThreadId = ThreadIdType, DataType = DataType, SignalType = SignalType>
            + 'params,
    >,
}

impl<'params, DataType, SignalType, ThreadIdType> Connection
    for Reader<'params, DataType, SignalType, ThreadIdType>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
}

impl<'params, DataType, SignalType> Reader<'params, DataType, SignalType, DefaultThread>
where
    DataType: Sync + Send,
    SignalType: Origin,
{
    pub fn new(
        pullable: impl Pullable<ThreadId = DefaultThread, DataType = DataType, SignalType = SignalType>
        + 'params,
    ) -> Self {
        let reader = Self {
            pullable: Box::new(pullable),
        };

        return reader;
    }
}

impl<'params, DataType, SignalType, ThreadIdType> Pullable
    for Reader<'params, DataType, SignalType, ThreadIdType>
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

impl<'params, DataType, SignalType, ThreadIdType> reader::Reader
    for Reader<'params, DataType, SignalType, ThreadIdType>
where
    DataType: Sync + Send,
    SignalType: Origin,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        Reader::pull(self)
    }

    /// [Pullable] [reader::Reader] is not [reader::Reader::poll]able since it
    /// maintains no buffer.
    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        Ok(None)
    }
}
