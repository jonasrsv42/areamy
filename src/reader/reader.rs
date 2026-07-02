use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;

/// Trait for reading data from a graph segment.
///
/// [`Reader`] provides the interface for consuming data from graph nodes.
/// Concrete implementations like `work::Reader` and `pull::Reader` provide
/// specific behaviors for different graph types.
pub trait Reader {
    type ThreadId;
    type DataType: Send + Sync;
    type SignalType: Origin + Send + Sync;

    /// [`Reader::read`] will [Reader::poll] and if there's no result it will schedule work until
    /// there is.
    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error>;

    /// [`Reader::poll`] will return the first result in a queue, otherwise None if it's empty.
    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error>;
}
