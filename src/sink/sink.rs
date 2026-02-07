use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;

/// Trait for reading data from a graph segment.
///
/// [`GraphSink`] provides the interface for consuming data from graph nodes.
/// Concrete implementations like `work::Sink` and `pull::Sink` provide
/// specific behaviors for different graph types.
pub trait GraphSink {
    type ThreadId;
    type DataType: Send + Sync;
    type SignalType: Origin + Send + Sync;

    /// [`GraphSink::read`] will [GraphSink::poll] and if there's no result it will schedule work until
    /// there is.
    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error>;

    /// [`GraphSink::poll`] will return the first result in a queue, otherwise None if it's empty.
    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error>;
}
