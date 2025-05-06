use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;

pub trait Sink {
    type ThreadId;
    type DataType: Send + Sync;
    type SignalType: Origin + Send + Sync;

    /// [`Sink::read`] will [Sink::poll] and if there's no result it will schedule work until
    /// there is.
    fn read(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error>;

    /// [`Sink::poll`] will return the first result in a queue, otherwise None if it's empty.
    fn poll(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error>;
}
