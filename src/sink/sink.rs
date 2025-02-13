use crate::error::Error;

pub trait Sink {
    type Message;
    type ThreadId;

    /// [`Sink::read`] will [Sink::poll] and if there's no result it will schedule work until
    /// there is.
    fn read(&mut self) -> Result<Self::Message, Error>;

    /// [`Sink::poll`] will return the first result in a queue, otherwise None if it's empty.
    fn poll(&mut self) -> Result<Option<Self::Message>, Error>;
}
