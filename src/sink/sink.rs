use crate::error::Error;

pub trait Sink {
    type Message;
    type ThreadId;

    fn read(&mut self) -> Result<Self::Message, Error>;
}
