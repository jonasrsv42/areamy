use crate::error::Error;
use crate::graph::Outlet;
use crate::message::Message;

mod default;

/// A [`Pushable`] always delivers: [Pushable::push] makes no promise about blocking.
pub trait Pushable: Outlet {
    /// Delivers the message or errors (e.g. closed); may block until there is room.
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;
}
