use crate::error::Error;
use crate::graph::Outlet;
use crate::message::Message;

mod default;

/// A [`Pushable`] always delivers: [Pushable::push] makes no promise about blocking. It knows no
/// threads or wakers: it blocks, and waking a producer is [crate::graph::Schedule]'s.
pub trait Pushable: Outlet {
    /// Delivers the message or errors (e.g. closed); may block until there is room.
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;
}
