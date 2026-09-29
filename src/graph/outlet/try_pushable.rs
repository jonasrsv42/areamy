use crate::error::Error;
use crate::graph::Outlet;
use crate::message::Message;

mod default;

/// A [`TryPushable`] pushes without ever blocking.
pub trait TryPushable: Outlet {
    /// Pushes without ever blocking; a full sink hands the message back. [TryPush::Pushed]
    /// means the message was consumed (delivered, or intentionally dropped by a wrapper such as
    /// a signal policy).
    ///
    /// A bounded sink must tell [TryPush::Full] from [TryPush::Stuck], tracked per producer
    /// handle: always answering `Full` lets a retrying producer spin, always answering `Stuck`
    /// can block a producer whose consumer is waiting on it.
    fn try_push(
        &mut self,
        msg: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error>;
}

/// Outcome of [TryPushable::try_push].
#[must_use = "a refused message must be kept (parked) or it is lost"]
#[derive(Debug, PartialEq)]
pub enum TryPush<MessageType> {
    /// The sink consumed the message.
    Pushed,
    /// The sink is full; the message is handed back. The consumer drained since this
    /// producer was last refused (or it wasn't refused before), so retrying later can succeed.
    Full(MessageType),
    /// The sink is full and nothing drained since this producer was last refused; the message
    /// is handed back. Retrying without waiting would spin.
    Stuck(MessageType),
}

impl<MessageType> TryPush<MessageType> {
    /// The handed-back message, `Full` or `Stuck` alike; `None` if the sink took it.
    pub fn refused(self) -> Option<MessageType> {
        match self {
            TryPush::Pushed => None,
            TryPush::Full(message) | TryPush::Stuck(message) => Some(message),
        }
    }
}
