use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;

mod default;

/// An [`Outlet`] is a data [Connection] in our graph, it is used for dataflow.
/// The associated [Outlet::DataType] and [Outlet::SignalType] types are used to specify the
/// types for the [Message<DataType, SignalType>] that can be pushed through this
/// [Connection]. How a push behaves is up to [TryPushable] and [Pushable].
///
/// Child nodes will typically hold [Outlet] references to queues owned by a parent
/// and push [Message] into them when scheduled.
///
/// <div class="warning"> Nodes should never implement [Outlet] as it
/// easily leads to circualar references and memory leaks </div>
///
/// Instead nodes hold a reference to something that is an [Outlet] such as a
/// [crate::edge::sync::Sender].
pub trait Outlet: Connection {
    /// The DataType used in [Message<DataType, SignalType>] for this [Outlet]
    type DataType;

    /// The SignalType used in [Message<DataType, SignalType>] for this [Outlet]
    type SignalType: Origin;
}

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

/// A [`Pushable`] always delivers: [Pushable::push] makes no promise about blocking.
pub trait Pushable: Outlet {
    /// Delivers the message or errors (e.g. closed); may block until there is room.
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;
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
