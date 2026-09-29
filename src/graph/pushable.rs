use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;

mod default;

/// A [`TryPushable`] is a data [Connection] in our graph, it is used for dataflow.
/// The associated [TryPushable::DataType] and [TryPushable::SignalType] types are used to
/// specify the types for the [Message<DataType, SignalType>] that can be pushed through this
/// [Connection].
///
/// Child nodes will typically hold [TryPushable] references to queues owned by a parent
/// and push [Message] into them when scheduled.
///
/// <div class="warning"> Nodes should never implement [TryPushable] as it
/// easily leads to circualar references and memory leaks </div>
///
/// Instead nodes hold a reference to something that is [TryPushable] such as a
/// [crate::edge::sync::Sender].
pub trait TryPushable: Connection {
    /// The DataType used in [Message<DataType, SignalType>] for this [TryPushable]
    type DataType;

    /// The SignalType used in [Message<DataType, SignalType>] for this [TryPushable]
    type SignalType: Origin;

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

/// A [`Pushable`] always delivers: [Pushable::push] makes no promise about blocking. Separate
/// from [TryPushable] so a sink that must never block can leave it out.
pub trait Pushable: TryPushable {
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
