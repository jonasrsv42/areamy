use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;

mod default;

/// A [`Pushable`] is a data [Connection] in our graph, it is used for dataflow.
/// The associated [Pushable::DataType] and [Pushable::SignalType] types are used to specify
/// the types for the [Message<DataType, SignalType>] that can be [Pushable::push]ed through this [Connection].
///
/// Child nodes will typically hold [Pushable] referencs to queues owned by a parent
/// and [Pushable::push] [Message] into them when scheduled.
///
/// <div class="warning"> Nodes should never implement [Pushable] as it
/// easily leads to circualar references and memory leaks </div>
///
/// Instead nodes hold a reference to something that is [Pushable] such as a [crate::edge::sync::Sender] or `Rc<RefCell<Vec<..>>>`
pub trait Pushable: Connection {
    /// The DataType used in [Message<DataType, SignalType>] for this [Pushable]
    type DataType;

    /// The SignalType used in [Message<DataType, SignalType>] for this [Pushable]
    type SignalType: Origin;

    /// Pushes a message to this [Pushable]. May block until there is room; use
    /// [Pushable::try_push] where blocking is not allowed.
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;

    /// Pushes without ever blocking; a full sink hands the message back. [TryPush::Pushed]
    /// means the message was consumed (delivered, or intentionally dropped by a wrapper such as
    /// a signal policy). Required: only the implementor knows whether its [Pushable::push] can
    /// block.
    ///
    /// A bounded sink must tell [TryPush::Full] from [TryPush::Stuck], tracked per producer
    /// handle: always answering `Full` lets a retrying producer spin, always answering `Stuck`
    /// can block a producer whose consumer is waiting on it.
    fn try_push(
        &mut self,
        msg: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error>;
}

/// Outcome of [Pushable::try_push].
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
