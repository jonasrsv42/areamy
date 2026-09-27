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

    /// Pushes a message to this [Pushable]
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;
}
