use crate::graph::marker::Connection;
use crate::signal::Origin;

mod default;
mod pushable;
mod try_pushable;

pub use pushable::Pushable;
pub use try_pushable::{TryPush, TryPushable};

/// An [`Outlet`] is a data [Connection] in our graph, it is used for dataflow.
/// The associated [Outlet::DataType] and [Outlet::SignalType] types are used to specify the
/// types for the [crate::message::Message<DataType, SignalType>] that can be pushed through
/// this [Connection]. How a push behaves is up to [TryPushable] and [Pushable].
///
/// Child nodes will typically hold [Outlet] references to queues owned by a parent
/// and push [crate::message::Message] into them when scheduled.
///
/// <div class="warning"> Nodes should never implement [Outlet] as it
/// easily leads to circualar references and memory leaks </div>
///
/// Instead nodes hold a reference to something that is an [Outlet] such as a
/// [crate::edge::sync::Sender].
pub trait Outlet: Connection {
    /// The DataType used in [crate::message::Message<DataType, SignalType>] for this [Outlet]
    type DataType;

    /// The SignalType used in [crate::message::Message<DataType, SignalType>] for this [Outlet]
    type SignalType: Origin;
}
