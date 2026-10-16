use crate::graph::marker::Connection;
use crate::signal::Origin;

mod default;
mod receivable;

pub use receivable::Receivable;

/// An [`Inlet`] is a data [Connection] a node receives from, the counterpart of
/// [crate::graph::Outlet]. The associated [Inlet::DataType] and [Inlet::SignalType] types are
/// the [crate::message::Message<DataType, SignalType>] it yields. How a receive behaves is up to
/// [Receivable].
pub trait Inlet: Connection {
    /// The DataType used in [crate::message::Message<DataType, SignalType>] for this [Inlet]
    type DataType;

    /// The SignalType used in [crate::message::Message<DataType, SignalType>] for this [Inlet]
    type SignalType: Origin;
}
