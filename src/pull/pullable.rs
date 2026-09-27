use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::thread::ThreadId;

mod default;

/// [`Pullable`] combines [crate::work::Workable] and [crate::graph::Pushable] in being both a
/// scheduling and dataflow [Connection]. As with [crate::work::Workable] the [Pullable] connection
/// has a unique [Pullable::ThreadId] associated with it as well as a [Message<DataType, SignalType>]
/// that it will yield upon scheduling.
///
/// A [Pullable] is less flexible but is useful to declare line segments in the graph. It lets
/// us skip the syncronization parts of [crate::graph::Pushable] connections.
///
/// TL;DR it lets us skip a few mutexes, condvars and moves.
pub trait Pullable: Send + Connection {
    /// The thread that is allowed to schedule this node.
    type ThreadId: ThreadId;
    /// The DataType used in [Message<DataType, SignalType>] for this [Pullable]
    type DataType: Send + Sync;
    /// The SignalType used in [Message<DataType, SignalType>] for this [Pullable]
    type SignalType: Origin + Send + Sync;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error>;
}
