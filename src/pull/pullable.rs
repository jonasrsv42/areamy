use crate::error::Error;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::node::line::pull::Line;
use crate::node::line::routine::LineRoutine;
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

    /// [`Pullable::then`] creates a [Pullable] connection. The child takes ownership
    /// of the parent. This connection does not use synchronization or dynamic dispatch
    /// and is well suited for line segments in the graph where performance is necessary
    /// and where synchronization or message passing could be a overhead.
    ///
    /// ```ignore
    /// let frontend = root.then(Framer::new()).then(Mel::new());
    /// ```
    fn then<Out, RoutineType>(
        self,
        routine: RoutineType,
    ) -> Line<Self::DataType, Out, Self::SignalType, Self::ThreadId, RoutineType, Self>
    where
        Self: Sized,
        Out: Send + Sync,
        RoutineType: LineRoutine<Self::DataType, Out>,
    {
        Line::new(routine, self)
    }
}
