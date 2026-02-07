use crate::Pullable;
use crate::Pushable;
use crate::error::Error;

/// A [GraphPushSource] can be used to [Pushable::push] data into the graph and is common where
/// the source is some in-memory logic. It extends [Pushable] with a [GraphPushSource::close]
/// method to signal that no more data will be produced, allowing the graph to shut down gracefully.
pub trait GraphPushSource: Pushable {
    /// Close this source, signaling no more data will be produced.
    fn close(&mut self) -> Result<(), Error>;
}

/// A [GraphPullSource] is a data source that can be pulled from, typically implementing a Read
/// interface. It implements [crate::Pullable] to be consumed by pullable components in a graph.
/// A prototypical usecase is for a File input reader.
pub trait GraphPullSource: Pullable {
    /// Close this source, signaling no more data will be produced.
    fn close(&mut self) -> Result<(), Error>;
}
