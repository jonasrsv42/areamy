use crate::Pushable;
use crate::Pullable;

/// A [Source] can be used to [Pushable::push] data into the graph and is common where the source 
/// is some in-memory logic.
pub trait Source: Pushable {}

/// A [PullSource] is a data source that can be pulled from, typically implementing a Read interface.
/// It implements [crate::Pullable] to be consumed by pullable components in a graph. A prototypical
/// usecase is for a File input reader.
pub trait PullSource: Pullable {}

