use crate::graph;
use crate::graph::Pushable;

/// A work [`Sink`]: a [graph::Sink] that can also block on [Pushable::push]. Nodes hold their
/// outputs as [`Sink`]s and real terminal sinks inherit it.
pub trait Sink: graph::Sink + Pushable {}
impl<T: graph::Sink + Pushable> Sink for T {}
