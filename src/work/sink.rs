use crate::edge::policy::{Policied, PolicyEdge, SignalPolicy};
use crate::graph;
use crate::graph::Pushable;
use crate::signal::Origin;

/// A work [`Sink`]: a [graph::Sink] that can also block on [Pushable::push]. Nodes hold their
/// outputs as [`Sink`]s and real terminal sinks inherit it.
pub trait Sink: graph::Sink + Pushable {}
impl<T: graph::Sink + Pushable> Sink for T {}

impl<'params, DataType, SignalType> Policied
    for dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params
where
    DataType: 'params,
    SignalType: Origin + 'params,
{
    fn with_policy(this: Box<Self>, policy: SignalPolicy) -> Box<Self> {
        Box::new(PolicyEdge::new(this, policy))
    }
}
