use crate::edge::policy::{Policied, PolicyEdge, SignalPolicy};
use crate::graph;
use crate::graph::{BoxAs, Pushable};
use crate::signal::Origin;

/// A work [`Sink`]: a [graph::Sink] that can also block on [Pushable::push]. Nodes hold their
/// outputs as [`Sink`]s and real terminal sinks inherit it.
pub trait Sink: graph::Sink + Pushable {}
impl<T: graph::Sink + Pushable> Sink for T {}

/// A boxed work [`Sink`], as nodes hold their outputs.
pub type DynSink<'params, DataType, SignalType> =
    Box<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>;

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

/// Anything that is a work [`Sink`] boxes as one.
impl<'params, T>
    BoxAs<dyn Sink<DataType = T::DataType, SignalType = T::SignalType> + Send + Sync + 'params>
    for T
where
    T: Sink + Send + Sync + 'params,
{
    fn box_as(
        self,
    ) -> Box<dyn Sink<DataType = T::DataType, SignalType = T::SignalType> + Send + Sync + 'params>
    {
        Box::new(self)
    }
}
