use crate::edge::policy::{Policied, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::Unary;
use crate::graph::{Add, Get, Outlet, Outputs, Pushes};

impl<NodeType> Pushes for NodeType
where
    NodeType: Outputs<Unary>,
    NodeType::Sink: Policied,
{
    fn pushes<DataType>(&mut self, down: &impl Get<Self::Sink, Unary>) -> Result<(), Error>
    where
        Self::Sink: Outlet<DataType = DataType>,
    {
        let sink = Policied::with_policy(down.get()?, SignalPolicy::FollowData);
        Add::add(self, sink)
    }
}
