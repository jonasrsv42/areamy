use crate::edge::policy::FollowsData;
use crate::error::Error;
use crate::graph::marker::Unary;
use crate::graph::{Add, Get, Outlet, Outputs, Pushes};

impl<NodeType> Pushes for NodeType
where
    NodeType: Outputs<Unary>,
    NodeType::Sink: FollowsData,
{
    fn pushes<DataType>(&mut self, down: &impl Get<Self::Sink, Unary>) -> Result<(), Error>
    where
        Self::Sink: Outlet<DataType = DataType>,
    {
        let sink = FollowsData::follow_data(down.get()?);
        Add::add(self, sink)
    }
}
